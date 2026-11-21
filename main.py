import os, json, base64, asyncio, time
from urllib.parse import urlencode

import anthropic
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from websockets.asyncio.client import connect as ws_connect

app = FastAPI()
DOMAIN = os.environ["DOMAIN"]                      # e.g. my-app.onrender.com (no https://)
DEEPGRAM_API_KEY = os.environ["DEEPGRAM_API_KEY"]  # never hard-code the key

# Twilio Media Streams send/expect 8 kHz mu-law mono audio.
DG_LISTEN_URL = "wss://api.deepgram.com/v1/listen?" + urlencode({
    "model": "nova-3",
    "encoding": "mulaw",
    "sample_rate": 8000,
    "channels": 1,
    "smart_format": "true",
    "interim_results": "true",
    "endpointing": 300,         # ms of silence that ends a phrase
    "utterance_end_ms": 1000,   # backup "caller finished talking" signal
})
DG_SPEAK_URL = "https://api.deepgram.com/v1/speak?" + urlencode({
    "model": "aura-2-thalia-en",
    "encoding": "mulaw",
    "sample_rate": 8000,
    "container": "none",        # raw audio, no WAV header
})
DG_HEADERS = {"Authorization": f"Token {DEEPGRAM_API_KEY}"}

# Reads ANTHROPIC_API_KEY from the environment.
claude = anthropic.AsyncAnthropic()
CLAUDE_MODEL = "claude-opus-5-5"
CLAUDE_SYSTEM = (
    "You answer questions asked by phone callers. Your reply is spoken aloud, "
    "so answer each question in exactly one sentence that is precise and to "
    "the point. Use plain spoken language: no lists, markdown, or emojis."
)
FALLBACK_REPLY = "Sorry, I didn't catch that, could you say it again?"


@app.get("/")
def hello():
    return {"msg": "Hello World!"}


# Endpoint A: Twilio hits this when someone calls
@app.post("/voice")
def voice():
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say>Hi! Ask me any question and I'll answer it.</Say>
  <Connect>
    <Stream url="wss://{DOMAIN}/ws" />
  </Connect>
</Response>"""
    return Response(content=twiml, media_type="text/xml")


async def text_to_speech(http: httpx.AsyncClient, text: str) -> bytes:
    """Deepgram Aura TTS -> raw 8 kHz mu-law bytes, ready for Twilio."""
    r = await http.post(DG_SPEAK_URL, headers=DG_HEADERS, json={"text": text})
    r.raise_for_status()
    return r.content


async def ask_claude(history: list[dict]) -> str:
    """Send the conversation so far to Claude and return its one-sentence reply."""
    try:
        response = await claude.beta.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,   # thinking counts toward this, so leave room
            system=CLAUDE_SYSTEM,
            messages=history,
            output_config={"effort": "low"},   # low latency for a live call
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",               # retry on another model if refused
        )
    except anthropic.APIError as e:
        print("Claude error:", e)
        return FALLBACK_REPLY
    if response.stop_reason == "refusal":
        return FALLBACK_REPLY
    if response.stop_reason == "max_tokens":
        print("Claude reply hit max_tokens and was truncated")
    text = " ".join(b.text for b in response.content if b.type == "text").strip()
    return text or FALLBACK_REPLY


# Endpoint B: the live call audio (Twilio Media Stream)
@app.websocket("/ws")
async def ws(twilio: WebSocket):
    await twilio.accept()
    stream_sid = None
    speaking: asyncio.Task | None = None
    playing_until = 0.0       # when Twilio should finish playing our audio
    history: list[dict] = []  # this call's conversation with Claude

    async with httpx.AsyncClient(timeout=15) as http, \
               ws_connect(DG_LISTEN_URL, additional_headers=DG_HEADERS) as deepgram:

        async def respond(caller_text: str):
            # Speech-to-text result -> Claude -> text-to-speech -> caller.
            history.append({"role": "user", "content": caller_text})
            reply = await ask_claude(history)
            history.append({"role": "assistant", "content": reply})
            print("Claude:", reply)
            await speak(reply)

        async def speak(text: str):
            nonlocal playing_until
            audio = await text_to_speech(http, text)
            # 8 kHz mu-law = 8000 bytes per second of audio.
            playing_until = time.monotonic() + len(audio) / 8000
            chunk = 8000  # ~1 second of audio per message
            for i in range(0, len(audio), chunk):
                await twilio.send_text(json.dumps({
                    "event": "media",
                    "streamSid": stream_sid,
                    "media": {"payload": base64.b64encode(audio[i:i + chunk]).decode()},
                }))

        def stop_speaking(heard: str):
            # Caller said real words while we were answering: stop (barge-in).
            nonlocal playing_until
            busy = speaking and not speaking.done()
            if not busy and time.monotonic() >= playing_until:
                return  # nothing is playing, nothing to interrupt
            print("Barge-in, caller said:", heard)
            if busy:
                speaking.cancel()
            playing_until = 0.0
            if stream_sid:
                asyncio.create_task(twilio.send_text(json.dumps(
                    {"event": "clear", "streamSid": stream_sid})))

        async def twilio_to_deepgram():
            nonlocal stream_sid
            try:
                async for raw in twilio.iter_text():
                    msg = json.loads(raw)
                    event = msg.get("event")
                    if event == "start":
                        stream_sid = msg["start"]["streamSid"]
                        print("Call started:", msg["start"].get("callSid"))
                    elif event == "media":
                        await deepgram.send(base64.b64decode(msg["media"]["payload"]))
                    elif event == "stop":
                        print("Call ended")
                        break
            except WebSocketDisconnect:
                pass
            finally:
                await deepgram.send(json.dumps({"type": "CloseStream"}))

        async def deepgram_to_twilio():
            nonlocal speaking
            parts: list[str] = []  # final pieces of the current utterance

            def flush():
                nonlocal speaking
                text = " ".join(parts).strip()
                parts.clear()
                if not text:
                    return
                print("Caller:", text)
                speaking = asyncio.create_task(respond(text))

            async for raw in deepgram:
                msg = json.loads(raw)
                kind = msg.get("type")
                if kind == "Results":
                    transcript = msg["channel"]["alternatives"][0]["transcript"]
                    if transcript:
                        # Only transcribed words count as an interruption,
                        # not noise, coughs, or echo of our own audio.
                        stop_speaking(transcript)
                    if msg.get("is_final") and transcript:
                        parts.append(transcript)
                    if msg.get("speech_final"):
                        flush()
                elif kind == "UtteranceEnd":
                    flush()

        await asyncio.gather(twilio_to_deepgram(), deepgram_to_twilio())
