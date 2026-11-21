import os, json, base64, asyncio
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from websockets.asyncio.client import connect as ws_connect

app = FastAPI()
DOMAIN = os.environ["DOMAIN"]                      # e.g. my-restaurant.onrender.com (no https://)
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
    "vad_events": "true",       # SpeechStarted events, used for barge-in
})
DG_SPEAK_URL = "https://api.deepgram.com/v1/speak?" + urlencode({
    "model": "aura-2-thalia-en",
    "encoding": "mulaw",
    "sample_rate": 8000,
    "container": "none",        # raw audio, no WAV header
})
DG_HEADERS = {"Authorization": f"Token {DEEPGRAM_API_KEY}"}


@app.get("/")
def hello():
    return {"msg": "Hello World!"}


# Endpoint A: Twilio hits this when someone calls
@app.post("/voice")
def voice():
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say>Hi, thanks for calling Mario's! What can I get for you today?</Say>
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


# Endpoint B: the live call audio (Twilio Media Stream)
@app.websocket("/ws")
async def ws(twilio: WebSocket):
    await twilio.accept()
    stream_sid = None
    speaking: asyncio.Task | None = None

    async with httpx.AsyncClient(timeout=15) as http, \
               ws_connect(DG_LISTEN_URL, additional_headers=DG_HEADERS) as deepgram:

        async def speak(text: str):
            audio = await text_to_speech(http, text)
            chunk = 8000  # ~1 second of audio per message
            for i in range(0, len(audio), chunk):
                await twilio.send_text(json.dumps({
                    "event": "media",
                    "streamSid": stream_sid,
                    "media": {"payload": base64.b64encode(audio[i:i + chunk]).decode()},
                }))

        def stop_speaking():
            # Caller started talking: stop our audio (barge-in).
            nonlocal speaking
            if speaking and not speaking.done():
                speaking.cancel()
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
                reply = f"Converted speech: {text}"   # <- later: your AI agent goes here
                speaking = asyncio.create_task(speak(reply))

            async for raw in deepgram:
                msg = json.loads(raw)
                kind = msg.get("type")
                if kind == "Results":
                    transcript = msg["channel"]["alternatives"][0]["transcript"]
                    if msg.get("is_final") and transcript:
                        parts.append(transcript)
                    if msg.get("speech_final"):
                        flush()
                elif kind == "UtteranceEnd":
                    flush()
                elif kind == "SpeechStarted":
                    stop_speaking()

        await asyncio.gather(twilio_to_deepgram(), deepgram_to_twilio())
