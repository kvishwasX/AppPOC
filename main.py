import os, json
from fastapi import FastAPI, WebSocket
from fastapi.responses import Response

app = FastAPI()
DOMAIN = os.environ["DOMAIN"]   # e.g. my-restaurant.onrender.com (no https://)

@app.get("/")
def hello():
    return {"msg": "Hello World!"}

# Endpoint A: Twilio hits this when someone calls
@app.post("/voice")
def voice():
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <ConversationRelay url="wss://{DOMAIN}/ws"
      welcomeGreeting="Hi, thanks for calling Mario's! What can I get for you today?" />
  </Connect>
</Response>"""
    return Response(content=twiml, media_type="text/xml")

# Endpoint B: the live conversation
@app.websocket("/ws")
async def ws(websocket: WebSocket):
    await websocket.accept()
    async for raw in websocket.iter_text():
        msg = json.loads(raw)

        if msg["type"] == "setup":
            print("Call started:", msg.get("callSid"), msg.get("from"))

        elif msg["type"] == "prompt" and msg.get("last", True):
            caller_said = msg["voicePrompt"]
            print("Caller:", caller_said)

            reply = f"You said: {caller_said}"   # <- later: your AI agent goes here

            await websocket.send_text(json.dumps({
                "type": "text", "token": reply, "last": True
            }))

        elif msg["type"] == "interrupt":
            print("Caller interrupted the agent")

        elif msg["type"] == "error":
            print("Twilio error:", msg)
