import os
import re
import asyncio
import logging
import requests
from flask import Flask, Response, request
from telethon import TelegramClient, events
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# 1. Flask App Setup
app = Flask(__name__)

# 2. Configuration
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

SOURCE_CHAT = "@sxhckfufig"
CHATGPT_BOT = "@chatgpt"

FIREBASE_BASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com"
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://my-bot-qkto.onrender.com")

if not API_ID or not API_HASH or not SESSION_STRING:
    raise RuntimeError("API_ID, API_HASH, ya SESSION_STRING missing hain!")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# ग्लोबल डिक्शनरी ताकि चैट आईडी और मैसेज ट्रैक रह सके
PENDING_MEDIA = {}

@app.route('/')
def home():
    return "Stream & Forwarder Bot is Running!"

# -------------------------------------------------------------
# 3. PROXY STREAMING ROUTE (बिना डिस्क में सेव किए डायरेक्ट स्ट्रीम)
# -------------------------------------------------------------
@app.route('/stream/<int:msg_id>')
def stream_file(msg_id):
    """ टेलीग्राम से डायरेक्ट बिना डाउनलोड किए चंक्स (Chunks) में स्ट्रीम करेगा """
    async def generate():
        async with client:
            message = await client.get_messages(SOURCE_CHAT, ids=msg_id)
            if message and message.media:
                async for chunk in client.download_media(message, file=bytes, chunk_size=1024 * 1024):
                    yield chunk

    # Async generator को Flask Response में भेजना
    return Response(asyncio.run_coroutine_threadsafe(generate(), client.loop).result(), 
                    mimetype='application/octet-stream')


# -------------------------------------------------------------
# 4. HELPER: CHAPTER CLEANING
# -------------------------------------------------------------
def clean_chapter_name(raw_name):
    if not raw_name:
        return "Uncategorized"
    name = raw_name.replace("@", "").strip()
    name = re.sub(r'(?i)\b(lec|lecture|part|dpp|notes|class)\b.*', '', name)
    name = re.sub(r'[\d\-_\:()\[\]]+$', '', name).strip()
    return name if name else "Uncategorized"


# -------------------------------------------------------------
# 5. FIREBASE PUSH WITH STREAM LINKS
# -------------------------------------------------------------
def process_reply_and_push_to_firebase(reply_text, media_info):
    if not reply_text:
        return

    reply_clean = reply_text.strip().lower()

    # AI सोचने वाली स्टेटस इग्नोर करें
    ignore_list = ["सोच...", "thinking...", "please wait...", "generating..."]
    if any(ig in reply_clean for ig in ignore_list):
        log.info("⏳ AI जवाब जनरेट कर रहा है, इंतज़ार...")
        return

    # Content Type
    if "@notes" in reply_clean:
        content_type = "@notes"
    elif "@dpp" in reply_clean:
        content_type = "@dpp"
    elif "@other" in reply_clean:
        content_type = "@other"
    else:
        content_type = "video"

    # Lecture Tag
    lec_match = re.search(r'(@Lec\s*\d+|@L\d+|Lec\s*\d+)', reply_text, re.IGNORECASE)
    lec_tag = lec_match.group(1) if lec_match else ""

    # Chapter Tag Extraction
    tags = re.findall(r'@[^\s@]+(?:\s+[^\s@]+)*', reply_text)
    subject_name = "Biology"
    raw_chapter_name = ""

    valid_tags = []
    for tag in tags:
        t_clean = tag.strip()
        t_lower = t_clean.lower()
        if t_lower not in ["@notes", "@dpp", "@other"] and not t_lower.startswith("@lec"):
            valid_tags.append(t_clean)

    if len(valid_tags) >= 2:
        subject_name = valid_tags[0].replace("@", "").strip()
        raw_chapter_name = valid_tags[1]
    elif len(valid_tags) == 1:
        raw_chapter_name = valid_tags[0]

    chapter_name = clean_chapter_name(raw_chapter_name)

    subject_key = re.sub(r'[.$#\[\]/]', '', subject_name)
    chapter_key = re.sub(r'[.$#\[\]/]', '', chapter_name)

    # Payload
    data_payload = {
        "content_type": content_type,
        "lecture_no": lec_tag,
        "raw_reply": reply_text,
        "timestamp": {".sv": "timestamp"}
    }

    # अगर फाइल/वीडियो अटैच है, तो उसका स्ट्रीम/डाउनलोड लिंक जोड़ें
    if media_info and "stream_link" in media_info:
        if content_type in ["@notes", "@dpp"]:
            data_payload["download_link"] = media_info["stream_link"]
        else:
            data_payload["stream_link"] = media_info["stream_link"]

    firebase_url = f"{FIREBASE_BASE_URL}/{subject_key}/{chapter_key}.json"

    try:
        res = requests.post(firebase_url, json=data_payload)
        if res.status_code == 200:
            log.info(f"🔥 Firebase Link & Data Push Success: {subject_key} ➔ {chapter_key}")
        else:
            log.error(f"❌ Firebase Error: {res.status_code} - {res.text}")
    except Exception as e:
        log.error(f"❌ Firebase Exception: {e}")


# -------------------------------------------------------------
# 6. TELETHON EVENTS
# -------------------------------------------------------------
@client.on(events.NewMessage(chats=SOURCE_CHAT))
async def forward_to_chatgpt(event):
    global PENDING_MEDIA
    try:
        log.info(f"[+] Naya message mila (ID: {event.id})...")
        
        # अगर वीडियो/पीडीएफ अटैचमेंट है, तो स्ट्रीम लिंक बनाओ
        stream_link = ""
        if event.media:
            stream_link = f"{RENDER_URL}/stream/{event.id}"
            log.info(f"🔗 Proxy Stream Link Generated: {stream_link}")

        PENDING_MEDIA['latest'] = {"stream_link": stream_link, "msg_id": event.id}

        # ChatGPT को मैसेज भेजो
        msg_text = event.text if event.text else "Media File Received"
        await client.send_message(CHATGPT_BOT, msg_text)
        log.info("➡️ ChatGPT bot ko query forward ho gayi!")
    except Exception as e:
        log.error(f"❌ Forward Error: {e}")


@client.on(events.NewMessage(chats=CHATGPT_BOT))
@client.on(events.MessageEdited(chats=CHATGPT_BOT))
async def handle_chatgpt_reply(event):
    try:
        log.info(f"[+] AI Reply: {event.text}")
        media_info = PENDING_MEDIA.get('latest', {})
        process_reply_and_push_to_firebase(event.text, media_info)
    except Exception as e:
        log.error(f"❌ Processing Error: {e}")


# -------------------------------------------------------------
# 7. MAIN RUNNER
# -------------------------------------------------------------
async def main():
    import hypercorn.asyncio
    from hypercorn.config import Config

    config = Config()
    config.bind = [f"0.0.0.0:{os.environ.get('PORT', 10000)}"]

    log.info("[+] Telethon client connecting...")
    await client.connect()

    if not await client.is_user_authorized():
        log.error("❌ Session Invalid!")
        return

    log.info("✅ Proxy Stream & Download Engine Active!")

    await asyncio.gather(
        hypercorn.asyncio.serve(app, config),
        client.run_until_disconnected()
    )

if __name__ == "__main__":
    asyncio.run(main())
