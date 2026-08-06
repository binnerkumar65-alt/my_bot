import os
import re
import asyncio
import logging
import requests
from quart import Quart, Response, request
from telethon import TelegramClient, events
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Quart(__name__)

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

SOURCE_CHAT = "@sxhckfufig"
CHATGPT_BOT = "@chatgpt"

FIREBASE_BASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com"
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL", "https://my-bot-qkto.onrender.com")

if not API_ID or not API_HASH or not SESSION_STRING:
    raise RuntimeError("API_ID, API_HASH, या SESSION_STRING मिसिंग हैं!")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

PENDING_MEDIA = {}

@app.route('/')
async def home():
    return "Stream & Forwarder Bot is Active!"

# -------------------------------------------------------------
# 3. ADVANCED MEDIA ROUTE (Range-based Video Stream + Fast PDF)
# -------------------------------------------------------------
@app.route('/stream/<int:msg_id>')
async def stream_file(msg_id):
    try:
        message = await client.get_messages(SOURCE_CHAT, ids=msg_id)
        if not message or not message.media:
            return "Media not found", 404

        file_size = 0
        mime_type = "application/octet-stream"
        file_name = f"file_{msg_id}"

        if hasattr(message.media, 'document'):
            doc = message.media.document
            file_size = doc.size
            mime_type = doc.mime_type or mime_type
            for attr in doc.attributes:
                if hasattr(attr, 'file_name') and attr.file_name:
                    file_name = attr.file_name

        # 📄 PDF / Documents Solution (यह पहले से सही काम कर रहा है)
        if "pdf" in mime_type.lower() or "document" in mime_type.lower():
            file_bytes = await client.download_media(message, file=bytes)
            return Response(
                file_bytes,
                mimetype=mime_type,
                headers={
                    "Content-Disposition": f'attachment; filename="{file_name}"',
                    "Content-Length": str(len(file_bytes))
                }
            )

        # 🎥 Video Seeking & Fast Proxy Stream (502 Timeout Fix)
        range_header = request.headers.get('Range', None)
        
        start_bytes = 0
        end_bytes = file_size - 1 if file_size > 0 else 0

        if range_header:
            match = re.search(r'bytes=(\d+)-(\d*)', range_header)
            if match:
                start_bytes = int(match.group(1))
                if match.group(2):
                    end_bytes = int(match.group(2))

        chunk_len = end_bytes - start_bytes + 1

        async def generate_video_chunks():
            try:
                # Telethon se specific byte-offset seek karke chunk manga rahe hain
                async for chunk in client.download_media(
                    message, 
                    file=bytes, 
                    offset=start_bytes, 
                    chunk_size=1024 * 512
                ):
                    yield chunk
            except Exception as e:
                log.error(f"Video Chunk Streaming Error: {e}")

        status_code = 206 if range_header else 200
        headers = {
            "Content-Type": "video/mp4",
            "Content-Disposition": f'inline; filename="{file_name}.mp4"',
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {start_bytes}-{end_bytes}/{file_size if file_size else '*'}",
            "Content-Length": str(chunk_len) if file_size > 0 else ""
        }

        return Response(generate_video_chunks(), status=status_code, headers=headers)

    except Exception as e:
        log.error(f"Stream Exception: {e}")
        return f"Streaming Error: {str(e)}", 500


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
# 5. FIREBASE PUSH LOGIC
# -------------------------------------------------------------
def process_reply_and_push_to_firebase(reply_text, media_info):
    if not reply_text:
        return

    reply_clean = reply_text.strip().lower()

    ignore_list = ["सोच...", "thinking...", "please wait...", "generating..."]
    if any(ig in reply_clean for ig in ignore_list):
        log.info("⏳ AI जवाब तैयार कर रहा है...")
        return

    if "@notes" in reply_clean:
        content_type = "@notes"
    elif "@dpp" in reply_clean:
        content_type = "@dpp"
    elif "@other" in reply_clean:
        content_type = "@other"
    else:
        content_type = "video"

    lec_match = re.search(r'(@Lec\s*\d+|@L\d+|Lec\s*\d+)', reply_text, re.IGNORECASE)
    lec_tag = lec_match.group(1) if lec_match else ""

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

    data_payload = {
        "content_type": content_type,
        "lecture_no": lec_tag,
        "raw_reply": reply_text,
        "timestamp": {".sv": "timestamp"}
    }

    if media_info and "stream_link" in media_info and media_info["stream_link"]:
        if content_type in ["@notes", "@dpp"]:
            data_payload["download_link"] = media_info["stream_link"]
        else:
            data_payload["stream_link"] = media_info["stream_link"]

    firebase_url = f"{FIREBASE_BASE_URL}/{subject_key}/{chapter_key}.json"

    try:
        res = requests.post(firebase_url, json=data_payload)
        if res.status_code == 200:
            log.info(f"🔥 Firebase Push Success: {subject_key} ➔ {chapter_key}")
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
        log.info(f"[+] Naya message (ID: {event.id})...")
        
        stream_link = ""
        if event.media:
            stream_link = f"{RENDER_URL}/stream/{event.id}"
            log.info(f"🔗 Generated Link: {stream_link}")

        PENDING_MEDIA['latest'] = {"stream_link": stream_link, "msg_id": event.id}

        msg_text = event.text if event.text else "Media File"
        await client.send_message(CHATGPT_BOT, msg_text)
        log.info("➡️ ChatGPT bot ko query bheji!")
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

    log.info("[+] Telethon client connect ho raha hai...")
    await client.connect()

    if not await client.is_user_authorized():
        log.error("❌ Session Invalid!")
        return

    log.info("✅ Range-Supported Video Proxy Server Ready!")

    await asyncio.gather(
        hypercorn.asyncio.serve(app, config),
        client.run_until_disconnected()
    )

if __name__ == "__main__":
    asyncio.run(main())
