import os
import re
import asyncio
import logging
import requests
from flask import Flask
from telethon import TelegramClient, events
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# 1. Flask App
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is Alive and Running!"

# 2. Configuration
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

SOURCE_CHAT = "@sxhckfufig"
CHATGPT_BOT = "@chatgpt"

FIREBASE_BASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com"

if not API_ID or not API_HASH or not SESSION_STRING:
    raise RuntimeError("API_ID, API_HASH, ya SESSION_STRING env vars missing hain!")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# -------------------------------------------------------------
# Helper: चैप्टर नेम से एक्स्ट्रा कचरा/नंबर हटाने का फ़ंक्शन
# -------------------------------------------------------------
def clean_chapter_name(raw_name):
    if not raw_name:
        return "Uncategorized"
    
    # 1. @ हटाएँ
    name = raw_name.replace("@", "").strip()
    
    # 2. 'Lec', 'Lecture', 'Part', '01-99' आदि एक्स्ट्रा शब्द हटाएँ
    name = re.sub(r'(?i)\b(lec|lecture|part|dpp|notes|class)\b.*', '', name)
    
    # 3. लास्ट के नंबर, स्पेशल कैरेक्टर (-, _, :, numbers) हटाएँ
    name = re.sub(r'[\d\-_\:()\[\]]+$', '', name).strip()
    
    # 4. अगर सफाई के बाद नाम खाली हो जाए तो डिफ़ॉल्ट दें
    return name if name else "Uncategorized"


# -------------------------------------------------------------
# 3. CHATGPT REPLY PARSER & FIREBASE PUSH
# -------------------------------------------------------------
def process_reply_and_push_to_firebase(reply_text):
    if not reply_text:
        return

    reply_clean = reply_text.strip().lower()

    # 🛑 1. AI की सोचने वाली स्टेटस को इग्नोर करना
    ignore_list = ["सोच...", "thinking...", "please wait...", "generating..."]
    if any(ig in reply_clean for ig in ignore_list):
        log.info("⏳ AI अभी सोच रहा है, इंतज़ार किया जा रहा है...")
        return

    # 2. Content Type पहचानना
    if "@notes" in reply_clean:
        content_type = "@notes"
    elif "@dpp" in reply_clean:
        content_type = "@dpp"
    elif "@other" in reply_clean:
        content_type = "@other"
    else:
        content_type = "video"

    # 3. Lecture Number निकालना
    lec_match = re.search(r'(@Lec\s*\d+|@L\d+|Lec\s*\d+)', reply_text, re.IGNORECASE)
    lec_tag = lec_match.group(1) if lec_match else ""

    # 4. Tags Extractions
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

    # ✨ चैप्टर नेम की सफाई (ताकि एक्स्ट्रा टेक्स्ट होने पर भी सही फोल्डर मिले)
    chapter_name = clean_chapter_name(raw_chapter_name)

    # Firebase Keys के लिए अमान्य कैरेक्टर्स (., $, #, [, ], /) हटाना
    subject_key = re.sub(r'[.$#\[\]/]', '', subject_name)
    chapter_key = re.sub(r'[.$#\[\]/]', '', chapter_name)

    # 5. Firebase Payload Setup
    data_payload = {
        "content_type": content_type,
        "lecture_no": lec_tag,
        "raw_reply": reply_text,
        "timestamp": {".sv": "timestamp"}
    }

    firebase_url = f"{FIREBASE_BASE_URL}/{subject_key}/{chapter_key}.json"

    try:
        res = requests.post(firebase_url, json=data_payload)
        if res.status_code == 200:
            log.info(f"🔥 Firebase Push Success: {subject_key} ➔ {chapter_key}")
        else:
            log.error(f"❌ Firebase Push Error: {res.status_code} - {res.text}")
    except Exception as e:
        log.error(f"❌ Firebase Exception: {e}")


# -------------------------------------------------------------
# 4. EVENT HANDLERS
# -------------------------------------------------------------
@client.on(events.NewMessage(chats=SOURCE_CHAT))
async def forward_to_chatgpt(event):
    try:
        log.info(f"[+] Naya message mila: {event.text[:30]}...")
        await client.send_message(CHATGPT_BOT, event.text)
        log.info("➡️ ChatGPT bot ko message forward ho gaya!")
    except Exception as e:
        log.error(f"❌ Forwarding Error: {e}")


@client.on(events.NewMessage(chats=CHATGPT_BOT))
@client.on(events.MessageEdited(chats=CHATGPT_BOT))
async def handle_chatgpt_reply(event):
    try:
        log.info(f"[+] AI Response: {event.text}")
        process_reply_and_push_to_firebase(event.text)
    except Exception as e:
        log.error(f"❌ Processing Error: {e}")


# -------------------------------------------------------------
# 5. MAIN RUNNER
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

    log.info("✅ Clean Chapter Filter implementation Active!")

    await asyncio.gather(
        hypercorn.asyncio.serve(app, config),
        client.run_until_disconnected()
    )

if __name__ == "__main__":
    asyncio.run(main())
