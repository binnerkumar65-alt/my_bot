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
CHATGPT_BOT = "@chatgpt"  # AI Bot username

FIREBASE_BASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com"

if not API_ID or not API_HASH or not SESSION_STRING:
    raise RuntimeError("API_ID, API_HASH, ya SESSION_STRING env vars missing hain!")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)


# -------------------------------------------------------------
# 3. CHATGPT REPLY PARSER & FIREBASE PUSH
# -------------------------------------------------------------
def process_reply_and_push_to_firebase(reply_text):
    if not reply_text:
        return

    reply_clean = reply_text.strip().lower()

    # 🛑 1. AI की प्रोसेसिंग/सोचने वाली स्टेटस को रिजेक्ट करें
    ignore_list = ["सोच...", "thinking...", "please wait...", "generating..."]
    if any(ig in reply_clean for ig in ignore_list):
        log.info("⏳ AI अभी सोच रहा है, फाइनल उत्तर का इंतज़ार किया जा रहा है...")
        return

    # 2. Content Type पहचानना
    if "@notes" in reply_clean:
        content_type = "@notes"
    elif "@dpp" in reply_clean:
        content_type = "@dpp"
    elif "@other" in reply_clean:
        content_type = "@other"
    else:
        content_type = "video"  # अगर तीनों में से कुछ न मिले तो Video

    # 3. Lecture Number निकालना
    lec_match = re.search(r'(@Lec\s*\d+|@L\d+|Lec\s*\d+)', reply_text, re.IGNORECASE)
    lec_tag = lec_match.group(1) if lec_match else ""

    # 4. Chapter Name निकालना (@ से शुरू होने वाला नाम)
    tags = re.findall(r'@\w+', reply_text)
    chapter_name = "Uncategorized"
    
    for tag in tags:
        tag_lower = tag.lower()
        if tag_lower not in ["@notes", "@dpp", "@other"] and not tag_lower.startswith("@lec"):
            chapter_name = tag.replace("@", "").strip()
            break

    # 5. Firebase में Save करने के लिए Data Payload
    data_payload = {
        "content_type": content_type,
        "lecture_no": lec_tag,
        "raw_reply": reply_text,
        "timestamp": {".sv": "timestamp"}
    }

    firebase_url = f"{FIREBASE_BASE_URL}/{chapter_name}.json"

    try:
        res = requests.post(firebase_url, json=data_payload)
        if res.status_code == 200:
            log.info(f"🔥 Firebase me Chapter '{chapter_name}' ke andar FINAL data push ho gaya!")
        else:
            log.error(f"❌ Firebase Push Error: {res.status_code} - {res.text}")
    except Exception as e:
        log.error(f"❌ Firebase Exception: {e}")


# -------------------------------------------------------------
# 4. EVENT HANDLERS
# -------------------------------------------------------------

# Step A: Source Channel से मैसेज AI बॉट को भेजना
@client.on(events.NewMessage(chats=SOURCE_CHAT))
async def forward_to_chatgpt(event):
    try:
        log.info(f"[+] Naya message mila, ChatGPT ko bhej rahe hain: {event.text[:30]}...")
        await client.send_message(CHATGPT_BOT, event.text)
        log.info("➡️ ChatGPT bot ko message forward ho gaya!")
    except Exception as e:
        log.error(f"❌ Forwarding Error: {e}")


# Step B: AI का नया मैसेज या एडिटेड मैसेज पकड़ना
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

    log.info("✅ Bot fully active and connected to Firebase flow!")

    await asyncio.gather(
        hypercorn.asyncio.serve(app, config),
        client.run_until_disconnected()
    )

if __name__ == "__main__":
    asyncio.run(main())
