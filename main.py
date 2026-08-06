import os
import asyncio
import logging
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

# 2. Configuration (Render के Environment Variables से खुद-ब-खुद उठेंगे)
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

# --------------------------------------------------------------------------
# 👇 केवल इन 2 लाइनों को बदलना है:
SOURCE_CHAT = "@sxhckfufig"         # जहाँ से मैसेज उठाना है
TARGET_CHAT = "@chatgpt"  # 👈 यहाँ अपने असली चैनल का Username या ID डालें
# --------------------------------------------------------------------------

if not API_ID or not API_HASH or not SESSION_STRING:
    raise RuntimeError("API_ID, API_HASH, ya SESSION_STRING env vars missing hain! Ye set karo deployment settings me.")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# 3. Forward Event
@client.on(events.NewMessage(chats=SOURCE_CHAT))
async def forward_handler(event):
    try:
        log.info(f"[+] Naya message mila: {event.text}")
        if event.text:
            await client.send_message(TARGET_CHAT, event.text)
            log.info("✅ Message forward ho gaya!")
    except Exception as e:
        log.error(f"❌ Error: {e}")

# 4. Main Runner
async def main():
    import hypercorn.asyncio
    from hypercorn.config import Config

    config = Config()
    config.bind = [f"0.0.0.0:{os.environ.get('PORT', 10000)}"]

    log.info("[+] Telethon client connect ho raha hai...")
    await client.connect()

    if not await client.is_user_authorized():
        log.error("❌ Session invalid ya login nahi hua! SESSION_STRING check karo.")
        return

    log.info("✅ Login successful! Messages sunne ke liye taiyar...")

    await asyncio.gather(
        hypercorn.asyncio.serve(app, config),
        client.run_until_disconnected()
    )

if __name__ == "__main__":
    asyncio.run(main())
