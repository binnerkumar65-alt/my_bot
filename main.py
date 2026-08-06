import os
import asyncio
from flask import Flask
from threading import Thread
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# -------------------------------------------------------------
# 1. FLASK SERVER (Render Uptime के लिए)
# -------------------------------------------------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is Alive and Running!"

def run_flask():
    # Render स्वचालित रूप से PORT चुनता है (default 10000)
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

# Background में Flask सर्वर स्टार्ट करें
Thread(target=run_flask, daemon=True).start()

# -------------------------------------------------------------
# 2. CONFIGURATION (Environment Variables से उठाएगा)
# -------------------------------------------------------------
# आप चाहें तो Environment Variables यूज़ करें या डायरेक्ट अपनी डिटेल्स डालें
API_ID = int(os.environ.get("30414263", 1234567))        # अपना API_ID लिखें
API_HASH = os.environ.get("7ac29590d4ad54e141856dfa4cc04dac", "YOUR_API_HASH") # अपना API_HASH लिखें
SESSION_STRING = os.environ.get("SESSION_STRING")      # Render में डाला गया Session String

# -------------------------------------------------------------
# 3. TELETHON CLIENT INITIALIZATION
# -------------------------------------------------------------
if not SESSION_STRING:
    print("❌ ERROR: SESSION_STRING नहीं मिला! Render के Environment Variables में SESSION_STRING सेट करें।")
    exit(1)

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# -------------------------------------------------------------
# 4. BOT EVENTS & LOGIC
# -------------------------------------------------------------
# उदाहरण: जब कोई मैसेज आए (आप अपना ऑटोमेशन लॉजिक यहाँ बदल सकते हैं)
@client.on(events.NewMessage)
async def my_event_handler(event):
    # Telegram Channel (@sxhckfufig) से मैसेज हैंडलिंग
    print(f"[+] Message received: {event.text}")

# -------------------------------------------------------------
# 5. MAIN ASYNC RUNNER
# -------------------------------------------------------------
async def main():
    print("[+] Telethon Client स्टार्ट हो रहा है...")
    await client.start()
    print("✅ Bot सफलतापूर्वक लॉगिन हो गया है और लाइव है!")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
