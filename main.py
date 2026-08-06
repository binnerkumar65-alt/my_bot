import os
import asyncio
from flask import Flask
from threading import Thread
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# -------------------------------------------------------------
# 1. FLASK SERVER (Render Uptime Keep-Alive)
# -------------------------------------------------------------
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is Alive and Running!"

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

# Background में Flask चलाएं
Thread(target=run_flask, daemon=True).start()

# -------------------------------------------------------------
# 2. CONFIGURATION
# -------------------------------------------------------------
API_ID = int(os.environ.get("API_ID", 30414263))
API_HASH = os.environ.get("API_HASH", "7ac29590d4ad54e141856dfa4cc04dac")
SESSION_STRING = os.environ.get("1BVtsOLUBu7NW664h3NBQatAj2edrdpG5Gi8MuFIGoCTBtzNu1HQBuziC3HrOF8YRuo1kt5yD91tVGKETYuALE_gS02KfMNN4R5Mn3xmYvOAH1Muc3S0bsYcYueXEa35-DIKHfM8xQDTXwODRs5PdeKdKwhtH_BhvY0um1lo4_mWeUU8Ew9vGqLJCEvQZtPrxIkLF9RP864uFY8a4dZickEoxXbO9GE-lffbOiv7BXJVYQCWsVloHd__Dw1i5A1Z-qiyOuNgqDKJrFvsAMzWKwLcdVIwWeJrtuAUsnGKmZYwMth4YOEhEhriInlX9x4UJ8_cAPDYH_DeMFtOTj71fJWYZZJE-cbQ=")

# यहाँ अपने सोर्स और टारगेट चैनल के यूज़रनेम / ID डालें
SOURCE_CHAT = "@sxhckfufig"      # जहाँ से मैसेज पढ़ना है
TARGET_CHAT = "@your_target_chat" # जहाँ मैसेज भेजना है (बदल लें)

if not SESSION_STRING:
    print("❌ SESSION_STRING नहीं मिला!")
    exit(1)

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

# -------------------------------------------------------------
# 3. MESSAGE FORWARDING EVENT HANDLER
# -------------------------------------------------------------
@client.on(events.NewMessage(chats=SOURCE_CHAT))
async def forward_handler(event):
    try:
        print(f"[+] नया मैसेज मिला: {event.text}")
        
        # मैसेज फॉरवर्ड/कॉपी करना
        if event.text:
            await client.send_message(TARGET_CHAT, event.text)
            print("✅ टेक्स्ट मैसेज सफलतापूर्वक फॉरवर्ड हो गया!")
            
    except Exception as e:
        print(f"❌ Forward Error: {e}")

# -------------------------------------------------------------
# 4. MAIN ASYNC RUNNER
# -------------------------------------------------------------
async def main():
    print("[+] Telethon Client कनेक्ट हो रहा है...")
    await client.start()
    print("✅ Telethon सफलतापूर्वक लॉगिन हो गया है! मैसेज सुनने के लिए तैयार...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
