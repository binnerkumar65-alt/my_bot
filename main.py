import os
import asyncio
import requests
from telethon import TelegramClient, events

# 1. Telegram Credentials (यहाँ अपनी डिटेल्स भरें)
API_ID = 30414263  # अपना my.telegram.org वाला API ID डालें (बिना Quotes के)
API_HASH = "7ac29590d4ad54e141856dfa4cc04dac"  # अपना API HASH डालें

# 2. Target Channels & Bots
SOURCE_CHANNEL = "sxhckfufig"
TARGET_BOT = "chatgpt"

# 3. Firebase URL
FIREBASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com/telegram_data.json"

# Client Init (यह अपने आप 'my_session.session' फ़ाइल बना लेगा)
client = TelegramClient('my_session', API_ID, API_HASH)

def push_to_firebase(data):
    try:
        res = requests.post(FIREBASE_URL, json=data)
        if res.status_code == 200:
            print("[+] Successfully pushed to Firebase!")
        else:
            print(f"[-] Firebase Error: {res.status_code}")
    except Exception as e:
        print(f"[-] Firebase Push Failed: {e}")

@client.on(events.NewMessage(chats=SOURCE_CHANNEL))
async def handle_new_message(event):
    source_text = event.message.message
    if not source_text:
        return

    print(f"\n[+] New message from @{SOURCE_CHANNEL}")

    try:
        # Step 1: ChatGPT Bot को मैसेज भेजें
        async with client.conversation(TARGET_BOT, timeout=60) as conv:
            await conv.send_message(source_text)
            
            # Step 2: ChatGPT Bot के रिप्लाई का इंतज़ार करें
            response = await conv.get_response()
            reply_text = response.text

            print(f"[+] Received reply from @{TARGET_BOT}")

            # Step 3: Firebase Payload
            payload = {
                "source_text": source_text,
                "processed_tags": reply_text,
                "timestamp": event.message.date.isoformat()
            }

            # Step 4: Firebase में भेजें
            push_to_firebase(payload)

    except asyncio.TimeoutError:
        print(f"[-] Timeout: @{TARGET_BOT} response took too long.")
    except Exception as e:
        print(f"[-] Error: {e}")

async def main():
    print("[+] Starting Telethon Client...")
    await client.start()  # पहली बार चलाने पर यही आपसे Phone Number + OTP पूछेगा
    print("[+] Client is running & listening for messages...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
