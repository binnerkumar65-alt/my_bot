import os
import asyncio
import requests
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# Environment Variables से डेटा उठाएगा
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING")

# Target Details
SOURCE_CHANNEL = "sxhckfufig"
TARGET_BOT = "chatgpt"
FIREBASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com/telegram_data.json"

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

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

    print(f"\n[+] New message received from @{SOURCE_CHANNEL}")

    try:
        # ChatGPT Bot से बात करना
        async with client.conversation(TARGET_BOT, timeout=60) as conv:
            await conv.send_message(source_text)
            response = await conv.get_response()
            reply_text = response.text

            print(f"[+] Received reply from @{TARGET_BOT}")

            # Firebase Payload
            payload = {
                "source_text": source_text,
                "processed_tags": reply_text,
                "timestamp": event.message.date.isoformat()
            }

            push_to_firebase(payload)

    except asyncio.TimeoutError:
        print(f"[-] Timeout: @{TARGET_BOT} response took too long.")
    except Exception as e:
        print(f"[-] Error: {e}")

async def main():
    print("[+] Starting Telethon Client...")
    await client.start()
    print("[+] Client is running & listening for messages...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
