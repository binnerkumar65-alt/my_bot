import asyncio
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# 1. अपनी Telegram API डिटेल्स डालें
API_ID = 30414263  # my.telegram.org वाला API ID
API_HASH = "7ac29590d4ad54e141856dfa4cc04dac"  # my.telegram.org वाला API HASH

# 2. अपने Telegram Bot का Token (BotFather वाला) डालें
BOT_TOKEN = "8870188556:AAGm_fIAr3FftXU2ySZC-UEzxSVXaWuoBqo"

bot = TelegramClient('bot_session', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

user_data = {}

@bot.on(events.NewMessage(pattern='/start'))
async def start(event):
    await event.respond("👋 नमस्ते! अपना Telegram Mobile Number (+91...) यहाँ भेजें:")

@bot.on(events.NewMessage)
async def handle_message(event):
    user_id = event.sender_id
    text = event.text.strip()

    if text == '/start':
        return

    # Step A: Phone Number रिसीव करना
    if user_id not in user_data:
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        
        try:
            res = await client.send_code_request(text)
            user_data[user_id] = {
                'client': client,
                'phone': text,
                'phone_code_hash': res.phone_code_hash
            }
            await event.respond("📩 Telegram App पर एक **OTP Code** आया होगा। कृपया वह OTP यहाँ भेजें:")
        except Exception as e:
            await event.respond(f"❌ Error: {e}\n\n/start दबाकर दोबारा कोशिश करें।")

    # Step B: OTP रिसीव करना और Login करना
    elif 'phone_code_hash' in user_data[user_id]:
        data = user_data[user_id]
        client = data['client']
        
        try:
            await client.sign_in(
                phone=data['phone'],
                code=text,
                phone_code_hash=data['phone_code_hash']
            )
            
            # Session String प्राप्त करना
            session_str = client.session.save()
            
            await event.respond("✅ **Login Successful!**\n\nआपकी `SESSION_STRING` यह रही:")
            await event.respond(f"`{session_str}`")
            await event.respond("👆 इसे पूरी तरह से कॉपी करें और Render के Environment Variables में paste कर दें।")
            
            await client.disconnect()
            del user_data[user_id]

        except Exception as e:
            await event.respond(f"❌ OTP गलत है या Error आया: {e}")

print("Session Generator Bot चालू हो गया है...")
bot.run_until_disconnected()
