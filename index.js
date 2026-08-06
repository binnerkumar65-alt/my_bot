// -------------------------------------------------------------
// 6. MAIN RUNNER & TELEGRAM EVENT LISTENER
// -------------------------------------------------------------
const { NewMessage } = require("telegram/events");

async function startServer() {
  await client.connect();
  console.log("✅ Telegram Client Connected!");

  // Event Listener with NewMessage Filter
  client.addEventHandler(async (event) => {
    try {
      const message = event.message;
      if (!message) return;

      const chat = await message.getChat();
      if (!chat) return;

      // 1. Check if message is from SOURCE_CHAT (@sxhckfufig)
      const isSourceChat = 
        (chat.username && chat.username.toLowerCase() === "sxhckfufig") ||
        (SOURCE_CHAT.includes(chat.id?.toString()));

      if (isSourceChat) {
        console.log(`[+] Naya Message Aaya (ID: ${message.id})`);
        
        let streamLink = "";
        if (message.media) {
          streamLink = `${RENDER_URL}/stream/${message.id}`;
          console.log(`🔗 Stream Link: ${streamLink}`);
        }

        pendingMedia["latest"] = { stream_link: streamLink, msg_id: message.id };

        // Send to ChatGPT Bot
        const chatgptEntity = await client.getEntity(CHATGPT_BOT);
        const msgText = message.text || "Media File";
        
        await client.sendMessage(chatgptEntity, { message: msgText });
        console.log("➡️ ChatGPT bot ko successfully bhej diya!");
      }

      // 2. Check if reply is from CHATGPT_BOT (@chatgpt)
      const isChatGPT = chat.username && chat.username.toLowerCase() === "chatgpt";
      
      if (isChatGPT) {
        console.log(`[+] AI Reply Aaya: ${message.text}`);
        const mediaInfo = pendingMedia["latest"] || {};
        await processReplyAndPushToFirebase(message.text, mediaInfo);
      }

    } catch (err) {
      console.error("❌ Event Handler Error:", err);
    }
  }, new NewMessage({}));

  app.listen(PORT, () => {
    console.log(`🚀 Node.js Streaming Proxy Running on Port ${PORT}`);
  });
}

startServer();
