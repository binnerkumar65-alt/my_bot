process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (err) => {
  console.error('❌ Uncaught Exception:', err);
});

const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { NewMessage, EditedMessage } = require("telegram/events");
const express = require("express");
const axios = require("axios");
const bigInt = require("big-integer");

const app = express();
const PORT = process.env.PORT || 10000;

// Environment Variables Configuration
const apiId = parseInt(process.env.API_ID || "0");
const apiHash = process.env.API_HASH || "";
const stringSession = new StringSession(process.env.SESSION_STRING || "");

const SOURCE_CHAT = "@sxhckfufig";
const CHATGPT_BOT = "@chatgpt";
const FIREBASE_BASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com";
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || "https://my-bot-kgrk.onrender.com";

if (!apiId || !apiHash) {
  console.error("❌ API_ID या API_HASH missing हैं!");
  process.exit(1);
}

const client = new TelegramClient(stringSession, apiId, apiHash, {
  connectionRetries: 5,
});

let pendingMedia = {};

// Resolved once at startup - used for reliable ChatGPT bot detection
let chatgptBotId = null;

// Root Check for Render Health Check
app.get("/", (req, res) => {
  res.send("Node.js Proxy & Bot is Active!");
});

// -------------------------------------------------------------
// 1. STREAMING & DOWNLOAD ROUTE (chunked + Range support)
// -------------------------------------------------------------
app.get("/stream/:msgId", async (req, res) => {
  try {
    const msgId = parseInt(req.params.msgId);
    const entity = await client.getEntity(SOURCE_CHAT);
    const messages = await client.getMessages(entity, { ids: msgId });

    if (!messages || messages.length === 0 || !messages[0].media) {
      return res.status(404).send("Media not found");
    }

    const message = messages[0];
    const media = message.media;
    let mimeType = "video/mp4";
    let fileName = `file_${msgId}.mp4`;
    let fileSize = 0;

    if (media.document) {
      mimeType = media.document.mimeType || "video/mp4";
      fileSize = Number(media.document.size) || 0;
      if (media.document.attributes) {
        for (const attr of media.document.attributes) {
          if (attr.fileName) fileName = attr.fileName;
        }
      }
    }

    const range = req.headers.range;

    if (range && fileSize) {
      // Partial content request (seeking / progressive playback)
      const [startStr, endStr] = range.replace(/bytes=/, "").split("-");
      const start = parseInt(startStr, 10);
      const end = endStr ? parseInt(endStr, 10) : fileSize - 1;
      const chunkSize = end - start + 1;

      res.writeHead(206, {
        "Content-Range": `bytes ${start}-${end}/${fileSize}`,
        "Accept-Ranges": "bytes",
        "Content-Length": chunkSize,
        "Content-Type": mimeType,
      });

      const stream = client.iterDownload({
        file: media,
        offset: bigInt(start),
        limit: chunkSize,
      });

      for await (const chunk of stream) {
        res.write(chunk);
      }
      res.end();
    } else {
      // Full file request, still streamed in chunks (not buffered fully first)
      res.writeHead(200, {
        "Content-Type": mimeType,
        "Content-Length": fileSize,
        "Accept-Ranges": "bytes",
        "Content-Disposition": `inline; filename="${fileName}"`,
      });

      const stream = client.iterDownload({ file: media, offset: bigInt(0) });

      for await (const chunk of stream) {
        res.write(chunk);
      }
      res.end();
    }
  } catch (err) {
    console.error("❌ Stream Route Error:", err);
    if (!res.headersSent) {
      res.status(500).send("Streaming Error: " + err.message);
    }
  }
});

// -------------------------------------------------------------
// 2. FIREBASE PUSH LOGIC
// -------------------------------------------------------------
async function processReplyAndPushToFirebase(replyText, mediaInfo) {
  if (!replyText) return;

  const replyClean = replyText.trim().toLowerCase();
  const ignoreList = ["सोच...", "thinking...", "please wait...", "generating..."];

  if (ignoreList.some((ig) => replyClean.includes(ig))) {
    console.log("⏳ AI अभी सोच रहा है (सोच... state), स्किप कर रहे हैं।");
    return;
  }

  console.log(`📩 ChatGPT का असली जवाब: "${replyText}"`);

  let contentType = "@other";
  if (replyClean.includes("@dpp")) contentType = "@dpp";
  else if (replyClean.includes("@notes")) contentType = "@notes";

  const lecMatch = replyText.match(/@Lec\s*\d+/i);
  const lecTag = lecMatch ? lecMatch[0] : "";

  const rawTags = replyText.match(/@[^\s@]+/g) || [];
  const systemTags = ["@dpp", "@notes", "@other"];
  const validTags = [];

  for (const tag of rawTags) {
    const tLower = tag.toLowerCase();
    if (!systemTags.includes(tLower) && !tLower.startsWith("@lec")) {
      validTags.push(tag.replace("@", "").trim());
    }
  }

  let subjectName = validTags[0] || "General";
  let chapterName = validTags[1] || "General_Lectures";

  const subjectKey = subjectName.replace(/[.$#\[\]/]/g, "_");
  const chapterKey = chapterName.replace(/[.$#\[\]/]/g, "_");

  const dataPayload = {
    content_type: contentType,
    lecture_no: lecTag,
    raw_reply: replyText,
    timestamp: { ".sv": "timestamp" },
  };

  if (mediaInfo && mediaInfo.stream_link) {
    if (["@notes", "@dpp"].includes(contentType)) {
      dataPayload["download_link"] = mediaInfo.stream_link;
    } else {
      dataPayload["stream_link"] = mediaInfo.stream_link;
    }
  }

  const firebaseUrl = `${FIREBASE_BASE_URL}/${subjectKey}/${chapterKey}.json`;
  console.log(`🚀 Push target: ${subjectKey} ➔ ${chapterKey}`);

  try {
    const res = await axios.post(firebaseUrl, dataPayload);
    if (res.status === 200 || res.status === 201) {
      console.log(`🔥 SUCCESS! Firebase में डेटा पुश हो गया! Path: ${subjectKey} ➔ ${chapterKey}`);
    } else {
      console.error(`❌ Firebase Error Status: ${res.status}`);
    }
  } catch (err) {
    console.error(`❌ Firebase Exception:`, err.response ? err.response.data : err.message);
  }
}

// -------------------------------------------------------------
// 3. EVENT HANDLERS
// -------------------------------------------------------------
async function handleIncomingMessage(event) {
  try {
    const message = event.message;
    if (!message) return;

    const currentText = message.text || message.message || "";

    const chat = await message.getChat();
    const sender = await message.getSender();

    const chatUsername = (chat && chat.username ? chat.username : "").toLowerCase();
    const chatTitle = (chat && chat.title ? chat.title : "").toLowerCase();
    const senderUsername = (sender && sender.username ? sender.username : "").toLowerCase();
    const senderId = sender && sender.id ? sender.id.toString() : "";

    console.log(`🔍 DEBUG chat=${chatUsername} title=${chatTitle} sender=${senderUsername} senderId=${senderId}`);

    // A. Source Channel Check
    const isSourceChat =
      chatUsername === "sxhckfufig" ||
      chatTitle.includes("sxhckfufig");

    if (isSourceChat) {
      console.log(`\n📩 [STEP 1] Channel se Naya Message Aaya (ID: ${message.id})`);

      let streamLink = "";
      if (message.media) {
        streamLink = `${RENDER_URL}/stream/${message.id}`;
        console.log(`🔗 Stream Link Banna: ${streamLink}`);
      }

      pendingMedia["latest"] = { stream_link: streamLink, msg_id: message.id };

      const chatgptEntity = await client.getEntity(CHATGPT_BOT);
      const msgText = currentText || "Media File";

      await client.sendMessage(chatgptEntity, { message: msgText });
      console.log("➡️ [STEP 2] ChatGPT Bot ko query bhej di gayi!");
      return;
    }

    // B. ChatGPT Response Check - matched reliably via resolved bot ID
    const isChatGPT = chatgptBotId && senderId === chatgptBotId;

    if (isChatGPT) {
      console.log(`\n🤖 [STEP 3] ChatGPT Response Detect Hua: "${currentText}"`);
      const mediaInfo = pendingMedia["latest"] || {};
      await processReplyAndPushToFirebase(currentText, mediaInfo);
    }

  } catch (err) {
    console.error("❌ Event Handler Error:", err);
  }
}

// -------------------------------------------------------------
// 4. SERVER STARTUP
// -------------------------------------------------------------
async function startServer() {
  // 1. Render Port binding
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Server listening on 0.0.0.0:${PORT}`);
  });

  // 2. Connect Telegram Client
  try {
    await client.connect();
    console.log("✅ Telegram Client Connected!");

    // Resolve the ChatGPT bot's numeric ID once - used for reliable matching
    try {
      const botEntity = await client.getEntity(CHATGPT_BOT);
      chatgptBotId = botEntity.id.toString();
      console.log("🤖 ChatGPT Bot ID resolved:", chatgptBotId);
    } catch (e) {
      console.error("❌ ChatGPT Bot ID resolve nahi hua:", e.message);
    }

    // Safely add event handlers for New Message and Edited Message
    client.addEventHandler(handleIncomingMessage, new NewMessage({}));

    try {
      client.addEventHandler(handleIncomingMessage, new EditedMessage({}));
    } catch (e) {
      console.log("⚠️ EditedMessage direct constructor not supported, using fallback handler.");
    }

    console.log("🤖 Event Handlers Successfully Registered!");
  } catch (err) {
    console.error("❌ Telegram Client Connection Error:", err);
  }
}

startServer();
