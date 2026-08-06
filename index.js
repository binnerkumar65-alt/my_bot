process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (err) => {
  console.error('❌ Uncaught Exception:', err);
});

const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { NewMessage } = require("telegram/events");
const express = require("express");
const axios = require("axios");

const app = express();
const PORT = process.env.PORT || 10000;

// Environment Variables Configuration
const apiId = parseInt(process.env.API_ID || "0");
const apiHash = process.env.API_HASH || "";
const stringSession = new StringSession(process.env.SESSION_STRING || "");

const SOURCE_CHAT = "@sxhckfufig";
const CHATGPT_BOT = "@chatgpt";
const FIREBASE_BASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com";
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || "https://my-bot-ck6m.onrender.com";

if (!apiId || !apiHash) {
  console.error("❌ API_ID या API_HASH missing हैं!");
  process.exit(1);
}

// GramJS Telegram Client Initialization
const client = new TelegramClient(stringSession, apiId, apiHash, {
  connectionRetries: 5,
});

let pendingMedia = {};

// Root Status Check
app.get("/", (req, res) => {
  res.send("Node.js Fast Streaming & Forwarder Bot is Active!");
});

// -------------------------------------------------------------
// 1. FAST STREAMING & DOWNLOAD ROUTE
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
    let fileSize = 0;
    let fileName = `file_${msgId}`;

    if (media.document) {
      mimeType = media.document.mimeType || "video/mp4";
      fileSize = media.document.size ? Number(media.document.size) : 0;
      if (media.document.attributes) {
        for (const attr of media.document.attributes) {
          if (attr.fileName) fileName = attr.fileName;
        }
      }
    }

    // PDF or Document Handling
    if (mimeType.includes("pdf") || mimeType.includes("document")) {
      res.setHeader("Content-Type", mimeType);
      res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
      const buffer = await client.downloadMedia(message);
      return res.send(buffer);
    }

    // Video Streaming Range Handling
    const range = req.headers.range;
    let start = 0;
    let end = fileSize ? fileSize - 1 : 0;

    if (range) {
      const parts = range.replace(/bytes=/, "").split("-");
      start = parseInt(parts[0], 10);
      end = parts[1] ? parseInt(parts[1], 10) : end;
    }

    const chunkSize = end - start + 1;

    res.writeHead(range ? 206 : 200, {
      "Content-Range": fileSize ? `bytes ${start}-${end}/${fileSize}` : undefined,
      "Accept-Ranges": "bytes",
      "Content-Length": chunkSize,
      "Content-Type": mimeType,
      "Content-Disposition": `inline; filename="${fileName}"`,
    });

    const stream = client.iterDownload({
      file: media,
      offset: BigInt(start),
      requestSize: 512 * 1024,
    });

    for await (const chunk of stream) {
      res.write(chunk);
    }
    res.end();
  } catch (err) {
    console.error("❌ Stream Route Error:", err);
    if (!res.headersSent) {
      res.status(500).send("Streaming Error: " + err.message);
    }
  }
});

// -------------------------------------------------------------
// 2. FIREBASE PUSH LOGIC (ACCORDING TO CHATGPT RULES)
// -------------------------------------------------------------
async function processReplyAndPushToFirebase(replyText, mediaInfo) {
  if (!replyText) return;

  console.log(`📩 ChatGPT Response Received: "${replyText}"`);

  const replyClean = replyText.trim().toLowerCase();
  const ignoreList = ["सोच...", "thinking...", "please wait...", "generating..."];

  if (ignoreList.some((ig) => replyClean.includes(ig))) {
    console.log("⏳ AI जवाब तैयार कर रहा है...");
    return;
  }

  // Content Type Check (@dpp, @notes, or @other)
  let contentType = "@other";
  if (replyClean.includes("@dpp")) contentType = "@dpp";
  else if (replyClean.includes("@notes")) contentType = "@notes";

  // Lecture Tag Extraction (@Lec XX)
  const lecMatch = replyText.match(/@Lec\s*\d+/i);
  const lecTag = lecMatch ? lecMatch[0] : "";

  // Extract all tags including Unicode/Hindi
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

  // Firebase Keys Cleanup (Firebase disallows ., $, #, [, ], /)
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
  console.log(`🚀 Firebase Push Target: ${subjectKey} ➔ ${chapterKey}`);

  try {
    const res = await axios.post(firebaseUrl, dataPayload);
    if (res.status === 200 || res.status === 201) {
      console.log(`🔥 SUCCESS: Firebase में डेटा सेव हुआ! Path: ${subjectKey} ➔ ${chapterKey}`);
    } else {
      console.error(`❌ Firebase Error Status: ${res.status}`);
    }
  } catch (err) {
    console.error(`❌ Firebase Exception:`, err.response ? err.response.data : err.message);
  }
}
// -------------------------------------------------------------
// 3. MAIN RUNNER & TELEGRAM EVENT LISTENER
// -------------------------------------------------------------
async function startServer() {
  await client.connect();
  console.log("✅ Telegram Client Connected!");

  client.addEventHandler(async (event) => {
    try {
      const message = event.message;
      if (!message) return;

      const chat = await message.getChat();
      if (!chat) return;

      const chatUsername = (chat.username || "").toLowerCase();
      const chatTitle = (chat.title || "").toLowerCase();

      // 1. Check Channel Message (@sxhckfufig)
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
        const msgText = message.text || "Media File";
        
        await client.sendMessage(chatgptEntity, { message: msgText });
        console.log("➡️ [STEP 2] ChatGPT Bot ko query bhej di gayi!");
        return;
      }

      // 2. Check ChatGPT Reply (@chatgpt ya ChatGPT naam ka koi bot)
      const isChatGPT = 
        chatUsername.includes("chatgpt") || 
        chatTitle.includes("chatgpt");

      if (isChatGPT) {
        console.log(`\n🤖 [STEP 3] ChatGPT Response Aaya: "${message.text}"`);
        const mediaInfo = pendingMedia["latest"] || {};
        
        console.log("🚀 [STEP 4] Firebase Push Function Trigger Ho Raha Hai...");
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

