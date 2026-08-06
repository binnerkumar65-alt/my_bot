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

// Configuration Environment Variables
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

// Client definition
const client = new TelegramClient(stringSession, apiId, apiHash, {
  connectionRetries: 5,
});

let pendingMedia = {};

app.get("/", (req, res) => {
  res.send("Node.js Fast Streaming & Forwarder Bot is Active!");
});

// -------------------------------------------------------------
// STREAMING ROUTE
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

    if (mimeType.includes("pdf") || mimeType.includes("document")) {
      res.setHeader("Content-Type", mimeType);
      res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
      const buffer = await client.downloadMedia(message);
      return res.send(buffer);
    }

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
    console.error("Stream Route Error:", err);
    if (!res.headersSent) {
      res.status(500).send("Streaming Error: " + err.message);
    }
  }
});

// -------------------------------------------------------------
// HELPER: CHAPTER CLEANING
// -------------------------------------------------------------
function cleanChapterName(rawName) {
  if (!rawName) return "Uncategorized";
  let name = rawName.replace(/@/g, "").trim();
  name = name.replace(/\b(lec|lecture|part|dpp|notes|class)\b.*/gi, "");
  name = name.replace(/[\d\-_\:()\[\]]+$/g, "").trim();
  return name || "Uncategorized";
}

// -------------------------------------------------------------
// FIREBASE PUSH LOGIC
// -------------------------------------------------------------
async function processReplyAndPushToFirebase(replyText, mediaInfo) {
  if (!replyText) return;

  const replyClean = replyText.trim().toLowerCase();
  const ignoreList = ["सोच...", "thinking...", "please wait...", "generating..."];

  if (ignoreList.some((ig) => replyClean.includes(ig))) {
    console.log("⏳ AI जवाब तैयार कर रहा है...");
    return;
  }

  let contentType = "video";
  if (replyClean.includes("@notes")) contentType = "@notes";
  else if (replyClean.includes("@dpp")) contentType = "@dpp";
  else if (replyClean.includes("@other")) contentType = "@other";

  const lecMatch = replyText.match(/(@Lec\s*\d+|@L\d+|Lec\s*\d+)/i);
  const lecTag = lecMatch ? lecMatch[1] : "";

  const tags = replyText.match(/@[^\s@]+(?:\s+[^\s@]+)*/g) || [];
  let subjectName = "Biology";
  let rawChapterName = "";

  const validTags = [];
  for (const tag of tags) {
    const tClean = tag.trim();
    const tLower = tClean.toLowerCase();
    if (!["@notes", "@dpp", "@other"].includes(tLower) && !tLower.startsWith("@lec")) {
      validTags.push(tClean);
    }
  }

  if (validTags.length >= 2) {
    subjectName = validTags[0].replace(/@/g, "").trim();
    rawChapterName = validTags[1];
  } else if (validTags.length === 1) {
    rawChapterName = validTags[0];
  }

  const chapterName = cleanChapterName(rawChapterName);
  const subjectKey = subjectName.replace(/[.$#\[\]/]/g, "");
  const chapterKey = chapterName.replace(/[.$#\[\]/]/g, "");

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

  try {
    const res = await axios.post(firebaseUrl, dataPayload);
    if (res.status === 200) {
      console.log(`🔥 Firebase Push Success: ${subjectKey} ➔ ${chapterKey}`);
    } else {
      console.error(`❌ Firebase Error: ${res.status}`);
    }
  } catch (err) {
    console.error(`❌ Firebase Exception:`, err.message);
  }
}

// -------------------------------------------------------------
// MAIN RUNNER & TELEGRAM EVENT LISTENER
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

      // 1. Channel @sxhckfufig check
      const isSourceChat = 
        (chat.username && chat.username.toLowerCase() === "sxhckfufig") ||
        (chat.title && chat.title.toLowerCase().includes("sxhckfufig"));

      if (isSourceChat) {
        console.log(`[+] Naya Message Aaya (ID: ${message.id})`);
        
        let streamLink = "";
        if (message.media) {
          streamLink = `${RENDER_URL}/stream/${message.id}`;
          console.log(`🔗 Stream Link: ${streamLink}`);
        }

        pendingMedia["latest"] = { stream_link: streamLink, msg_id: message.id };

        const chatgptEntity = await client.getEntity(CHATGPT_BOT);
        const msgText = message.text || "Media File";
        
        await client.sendMessage(chatgptEntity, { message: msgText });
        console.log("➡️ ChatGPT bot ko query bheji!");
      }

      // 2. Reply from @chatgpt check
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
