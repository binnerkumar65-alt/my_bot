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
const bigInt = require("big-integer");
const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");

const app = express();
const PORT = process.env.PORT || 10000;
app.use(express.json({ limit: "2mb" }));

// Environment Variables Configuration
const apiId = parseInt(process.env.API_ID || "0");
const apiHash = process.env.API_HASH || "";
const stringSession = new StringSession(process.env.SESSION_STRING || "");

const SOURCE_CHAT = "@sxhckfufig";
const NEW_SCREENSHOT_BOT = "@screenshort17_bot";
const FIREBASE_BASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com";
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || "https://my-bot-kgrk.onrender.com";

const ARCHIVE_ACCESS_KEY = process.env.ARCHIVE_ACCESS_KEY || "";
const ARCHIVE_SECRET_KEY = process.env.ARCHIVE_SECRET_KEY || "";

if (!apiId || !apiHash) {
  console.error("❌ API_ID या API_HASH missing हैं!");
  process.exit(1);
}

const client = new TelegramClient(stringSession, apiId, apiHash, {
  connectionRetries: 5,
});

let sourceChatIdStr = null;
let sourceEntity = null;
let screenshotBotIdStr = null;
let screenshotEntity = null;

const messageCache = new Map();
const thumbPromises = new Map();

// Helper Sleep
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// -------------------------------------------------------------
// 🧹 MEMORY & TEMP FILE CLEANUP
// -------------------------------------------------------------
function clearMemory() {
  try {
    messageCache.clear();
    if (global.gc) {
      global.gc();
      console.log("🧹 [RAM-CLEANUP] Memory saaf kar di gayi.");
    }
  } catch (e) {
    console.error("❌ RAM Cleanup Error:", e.message);
  }
}
setInterval(clearMemory, 2 * 60 * 1000);

const TEMP_PREFIXES = ["transcoded_", "transcodesrc_", "thumbsrc_", "thumbout_"];
function cleanupTempFiles() {
  fs.readdir(os.tmpdir(), (err, files) => {
    if (err) return;
    const now = Date.now();
    files
      .filter((f) => TEMP_PREFIXES.some((prefix) => f.startsWith(prefix)))
      .forEach((f) => {
        const p = path.join(os.tmpdir(), f);
        fs.stat(p, (statErr, stats) => {
          if (!statErr && (now - stats.mtimeMs > 10 * 1000)) {
            fs.unlink(p, () => {});
          }
        });
      });
  });
}
setInterval(cleanupTempFiles, 15 * 1000);

// -------------------------------------------------------------
// SCREENSHOT BOT PIPELINE
// -------------------------------------------------------------
let latestScreenshotPhoto = null;

async function getThumbViaScreenshotBot(streamLink) {
  if (!screenshotEntity) return null;

  try {
    latestScreenshotPhoto = null;

    await client.sendMessage(screenshotEntity, { message: streamLink });
    console.log(`📤 [THUMB-BOT] @screenshort17_bot ko URL bhej diya: ${streamLink}`);

    let waitCount = 0;
    while (!latestScreenshotPhoto && waitCount < 90) {
      await sleep(1000);
      waitCount++;
    }

    if (!latestScreenshotPhoto) {
      console.error(`⚠️ [THUMB-BOT] Screenshot bot se photo nahi mili (Timeout).`);
      return null;
    }

    console.log(`📸 [THUMB-BOT] Photo mil gayi! Downloading...`);
    const buffer = await client.downloadMedia(latestScreenshotPhoto);
    latestScreenshotPhoto = null;

    if (buffer && buffer.length) {
      console.log(`✅ [THUMB-BOT] Thumbnail image download ho gayi!`);
      return buffer;
    }
    return null;
  } catch (e) {
    console.error(`❌ [THUMB-BOT] Pipeline Error:`, e.message);
    return null;
  }
}

// -------------------------------------------------------------
// ARCHIVE UPLOAD
// -------------------------------------------------------------
async function uploadToArchive(buffer, idPrefix) {
  if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) {
    console.error("❌ ARCHIVE KEYS missing!");
    return null;
  }

  const identifier = `${idPrefix}-${Date.now()}-${crypto.randomBytes(3).toString("hex")}`.toLowerCase();
  const filename = "thumb.jpg";
  const uploadUrl = `https://s3.us.archive.org/${identifier}/${filename}`;

  try {
    await axios.put(uploadUrl, buffer, {
      headers: {
        "Content-Type": "image/jpeg",
        "Authorization": `LOW ${ARCHIVE_ACCESS_KEY.trim()}:${ARCHIVE_SECRET_KEY.trim()}`,
        "x-archive-auto-make-bucket": "1",
        "x-archive-meta-mediatype": "image",
        "x-archive-meta-title": `Thumbnail ${identifier}`,
      },
      timeout: 60000,
    });

    console.log(`✅ [ARCHIVE] Upload Success! Identifier: ${identifier}`);
    return `https://archive.org/download/${identifier}/${filename}`;
  } catch (e) {
    const errDetails = e.response?.data ? String(e.response.data) : e.message;
    console.error("❌ [ARCHIVE] Error:", errDetails);
    return null;
  }
}

function startThumbUpload(msgId, streamLink) {
  let resolvePromise;
  const promise = new Promise((r) => { resolvePromise = r; });
  thumbPromises.set(msgId, promise);

  (async () => {
    let result = null;
    try {
      console.log(`⏳ [ARCHIVE] Process start for msgId=${msgId}...`);
      const buffer = await getThumbViaScreenshotBot(streamLink);
      if (buffer) {
        result = await uploadToArchive(buffer, `labdesk-thumb-${msgId}`);
        console.log(`🔗 [ARCHIVE] Direct Link: ${result}`);
      }
    } catch (e) {
      console.error("❌ Thumb Upload Error:", e.message);
    } finally {
      resolvePromise(result);
      setTimeout(() => thumbPromises.delete(msgId), 10 * 60 * 1000);
      clearMemory();
    }
  })();
}

// -------------------------------------------------------------
// EXPRESS ROUTE
// -------------------------------------------------------------
app.get("/", (req, res) => res.send("Bot Active - ChatGPT Detection Fixed"));

app.get("/stream/:msgId", async (req, res) => {
  try {
    const msgId = parseInt(req.params.msgId);
    if (!sourceEntity) sourceEntity = await client.getEntity(SOURCE_CHAT);

    const messages = await client.getMessages(sourceEntity, { ids: msgId });
    if (!messages || !messages[0] || !messages[0].media) return res.status(404).send("Not Found");

    const message = messages[0];
    const media = message.media;
    const fileSize = Number(media.document ? media.document.size : 0);
    const range = req.headers.range;

    if (range && fileSize) {
      const [startStr, endStr] = range.replace(/bytes=/, "").split("-");
      const start = parseInt(startStr, 10);
      const end = endStr ? parseInt(endStr, 10) : fileSize - 1;
      const chunkSize = end - start + 1;

      res.writeHead(206, {
        "Content-Range": `bytes ${start}-${end}/${fileSize}`,
        "Accept-Ranges": "bytes",
        "Content-Length": chunkSize,
        "Content-Type": "video/mp4",
      });

      const stream = client.iterDownload({ file: media, offset: bigInt(start), limit: chunkSize });
      for await (const chunk of stream) {
        if (!res.write(chunk)) await new Promise((r) => res.once("drain", r));
      }
      res.end();
    } else {
      res.writeHead(200, { "Content-Type": "video/mp4", "Content-Length": fileSize });
      const stream = client.iterDownload({ file: media, offset: bigInt(0) });
      for await (const chunk of stream) {
        if (!res.write(chunk)) await new Promise((r) => res.once("drain", r));
      }
      res.end();
    }
  } catch (e) {
    if (!res.headersSent) res.status(500).send("Streaming Error");
  } finally {
    clearMemory();
  }
});

// -------------------------------------------------------------
// FIREBASE PUSH - AB SEEDHA, AI wala tagging hata diya gaya hai. Sirf
// stream_link (video) aur thumb_link (archive.org se aayi thumbnail)
// push hote hain, kisi ChatGPT reply/tag ka koi matlab nahi raha.
// -------------------------------------------------------------
async function pushDirectToFirebase(msgId, streamLink) {
  const pushUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Uploads/${msgId}.json`;

  const dataPayload = {
    msg_id: msgId,
    stream_link: streamLink,
    timestamp: { ".sv": "timestamp" },
  };

  try {
    await axios.put(pushUrl, dataPayload); // PUT - msgId hi key hai, taaki thumb_link baad mein isi path pe PATCH ho sake
    console.log(`🔥 [FIREBASE] stream_link push ho gaya (msgId=${msgId})`);
  } catch (e) {
    console.error("❌ [FIREBASE] stream_link push error:", e.response?.data || e.message);
    return;
  }

  // Thumbnail background mein already ban rahi hai (startThumbUpload se) -
  // jab wo ready ho jaaye, usi entry mein thumb_link add (PATCH) kar do.
  if (thumbPromises.has(msgId)) {
    console.log(`⏳ [FIREBASE] msgId=${msgId} ke thumbnail ka wait ho raha hai...`);
    const thumbUrl = await thumbPromises.get(msgId);
    if (thumbUrl) {
      try {
        await axios.patch(pushUrl, { thumb_link: thumbUrl });
        console.log(`✅ [FIREBASE] thumb_link add ho gaya (msgId=${msgId}): ${thumbUrl}`);
      } catch (e) {
        console.error("❌ [FIREBASE] thumb_link patch error:", e.response?.data || e.message);
      }
    } else {
      console.log(`⚠️ [FIREBASE] msgId=${msgId} ke liye thumbnail nahi mil paayi, stream_link phir bhi save hai.`);
    }
  }
}

// -------------------------------------------------------------
// EVENT HANDLER
// -------------------------------------------------------------
async function handleIncomingMessage(event) {
  try {
    const message = event.message;
    if (!message) return;

    let chatIdStr = "";
    if (message.peerId) {
      if (message.peerId.channelId) chatIdStr = message.peerId.channelId.toString();
      else if (message.peerId.chatId) chatIdStr = message.peerId.chatId.toString();
      else if (message.peerId.userId) chatIdStr = message.peerId.userId.toString();
    }

    const senderIdSync = message.senderId ? message.senderId.toString() : "";

    // 1. Source Channel Message - ab seedha push, ChatGPT ko kuch bhejna
    // hi nahi hai.
    if (sourceEntity && (chatIdStr.includes(sourceChatIdStr) || message.chatId?.toString() === sourceChatIdStr)) {
      console.log(`⚡ Channel se new message आया ID=${message.id}`);
      const streamLink = `${RENDER_URL}/stream/${message.id}`;
      startThumbUpload(message.id, streamLink);
      pushDirectToFirebase(message.id, streamLink); // fire-and-forget - andar khud thumbnail ka wait karke PATCH karega
      return;
    }

    // 2. New Screenshot Bot Reply (@screenshort17_bot)
    const isFromScreenshotBot = (screenshotBotIdStr && senderIdSync === screenshotBotIdStr) || 
                                (screenshotBotIdStr && chatIdStr.includes(screenshotBotIdStr));

    if (isFromScreenshotBot) {
      if (message.photo || (message.media && message.media.className === 'MessageMediaPhoto')) {
        latestScreenshotPhoto = message;
      }
      return;
    }
  } catch (e) {
    console.error("❌ Event Error:", e.message);
  }
}

// -------------------------------------------------------------
// SERVER INIT
// -------------------------------------------------------------
async function startServer() {
  app.listen(PORT, '0.0.0.0', () => console.log(`🚀 Server on port ${PORT}`));

  console.log("🧹 [INIT] Memory aur temp files saaf kar rahe hain...");
  clearMemory();
  cleanupTempFiles();

  try {
    await client.connect();

    sourceEntity = await client.getEntity(SOURCE_CHAT);
    sourceChatIdStr = sourceEntity.id.toString();

    screenshotEntity = await client.getEntity(NEW_SCREENSHOT_BOT);
    screenshotBotIdStr = screenshotEntity.id.toString();

    console.log(`📌 Target IDs Loaded - Source: ${sourceChatIdStr} | ScreenBot: ${screenshotBotIdStr}`);

    client.addEventHandler(handleIncomingMessage, new NewMessage({}));

    console.log("🤖 Client Ready! Detection pipeline synchronized.");
  } catch (e) {
    console.error("❌ Init Error:", e.message);
  }
}

startServer();
