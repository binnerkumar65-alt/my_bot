process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (err) => {
  console.error('❌ Uncaught Exception:', err);
});

const { TelegramClient, Api } = require("telegram");
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
const CHATGPT_BOT = "@chatgpt";
const SCREENSHOT_BOT = "@screenshotit_bot";
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

let chatgptBotId = null;
let chatgptEntity = null;
let sourceChatId = null;
let sourceEntity = null;
let screenshotBotId = null;
let screenshotEntity = null;

const messageCache = new Map();
const thumbPromises = new Map();

// Helper Sleep Function
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// -------------------------------------------------------------
// 🧹 AGGRESSIVE MEMORY & TEMP FILE CLEANUP
// -------------------------------------------------------------
function clearMemory() {
  try {
    messageCache.clear();
    if (global.gc) {
      global.gc();
      console.log("🧹 [RAM-CLEANUP] Memory/Heap saaf kar di gayi hai.");
    }
  } catch (e) {
    console.error("❌ RAM Cleanup Error:", e.message);
  }
}

// Har 2 minute mein RAM saaf hoga
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
          if (!statErr) {
            // Delete files immediately if older than 10 seconds
            if (now - stats.mtimeMs > 10 * 1000) {
              fs.unlink(p, () => {});
            }
          }
        });
      });
  });
}
setInterval(cleanupTempFiles, 15 * 1000);

// -------------------------------------------------------------
// SEQUENTIAL QUEUE
// -------------------------------------------------------------
const messageQueue = [];
let isProcessingQueue = false;
let currentMediaInfo = null;
let resolveCurrentReply = null;

function enqueueSourceMessage(item) {
  messageQueue.push(item);
  console.log(`📥 [QUEUE] Add hua ID=${item.msgId} | queue length: ${messageQueue.length}`);
  if (!isProcessingQueue) {
    processQueue().catch((e) => {
      console.error("❌ [QUEUE] processQueue crash:", e);
      isProcessingQueue = false;
    });
  }
}

function waitForChatGPTReply(timeoutMs) {
  return new Promise((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        console.log("⏱️ [QUEUE] ChatGPT Timeout - proceeding to next item.");
        resolve();
      }
    }, timeoutMs);

    resolveCurrentReply = () => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        resolve();
      }
    };
  });
}

async function processQueue() {
  if (isProcessingQueue) return;
  isProcessingQueue = true;

  while (messageQueue.length > 0) {
    const item = messageQueue.shift();
    currentMediaInfo = { stream_link: item.streamLink, msg_id: item.msgId };

    console.log(`\n➡️ [QUEUE] Processing ID=${item.msgId}`);

    try {
      if (!chatgptEntity) chatgptEntity = await client.getEntity(CHATGPT_BOT);
      await client.sendMessage(chatgptEntity, { message: item.text });
      console.log("📨 [QUEUE] ChatGPT ko bhej diya, response ka wait kar rahe hain...");

      await waitForChatGPTReply(180000);
    } catch (e) {
      console.error("❌ [QUEUE] Error:", e.message);
    }

    resolveCurrentReply = null;
    currentMediaInfo = null;
    clearMemory();
  }

  isProcessingQueue = false;
}

// -------------------------------------------------------------
// SCREENSHOT BOT PIPELINE (WITH 10s DELAY & AUTO TRIGGER)
// -------------------------------------------------------------
const screenshotBotWaiters = [];

function waitForScreenshotBotMessage(timeoutMs, predicate) {
  return new Promise((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) { settled = true; resolve(null); }
    }, timeoutMs);

    screenshotBotWaiters.push({
      predicate,
      resolve: (msg) => {
        if (!settled) { settled = true; clearTimeout(timer); resolve(msg); }
      },
    });
  });
}

function handleScreenshotBotMessage(message) {
  for (let i = 0; i < screenshotBotWaiters.length; i++) {
    if (screenshotBotWaiters[i].predicate(message)) {
      const waiter = screenshotBotWaiters.splice(i, 1)[0];
      waiter.resolve(message);
      return;
    }
  }
}

async function triggerGetThumbs(msg) {
  let success = false;
  try {
    if (msg) {
      await msg.click({ text: "Get Thumbs" });
      console.log(`🖱️ [THUMB-BOT] msg.click("Get Thumbs") try kiya.`);
      success = true;
    }
  } catch (e) {}

  try {
    await client.sendMessage(screenshotEntity, { message: "Get Thumbs" });
    console.log(`💬 [THUMB-BOT] Direct 'Get Thumbs' text message bhej diya.`);
    success = true;
  } catch (e) {}

  return success;
}

async function getThumbViaScreenshotBot(message) {
  if (!screenshotEntity || !sourceEntity) return null;

  try {
    // 1. Forward Video to Screenshot Bot
    await client.forwardMessages(screenshotEntity, {
      messages: [message.id],
      fromPeer: sourceEntity,
    });
    console.log(`📤 [THUMB-BOT] msgId=${message.id} Screenshot Bot ko forward kar diya.`);

    // 2. Exact 10 Seconds ka delay (aapke kahe anusar)
    console.log(`⏳ [THUMB-BOT] Video forward ho gaya. Exact 10 seconds wait kar rahe hain...`);
    await sleep(10000);

    // 3. Catch menu or Trigger Direct
    const menuMsg = await waitForScreenshotBotMessage(3000, (m) => !!m.replyMarkup);
    console.log(`🚀 [THUMB-BOT] 10 second pure hue! Ab 'Get Thumbs' trigger kar rahe hain...`);
    await triggerGetThumbs(menuMsg);

    // 4. Wait for Photo Reply (Up to 180s)
    const photoMsg = await waitForScreenshotBotMessage(180000, (m) => {
      return !!(m.photo || (m.media && m.media.className === 'MessageMediaPhoto'));
    });

    if (!photoMsg) {
      console.error(`❌ [THUMB-BOT] msgId=${message.id}: Timeout (Photo nahi mili).`);
      return null;
    }

    // 5. Download Photo Buffer
    const buffer = await client.downloadMedia(photoMsg);
    if (buffer && buffer.length) {
      console.log(`✅ [THUMB-BOT] Thumbnail image successfully mil gayi!`);
      return buffer;
    }
    return null;
  } catch (e) {
    console.error(`❌ [THUMB-BOT] Pipeline Error:`, e.message);
    return null;
  }
}

// -------------------------------------------------------------
// ARCHIVE UPLOAD & THUMB QUEUE
// -------------------------------------------------------------
async function uploadToArchive(buffer, idPrefix) {
  if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) return null;
  const identifier = `${idPrefix}-${Date.now()}-${crypto.randomBytes(3).toString("hex")}`.toLowerCase();
  const filename = "thumb.jpg";
  const uploadUrl = `https://s3.us.archive.org/${identifier}/${filename}`;

  try {
    await axios.put(uploadUrl, buffer, {
      headers: {
        "Content-Type": "image/jpeg",
        "Authorization": `LOW ${ARCHIVE_ACCESS_KEY}:${ARCHIVE_SECRET_KEY}`,
        "x-archive-auto-make-bucket": "1",
        "x-archive-meta-mediatype": "image",
      },
      timeout: 30000,
    });
    return `https://archive.org/download/${identifier}/${filename}`;
  } catch (e) {
    return null;
  }
}

function startThumbUpload(message) {
  const msgId = message.id;
  let resolvePromise;
  const promise = new Promise((r) => { resolvePromise = r; });
  thumbPromises.set(msgId, promise);

  (async () => {
    let result = null;
    try {
      const buffer = await getThumbViaScreenshotBot(message);
      if (buffer) {
        result = await uploadToArchive(buffer, `labdesk-thumb-${msgId}`);
      }
    } catch (e) {
      console.error("❌ Thumb Upload Exception:", e.message);
    } finally {
      resolvePromise(result);
      setTimeout(() => thumbPromises.delete(msgId), 30000);
      clearMemory();
    }
  })();
}

// -------------------------------------------------------------
// EXPRESS ROUTE
// -------------------------------------------------------------
app.get("/", (req, res) => res.send("Bot Active - Memory Optimized & 10s Trigger Set"));

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
// FIREBASE PUSH
// -------------------------------------------------------------
async function processReplyAndPushToFirebase(replyText, mediaInfo) {
  if (!replyText) return false;
  if (["सोच...", "thinking..."].some((ig) => replyText.toLowerCase().includes(ig))) return false;

  const segments = replyText.split("@").map((s) => s.trim()).filter(Boolean);
  let contentType = "@other", lecTag = "", subjectName = "General", chapterName = "General_Lectures";

  for (const seg of segments) {
    const sLower = seg.toLowerCase();
    if (sLower.startsWith("dpp")) contentType = "@dpp";
    else if (sLower.startsWith("notes")) contentType = "@notes";
    else if (sLower.startsWith("lec")) lecTag = "@" + seg;
    else if (/[\u0900-\u097F]/.test(seg)) chapterName = seg;
    else subjectName = seg;
  }

  const subjectKey = subjectName.replace(/[.$#\[\]/]/g, "_");
  const chapterKey = chapterName.replace(/[.$#\[\]/]/g, "_");

  const dataPayload = {
    content_type: contentType,
    lecture_no: lecTag,
    raw_reply: replyText,
    display_title: chapterName,
    timestamp: { ".sv": "timestamp" },
  };

  if (mediaInfo && mediaInfo.stream_link) {
    dataPayload["stream_link"] = mediaInfo.stream_link;
    if (mediaInfo.msg_id && thumbPromises.has(mediaInfo.msg_id)) {
      const thumbUrl = await Promise.race([
        thumbPromises.get(mediaInfo.msg_id),
        new Promise((r) => setTimeout(() => r(null), 180000)),
      ]);
      if (thumbUrl) dataPayload["thumb_link"] = thumbUrl;
    }
  }

  try {
    await axios.post(`${FIREBASE_BASE_URL}/${subjectKey}/${chapterKey}.json`, dataPayload);
    console.log(`🔥 Firebase mein successfully push ho gaya!`);
    return true;
  } catch (e) {
    return true;
  }
}

// -------------------------------------------------------------
// EVENT HANDLER
// -------------------------------------------------------------
async function handleIncomingMessage(event) {
  try {
    const message = event.message;
    if (!message) return;

    const chatIdStr = message.chatId ? message.chatId.toString() : "";
    const senderIdSync = message.senderId ? message.senderId.toString() : "";

    if (sourceChatId && chatIdStr === sourceChatId) {
      let streamLink = `${RENDER_URL}/stream/${message.id}`;
      startThumbUpload(message);
      enqueueSourceMessage({ msgId: message.id, streamLink, text: message.text || "Media File" });
      return;
    }

    if (chatgptBotId && senderIdSync === chatgptBotId) {
      const wasFinal = await processReplyAndPushToFirebase(message.text || "", currentMediaInfo || {});
      if (wasFinal && resolveCurrentReply) resolveCurrentReply();
    }

    if (screenshotBotId && senderIdSync === screenshotBotId) {
      handleScreenshotBotMessage(message);
    }
  } catch (e) {
    console.error("❌ Event Error:", e.message);
  }
}

// -------------------------------------------------------------
// SERVER INIT (PURANI FILES SAFF KARNE KE SAATH)
// -------------------------------------------------------------
async function startServer() {
  app.listen(PORT, '0.0.0.0', () => console.log(`🚀 Server on port ${PORT}`));

  // 🧹 Server shuru hote hi RAM aur temp files saaf karna
  console.log("🧹 [INIT] Purani sabhi temp files aur memory ko fully saaf kiya ja raha hai...");
  clearMemory();
  cleanupTempFiles();

  try {
    await client.connect();
    chatgptEntity = await client.getEntity(CHATGPT_BOT);
    chatgptBotId = chatgptEntity.id.toString();

    sourceEntity = await client.getEntity(SOURCE_CHAT);
    sourceChatId = sourceEntity.id.toString();

    screenshotEntity = await client.getEntity(SCREENSHOT_BOT);
    screenshotBotId = screenshotEntity.id.toString();

    client.addEventHandler(handleIncomingMessage, new NewMessage({}));
    console.log("🤖 Client Ready! Full Cleanup + 10s Delay Click Enabled.");
  } catch (e) {
    console.error("❌ Init Error:", e.message);
  }
}

startServer();
