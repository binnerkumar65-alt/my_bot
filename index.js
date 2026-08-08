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
const CHATGPT_BOT = "@chatgpt";
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

let chatgptBotIdStr = null;
let chatgptEntity = null;
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
// PIPELINE QUEUE (no waiting) - har forward turant ChatGPT ko chala jaata
// hai, agle ka wait kiye bina. Har item ki apni pehchan (msgId) "pending
// replies" list mein FIFO order mein lag jaati hai - jab bhi ChatGPT ka
// asli (tagged) jawab aata hai, wo hamesha sabse pehle wale pending item
// se match hota hai, taaki data hamesha sahi jagah (sahi lecture/notes/dpp
// entry) mein jaaye, chahe kitne bhi messages ek saath bheje gaye ho.
// -------------------------------------------------------------
const sendQueue = [];           // abhi tak ChatGPT ko bheje nahi gaye
const pendingReplyQueue = [];   // bhej diye gaye, reply ka wait hai (FIFO)
let isSending = false;

function enqueueSourceMessage(item) {
  sendQueue.push(item);
  console.log(`📥 [QUEUE] Add hua ID=${item.msgId} | bhejne ko baaki: ${sendQueue.length}`);
  if (!isSending) {
    processSendQueue().catch((e) => {
      console.error("❌ [QUEUE] processSendQueue error:", e);
      isSending = false;
    });
  }
}

async function processSendQueue() {
  if (isSending) return;
  isSending = true;

  while (sendQueue.length > 0) {
    const item = sendQueue.shift();
    try {
      if (!chatgptEntity) chatgptEntity = await client.getEntity(CHATGPT_BOT);
      await client.sendMessage(chatgptEntity, { message: item.text });

      // Reply ka wait kiye BINA turant agle item pe badh jaayenge - ye
      // item apni ID (msgId) ke saath pending list ke aakhir mein lag
      // jaata hai, jawab aane par yahi se uthaya jayega (FIFO).
      pendingReplyQueue.push({ msgId: item.msgId, streamLink: item.streamLink });
      console.log(`📨 [QUEUE] ChatGPT ko bhej diya ID=${item.msgId} | pending replies: ${pendingReplyQueue.length}`);
    } catch (e) {
      console.error("❌ [QUEUE] ChatGPT Send Error:", e.message);
    }

    await sleep(1200); // Telegram flood-limit se bachne ke liye halka sa gap
  }

  isSending = false;
}

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
// FIREBASE PUSH
// -------------------------------------------------------------
async function processReplyAndPushToFirebase(replyText, mediaInfo) {
  if (!replyText) return false;
  if (["सोच...", "thinking..."].some((ig) => replyText.toLowerCase().includes(ig))) {
    console.log(`⏳ [FIREBASE] AI abhi soch raha hai ("${replyText}") - skip kar rahe hain, edit ka wait...`);
    return false;
  }

  console.log(`📝 [FIREBASE] ChatGPT Reply Received: "${replyText.substring(0, 50)}..."`);

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

  const subjectKey = subjectName.trim().replace(/[.$#\[\]/]/g, "_");
  const chapterKey = chapterName.trim().replace(/[.$#\[\]/]/g, "_");

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
      console.log(`⏳ [FIREBASE] msgId=${mediaInfo.msg_id} ke Archive Link ka wait ho raha hai...`);
      
      const thumbUrl = await Promise.race([
        thumbPromises.get(mediaInfo.msg_id),
        new Promise((r) => setTimeout(() => r(null), 90000))
      ]);
      
      if (thumbUrl) {
        dataPayload["thumb_link"] = thumbUrl;
        console.log(`✅ [FIREBASE] thumb_link add ho gaya: ${thumbUrl}`);
      } else {
        console.log(`⚠️ [FIREBASE] msgId=${mediaInfo.msg_id} ka thumb_link nahi mila, direct data push kar rahe hain.`);
      }
    }
  }

  const pushUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/${encodeURIComponent(subjectKey)}/${encodeURIComponent(chapterKey)}.json`;
  console.log(`🚀 [FIREBASE] Pushing to URL: ${pushUrl}`);

  try {
    const res = await axios.post(pushUrl, dataPayload);
    console.log(`🔥 [FIREBASE SUCCESS] Data saved successfully! Key: ${res.data?.name}`);
    return true;
  } catch (e) {
    console.error("❌ [FIREBASE ERROR DETAILS]:", e.response?.status, e.response?.data || e.message);
    return true;
  }
}

// -------------------------------------------------------------
// EVENT HANDLER (FIXED CHATGPT MATCHING)
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

    // 1. Source Channel Message
    if (sourceEntity && (chatIdStr.includes(sourceChatIdStr) || message.chatId?.toString() === sourceChatIdStr)) {
      console.log(`⚡ Channel se new message आया ID=${message.id}`);
      let streamLink = `${RENDER_URL}/stream/${message.id}`;
      startThumbUpload(message.id, streamLink);
      enqueueSourceMessage({ msgId: message.id, streamLink, text: message.text || "Media File" });
      return;
    }

    // 2. ChatGPT Bot Reply (Strict Match via ID & ChatId)
    const isFromChatGPT = (chatgptBotIdStr && senderIdSync === chatgptBotIdStr) || 
                          (chatgptBotIdStr && chatIdStr.includes(chatgptBotIdStr));

    if (isFromChatGPT) {
      const replyText = message.text || "";
      const replyClean = replyText.toLowerCase();
      const isThinking = ["सोच...", "thinking..."].some((t) => replyClean.includes(t));

      if (isThinking) {
        console.log(`⏳ AI abhi soch raha hai ("${replyText}") - pending queue ko chhedte nahi, wait continue.`);
        return;
      }

      if (!replyText.includes("@")) {
        console.log("ℹ️ Non-tagged reply aayi - koi pending item consume nahi karenge.");
        return;
      }

      // Sabse pehle bheja gaya item hi is jawab ka sahi match hai (FIFO) -
      // isliye links/tags kabhi mix nahi hote, chahe kitne bhi messages
      // ek saath ya lagataar bina wait kiye bheje ho.
      const matched = pendingReplyQueue.shift();
      console.log(`🤖 ChatGPT ka message detected! Match hua ID=${matched ? matched.msgId : "unknown"} se. Parsing reply...`);

      const mediaInfo = matched ? { stream_link: matched.streamLink, msg_id: matched.msgId } : {};
      await processReplyAndPushToFirebase(replyText, mediaInfo);
      return;
    }

    // 3. New Screenshot Bot Reply (@screenshort17_bot)
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

    chatgptEntity = await client.getEntity(CHATGPT_BOT);
    chatgptBotIdStr = chatgptEntity.id.toString();

    sourceEntity = await client.getEntity(SOURCE_CHAT);
    sourceChatIdStr = sourceEntity.id.toString();

    screenshotEntity = await client.getEntity(NEW_SCREENSHOT_BOT);
    screenshotBotIdStr = screenshotEntity.id.toString();

    console.log(`📌 Target IDs Loaded - ChatGPT: ${chatgptBotIdStr} | Source: ${sourceChatIdStr} | ScreenBot: ${screenshotBotIdStr}`);

    client.addEventHandler(handleIncomingMessage, new NewMessage({}));

    // YE HISSA CRITICAL HAI - ChatGPT bot pehle "सोच..." bhejta hai
    // (NewMessage se pakda jaata hai), fir USI message ko EDIT karke asli
    // tags/answer daalta hai. `EditedMessage` event class is gramjs version
    // mein reliably fire nahi ho raha tha, isliye raw update ko seedha check
    // kar rahe hain - ye approach pehle confirm working thi.
    client.addEventHandler(async (update) => {
      try {
        if (
          update.className === "UpdateEditMessage" ||
          update.className === "UpdateEditChannelMessage"
        ) {
          console.log("✏️ Raw Edit Update Detect Hua, process kar rahe hain...");
          await handleIncomingMessage({ message: update.message });
        }
      } catch (e) {
        console.error("❌ Raw Edit Handler Error:", e.message);
      }
    });

    console.log("🤖 Client Ready! Detection pipeline synchronized.");
  } catch (e) {
    console.error("❌ Init Error:", e.message);
  }
}

startServer();
