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
// CHATGPT TAGGING PIPELINE (enrichment only) - stream_link/thumb_link
// already push turant ho jaate hain (neeche pushDirectToFirebase se).
// Ye pipeline sirf ADDITIONAL tags (subject/chapter/content_type/lecture_no)
// nikaal ke SAME /Uploads/{msgId} entry mein PATCH karta hai - koi naya
// entry kabhi nahi banata, isliye tags kabhi galat jagah nahi jaate.
// Matching FIFO order se hoti hai (jis order mein ChatGPT ko bheja gaya,
// usi order mein uske jawab wapas aate hain).
// -------------------------------------------------------------
const sendQueue = [];
const pendingReplyQueue = []; // { msgId }
let isSending = false;

function enqueueForTagging(msgId, text) {
  sendQueue.push({ msgId, text });
  console.log(`📥 [TAG-QUEUE] Add hua msgId=${msgId} | bhejne ko baaki: ${sendQueue.length}`);
  if (!isSending) {
    processSendQueue().catch((e) => {
      console.error("❌ [TAG-QUEUE] processSendQueue error:", e);
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
      pendingReplyQueue.push({ msgId: item.msgId });
      console.log(`📨 [TAG-QUEUE] ChatGPT ko bheja gaya msgId=${item.msgId} | pending replies: ${pendingReplyQueue.length}`);
    } catch (e) {
      console.error("❌ [TAG-QUEUE] Send error:", e.message);
    }
    await sleep(1200); // Telegram flood-limit se bachne ke liye halka gap
  }

  isSending = false;
}

async function patchAiTagsToFirebase(msgId, replyText) {
  const segments = replyText.split("@").map((s) => s.trim()).filter(Boolean);
  let contentType = "@other", lecTag = "", subjectName = "General", chapterName = "General_Lectures";

  for (const seg of segments) {
    const sLower = seg.toLowerCase();
    if (sLower.startsWith("dpp")) contentType = "@dpp";
    else if (sLower.startsWith("notes")) contentType = "@notes";
    else if (sLower.startsWith("other")) contentType = "@other";
    else if (sLower.startsWith("lec")) lecTag = "@" + seg;
    else if (/[\u0900-\u097F]/.test(seg)) chapterName = seg;
    else subjectName = seg;
  }
  subjectName = subjectName.trim();
  chapterName = chapterName.trim();

  // HTML dashboard hamesha /{Subject}/{Chapter}/{msgId} nested path se
  // padhta hai (subject match 'phys'/'chem'/'bio' se, phir usi subject ke
  // andar chapter-naam ke node ke andar saari entries - videos + notes +
  // dpp sab EK hi chapter-object ke andar, content_type field se pehchane
  // jaate hain). Isliye final entry seedha usi nested path pe likhni hai -
  // /Uploads/{msgId} pe flat likhna hi "chapters bikhar jaane" wala bug tha,
  // kyunki HTML us path ko kabhi padhta hi nahi.
  const lecNum = lecTag.replace(/^@?Lec\s*/i, "").trim();
  const displayTitle = lecNum ? `${chapterName} — Lecture ${lecNum}` : chapterName;

  // Thumbnail alag se, background mein (screenshot-bot pipeline se) taiyar
  // ho rahi hoti hai - tags se pehle ya baad mein, kabhi bhi aa sakti hai.
  // Yahan wait kar lete hain taaki final (nested) entry mein thumb_link
  // kabhi chhoote na - warna Pending record delete hone ke baad thumb_link
  // hamesha ke liye kho jaata.
  let thumbUrl = null;
  if (thumbPromises.has(msgId)) {
    thumbUrl = await thumbPromises.get(msgId);
  }

  const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${msgId}.json`;
  let staged = {};
  try {
    const res = await axios.get(pendingUrl);
    staged = res.data || {};
  } catch (e) {
    console.error("❌ [FIREBASE] Pending entry read error:", e.response?.data || e.message);
  }

  const finalPayload = {
    msg_id: msgId,
    stream_link: staged.stream_link || `${RENDER_URL}/stream/${msgId}`,
    thumb_link: thumbUrl || staged.thumb_link || "",
    timestamp: staged.timestamp || { ".sv": "timestamp" },
    subject: subjectName,
    chapter: chapterName,
    content_type: contentType,
    lecture_no: lecTag,
    display_title: displayTitle,
    raw_reply: replyText,
  };

  const finalUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/${encodeURIComponent(subjectName)}/${encodeURIComponent(chapterName)}/${msgId}.json`;

  try {
    await axios.put(finalUrl, finalPayload);
    console.log(`🏷️ [FIREBASE] Final entry likh diya: ${subjectName} > ${chapterName} > ${contentType} (msgId=${msgId})`);
    axios.delete(pendingUrl).catch(() => {}); // ab staging entry ki zaroorat nahi
  } catch (e) {
    console.error("❌ [FIREBASE] Final write error:", e.response?.data || e.message);
  }
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
// FIREBASE STAGING PUSH - jab tak ChatGPT se subject/chapter tags nahi aa
// jaate, humein pata hi nahi hota entry final nested path
// (/{Subject}/{Chapter}/{msgId}) pe kahan jaayegi. Isliye stream_link
// (aur baad mein thumb_link) turant sirf ek temporary /Pending/{msgId}
// record mein save hote hain. Jaise hi tags aate hain, patchAiTagsToFirebase
// isi Pending record ko padh kar sahi chapter ke andar FINAL likh deta hai
// aur Pending record delete kar deta hai - isliye ab koi bhi entry kabhi
// root mein akeli/bikhri hui flat nahi padi rahti.
// -------------------------------------------------------------
async function pushDirectToFirebase(msgId, streamLink) {
  const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${msgId}.json`;

  const dataPayload = {
    msg_id: msgId,
    stream_link: streamLink,
    timestamp: { ".sv": "timestamp" },
  };

  try {
    await axios.put(pendingUrl, dataPayload); // PUT - msgId hi key hai, taaki thumb_link baad mein isi path pe PATCH ho sake
    console.log(`🔥 [FIREBASE] stream_link Pending mein push ho gaya (msgId=${msgId})`);
  } catch (e) {
    console.error("❌ [FIREBASE] stream_link push error:", e.response?.data || e.message);
    return;
  }

  // Thumbnail background mein already ban rahi hai (startThumbUpload se) -
  // jab wo ready ho jaaye, Pending record mein thumb_link add (PATCH) kar do,
  // taaki patchAiTagsToFirebase ko final entry banate waqt mil jaaye.
  if (thumbPromises.has(msgId)) {
    console.log(`⏳ [FIREBASE] msgId=${msgId} ke thumbnail ka wait ho raha hai...`);
    const thumbUrl = await thumbPromises.get(msgId);
    if (thumbUrl) {
      try {
        await axios.patch(pendingUrl, { thumb_link: thumbUrl });
        console.log(`✅ [FIREBASE] thumb_link Pending mein add ho gaya (msgId=${msgId}): ${thumbUrl}`);
      } catch (e) {
        console.error("❌ [FIREBASE] thumb_link patch error:", e.response?.data || e.message);
      }
    } else {
      console.log(`⚠️ [FIREBASE] msgId=${msgId} ke liye thumbnail nahi mil paayi, stream_link phir bhi Pending mein hai.`);
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

    // 1. Source Channel Message - stream_link turant push hota hai, aur
    // saath hi ChatGPT ko tagging ke liye bhi bhej dete hain (enrichment).
    if (sourceEntity && (chatIdStr.includes(sourceChatIdStr) || message.chatId?.toString() === sourceChatIdStr)) {
      console.log(`⚡ Channel se new message आया ID=${message.id}`);
      const streamLink = `${RENDER_URL}/stream/${message.id}`;
      startThumbUpload(message.id, streamLink);
      pushDirectToFirebase(message.id, streamLink); // fire-and-forget - andar khud thumbnail ka wait karke PATCH karega
      enqueueForTagging(message.id, message.message || message.text || "Media File"); // fire-and-forget - jawab aane par isi msgId mein tags PATCH honge
      return;
    }

    // 2. ChatGPT Bot Reply - sirf TAGS nikaal ke same /Uploads/{msgId}
    // entry mein PATCH karta hai, koi naya entry nahi banata.
    const isFromChatGPT = (chatgptBotIdStr && senderIdSync === chatgptBotIdStr) ||
                          (chatgptBotIdStr && chatIdStr.includes(chatgptBotIdStr));

    if (isFromChatGPT) {
      // IMPORTANT: message.message (raw field) use ho raha hai, message.text
      // (computed getter) NAHI - raw edit-update se aaye message object pe
      // .text kabhi khaali/galat aa jaata tha, jisse asli tagged reply bhi
      // "non-tagged" dikhta tha aur Firebase mein kabhi PATCH hi nahi hota tha.
      const replyText = message.message || message.text || "";
      console.log(`🔍 [DEBUG] ChatGPT se mila raw text: "${replyText}"`);
      const replyClean = replyText.toLowerCase();
      const isThinking = ["सोच...", "thinking..."].some((t) => replyClean.includes(t));

      if (isThinking) {
        console.log(`⏳ AI abhi soch raha hai ("${replyText}") - pending queue ko chhedte nahi, edit ka wait.`);
        return;
      }

      if (!replyText.includes("@")) {
        console.log("ℹ️ Non-tagged reply aayi - koi pending item consume nahi karenge.");
        return;
      }

      const matched = pendingReplyQueue.shift();
      if (matched) {
        console.log(`🏷️ ChatGPT tags match hue msgId=${matched.msgId} se. Parsing...`);
        await patchAiTagsToFirebase(matched.msgId, replyText);
      } else {
        console.log("⚠️ ChatGPT reply aayi lekin koi pending item match karne ke liye nahi mila.");
      }
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

    // ChatGPT bot pehle "सोच..." bhejta hai (NewMessage se pakda jaata hai),
    // fir USI message ko EDIT karke asli tags daalta hai - is raw update
    // handler ke bina wo asli jawab kabhi detect hi nahi hoga.
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
