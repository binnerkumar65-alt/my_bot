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
const { spawn } = require("child_process");

// 🆕 FFMPEG PATH — pehle ffmpeg-static (npm package, free tier pe bhi chalta hai) try karo,
// warna system PATH ka "ffmpeg" use karo (agar Render Build Command mein install kiya ho)
let ffmpegPath = "ffmpeg";
try {
  ffmpegPath = require("ffmpeg-static") || "ffmpeg";
  console.log(`🎬 [FFMPEG] Static binary mil gaya: ${ffmpegPath}`);
} catch (e) {
  console.log(`⚠️ [FFMPEG] ffmpeg-static package nahi mila, system "ffmpeg" try karenge. (npm install ffmpeg-static karo)`);
}

const app = express();
const PORT = process.env.PORT || 10000;
app.use(express.json({ limit: "2mb" }));

const apiId = parseInt(process.env.API_ID || "0");
const apiHash = process.env.API_HASH || "";
const stringSession = new StringSession(process.env.SESSION_STRING || "");

// 🆕 DONO CHANNELS SUPPORT
const SOURCE_CHATS = ["@sxhckfufig", "@YAKEEN_NEET_HINDI_2027_LEC"];
const CHATGPT_BOT = "@chatgpt";
const TYPE_CHECKER_BOT = "@P840bot";
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
let typeCheckerBotIdStr = null;
let typeCheckerEntity = null;
let screenshotBotIdStr = null;
let screenshotEntity = null;

// 🆕 Multiple source channels tracking
const sourceEntities = new Map(); // chatIdStr -> entity
const sourceChatIdStrs = new Set();

// 🆕 JUGAAD #2: msgId -> chatIdStr cache — stream request pe har baar dono channels
// mein loop-search nahi karna padega, seedha pata chal jayega video kaunse channel mein hai
const videoLocationCache = new Map(); // msgId -> chatIdStr

const messageCache = new Map();
const thumbPromises = new Map();
const finalizedDocPaths = new Map();
const pendingTimestamps = new Map();

// 🆕 THUMBNAIL FIFO QUEUE (pehle wala Map hata diya — ab queue se match hoga)
const screenshotRequestQueue = []; // FIFO: msgIds waiting for screenshot
const screenshotResults = new Map(); // msgId -> photoMessage

// 🆕 CORRELATION MAPS (replyTo se exact match ke liye)
const chatgptSentToOriginal = new Map(); // sentMsgId -> originalMsgId
const typeCheckerSentToOriginal = new Map(); // forwardedMsgId -> originalMsgId

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function clearMemory() {
  try {
    messageCache.clear();
    // Stale correlation entries clean karo
    if (chatgptSentToOriginal.size > 200) {
      const keys = Array.from(chatgptSentToOriginal.keys()).slice(0, chatgptSentToOriginal.size - 200);
      keys.forEach(k => chatgptSentToOriginal.delete(k));
    }
    if (typeCheckerSentToOriginal.size > 200) {
      const keys = Array.from(typeCheckerSentToOriginal.keys()).slice(0, typeCheckerSentToOriginal.size - 200);
      keys.forEach(k => typeCheckerSentToOriginal.delete(k));
    }
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

const RULE_REMINDER_TEXT = `नियम याद दिला रहा हूँ:
1. विषय टैग (जैसे: @Physics, @Chemistry, @Biology)
2. अध्याय टैग (जैसे: @समतल में गति)
3. लेक्चर नंबर (अगर टेक्स्ट में दिया हो) ➔ @Lec XX

ध्यान दें: Notes या DPP की टैगिंग की ज़रूरत नहीं है। सिर्फ Subject, Chapter और Lecture No. ही टैग करें।`;

let tagMsgCount = 0;

async function loadTagMsgCount() {
  try {
    const res = await axios.get(`${FIREBASE_BASE_URL.replace(/\/$/, "")}/Meta.json`);
    if (res.data && typeof res.data === "object" && typeof res.data.tagMsgCount === "number") {
      tagMsgCount = res.data.tagMsgCount;
    } else {
      tagMsgCount = 0;
    }
    console.log(`🔢 [RULE-REMINDER] Counter Firebase se load hua: ${tagMsgCount}`);
  } catch (e) {
    console.error("❌ [RULE-REMINDER] Counter load error:", e.message);
    tagMsgCount = 0;
  }
}

function saveTagMsgCount() {
  axios
    .patch(`${FIREBASE_BASE_URL.replace(/\/$/, "")}/Meta.json`, { tagMsgCount: tagMsgCount })
    .then(() => console.log(`🔢 [RULE-REMINDER] Counter Firebase mein save hua: ${tagMsgCount}`))
    .catch((e) => console.error("❌ [RULE-REMINDER] Counter save error:", e.response?.data || e.message));
}

const sendQueue = [];
const pendingReplyQueue = [];
const pendingTypeQueue = [];
let isSending = false;

function enqueueForTagging(msgId, text) {
  sendQueue.push({ msgId, text, isReminder: false });

  tagMsgCount++;
  console.log(`📥 [TAG-QUEUE] Add hua msgId=${msgId} | Counter: ${tagMsgCount} | bhejne ko baaki: ${sendQueue.length}`);

  if (tagMsgCount >= 10) {
    sendQueue.push({ msgId: null, text: RULE_REMINDER_TEXT, isReminder: true });
    console.log(`🔁 [RULE-REMINDER] 10 messages ho gaye - rules dobara ChatGPT ko bheje jaa rahe hain.`);
    tagMsgCount = 0;
  }
  saveTagMsgCount();

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
      if (item.isReminder) {
        await client.sendMessage(chatgptEntity, { message: item.text });
        console.log(`🔁 [RULE-REMINDER] Rules ChatGPT ko bhej diye.`);
      } else {
        const sent = await client.sendMessage(chatgptEntity, { message: item.text });
        const sentMsgId = sent.id.toString();
        chatgptSentToOriginal.set(sentMsgId, item.msgId);
        pendingReplyQueue.push({ msgId: item.msgId, sentMsgId: sentMsgId });
        console.log(`📨 [TAG-QUEUE] ChatGPT ko bheja gaya msgId=${item.msgId} (sentId=${sentMsgId}) | pending replies: ${pendingReplyQueue.length}`);
      }
    } catch (e) {
      console.error("❌ [TAG-QUEUE] Send error:", e.message);
    }
    await sleep(1200);
  }

  isSending = false;
}

async function forwardDocToTypeChecker(messageId, fromPeer) {
  try {
    if (!typeCheckerEntity) typeCheckerEntity = await client.getEntity(TYPE_CHECKER_BOT);

    const result = await client.forwardMessages(typeCheckerEntity, {
      messages: [messageId],
      fromPeer: fromPeer,
    });

    let forwardedId = null;
    if (result) {
      const fwdMsgs = Array.isArray(result) ? result : (result.messages || []);
      if (fwdMsgs.length > 0 && fwdMsgs[0].id) {
        forwardedId = fwdMsgs[0].id.toString();
      }
    }

    if (forwardedId) {
      typeCheckerSentToOriginal.set(forwardedId, messageId);
      pendingTypeQueue.push({ msgId: messageId, forwardedId: forwardedId });
      console.log(`⏩ [TYPE-CHECKER] Document msgId=${messageId} @P840bot ko forward kar diya (fwdId=${forwardedId}).`);
    } else {
      pendingTypeQueue.push({ msgId: messageId });
      console.log(`⏩ [TYPE-CHECKER] Document msgId=${messageId} @P840bot ko forward kar diya (no fwdId).`);
    }
  } catch (e) {
    console.error("❌ [TYPE-CHECKER] Forwarding Error:", e.message);
  }
}

async function patchAiTagsToFirebase(msgId, replyText) {
  const segments = replyText.split("@").map((s) => s.trim()).filter(Boolean);

  const subjectName = (segments[0] || "General").trim();
  const chapterName = (segments[1] || "General_Lectures").trim();
  const seg3 = (segments[2] || "").trim();

  let lecTag = "";
  if (/^lec/i.test(seg3)) lecTag = "@" + seg3;

  const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${msgId}.json`;
  let staged = {};
  try {
    const res = await axios.get(pendingUrl);
    staged = res.data || {};
  } catch (e) {
    console.error("❌ [FIREBASE] Pending entry read error:", e.response?.data || e.message);
  }

  const contentType = staged.content_type || "@other";

  // 🆕 THUMB KA WAIT NAHI KARENGE — agar pehle se pending mein hai to use karo, warna blank
  let thumbUrl = staged.thumb_link || "";

  const lecNum = lecTag.replace(/^@?Lec\s*/i, "").trim();
  const displayTitle = lecNum ? `${chapterName} — Lecture ${lecNum}` : chapterName;

  const finalPayload = {
    msg_id: msgId,
    stream_link: staged.stream_link || `${RENDER_URL}/stream/${msgId}`,
    download_link: (contentType === "@notes" || contentType === "@dpp")
      ? `${RENDER_URL}/download/${msgId}`
      : "",
    thumb_link: thumbUrl,
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

    finalizedDocPaths.set(msgId, { subject: subjectName, chapter: chapterName });
    setTimeout(() => finalizedDocPaths.delete(msgId), 30 * 60 * 1000);

    axios.delete(pendingUrl).catch(() => {});
    pendingTimestamps.delete(msgId);
  } catch (e) {
    console.error("❌ [FIREBASE] Final write error:", e.response?.data || e.message);
  }
}

async function getThumbViaScreenshotBot(msgId, streamLink) {
  if (!screenshotEntity) return null;

  try {
    screenshotRequestQueue.push(msgId);
    screenshotResults.delete(msgId); // clean stale

    await client.sendMessage(screenshotEntity, { message: streamLink });
    console.log(`📤 [THUMB-BOT] (msgId=${msgId}) @screenshort17_bot ko URL bhej diya: ${streamLink}`);

    let waitCount = 0;
    while (!screenshotResults.has(msgId) && waitCount < 90) {
      await sleep(1000);
      waitCount++;
    }

    const photoMsg = screenshotResults.get(msgId);
    screenshotResults.delete(msgId);
    // Remove from queue if still present (timeout case)
    const idx = screenshotRequestQueue.indexOf(msgId);
    if (idx !== -1) screenshotRequestQueue.splice(idx, 1);

    if (!photoMsg) {
      console.error(`⚠️ [THUMB-BOT] (msgId=${msgId}) Screenshot bot se photo nahi mili (Timeout).`);
      return null;
    }

    console.log(`📸 [THUMB-BOT] (msgId=${msgId}) Photo mil gayi! Downloading...`);
    const buffer = await client.downloadMedia(photoMsg);

    if (buffer && buffer.length) {
      console.log(`✅ [THUMB-BOT] (msgId=${msgId}) Thumbnail image download ho gayi!`);
      return buffer;
    }
    return null;
  } catch (e) {
    console.error(`❌ [THUMB-BOT] (msgId=${msgId}) Pipeline Error:`, e.message);
    const idx = screenshotRequestQueue.indexOf(msgId);
    if (idx !== -1) screenshotRequestQueue.splice(idx, 1);
    screenshotResults.delete(msgId);
    return null;
  }
}

async function uploadToArchive(buffer, idPrefix) {
  if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) {
    console.error("❌ ARCHIVE KEYS missing!");
    return null;
  }

  const identifier = `${idPrefix}-${Date.now()}-${crypto.randomBytes(4).toString("hex")}`.toLowerCase();
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

function getDocumentFileName(message) {
  const doc = message.media && message.media.document;
  if (!doc || !doc.attributes) return null;
  const nameAttr = doc.attributes.find((a) => a.className === "DocumentAttributeFilename");
  if (!nameAttr || !nameAttr.fileName) return null;
  return nameAttr.fileName.replace(/\.[a-zA-Z0-9]+$/, "").replace(/_/g, " ").trim();
}

function isVideoMessage(message) {
  const doc = message.media && message.media.document;
  if (!doc) return false;
  if (doc.mimeType && doc.mimeType.startsWith("video/")) return true;
  if (doc.attributes && doc.attributes.some((a) => a.className === "DocumentAttributeVideo")) return true;
  return false;
}

function startThumbUpload(msgId, streamLink) {
  let resolvePromise;
  const promise = new Promise((r) => { resolvePromise = r; });
  thumbPromises.set(msgId, promise);

  (async () => {
    let result = null;
    try {
      console.log(`⏳ [ARCHIVE] Process start for msgId=${msgId}...`);
      const buffer = await getThumbViaScreenshotBot(msgId, streamLink);
      if (buffer) {
        result = await uploadToArchive(buffer, `labdesk-thumb-${msgId}`);
        console.log(`🔗 [ARCHIVE] Direct Link for msgId=${msgId}: ${result}`);

        const finalPath = finalizedDocPaths.get(msgId);
        if (finalPath && result) {
          try {
            const finalPatchUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/${encodeURIComponent(finalPath.subject)}/${encodeURIComponent(finalPath.chapter)}/${msgId}.json`;
            await axios.patch(finalPatchUrl, { thumb_link: result });
            console.log(`✅ [ARCHIVE] Final entry thumb patched for msgId=${msgId}`);
          } catch (e) {
            console.error(`❌ [ARCHIVE] Final thumb patch error (msgId=${msgId}):`, e.message);
          }
        } else if (result) {
          const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${msgId}.json`;
          try {
            await axios.patch(pendingUrl, { thumb_link: result });
            console.log(`✅ [ARCHIVE] Pending entry thumb patched for msgId=${msgId}`);
          } catch (e) {
            if (e.response?.status !== 404) {
              console.error(`❌ [ARCHIVE] Pending thumb patch error (msgId=${msgId}):`, e.message);
            }
          }
        }
      }
    } catch (e) {
      console.error(`❌ Thumb Upload Error (msgId=${msgId}):`, e.message);
    } finally {
      resolvePromise(result);
      setTimeout(() => thumbPromises.delete(msgId), 10 * 60 * 1000);
      clearMemory();
    }
  })();
}

// ============================================================
// 🆕 VIDEO CHUNK CACHE SYSTEM
// ============================================================
const VIDEO_CACHE_DIR = path.join(os.tmpdir(), 'video-chunk-cache');
if (!fs.existsSync(VIDEO_CACHE_DIR)) {
  fs.mkdirSync(VIDEO_CACHE_DIR, { recursive: true });
  console.log(`📁 [CACHE] Video cache directory created: ${VIDEO_CACHE_DIR}`);
}

// 🆕 Har chunk (download ho ya cache) max itna hi bada — poori video kabhi ek saath
// disk pe nahi aayegi, chahe player kaisa bhi range request bheje
const MAX_CHUNK_BYTES = 3 * 1024 * 1024; // 3 MB

const inflightChunks = new Set(); // "msgId_start_end" -> currently downloading

// 🆕 JUGAAD (DISK FIX): har video ka sirf EK active chunk disk pe rahega.
// Naya chunk cache hote hi, usi msgId ka PURANA chunk turant delete ho jata hai —
// isliye "current watch time" ke alawa kuch bhi disk pe accumulate nahi hota,
// chahe kitne bhi video ek saath aayein ya dekhe jayein.
const activeChunkPath = new Map(); // msgId -> currently cached chunk file path

function markActiveChunkAndEvictOld(msgId, newFilePath) {
  const prev = activeChunkPath.get(msgId);
  if (prev && prev !== newFilePath) {
    fs.unlink(prev, () => {}); // purana turant delete
  }
  activeChunkPath.set(msgId, newFilePath);
}

function getChunkCachePath(msgId, start, end) {
  return path.join(VIDEO_CACHE_DIR, `${msgId}_${start}_${end}.chunk`);
}

async function getCachedChunk(msgId, start, end) {
  const filePath = getChunkCachePath(msgId, start, end);
  try {
    await fs.promises.access(filePath);
    // Update mtime (touch) so cleanup doesn't delete recently used chunks
    const now = new Date();
    await fs.promises.utimes(filePath, now, now);
    const data = await fs.promises.readFile(filePath);
    return data;
  } catch {
    return null;
  }
}

async function saveChunkCache(msgId, start, end, buffer) {
  const filePath = getChunkCachePath(msgId, start, end);
  try {
    await fs.promises.writeFile(filePath, buffer);
  } catch (e) {
    console.error(`❌ [CACHE] Save failed msgId=${msgId}:`, e.message);
  }
}

// 🆕 SAFETY-NET SWEEP — har 30 second par chalta hai. Ye ek "backup" hai:
// normal flow mein chunks turant delete ho jaate hain (upar wala eviction), lekin
// agar koi error/crash ki wajah se koi file RAM/disk pe reh jaaye (orphan), ye
// force-delete kar deta hai. Render ka disk kabhi bhi bharega nahi.
function globalDiskSweep() {
  const now = Date.now();

  // 1. Video chunk cache — 60 second se purana koi bhi chunk = stale, delete
  //    (normal use mein to naya chunk aate hi purana turant delete ho jata hai,
  //    ye sirf un chunks ko pakadta hai jo kisi wajah se reh gaye)
  const CHUNK_STALE_MS = 60 * 1000;
  fs.readdir(VIDEO_CACHE_DIR, (err, files) => {
    if (err) return;
    let deleted = 0;
    files.forEach(file => {
      const filePath = path.join(VIDEO_CACHE_DIR, file);
      fs.stat(filePath, (err, stats) => {
        if (!err && (now - stats.mtimeMs > CHUNK_STALE_MS)) {
          fs.unlink(filePath, () => {});
          deleted++;
        }
      });
    });
    if (deleted > 0) console.log(`🧹 [SWEEP] ${deleted} stale chunk(s) force-deleted (video-chunk-cache)`);
  });

  // 2. Transcode staging dir — agar archive.org upload fail hua tha aur file
  //    fallback ke roop mein padi hai, 15 min se purani ho to force-delete
  const TRANSCODE_STALE_MS = 15 * 60 * 1000;
  if (fs.existsSync(TRANSCODE_DIR)) {
    fs.readdir(TRANSCODE_DIR, (err, files) => {
      if (err) return;
      let deleted = 0;
      files.forEach(file => {
        const filePath = path.join(TRANSCODE_DIR, file);
        fs.stat(filePath, (err, stats) => {
          if (!err && (now - stats.mtimeMs > TRANSCODE_STALE_MS)) {
            fs.unlink(filePath, () => {});
            deleted++;
          }
        });
      });
      if (deleted > 0) console.log(`🧹 [SWEEP] ${deleted} stale transcode file(s) force-deleted`);
    });
  }

  // 3. Orphan original-source temp files (transcodesrc_*) — agar transcode ke
  //    beech process crash/restart ho jaaye to ye files kabhi delete nahi hoti
  //    normal "finally" block se. 15 min se purani ho to safety-delete.
  fs.readdir(os.tmpdir(), (err, files) => {
    if (err) return;
    let deleted = 0;
    files.forEach(file => {
      if (!file.startsWith("transcodesrc_")) return;
      const filePath = path.join(os.tmpdir(), file);
      fs.stat(filePath, (err, stats) => {
        if (!err && (now - stats.mtimeMs > TRANSCODE_STALE_MS)) {
          fs.unlink(filePath, () => {});
          deleted++;
        }
      });
    });
    if (deleted > 0) console.log(`🧹 [SWEEP] ${deleted} orphan source temp file(s) force-deleted`);
  });
}
setInterval(globalDiskSweep, 30 * 1000);

// ============================================================
// 🆕 BACKGROUND TRANSCODE — 360p (JUGAAD #1: quality kam karke bandwidth/CPU bachao)
// ============================================================
// Jab video channel pe aata hai, background mein turant 360p mein convert ho jata hai,
// aur turant archive.org pe upload ho jata hai — Render ka disk isse touch hi nahi hota
// (upload hote hi local file delete). Stream endpoint seedha archive.org pe redirect kar deta hai.
// Kuch der baad (30 min) archive.org se bhi file auto-delete ho jaati hai.
const TRANSCODE_DIR = path.join(os.tmpdir(), "video-transcode-360p"); // sirf temp/staging ke liye
if (!fs.existsSync(TRANSCODE_DIR)) {
  fs.mkdirSync(TRANSCODE_DIR, { recursive: true });
  console.log(`📁 [TRANSCODE] 360p staging directory created: ${TRANSCODE_DIR}`);
}

const transcodingInProgress = new Set(); // msgId jo abhi transcode ho rahe hain

// 🆕 JUGAAD #3: TRANSCODE QUEUE — ek saath bahut saare videos aa jayen (batch upload) to
// bhi Render CPU/RAM/disk overload na ho. Sirf MAX_CONCURRENT_TRANSCODES video ek time pe
// process honge, baaki line mein wait karenge (thumbnail/tagging/streaming pe koi asar nahi,
// sirf 360p transcode limited hai).
const MAX_CONCURRENT_TRANSCODES = parseInt(process.env.MAX_CONCURRENT_TRANSCODES || "2");
const transcodeQueue = []; // [{ msgId, message }]
const queuedTranscodeIds = new Set(); // duplicate queue-entry se bachne ke liye
let activeTranscodeCount = 0;

function enqueueTranscode(msgId, message) {
  if (transcodingInProgress.has(msgId) || queuedTranscodeIds.has(msgId) || isTranscodeReady(msgId)) return;
  transcodeQueue.push({ msgId, message });
  queuedTranscodeIds.add(msgId);
  console.log(`📥 [TRANSCODE-QUEUE] msgId=${msgId} queue mein add hua | active=${activeTranscodeCount}/${MAX_CONCURRENT_TRANSCODES} | waiting=${transcodeQueue.length}`);
  processTranscodeQueue();
}

function processTranscodeQueue() {
  while (activeTranscodeCount < MAX_CONCURRENT_TRANSCODES && transcodeQueue.length > 0) {
    const { msgId, message } = transcodeQueue.shift();
    queuedTranscodeIds.delete(msgId);
    activeTranscodeCount++;
    startBackgroundTranscode(msgId, message).finally(() => {
      activeTranscodeCount--;
      processTranscodeQueue(); // agla queue wala uthao
    });
  }
}
const transcodeDeleteTimers = new Map(); // fallback: msgId -> setTimeout (agar archive upload fail ho)
const archiveVideoMap = new Map(); // msgId -> { url, identifier, filename } (archive.org pe 360p)
const archiveDeleteTimers = new Map(); // msgId -> setTimeout handle (archive.org se auto-delete)
const ARCHIVE_VIDEO_TTL = 30 * 60 * 1000; // 30 minute baad archive.org se bhi delete

function getTranscodedPath(msgId) {
  return path.join(TRANSCODE_DIR, `transcoded_${msgId}.mp4`);
}

function isTranscodeReady(msgId) {
  return archiveVideoMap.has(msgId) || fs.existsSync(getTranscodedPath(msgId));
}

// FALLBACK ONLY (agar archive upload fail ho jaaye) — Render disk se 30min baad delete
function scheduleTranscodeDelete(msgId) {
  if (transcodeDeleteTimers.has(msgId)) {
    clearTimeout(transcodeDeleteTimers.get(msgId));
  }
  const timer = setTimeout(() => {
    fs.unlink(getTranscodedPath(msgId), (err) => {
      if (!err) console.log(`🗑️ [TRANSCODE] 360p fallback file auto-deleted (Render disk) msgId=${msgId}`);
    });
    transcodeDeleteTimers.delete(msgId);
  }, 30 * 60 * 1000);
  transcodeDeleteTimers.set(msgId, timer);
}

// Har baar access ho, TTL timer reset (touch) ho jata hai — jab tak koi dekh raha hai, expire nahi hoga
function scheduleArchiveVideoDelete(msgId) {
  if (archiveDeleteTimers.has(msgId)) {
    clearTimeout(archiveDeleteTimers.get(msgId));
  }
  const timer = setTimeout(() => {
    const entry = archiveVideoMap.get(msgId);
    if (entry) deleteVideoFromArchive(entry.identifier, entry.filename);
    archiveVideoMap.delete(msgId);
    archiveDeleteTimers.delete(msgId);
  }, ARCHIVE_VIDEO_TTL);
  archiveDeleteTimers.set(msgId, timer);
}

async function uploadVideoToArchive(filePath, idPrefix) {
  if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) {
    console.error("❌ [ARCHIVE-VIDEO] ARCHIVE KEYS missing, upload skip!");
    return null;
  }

  const identifier = `${idPrefix}-${Date.now()}-${crypto.randomBytes(4).toString("hex")}`.toLowerCase();
  const filename = "video360p.mp4";
  const uploadUrl = `https://s3.us.archive.org/${identifier}/${filename}`;
  const stat = fs.statSync(filePath);

  try {
    await axios.put(uploadUrl, fs.createReadStream(filePath), {
      headers: {
        "Content-Type": "video/mp4",
        "Content-Length": stat.size,
        "Authorization": `LOW ${ARCHIVE_ACCESS_KEY.trim()}:${ARCHIVE_SECRET_KEY.trim()}`,
        "x-archive-auto-make-bucket": "1",
        "x-archive-meta-mediatype": "movies",
        "x-archive-meta-title": `360p ${identifier}`,
      },
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      timeout: 10 * 60 * 1000, // bade video ke liye 10 min timeout
    });

    console.log(`✅ [ARCHIVE-VIDEO] Upload success: ${identifier} (${(stat.size / 1024 / 1024).toFixed(1)}MB)`);
    return { url: `https://archive.org/download/${identifier}/${filename}`, identifier, filename };
  } catch (e) {
    const errDetails = e.response?.data ? String(e.response.data) : e.message;
    console.error("❌ [ARCHIVE-VIDEO] Upload error:", errDetails);
    return null;
  }
}

async function deleteVideoFromArchive(identifier, filename) {
  if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) return;
  const url = `https://s3.us.archive.org/${identifier}/${filename}`;
  try {
    await axios.delete(url, {
      headers: {
        "Authorization": `LOW ${ARCHIVE_ACCESS_KEY.trim()}:${ARCHIVE_SECRET_KEY.trim()}`,
      },
    });
    console.log(`🗑️ [ARCHIVE-VIDEO] archive.org se delete ho gaya: ${identifier}`);
  } catch (e) {
    const errDetails = e.response?.data ? String(e.response.data) : e.message;
    console.error(`❌ [ARCHIVE-VIDEO] Delete error (${identifier}):`, errDetails);
  }
}

async function startBackgroundTranscode(msgId, message) {
  if (transcodingInProgress.has(msgId) || isTranscodeReady(msgId)) return;
  transcodingInProgress.add(msgId);

  const srcPath = path.join(os.tmpdir(), `transcodesrc_${msgId}_${Date.now()}.mp4`);
  const outPath = getTranscodedPath(msgId);

  try {
    console.log(`🎬 [TRANSCODE] Background transcode shuru — msgId=${msgId}`);

    // 1. Poora original video Telegram se download karo temp file mein
    const media = message.media;
    await new Promise((resolve, reject) => {
      const writeStream = fs.createWriteStream(srcPath);
      writeStream.on("error", reject);
      (async () => {
        try {
          const dlStream = client.iterDownload({ file: media });
          for await (const chunk of dlStream) {
            if (!writeStream.write(chunk)) {
              await new Promise((r) => writeStream.once("drain", r));
            }
          }
          writeStream.end();
        } catch (dlErr) {
          reject(dlErr);
        }
      })();
      writeStream.on("finish", resolve);
    });

    // 2. ffmpeg se 360p mein convert karo (staging: Render ke temp disk pe, thodi der ke liye)
    await new Promise((resolve, reject) => {
      const ff = spawn(ffmpegPath, [
        "-y",
        "-i", srcPath,
        "-vf", "scale=-2:360",
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "28",
        "-c:a", "aac",
        "-b:a", "96k",
        "-movflags", "+faststart",
        outPath,
      ]);

      let stderrTail = "";
      ff.stderr.on("data", (d) => {
        stderrTail = (stderrTail + d.toString()).slice(-2000); // sirf last 2000 chars rakho (RAM bachane ke liye)
      });
      ff.on("close", (code) => {
        if (code === 0) resolve();
        else reject(new Error(`ffmpeg exit code ${code} | ${stderrTail.slice(-300)}`));
      });
      ff.on("error", reject); // e.g. ffmpeg binary hi nahi mila
    });

    console.log(`✅ [TRANSCODE] 360p ready msgId=${msgId} → uploading to archive.org...`);

    // 3. 🆕 Render disk pe rakhne ke bajaye turant archive.org pe upload karo
    const archiveResult = await uploadVideoToArchive(outPath, `labdesk-360p-${msgId}`);

    if (archiveResult) {
      archiveVideoMap.set(msgId, archiveResult);
      scheduleArchiveVideoDelete(msgId);
      fs.unlink(outPath, () => {}); // 🆕 Render disk TURANT khali — 30min wait nahi karna
      console.log(`☁️ [ARCHIVE-VIDEO] msgId=${msgId} ab archive.org se serve hoga, Render disk saaf hai.`);
    } else {
      // Upload fail ho gaya (keys missing / network issue) — fallback: Render disk pe hi rakho
      console.log(`⚠️ [ARCHIVE-VIDEO] Upload fail — fallback: msgId=${msgId} Render disk se serve hoga (30min).`);
      scheduleTranscodeDelete(msgId);
    }
  } catch (e) {
    console.error(`❌ [TRANSCODE] Failed msgId=${msgId}:`, e.message);
    fs.unlink(outPath, () => {});
  } finally {
    fs.unlink(srcPath, () => {});
    transcodingInProgress.delete(msgId);
  }
}

app.get("/", (req, res) => res.send("Bot Active - Multi-Source Pipeline + Chunk Cache + 360p Transcode"));

// ============================================================
// 🆕 JUGAAD #2: PREWARM — video aate hi pehla chunk turant cache mein daal do
// ============================================================
// Player play dabate hi pehle ~1.5MB (moov atom + start) maangta hai.
// Agar ye pehle se cache mein pada ho, to first request TURANT (0ms Telegram-wait) serve hoga —
// user ko "buffering" ka wait nahi karna padega. Ye AI-tagging pipeline ke parallel chalta hai,
// isiliye by the time link kisi ko milta hai, cache already garam ho chuka hota hai.
const PREWARM_BYTES = MAX_CHUNK_BYTES; // 🆕 same cap use karo — taaki cache-key match ho aur pehla real click turant HIT ho

// 🆕 Prewarm bhi queue se guzarta hai (halka hai, par 10 ek-saath aane par Telegram
// flood-wait bhi de sakta hai — isliye max 4 parallel prewarm)
const MAX_CONCURRENT_PREWARMS = parseInt(process.env.MAX_CONCURRENT_PREWARMS || "4");
const prewarmQueue = [];
const queuedPrewarmIds = new Set();
let activePrewarmCount = 0;

function enqueuePrewarm(msgId, entity, media) {
  if (queuedPrewarmIds.has(msgId)) return;
  prewarmQueue.push({ msgId, entity, media });
  queuedPrewarmIds.add(msgId);
  processPrewarmQueue();
}

function processPrewarmQueue() {
  while (activePrewarmCount < MAX_CONCURRENT_PREWARMS && prewarmQueue.length > 0) {
    const { msgId, entity, media } = prewarmQueue.shift();
    queuedPrewarmIds.delete(msgId);
    activePrewarmCount++;
    prewarmStream(msgId, entity, media).finally(() => {
      activePrewarmCount--;
      processPrewarmQueue();
    });
  }
}

async function prewarmStream(msgId, entity, media) {
  const fileSize = Number(media.document ? media.document.size : 0);
  if (!fileSize) return;

  const end = Math.min(PREWARM_BYTES - 1, fileSize - 1);
  const cacheKey = `${msgId}_0_${end}`;

  try {
    if (inflightChunks.has(cacheKey)) return;

    const existing = await getCachedChunk(msgId, 0, end);
    if (existing) return; // already warm

    inflightChunks.add(cacheKey);
    console.log(`🔥 [PREWARM] msgId=${msgId} ka pehla ${(end + 1)} bytes cache kar rahe hain...`);

    const chunks = [];
    const stream = client.iterDownload({ file: media, offset: bigInt(0), limit: end + 1 });
    for await (const chunk of stream) chunks.push(chunk);

    const buffer = Buffer.concat(chunks);
    if (buffer.length) {
      await saveChunkCache(msgId, 0, end, buffer);
      markActiveChunkAndEvictOld(msgId, getChunkCachePath(msgId, 0, end)); // 🆕 eviction-map mein register karo
      console.log(`✅ [PREWARM] msgId=${msgId} cache READY — ab first click instant play hoga.`);
    }
  } catch (e) {
    console.error(`❌ [PREWARM] Failed msgId=${msgId}:`, e.message);
  } finally {
    inflightChunks.delete(cacheKey);
  }
}

// ============================================================
// 🆕 STREAM: Chunk Cache ke saath
// ============================================================
app.get("/stream/:msgId", async (req, res) => {
  try {
    const msgId = parseInt(req.params.msgId);

    // 🆕 360p ARCHIVE.ORG CHECK — agar upload ho chuka hai to Render ka bandwidth/disk
    // istemal kiye bina seedha archive.org pe redirect kar do (fastest, Render pe load zero)
    if (archiveVideoMap.has(msgId)) {
      scheduleArchiveVideoDelete(msgId); // access hua to 30min TTL reset (touch)
      const archiveUrl = archiveVideoMap.get(msgId).url;
      console.log(`☁️ [ARCHIVE-VIDEO] msgId=${msgId} → redirect to ${archiveUrl}`);
      return res.redirect(302, archiveUrl);
    }

    // 🆕 360p TRANSCODE FALLBACK — agar archive upload fail hua tha, Render disk se serve
    // (fastest local fallback + Telegram rate-limit se bachaav)
    const transcodedPath = getTranscodedPath(msgId);
    if (fs.existsSync(transcodedPath)) {
      scheduleTranscodeDelete(msgId); // access hua to 30min timer reset (touch)
      const stat = fs.statSync(transcodedPath);
      const fileSize = stat.size;
      const range = req.headers.range;

      if (range) {
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
        fs.createReadStream(transcodedPath, { start, end }).pipe(res);
      } else {
        res.writeHead(200, {
          "Content-Type": "video/mp4",
          "Content-Length": fileSize,
        });
        fs.createReadStream(transcodedPath).pipe(res);
      }
      console.log(`⚡ [TRANSCODE] 360p SERVED from Render disk (fallback) msgId=${msgId} (${fileSize} bytes)`);
      return;
    }

    // 🆕 JUGAAD #2: agar iss video ka channel pehle se pata hai, seedha wahi query karo
    // (dono channels mein loop-search karke time waste nahi karna)
    const knownChatIdStr = videoLocationCache.get(msgId);
    const entitiesToTry = knownChatIdStr && sourceEntities.has(knownChatIdStr)
      ? [[knownChatIdStr, sourceEntities.get(knownChatIdStr)]]
      : Array.from(sourceEntities.entries());

    for (const [chatIdStr, entity] of entitiesToTry) {
      const messages = await client.getMessages(entity, { ids: msgId });
      if (!messages || !messages[0] || !messages[0].media) continue;

      videoLocationCache.set(msgId, chatIdStr); // 🆕 agli baar ke liye yaad rakho

      const message = messages[0];
      const media = message.media;

      // 🆕 JUGAAD (LAZY ACTIVATION): user ne is video pe click kiya hai — SIRF ab
      // background 360p transcode + prewarm queue mein daalo. Function khud duplicate-safe
      // hain (already-queued/in-progress/ready check karte hain), isliye baar-baar seek/reload
      // pe bhi dobara kaam nahi hoga. Ye fire-and-forget hai, current request ko block nahi karta.
      if (isVideoMessage(message)) {
        enqueueTranscode(msgId, message);
        enqueuePrewarm(msgId, entity, media);
      }

      const fileSize = Number(media.document ? media.document.size : 0);
      const range = req.headers.range;

      let start = 0;
      let end = fileSize - 1;
      let chunkSize = fileSize;
      let isRange = false;

      if (range && fileSize) {
        const [startStr, endStr] = range.replace(/bytes=/, "").split("-");
        start = parseInt(startStr, 10);
        const requestedEnd = endStr ? parseInt(endStr, 10) : fileSize - 1;
        // 🆕 DISK-FIX (asli bug yahi tha): pehle "bytes=0-" (open-ended, jo har player
        // bhejta hai) ko poori video maan liya jata tha aur POORA video cache/download
        // ho jaata tha. Ab chunk hamesha MAX_CHUNK_BYTES tak hi capped hai — player khud
        // agla range maang lega jab usse aur data chahiye (normal streaming behavior).
        end = Math.min(requestedEnd, start + MAX_CHUNK_BYTES - 1, fileSize - 1);
        chunkSize = end - start + 1;
        isRange = true;
      }

      // 🆕 CACHE CHECK
      const cacheKey = `${msgId}_${start}_${end}`;
      const cached = await getCachedChunk(msgId, start, end);

      if (cached) {
        console.log(`⚡ [CACHE] HIT msgId=${msgId} [${start}-${end}] (${cached.length} bytes)`);
        markActiveChunkAndEvictOld(msgId, getChunkCachePath(msgId, start, end)); // 🆕 same file, bas map sync
        const headers = isRange ? {
          "Content-Range": `bytes ${start}-${end}/${fileSize}`,
          "Accept-Ranges": "bytes",
          "Content-Length": chunkSize,
          "Content-Type": "video/mp4",
        } : {
          "Content-Type": "video/mp4",
          "Content-Length": fileSize,
        };
        res.writeHead(isRange ? 206 : 200, headers);
        res.end(cached);
        return;
      }

      // 🆕 If another request is already downloading this same chunk, wait for it
      if (inflightChunks.has(cacheKey)) {
        let waitLoops = 0;
        while (inflightChunks.has(cacheKey) && waitLoops < 40) {
          await sleep(250);
          waitLoops++;
        }
        const retryCached = await getCachedChunk(msgId, start, end);
        if (retryCached) {
          console.log(`⚡ [CACHE] HIT after wait msgId=${msgId} [${start}-${end}]`);
          const headers = isRange ? {
            "Content-Range": `bytes ${start}-${end}/${fileSize}`,
            "Accept-Ranges": "bytes",
            "Content-Length": chunkSize,
            "Content-Type": "video/mp4",
          } : {
            "Content-Type": "video/mp4",
            "Content-Length": fileSize,
          };
          res.writeHead(isRange ? 206 : 200, headers);
          res.end(retryCached);
          return;
        }
      }

      // 🆕 DOWNLOAD FROM TELEGRAM + CACHE
      console.log(`📥 [CACHE] MISS msgId=${msgId} [${start}-${end}] — downloading from Telegram...`);
      inflightChunks.add(cacheKey);

      const headers = isRange ? {
        "Content-Range": `bytes ${start}-${end}/${fileSize}`,
        "Accept-Ranges": "bytes",
        "Content-Length": chunkSize,
        "Content-Type": "video/mp4",
      } : {
        "Content-Type": "video/mp4",
        "Content-Length": fileSize,
      };
      res.writeHead(isRange ? 206 : 200, headers);

      const chunks = [];
      let clientClosed = false;
      res.on('close', () => { clientClosed = true; });

      try {
        const stream = client.iterDownload({ file: media, offset: bigInt(start), limit: chunkSize });
        for await (const chunk of stream) {
          if (clientClosed) break;
          chunks.push(chunk);
          if (!res.write(chunk)) await new Promise((r) => res.once("drain", r));
        }

        if (!clientClosed) {
          res.end();
          // Save to cache only if we got complete data
          const fullBuffer = Buffer.concat(chunks);
          if (fullBuffer.length === chunkSize) {
            await saveChunkCache(msgId, start, end, fullBuffer);
            markActiveChunkAndEvictOld(msgId, getChunkCachePath(msgId, start, end)); // 🆕 purana chunk turant delete
            console.log(`💾 [CACHE] SAVED msgId=${msgId} [${start}-${end}] (${fullBuffer.length} bytes) — purana chunk evict ho gaya`);
          } else {
            console.log(`⚠️ [CACHE] Incomplete download msgId=${msgId} — not cached (${fullBuffer.length}/${chunkSize})`);
          }
        }
      } catch (streamErr) {
        console.error(`❌ [STREAM] Error msgId=${msgId}:`, streamErr.message);
        if (!res.headersSent) res.status(500).send("Stream Error");
      } finally {
        inflightChunks.delete(cacheKey);
      }
      return;
    }

    res.status(404).send("Not Found");
  } catch (e) {
    console.error("❌ [STREAM] Route Error:", e.message);
    if (!res.headersSent) res.status(500).send("Streaming Error");
  } finally {
    clearMemory();
  }
});

// 🆕 DOWNLOAD: Dono channels mein se dhoondhega (no cache needed for downloads)
app.get("/download/:msgId", async (req, res) => {
  try {
    const msgId = parseInt(req.params.msgId);

    for (const [chatIdStr, entity] of sourceEntities) {
      const messages = await client.getMessages(entity, { ids: msgId });
      if (!messages || !messages[0] || !messages[0].media) continue;

      const message = messages[0];

      if (isVideoMessage(message)) {
        return res.status(403).send("Video downloads are not allowed.");
      }

      const media = message.media;
      const doc = media.document;
      const fileSize = Number(doc ? doc.size : 0);
      const mimeType = (doc && doc.mimeType) || "application/octet-stream";

      let fileName = `file_${msgId}`;
      if (doc && doc.attributes) {
        const nameAttr = doc.attributes.find((a) => a.className === "DocumentAttributeFilename");
        if (nameAttr && nameAttr.fileName) fileName = nameAttr.fileName;
      }

      res.writeHead(200, {
        "Content-Type": mimeType,
        "Content-Length": fileSize,
        "Content-Disposition": `attachment; filename="${encodeURIComponent(fileName)}"`,
      });

      const stream = client.iterDownload({ file: media, offset: bigInt(0) });
      for await (const chunk of stream) {
        if (!res.write(chunk)) await new Promise((r) => res.once("drain", r));
      }
      res.end();
      return;
    }

    res.status(404).send("Not Found");
  } catch (e) {
    if (!res.headersSent) res.status(500).send("Download Error");
  } finally {
    clearMemory();
  }
});

async function pushDirectToFirebase(msgId, streamLink) {
  const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${msgId}.json`;

  const dataPayload = {
    msg_id: msgId,
    stream_link: streamLink,
    status: "pending",
    timestamp: { ".sv": "timestamp" },
  };

  try {
    await axios.put(pendingUrl, dataPayload);
    console.log(`🔥 [FIREBASE] Data Pending state mein push ho gaya (msgId=${msgId})`);
    pendingTimestamps.set(msgId, Date.now());
  } catch (e) {
    console.error("❌ [FIREBASE] Pending push error:", e.response?.data || e.message);
  }
}

// -------------------------------------------------------------
// 🧹 AUTO-CLEANUP: 4 minute timeout
// -------------------------------------------------------------
async function cleanupOldPendingEntries() {
  const now = Date.now();
  const FOUR_MINUTES = 4 * 60 * 1000;

  const expiredEntries = [];
  for (const [msgId, timestamp] of pendingTimestamps.entries()) {
    if (now - timestamp > FOUR_MINUTES) {
      expiredEntries.push(msgId);
    }
  }

  for (const msgId of expiredEntries) {
    const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${msgId}.json`;
    try {
      await axios.delete(pendingUrl);
      console.log(`🗑️ [AUTO-CLEANUP] Pending msgId=${msgId} deleted (4 min timeout)`);
    } catch (e) {
      console.error(`❌ [AUTO-CLEANUP] Failed to delete msgId=${msgId}:`, e.message);
    }

    pendingTimestamps.delete(msgId);

    const idx = screenshotRequestQueue.indexOf(msgId);
    if (idx !== -1) screenshotRequestQueue.splice(idx, 1);
    screenshotResults.delete(msgId);
    thumbPromises.delete(msgId);

    for (let i = pendingReplyQueue.length - 1; i >= 0; i--) {
      if (pendingReplyQueue[i].msgId === msgId) {
        pendingReplyQueue.splice(i, 1);
      }
    }
    for (let i = pendingTypeQueue.length - 1; i >= 0; i--) {
      if (pendingTypeQueue[i].msgId === msgId) {
        pendingTypeQueue.splice(i, 1);
      }
    }

    for (const [sentId, origId] of chatgptSentToOriginal.entries()) {
      if (origId === msgId) chatgptSentToOriginal.delete(sentId);
    }
    for (const [fwdId, origId] of typeCheckerSentToOriginal.entries()) {
      if (origId === msgId) typeCheckerSentToOriginal.delete(fwdId);
    }
  }
}
setInterval(cleanupOldPendingEntries, 30 * 1000);

// -------------------------------------------------------------
// EVENT HANDLER
// -------------------------------------------------------------
async function handleIncomingMessage(event) {
  try {
    const message = event.message;
    if (!message) return;

    let msgChatIdStr = "";
    if (message.peerId) {
      if (message.peerId.channelId) msgChatIdStr = bigInt(message.peerId.channelId).toString();
      else if (message.peerId.chatId) msgChatIdStr = bigInt(message.peerId.chatId).toString();
      else if (message.peerId.userId) msgChatIdStr = bigInt(message.peerId.userId).toString();
    }

    const senderIdSync = message.senderId ? message.senderId.toString() : "";

    // 🔍 DEBUG LOG: Har message ka source dikhayega
    if (msgChatIdStr) {
      const isKnown = sourceChatIdStrs.has(msgChatIdStr);
      console.log(`🔍 [DEBUG] msgId=${message.id} | chatId=${msgChatIdStr} | knownSource=${isKnown} | sources=[${Array.from(sourceChatIdStrs).join(", ")}]`);
    }

    // 🆕 DONO CHANNELS CHECK
    if (sourceChatIdStrs.has(msgChatIdStr)) {
      console.log(`⚡ Channel se new message aaya ID=${message.id} from chat=${msgChatIdStr}`);

      const currentSourceEntity = sourceEntities.get(msgChatIdStr);
      if (!currentSourceEntity) {
        console.error(`❌ Source entity not found for ${msgChatIdStr}`);
        return;
      }

      const hasMedia = message.media && (message.media.document || message.media.photo);
      if (!hasMedia) {
        console.log(`⏩ [IGNORE] Sirf text/link hai ID=${message.id}, ignore kar diya.`);
        return;
      }

      const streamLink = `${RENDER_URL}/stream/${message.id}`;
      const captionText = message.message || message.text || "";

      if (isVideoMessage(message)) {
        videoLocationCache.set(message.id, msgChatIdStr); // 🆕 stream lookup ke liye yaad rakho
        pushDirectToFirebase(message.id, streamLink);
        startThumbUpload(message.id, streamLink);
        // 🆕 Transcode/prewarm ab yahan SHURU NAHI hote — sirf jab user pehli baar
        // /stream pe click karega tab hi trigger honge (dekho /stream/:msgId handler).
        // Isse sirf "active" (dekhe ja rahe) video hi CPU/disk/bandwidth use karte hain,
        // upload hote hi saare videos process nahi hote.

        const fallbackText = captionText || "Media File";
        enqueueForTagging(message.id, fallbackText);
        console.log(`📨 [VIDEO] msgId=${message.id} → AI ko caption bheja: "${fallbackText.substring(0, 60)}..."`);
      }
      else if (message.media && message.media.document) {
        console.log(`📄 Document detected (ID=${message.id}). Forwarding to @P840bot + AI...`);
        pushDirectToFirebase(message.id, streamLink);
        forwardDocToTypeChecker(message.id, currentSourceEntity);

        const docCaption = getDocumentFileName(message) || captionText || "Document File";
        enqueueForTagging(message.id, docCaption);
        console.log(`📨 [DOCUMENT] msgId=${message.id} → AI ko caption bheja: "${docCaption.substring(0, 60)}..."`);
      }
      else {
        console.log(`⏩ [IGNORE] Photo/Other media ID=${message.id}, ignore kar diya.`);
      }

      return;
    }

    // 2. Reply from @P840bot
    const isFromTypeChecker = (typeCheckerBotIdStr && senderIdSync === typeCheckerBotIdStr) ||
                             (typeCheckerBotIdStr && msgChatIdStr.includes(typeCheckerBotIdStr));

    if (isFromTypeChecker) {
      const replyText = message.message || message.text || "";
      console.log(`🤖 [@P840bot Response]: "${replyText}"`);

      let detectedType = "@other";
      if (replyText.includes("@dpp")) detectedType = "@dpp";
      else if (replyText.includes("@notes")) detectedType = "@notes";

      let matched = null;
      const replyToId = (message.replyTo?.replyToMsgId || message.replyToMsgId)?.toString();
      if (replyToId && typeCheckerSentToOriginal.has(replyToId)) {
        const originalMsgId = typeCheckerSentToOriginal.get(replyToId);
        const idx = pendingTypeQueue.findIndex(p => p.msgId === originalMsgId);
        if (idx !== -1) {
          matched = pendingTypeQueue.splice(idx, 1)[0];
          typeCheckerSentToOriginal.delete(replyToId);
          console.log(`🔗 [TYPE-CHECKER] Correlated via replyTo: replyToId=${replyToId} -> originalMsgId=${originalMsgId}`);
        }
      }
      if (!matched) {
        matched = pendingTypeQueue.shift();
        if (matched) {
          console.log(`⚠️ [TYPE-CHECKER] replyTo missing, using FIFO fallback for msgId=${matched.msgId}`);
        }
      }

      if (matched) {
        console.log(`📝 [TYPE-CHECKER] msgId=${matched.msgId} ka type "${detectedType}" mil gaya.`);
        const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${matched.msgId}.json`;

        try {
          await axios.patch(pendingUrl, {
            content_type: detectedType,
            download_link: `${RENDER_URL}/download/${matched.msgId}`
          });
        } catch (e) {
          if (e.response && e.response.status === 404) {
            const finalPath = finalizedDocPaths.get(matched.msgId);
            if (finalPath) {
              const finalPatchUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/${encodeURIComponent(finalPath.subject)}/${encodeURIComponent(finalPath.chapter)}/${matched.msgId}.json`;
              try {
                await axios.patch(finalPatchUrl, {
                  content_type: detectedType,
                  download_link: `${RENDER_URL}/download/${matched.msgId}`
                });
                console.log(`📝 [TYPE-CHECKER] Final entry patched for msgId=${matched.msgId}`);
              } catch (e2) {
                console.error("❌ Final Patch Error:", e2.message);
              }
            }
          } else {
            console.error("❌ Type Patch Error:", e.message);
          }
        }
      }
      return;
    }

    // 3. ChatGPT Bot Reply
    const isFromChatGPT = (chatgptBotIdStr && senderIdSync === chatgptBotIdStr) ||
                         (chatgptBotIdStr && msgChatIdStr.includes(chatgptBotIdStr));

    if (isFromChatGPT) {
      const replyText = message.message || message.text || "";
      const replyClean = replyText.toLowerCase();
      const isThinking = ["सोच...", "thinking..."].some((t) => replyClean.includes(t));

      if (isThinking) {
        console.log(`⏳ [CHATGPT] Thinking message ignore kiya.`);
        return;
      }

      let matched = null;
      const replyToId = (message.replyTo?.replyToMsgId || message.replyToMsgId)?.toString();
      if (replyToId && chatgptSentToOriginal.has(replyToId)) {
        const originalMsgId = chatgptSentToOriginal.get(replyToId);
        const idx = pendingReplyQueue.findIndex(p => p.msgId === originalMsgId);
        if (idx !== -1) {
          matched = pendingReplyQueue.splice(idx, 1)[0];
          chatgptSentToOriginal.delete(replyToId);
          console.log(`🔗 [CHATGPT] Correlated via replyTo: replyToId=${replyToId} -> originalMsgId=${originalMsgId}`);
        }
      }
      if (!matched) {
        matched = pendingReplyQueue.shift();
        if (matched) {
          console.log(`⚠️ [CHATGPT] replyTo missing, using FIFO fallback for msgId=${matched.msgId}`);
        }
      }

      if (!matched) {
        console.log(`⚠️ [CHATGPT] Reply aaya par pendingReplyQueue khaali thi.`);
        return;
      }

      if (!replyText.trim().startsWith("@")) {
        console.log(`⚠️ [CHATGPT] Galat reply (@ se start nahi) msgId=${matched.msgId}: "${replyText.substring(0, 80)}..." → IGNORE + DELETE PENDING`);

        const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${matched.msgId}.json`;
        try {
          await axios.delete(pendingUrl);
          console.log(`🗑️ [FIREBASE] Pending entry DELETED for msgId=${matched.msgId} (invalid AI reply)`);
        } catch (e) {
          console.error("❌ [FIREBASE] Pending delete error:", e.message);
        }

        pendingTimestamps.delete(matched.msgId);

        sendQueue.push({ msgId: null, text: RULE_REMINDER_TEXT, isReminder: true });
        console.log(`🔁 [RULE-REMINDER] AI se galat jawab aaya - TURANT rules bhej rahe hain.`);

        if (!isSending) {
          processSendQueue().catch((e) => {
            console.error("❌ [TAG-QUEUE] processSendQueue error:", e);
            isSending = false;
          });
        }
        return;
      }

      const segments = replyText.split("@").map((s) => s.trim()).filter(Boolean);
      const hasLectureTag = segments.length >= 3 && /^lec/i.test(segments[2]);

      if (segments.length < 3 || !hasLectureTag) {
        console.log(`⚠️ [CHATGPT] Incomplete reply msgId=${matched.msgId}: "${replyText.substring(0, 80)}..." → IGNORE + DELETE PENDING`);

        const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${matched.msgId}.json`;
        try {
          await axios.delete(pendingUrl);
          console.log(`🗑️ [FIREBASE] Pending entry DELETED for msgId=${matched.msgId} (incomplete AI reply)`);
        } catch (e) {
          console.error("❌ [FIREBASE] Pending delete error:", e.message);
        }

        pendingTimestamps.delete(matched.msgId);

        sendQueue.push({ msgId: null, text: RULE_REMINDER_TEXT, isReminder: true });
        console.log(`🔁 [RULE-REMINDER] AI se galat jawab aaya - TURANT rules bhej rahe hain.`);

        if (!isSending) {
          processSendQueue().catch((e) => {
            console.error("❌ [TAG-QUEUE] processSendQueue error:", e);
            isSending = false;
          });
        }
        return;
      }

      console.log(`🏷️ ChatGPT tags match hue msgId=${matched.msgId} se. Parsing...`);
      await patchAiTagsToFirebase(matched.msgId, replyText);
      return;
    }

    // 4. Screenshot Bot Reply
    const isFromScreenshotBot = (screenshotBotIdStr && senderIdSync === screenshotBotIdStr) ||
                                (screenshotBotIdStr && msgChatIdStr.includes(screenshotBotIdStr));

    if (isFromScreenshotBot) {
      if (message.photo || (message.media && message.media.className === 'MessageMediaPhoto')) {
        const targetMsgId = screenshotRequestQueue.shift();
        if (targetMsgId) {
          screenshotResults.set(targetMsgId, message);
          console.log(`📸 [THUMB-BOT] Screenshot successfully matched for msgId=${targetMsgId}`);
        } else {
          console.log(`⚠️ [THUMB-BOT] Unexpected photo received, no pending request.`);
        }
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

  clearMemory();
  cleanupTempFiles();

  try {
    await client.connect();

    let me = null;
    try {
      me = await client.getMe();
      console.log(`🤖 [BOT-IDENTITY] Account: @${me.username || 'no-username'} | ID=${me.id} | Name=${me.firstName}${me.lastName ? ' ' + me.lastName : ''}`);
    } catch (e) {
      console.error("❌ [BOT-IDENTITY] getMe failed:", e.message);
    }

    chatgptEntity = await client.getEntity(CHATGPT_BOT);
    chatgptBotIdStr = chatgptEntity.id.toString();

    typeCheckerEntity = await client.getEntity(TYPE_CHECKER_BOT);
    typeCheckerBotIdStr = typeCheckerEntity.id.toString();

    console.log(`📡 [CHANNEL-CHECK] ${SOURCE_CHATS.length} channels load kar raha hai...`);
    for (const chat of SOURCE_CHATS) {
      try {
        const entity = await client.getEntity(chat);
        const idStr = bigInt(entity.id).toString();
        sourceEntities.set(idStr, entity);
        sourceChatIdStrs.add(idStr);
        console.log(`📌 [CHANNEL-LOAD] ${chat} -> ID=${idStr} | Title="${entity.title || 'N/A'}"`);

        try {
          const testMsgs = await client.getMessages(entity, { limit: 1 });
          if (testMsgs && testMsgs.length > 0 && testMsgs[0]) {
            console.log(`✅ [CHANNEL-ACCESS] ${chat} CONNECTED! | LastMsgID=${testMsgs[0].id} | Access=OK`);
          } else {
            console.log(`⚠️ [CHANNEL-ACCESS] ${chat} Connected but NO MESSAGES found (empty channel?)`);
          }
        } catch (accessErr) {
          console.error(`❌ [CHANNEL-ACCESS] ${chat} FAILED! | Error: ${accessErr.message}`);
          console.error(`   ➜ Reason: Bot account channel ka member nahi hai ya access nahi hai.`);
          console.error(`   ➜ Fix: Bot account (@${me?.username || 'unknown'}) ko channel mein add/join karwao.`);
        }
      } catch (e) {
        console.error(`❌ [CHANNEL-LOAD] ${chat} FAILED! | Error: ${e.message}`);
      }
    }

    console.log(`📊 [CHANNEL-SUMMARY] Total Loaded: ${sourceEntities.size}/${SOURCE_CHATS.length}`);
    console.log(`📊 [CHANNEL-SUMMARY] Active IDs: [${Array.from(sourceChatIdStrs).join(", ")}]`);

    screenshotEntity = await client.getEntity(NEW_SCREENSHOT_BOT);
    screenshotBotIdStr = screenshotEntity.id.toString();

    await loadTagMsgCount();

    console.log(`📌 Bots Loaded: ChatGPT=${chatgptBotIdStr} | TypeChecker=${typeCheckerBotIdStr}`);

    client.addEventHandler(handleIncomingMessage, new NewMessage({}));

    client.addEventHandler(async (update) => {
      try {
        if (
          update.className === "UpdateEditMessage" ||
          update.className === "UpdateEditChannelMessage"
        ) {
          await handleIncomingMessage({ message: update.message });
        }
      } catch (e) {
        console.error("❌ Raw Edit Handler Error:", e.message);
      }
    });

    console.log("🤖 Client Ready! Multi-Source Workflow + Chunk Cache Active.");
  } catch (e) {
    console.error("❌ Init Error:", e.message);
  }
}

startServer();
