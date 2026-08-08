process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (err) => {
  console.error('❌ Uncaught Exception:', err);
});

const { TelegramClient, Api } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { NewMessage, EditedMessage } = require("telegram/events");
const express = require("express");
const axios = require("axios");
const bigInt = require("big-integer");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawn } = require("child_process");
const ffmpegPath = require("ffmpeg-static");
const crypto = require("crypto");

const app = express();
const PORT = process.env.PORT || 10000;
app.use(express.json({ limit: "8mb" }));

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
let sourceMessageCount = 0;
const MESSAGES_PER_CLEANUP = 12;
const KEEP_RECENT_IN_CACHE = 3;

function cleanupMessageCache() {
  const keys = Array.from(messageCache.keys());
  const toDelete = keys.slice(0, Math.max(0, keys.length - KEEP_RECENT_IN_CACHE));
  toDelete.forEach((k) => messageCache.delete(k));
  console.log(`🧹 [CLEANUP] ${toDelete.length} purane messageCache entries hataye | ab bache: ${messageCache.size}`);
}

// -------------------------------------------------------------
// SEQUENTIAL QUEUE
// -------------------------------------------------------------
const messageQueue = [];
let isProcessingQueue = false;
let currentMediaInfo = null;
let resolveCurrentReply = null;

function enqueueSourceMessage(item) {
  messageQueue.push(item);
  console.log(`📥 [QUEUE] Add hua ID=${item.msgId} | queue length ab: ${messageQueue.length}`);
  if (!isProcessingQueue) {
    processQueue().catch((e) => {
      console.error("❌ [QUEUE] processQueue crash hua:", e);
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
        console.log("⏱️ [QUEUE] Timeout - is item ka reply nahi mila, queue ko rok nahi sakte, agle item pe badh rahe hain");
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

    console.log(`\n➡️ [QUEUE] Process ho raha hai ID=${item.msgId} | baaki bache queue mein: ${messageQueue.length}`);

    try {
      if (!chatgptEntity) {
        chatgptEntity = await client.getEntity(CHATGPT_BOT);
      }
      await client.sendMessage(chatgptEntity, { message: item.text });
      console.log("📨 [QUEUE] ChatGPT ko bhej diya, ab sirf ISI item ke asli jawab ka wait kar rahe hain...");

      await waitForChatGPTReply(60000);
    } catch (e) {
      console.error("❌ [QUEUE] Processing error:", e.message);
    }

    resolveCurrentReply = null;
    currentMediaInfo = null;
  }

  isProcessingQueue = false;
}

// -------------------------------------------------------------
// MANUAL VIDEO QUALITY
// -------------------------------------------------------------
const QUALITY_PRESETS = {
  low: { height: 360, videoBitrate: "500k", audioBitrate: "64k" },
  medium: { height: 480, videoBitrate: "900k", audioBitrate: "96k" },
};
const MAX_TRANSCODE_SOURCE_BYTES = 600 * 1024 * 1024;
const transcodeJobs = new Map();

function transcodedFilePath(msgId, quality) {
  return path.join(os.tmpdir(), `transcoded_${msgId}_${quality}.mp4`);
}

async function downloadFullFile(message, destPath) {
  const writeStream = fs.createWriteStream(destPath);
  const stream = client.iterDownload({ file: message.media, offset: bigInt(0) });
  try {
    for await (const chunk of stream) {
      if (!writeStream.write(chunk)) {
        await new Promise((resolve) => writeStream.once("drain", resolve));
      }
    }
  } finally {
    await new Promise((resolve, reject) => {
      writeStream.end((err) => (err ? reject(err) : resolve()));
    });
  }
}

function runFfmpegTranscode(srcPath, destPath, preset) {
  return new Promise((resolve, reject) => {
    const args = [
      "-y",
      "-i", srcPath,
      "-vf", `scale=-2:${preset.height}`,
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-b:v", preset.videoBitrate,
      "-maxrate", preset.videoBitrate,
      "-bufsize", `${parseInt(preset.videoBitrate, 10) * 2}k`,
      "-c:a", "aac",
      "-b:a", preset.audioBitrate,
      "-movflags", "+faststart",
      destPath,
    ];
    const proc = spawn(ffmpegPath, args);
    let stderr = "";
    proc.stderr.on("data", (d) => (stderr += d.toString()));
    proc.on("error", reject);
    proc.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg transcode exited ${code}: ${stderr.slice(-500)}`));
    });
  });
}

async function ensureTranscoded(message, msgId, quality) {
  const preset = QUALITY_PRESETS[quality];
  if (!preset) return null;

  const doc = message.media && message.media.document;
  const fileSize = doc ? Number(doc.size) || 0 : 0;
  if (fileSize && fileSize > MAX_TRANSCODE_SOURCE_BYTES) {
    console.log(`⚠️ [QUALITY] msgId=${msgId} file bahut badi hai (${fileSize} bytes) - HD original bhej rahe hain.`);
    return null;
  }

  const destPath = transcodedFilePath(msgId, quality);
  if (fs.existsSync(destPath)) return destPath;

  const jobKey = `${msgId}_${quality}`;
  if (transcodeJobs.has(jobKey)) return transcodeJobs.get(jobKey);

  const job = (async () => {
    const srcPath = path.join(os.tmpdir(), `transcodesrc_${msgId}_${Date.now()}.mp4`);
    try {
      console.log(`🎚️ [QUALITY] "${quality}" transcode shuru ho raha hai msgId=${msgId}`);
      await downloadFullFile(message, srcPath);
      await runFfmpegTranscode(srcPath, destPath, preset);
      console.log(`✅ [QUALITY] "${quality}" transcode ban gaya msgId=${msgId}`);
      return destPath;
    } catch (e) {
      console.error(`❌ [QUALITY] Transcode fail hui msgId=${msgId} quality=${quality}:`, e.message);
      fs.promises.unlink(destPath).catch(() => {});
      return null;
    } finally {
      fs.promises.unlink(srcPath).catch(() => {});
      transcodeJobs.delete(jobKey);
    }
  })();

  transcodeJobs.set(jobKey, job);
  return job;
}

function serveLocalFile(filePath, req, res) {
  return new Promise((resolve) => {
    fs.stat(filePath, (err, stats) => {
      if (err) {
        if (!res.headersSent) res.status(500).send("Quality file error");
        return resolve();
      }
      const fileSize = stats.size;
      const range = req.headers.range;
      if (range) {
        const [startStr, endStr] = range.replace(/bytes=/, "").split("-");
        const start = parseInt(startStr, 10);
        const end = endStr ? parseInt(endStr, 10) : fileSize - 1;
        res.writeHead(206, {
          "Content-Range": `bytes ${start}-${end}/${fileSize}`,
          "Accept-Ranges": "bytes",
          "Content-Length": end - start + 1,
          "Content-Type": "video/mp4",
        });
        fs.createReadStream(filePath, { start, end })
          .pipe(res)
          .on("finish", resolve)
          .on("error", resolve);
      } else {
        res.writeHead(200, {
          "Content-Type": "video/mp4",
          "Content-Length": fileSize,
          "Accept-Ranges": "bytes",
        });
        fs.createReadStream(filePath).pipe(res).on("finish", resolve).on("error", resolve);
      }
    });
  });
}

const TEMP_FILE_PREFIXES = ["transcoded_", "transcodesrc_", "thumbsrc_", "thumbout_"];
const TEMP_FILE_MAX_AGE_MS = 60 * 1000;
function cleanupTempFiles() {
  fs.readdir(os.tmpdir(), (err, files) => {
    if (err) return;
    const now = Date.now();
    files
      .filter((f) => TEMP_FILE_PREFIXES.some((prefix) => f.startsWith(prefix)))
      .forEach((f) => {
        const p = path.join(os.tmpdir(), f);
        fs.stat(p, (statErr, stats) => {
          if (statErr) return;
          if (now - stats.mtimeMs > TEMP_FILE_MAX_AGE_MS) {
            fs.unlink(p, () => {});
          }
        });
      });
  });
}
setInterval(cleanupTempFiles, 60 * 1000);
cleanupTempFiles();

// -------------------------------------------------------------
// THUMBNAIL QUEUE
// -------------------------------------------------------------
const thumbPromises = new Map();
const thumbQueue = [];
let isThumbQueueRunning = false;

function enqueueThumbJob(job) {
  thumbQueue.push(job);
  if (!isThumbQueueRunning) runThumbQueue();
}

async function runThumbQueue() {
  isThumbQueueRunning = true;
  while (thumbQueue.length > 0) {
    const job = thumbQueue.shift();
    try {
      await job();
    } catch (e) {
      console.error("❌ [THUMB-QUEUE] Job crash hua:", e.message);
    }
  }
  isThumbQueueRunning = false;
}

const DOWNLOAD_ATTEMPT_TIMEOUT_MS = 15000;

function withTimeout(promise, ms, timeoutMessage) {
  let timedOut = false;
  const guarded = Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => { timedOut = true; reject(new Error(timeoutMessage)); }, ms)),
  ]);
  promise.then(
    () => { if (timedOut) console.log(`ℹ️ [THUMB-DIAG] Timeout ke baad request asal mein safal ho gaya tha.`); },
    (err) => { if (timedOut) console.error(`ℹ️ [THUMB-DIAG] Timeout ke baad asli underlying error mila:`, err.message); }
  );
  return guarded;
}

function isVideoDocument(message) {
  const doc = message.media && message.media.document;
  if (!doc) return false;
  if (doc.mimeType && doc.mimeType.startsWith("video/")) return true;
  if (doc.attributes) {
    return doc.attributes.some((a) => a.className === "DocumentAttributeVideo");
  }
  return false;
}

// -------------------------------------------------------------
// SCREENSHOT-BOT PIPELINE (FIXED)
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

const SCREENSHOT_MENU_TIMEOUT_MS = 30000;
const SCREENSHOT_PHOTO_TIMEOUT_MS = 180000; // Increased to 3 minutes for slow bot processing

// FIXED: Using GramJS built-in msg.click() with fallback
async function clickInlineButton(msg, buttonText) {
  if (!msg) {
    console.error("❌ [THUMB-BOT] Menu message null hai.");
    return null;
  }

  try {
    // Primary & most reliable GramJS method: msg.click({ text: '...' })
    const clickResult = await msg.click({ text: buttonText });
    console.log(`🖱️ [THUMB-BOT] msg.click() se '${buttonText}' click ho gaya.`);
    return clickResult || true;
  } catch (err) {
    console.warn(`⚠️ msg.click() fail hua (${err.message}), Direct Callback Invoke try kar rahe hain...`);
    
    const rows = msg.replyMarkup && msg.replyMarkup.rows;
    if (!rows || !rows.length) return null;

    let targetButton = null;
    for (const row of rows) {
      for (const btn of row.buttons) {
        if (btn.text === buttonText) {
          targetButton = btn;
          break;
        }
      }
      if (targetButton) break;
    }

    if (!targetButton || !targetButton.data) return null;

    return await client.invoke(
      new Api.messages.GetBotCallbackAnswer({
        peer: screenshotEntity,
        msgId: msg.id,
        data: targetButton.data,
        game: false
      })
    );
  }
}

async function getThumbViaScreenshotBot(message) {
  if (!screenshotEntity || !sourceEntity) {
    console.log("⚠️ [THUMB-BOT] Entities resolve nahi hue - skip.");
    return null;
  }

  try {
    // 1. Video ko Screenshot-bot ko forward karo
    await client.forwardMessages(screenshotEntity, {
      messages: [message.id],
      fromPeer: sourceEntity,
    });
    console.log(`📤 [THUMB-BOT] msgId=${message.id} Screenshot-bot ko forward kiya.`);

    // 2. Bot ke options-menu reply ka wait
    const menuMsg = await waitForScreenshotBotMessage(SCREENSHOT_MENU_TIMEOUT_MS, (m) => {
      return !!(m.replyMarkup && m.replyMarkup.rows);
    });

    if (!menuMsg) {
      console.error(`❌ [THUMB-BOT] msgId=${message.id}: Options-menu reply nahi mila.`);
      return null;
    }

    // 3. "Get Thumbs" button click karo
    const clicked = await clickInlineButton(menuMsg, "Get Thumbs");
    if (!clicked) {
      console.error(`❌ [THUMB-BOT] msgId=${message.id}: Button click execute nahi hua.`);
      return null;
    }
    console.log(`🖱️ [THUMB-BOT] msgId=${message.id}: "Get Thumbs" par click triggered.`);

    // 4. Thumbnail photo ka wait (180s)
    const photoMsg = await waitForScreenshotBotMessage(SCREENSHOT_PHOTO_TIMEOUT_MS, (m) => {
      return !!(m.photo || (m.media && m.media.className === 'MessageMediaPhoto'));
    });

    if (!photoMsg) {
      console.error(`❌ [THUMB-BOT] msgId=${message.id}: Thumbnail photo ka reply nahi mila (${SCREENSHOT_PHOTO_TIMEOUT_MS / 1000}s).`);
      return null;
    }

    // 5. Photo download karo
    const buffer = await client.downloadMedia(photoMsg);
    if (buffer && buffer.length) {
      console.log(`✅ [THUMB-BOT] msgId=${message.id}: Thumbnail photo mil gayi.`);
      return buffer;
    }
    return null;
  } catch (e) {
    console.error(`❌ [THUMB-BOT] Error msgId=${message.id}:`, e && e.stack ? e.stack : e);
    return null;
  }
}

async function generateThumbFrame(message) {
  if (!isVideoDocument(message)) return null;

  const viaBot = await getThumbViaScreenshotBot(message);
  if (viaBot && viaBot.length) return viaBot;

  console.log(`⚠️ [THUMB] Screenshot-bot se nahi mila msgId=${message.id} - embedded thumb try kar rahe hain.`);

  const thumbs = message.media && message.media.document && message.media.document.thumbs;
  if (!thumbs || !thumbs.length) {
    console.log(`⚠️ [THUMB] msgId=${message.id} ke paas Telegram embedded thumb bhi nahi hai - skip.`);
    return null;
  }

  try {
    const thumb = await withTimeout(
      client.downloadMedia(message, { thumb: -1 }),
      DOWNLOAD_ATTEMPT_TIMEOUT_MS,
      `embedded thumb download ${DOWNLOAD_ATTEMPT_TIMEOUT_MS / 1000}s timeout`
    );
    if (thumb && thumb.length) return thumb;
    return null;
  } catch (e) {
    console.error("❌ [THUMB] Embedded thumb download fail hui:", e.message);
    return null;
  }
}

// -------------------------------------------------------------
// THUMBNAIL HOSTING (archive.org)
// -------------------------------------------------------------
async function uploadToArchive(buffer, idPrefix) {
  if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) {
    console.log("⚠️ [ARCHIVE] Keys missing - upload skip.");
    return null;
  }
  const identifier = `${idPrefix}-${Date.now()}-${crypto.randomBytes(3).toString("hex")}`.toLowerCase();
  const filename = "thumb.jpg";
  const uploadUrl = `https://s3.us.archive.org/${identifier}/${filename}`;

  try {
    console.log(`⬆️ [ARCHIVE] Upload shuru: ${uploadUrl}`);
    await axios.put(uploadUrl, buffer, {
      headers: {
        "Content-Type": "image/jpeg",
        "Authorization": `LOW ${ARCHIVE_ACCESS_KEY}:${ARCHIVE_SECRET_KEY}`,
        "x-archive-auto-make-bucket": "1",
        "x-archive-meta-mediatype": "image",
        "x-archive-meta-collection": "opensource_media",
      },
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      timeout: 30000,
    });
    console.log(`✅ [ARCHIVE] Upload ban gaya: ${uploadUrl}`);
    return `https://archive.org/download/${identifier}/${filename}`;
  } catch (e) {
    const reason = e.code === "ECONNABORTED"
      ? "30s timeout - archive.org se connection slow/block"
      : (e.response ? JSON.stringify(e.response.data) : e.message);
    console.error(`❌ [ARCHIVE] Upload fail hui:`, reason);
    return null;
  }
}

function startThumbUpload(message) {
  if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) {
    console.log("⚠️ [THUMB] Keys missing - thumbnail upload skip.");
    return;
  }
  const msgId = message.id;

  let resolvePromise;
  const promise = new Promise((resolve) => { resolvePromise = resolve; });
  thumbPromises.set(msgId, promise);

  enqueueThumbJob(async () => {
    let result = null;
    try {
      const frameBuffer = await Promise.race([
        generateThumbFrame(message),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("generateThumbFrame 200s timeout")), 200000)
        ),
      ]);
      if (!frameBuffer || !frameBuffer.length) {
        console.error(`❌ [THUMB] Koi bhi frame nahi mila msgId=${msgId}`);
      } else {
        const url = await uploadToArchive(frameBuffer, `labdesk-thumb-${msgId}`);
        if (url) {
          console.log(`🖼️ [THUMB] Archive.org upload OK msgId=${msgId}: ${url}`);
        } else {
          console.error(`❌ [THUMB] Archive.org se URL nahi mila, msgId=${msgId}`);
        }
        result = url || null;
      }
    } catch (e) {
      console.error(`❌ [THUMB] Upload fail hui msgId=${msgId}:`, e.response ? e.response.data : e.message);
    } finally {
      setTimeout(() => thumbPromises.delete(msgId), 5 * 60 * 1000);
    }
    resolvePromise(result);
  });
}

async function preWarmStream(msgId) {
  try {
    if (!sourceEntity) {
      sourceEntity = await client.getEntity(SOURCE_CHAT);
    }
    const messages = await client.getMessages(sourceEntity, { ids: msgId });
    if (messages && messages.length && messages[0].media) {
      messageCache.set(msgId, messages[0]);
      console.log(`🔥 [PRE-WARM] Message ID=${msgId} cache mein ready.`);
    }
  } catch (e) {
    console.error("❌ [PRE-WARM] Error:", e.message);
  }
}

function extractFileNameText(message) {
  try {
    if (message.media && message.media.document && message.media.document.attributes) {
      for (const attr of message.media.document.attributes) {
        if (attr.fileName) {
          return attr.fileName
            .replace(/\.[a-zA-Z0-9]{2,5}$/, "")
            .replace(/[_\-]+/g, " ")
            .replace(/\s{2,}/g, " ")
            .trim();
        }
      }
    }
  } catch (e) {}
  return "";
}

app.get("/", (req, res) => {
  res.send("Node.js Proxy & Bot is Active!");
});

// -------------------------------------------------------------
// STREAMING & DOWNLOAD ROUTE
// -------------------------------------------------------------
app.get("/stream/:msgId", async (req, res) => {
  try {
    const msgId = parseInt(req.params.msgId);

    let message = messageCache.get(msgId);
    if (!message) {
      if (!sourceEntity) {
        sourceEntity = await client.getEntity(SOURCE_CHAT);
      }
      const messages = await client.getMessages(sourceEntity, { ids: msgId });
      if (!messages || messages.length === 0 || !messages[0].media) {
        return res.status(404).send("Media not found");
      }
      message = messages[0];
      messageCache.set(msgId, message);
    }

    if (!message.media) {
      return res.status(404).send("Media not found");
    }

    const requestedQuality = req.query.q;
    if (requestedQuality && QUALITY_PRESETS[requestedQuality]) {
      const localPath = await ensureTranscoded(message, msgId, requestedQuality);
      if (localPath) {
        return await serveLocalFile(localPath, req, res);
      }
      console.log(`⚠️ [QUALITY] msgId=${msgId} ke liye "${requestedQuality}" nahi mili - HD original bhej rahe hain.`);
    }

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
    const forceDownload = req.query.dl === "1";
    const asciiFallback = fileName.replace(/[^\x20-\x7E]/g, "_");
    const encodedName = encodeURIComponent(fileName);
    const dispositionType = forceDownload ? "attachment" : "inline";
    const contentDisposition = `${dispositionType}; filename="${asciiFallback}"; filename*=UTF-8''${encodedName}`;

    if (range && fileSize) {
      const [startStr, endStr] = range.replace(/bytes=/, "").split("-");
      const start = parseInt(startStr, 10);
      const end = endStr ? parseInt(endStr, 10) : fileSize - 1;
      const chunkSize = end - start + 1;

      res.writeHead(206, {
        "Content-Range": `bytes ${start}-${end}/${fileSize}`,
        "Accept-Ranges": "bytes",
        "Content-Length": chunkSize,
        "Content-Type": mimeType,
        "Content-Disposition": contentDisposition,
      });

      const stream = client.iterDownload({
        file: media,
        offset: bigInt(start),
        limit: chunkSize,
      });

      for await (const chunk of stream) {
        if (!res.write(chunk)) {
          await new Promise((resolve) => res.once("drain", resolve));
        }
      }
      res.end();
    } else {
      res.writeHead(200, {
        "Content-Type": mimeType,
        "Content-Length": fileSize,
        "Accept-Ranges": "bytes",
        "Content-Disposition": contentDisposition,
      });

      const stream = client.iterDownload({ file: media, offset: bigInt(0) });

      for await (const chunk of stream) {
        if (!res.write(chunk)) {
          await new Promise((resolve) => res.once("drain", resolve));
        }
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

app.post("/thumb-fallback", async (req, res) => {
  try {
    const { image, subjectKey, chapterName, entryKey } = req.body || {};
    if (!image || !subjectKey || !chapterName || !entryKey) {
      return res.status(400).json({ error: "image, subjectKey, chapterName, entryKey zaroori hain" });
    }
    if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) {
      return res.status(500).json({ error: "ARCHIVE_ACCESS_KEY/ARCHIVE_SECRET_KEY set nahi hain" });
    }

    const base64Image = String(image).replace(/^data:image\/\w+;base64,/, "");
    const imageBuffer = Buffer.from(base64Image, "base64");

    const safeEntryKey = String(entryKey).toLowerCase().replace(/[^a-z0-9]+/g, "-").slice(0, 40);
    const url = await uploadToArchive(imageBuffer, `labdesk-fallback-${safeEntryKey}`);
    if (!url) {
      return res.status(502).json({ error: "Archive.org upload se URL nahi mila" });
    }

    const firebasePath = `${FIREBASE_BASE_URL}/${encodeURIComponent(subjectKey)}/${encodeURIComponent(chapterName)}/${encodeURIComponent(entryKey)}/thumb_link.json`;
    await axios.put(firebasePath, JSON.stringify(url));

    console.log(`🖼️ [THUMB-FALLBACK] entryKey=${entryKey} -> ${url}`);
    return res.json({ url });
  } catch (e) {
    console.error("❌ [THUMB-FALLBACK] Error:", e.response ? e.response.data : e.message);
    return res.status(500).json({ error: "Internal error" });
  }
});

// -------------------------------------------------------------
// FIREBASE PUSH LOGIC
// -------------------------------------------------------------
async function processReplyAndPushToFirebase(replyText, mediaInfo) {
  if (!replyText) return false;

  const replyClean = replyText.trim().toLowerCase();
  const ignoreList = ["सोच...", "thinking...", "please wait...", "generating..."];

  if (ignoreList.some((ig) => replyClean.includes(ig))) {
    console.log("⏳ AI अभी सोच रहा है (सोच... state), स्किप कर रहे हैं।");
    return false;
  }

  console.log(`📩 ChatGPT का असली जवाब: "${replyText}"`);

  const segments = replyText
    .split("@")
    .map((s) => s.trim())
    .filter(Boolean);

  const devanagariRegex = /[\u0900-\u097F]/;

  let contentType = "@other";
  let lecTag = "";
  let subjectName = "";
  let chapterName = "";

  for (const seg of segments) {
    const segLower = seg.toLowerCase();

    if (segLower.startsWith("dpp") || segLower.includes("practice sheet")) {
      contentType = "@dpp";
    } else if (segLower.startsWith("notes")) {
      contentType = "@notes";
    } else if (segLower.startsWith("other")) {
      contentType = "@other";
    } else if (segLower.startsWith("lec")) {
      lecTag = "@" + seg;
    } else if (devanagariRegex.test(seg)) {
      chapterName = seg;
    } else if (!subjectName) {
      subjectName = seg;
    }
  }

  subjectName = subjectName || "General";
  chapterName = chapterName || "General_Lectures";

  const subjectKey = subjectName.replace(/[.$#\[\]/]/g, "_");
  const chapterKey = chapterName.replace(/[.$#\[\]/]/g, "_");

  const lecNum = lecTag.replace(/^@?lec\s*/i, "").trim();
  let displayTitle;
  if (contentType === "@notes") {
    displayTitle = `${chapterName} — Notes${lecNum ? " (" + lecNum + ")" : ""}`;
  } else if (contentType === "@dpp") {
    displayTitle = `${chapterName} — DPP${lecNum ? " (" + lecNum + ")" : ""}`;
  } else {
    displayTitle = `${chapterName}${lecNum ? " — Lecture " + lecNum : ""}`;
  }

  const dataPayload = {
    content_type: contentType,
    lecture_no: lecTag,
    raw_reply: replyText,
    display_title: displayTitle,
    timestamp: { ".sv": "timestamp" },
  };

  if (mediaInfo && mediaInfo.stream_link) {
    if (["@notes", "@dpp"].includes(contentType)) {
      dataPayload["download_link"] = `${mediaInfo.stream_link}?dl=1`;
    } else {
      dataPayload["stream_link"] = mediaInfo.stream_link;

      if (mediaInfo.msg_id && thumbPromises.has(mediaInfo.msg_id)) {
        const thumbUrl = await Promise.race([
          thumbPromises.get(mediaInfo.msg_id),
          new Promise((resolve) => setTimeout(() => resolve(null), 180000)),
        ]);
        if (thumbUrl) dataPayload["thumb_link"] = thumbUrl;
        thumbPromises.delete(mediaInfo.msg_id);
      }
    }
  }

  const firebaseUrl = `${FIREBASE_BASE_URL}/${subjectKey}/${chapterKey}.json`;
  console.log(`🚀 Push target: ${subjectKey} ➔ ${chapterKey}`);

  try {
    const res = await axios.post(firebaseUrl, dataPayload);
    if (res.status === 200 || res.status === 201) {
      console.log(`🔥 SUCCESS! Firebase में डेटा पुश हो गया! Path: ${subjectKey} ➔ ${chapterKey}`);
      return true;
    } else {
      console.error(`❌ Firebase Error Status: ${res.status}`);
      return true;
    }
  } catch (err) {
    console.error(`❌ Firebase Exception:`, err.response ? err.response.data : err.message);
    return true;
  }
}

// -------------------------------------------------------------
// EVENT HANDLERS
// -------------------------------------------------------------
async function handleIncomingMessage(event) {
  try {
    const message = event.message;
    if (!message) return;

    const currentText = message.text || message.message || "";
    const chatIdStr = message.chatId ? message.chatId.toString() : "";
    const senderIdSync = message.senderId ? message.senderId.toString() : "";

    let chatUsername = "", chatTitle = "", senderUsername = "";
    try {
      const chat = await message.getChat();
      chatUsername = (chat && chat.username ? chat.username : "").toLowerCase();
      chatTitle = (chat && chat.title ? chat.title : "").toLowerCase();
    } catch (e) {}
    try {
      const sender = await message.getSender();
      senderUsername = (sender && sender.username ? sender.username : "").toLowerCase();
    } catch (e) {}

    console.log(`🔍 DEBUG chat=${chatUsername} title=${chatTitle} sender=${senderUsername} chatId=${chatIdStr} senderId=${senderIdSync} msgId=${message.id}`);

    const isSourceChat =
      (sourceChatId && chatIdStr === sourceChatId) ||
      chatUsername === "sxhckfufig" ||
      chatTitle.includes("sxhckfufig");

    if (isSourceChat) {
      console.log(`\n📩 [STEP 1] Channel se Naya Message Aaya (ID: ${message.id})`);

      sourceMessageCount++;
      if (sourceMessageCount % MESSAGES_PER_CLEANUP === 0) {
        cleanupMessageCache();
      }

      let streamLink = "";
      if (message.media) {
        streamLink = `${RENDER_URL}/stream/${message.id}`;
        console.log(`🔗 Stream Link Banna: ${streamLink}`);
        preWarmStream(message.id);
        startThumbUpload(message);
      }

      const msgText = currentText || extractFileNameText(message) || "Media File";
      enqueueSourceMessage({ msgId: message.id, streamLink, text: msgText });
      console.log(`➡️ [STEP 2] Queue mein daal diya gaya`);
      return;
    }

    const isChatGPT = chatgptBotId && senderIdSync === chatgptBotId;

    if (isChatGPT) {
      console.log(`\n🤖 [STEP 3] ChatGPT Response Detect Hua: "${currentText}"`);
      const mediaInfo = currentMediaInfo || {};
      const wasFinalAnswer = await processReplyAndPushToFirebase(currentText, mediaInfo);

      if (wasFinalAnswer && resolveCurrentReply) {
        resolveCurrentReply();
      }
    }

    const isScreenshotBot = screenshotBotId && senderIdSync === screenshotBotId;
    if (isScreenshotBot) {
      handleScreenshotBotMessage(message);
    }

  } catch (err) {
    console.error("❌ Event Handler Error:", err);
  }
}

// -------------------------------------------------------------
// SERVER STARTUP
// -------------------------------------------------------------
function startKeepAlivePing() {
  setInterval(() => {
    axios.get(RENDER_URL).catch(() => {});
  }, 10 * 60 * 1000);
}

async function startServer() {
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Server listening on 0.0.0.0:${PORT}`);
    startKeepAlivePing();
  });

  try {
    await client.connect();
    console.log("✅ Telegram Client Connected!");

    try {
      chatgptEntity = await client.getEntity(CHATGPT_BOT);
      chatgptBotId = chatgptEntity.id.toString();
      console.log("🤖 ChatGPT Bot ID resolved:", chatgptBotId);
    } catch (e) {
      console.error("❌ ChatGPT Bot ID resolve nahi hua:", e.message);
    }

    try {
      sourceEntity = await client.getEntity(SOURCE_CHAT);
      sourceChatId = sourceEntity.id.toString();
      console.log("📡 Source Chat ID resolved:", sourceChatId);
    } catch (e) {
      console.error("❌ Source Chat ID resolve nahi hua:", e.message);
    }

    try {
      screenshotEntity = await client.getEntity(SCREENSHOT_BOT);
      screenshotBotId = screenshotEntity.id.toString();
      console.log("📸 Screenshot Bot ID resolved:", screenshotBotId);
    } catch (e) {
      console.error("❌ Screenshot Bot ID resolve nahi hua:", e.message);
    }

    client.addEventHandler(handleIncomingMessage, new NewMessage({}));

    client.addEventHandler(async (update) => {
      try {
        if (
          update.className === "UpdateEditMessage" ||
          update.className === "UpdateEditChannelMessage"
        ) {
          console.log("✏️ Raw Edit Update Detect Hua...");
          await handleIncomingMessage({ message: update.message });
        }
      } catch (e) {
        console.error("❌ Raw Edit Handler Error:", e);
      }
    });

    console.log("🤖 Event Handlers Successfully Registered!");
  } catch (err) {
    console.error("❌ Telegram Client Connection Error:", err);
  }
}

startServer();
