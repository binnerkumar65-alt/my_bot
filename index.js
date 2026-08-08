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
app.use(express.json({ limit: "8mb" })); // client se aane waale base64 thumbnail frame ke liye

// Environment Variables Configuration
const apiId = parseInt(process.env.API_ID || "0");
const apiHash = process.env.API_HASH || "";
const stringSession = new StringSession(process.env.SESSION_STRING || "");

const SOURCE_CHAT = "@sxhckfufig";
const CHATGPT_BOT = "@chatgpt";
// "Screenshot Generator Bot" - video bhej kar "Get Thumbs" click karne par
// wo video ka ek real thumbnail (photo) reply karta hai. Telegram ke apne
// embedded thumb se zyada reliable hai (bahut saari videos mein embedded
// thumb hota hi nahi), isliye ab yahi hamara primary thumbnail source hai.
const SCREENSHOT_BOT = "@screenshotit_bot";
const FIREBASE_BASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com";
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || "https://my-bot-kgrk.onrender.com";
// Internet Archive (archive.org) IAS3 upload API - ImgBB ki jagah, kyunki
// ImgBB Render ke datacenter IP ko block kar raha tha ("forbidden", code
// 103). Archive.org ka upload S3-jaisa hai - single key nahi, ek ACCESS
// key aur ek SECRET key chahiye. Dono https://archive.org/account/s3.php
// se milte hain (archive.org account banao -> is page par jao -> "Create
// S3 Keys").
const ARCHIVE_ACCESS_KEY = process.env.ARCHIVE_ACCESS_KEY || "";
const ARCHIVE_SECRET_KEY = process.env.ARCHIVE_SECRET_KEY || "";

if (!apiId || !apiHash) {
  console.error("❌ API_ID या API_HASH missing हैं!");
  process.exit(1);
}

const client = new TelegramClient(stringSession, apiId, apiHash, {
  connectionRetries: 5,
});

// Resolved once at startup - used for reliable ChatGPT bot detection
let chatgptBotId = null;
// Cached ChatGPT bot entity object - resolved once at startup, reused by
// the queue so every queued item doesn't need its own Telegram lookup.
let chatgptEntity = null;
// Resolved once at startup - used for reliable source-channel detection
let sourceChatId = null;
// Cached entity object (not just the ID) - reused by /stream so every
// request doesn't re-fetch it from Telegram, which was adding latency
// on every single click.
let sourceEntity = null;

// Resolved once at startup - used for reliable Screenshot-bot reply detection
let screenshotBotId = null;
let screenshotEntity = null;

// Small in-memory cache of message objects, keyed by msgId. Populated
// right after a message arrives (pre-warm), so by the time the HTML
// requests /stream/:msgId, the Telegram lookup is already done and the
// response starts immediately instead of waiting on a fresh getMessages call.
const messageCache = new Map();

// Render ke free instance par RAM seemit hai - agar messageCache hamesha
// badhta rahe (har naya forward add hota jaaye, kabhi hatta na) to instance
// dheere-dheere slow/heavy ho jaata hai aur ussi se buffering badhti hai.
// Har 12 naye source-messages ke baad, purane cache entries khud-ba-khud
// saaf kar do - sirf sabse recent 3 rakho (jo abhi user dekh sakta hai),
// baaki hata do.
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
// SEQUENTIAL QUEUE - fixes Notes/DPP (or any batch of forwards)
// getting mixed up when several messages are forwarded together.
// Only ONE message is in-flight with ChatGPT at a time; the next one
// is sent only after the current one's real (non-"सोच...") reply lands.
// -------------------------------------------------------------
const messageQueue = [];
let isProcessingQueue = false;
let currentMediaInfo = null;   // media info for whichever item is in-flight right now
let resolveCurrentReply = null; // resolves once the real ChatGPT answer for the in-flight item arrives

function enqueueSourceMessage(item) {
  messageQueue.push(item);
  console.log(`📥 [QUEUE] Add hua ID=${item.msgId} | queue length ab: ${messageQueue.length}`);
  if (!isProcessingQueue) {
    processQueue().catch((e) => {
      console.error("❌ [QUEUE] processQueue crash hua:", e);
      isProcessingQueue = false; // safety - don't let the queue get stuck forever
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

      // Wait for the real (non-thinking) reply before moving to the next
      // queued item - this is what keeps Notes/DPP/lecture links from
      // ever getting attached to the wrong message.
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
// MANUAL VIDEO QUALITY (Low / Medium / HD) - person ko manually kam ya
// zyada quality chunne ka control chahiye tha. HD = seedha original
// Telegram file (jaisa pehle se chal raha tha, koi transcode nahi).
// Low/Medium ek baar ffmpeg se chhoti resolution/bitrate mein transcode
// karke disk pe cache ho jaate hain - taaki usi video ko dubara us
// quality par kholne par turant (bina dobara transcode kiye) mil jaaye.
// -------------------------------------------------------------
const QUALITY_PRESETS = {
  low: { height: 360, videoBitrate: "500k", audioBitrate: "64k" },
  medium: { height: 480, videoBitrate: "900k", audioBitrate: "96k" },
};
// Render ke free tier par bahut badi file transcode karna disk/time ke
// hisaab se risky hai - itni badi file ke liye seedha HD original bhej do.
const MAX_TRANSCODE_SOURCE_BYTES = 600 * 1024 * 1024; // 600MB

// jobKey (`${msgId}_${quality}`) -> in-flight Promise<string|null>, taaki
// ek hi video/quality ke liye do requests ek saath aayein to dono ek hi
// transcode job share karein, do baar kaam na ho.
const transcodeJobs = new Map();

function transcodedFilePath(msgId, quality) {
  return path.join(os.tmpdir(), `transcoded_${msgId}_${quality}.mp4`);
}

// Poori video Telegram se local disk par utaaro (chunk-by-chunk likhte
// hue, poori cheez RAM mein ek saath nahi rakhte) - ffmpeg ko faststart
// output banane ke liye ek seekable local input chahiye.
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

// Disk pe pehle se transcoded file ho to seedha wahi return karo. Nahi ho
// to ek hi baar (dedup ke saath) download+transcode karo aur cache kar do.
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

// Local (already-transcoded) file ko Range support ke saath serve karo -
// bilkul original /stream route jaisa behaviour, seeking bhi kaam karta hai.
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

// User ne mana kiya hai ki Render ke disk par kuch bhi download/cached
// padа na rahe - har video/thumbnail download ke liye hum apne temp
// files ek fixed naming pattern se banate hain (transcoded_, transcodesrc_,
// thumbsrc_, thumbout_). Normal flow mein ye sab apne-aap turant delete ho
// jaate hain (har function ke finally block mein) - ye cleanup sirf ek
// SAFETY NET hai un cases ke liye jab process crash ho jaaye ya koi
// unlink chhoot jaaye. Har 1 minute mein chalta hai aur 1 minute se
// purani kisi bhi hamari temp file ko hata deta hai - matlab transcoded
// quality cache bhi zyada se zyada ~1 minute tak hi disk par rehta hai
// (uske baad quality dobara switch karne par fresh transcode hoga).
const TEMP_FILE_PREFIXES = ["transcoded_", "transcodesrc_", "thumbsrc_", "thumbout_"];
const TEMP_FILE_MAX_AGE_MS = 60 * 1000; // 1 minute
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
cleanupTempFiles(); // startup par bhi ek baar chala do, purani reboot se bachi files saaf karne ke liye

// -------------------------------------------------------------
// THUMBNAIL (archive.org) - Telegram's embedded preview JPEG was unreliable
// (missing for a lot of videos -> nothing ever reached hosting/Firebase).
// So instead we pull the ACTUAL video's frame at the ~4-second mark
// (matches what the HTML already seeks to for its own client-side
// fallback) by downloading just the first chunk of bytes and running
// ffmpeg on it - no need to pull the whole video for a thumbnail.
// -------------------------------------------------------------
const thumbPromises = new Map(); // msgId -> Promise<string|null>

// -------------------------------------------------------------
// THUMBNAIL QUEUE - ek time pe sirf EK thumbnail generate hota hai.
// Pehle har naye video ke liye turant, saath-saath (parallel) download
// shuru ho jaata tha - agar Notes/DPP ki tarah 2-3 videos ek saath
// forward ho jaayein, sabke chunk downloads ek hi Telegram connection
// par ek saath maange jaate the. Ye contention hi timeouts (msgId=802
// jaisa "Sab attempts fail") ki asli wajah ban raha tha. Ab thumbnails
// ek sequential queue se, ek-ek karke process hote hain - naya video
// stream link turant milta hai (streamLink pehle hi bana diya jaata
// hai), sirf thumbnail image thoda der se aa sakti hai jab queue mein
// aage kaam ho.
// -------------------------------------------------------------
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

// Embedded thumb download ke liye ek attempt ka max time - agar Telegram
// se connection kahin stall ho jaaye, to hamesha ke liye latakne ki
// jagah itne time ke baad hi fail ho jaao.
const DOWNLOAD_ATTEMPT_TIMEOUT_MS = 15000;

function withTimeout(promise, ms, timeoutMessage) {
  let timedOut = false;
  const guarded = Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => { timedOut = true; reject(new Error(timeoutMessage)); }, ms)),
  ]);
  // Diagnostic: agar timeout fire hone ke BAAD bhi original request
  // eventually settle ho (success ya real error), wo yahan chhup nahi
  // jaana chahiye - warna hum hamesha generic "timeout" hi dekhte
  // rahenge aur asli wajah (jaise Telegram FLOOD_WAIT / rate-limit)
  // kabhi pata hi nahi chalegi.
  promise.then(
    () => { if (timedOut) console.log(`ℹ️ [THUMB-DIAG] Timeout ke baad request asal mein safal ho gaya tha - Telegram bas dheema tha, poori tarah band nahi tha.`); },
    (err) => { if (timedOut) console.error(`ℹ️ [THUMB-DIAG] Timeout ke baad asli underlying error mila:`, err.message); }
  );
  return guarded;
}

// Sirf video documents ke liye true - notes/DPP/PDF jaise non-video files
// ke liye thumbnail ki zaroorat hi nahi, unhe Screenshot-bot ko forward
// karna bhi bekaar hoga.
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
// SCREENSHOT-BOT (@screenshotit_bot) - video ko is bot ko forward karo,
// wo ek options-menu ke saath reply karta hai, usme "Get Thumbs" ek
// inline-keyboard button hai (message.click() se asal mein click/callback
// simulate karte hain, plain text nahi bhejte), aur uske baad wo asli
// video-frame se bani thumbnail photo bhejta hai. Ye Telegram ke apne
// embedded-thumb se zyada reliable hai (bahut saari videos mein embedded
// thumb hota hi nahi).
//
// Ek time pe sirf EK reply ka wait ho raha hota hai (thumbQueue pehle se
// sequential hai - ek waqt sirf ek hi video is pipeline mein hota hai),
// isliye bot ke replies kabhi mix nahi hote.
// -------------------------------------------------------------
const screenshotBotWaiters = []; // { predicate(message) => bool, resolve(message) }

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

// handleIncomingMessage se call hota hai jab bhi Screenshot-bot ka koi
// naya message aaye - jo bhi sabse pehla matching waiter mile use resolve
// kar do.
function handleScreenshotBotMessage(message) {
  for (let i = 0; i < screenshotBotWaiters.length; i++) {
    if (screenshotBotWaiters[i].predicate(message)) {
      const waiter = screenshotBotWaiters.splice(i, 1)[0];
      waiter.resolve(message);
      return;
    }
  }
}

const SCREENSHOT_MENU_TIMEOUT_MS = 30000; // bot options-menu bhejne mein itna time le sakta hai
const SCREENSHOT_PHOTO_TIMEOUT_MS = 90000; // bot ko thumbnail generate karne mein itna time lag sakta hai

// Message ke raw replyMarkup.rows[][].buttons se diye gaye text waala
// inline button dhoondh kar, seedha Telegram ke raw messages.GetBotCallbackAnswer
// API se asal "click" (callback query) bhejta hai - Telethon ke andar se
// bilkul waisa hi click jaisa app mein tap karne par hota hai. gramJS ke
// helper `message.click()` pe depend nahi karte (version ke hisaab se
// missing/unreliable ho sakta hai) - ye seedha low-level API call hai,
// isliye hamesha kaam karega jab tak button waqai inline-callback ho.
async function clickInlineButton(msg, buttonText) {
  const rows = msg.replyMarkup && msg.replyMarkup.rows;
  if (!rows || !rows.length) {
    console.error(`❌ [THUMB-BOT] msgId(menu)=${msg.id}: reply mein koi buttons hi nahi mile (replyMarkup empty).`);
    return null;
  }

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

  if (!targetButton) {
    console.error(`❌ [THUMB-BOT] msgId(menu)=${msg.id}: "${buttonText}" naam ka button nahi mila.`);
    return null;
  }
  if (!targetButton.data) {
    console.error(`❌ [THUMB-BOT] msgId(menu)=${msg.id}: "${buttonText}" button inline-callback nahi hai (data missing).`);
    return null;
  }

  return client.invoke(
    new Api.messages.GetBotCallbackAnswer({
      peer: screenshotEntity,
      msgId: msg.id,
      data: targetButton.data,
    })
  );
}

async function getThumbViaScreenshotBot(message) {
  if (!screenshotEntity) {
    console.log("⚠️ [THUMB-BOT] Screenshot bot resolve nahi hua tha - skip.");
    return null;
  }
  if (!sourceEntity) {
    console.log("⚠️ [THUMB-BOT] Source entity resolve nahi hua - skip.");
    return null;
  }

  try {
    // 1. Video ko Screenshot-bot ko forward karo
    await client.forwardMessages(screenshotEntity, {
      messages: [message.id],
      fromPeer: sourceEntity,
    });
    console.log(`📤 [THUMB-BOT] msgId=${message.id} Screenshot-bot ko forward kiya.`);

    // 2. Bot ke options-menu reply ka wait ("Choose one of the options.")
    const menuMsg = await waitForScreenshotBotMessage(SCREENSHOT_MENU_TIMEOUT_MS, (m) => {
      const t = (m.text || m.message || "").toLowerCase();
      return t.includes("choose one of the options") || t.includes("choose one");
    });
    if (!menuMsg) {
      console.error(`❌ [THUMB-BOT] msgId=${message.id}: options-menu ka reply nahi mila (${SCREENSHOT_MENU_TIMEOUT_MS / 1000}s).`);
      return null;
    }

    // 3. "Get Thumbs" button asal mein CLICK karo (raw callback API se,
    // koi text message NAHI bhejte - ye inline-keyboard button hai).
    const clickResult = await clickInlineButton(menuMsg, "Get Thumbs");
    if (!clickResult) {
      return null; // clickInlineButton ne already specific error log kar diya hai
    }
    console.log(`🖱️ [THUMB-BOT] msgId=${message.id}: "Get Thumbs" click kiya.`);

    // 4. Thumbnail photo ka wait
    const photoMsg = await waitForScreenshotBotMessage(SCREENSHOT_PHOTO_TIMEOUT_MS, (m) => !!m.photo);
    if (!photoMsg) {
      console.error(`❌ [THUMB-BOT] msgId=${message.id}: thumbnail photo ka reply nahi mila (${SCREENSHOT_PHOTO_TIMEOUT_MS / 1000}s).`);
      return null;
    }

    // 5. Photo download karo
    const buffer = await client.downloadMedia(photoMsg);
    if (buffer && buffer.length) {
      console.log(`✅ [THUMB-BOT] msgId=${message.id}: thumbnail photo mil gayi.`);
      return buffer;
    }
    return null;
  } catch (e) {
    console.error(`❌ [THUMB-BOT] Error msgId=${message.id}:`, e && e.stack ? e.stack : e);
    return null;
  }
}

// Primary: Screenshot-bot se real video-frame thumbnail mangwao. Wo fail
// ho (bot down, timeout, etc.) to Telegram ke apne embedded thumb (agar
// available ho) pe fallback karo, taaki thumbnail kabhi bhi poori tarah
// khaali na jaaye.
async function generateThumbFrame(message) {
  if (!isVideoDocument(message)) return null;

  const viaBot = await getThumbViaScreenshotBot(message);
  if (viaBot && viaBot.length) return viaBot;

  console.log(`⚠️ [THUMB] Screenshot-bot se nahi mila msgId=${message.id} - embedded thumb try kar rahe hain.`);

  // Kai videos (khaaskar documents/GIFs ki tarah bheji gayi files) mein
  // Telegram apna embedded thumbnail banata hi nahi - aise case mein
  // downloadMedia(thumb:-1) turant "nahi hai" bolne ki jagah poore 15
  // second latak kar hi timeout deta tha. Ab pehle hi thumbs array check
  // kar lete hain - agar khaali hai to bina wait kiye turant null.
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
    console.log(`⚠️ [THUMB] Embedded thumb khaali/nahi mila msgId=${message.id}`);
    return null;
  } catch (e) {
    console.error("❌ [THUMB] Embedded thumb download fail hui:", e.message);
    return null;
  }
}

// -------------------------------------------------------------
// THUMBNAIL HOSTING - archive.org (Internet Archive) IAS3 upload API se.
// Har thumbnail apna alag, globally-unique "item" banata hai (archive.org
// identifiers duplicate nahi ho sakte), aur upload ke turant baad
// https://archive.org/download/<identifier>/<filename> link se access
// hota hai. Note: archive.org kabhi-kabhi ImgBB jitna instant nahi hota -
// naya item banne ke turant baad ek-do second ka propagation delay ho
// sakta hai, isliye is function ke poora hone ke baad hi link Firebase
// mein save karo (jo pehle se ho raha hai).
// -------------------------------------------------------------
async function uploadToArchive(buffer, idPrefix) {
  if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) {
    console.log("⚠️ [ARCHIVE] ARCHIVE_ACCESS_KEY/ARCHIVE_SECRET_KEY set nahi hain - upload skip.");
    return null;
  }
  // archive.org identifier: sirf lowercase letters/digits/-/_ /., globally
  // unique honi chahiye - isliye timestamp + random hex jod diya.
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
      // Pehle koi timeout nahi tha - agar Render se archive.org tak
      // connection slow/blocked ho to axios bina kisi error ke hamesha
      // ke liye latak sakta tha (na success, na error, chup-chaap). Ab
      // 30 second ke baad khud hi fail ho jaayega taaki error dikhe aur
      // fallback (Telegram embedded thumb) turant try ho sake.
      timeout: 30000,
    });
    console.log(`✅ [ARCHIVE] Upload ban gaya: ${uploadUrl}`);
    return `https://archive.org/download/${identifier}/${filename}`;
  } catch (e) {
    const reason = e.code === "ECONNABORTED"
      ? "30s timeout - archive.org se connection slow/block ho raha hai"
      : (e.response ? JSON.stringify(e.response.data) : e.message);
    console.error(`❌ [ARCHIVE] Upload fail hui:`, reason);
    return null;
  }
}

function startThumbUpload(message) {
  if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) {
    console.log("⚠️ [THUMB] ARCHIVE_ACCESS_KEY/ARCHIVE_SECRET_KEY set nahi hain - thumbnail upload skip.");
    return;
  }
  const msgId = message.id;

  // thumbPromises mein turant (synchronously) ek pending promise daal do,
  // taaki koi aur code jo thumbPromises.get(msgId) check kare use turant
  // mil jaaye - chahe actual kaam abhi queue mein wait kar raha ho.
  let resolvePromise;
  const promise = new Promise((resolve) => { resolvePromise = resolve; });
  thumbPromises.set(msgId, promise);

  enqueueThumbJob(async () => {
    let result = null;
    try {
      // Overall safety timeout - Screenshot-bot flow (menu wait + "Get
      // Thumbs" + photo wait) khud hi ~120s tak le sakta hai, isliye ye
      // outer cap usse bada rakha hai (embedded-thumb fallback ke liye
      // thoda extra buffer ke saath) - taaki poori pipeline kabhi bina
      // kisi log/error ke hamesha ke liye latak na jaaye.
      const frameBuffer = await Promise.race([
        generateThumbFrame(message),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("generateThumbFrame 160s timeout - thumbnail pipeline atak gaya")), 160000)
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
      // Safety cleanup - agar ye thumb kabhi consume nahi hua (e.g. notes/dpp
      // message jiske liye thumb ki zaroorat hi nahi thi), map mein hamesha
      // ke liye na pada rahe.
      setTimeout(() => thumbPromises.delete(msgId), 5 * 60 * 1000);
    }
    resolvePromise(result);
  });
}

// Fire-and-forget pre-warm: resolve the source message ahead of time so
// the /stream route below doesn't have to do a fresh Telegram lookup on
// the person's first click - combined with the HTML's own preload, this
// is what makes playback start instantly instead of buffering.
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

// Jab caption khaali chhod di jaaye (sirf file forward ki gayi ho), to file
// ke apne naam se hi ek usable text bana lo - taaki ChatGPT ko kम se kम
// kuch to milta rahe tagging ke liye, "Media File" jaisa khaali text nahi.
// e.g. "समतल_में_गति_08_Concise_notes_XYZ.pdf" -> "समतल में गति 08 Concise notes XYZ"
function extractFileNameText(message) {
  try {
    if (message.media && message.media.document && message.media.document.attributes) {
      for (const attr of message.media.document.attributes) {
        if (attr.fileName) {
          return attr.fileName
            .replace(/\.[a-zA-Z0-9]{2,5}$/, "")   // extension hatao
            .replace(/[_\-]+/g, " ")              // underscores/dashes -> space
            .replace(/\s{2,}/g, " ")
            .trim();
        }
      }
    }
  } catch (e) { /* ignore - fallback string will be used instead */ }
  return "";
}

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

    // Use the pre-warmed cache when available - this is what makes repeat
    // requests (like the HTML's background preload, then the real click)
    // skip the Telegram round-trip entirely.
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

    // ?q=low ya ?q=medium -> manually chuni gayi chhoti quality. Query
    // hi nahi hai (ya q=high) -> hamesha jaisa original/HD passthrough.
    const requestedQuality = req.query.q;
    if (requestedQuality && QUALITY_PRESETS[requestedQuality]) {
      const localPath = await ensureTranscoded(message, msgId, requestedQuality);
      if (localPath) {
        return await serveLocalFile(localPath, req, res);
      }
      console.log(`⚠️ [QUALITY] msgId=${msgId} ke liye "${requestedQuality}" nahi mili - HD original bhej rahe hain.`);
      // fall through - neeche wala normal HD passthrough chalega
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

    // Notes/DPP links ab ?dl=1 ke saath aate hain -> force download.
    // Video stream links ke bina query param ke aate hain -> inline (player mein play).
    const forceDownload = req.query.dl === "1";
    const asciiFallback = fileName.replace(/[^\x20-\x7E]/g, "_");
    const encodedName = encodeURIComponent(fileName);
    const dispositionType = forceDownload ? "attachment" : "inline";
    const contentDisposition = `${dispositionType}; filename="${asciiFallback}"; filename*=UTF-8''${encodedName}`;

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
        "Content-Disposition": contentDisposition,
      });

      const stream = client.iterDownload({
        file: media,
        offset: bigInt(start),
        limit: chunkSize,
      });

      // IMPORTANT: res.write() ka return value check karna zaroori hai. Agar
      // client (mobile/slow network) itni tezi se data consume nahi kar
      // paa raha jitni tezi se Telegram se chunks aa rahe hain, to bina
      // is drain-wait ke Node internally sab kuch memory mein buffer
      // karta rehta hai - yahi 512MB OOM crash ("Ran out of memory") ki
      // sabse badi wajah thi, khaaskar poori/badi video file stream karte
      // waqt.
      for await (const chunk of stream) {
        if (!res.write(chunk)) {
          await new Promise((resolve) => res.once("drain", resolve));
        }
      }
      res.end();
    } else {
      // Full file request, still streamed in chunks (not buffered fully first)
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

// -------------------------------------------------------------
// 1b. CLIENT-SIDE THUMBNAIL FALLBACK
// Purane/thumb-less videos ke liye HTML pehli baar <video> tag se hi
// frame render karta hai (4s wale point tak seek karke). Jaise hi woh
// frame browser mein dikh jaata hai, HTML usi frame ko canvas se capture
// karke yahan bhej deta hai. Hum yahan se archive.org pe upload karte hain
// (bilkul waise hi jaise naye messages ke liye startThumbUpload karta
// hai) aur Firebase mein us entry ke thumb_link field mein permanently
// save kar dete hain - taaki agli baar se seedha archive.org link se
// load ho, video ko dobara seek karne ki zaroorat na pade.
// -------------------------------------------------------------
app.post("/thumb-fallback", async (req, res) => {
  try {
    const { image, subjectKey, chapterName, entryKey } = req.body || {};
    if (!image || !subjectKey || !chapterName || !entryKey) {
      return res.status(400).json({ error: "image, subjectKey, chapterName, entryKey zaroori hain" });
    }
    if (!ARCHIVE_ACCESS_KEY || !ARCHIVE_SECRET_KEY) {
      return res.status(500).json({ error: "ARCHIVE_ACCESS_KEY/ARCHIVE_SECRET_KEY set nahi hain" });
    }

    // "data:image/jpeg;base64,...." prefix (agar client ne canvas.toDataURL
    // se bheja hai) hata do - sirf raw base64/binary chahiye.
    const base64Image = String(image).replace(/^data:image\/\w+;base64,/, "");
    const imageBuffer = Buffer.from(base64Image, "base64");

    const safeEntryKey = String(entryKey).toLowerCase().replace(/[^a-z0-9]+/g, "-").slice(0, 40);
    const url = await uploadToArchive(imageBuffer, `labdesk-fallback-${safeEntryKey}`);
    if (!url) {
      console.error(`❌ [THUMB-FALLBACK] Archive.org se URL nahi mila, entryKey=${entryKey}`);
      return res.status(502).json({ error: "Archive.org upload se URL nahi mila" });
    }

    // Sirf usi entry ke thumb_link field ko set karo - baaki data (title,
    // stream_link, timestamp, etc.) chhede bina.
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
// 2. FIREBASE PUSH LOGIC
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

  // Split on "@" instead of whitespace, so multi-word tags
  // (e.g. "@वनस्पति जगत") are captured fully, not cut at the first space.
  const segments = replyText
    .split("@")
    .map((s) => s.trim())
    .filter(Boolean);

  const devanagariRegex = /[\u0900-\u097F]/; // Hindi script range

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
      // Hindi script tag => always the chapter name, regardless of position
      chapterName = seg;
    } else if (!subjectName) {
      // First remaining non-system, non-Hindi tag => subject
      subjectName = seg;
    }
  }

  subjectName = subjectName || "General";
  chapterName = chapterName || "General_Lectures";

  const subjectKey = subjectName.replace(/[.$#\[\]/]/g, "_");
  const chapterKey = chapterName.replace(/[.$#\[\]/]/g, "_");

  // ChatGPT ka reply sirf tags hota hai ("@Chemistry @साम्यावस्था @other
  // @Lec 08") - koi asli title/description text nahi hota. Isliye raw_reply
  // se @tags hata kar title banane ki koshish karne se sirf bacha-khucha
  // number ("08") milta tha. Ab tags se hi ek saaf, padhne-laayak title
  // seedha bana rahe hain.
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
      // ?dl=1 -> /stream route ab Content-Disposition: attachment bhejega,
      // isliye link par click karte hi seedha download shuru hoga, "view"
      // wale tab mein khulne (aur phir device mein load hone) ki jagah.
      dataPayload["download_link"] = `${mediaInfo.stream_link}?dl=1`;
    } else {
      dataPayload["stream_link"] = mediaInfo.stream_link;

      // Thumb upload started the moment the message arrived, in parallel
      // with the ChatGPT round-trip above. Screenshot-bot ka poora flow
      // (forward -> menu wait -> "Get Thumbs" -> photo wait) ~2 minute
      // tak le sakta hai, isliye ab yahan bhi utna hi wait karte hain -
      // warna thumb_link is Firebase push mein kabhi include hi nahi
      // hota (baad mein koi separate update mechanism nahi hai jo use
      // add kare). NOTE: message-queue apna khud ka 60s safety-timeout
      // pehle se rakhta hai (waitForChatGPTReply), isliye agar thumb ko
      // yahan se zyada time lage to bhi queue agle item pe khud-ba-khud
      // badh jaati hai - is wait ka asar sirf isi push mein thumb_link
      // shaamil hone/na hone par padta hai, poori queue par nahi.
      if (mediaInfo.msg_id && thumbPromises.has(mediaInfo.msg_id)) {
        const thumbUrl = await Promise.race([
          thumbPromises.get(mediaInfo.msg_id),
          new Promise((resolve) => setTimeout(() => resolve(null), 150000)),
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
      return true; // still a real (non-thinking) reply - don't re-block the queue on a Firebase hiccup
    }
  } catch (err) {
    console.error(`❌ Firebase Exception:`, err.response ? err.response.data : err.message);
    return true; // same reasoning - the reply itself was final, only the push failed
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

    // IMPORTANT: message.chatId aur message.senderId SYNCHRONOUS getters hain
    // (koi network call nahi) - ye raw edit updates par bhi reliably kaam karte
    // hain. Purana code await message.getChat()/getSender() use karta tha, jo
    // network-fetch pe depend karta hai aur EDIT event par blank/fail ho jaata
    // tha - isi wajah se ChatGPT ka final (edited) jawab silently drop ho raha
    // tha aur Firebase tak nahi pahunchta tha.
    const chatIdStr = message.chatId ? message.chatId.toString() : "";
    const senderIdSync = message.senderId ? message.senderId.toString() : "";

    // Debug/logging ke liye purana getChat/getSender bhi try karo, lekin
    // isse koi core-logic decision mat lo - agar fail ho to bhi flow rukna
    // nahi chahiye.
    let chatUsername = "", chatTitle = "", senderUsername = "";
    try {
      const chat = await message.getChat();
      chatUsername = (chat && chat.username ? chat.username : "").toLowerCase();
      chatTitle = (chat && chat.title ? chat.title : "").toLowerCase();
    } catch (e) { /* ignore - sirf logging ke liye tha */ }
    try {
      const sender = await message.getSender();
      senderUsername = (sender && sender.username ? sender.username : "").toLowerCase();
    } catch (e) { /* ignore - sirf logging ke liye tha */ }

    console.log(`🔍 DEBUG chat=${chatUsername} title=${chatTitle} sender=${senderUsername} chatId=${chatIdStr} senderId=${senderIdSync} msgId=${message.id} isEdited=${!!message.editDate} editDate=${message.editDate || "N/A"}`);

    // A. Source Channel Check - resolved numeric ID se compare karo (reliable),
    // username/title text-match ko fallback ke taur pe rakha hai.
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
        preWarmStream(message.id); // fire-and-forget, don't block the queue on this
        startThumbUpload(message); // fire-and-forget - runs in parallel with the ChatGPT round-trip below
      }

      const msgText = currentText || extractFileNameText(message) || "Media File";
      if (!currentText) {
        console.log(`📎 Caption khaali thi - istemal kiya gaya text: "${msgText}"`);
      }

      // Har message ab QUEUE mein jaata hai - ek time pe sirf ek hi
      // ChatGPT ke paas jaata hai, taaki Notes/DPP/lecture ek saath
      // forward karne par unke links aapas mein mix na ho.
      enqueueSourceMessage({ msgId: message.id, streamLink, text: msgText });
      console.log(`➡️ [STEP 2] Queue mein daal diya gaya (position ke hisaab se process hoga)`);
      return;
    }

    // B. ChatGPT Response Check - matched reliably via resolved bot ID.
    // senderIdSync (synchronous getter) is used here specifically because it
    // still resolves correctly on EDIT events, unlike the old getSender()
    // based value which came back blank on edits.
    const isChatGPT = chatgptBotId && senderIdSync === chatgptBotId;

    if (isChatGPT) {
      console.log(`\n🤖 [STEP 3] ChatGPT Response Detect Hua: "${currentText}"`);
      // currentMediaInfo hamesha sirf USI item ki jaankari rakhta hai jo
      // is waqt queue mein process ho raha hai - isliye link kabhi mix
      // nahi hota, chahe kitne bhi messages ek saath forward kiye ho.
      const mediaInfo = currentMediaInfo || {};
      const wasFinalAnswer = await processReplyAndPushToFirebase(currentText, mediaInfo);

      if (wasFinalAnswer && resolveCurrentReply) {
        resolveCurrentReply(); // ab queue agle item pe badh sakti hai
      }
    }

    // C. Screenshot-bot Response Check - iske messages sirf getThumbViaScreenshotBot
    // ke andar waale waiters ko resolve karte hain, koi aur core logic nahi.
    const isScreenshotBot = screenshotBotId && senderIdSync === screenshotBotId;
    if (isScreenshotBot) {
      handleScreenshotBotMessage(message);
    }

  } catch (err) {
    console.error("❌ Event Handler Error:", err);
  }
}

// -------------------------------------------------------------
// 4. SERVER STARTUP
// -------------------------------------------------------------
// Render ka free instance ~15 min inactivity ke baad spin-down ho jaata hai,
// aur agla request 50+ second le leta hai (cold start) - isi wajah se pehli
// baar video load hone mein 20+ second lag rahe the aur user bhaag jaata tha.
// Har 10 minute mein khud ko hi ek chhota ping bhej kar instance ko "active"
// dikhaya jaata hai, taaki active hours mein spin-down hi na ho.
// NOTE: ye sirf active-usage window mein help karta hai - agar poori raat/
// din koi bhi traffic na aaye to Render phir bhi spin-down kar sakta hai.
// Uss case ke liye external cron (UptimeRobot / cron-job.org) ya paid plan
// zaroori hoga - code se poori tarah fix nahi ho sakta.
function startKeepAlivePing() {
  setInterval(() => {
    axios.get(RENDER_URL).catch(() => {
      // ping fail hone par bhi kuch nahi karna - agla interval try karega
    });
  }, 10 * 60 * 1000);
}

async function startServer() {
  // 1. Render Port binding
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Server listening on 0.0.0.0:${PORT}`);
    startKeepAlivePing();
  });

  // 2. Connect Telegram Client
  try {
    await client.connect();
    console.log("✅ Telegram Client Connected!");

    // Resolve the ChatGPT bot's numeric ID (and cache the full entity) once
    try {
      chatgptEntity = await client.getEntity(CHATGPT_BOT);
      chatgptBotId = chatgptEntity.id.toString();
      console.log("🤖 ChatGPT Bot ID resolved:", chatgptBotId);
    } catch (e) {
      console.error("❌ ChatGPT Bot ID resolve nahi hua:", e.message);
    }

    // Resolve the source channel's numeric ID (and cache the full entity)
    // once - used for reliable matching AND reused by preWarmStream/stream
    // route so they never need a redundant Telegram lookup.
    try {
      sourceEntity = await client.getEntity(SOURCE_CHAT);
      sourceChatId = sourceEntity.id.toString();
      console.log("📡 Source Chat ID resolved:", sourceChatId);
    } catch (e) {
      console.error("❌ Source Chat ID resolve nahi hua:", e.message);
    }

    // Resolve the Screenshot-bot's numeric ID (and cache the full entity)
    // once - used to detect its replies for the video-thumbnail pipeline.
    try {
      screenshotEntity = await client.getEntity(SCREENSHOT_BOT);
      screenshotBotId = screenshotEntity.id.toString();
      console.log("📸 Screenshot Bot ID resolved:", screenshotBotId);
    } catch (e) {
      console.error("❌ Screenshot Bot ID resolve nahi hua:", e.message);
    }

    // New messages
    client.addEventHandler(handleIncomingMessage, new NewMessage({}));

    // Edited messages (ChatGPT bot edits its "सोच..." placeholder into the
    // final tagged answer, it does NOT send a new message). Listening on
    // raw updates directly is more reliable than the EditedMessage event
    // class, which can silently fail to register depending on library version.
    client.addEventHandler(async (update) => {
      try {
        if (
          update.className === "UpdateEditMessage" ||
          update.className === "UpdateEditChannelMessage"
        ) {
          console.log("✏️ Raw Edit Update Detect Hua, message process kar rahe hain...");
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
