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
const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawn } = require("child_process");
const ffmpegPath = require("ffmpeg-static");

const app = express();
const PORT = process.env.PORT || 10000;
app.use(express.json({ limit: "8mb" })); // client se aane waale base64 thumbnail frame ke liye

// Environment Variables Configuration
const apiId = parseInt(process.env.API_ID || "0");
const apiHash = process.env.API_HASH || "";
const stringSession = new StringSession(process.env.SESSION_STRING || "");

const SOURCE_CHAT = "@sxhckfufig";
const CHATGPT_BOT = "@chatgpt";
const FIREBASE_BASE_URL = "https://newfire-2258c-default-rtdb.firebaseio.com";
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || "https://my-bot-kgrk.onrender.com";
const IMGBB_API_KEY = process.env.IMGBB_API_KEY || "";

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

// Bahut purani transcoded cache files ko disk se saaf karte raho, warna
// Render ke free tier ka seemit disk bhar sakta hai.
const TRANSCODE_CACHE_MAX_AGE_MS = 3 * 60 * 60 * 1000; // 3 hours
function cleanupTranscodeCache() {
  fs.readdir(os.tmpdir(), (err, files) => {
    if (err) return;
    const now = Date.now();
    files
      .filter((f) => f.startsWith("transcoded_"))
      .forEach((f) => {
        const p = path.join(os.tmpdir(), f);
        fs.stat(p, (statErr, stats) => {
          if (statErr) return;
          if (now - stats.mtimeMs > TRANSCODE_CACHE_MAX_AGE_MS) {
            fs.unlink(p, () => {});
          }
        });
      });
  });
}
setInterval(cleanupTranscodeCache, 30 * 60 * 1000);

// -------------------------------------------------------------
// THUMBNAIL (ImgBB) - Telegram's embedded preview JPEG was unreliable
// (missing for a lot of videos -> nothing ever reached ImgBB/Firebase).
// So instead we pull the ACTUAL video's frame at the ~4-second mark
// (matches what the HTML already seeks to for its own client-side
// fallback) by downloading just the first chunk of bytes and running
// ffmpeg on it - no need to pull the whole video for a thumbnail.
// -------------------------------------------------------------
const thumbPromises = new Map(); // msgId -> Promise<string|null>

// Sirf video ke shuru ka itna hissa download karo jitna 4-second-mark
// tak ka frame nikaalne ke liye kaafi ho. Telegram/streaming-optimized
// mp4 mein moov atom aam taur pe shuru mein hi hota hai (isi wajah se
// /stream route pe seek turant kaam karta hai), isliye chhota chunk
// bhi ffmpeg ke liye decode karne ke liye kaafi hota hai.
async function downloadVideoChunk(message, maxBytes) {
  const doc = message.media && message.media.document;
  const fileSize = doc ? Number(doc.size) || 0 : 0;
  const limit = fileSize ? Math.min(fileSize, maxBytes) : maxBytes;

  const chunks = [];
  const stream = client.iterDownload({
    file: message.media,
    offset: bigInt(0),
    limit,
  });
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

// videoBuffer ke andar seekSeconds waale point ka frame nikaal kar
// JPEG buffer wapas karta hai. Temp files hamesha cleanup ho jaati hain,
// chahe ffmpeg fail ho jaaye.
function extractFrame(videoBuffer, seekSeconds) {
  const uid = `${Date.now()}_${Math.random().toString(36).slice(2)}`;
  const videoPath = path.join(os.tmpdir(), `thumbsrc_${uid}.mp4`);
  const framePath = path.join(os.tmpdir(), `thumbout_${uid}.jpg`);

  return (async () => {
    await fs.promises.writeFile(videoPath, videoBuffer);
    try {
      await new Promise((resolve, reject) => {
        const args = [
          "-y",
          "-ss", String(seekSeconds),
          "-i", videoPath,
          "-frames:v", "1",
          "-q:v", "2",
          framePath,
        ];
        const proc = spawn(ffmpegPath, args);
        let stderr = "";
        proc.stderr.on("data", (d) => (stderr += d.toString()));
        proc.on("error", reject);
        proc.on("close", (code) => {
          if (code === 0) resolve();
          else reject(new Error(`ffmpeg exited ${code}: ${stderr.slice(-300)}`));
        });
      });
      return await fs.promises.readFile(framePath);
    } finally {
      fs.promises.unlink(videoPath).catch(() => {});
      fs.promises.unlink(framePath).catch(() => {});
    }
  })();
}

// Chhote chunk se try karo, phir bade chunk se, phir earlier timestamps
// pe (agar video 4 second se chhota nikla). Sab fail ho to Telegram ke
// apne embedded preview thumb pe fallback karo - taaki thumbnail kabhi
// bhi poori tarah khaali na jaaye.
async function generateThumbFrame(message) {
  const attempts = [
    { bytes: 3 * 1024 * 1024, seek: 4 },
    { bytes: 8 * 1024 * 1024, seek: 4 },
    { bytes: 8 * 1024 * 1024, seek: 1 },
    { bytes: 8 * 1024 * 1024, seek: 0 },
  ];

  for (const attempt of attempts) {
    try {
      const videoBuffer = await downloadVideoChunk(message, attempt.bytes);
      if (!videoBuffer || !videoBuffer.length) continue;
      const frame = await extractFrame(videoBuffer, attempt.seek);
      if (frame && frame.length) return frame;
    } catch (e) {
      console.error(`⚠️ [THUMB] ffmpeg attempt (bytes=${attempt.bytes}, seek=${attempt.seek}) fail hui:`, e.message);
    }
  }

  console.log("⚠️ [THUMB] Sab ffmpeg attempts fail - Telegram ke embedded thumb pe fallback kar rahe hain.");
  try {
    return await client.downloadMedia(message, { thumb: -1 });
  } catch (e) {
    console.error("❌ [THUMB] Embedded thumb fallback bhi fail hui:", e.message);
    return null;
  }
}

function startThumbUpload(message) {
  if (!IMGBB_API_KEY) {
    console.log("⚠️ [THUMB] IMGBB_API_KEY set nahi hai - thumbnail upload skip.");
    return;
  }
  const msgId = message.id;
  const promise = (async () => {
    try {
      const frameBuffer = await generateThumbFrame(message);
      if (!frameBuffer || !frameBuffer.length) {
        console.error(`❌ [THUMB] Koi bhi frame nahi mila msgId=${msgId}`);
        return null;
      }

      const params = new URLSearchParams();
      params.append("key", IMGBB_API_KEY);
      params.append("image", frameBuffer.toString("base64"));

      // ImgBB apni anti-bot layer se Render jaise cloud/datacenter IPs se
      // aane waale plain axios requests ko block kar deta hai
      // ("You have been forbidden to use this website.", code 103) - isko
      // bypass karne ke liye normal browser jaisa User-Agent bhejte hain.
      const res = await axios.post("https://api.imgbb.com/1/upload", params, {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        },
      });
      const url = res.data && res.data.data && res.data.data.url;
      if (url) {
        console.log(`🖼️ [THUMB] ImgBB upload OK msgId=${msgId}: ${url}`);
      } else {
        console.error(`❌ [THUMB] ImgBB response mein URL nahi mila, msgId=${msgId}`);
      }
      return url || null;
    } catch (e) {
      console.error(`❌ [THUMB] Upload fail hui msgId=${msgId}:`, e.response ? e.response.data : e.message);
      return null;
    } finally {
      // Safety cleanup - agar ye thumb kabhi consume nahi hua (e.g. notes/dpp
      // message jiske liye thumb ki zaroorat hi nahi thi), map mein hamesha
      // ke liye na pada rahe.
      setTimeout(() => thumbPromises.delete(msgId), 5 * 60 * 1000);
    }
  })();
  thumbPromises.set(msgId, promise);
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
        "Content-Disposition": contentDisposition,
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
// 1b. CLIENT-SIDE THUMBNAIL FALLBACK
// Purane/thumb-less videos ke liye HTML pehli baar <video> tag se hi
// frame render karta hai (4s wale point tak seek karke). Jaise hi woh
// frame browser mein dikh jaata hai, HTML usi frame ko canvas se capture
// karke yahan bhej deta hai. Hum yahan se ImgBB pe upload karte hain
// (bilkul waise hi jaise naye messages ke liye startThumbUpload karta
// hai) aur Firebase mein us entry ke thumb_link field mein permanently
// save kar dete hain - taaki agli baar se seedha ImgBB link se load ho,
// video ko dobara seek karne ki zaroorat na pade.
// -------------------------------------------------------------
app.post("/thumb-fallback", async (req, res) => {
  try {
    const { image, subjectKey, chapterName, entryKey } = req.body || {};
    if (!image || !subjectKey || !chapterName || !entryKey) {
      return res.status(400).json({ error: "image, subjectKey, chapterName, entryKey zaroori hain" });
    }
    if (!IMGBB_API_KEY) {
      return res.status(500).json({ error: "IMGBB_API_KEY set nahi hai" });
    }

    // "data:image/jpeg;base64,...." prefix (agar client ne canvas.toDataURL
    // se bheja hai) hata do - ImgBB ko sirf raw base64 chahiye.
    const base64Image = String(image).replace(/^data:image\/\w+;base64,/, "");

    const params = new URLSearchParams();
    params.append("key", IMGBB_API_KEY);
    params.append("image", base64Image);

    const imgbbRes = await axios.post("https://api.imgbb.com/1/upload", params, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      },
    });
    const url = imgbbRes.data && imgbbRes.data.data && imgbbRes.data.data.url;
    if (!url) {
      console.error(`❌ [THUMB-FALLBACK] ImgBB response mein URL nahi mila, entryKey=${entryKey}`);
      return res.status(502).json({ error: "ImgBB upload se URL nahi mila" });
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
      // with the ChatGPT round-trip above - so by now it has almost always
      // already finished. Wait a little just in case, but never block the
      // Firebase push for long.
      if (mediaInfo.msg_id && thumbPromises.has(mediaInfo.msg_id)) {
        // Timeout badhaya gaya (8s -> 20s) - ab hum chunk download + ffmpeg
        // se real frame nikaal rahe hain, jo embedded-thumb download se
        // dheema hota hai, isliye purana 8s timeout thumb ko beech mein
        // hi kaat deta tha.
        const thumbUrl = await Promise.race([
          thumbPromises.get(mediaInfo.msg_id),
          new Promise((resolve) => setTimeout(() => resolve(null), 20000)),
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
