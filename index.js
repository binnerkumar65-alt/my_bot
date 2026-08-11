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

// 🎬 FFMPEG PATH Setup
let ffmpegPath = "ffmpeg";
try {
  ffmpegPath = require("ffmpeg-static") || "ffmpeg";
  console.log(`🎬 [FFMPEG] Static binary mil gaya: ${ffmpegPath}`);
} catch (e) {
  console.log(`⚠️ [FFMPEG] ffmpeg-static package nahi mila, system "ffmpeg" try karenge.`);
}

const app = express();
const PORT = process.env.PORT || 10000;
app.use(express.json({ limit: "2mb" }));

const apiId = parseInt(process.env.API_ID || "0");
const apiHash = process.env.API_HASH || "";
const stringSession = new StringSession(process.env.SESSION_STRING || "");

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

let chatgptEntity = null;
let typeCheckerEntity = null;
let screenshotEntity = null;

const sourceEntities = new Map();
const videoLocationCache = new Map();
const messageCache = new Map();
const chatgptSentToOriginal = new Map();
const typeCheckerSentToOriginal = new Map();

// 📁 DIRECTORIES SETUP
const VIDEO_CACHE_DIR = path.join(os.tmpdir(), 'video-chunk-cache');
const TRANSCODE_DIR = path.join(os.tmpdir(), "video-transcode-360p");

[VIDEO_CACHE_DIR, TRANSCODE_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// ============================================================
// 🚨 🔥 30-SECOND AGGRESSIVE DISK CLEANUP (FOR ALL STALE/FAILED FILES)
// ============================================================
function forceCleanAllTempFiles() {
  const now = Date.now();
  const dirsToScan = [os.tmpdir(), VIDEO_CACHE_DIR, TRANSCODE_DIR];

  dirsToScan.forEach((dir) => {
    fs.readdir(dir, (err, files) => {
      if (err || !files) return;

      files.forEach((file) => {
        const filePath = path.join(dir, file);

        fs.stat(filePath, (statErr, stats) => {
          if (statErr) return;

          // 30 sec se purani file ya failed temp files ko turant uda do
          const isStale = (now - stats.mtimeMs) > 30 * 1000;
          const isTempFile = file.startsWith("transcoded_") || 
                             file.startsWith("transcodesrc_") || 
                             file.startsWith("thumbsrc_") || 
                             file.endsWith(".chunk") || 
                             file.endsWith(".tmp");

          if (isStale || isTempFile) {
            fs.unlink(filePath, (unlinkErr) => {
              if (!unlinkErr) {
                console.log(`🗑️ [30s DISK-CLEANER] Auto Deleted: ${file}`);
              }
            });
          }
        });
      });
    });
  });

  if (global.gc) global.gc();
}
setInterval(forceCleanAllTempFiles, 30 * 1000);

// ============================================================
// 🔢 COUNTER & RESET SYSTEM
// ============================================================
let tagMsgCount = 0;

async function loadTagMsgCount() {
  try {
    const res = await axios.get(`${FIREBASE_BASE_URL.replace(/\/$/, "")}/Meta.json`);
    if (res.data && typeof res.data === "object" && typeof res.data.tagMsgCount === "number") {
      tagMsgCount = res.data.tagMsgCount;
    } else {
      tagMsgCount = 0;
    }
    console.log(`🔢 [RULE-REMINDER] Counter Loaded: ${tagMsgCount}`);
  } catch (e) {
    tagMsgCount = 0;
  }
}

app.get("/reset-counter", async (req, res) => {
  try {
    tagMsgCount = 0;
    await axios.put(`${FIREBASE_BASE_URL.replace(/\/$/, "")}/Meta.json`, { tagMsgCount: 0 });
    console.log("🔄 [RESET] Counter Firebase aur Bot me 0 ho gaya hai.");
    res.send("✅ Counter & History successfully reset to 0!");
  } catch (e) {
    res.status(500).send("❌ Reset failed: " + e.message);
  }
});

// ============================================================
// 📡 TELEGRAM CHANNEL LISTENER (मैसेज उठाने का मेन लॉजिक)
// ============================================================
async function initTelegramListeners() {
  try {
    chatgptEntity = await client.getEntity(CHATGPT_BOT).catch(() => null);
    typeCheckerEntity = await client.getEntity(TYPE_CHECKER_BOT).catch(() => null);
    screenshotEntity = await client.getEntity(NEW_SCREENSHOT_BOT).catch(() => null);

    for (const chatName of SOURCE_CHATS) {
      try {
        const entity = await client.getEntity(chatName);
        const chatIdStr = entity.id.toString();
        sourceEntities.set(chatIdStr, entity);
        console.log(`✅ [CHANNEL-CONNECTED] Listening to ${chatName} (ID: ${chatIdStr})`);
      } catch (err) {
        console.error(`❌ Channel connect error (${chatName}):`, err.message);
      }
    }

    // New Message Event
    client.addEventHandler(async (event) => {
      const message = event.message;
      if (!message) return;

      const chatIdStr = message.peerId?.channelId?.toString() || message.peerId?.chatId?.toString();
      
      // Check if message is from configured channels
      if (sourceEntities.has(chatIdStr)) {
        const msgId = message.id;
        console.log(`📩 [NEW-MESSAGE] Channel se message aaya! ID=${msgId}`);

        videoLocationCache.set(msgId, chatIdStr);
        messageCache.set(msgId, message);

        // Firebase Pending Node me entry
        const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${msgId}.json`;
        const payload = {
          msg_id: msgId,
          stream_link: `${RENDER_URL}/stream/${msgId}`,
          timestamp: { ".sv": "timestamp" },
          content_type: "@video"
        };

        axios.put(pendingUrl, payload).catch(e => console.error("Firebase write err:", e.message));
      }
    }, new NewMessage({}));

  } catch (e) {
    console.error("❌ Listener Init Error:", e.message);
  }
}

// ============================================================
// 🚀 EXPRESS ROUTES
// ============================================================
app.get("/", (req, res) => res.send("Bot Active - Channel Listening ON & 30s Disk Cleanup Ready"));

app.get("/stream/:msgId", async (req, res) => {
  const msgId = parseInt(req.params.msgId);
  const transcodedPath = path.join(TRANSCODE_DIR, `transcoded_${msgId}.mp4`);

  if (fs.existsSync(transcodedPath)) {
    const stat = fs.statSync(transcodedPath);
    res.writeHead(200, { "Content-Type": "video/mp4", "Content-Length": stat.size });
    return fs.createReadStream(transcodedPath).pipe(res);
  }

  return res.status(404).send("Video file missing or auto-cleaned.");
});

// START SERVER & CONNECT TELEGRAM
app.listen(PORT, async () => {
  console.log(`🚀 Server running on port ${PORT}`);
  await loadTagMsgCount();
  try {
    await client.start();
    console.log("🤖 Telegram Client Connected!");
    await initTelegramListeners();
  } catch (err) {
    console.error("❌ Telegram connection failed:", err.message);
  }
});
