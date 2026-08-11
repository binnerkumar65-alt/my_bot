process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (err) => {
  console.error('❌ Uncaught Exception:', err);
});

const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const express = require("express");
const axios = require("axios");
const bigInt = require("big-integer");
const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");

// 🎬 FFMPEG PATH
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

const videoLocationCache = new Map();
const messageCache = new Map();
const chatgptSentToOriginal = new Map();
const typeCheckerSentToOriginal = new Map();

// 📂 DIRS SETUPS
const VIDEO_CACHE_DIR = path.join(os.tmpdir(), 'video-chunk-cache');
const TRANSCODE_DIR = path.join(os.tmpdir(), "video-transcode-360p");

[VIDEO_CACHE_DIR, TRANSCODE_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// ============================================================
// 🚨 🔥 30-SECOND SUPER AGGRESSIVE DISK & RAM CLEANER 🔥
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

          // 1. अगर फ़ाइल 30 सेकेंड से पुरानी है, चाहे प्रोसेस सफल हुआ हो या क्रैश/फ़ेल — डिलीट कर दो
          const isStale = (now - stats.mtimeMs) > 30 * 1000;
          
          // 2. या अगर फ़ाइल अधूरी/फ़ेल ट्रांसकोड की अस्थायी फ़ाइल है (.tmp, transcoded_, etc)
          const isTempFile = file.startsWith("transcoded_") || 
                             file.startsWith("transcodesrc_") || 
                             file.startsWith("thumbsrc_") || 
                             file.endsWith(".chunk") || 
                             file.endsWith(".tmp");

          if (isStale || isTempFile) {
            fs.unlink(filePath, (unlinkErr) => {
              if (!unlinkErr) {
                console.log(`🗑️ [DISK-CLEANER 30s] Purani/Failed file delete ho gayi: ${file}`);
              }
            });
          }
        });
      });
    });
  });

  // Garbage collection (RAM clear)
  if (global.gc) {
    global.gc();
  }
}

// ⏱️ हर 30 सेकेंड में डिस्क खाली करने का कोड
setInterval(forceCleanAllTempFiles, 30 * 1000);

// ============================================================
// 🔢 RESET COUNTER API (0 से शुरुआत करने के लिए)
// ============================================================
let tagMsgCount = 0;

// आप ब्राउज़र में https://my-bot-kgrk.onrender.com/reset-counter खोलकर काउंटर को 0 कर सकते हैं
app.get("/reset-counter", async (req, res) => {
  try {
    tagMsgCount = 0;
    await axios.put(`${FIREBASE_BASE_URL.replace(/\/$/, "")}/Meta.json`, { tagMsgCount: 0 });
    console.log("🔄 [RESET] Counter Firebase aur Bot mein 0 kar diya gaya.");
    res.send("✅ Counter and ID history successfully reset to 0!");
  } catch (e) {
    res.status(500).send("❌ Reset failed: " + e.message);
  }
});

async function loadTagMsgCount() {
  try {
    const res = await axios.get(`${FIREBASE_BASE_URL.replace(/\/$/, "")}/Meta.json`);
    if (res.data && typeof res.data === "object" && typeof res.data.tagMsgCount === "number") {
      tagMsgCount = res.data.tagMsgCount;
    } else {
      tagMsgCount = 0;
    }
  } catch (e) {
    tagMsgCount = 0;
  }
}

// ============================================================
// STREAM ENDPOINT & BOT SERVER
// ============================================================
app.get("/", (req, res) => res.send("Bot Active - Auto 30s Disk Cleaning Enabled"));

app.get("/stream/:msgId", async (req, res) => {
  const msgId = parseInt(req.params.msgId);
  const transcodedPath = path.join(TRANSCODE_DIR, `transcoded_${msgId}.mp4`);

  if (fs.existsSync(transcodedPath)) {
    const stat = fs.statSync(transcodedPath);
    res.writeHead(200, { "Content-Type": "video/mp4", "Content-Length": stat.size });
    return fs.createReadStream(transcodedPath).pipe(res);
  }

  return res.status(404).send("Stream source currently initializing or cleaned up.");
});

app.listen(PORT, async () => {
  console.log(`🚀 Server running on port ${PORT}`);
  await loadTagMsgCount();
  try {
    await client.start();
    console.log("🤖 Telegram Client Connected!");
  } catch (err) {
    console.error("❌ Telegram Client connection failed:", err.message);
  }
});
