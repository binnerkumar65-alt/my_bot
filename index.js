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

let chatgptBotIdStr = null;
let chatgptEntity = null;
let typeCheckerBotIdStr = null;
let typeCheckerEntity = null;
let screenshotBotIdStr = null;
let screenshotEntity = null;

const sourceEntities = new Map();
const sourceChatIdStrs = new Set();

const messageCache = new Map();
const thumbPromises = new Map();
const finalizedDocPaths = new Map();
const pendingTimestamps = new Map();

const screenshotRequestQueue = [];
const screenshotResults = new Map();

const chatgptSentToOriginal = new Map();
const typeCheckerSentToOriginal = new Map();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function clearMemory() {
  try {
    messageCache.clear();
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
    timestamp: staged.timestamp || Date.now(),
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

    // Path store kar liya taaki thumnail upload late hone par yahan patch ho
    finalizedDocPaths.set(msgId, { subject: subjectName, chapter: chapterName });
    setTimeout(() => finalizedDocPaths.delete(msgId), 30 * 60 * 1000);

    // ✅ Pending Node ko complete delete kar do
    await axios.delete(pendingUrl);
    pendingTimestamps.delete(msgId);
    console.log(`🗑️ [FIREBASE] Successfully deleted Pending entry for msgId=${msgId}`);
  } catch (e) {
    console.error("❌ [FIREBASE] Final write error:", e.response?.data || e.message);
  }
}

async function getThumbViaScreenshotBot(msgId, streamLink) {
  if (!screenshotEntity) return null;

  try {
    screenshotRequestQueue.push(msgId);
    screenshotResults.delete(msgId);

    await client.sendMessage(screenshotEntity, { message: streamLink });
    console.log(`📤 [THUMB-BOT] (msgId=${msgId}) @screenshort17_bot ko URL bhej diya: ${streamLink}`);

    let waitCount = 0;
    while (!screenshotResults.has(msgId) && waitCount < 90) {
      await sleep(1000);
      waitCount++;
    }

    const photoMsg = screenshotResults.get(msgId);
    screenshotResults.delete(msgId);
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

        // ✅ Agar Final node create ho chuki hai, to wahan patch karein
        const finalPath = finalizedDocPaths.get(msgId);
        if (finalPath && result) {
          try {
            const finalPatchUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/${encodeURIComponent(finalPath.subject)}/${encodeURIComponent(finalPath.chapter)}/${msgId}.json`;
            await axios.patch(finalPatchUrl, { thumb_link: result });
            console.log(`✅ [ARCHIVE] Final entry thumb patched for msgId=${msgId}`);
          } catch (e) {
            console.error(`❌ [ARCHIVE] Final thumb patch error (msgId=${msgId}):`, e.message);
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

app.get("/", (req, res) => res.send("Bot Active - Multi-Source Pipeline Integrated"));

app.get("/stream/:msgId", async (req, res) => {
  try {
    const msgId = parseInt(req.params.msgId);

    for (const [chatIdStr, entity] of sourceEntities) {
      const messages = await client.getMessages(entity, { ids: msgId });
      if (!messages || !messages[0] || !messages[0].media) continue;

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
      return;
    }

    res.status(404).send("Not Found");
  } catch (e) {
    if (!res.headersSent) res.status(500).send("Streaming Error");
  } finally {
    clearMemory();
  }
});

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
    timestamp: Date.now(),
  };

  try {
    await axios.put(pendingUrl, dataPayload);
    console.log(`🔥 [FIREBASE] Data Pending state mein push ho gaya (msgId=${msgId})`);
    pendingTimestamps.set(msgId, Date.now());
  } catch (e) {
    console.error("❌ [FIREBASE] Pending push error:", e.response?.data || e.message);
    return;
  }

  if (thumbPromises.has(msgId)) {
    const thumbUrl = await thumbPromises.get(msgId);
    // ✅ Check karein ki file Final ho chuki hai ya nahi
    if (thumbUrl && !finalizedDocPaths.has(msgId)) {
      try {
        await axios.patch(pendingUrl, { thumb_link: thumbUrl });
        console.log(`✅ [FIREBASE] thumb_link Pending mein patch hua (msgId=${msgId})`);
      } catch (e) {
        console.error("❌ [FIREBASE] thumb_link patch error:", e.message);
      }
    }
  }
}

// -------------------------------------------------------------
// 🧹 AUTO-CLEANUP: 4 minute timeout & Firebase sync
// -------------------------------------------------------------
async function cleanupOldPendingEntries() {
  const now = Date.now();
  const FOUR_MINUTES = 4 * 60 * 1000;

  try {
    const res = await axios.get(`${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending.json`);
    const pendingData = res.data;

    if (pendingData && typeof pendingData === "object") {
      for (const [msgIdStr, item] of Object.entries(pendingData)) {
        const itemTimestamp = (item && item.timestamp) ? item.timestamp : pendingTimestamps.get(parseInt(msgIdStr));

        if (!itemTimestamp || (now - itemTimestamp > FOUR_MINUTES)) {
          const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${msgIdStr}.json`;
          await axios.delete(pendingUrl);
          console.log(`🗑️ [AUTO-CLEANUP] Pending msgId=${msgIdStr} permanently deleted (4 min timeout)`);
          
          const msgIdNum = parseInt(msgIdStr);
          pendingTimestamps.delete(msgIdNum);
          const idx = screenshotRequestQueue.indexOf(msgIdNum);
          if (idx !== -1) screenshotRequestQueue.splice(idx, 1);
          screenshotResults.delete(msgIdNum);
          thumbPromises.delete(msgIdNum);
        }
      }
    }
  } catch (e) {
    console.error("❌ [AUTO-CLEANUP] Sync cleanup error:", e.message);
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

    if (msgChatIdStr) {
      const isKnown = sourceChatIdStrs.has(msgChatIdStr);
      console.log(`🔍 [DEBUG] msgId=${message.id} | chatId=${msgChatIdStr} | knownSource=${isKnown}`);
    }

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
        pushDirectToFirebase(message.id, streamLink);
        startThumbUpload(message.id, streamLink);

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
      }

      if (!matched) {
        console.log(`⚠️ [CHATGPT] Reply aaya par pendingReplyQueue khaali thi.`);
        return;
      }

      if (!replyText.trim().startsWith("@")) {
        console.log(`⚠️ [CHATGPT] Galat reply msgId=${matched.msgId} → IGNORE + DELETE PENDING`);
        const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${matched.msgId}.json`;
        try {
          await axios.delete(pendingUrl);
        } catch (e) {}
        pendingTimestamps.delete(matched.msgId);

        sendQueue.push({ msgId: null, text: RULE_REMINDER_TEXT, isReminder: true });
        if (!isSending) processSendQueue().catch(() => { isSending = false; });
        return;
      }

      const segments = replyText.split("@").map((s) => s.trim()).filter(Boolean);
      const hasLectureTag = segments.length >= 3 && /^lec/i.test(segments[2]);

      if (segments.length < 3 || !hasLectureTag) {
        console.log(`⚠️ [CHATGPT] Incomplete reply msgId=${matched.msgId} → IGNORE + DELETE PENDING`);
        const pendingUrl = `${FIREBASE_BASE_URL.replace(/\/$/, "")}/Pending/${matched.msgId}.json`;
        try {
          await axios.delete(pendingUrl);
        } catch (e) {}
        pendingTimestamps.delete(matched.msgId);

        sendQueue.push({ msgId: null, text: RULE_REMINDER_TEXT, isReminder: true });
        if (!isSending) processSendQueue().catch(() => { isSending = false; });
        return;
      }

      console.log(`🏷️ ChatGPT tags match hue msgId=${matched.msgId} se. Parsing...`);
      await patchAiTagsToFirebase(matched.msgId, replyText);
      return;
    }

    const isFromScreenshotBot = (screenshotBotIdStr && senderIdSync === screenshotBotIdStr) ||
                                 (screenshotBotIdStr && msgChatIdStr.includes(screenshotBotIdStr));

    if (isFromScreenshotBot) {
      if (message.photo || (message.media && message.media.className === 'MessageMediaPhoto')) {
        const targetMsgId = screenshotRequestQueue.shift();
        if (targetMsgId) {
          screenshotResults.set(targetMsgId, message);
          console.log(`📸 [THUMB-BOT] Screenshot successfully matched for msgId=${targetMsgId}`);
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

    try {
      const me = await client.getMe();
      console.log(`🤖 [BOT-IDENTITY] Account: @${me.username || 'no-username'} | ID=${me.id}`);
    } catch (e) {
      console.error("❌ [BOT-IDENTITY] getMe failed:", e.message);
    }

    chatgptEntity = await client.getEntity(CHATGPT_BOT);
    chatgptBotIdStr = chatgptEntity.id.toString();

    typeCheckerEntity = await client.getEntity(TYPE_CHECKER_BOT);
    typeCheckerBotIdStr = typeCheckerEntity.id.toString();

    screenshotEntity = await client.getEntity(NEW_SCREENSHOT_BOT);
    screenshotBotIdStr = screenshotEntity.id.toString();

    for (const chat of SOURCE_CHATS) {
      try {
        const entity = await client.getEntity(chat);
        let idStr = "";
        if (entity.id) idStr = entity.id.toString();
        
        if (idStr) {
          sourceEntities.set(idStr, entity);
          sourceChatIdStrs.add(idStr);
          console.log(`✅ [CHANNEL] Registered: ${chat} (ID=${idStr})`);
        }
      } catch (err) {
        console.error(`❌ [CHANNEL] Failed to load ${chat}:`, err.message);
      }
    }

    await loadTagMsgCount();

    client.addEventHandler(handleIncomingMessage, new NewMessage({}));
    console.log("⚡ Telegram Client Listening...");

  } catch (err) {
    console.error("❌ Client connection failed:", err.message);
  }
}

startServer();
