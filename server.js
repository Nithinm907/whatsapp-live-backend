const express = require('express');
const http = require('http');
const https = require('https');
const jwt = require('jsonwebtoken');
const { Server } = require('socket.io');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret-change-me';
const PORT = process.env.PORT || 3000;
const FIREBASE_DB_URL =
  process.env.FIREBASE_DB_URL || 'https://whatsapp-live-4af52-default-rtdb.firebaseio.com';

const app = express();

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.use(express.json({ limit: '25mb' }));

const DATA_FILE = path.join(__dirname, 'chat_data.json');
let store = {
  users: {},
  contacts: {}, // { [ownerPhone]: { [contactPhone]: { phone, name, addedAt } } }
  chats: {},
  messages: {},
  statuses: [],
  calls: [],
  activityLogs: [],
};

const BACKUP_FILE = DATA_FILE + '.bak';

function tryLoad(file) {
  try {
    if (!fs.existsSync(file)) return null;
    const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch (_e) {
    return null;
  }
}

(function loadSavedData() {
  const main = tryLoad(DATA_FILE);
  const loaded = main || tryLoad(BACKUP_FILE);
  if (loaded) {
    store = loaded;
    if (!store.users) store.users = {};
    if (!store.contacts) store.contacts = {};
    if (!store.chats) store.chats = {};
    if (!store.messages) store.messages = {};
    if (!store.statuses) store.statuses = [];
    if (!store.calls) store.calls = [];
    if (!store.activityLogs) store.activityLogs = [];
  }
})();

function httpsRequest(options, payload) {
  return new Promise((resolve) => {
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', (d) => (body += d));
      res.on('end', () => resolve({ status: res.statusCode, body }));
    });
    req.on('error', (e) => resolve({ status: 0, body: String(e.message) }));
    req.setTimeout(15000, () => req.destroy());
    if (payload) req.write(payload);
    req.end();
  });
}

async function syncStoreToFirebase() {
  if (!FIREBASE_DB_URL) return;
  try {
    const urlObj = new URL(`${FIREBASE_DB_URL.replace(/\/$/, '')}/whatsapp_live_store.json`);
    const payload = JSON.stringify(store);
    await httpsRequest(
      {
        hostname: urlObj.hostname,
        path: urlObj.pathname + urlObj.search,
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
        },
      },
      payload
    );
  } catch (_e) {}
}

async function loadFromFirebaseOnStartup() {
  if (!FIREBASE_DB_URL) return;
  try {
    const urlObj = new URL(`${FIREBASE_DB_URL.replace(/\/$/, '')}/whatsapp_live_store.json`);
    const res = await httpsRequest({
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: 'GET',
    });
    if (res.status === 200 && res.body && res.body !== 'null') {
      const remoteData = JSON.parse(res.body);
      if (remoteData && typeof remoteData === 'object') {
        store.users = { ...(remoteData.users || {}), ...store.users };
        store.contacts = { ...(remoteData.contacts || {}), ...store.contacts };
        store.chats = { ...(remoteData.chats || {}), ...store.chats };
        store.messages = { ...(remoteData.messages || {}), ...store.messages };
        if (Array.isArray(remoteData.statuses) && store.statuses.length === 0) store.statuses = remoteData.statuses;
        if (Array.isArray(remoteData.calls) && store.calls.length === 0) store.calls = remoteData.calls;
        if (Array.isArray(remoteData.activityLogs) && store.activityLogs.length === 0) store.activityLogs = remoteData.activityLogs;
      }
    } else {
      syncStoreToFirebase();
    }
  } catch (_e) {}
}
loadFromFirebaseOnStartup();

let saveTimer = null;
function writeStoreNow() {
  try {
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(store, null, 2));
    if (fs.existsSync(DATA_FILE)) fs.copyFileSync(DATA_FILE, BACKUP_FILE);
    fs.renameSync(tmp, DATA_FILE);
  } catch (_e) {}
  syncStoreToFirebase();
}

function saveStore() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; writeStoreNow(); }, 300);
}

process.on('SIGINT', () => { writeStoreNow(); process.exit(0); });
process.on('SIGTERM', () => { writeStoreNow(); process.exit(0); });

function normalizePhone(raw) {
  return String(raw || '').replace(/[^\d+]/g, '').trim();
}

function makeDirectChatId(phoneA, phoneB) {
  const sorted = [normalizePhone(phoneA), normalizePhone(phoneB)].sort();
  return `dm_${sorted[0]}_${sorted[1]}`;
}

function getUserContactsMap(phone) {
  const clean = normalizePhone(phone);
  if (!store.contacts[clean]) store.contacts[clean] = {};
  return store.contacts[clean];
}

function emitToMembers(members, event, payload) {
  if (!Array.isArray(members)) return;
  members.forEach((m) => io.to('user:' + normalizePhone(m)).emit(event, payload));
}

const activeSocketByPhone = new Map();
const uploadDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir);

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadDir),
  filename: (_req, file, cb) => {
    const safeName = String(file.originalname || 'file').replace(/[^a-zA-Z0-9._-]/g, '_');
    cb(null, Date.now() + '-' + safeName);
  },
});
const upload = multer({ storage, limits: { fileSize: 25 * 1024 * 1024 } });
app.use('/uploads', express.static(uploadDir));

const SERVER_VERSION = 'v14-isolated-contacts-avatars';
app.get('/health', (_req, res) => res.json({ ok: true, version: SERVER_VERSION, firebaseDb: FIREBASE_DB_URL }));

app.post('/upload', upload.single('media'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file uploaded' });
  const isImage = /^image\//i.test(req.file.mimetype || '');
  res.json({
    url: `/uploads/${req.file.filename}`,
    fileName: req.file.originalname,
    mimeType: req.file.mimetype,
    kind: isImage ? 'image' : 'file',
  });
});

app.post('/upload-base64', (req, res) => {
  const { data, ext, fileName, mimeType } = req.body || {};
  if (!data) return res.status(400).json({ error: 'No file data received' });
  const rawExt = String(ext || (fileName ? path.extname(fileName).slice(1) : 'bin')).toLowerCase();
  const safeExt = /^[a-z0-9]{1,8}$/i.test(rawExt) ? rawExt : 'bin';
  const base = fileName
    ? path.basename(fileName, path.extname(fileName)).replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 40)
    : Math.random().toString(36).slice(2, 8);
  const filename = `${Date.now()}-${base}.${safeExt}`;
  try {
    const clean = String(data).replace(/^data:[^;]+;base64,/, '');
    fs.writeFileSync(path.join(uploadDir, filename), Buffer.from(clean, 'base64'));
  } catch (_e) {
    return res.status(500).json({ error: 'Could not save the file' });
  }
  const isImage = /^(jpg|jpeg|png|gif|webp)$/i.test(safeExt) || /^image\//i.test(mimeType || '');
  res.json({
    url: `/uploads/${filename}`,
    fileName: fileName || filename,
    mimeType: mimeType || (isImage ? 'image/jpeg' : 'application/octet-stream'),
    kind: isImage ? 'image' : 'file',
  });
});

// WebRTC Call Page
app.get('/rtc-call', (req, res) => {
  const { room, userId, callType } = req.query;
  const turnUrl = process.env.TURN_URL || '';
  const turnUsername = process.env.TURN_USERNAME || '';
  const turnCredential = process.env.TURN_CREDENTIAL || '';

  res.setHeader('Content-Type', 'text/html');
  res.send(`<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <script src="/socket.io/socket.io.js"></script>
  <style>
    body { margin: 0; background: #0b141a; color: #fff; font-family: sans-serif; overflow: hidden; display: flex; flex-direction: column; height: 100vh; justify-content: center; align-items: center; }
    #remoteVideo { width: 100%; height: 100%; object-fit: cover; position: absolute; top: 0; left: 0; z-index: 1; background: #0b141a; }
    #localVideo { width: 115px; height: 155px; object-fit: cover; position: absolute; bottom: 20px; right: 16px; border-radius: 12px; border: 2px solid #25D366; z-index: 3; background: #1f2c34; }
    #statusOverlay { z-index: 4; background: rgba(17,27,33,0.85); padding: 10px 18px; border-radius: 20px; text-align: center; font-size: 13px; color: #25D366; font-weight: bold; max-width: 90%; }
  </style>
</head>
<body>
  <div id="statusOverlay">Requesting Mic/Cam...</div>
  <video id="remoteVideo" autoplay playsinline></video>
  <audio id="remoteAudio" autoplay playsinline></audio>
  <video id="localVideo" autoplay playsinline muted></video>
  <script>
    const ROOM = ${JSON.stringify(room || 'default_room')};
    const USER_ID = ${JSON.stringify(userId || 'user')};
    const CALL_TYPE = ${JSON.stringify(callType || 'voice')};
    const TURN_URL = ${JSON.stringify(turnUrl)};
    const TURN_USERNAME = ${JSON.stringify(turnUsername)};
    const TURN_CREDENTIAL = ${JSON.stringify(turnCredential)};

    const statusEl = document.getElementById('statusOverlay');
    const remoteVideo = document.getElementById('remoteVideo');
    const remoteAudio = document.getElementById('remoteAudio');
    const localVideo = document.getElementById('localVideo');

    if (CALL_TYPE !== 'video') {
      localVideo.style.display = 'none';
      remoteVideo.style.display = 'none';
    }

    const iceServers = [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
      { urls: 'stun:stun2.l.google.com:19302' }
    ];
    if (TURN_URL) iceServers.push({ urls: TURN_URL, username: TURN_USERNAME || undefined, credential: TURN_CREDENTIAL || undefined });

    const socket = io({ transports: ['websocket', 'polling'] });
    let pc = null, localStream = null, pendingCandidates = [], makingOffer = false;

    function setStatus(msg) { statusEl.style.display = 'block'; statusEl.innerText = msg; }

    function createPeer() {
      if (pc) return pc;
      pc = new RTCPeerConnection({ iceServers });
      if (localStream) localStream.getTracks().forEach(track => pc.addTrack(track, localStream));
      pc.ontrack = (event) => {
        const stream = event.streams && event.streams[0] ? event.streams[0] : new MediaStream([event.track]);
        remoteAudio.srcObject = stream; remoteAudio.muted = false; remoteAudio.volume = 1.0; remoteAudio.play().catch(() => {});
        if (CALL_TYPE === 'video') {
          remoteVideo.srcObject = stream; remoteVideo.muted = false; remoteVideo.play().catch(() => {});
          setStatus('Live Video Connected');
          setTimeout(() => { statusEl.style.display = 'none'; }, 1500);
        } else { setStatus('Voice Connected'); }
      };
      pc.onicecandidate = (e) => { if (e.candidate) socket.emit('rtc:signal', { room: ROOM, sender: USER_ID, type: 'candidate', candidate: e.candidate }); };
      return pc;
    }

    async function flushCandidates(peer) {
      while (pendingCandidates.length > 0) {
        try { await peer.addIceCandidate(new RTCIceCandidate(pendingCandidates.shift())); } catch (_e) {}
      }
    }

    async function sendOffer() {
      const peer = createPeer();
      if (makingOffer || peer.signalingState !== 'stable') return;
      try {
        makingOffer = true;
        const offer = await peer.createOffer({ offerToReceiveAudio: true, offerToReceiveVideo: CALL_TYPE === 'video' });
        await peer.setLocalDescription(offer);
        socket.emit('rtc:signal', { room: ROOM, sender: USER_ID, type: 'offer', sdp: peer.localDescription });
      } catch (_e) {} finally { makingOffer = false; }
    }

    socket.on('connect', () => socket.emit('rtc:join', { room: ROOM, userId: USER_ID }));

    navigator.mediaDevices.getUserMedia({
      audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true },
      video: CALL_TYPE === 'video' ? { facingMode: 'user', width: { ideal: 640 }, height: { ideal: 480 } } : false
    }).then(stream => {
      localStream = stream;
      if (CALL_TYPE === 'video') { localVideo.srcObject = stream; localVideo.play().catch(() => {}); }
      if (pc && localStream) localStream.getTracks().forEach(track => pc.addTrack(track, localStream));
      else createPeer();
      socket.emit('rtc:join', { room: ROOM, userId: USER_ID });
      setStatus('Joined call room, waiting for peer...');
    }).catch(err => setStatus('Mic/Cam blocked: ' + (err.message || err.name)));

    socket.on('rtc:peer-joined', async ({ userId }) => { if (userId && userId !== USER_ID) { setStatus('Peer joined, starting call...'); await sendOffer(); } });

    socket.on('rtc:signal', async (data) => {
      if (!data || data.sender === USER_ID) return;
      const peer = createPeer();
      try {
        if (data.type === 'offer') {
          if (makingOffer || peer.signalingState !== 'stable') return;
          await peer.setRemoteDescription(new RTCSessionDescription(data.sdp));
          await flushCandidates(peer);
          const answer = await peer.createAnswer();
          await peer.setLocalDescription(answer);
          socket.emit('rtc:signal', { room: ROOM, sender: USER_ID, type: 'answer', sdp: peer.localDescription });
        } else if (data.type === 'answer') {
          await peer.setRemoteDescription(new RTCSessionDescription(data.sdp));
          await flushCandidates(peer);
        } else if (data.type === 'candidate' && data.candidate) {
          if (peer.remoteDescription && peer.remoteDescription.type) await peer.addIceCandidate(new RTCIceCandidate(data.candidate));
          else pendingCandidates.push(data.candidate);
        }
      } catch (_e) {}
    });

    window.addEventListener('message', (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.action === 'toggleMute' && localStream) localStream.getAudioTracks().forEach(t => { t.enabled = !msg.muted; });
        if (msg.action === 'toggleCam' && localStream) localStream.getVideoTracks().forEach(t => { t.enabled = msg.cameraOn; });
      } catch (_e) {}
    });
  </script>
</body>
</html>`);
});

// Auth: Fresh on first login, preserves all data on returning login
app.post('/auth/direct-login', (req, res) => {
  const phone = normalizePhone(req.body?.phone);
  const name = String(req.body?.name || '').trim();
  const platform = String(req.body?.platform || 'unknown');
  if (!phone || phone.length < 4) return res.status(400).json({ error: 'Please enter a valid phone number.' });
  if (!name) return res.status(400).json({ error: 'Please enter your display name.' });

  const sessionId = 'sess_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  const existingSession = activeSocketByPhone.get(phone);
  if (existingSession && existingSession.socket) {
    existingSession.socket.emit('session:kicked', { reason: 'Logged in on another device.' });
    setTimeout(() => { try { existingSession.socket.disconnect(true); } catch (_e) {} }, 300);
  }

  const existingUser = store.users[phone] || {};
  const userObj = {
    id: phone,
    phone,
    name: existingUser.name || name,
    about: existingUser.about || 'Hey there! I am using WhatsApp Live.',
    avatar: existingUser.avatar || null,
    platform,
    online: true,
    activeSessionId: sessionId,
    lastSeen: new Date().toISOString(),
  };

  store.users[phone] = userObj;
  if (!store.contacts[phone]) store.contacts[phone] = {};
  saveStore();

  const token = jwt.sign({ sub: phone, phone, name: userObj.name, sessionId, platform }, JWT_SECRET, { expiresIn: '30d' });
  res.json({ token, user: userObj, sessionId });
});

// Update User Profile (Name, About, Avatar)
app.put('/users/:phone/profile', (req, res) => {
  const phone = normalizePhone(req.params.phone);
  const { name, about, avatar } = req.body || {};
  if (!store.users[phone]) return res.status(404).json({ error: 'User not found' });
  if (typeof name === 'string' && name.trim()) store.users[phone].name = name.trim();
  if (typeof about === 'string') store.users[phone].about = about.trim() || 'Hey there! I am using WhatsApp Live.';
  if (avatar !== undefined) store.users[phone].avatar = avatar || null;
  saveStore();
  io.emit('presence', { userId: phone, online: store.users[phone].online, user: store.users[phone] });
  res.json({ success: true, user: store.users[phone] });
});

// Contacts-Only Endpoints
app.get('/contacts', (req, res) => {
  const userId = normalizePhone(req.query.userId);
  if (!userId) return res.json([]);
  const map = getUserContactsMap(userId);
  const list = Object.values(map).map((c) => {
    const u = store.users[c.phone];
    return {
      phone: c.phone,
      name: c.name || u?.name || c.phone,
      about: u?.about || 'Hey there! I am using WhatsApp Live.',
      avatar: u?.avatar || null,
      online: !!u?.online,
      lastSeen: u?.lastSeen || null,
      registered: !!u,
    };
  });
  res.json(list);
});

app.post('/contacts', (req, res) => {
  const userId = normalizePhone(req.body?.userId);
  const contactPhone = normalizePhone(req.body?.contactPhone);
  const contactName = String(req.body?.contactName || contactPhone).trim();
  if (!userId || !contactPhone || contactPhone.length < 4) return res.status(400).json({ error: 'Valid phone number required' });
  if (userId === contactPhone) return res.status(400).json({ error: 'You cannot add yourself as a contact.' });

  const map = getUserContactsMap(userId);
  map[contactPhone] = { phone: contactPhone, name: contactName || contactPhone, addedAt: new Date().toISOString() };
  saveStore();
  res.json({ success: true, contact: map[contactPhone] });
});

app.delete('/contacts/:contactPhone', (req, res) => {
  const userId = normalizePhone(req.query.userId);
  const contactPhone = normalizePhone(req.params.contactPhone);
  if (!userId) return res.status(400).json({ error: 'userId required' });
  const map = getUserContactsMap(userId);
  delete map[contactPhone];
  saveStore();
  res.json({ success: true });
});

function visibleMessages(chatId, uid) {
  const chat = store.chats[chatId];
  const clearedAt = chat && chat.clearedAt && chat.clearedAt[uid] ? new Date(chat.clearedAt[uid]).getTime() : 0;
  return (store.messages[chatId] || [])
    .filter((m) => {
      if (clearedAt && new Date(m.createdAt).getTime() <= clearedAt) return false;
      if (Array.isArray(m.deletedFor) && m.deletedFor.includes(uid)) return false;
      return true;
    })
    .map((m) => (m.deletedForEveryone ? { ...m, type: 'text', body: 'This message was deleted' } : m));
}

// Strict User-Isolated Chats
app.get('/chats', (req, res) => {
  const userId = normalizePhone(req.query.userId);
  if (!userId) return res.json([]);
  const myContacts = getUserContactsMap(userId);

  const visibleChats = Object.values(store.chats)
    .filter((c) => {
      if (!Array.isArray(c.members) || !c.members.includes(userId)) return false;
      if (c.clearedAt && c.clearedAt[userId] && visibleMessages(c.id, userId).length === 0) return false;
      return true;
    })
    .map((c) => {
      const msgs = visibleMessages(c.id, userId);
      const lastMsg = msgs[msgs.length - 1];
      let displayTitle = c.title;
      let avatar = c.avatar || null;
      let otherUser = null;
      let otherPhone = null;
      let isContact = true;

      if (!c.isGroup && Array.isArray(c.members)) {
        otherPhone = c.members.find((m) => m !== userId) || userId;
        otherUser = store.users[otherPhone] || null;
        const savedContact = myContacts[otherPhone];
        isContact = !!savedContact;
        displayTitle = savedContact ? savedContact.name : otherUser ? `${otherUser.name} (${otherPhone})` : otherPhone;
        avatar = otherUser?.avatar || null;
      }

      return {
        ...c,
        title: displayTitle,
        avatar,
        otherPhone,
        isContact,
        otherOnline: otherUser ? !!otherUser.online : false,
        otherLastSeen: otherUser ? otherUser.lastSeen : null,
        otherAbout: otherUser ? otherUser.about : null,
        updatedAt: lastMsg ? lastMsg.createdAt : c.updatedAt || c.createdAt,
        lastMessage: lastMsg
          ? lastMsg.deletedForEveryone
            ? '🚫 This message was deleted'
            : lastMsg.type === 'image'
            ? '📷 Image'
            : lastMsg.type === 'file'
            ? `📎 ${lastMsg.fileName || 'Document'}`
            : `${lastMsg.senderName}: ${lastMsg.body}`
          : 'Tap to start chatting',
        lastMessageTime: lastMsg ? lastMsg.createdAt : c.createdAt,
      };
    })
    .sort((a, b) => new Date(b.updatedAt || 0) - new Date(a.updatedAt || 0));

  res.json(visibleChats);
});

// Create Custom Group (Name, Description, Avatar, Members)
app.post('/chats', (req, res) => {
  const { title, description, avatar, creatorPhone, members } = req.body || {};
  if (!title || !title.trim()) return res.status(400).json({ error: 'Group name is required' });
  const cleanCreator = normalizePhone(creatorPhone);
  if (!cleanCreator) return res.status(400).json({ error: 'Creator phone required' });

  const chatId = 'group_' + Date.now().toString(36);
  const now = new Date().toISOString();
  const initialMembers = Array.from(new Set([cleanCreator, ...(Array.isArray(members) ? members.map(normalizePhone) : [])].filter(Boolean)));

  const chatObj = {
    id: chatId,
    title: title.trim(),
    description: String(description || '').trim(),
    avatar: avatar || null,
    isGroup: true,
    adminId: cleanCreator,
    admins: [cleanCreator],
    members: initialMembers,
    updatedAt: now,
    createdAt: now,
  };

  store.chats[chatId] = chatObj;
  store.messages[chatId] = [];
  saveStore();
  emitToMembers(initialMembers, 'chat:created', chatObj);
  res.json({ success: true, chat: chatObj });
});

// Edit Group Profile (Name, Description, Avatar)
app.put('/chats/:chatId/profile', (req, res) => {
  const chat = store.chats[req.params.chatId];
  if (!chat || !chat.isGroup) return res.status(404).json({ error: 'Group not found' });
  const requesterPhone = normalizePhone(req.body?.requesterPhone);
  const admins = Array.isArray(chat.admins) && chat.admins.length > 0 ? chat.admins : [chat.adminId];
  if (!admins.includes(requesterPhone)) return res.status(403).json({ error: 'Only group admins can edit group info.' });

  if (typeof req.body?.title === 'string' && req.body.title.trim()) chat.title = req.body.title.trim();
  if (typeof req.body?.description === 'string') chat.description = req.body.description.trim();
  if (req.body?.avatar !== undefined) chat.avatar = req.body.avatar || null;
  chat.updatedAt = new Date().toISOString();
  saveStore();
  emitToMembers(chat.members, 'chat:updated', { chatId: chat.id });
  res.json({ success: true, chat });
});

app.get('/chats/:chatId/details', (req, res) => {
  const chat = store.chats[req.params.chatId];
  if (!chat) return res.status(404).json({ error: 'Chat not found' });
  const admins = Array.isArray(chat.admins) && chat.admins.length > 0 ? chat.admins : chat.adminId ? [chat.adminId] : [];
  const membersDetailed = (Array.isArray(chat.members) ? chat.members : []).map((p) => {
    const u = store.users[p];
    return {
      phone: p,
      name: u?.name || p,
      about: u?.about || 'Hey there! I am using WhatsApp Live.',
      avatar: u?.avatar || null,
      online: !!u?.online,
      isAdmin: admins.includes(p),
    };
  });
  res.json({ ...chat, admins, membersDetailed });
});

app.post('/chats/:chatId/members', (req, res) => {
  const chat = store.chats[req.params.chatId];
  if (!chat || !chat.isGroup) return res.status(404).json({ error: 'Group not found' });
  const requesterPhone = normalizePhone(req.body?.requesterPhone);
  const memberPhone = normalizePhone(req.body?.memberPhone);
  if (!memberPhone || memberPhone.length < 4) return res.status(400).json({ error: 'Valid phone number required' });
  const admins = Array.isArray(chat.admins) && chat.admins.length > 0 ? chat.admins : [chat.adminId];
  if (!admins.includes(requesterPhone)) return res.status(403).json({ error: 'Only group admins can add members.' });

  if (!Array.isArray(chat.members)) chat.members = [];
  if (!chat.members.includes(memberPhone)) chat.members.push(memberPhone);
  chat.updatedAt = new Date().toISOString();
  saveStore();
  emitToMembers(chat.members, 'chat:updated', { chatId: chat.id });
  res.json({ success: true, chat });
});

app.post('/chats/:chatId/admins', (req, res) => {
  const chat = store.chats[req.params.chatId];
  if (!chat || !chat.isGroup) return res.status(404).json({ error: 'Group not found' });
  const requesterPhone = normalizePhone(req.body?.requesterPhone);
  const targetPhone = normalizePhone(req.body?.targetPhone);
  const makeAdmin = req.body?.makeAdmin !== false;
  const admins = Array.isArray(chat.admins) && chat.admins.length > 0 ? [...chat.admins] : [chat.adminId];
  if (!admins.includes(requesterPhone)) return res.status(403).json({ error: 'Only group admins can manage admin roles.' });
  if (makeAdmin) {
    if (!admins.includes(targetPhone)) admins.push(targetPhone);
  } else {
    if (admins.length <= 1) return res.status(400).json({ error: 'Group must have at least one admin.' });
    const idx = admins.indexOf(targetPhone);
    if (idx !== -1) admins.splice(idx, 1);
  }
  chat.admins = admins;
  chat.adminId = admins[0] || null;
  saveStore();
  emitToMembers(chat.members, 'chat:updated', { chatId: chat.id });
  res.json({ success: true, chat });
});

app.delete('/chats/:chatId/members/:memberPhone', (req, res) => {
  const chat = store.chats[req.params.chatId];
  if (!chat || !chat.isGroup) return res.status(404).json({ error: 'Group not found' });
  const requesterPhone = normalizePhone(req.query.requesterPhone || req.body?.requesterPhone);
  const targetPhone = normalizePhone(req.params.memberPhone);
  const admins = Array.isArray(chat.admins) && chat.admins.length > 0 ? [...chat.admins] : [chat.adminId];
  if (requesterPhone !== targetPhone && !admins.includes(requesterPhone)) {
    return res.status(403).json({ error: 'Only group admins can remove members.' });
  }
  chat.members = (chat.members || []).filter((p) => p !== targetPhone);
  chat.admins = admins.filter((p) => p !== targetPhone);
  if (chat.admins.length === 0 && chat.members.length > 0) chat.admins = [chat.members[0]];
  chat.adminId = chat.admins[0] || null;
  saveStore();
  io.to('user:' + targetPhone).emit('chat:deleted', { chatId: chat.id });
  emitToMembers(chat.members, 'chat:updated', { chatId: chat.id });
  res.json({ success: true, chat });
});

// Direct Chat: Enforces Contacts-Only
app.post('/chats/direct', (req, res) => {
  const myPhone = normalizePhone(req.body?.myPhone);
  const friendPhone = normalizePhone(req.body?.friendPhone);
  if (!myPhone || !friendPhone) return res.status(400).json({ error: 'Both phones required' });

  const myContacts = getUserContactsMap(myPhone);
  if (!myContacts[friendPhone]) {
    return res.status(403).json({ error: 'Add this person to your Contacts first before starting a chat.' });
  }

  const chatId = makeDirectChatId(myPhone, friendPhone);
  const now = new Date().toISOString();
  if (!store.chats[chatId]) {
    store.chats[chatId] = {
      id: chatId,
      title: myContacts[friendPhone].name,
      isGroup: false,
      members: [myPhone, friendPhone],
      updatedAt: now,
      createdAt: now,
    };
  }
  if (!store.messages[chatId]) store.messages[chatId] = [];
  saveStore();

  const otherUser = store.users[friendPhone] || null;
  emitToMembers([myPhone, friendPhone], 'chat:created', store.chats[chatId]);
  res.json({
    success: true,
    chat: {
      ...store.chats[chatId],
      title: myContacts[friendPhone].name,
      avatar: otherUser?.avatar || null,
      otherPhone: friendPhone,
      otherOnline: !!otherUser?.online,
      otherLastSeen: otherUser?.lastSeen || null,
      otherAbout: otherUser?.about || null,
      isContact: true,
    },
  });
});

app.delete('/chats/:chatId', (req, res) => {
  const chatId = req.params.chatId;
  const userId = normalizePhone(req.query.userId || req.body?.userId);
  if (!userId) return res.status(400).json({ error: 'userId required' });
  const chat = store.chats[chatId];
  if (!chat) return res.status(404).json({ error: 'Chat not found' });
  if (!chat.clearedAt) chat.clearedAt = {};
  chat.clearedAt[userId] = new Date().toISOString();
  saveStore();
  io.to('user:' + userId).emit('chat:deleted', { chatId });
  res.json({ success: true });
});

// Statuses: Only Self + Saved Contacts
app.get('/statuses', (req, res) => {
  const userId = normalizePhone(req.query.userId);
  const now = Date.now();
  store.statuses = (store.statuses || []).filter((s) => now - new Date(s.createdAt).getTime() < 86400000);
  saveStore();
  if (!userId) return res.json([]);
  const myContacts = getUserContactsMap(userId);
  const visible = store.statuses.filter((s) => s.phone === userId || !!myContacts[s.phone]);
  res.json(visible);
});

app.post('/statuses', (req, res) => {
  const { phone, name, type, content } = req.body || {};
  if (!content) return res.status(400).json({ error: 'Content required' });
  const cleanPhone = normalizePhone(phone);
  const statusObj = {
    id: 'status_' + Date.now() + Math.random().toString(36).slice(2, 6),
    phone: cleanPhone,
    name: name || cleanPhone,
    type: type || 'text',
    content,
    createdAt: new Date().toISOString(),
  };
  store.statuses.push(statusObj);
  saveStore();

  // Notify self + anyone who has this user in their contacts
  io.to('user:' + cleanPhone).emit('status:new', statusObj);
  Object.keys(store.contacts).forEach((owner) => {
    if (store.contacts[owner] && store.contacts[owner][cleanPhone]) {
      io.to('user:' + owner).emit('status:new', statusObj);
    }
  });
  res.json({ success: true, status: statusObj });
});

app.delete('/statuses/:statusId', (req, res) => {
  const statusId = req.params.statusId;
  store.statuses = (store.statuses || []).filter((s) => s.id !== statusId);
  saveStore();
  io.emit('status:deleted', { statusId });
  res.json({ success: true });
});

app.get('/calls', (req, res) => {
  const userId = normalizePhone(req.query.userId);
  if (!userId) return res.json([]);
  res.json(store.calls.filter((c) => c.callerId === userId || c.targetId === userId).slice(-50).reverse());
});

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' }, maxHttpBufferSize: 3e7 });

io.on('connection', (socket) => {
  socket.on('rtc:join', ({ room, userId }) => {
    socket.join('rtc:' + room);
    socket.to('rtc:' + room).emit('rtc:peer-joined', { userId });
  });
  socket.on('rtc:signal', (data) => socket.to('rtc:' + data.room).emit('rtc:signal', data));

  const token = socket.handshake.auth && socket.handshake.auth.token;
  if (!token) return;
  let payload = null;
  try { payload = jwt.verify(token, JWT_SECRET); } catch (_e) { return socket.disconnect(true); }

  const uid = payload.sub;
  const sessionId = payload.sessionId;
  const userRecord = store.users[uid];
  if (userRecord && userRecord.activeSessionId && sessionId && userRecord.activeSessionId !== sessionId) {
    socket.emit('session:kicked', { reason: 'Active on another device.' });
    return socket.disconnect(true);
  }

  const prev = activeSocketByPhone.get(uid);
  if (prev && prev.socket.id !== socket.id) {
    prev.socket.emit('session:kicked', { reason: 'Logged in elsewhere.' });
    setTimeout(() => { try { prev.socket.disconnect(true); } catch (_e) {} }, 200);
  }

  activeSocketByPhone.set(uid, { socket, sessionId });
  socket.join('user:' + uid);

  if (store.users[uid]) {
    store.users[uid].online = true;
    store.users[uid].lastSeen = new Date().toISOString();
    saveStore();
  }
  io.emit('presence', { userId: uid, online: true });

  socket.on('chat:join', (chatId) => {
    const chat = store.chats[chatId];
    if (chat && Array.isArray(chat.members) && !chat.members.includes(uid)) return;
    socket.join('chat:' + chatId);
    let changed = false;
    (store.messages[chatId] || []).forEach((m) => {
      if (m.senderId !== uid) {
        if (!Array.isArray(m.readBy)) m.readBy = [];
        if (!m.readBy.includes(uid)) { m.readBy.push(uid); m.status = 'read'; changed = true; }
      }
    });
    if (changed) {
      saveStore();
      socket.to('chat:' + chatId).emit('messages:read', { chatId, readerId: uid });
    }
    socket.emit('chat:history', visibleMessages(chatId, uid));
  });

  socket.on('messages:mark-read', ({ chatId }) => {
    let changed = false;
    (store.messages[chatId] || []).forEach((m) => {
      if (m.senderId !== uid) {
        if (!Array.isArray(m.readBy)) m.readBy = [];
        if (!m.readBy.includes(uid)) { m.readBy.push(uid); m.status = 'read'; changed = true; }
      }
    });
    if (changed) {
      saveStore();
      socket.to('chat:' + chatId).emit('messages:read', { chatId, readerId: uid });
    }
  });

  socket.on('message:react', ({ chatId, messageId, emoji }) => {
    const msg = (store.messages[chatId] || []).find((m) => m.id === messageId);
    if (!msg) return;
    if (!msg.reactions) msg.reactions = {};
    if (msg.reactions[uid] === emoji) delete msg.reactions[uid];
    else msg.reactions[uid] = emoji;
    saveStore();
    io.to('chat:' + chatId).emit('message:reacted', { chatId, messageId, reactions: msg.reactions });
  });

  socket.on('message:delete', ({ chatId, messageId, scope }, ack) => {
    const reply = (r) => { if (typeof ack === 'function') ack(r); };
    const msg = (store.messages[chatId] || []).find((m) => m.id === messageId);
    if (!msg) return reply({ error: 'Message not found' });
    const room = store.chats[chatId];
    if (scope === 'everyone') {
      if (msg.senderId !== uid) return reply({ error: 'You can delete for everyone only your own messages.' });
      if (Date.now() - new Date(msg.createdAt).getTime() > 5 * 60 * 1000) {
        return reply({ error: 'You can delete for everyone only within 5 minutes of sending.' });
      }
      msg.deletedForEveryone = true;
      msg.deletedAt = new Date().toISOString();
      saveStore();
      io.to('chat:' + chatId).emit('message:deleted', { chatId, messageId, scope: 'everyone' });
      if (room) emitToMembers(room.members, 'chat:updated', { chatId });
      return reply({ success: true });
    }
    if (!Array.isArray(msg.deletedFor)) msg.deletedFor = [];
    if (!msg.deletedFor.includes(uid)) msg.deletedFor.push(uid);
    saveStore();
    io.to('user:' + uid).emit('message:deleted', { chatId, messageId, scope: 'me' });
    io.to('user:' + uid).emit('chat:updated', { chatId });
    reply({ success: true });
  });

  socket.on('typing:start', ({ chatId }) => {
    const senderName = store.users[uid]?.name || payload.name;
    socket.to('chat:' + chatId).emit('typing', { chatId, userId: uid, userName: senderName, typing: true });
  });

  socket.on('typing:stop', ({ chatId }) => {
    const senderName = store.users[uid]?.name || payload.name;
    socket.to('chat:' + chatId).emit('typing', { chatId, userId: uid, userName: senderName, typing: false });
  });

  socket.on('message:send', (msg, ack) => {
    const room = store.chats[msg.chatId];
    if (!room || !Array.isArray(room.members) || !room.members.includes(uid)) {
      if (typeof ack === 'function') ack({ error: 'Not a member of this chat' });
      return;
    }

    const messageId = Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
    const createdAt = new Date().toISOString();
    room.updatedAt = createdAt;

    const senderName = store.users[uid]?.name || payload.name;
    const saved = {
      id: messageId,
      chatId: msg.chatId,
      senderId: uid,
      senderName,
      type: msg.type || 'text',
      body: msg.body,
      fileName: msg.fileName || null,
      mimeType: msg.mimeType || null,
      replyTo: msg.replyTo || null,
      reactions: {},
      readBy: [],
      status: 'delivered',
      createdAt,
    };
    if (!store.messages[msg.chatId]) store.messages[msg.chatId] = [];
    store.messages[msg.chatId].push(saved);
    saveStore();

    socket.to('chat:' + msg.chatId).emit('message:new', saved);
    emitToMembers(room.members, 'chat:updated', { chatId: msg.chatId });

    room.members.forEach((memberPhone) => {
      if (memberPhone !== uid) {
        io.to('user:' + memberPhone).emit('notification:message', {
          chatId: msg.chatId,
          chatTitle: room.isGroup ? room.title : `${senderName} (${uid})`,
          isGroup: room.isGroup,
          otherPhone: room.isGroup ? null : uid,
          message: saved,
        });
      }
    });

    if (typeof ack === 'function') ack({ status: 'delivered', id: saved.id, createdAt: saved.createdAt });
  });

  socket.on('call:start', (callPayload) => {
    const callerName = store.users[uid]?.name || payload.name;
    const callRecord = {
      id: callPayload.callId || 'call_' + Date.now(),
      chatId: callPayload.chatId,
      callerId: uid,
      callerName,
      targetId: callPayload.targetId || callPayload.chatId,
      targetName: callPayload.chatTitle || callPayload.targetId,
      callType: callPayload.callType || 'voice',
      status: 'ringing',
      createdAt: new Date().toISOString(),
    };
    store.calls.push(callRecord);
    saveStore();
    if (callPayload.targetId && callPayload.targetId !== uid) {
      io.to('user:' + callPayload.targetId).emit('call:incoming', callRecord);
    } else {
      socket.to('chat:' + callPayload.chatId).emit('call:incoming', callRecord);
    }
  });

  socket.on('call:answer', (callPayload) => {
    io.to('user:' + callPayload.callerId).emit('call:accepted', callPayload);
    socket.to('chat:' + callPayload.chatId).emit('call:accepted', callPayload);
  });

  socket.on('call:end', (callPayload) => {
    if (callPayload.callerId) io.to('user:' + callPayload.callerId).emit('call:ended', callPayload);
    if (callPayload.targetId) io.to('user:' + callPayload.targetId).emit('call:ended', callPayload);
    if (callPayload.chatId) socket.to('chat:' + callPayload.chatId).emit('call:ended', callPayload);
  });

  socket.on('disconnect', () => {
    const current = activeSocketByPhone.get(uid);
    if (current && current.socket.id === socket.id) {
      activeSocketByPhone.delete(uid);
      const lastSeen = new Date().toISOString();
      if (store.users[uid]) {
        store.users[uid].online = false;
        store.users[uid].lastSeen = lastSeen;
        saveStore();
      }
      io.emit('presence', { userId: uid, online: false, lastSeen });
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`WhatsApp Real-Time Server running on http://0.0.0.0:${PORT}`);
  console.log('Server code version:', SERVER_VERSION);
  console.log('Firebase Cloud Storage Sync URL:', FIREBASE_DB_URL);
});