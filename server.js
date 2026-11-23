const express = require('express');
const http = require('http');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const nodemailer = require('nodemailer');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3001;
const JWT_SECRET = process.env.JWT_SECRET || 'local-testing-secret';
if (!process.env.JWT_SECRET && (process.env.RENDER || process.env.NODE_ENV === 'production')) {
  console.error('STOP: set a long random JWT_SECRET environment variable on Render (at least 32 random characters).');
  process.exit(1);
}
if (process.env.JWT_SECRET && process.env.JWT_SECRET.length < 32) {
  console.error('STOP: JWT_SECRET is too short. Use at least 32 random characters.');
  process.exit(1);
}
const crypto = require('crypto');
const FIREBASE_DB_SECRET = process.env.FIREBASE_DB_SECRET || '';
const FIREBASE_DB_URL =
  process.env.FIREBASE_DB_URL ||
  'https://whatsapp-live-4af52-default-rtdb.firebaseio.com/whatsapp_live_store.json';

const app = express();
app.set('trust proxy', 1);
app.use(cors());

// Simple rate limiter: allows `limit` hits per `windowMs` for a key
const rateBuckets = new Map();
function allowHit(key, limit, windowMs) {
  const now = Date.now();
  const list = (rateBuckets.get(key) || []).filter((t) => now - t < windowMs);
  if (list.length >= limit) { rateBuckets.set(key, list); return false; }
  list.push(now);
  rateBuckets.set(key, list);
  return true;
}
app.use(express.json({ limit: '10mb' }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST', 'PUT', 'DELETE'] },
});

// ---------------- IN-MEMORY + FIREBASE STORE ----------------
let store = {
  users: {},    // { [phone]: { id, phone, email, name, about, avatar, online, lastSeen, platform } }
  contacts: {}, // { [ownerPhone]: { [contactPhone]: { phone, name, addedAt } } }
  chats: {},    // { [chatId]: { id, isGroup, title, description, avatar, creatorPhone, members: [], lastMessage, updatedAt } }
  messages: {}, // { [chatId]: [ { id, chatId, senderId, senderName, type, body, createdAt } ] }
  statuses: [], // [ { id, phone, name, type, content, createdAt } ]
  calls: [],    // [ { id, callId, chatId, callerId, callerName, targetId, targetName, callType, createdAt } ]
};

const otpStore = new Map();            // { [phone]: { otp, email, name, expiresAt } }
const activeSocketByPhone = new Map(); // { [phone]: { socket, sessionId } }

const normalizePhone = (val) => String(val || '').replace(/[^\d]/g, '').trim();

// The database address, with the server's secret added so the Firebase rules can stay locked for everyone else
const dbUrl = () =>
  FIREBASE_DB_SECRET
    ? `${FIREBASE_DB_URL}${FIREBASE_DB_URL.includes('?') ? '&' : '?'}auth=${encodeURIComponent(FIREBASE_DB_SECRET)}`
    : FIREBASE_DB_URL;

let storeLoaded = false; // nothing is saved until the old data has been loaded, so a cold start can never wipe it
let saveTimer = null;
const saveStore = () => {
  if (!storeLoaded) return;
  if (saveTimer) clearTimeout(saveTimer);
  saveTimer = setTimeout(async () => {
    try {
      const r = await fetch(dbUrl(), {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(store),
      });
      if (!r.ok) console.error('Firebase save failed:', r.status);
    } catch (err) {
      console.error('Firebase save error:', err.message);
    }
  }, 400);
};

const loadStore = async () => {
  for (;;) {
    try {
      const res = await fetch(dbUrl());
      if (res.ok) {
        const data = await res.json();
        if (data && typeof data === 'object') {
          store.users = data.users || {};
          store.contacts = data.contacts || {};
          store.chats = data.chats || {};
          store.messages = data.messages || {};
          store.statuses = Array.isArray(data.statuses) ? data.statuses : [];
          store.calls = Array.isArray(data.calls) ? data.calls : [];
          Object.values(store.users).forEach((u) => { u.online = false; }); // reset online flags on cold boot
        }
        storeLoaded = true;
        console.log('Loaded store from Firebase.');
        return;
      }
      console.error('Firebase load failed:', res.status, '(check FIREBASE_DB_SECRET and the database rules)');
    } catch (err) {
      console.error('Firebase load error:', err.message);
    }
    await new Promise((r) => setTimeout(r, 5000));
  }
};
loadStore();

// ---------------- EMAIL SENDER (BREVO HTTP API OR GMAIL SMTP) ----------------
const mailTransporter =
  process.env.SMTP_USER && process.env.SMTP_PASS
    ? nodemailer.createTransport({
        service: 'gmail',
        auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
      })
    : null;

async function sendOtpEmail({ to, name, otp }) {
  const subject = 'Your Kutumbham Live Login OTP';
  const textContent = `Hello ${name},\n\nYour 6-digit login OTP is: ${otp}\n\nThis code expires in 5 minutes.`;

  // Option A (Recommended on Render Free Tier): Brevo HTTP API over HTTPS
  if (process.env.BREVO_API_KEY) {
    const senderEmail = process.env.SMTP_USER || process.env.BREVO_SENDER_EMAIL;
    if (!senderEmail) {
      throw new Error('Set SMTP_USER (your verified Brevo sender email) on Render.');
    }
    const resp = await fetch('https://api.brevo.com/v3/smtp/email', {
      method: 'POST',
      headers: {
        'api-key': process.env.BREVO_API_KEY,
        'content-type': 'application/json',
        accept: 'application/json',
      },
      body: JSON.stringify({
        sender: { name: 'Kutumbham Live', email: senderEmail },
        to: [{ email: to, name }],
        subject,
        textContent,
      }),
    });
    if (!resp.ok) {
      const errText = await resp.text();
      throw new Error(`Brevo API error (${resp.status}): ${errText}`);
    }
    return;
  }

  // Option B (Localhost or Paid Render): Gmail SMTP via Nodemailer
  if (mailTransporter) {
    await mailTransporter.sendMail({
      from: `"Kutumbham Live" <${process.env.SMTP_USER}>`,
      to,
      subject,
      text: textContent,
    });
    return;
  }

  throw new Error('Email service not configured. Add BREVO_API_KEY + SMTP_USER (or SMTP_USER + SMTP_PASS) on Render.');
}

// ---------------- HELPERS ----------------
const emitToUser = (phone, event, payload) => {
  io.to(`user:${phone}`).emit(event, payload);
};

const formatChatForUser = (chat, viewerPhone) => {
  if (chat.isGroup) {
    return {
      ...chat,
      isContact: true,
    };
  }
  const otherPhone = (chat.members || []).find((m) => m !== viewerPhone) || viewerPhone;
  const otherUser = store.users[otherPhone] || {};
  const savedContact = store.contacts[viewerPhone]?.[otherPhone];
  return {
    ...chat,
    title: savedContact?.name || otherUser.name || otherPhone,
    avatar: otherUser.avatar || null,
    otherPhone,
    otherAbout: otherUser.about || 'Hey there! I am using WhatsApp Live.',
    otherOnline: Boolean(otherUser.online),
    otherLastSeen: otherUser.lastSeen || null,
    isContact: Boolean(savedContact),
  };
};

const createSessionResponse = (phone, name, email, platform) => {
  const sessionId = 'sess_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  const existingSession = activeSocketByPhone.get(phone);
  if (existingSession && existingSession.socket) {
    existingSession.socket.emit('session:kicked', { reason: 'Logged in on another device.' });
    setTimeout(() => {
      try { existingSession.socket.disconnect(true); } catch (_e) {}
    }, 300);
  }

  const existingUser = store.users[phone] || {};
  const userObj = {
    id: phone,
    phone,
    email: existingUser.email || email || '',
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

  const token = jwt.sign(
    { sub: phone, phone, name: userObj.name, sessionId, platform },
    JWT_SECRET,
    { expiresIn: '30d' }
  );
  return { token, user: userObj, sessionId };
};

// ---------------- HEALTH ----------------
app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    version: 'v16-secure-login',
    emailConfigured: Boolean(process.env.BREVO_API_KEY || mailTransporter),
    ready: storeLoaded,
  });
});

// ---------------- AUTH ROUTES ----------------
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

// While the server is waking up and loading data, ask the app to try again
app.use((req, res, next) => {
  if (storeLoaded || req.path === '/health' || req.path === '/rtc-call') return next();
  return res.status(503).json({ error: 'The server is waking up. Please try again in a few seconds.' });
});

app.post('/auth/send-otp', async (req, res) => {
  const phone = normalizePhone(req.body?.phone);
  const name = String(req.body?.name || '').trim().slice(0, 60);
  const email = String(req.body?.email || '').trim().toLowerCase();

  if (!phone || phone.length < 4) return res.status(400).json({ error: 'Enter a valid phone number.' });
  if (!name) return res.status(400).json({ error: 'Enter your display name.' });
  if (!EMAIL_RE.test(email)) return res.status(400).json({ error: 'Enter a valid email address.' });

  // A number belongs to the email it was first registered with
  const existing = store.users[phone];
  if (existing) {
    if (existing.email && existing.email !== email) {
      return res.status(403).json({ error: 'This number is already registered with a different email. Use the email you signed up with.' });
    }
    if (!existing.email && process.env.ALLOW_LEGACY_CLAIM !== '1') {
      return res.status(403).json({ error: 'This number has no email saved yet. Ask the app owner for help.' });
    }
  }

  // Limits so nobody can spam emails or use up your email quota
  const pending = otpStore.get(phone);
  if (pending && Date.now() - (pending.sentAt || 0) < 30000) {
    return res.status(429).json({ error: 'Please wait 30 seconds before asking for another OTP.' });
  }
  if (!allowHit('ip:' + req.ip, 20, 3600000) || !allowHit('phone:' + phone, 5, 3600000) || !allowHit('email:' + email, 5, 3600000)) {
    return res.status(429).json({ error: 'Too many OTP requests. Please try again in an hour.' });
  }

  const otp = String(crypto.randomInt(100000, 1000000));
  otpStore.set(phone, { otp, email, name, expiresAt: Date.now() + 5 * 60 * 1000, attempts: 0, sentAt: Date.now() });

  try {
    await sendOtpEmail({ to: email, name, otp });
    res.json({ success: true, message: 'OTP sent to your email.' });
  } catch (err) {
    otpStore.delete(phone);
    console.error('OTP email error:', err.message);
    res.status(500).json({ error: 'Could not send the email. Please try again later.' });
  }
});

app.post('/auth/verify-otp', (req, res) => {
  const phone = normalizePhone(req.body?.phone);
  const otp = String(req.body?.otp || '').trim();
  const platform = String(req.body?.platform || 'web').slice(0, 20);

  const record = otpStore.get(phone);
  if (!record) return res.status(400).json({ error: 'No OTP found. Please request a new one.' });
  if (Date.now() > record.expiresAt) {
    otpStore.delete(phone);
    return res.status(400).json({ error: 'OTP expired. Please request a new one.' });
  }
  if (record.otp !== otp) {
    record.attempts = (record.attempts || 0) + 1;
    if (record.attempts >= 5) {
      otpStore.delete(phone);
      return res.status(400).json({ error: 'Too many wrong tries. Please request a new OTP.' });
    }
    return res.status(400).json({ error: 'Incorrect OTP. Try again.' });
  }

  otpStore.delete(phone);
  res.json(createSessionResponse(phone, record.name, record.email, platform));
});

// Every request below this line needs a valid login token. The user is taken from the token,
// never from what the app sends, so nobody can read or change another person's data.
const requireAuth = (req, res, next) => {
  if (req.path === '/rtc-call') return next(); // the call page loads inside a frame and cannot send the token
  const header = req.get('authorization') || '';
  const token = header.startsWith('Bearer ') ? header.slice(7) : '';
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const phone = normalizePhone(decoded.phone);
    const user = store.users[phone];
    if (!user || user.activeSessionId !== decoded.sessionId) {
      return res.status(401).json({ error: 'Session ended. Please log in again.' });
    }
    req.userPhone = phone;
  } catch (_e) {
    return res.status(401).json({ error: 'Please log in again.' });
  }
  Object.defineProperty(req, 'query', { value: { ...req.query, ...(req.query.userId !== undefined ? { userId: req.userPhone } : {}) }, writable: true, configurable: true });
  if (req.body && typeof req.body === 'object') {
    ['userId', 'myPhone', 'creatorPhone', 'requesterPhone', 'phone'].forEach((k) => {
      if (k in req.body) req.body[k] = req.userPhone;
    });
  }
  next();
};
app.use(requireAuth);

// ---------------- PROFILE ----------------
app.put('/users/:phone/profile', (req, res) => {
  const phone = normalizePhone(req.params.phone);
  if (phone !== req.userPhone) return res.status(403).json({ error: 'You can only edit your own profile.' });
  if (!store.users[phone]) return res.status(404).json({ error: 'User not found.' });

  const name = String(req.body?.name || '').trim();
  const about = String(req.body?.about || '').trim();
  const avatar = req.body?.avatar !== undefined ? req.body.avatar : store.users[phone].avatar;

  if (!name) return res.status(400).json({ error: 'Name cannot be empty.' });

  store.users[phone] = {
    ...store.users[phone],
    name,
    about: about || 'Hey there! I am using WhatsApp Live.',
    avatar,
  };
  saveStore();

  // Notify anyone who shares a chat or has this user in contacts
  const notifyPhones = new Set([phone]);
  Object.values(store.chats).forEach((chat) => {
    if ((chat.members || []).includes(phone)) {
      chat.members.forEach((m) => notifyPhones.add(m));
    }
  });
  Object.entries(store.contacts).forEach(([owner, list]) => {
    if (list && list[phone]) notifyPhones.add(owner);
  });
  notifyPhones.forEach((p) => emitToUser(p, 'chat:updated', { phone }));

  res.json({ user: store.users[phone] });
});

// ---------------- CONTACTS (USER-ISOLATED) ----------------
app.get('/contacts', (req, res) => {
  const userId = normalizePhone(req.query.userId);
  if (!userId) return res.json([]);
  const myContacts = store.contacts[userId] || {};
  const list = Object.values(myContacts).map((c) => {
    const u = store.users[c.phone] || {};
    return {
      phone: c.phone,
      name: c.name,
      avatar: u.avatar || null,
      about: u.about || 'Hey there! I am using WhatsApp Live.',
      online: Boolean(u.online),
      lastSeen: u.lastSeen || null,
    };
  });
  res.json(list);
});

app.post('/contacts', (req, res) => {
  const userId = normalizePhone(req.body?.userId);
  const contactPhone = normalizePhone(req.body?.contactPhone);
  const contactName = String(req.body?.contactName || '').trim() || contactPhone;

  if (!userId || !contactPhone) return res.status(400).json({ error: 'Valid phone numbers are required.' });
  if (userId === contactPhone) return res.status(400).json({ error: 'You cannot add yourself.' });

  if (!store.contacts[userId]) store.contacts[userId] = {};
  store.contacts[userId][contactPhone] = {
    phone: contactPhone,
    name: contactName,
    addedAt: new Date().toISOString(),
  };
  saveStore();
  emitToUser(userId, 'chat:updated', {});
  res.json({ success: true, contact: store.contacts[userId][contactPhone] });
});

// ---------------- CHATS & CUSTOM GROUPS (USER-ISOLATED) ----------------
app.get('/chats', (req, res) => {
  const userId = normalizePhone(req.query.userId);
  if (!userId) return res.json([]);
  const userChats = Object.values(store.chats)
    .filter((c) => Array.isArray(c.members) && c.members.includes(userId))
    .map((c) => formatChatForUser(c, userId))
    .sort((a, b) => new Date(b.updatedAt || 0) - new Date(a.updatedAt || 0));
  res.json(userChats);
});

app.post('/chats/direct', (req, res) => {
  const myPhone = normalizePhone(req.body?.myPhone);
  const friendPhone = normalizePhone(req.body?.friendPhone);
  if (!myPhone || !friendPhone) return res.status(400).json({ error: 'Phone numbers required.' });

  if (!store.contacts[myPhone]?.[friendPhone]) {
    return res.status(403).json({ error: 'Add this person to your Contacts first to start a chat.' });
  }

  const pair = [myPhone, friendPhone].sort();
  const chatId = `dm_${pair[0]}_${pair[1]}`;
  if (!store.chats[chatId]) {
    store.chats[chatId] = {
      id: chatId,
      isGroup: false,
      title: friendPhone,
      members: pair,
      lastMessage: 'Start a conversation',
      updatedAt: new Date().toISOString(),
    };
    if (!store.messages[chatId]) store.messages[chatId] = [];
    saveStore();
    pair.forEach((p) => emitToUser(p, 'chat:created', { chatId }));
  }

  res.json({ chat: formatChatForUser(store.chats[chatId], myPhone) });
});

app.post('/chats', (req, res) => {
  const creatorPhone = normalizePhone(req.body?.creatorPhone);
  const title = String(req.body?.title || '').trim();
  const description = String(req.body?.description || '').trim();
  const avatar = req.body?.avatar || null;
  const rawMembers = Array.isArray(req.body?.members) ? req.body.members : [];

  if (!creatorPhone || !title) return res.status(400).json({ error: 'Group title and creator are required.' });

  const memberSet = new Set([creatorPhone]);
  rawMembers.forEach((m) => {
    const clean = normalizePhone(m);
    if (clean) memberSet.add(clean);
  });
  const members = Array.from(memberSet);

  const chatId = 'grp_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
  store.chats[chatId] = {
    id: chatId,
    isGroup: true,
    title,
    description,
    avatar,
    creatorPhone,
    members,
    lastMessage: 'Group created',
    updatedAt: new Date().toISOString(),
  };
  store.messages[chatId] = [];
  saveStore();

  members.forEach((m) => emitToUser(m, 'chat:created', { chatId }));
  res.json({ chat: store.chats[chatId] });
});

app.put('/chats/:id/profile', (req, res) => {
  const chat = store.chats[req.params.id];
  const requesterPhone = normalizePhone(req.body?.requesterPhone);
  if (!chat || !chat.isGroup) return res.status(404).json({ error: 'Group not found.' });
  if (!chat.members.includes(requesterPhone)) return res.status(403).json({ error: 'Not a group member.' });

  if (req.body?.title) chat.title = String(req.body.title).trim();
  if (req.body?.description !== undefined) chat.description = String(req.body.description).trim();
  if (req.body?.avatar !== undefined) chat.avatar = req.body.avatar;
  chat.updatedAt = new Date().toISOString();
  saveStore();

  chat.members.forEach((m) => emitToUser(m, 'chat:updated', { chatId: chat.id }));
  res.json({ chat });
});

app.post('/chats/:id/members', (req, res) => {
  const chat = store.chats[req.params.id];
  const requesterPhone = normalizePhone(req.body?.requesterPhone);
  const memberPhone = normalizePhone(req.body?.memberPhone);

  if (!chat || !chat.isGroup) return res.status(404).json({ error: 'Group not found.' });
  if (!chat.members.includes(requesterPhone)) return res.status(403).json({ error: 'Not a group member.' });
  if (!memberPhone) return res.status(400).json({ error: 'Enter a valid phone number.' });

  if (!chat.members.includes(memberPhone)) {
    chat.members.push(memberPhone);
    chat.updatedAt = new Date().toISOString();
    saveStore();
    chat.members.forEach((m) => emitToUser(m, 'chat:updated', { chatId: chat.id }));
  }
  res.json({ chat });
});

// ---------------- STATUSES (24H, CONTACTS-ONLY) ----------------
app.get('/statuses', (req, res) => {
  const userId = normalizePhone(req.query.userId);
  if (!userId) return res.json([]);
  const cutoff = Date.now() - 24 * 60 * 60 * 1000;
  store.statuses = store.statuses.filter((s) => new Date(s.createdAt).getTime() > cutoff);

  const allowed = new Set([userId, ...Object.keys(store.contacts[userId] || {})]);
  const visible = store.statuses
    .filter((s) => allowed.has(s.phone))
    .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  res.json(visible);
});

app.post('/statuses', (req, res) => {
  const phone = normalizePhone(req.body?.phone);
  const name = String(req.body?.name || store.users[phone]?.name || phone);
  const type = req.body?.type === 'image' ? 'image' : 'text';
  const content = String(req.body?.content || '').trim();
  if (!phone || !content) return res.status(400).json({ error: 'Status content required.' });

  const statusObj = {
    id: 'st_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
    phone,
    name,
    type,
    content,
    createdAt: new Date().toISOString(),
  };
  store.statuses.push(statusObj);
  saveStore();

  // Notify self + everyone who has this user in their contacts
  const notifyPhones = new Set([phone]);
  Object.entries(store.contacts).forEach(([owner, list]) => {
    if (list && list[phone]) notifyPhones.add(owner);
  });
  notifyPhones.forEach((p) => emitToUser(p, 'status:new', statusObj));
  res.json(statusObj);
});

// ---------------- CALLS & WEBRTC ROOM ----------------
app.get('/calls', (req, res) => {
  const userId = normalizePhone(req.query.userId);
  if (!userId) return res.json([]);
  const myCalls = store.calls
    .filter((c) => c.callerId === userId || c.targetId === userId)
    .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt))
    .slice(0, 50);
  res.json(myCalls);
});

app.get('/rtc-call', (req, res) => {
  const room = String(req.query.room || 'default');
  const userId = String(req.query.userId || 'guest');
  const callType = req.query.callType === 'video' ? 'video' : 'voice';

  res.send(`<!DOCTYPE html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <style>
    body { margin:0; background:#0b141a; color:#fff; font-family:sans-serif; display:flex; flex-direction:column; align-items:center; justify-content:center; height:100vh; }
    video { width:92%; max-width:480px; border-radius:12px; background:#111b21; margin:8px 0; }
    #local { width:140px; position:fixed; bottom:16px; right:16px; border:2px solid #008069; }
    #status { color:#d9fdd3; margin-bottom:10px; }
  </style>
</head>
<body>
  <div id="status">Connecting ${callType} call...</div>
  <video id="remote" autoplay playsinline></video>
  <video id="local" autoplay muted playsinline></video>
  <script src="/socket.io/socket.io.js"></script>
  <script>
    const room = ${JSON.stringify(room)};
    const userId = ${JSON.stringify(userId)};
    const isVideo = ${JSON.stringify(callType === 'video')};
    const socket = io();
    let pc, localStream;

    async function start() {
      try {
        localStream = await navigator.mediaDevices.getUserMedia({ audio: true, video: isVideo });
        document.getElementById('local').srcObject = localStream;
        pc = new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] });
        localStream.getTracks().forEach(t => pc.addTrack(t, localStream));
        pc.ontrack = (e) => {
          document.getElementById('remote').srcObject = e.streams[0];
          document.getElementById('status').innerText = 'Connected';
        };
        pc.onicecandidate = (e) => {
          if (e.candidate) socket.emit('rtc:ice', { room, candidate: e.candidate });
        };
        socket.emit('rtc:join', { room, userId });
      } catch (err) {
        document.getElementById('status').innerText = 'Camera/Mic permission needed: ' + err.message;
      }
    }

    socket.on('rtc:peer-joined', async () => {
      if (!pc) return;
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      socket.emit('rtc:offer', { room, offer });
    });
    socket.on('rtc:offer', async ({ offer }) => {
      if (!pc) return;
      await pc.setRemoteDescription(new RTCSessionDescription(offer));
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      socket.emit('rtc:answer', { room, answer });
    });
    socket.on('rtc:answer', async ({ answer }) => {
      if (pc) await pc.setRemoteDescription(new RTCSessionDescription(answer));
    });
    socket.on('rtc:ice', async ({ candidate }) => {
      if (pc && candidate) await pc.addIceCandidate(new RTCIceCandidate(candidate));
    });

    start();
  </script>
</body>
</html>`);
});

// ---------------- SOCKET.IO ----------------
io.on('connection', (socket) => {
  // WebRTC signaling (iframe connects without JWT)
  socket.on('rtc:join', ({ room }) => {
    socket.join(`rtc:${room}`);
    socket.to(`rtc:${room}`).emit('rtc:peer-joined');
  });
  socket.on('rtc:offer', ({ room, offer }) => socket.to(`rtc:${room}`).emit('rtc:offer', { offer }));
  socket.on('rtc:answer', ({ room, answer }) => socket.to(`rtc:${room}`).emit('rtc:answer', { answer }));
  socket.on('rtc:ice', ({ room, candidate }) => socket.to(`rtc:${room}`).emit('rtc:ice', { candidate }));

  // Authenticated chat socket
  const token = socket.handshake.auth?.token;
  if (!token) return;

  let decoded;
  try {
    decoded = jwt.verify(token, JWT_SECRET);
  } catch (_e) {
    socket.disconnect(true);
    return;
  }

  const phone = normalizePhone(decoded.phone);
  socket.userPhone = phone;
  socket.join(`user:${phone}`);
  activeSocketByPhone.set(phone, { socket, sessionId: decoded.sessionId });

  if (store.users[phone]) {
    store.users[phone].online = true;
    store.users[phone].lastSeen = new Date().toISOString();
    saveStore();
    io.emit('presence', { phone, online: true });
  }

  socket.on('chat:join', (chatId) => {
    const chat = store.chats[chatId];
    if (!chat || !chat.members.includes(phone)) return;
    socket.join(`chat:${chatId}`);
    socket.emit('chat:history', store.messages[chatId] || []);
  });

  socket.on('message:send', ({ chatId, type, body }, ack) => {
    const chat = store.chats[chatId];
    if (!chat || !chat.members.includes(phone)) return;

    const msg = {
      id: 'msg_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
      chatId,
      senderId: phone,
      senderName: store.users[phone]?.name || phone,
      type: type || 'text',
      body: String(body || '').trim(),
      createdAt: new Date().toISOString(),
    };
    if (!msg.body) return;

    if (!store.messages[chatId]) store.messages[chatId] = [];
    store.messages[chatId].push(msg);
    chat.lastMessage = msg.body;
    chat.updatedAt = msg.createdAt;
    saveStore();

    chat.members.forEach((m) => emitToUser(m, 'message:new', msg));
    if (typeof ack === 'function') ack({ id: msg.id });
  });

  socket.on('call:start', (payload) => {
    const callRecord = {
      id: 'cl_' + Date.now(),
      callId: payload.callId,
      chatId: payload.chatId,
      callerId: phone,
      callerName: store.users[phone]?.name || phone,
      targetId: payload.targetId,
      targetName: payload.chatTitle || payload.targetId,
      callType: payload.callType || 'voice',
      createdAt: new Date().toISOString(),
    };
    store.calls.push(callRecord);
    saveStore();

    const chat = store.chats[payload.chatId];
    if (chat && Array.isArray(chat.members)) {
      chat.members.forEach((m) => {
        if (m !== phone) emitToUser(m, 'call:incoming', callRecord);
      });
    } else if (payload.targetId) {
      emitToUser(normalizePhone(payload.targetId), 'call:incoming', callRecord);
    }
  });

  socket.on('call:answer', (payload) => {
    if (payload?.callerId) emitToUser(normalizePhone(payload.callerId), 'call:answered', payload);
  });

  socket.on('call:end', (payload) => {
    const chat = store.chats[payload?.chatId];
    if (chat && Array.isArray(chat.members)) {
      chat.members.forEach((m) => emitToUser(m, 'call:ended', payload));
    }
  });

  socket.on('disconnect', () => {
    const current = activeSocketByPhone.get(phone);
    if (current && current.socket.id === socket.id) {
      activeSocketByPhone.delete(phone);
      if (store.users[phone]) {
        store.users[phone].online = false;
        store.users[phone].lastSeen = new Date().toISOString();
        saveStore();
        io.emit('presence', { phone, online: false });
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`WhatsApp Live backend v15 running on port ${PORT}`);
});