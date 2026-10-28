/**
 * VOX Signaling — MQTT over WSS (public broker) แทน Python WebSocket Server
 *
 * ทำไมต้องเป็น MQTT?
 *  - GitHub Pages เสิร์ฟได้เฉพาะ static files จึงรันเซิร์ฟเวอร์ของตัวเองไม่ได้
 *  - MQTT มี topic ซึ่งใช้แทน "ช่องสัญญาณ" ได้พอดี + wildcard subscription + retained message
 *    ทำให้คงสถานะ PTT lock / รายชื่อสมาชิกไว้ได้โดยไม่ต้องมีเซิร์ฟเวอร์
 *  - เสียงวิทยุยังวิ่งผ่าน WebRTC (P2P) เหมือนเดิม MQTT ทำหน้าที่แค่ signaling + presence
 *
 * รูปแบบ retained state ทำหน้าที่แทน `channels[...]['locked_by']` ใน server.py
 */

const VOX = (() => {
  'use strict';

  // Broker สาธารณะแบบไม่ต้องสมัครสมาชิก (เรียงตามลำดับที่จะลอง)
  const BROKERS = [
    { url: 'wss://broker.emqx.io:8084/mqtt', name: 'EMQX' },
    { url: 'wss://test.mosquitto.org:8081/mqtt', name: 'MOSQUITTO' },
    { url: 'wss://broker.hivemq.com:8884/mqtt', name: 'HIVEMQ' }
  ];

  const ROOT = 'vox-ptt/v1';
  const PRESENCE_TTL = 45000;      // สมาชิกถือว่า offline ถ้าไม่ announce ภายใน 45 วิ
  const ANNOUNCE_EVERY = 15000;    // announce ตัวเองทุก 15 วิ (heartbeat)
  const SWEEP_EVERY = 10000;       // กวาดสมาชิก/ล็อกค้าง ทุก 10 วิ
  const LOCK_STALE = 70000;        // ล็อกที่ค้างเกิน 70 วิ ถือว่าเป็นอิสระ (เจ้าของดับไปแล้ว)
  const PTT_MAX_HOLD = 60000;      // ตัด PTT อัตโนมัติที่ 60 วิ (เหมือน server.py)
  const MAX_RETRY_DELAY = 15000;

  // ---- state ----
  let client = null;
  let connected = false;
  let brokerIndex = 0;
  let brokerFailures = 0;
  let retryDelay = 1000;
  let reconnectTimer = null;
  let statusLabel = '';

  let session = null;   // { userId, name, channel }
  let selfId = null;    // userId ของเรา (ใช้ตั้งค่า Last Will ก่อนที่ session จะถูกสร้าง)
  let joined = false;   // subscribe + announce แล้วหรือยัง
  let initialSyncUntil = 0;

  let members = new Map();  // userId -> { id, name, ts }
  let lock = null;          // { lockedBy, speakerId, speakerName, ts }  mirror ของ retained state
  let pendingPtt = false;   // กด PTT แล้วรอคำตอบจาก broker
  let pendingStop = false;
  let pendingStopTimer = null;
  let lockTimer = null;     // ตัด PTT 60 วิ

  let announceTimer = null;
  let sweepTimer = null;
  let listTimer = null;
  let clientGen = 0;  // กัน event ของ client เก่า (ที่เพิ่งปิด) เข้ามายุ่งกับ client ใหม่

  // คิวทำ (เข้า/ถอด) รหัส — ต้องเป็นแนวยาวเพื่อไม่ให้ลำดับข้อความเพี้ยน
  let txChain = Promise.resolve();
  let rxChain = Promise.resolve();

  const handlers = {
    status: [], user_list: [], user_joined: [], user_left: [],
    signal: [], ptt_granted: [], ptt_denied: [], ptt_idle: []
  };

  function on(type, fn) {
    if (handlers[type]) handlers[type].push(fn);
    return VOX;
  }

  function emit(type, payload) {
    (handlers[type] || []).forEach((fn) => {
      try { fn(payload); } catch (e) { console.error('[Signaling] handler error', type, e); }
    });
  }

  // ---- topics ----
  // ช่องสาธารณะ -> ch/<ช่อง>  (เหมือนเดิมทุกประการ)
  // ห้องส่วนตัว  -> r/<tag>    (tag = hash(ชื่อห้อง+รหัสผ่าน) เดาชื่อห้องจาก topic ไม่ได้)
  // ทุก topic อ่านจาก session ปัจจุบัน พารามิเตอร์เดิมจึงถูกแทนที่เฉยๆ
  function safeChannel(ch) { return String(ch || '').replace(/[#\+]/g, '_'); }

  function roomBase() {
    if (!session) return null;
    if (session.private) return `${ROOT}/r/${session.tag}`;
    return `${ROOT}/ch/${safeChannel(session.channel)}`;
  }

  function topicMsg(_ch) { return `${roomBase()}/msg`; }
  function topicPresence(_ch) { return `${roomBase()}/presence/#`; }
  function topicPresenceOf(_ch, uid) { return `${roomBase()}/presence/${uid}`; }
  function topicState(_ch) { return `${roomBase()}/state`; }
  function topicGone(uid) {
    if (session && session.private) return `${ROOT}/r/${session.tag}/user/${uid}/gone`;
    return `${ROOT}/user/${uid}/gone`;
  }
  function topicGoneAll() {
    if (session && session.private) return `${ROOT}/r/${session.tag}/user/#`;
    return `${ROOT}/user/#`;
  }

  // ---- crypto (ห้องส่วนตัว) ----
  const enc8 = new TextEncoder();
  const dec8 = new TextDecoder();

  function hexOf(buf) {
    return Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, '0')).join('');
  }

  async function deriveRoomSecret(roomName, password) {
    // tag -> ใช้เป็นชื่อ topic (เดาไม่ได้จากชื่อห้อง)
    const tag = hexOf(await crypto.subtle.digest('SHA-256', enc8.encode(`vox-room|${roomName}|${password}`))).slice(0, 32);
    // key -> PBKDF2 (200k) เพื่อให้ถอดรหัสย้อนหลังจาก ciphertext ราคาแพง
    const material = await crypto.subtle.importKey('raw', enc8.encode(password), 'PBKDF2', false, ['deriveKey']);
    const key = await crypto.subtle.deriveKey(
      { name: 'PBKDF2', salt: enc8.encode(`vox-ptt-room|${roomName}`), iterations: 200000, hash: 'SHA-256' },
      material,
      { name: 'AES-GCM', length: 256 },
      false,
      ['encrypt', 'decrypt']
    );
    return { tag, key };
  }

  async function encryptText(key, topic, text) {
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const ct = await crypto.subtle.encrypt(
      { name: 'AES-GCM', iv, additionalData: enc8.encode(topic) }, key, enc8.encode(text)
    );
    const out = new Uint8Array(12 + ct.byteLength);
    out.set(iv, 0);
    out.set(new Uint8Array(ct), 12);
    return out;
  }

  async function decryptText(key, topic, bytes) {
    const pt = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv: bytes.slice(0, 12), additionalData: enc8.encode(topic) },
      key, bytes.slice(12)
    );
    return dec8.decode(pt);
  }

  function publishRaw(topic, payload, o) {
    return new Promise((resolve) => {
      if (!client || !connected) { resolve(); return; }
      let done = false;
      const finish = () => { if (!done) { done = true; resolve(); } };
      const guard = setTimeout(finish, 3000); // กันคิวค้างถ้า broker ไม่เรียก callback
      try {
        client.publish(topic, payload, { qos: o.qos || 0, retain: !!o.retain }, (err) => {
          clearTimeout(guard);
          if (err) console.warn('[Signaling] publish failed:', topic, err.message);
          finish();
        });
      } catch (e) {
        clearTimeout(guard);
        finish();
      }
    });
  }

  function pub(topic, obj, opts) {
    if (!client || !connected) return false;
    const o = opts || {};
    const key = session ? session.key : null; // จับ key ณ ตอนเรียก (คนละห้อง/คนละช่วงเวลาต้องใช้คนละคีย์)
    if (obj === null || obj === undefined) {
      txChain = txChain.then(() => publishRaw(topic, '', o)).catch(() => {});
      return true;
    }
    const text = JSON.stringify(obj);
    txChain = txChain
      .then(() => (key ? encryptText(key, topic, text) : text))
      .then((payload) => publishRaw(topic, payload, o))
      .catch((e) => console.warn('[Signaling] publish error:', e && e.message));
    return true;
  }

  function pubMsg(ch, obj) {
    if (!session) return false;
    return pub(topicMsg(ch), Object.assign({ v: 1, from: session.userId, ts: Date.now() }, obj), { qos: 0 });
  }

  // ---- members / user_list ----
  function emitListNow() {
    if (!session) return;
    const users = [];
    members.forEach((m) => users.push({ id: m.id, name: m.name }));
    if (!members.has(session.userId)) {
      users.push({ id: session.userId, name: session.name });
      members.set(session.userId, { id: session.userId, name: session.name, ts: Date.now() });
    }
    emit('user_list', { users });
  }

  function scheduleListEmit() {
    if (listTimer) return;
    listTimer = setTimeout(() => { listTimer = null; emitListNow(); }, 150);
  }

  function addMember(id, name, ts, silentIfNew) {
    if (!session || !id) return;
    const existing = members.get(id);
    if (existing) {
      existing.ts = ts || Date.now();
      if (name && name !== existing.name) {
        existing.name = name;
        scheduleListEmit(); // ชื่อเปลี่ยน -> ต้อง render ใหม่ด้วย
      }
      return;
    }
    members.set(id, { id, name: name || 'Anonymous', ts: ts || Date.now() });
    scheduleListEmit();
    if (!silentIfNew && Date.now() >= initialSyncUntil && id !== session.userId) {
      emit('user_joined', { user: { id, name: name || 'Anonymous' } });
    }
  }

  function removeMember(id) {
    if (!id || !members.has(id)) return;
    if (session && id === session.userId) return; // ตัวเราเองห้ามถูกกวาดออก
    members.delete(id);
    scheduleListEmit();
    emit('user_left', { userId: id });
  }

  // ---- PTT lock (retained state) ----
  function clearLockTimer() {
    if (lockTimer) { clearTimeout(lockTimer); lockTimer = null; }
  }

  function startLockTimer() {
    clearLockTimer();
    lockTimer = setTimeout(() => {
      console.warn('[Signaling] 60s PTT timeout -> auto release');
      releaseLock();
    }, PTT_MAX_HOLD);
  }

  function releaseLock() {
    if (!session) return;
    clearLockTimer();
    lock = null;
    pendingPtt = false;
    pendingStop = false;
    pub(topicState(session.channel), null, { retain: true, qos: 1 });
    pubMsg(session.channel, { type: 'ptt_idle' });
    emit('ptt_idle', {});
  }

  function applyState(next) {
    if (!session) return;
    const me = session.userId;
    const prevLockedBy = lock ? lock.lockedBy : null;
    const nextLockedBy = next ? next.lockedBy : null;

    // เราเพิ่งกด "ปล่อย" แต่คำตอบ (retain echo) เพิ่งย้อนกลับมา -> เคลียร์เงียบๆ
    if (pendingStop && nextLockedBy === me) {
      pendingStop = false;
      clearLockTimer();
      lock = null;
      pub(topicState(session.channel), null, { retain: true, qos: 1 });
      pubMsg(session.channel, { type: 'ptt_idle' });
      return;
    }

    if ((nextLockedBy || null) === (prevLockedBy || null)) return; // ไม่มีการเปลี่ยนแปลง

    if (!nextLockedBy) {
      clearLockTimer();
      lock = null;
      pendingPtt = false;
      emit('ptt_idle', {});
      return;
    }

    // มีคนครองช่อง (ถ้าเพิ่งเสียล็อกให้ยกเลิก timer ตัด 60 วิ ของเราด้วย)
    clearLockTimer();
    const speakerName = next.speakerName || 'Unknown';
    lock = {
      lockedBy: nextLockedBy,
      speakerId: next.speakerId || nextLockedBy,
      speakerName,
      ts: next.ts || Date.now()
    };

    if (nextLockedBy === me) {
      pendingPtt = false;
      startLockTimer();
      emit('ptt_granted', { speakerId: me, speakerName });
    } else if (pendingPtt) {
      // เราขอพูดแต่มีคนแย่งไปก่อน
      pendingPtt = false;
      clearLockTimer();
      emit('ptt_denied', { speakerName });
    } else {
      emit('ptt_granted', { speakerId: lock.speakerId, speakerName });
    }
  }

  function requestPttStart() {
    if (!session || !connected) return { ok: false, reason: 'offline' };
    const me = session.userId;
    const now = Date.now();

    if (lock && lock.lockedBy && now - (lock.ts || 0) < LOCK_STALE) {
      if (lock.lockedBy === me) return { ok: true, already: true };
      return { ok: false, reason: 'busy', speakerName: lock.speakerName };
    }

    const state = {
      lockedBy: me,
      speakerId: me,
      speakerName: session.name,
      ts: now
    };
    pub(topicState(session.channel), state, { retain: true, qos: 1 });
    pubMsg(session.channel, { type: 'ptt_start', speakerId: me, speakerName: session.name, ts: now });
    pendingPtt = true;
    // คำตอบจะกลับมาในรูป retained/ข่าว topic เดียวกัน -> applyState
    return { ok: true, pending: true };
  }

  function requestPttStop() {
    if (!session) return;
    pendingPtt = false;
    if (lock && lock.lockedBy === session.userId) { releaseLock(); return; }

    // อาจยังไม่ได้รับ echo ของล็อกตัวเอง -> รอแป๊บเดียวแล้วเคลียร์
    pendingStop = true;
    if (pendingStopTimer) clearTimeout(pendingStopTimer);
    pendingStopTimer = setTimeout(() => {
      pendingStopTimer = null;
      if (!pendingStop) return;
      pendingStop = false;
      if (lock && lock.lockedBy === session.userId) releaseLock();
    }, 900);
  }

  // ---- presence ----
  function announcePresence() {
    if (!session || !connected) return;
    members.set(session.userId, { id: session.userId, name: session.name, ts: Date.now() });
    pub(topicPresenceOf(session.channel, session.userId), {
      id: session.userId, name: session.name, ts: Date.now()
    }, { retain: true, qos: 1 });
  }

  function clearOwnPresence() {
    if (!session) return;
    pub(topicPresenceOf(session.channel, session.userId), null, { retain: true, qos: 1 });
  }

  function sweep() {
    if (!session) return;
    const now = Date.now();
    members.forEach((m, id) => {
      if (id !== session.userId && now - (m.ts || 0) > PRESENCE_TTL) {
        console.log('[Signaling] presence timeout:', m.name);
        members.delete(id);
        scheduleListEmit();
        emit('user_left', { userId: id });
      }
    });

    // ล็อกที่ค้าง (เจ้าของปิดเบราว์เซอร์ไปแล้ว) -> เคลียร์อัตโนมัติ
    if (lock && lock.lockedBy !== session.userId && now - (lock.ts || 0) > LOCK_STALE) {
      applyState(null);
      pub(topicState(session.channel), null, { retain: true, qos: 1 });
      return;
    }

    // ผู้พูดออกจากช่องไปแล้วแต่ยังถือล็อกอยู่ (ไม่ได้ unsubscribe retained) -> ปล่อยล็อกให้
    if (lock && lock.lockedBy !== session.userId && !members.has(lock.lockedBy)) {
      applyState(null);
      pub(topicState(session.channel), null, { retain: true, qos: 1 });
    }
  }

  function clearPendingStopTimer() {
    if (pendingStopTimer) { clearTimeout(pendingStopTimer); pendingStopTimer = null; }
    pendingStop = false;
  }

  // ---- incoming messages ----
  function onChannelMessage(raw) {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || !m.type || !session) return;

    switch (m.type) {
      case 'user_joined':
        addMember(m.userId, m.name, m.ts, false);
        break;

      case 'user_left':
        removeMember(m.userId);
        break;

      case 'signal':
        if (m.target === session.userId && m.data) {
          emit('signal', { sender: m.from, data: m.data });
        }
        break;

      case 'ptt_start':
        applyState({ lockedBy: m.speakerId, speakerId: m.speakerId, speakerName: m.speakerName, ts: m.ts });
        break;

      case 'ptt_idle':
        applyState(null);
        break;
    }
  }

  function onPresence(raw, uidFromTopic) {
    if (!session || !uidFromTopic || uidFromTopic === session.userId) return;
    if (!raw) { removeMember(uidFromTopic); return; }
    const data = (() => { try { return JSON.parse(raw); } catch (e) { return null; } })();
    if (!data || !data.id) return;
    // retained ที่เก่ากว่า TTL = คนออกจากช่องไปแล้ว (มักติดมาตอน reconnect) -> อย่าเพิ่มกลับ
    const ts = data.ts || Date.now();
    if (Date.now() - ts > PRESENCE_TTL) return;
    addMember(data.id, data.name, ts, false);
  }

  function onState(raw) {
    if (!session) return;
    if (!raw) { applyState(null); return; }
    let data;
    try { data = JSON.parse(raw); } catch (e) { return; }
    if (!data || !data.lockedBy) { applyState(null); return; }
    if (Date.now() - (data.ts || 0) > LOCK_STALE) {
      // retained ค้าง -> เคลียร์ทิ้ง
      pub(topicState(session.channel), null, { retain: true, qos: 1 });
      applyState(null);
      return;
    }
    applyState(data);
  }

  function topicMatches(topic) {
    if (!session) return false;
    if (topic.indexOf(`${roomBase()}/`) === 0) return true;
    // will ระดับ global ใช้เฉพาะช่องสาธารณะ
    return !session.private && topic.indexOf(`${ROOT}/user/`) === 0;
  }

  function routeMessage(topic, raw) {
    // Last Will: broker แจ้งเองเมื่อ client ขาดการเชื่อมต่อ (ปิดแท็บ / เน็ตหลุด)
    if (topic.indexOf('/user/') !== -1) {
      let d = null;
      try { d = JSON.parse(raw); } catch (e) { /* ignore */ }
      const goneId = d && d.userId ? d.userId : topic.slice(topic.lastIndexOf('/user/') + 6, topic.lastIndexOf('/gone'));
      if (goneId && members.has(goneId)) removeMember(goneId);
      return;
    }
    if (topic.endsWith('/state')) { onState(raw); return; }
    const idx = topic.lastIndexOf('/presence/');
    if (idx !== -1) { onPresence(raw, topic.slice(idx + '/presence/'.length)); return; }
    if (topic.endsWith('/msg')) { onChannelMessage(raw); return; }
  }

  // ---- subscribe / join / leave ----
  function subscribeSession() {
    if (!client || !connected || !session) return;
    const ch = session.channel;
    const subs = {};
    subs[topicMsg(ch)] = { qos: 0 };
    subs[topicPresence(ch)] = { qos: 1 };
    subs[topicState(ch)] = { qos: 1 };
    subs[topicGoneAll()] = { qos: 1 }; // Last Will ของคนในห้อง/ช่อง

    client.subscribe(subs, (err, granted) => {
      if (err) {
        console.error('[Signaling] subscribe error:', err.message);
        return;
      }
      joined = true;
      initialSyncUntil = Date.now() + 1500; // ช่วงนี้อย่าเล่นเสียง "เข้าร่วม" ซ้ำทุกคนที่มีอยู่แล้ว
      announcePresence();
      pubMsg(ch, { type: 'user_joined', userId: session.userId, name: session.name });
      emitListNow();

      if (announceTimer) clearInterval(announceTimer);
      announceTimer = setInterval(announcePresence, ANNOUNCE_EVERY);
      if (sweepTimer) clearInterval(sweepTimer);
      sweepTimer = setInterval(sweep, SWEEP_EVERY);

      // กวาดทันทีครั้งแรก: retained ที่ค้าง (สมาชิกที่ไปแล้ว / ล็อก PTT ค้าง)
      // เข้ามาพร้อมกับตอน subscribe โดยยังไม่ผ่าน setTimeout แรก
      sweep();
    });
  }

  function unsubscribeSession() {
    if (!client || !connected || !session) return;
    const ch = session.channel;
    client.unsubscribe([topicMsg(ch), topicPresence(ch), topicState(ch), topicGoneAll()], () => {});
  }

  async function join(opts) {
    const wantPrivate = !!(opts.password && opts.roomName && opts.password.length);
    const roomName = wantPrivate ? String(opts.roomName).trim() : null;
    const password = wantPrivate ? opts.password : null;
    const secret = wantPrivate ? await deriveRoomSecret(roomName, password) : null;

    const sameRoom = session && joined && session.userId === opts.userId &&
      session.channel === opts.channel && !!session.private === wantPrivate &&
      (!wantPrivate || session.tag === secret.tag);
    if (sameRoom) {
      // เปลี่ยนแค่ชื่อเรียกขาน -> announce ใหม่
      session.name = opts.name;
      announcePresence();
      return;
    }

    const prevPrivate = session ? !!session.private : false;
    const prevTag = session ? session.tag : null;
    if (session) leave(); // unsubscribe + เคลียร์ presence/lock ของห้องเดิม (ใช้ topic เดิม)

    session = {
      userId: opts.userId,
      name: opts.name,
      channel: opts.channel,
      private: wantPrivate,
      roomName,
      tag: wantPrivate ? secret.tag : null,
      key: wantPrivate ? secret.key : null
    };
    selfId = opts.userId;
    members = new Map();
    lock = null;
    clearLockTimer();
    clearPendingStopTimer();
    joined = false;
    members.set(session.userId, { id: session.userId, name: session.name, ts: Date.now() });

    // will/subscribe ของห้องส่วนตัวผูกกับ topic ห้อง -> ต้องเปิด connection ใหม่
    const needRestart = !!client && (prevPrivate !== wantPrivate || (wantPrivate && prevTag !== session.tag));
    if (needRestart) {
      restartClient();
    } else if (connected) {
      subscribeSession();
    }
    emitListNow(); // UI มีตัวเราทันที
  }

  // เปิด connection ใหม่ (ใช้ตอนเปลี่ยนห้องส่วนตัว เพราะ Last Will ผูกกับ topic ห้อง)
  function restartClient() {
    const old = client;
    if (announceTimer) { clearInterval(announceTimer); announceTimer = null; }
    if (sweepTimer) { clearInterval(sweepTimer); sweepTimer = null; }
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    if (!old) { openClient(); return; }

    // รอให้คิว publish ของห้องเดิม (เคลียร์ presence / ปล่อยล็อก) ส่งผ่าน connection เก่าให้เสร็จก่อน
    txChain.then(() => {
      if (client !== old) return;
      client = null;
      connected = false;
      joined = false;
      try { old.end(true); } catch (e) { /* noop */ }
      openClient();
    }).catch(() => { openClient(); });
  }

  function leave() {
    if (!session) return;
    clearLockTimer();
    clearPendingStopTimer();
    if (announceTimer) { clearInterval(announceTimer); announceTimer = null; }
    if (sweepTimer) { clearInterval(sweepTimer); sweepTimer = null; }

    // ถ้าเราครองช่องอยู่ -> ปล่อยล็อกก่อนไป
    if (lock && lock.lockedBy === session.userId) {
      lock = null;
      pub(topicState(session.channel), null, { retain: true, qos: 1 });
    }
    pubMsg(session.channel, { type: 'user_left', userId: session.userId });
    clearOwnPresence();
    unsubscribeSession();

    session = null;
    joined = false;
    members = new Map();
    lock = null;
  }

  function sendSignal(targetId, senderId, data) {
    if (!session) return false;
    return pubMsg(session.channel, { type: 'signal', target: targetId, sender: senderId, data });
  }

  function updateName(name) {
    if (!session) return;
    session.name = name;
    announcePresence();
    scheduleListEmit(); // อัปเดต label ตัวเองในรายชื่อด้วย
    if (lock && lock.lockedBy === session.userId) {
      lock.speakerName = name;
      pub(topicState(session.channel), lock, { retain: true, qos: 1 });
    }
  }

  // ---- โหมด LAN (ระบบสำรองไว้ใช้ตอนไม่มีอินเทอร์เน็ต) ----
  // เมื่อแอปถูกเสิร์ฟจาก lan_server.py บนเครื่องในวง Wi-Fi เดียวกัน
  // จะคุยกับเซิร์ฟเวอร์นั้นผ่าน WebSocket ที่ /ws แทน MQTT สาธารณะ
  // ดังนั้นทั้ง signaling และ presence จะทำงานได้โดยไม่ต้องมีเน็ตเลย
  let forceMqtt = false;     // ลอง LAN แล้วไม่สำเร็จ -> ถอยไปใช้ broker สาธารณะ
  let localFailures = 0;

  function useLocalSignaling() {
    if (forceMqtt || typeof location === 'undefined' || !location.host) return false;
    const q = new URLSearchParams(location.search);
    if (q.get('sig') === 'mqtt') return false;   // บังคับใช้ broker สาธารณะ
    if (q.get('sig') === 'lan') return true;     // บังคับใช้ LAN
    if (location.protocol === 'file:') return false; // เปิดไฟล์ตรงๆ -> เหมือนเดิม
    const h = (location.hostname || '').toLowerCase();
    if (h === 'localhost' || h === '127.0.0.1' || h === '::1' || h === '[::1]') return true;
    if (h.endsWith('.local')) return true;
    if (/^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(h)) return true;
    return !/\.github\.io$/.test(h); // ที่อื่นที่ไม่ใช่ GitHub Pages = เซิร์ฟเวอร์ LAN ของเรา
  }

  function brokerName() {
    if (useLocalSignaling()) return 'LAN (ในวงเดียวกัน)';
    return BROKERS[brokerIndex % BROKERS.length].name;
  }

  // ---- ตัวแปลง payload: mqtt.js ส่ง Buffer แต่ของ LAN ส่ง JSON ----
  function encodePayload(payload) {
    if (typeof payload === 'string') return payload;
    if (payload instanceof Uint8Array) {         // ข้อความเข้ารหัส (ห้องส่วนตัว)
      let s = '';
      for (let i = 0; i < payload.length; i++) s += String.fromCharCode(payload[i]);
      return { b64: btoa(s) };
    }
    if (payload === undefined || payload === null) return '';
    return String(payload);
  }

  function decodePayload(payload) {
    if (payload && typeof payload === 'object' && typeof payload.b64 === 'string') {
      const bin = atob(payload.b64);
      const out = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
      return out;
    }
    return enc8.encode(typeof payload === 'string' ? payload : '');
  }

  /**
   * LocalClient — หน้าตาและพฤติกรรมเหมือน mqtt.connect() ทุกอย่างที่โค้ดใช้
   * (on/publish/subscribe/unsubscribe/end) แต่วิ่งผ่าน WebSocket ของ lan_server.py
   * แทนที่จะวิ่งออกไปหา broker ภายนอก
   */
  class LocalClient {
    constructor(opts, onFail) {
      this.opts = opts || {};
      this.onFail = onFail || null;
      this.handlers = {};
      this.pendingSubs = [];
      this.outbox = [];
      this.ended = false;
      this.sawOpen = false;
      this.failed = false;
      this.open();
    }

    on(type, fn) {
      (this.handlers[type] = this.handlers[type] || []).push(fn);
      return this;
    }

    emit(type, a, b) {
      (this.handlers[type] || []).slice().forEach((fn) => {
        try { fn(a, b); } catch (e) { console.error('[Signaling] handler error', type, e); }
      });
    }

    fail() {
      if (this.failed || this.ended) return;
      this.failed = true;
      if (this.onFail) { try { this.onFail(); } catch (e) { /* noop */ } }
    }

    open() {
      const url = `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws`;
      let ws;
      try {
        ws = new WebSocket(url);
      } catch (e) {
        this.fail();
        setTimeout(() => this.emit('close'), 0);
        return;
      }
      this.ws = ws;
      ws.onopen = () => {
        if (this.ended) { try { ws.close(); } catch (e) { /* noop */ } return; }
        this.sawOpen = true;
        // Last Will: เหมือน MQTT — ถ้าขาดการเชื่อมต่อกะทันหัน ให้เซิร์ฟเวอร์แจ้งคนอื่นทันที
        if (this.opts.will) {
          this.send({ t: 'will', topic: this.opts.will.topic, payload: encodePayload(this.opts.will.payload) });
        }
        this.flush();
        this.emit('connect');
      };
      ws.onmessage = (ev) => this.onMessage(ev.data);
      ws.onerror = () => this.fail();
      ws.onclose = () => {
        if (this.ended) return;
        this.fail();
        this.emit('close');
      };
    }

    send(obj) {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify(obj));
        return true;
      }
      this.outbox.push(obj);
      return false;
    }

    flush() {
      const queued = this.outbox.splice(0);
      queued.forEach((obj) => this.send(obj));
    }

    publish(topic, payload, opts, cb) {
      const o = opts || {};
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        if (cb) cb(new Error('not connected'));
        return;
      }
      this.send({ t: 'pub', topic, payload: encodePayload(payload), retain: !!o.retain });
      if (cb) cb(null);
    }

    subscribe(subs, cb) {
      const topics = Array.isArray(subs) ? subs : Object.keys(subs || {});
      this.pendingSubs.push(cb || null);
      this.send({ t: 'sub', topics });
    }

    unsubscribe(topics, cb) {
      const list = Array.isArray(topics) ? topics : Object.keys(topics || {});
      this.send({ t: 'unsub', topics: list });
      if (cb) cb(null, []);
    }

    end() {
      this.ended = true;
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        // บอกเซิร์ฟเวอร์ว่าปิดแบบตั้งใจ -> ห้ามยิง Last Will (เหมือน MQTT DISCONNECT)
        this.send({ t: 'bye' });
        setTimeout(() => { try { this.ws.close(); } catch (e) { /* noop */ } }, 60);
      } else if (this.ws) {
        try { this.ws.close(); } catch (e) { /* noop */ }
      }
    }

    onMessage(text) {
      let msg;
      try { msg = JSON.parse(text); } catch (e) { return; }
      if (!msg || typeof msg !== 'object') return;
      if (msg.t === 'msg') {
        this.emit('message', msg.topic, decodePayload(msg.payload));
      } else if (msg.t === 'suback') {
        const cb = this.pendingSubs.shift();
        if (cb) { try { cb(null, []); } catch (e) { /* noop */ } }
      }
      // ready / pong / unsuback: ไม่มีอะไรต้องทำฝั่ง client
    }
  }

  // ---- connection ----
  function setStatus(state, label) {
    statusLabel = label || '';
    emit('status', { state, label: statusLabel, broker: brokerName() });
  }

  function scheduleReconnect() {
    if (reconnectTimer) return;
    setStatus('reconnecting', `รอเชื่อมต่อใหม่ใน ${Math.round(retryDelay / 1000)} วิ`);
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      // ถ้า broker ปัจจุบันล่มหลายครั้ง ค่อยสลับ broker (ทุกเครื่องจะลองลำดับเดียวกัน)
      brokerFailures += 1;
      if (brokerFailures >= 3) {
        brokerFailures = 0;
        brokerIndex = (brokerIndex + 1) % BROKERS.length;
        retryDelay = 1000;
      }
      openClient();
    }, retryDelay);
    retryDelay = Math.min(retryDelay * 2, MAX_RETRY_DELAY);
  }

  function openClient() {
    const gen = ++clientGen;
    const local = useLocalSignaling();
    const broker = local ? { url: 'ws(s)://<host>/ws', name: brokerName() }
                         : BROKERS[brokerIndex % BROKERS.length];
    setStatus('connecting', `กำลังเชื่อมต่อ ${broker.name}...`);

    try {
      const opts = {
        connectTimeout: 8000,
        reconnectPeriod: 0,      // จัดการ reconnect เอง (สลับ broker ได้)
        clean: true,
        keepalive: 30,
        resubscribe: false,
        clientId: 'vox_' + Math.random().toString(36).slice(2, 11)
      };
      // Last Will: ถ้าเราขาดการเชื่อมต่อแบบกะทันหัน ให้ broker แจ้งคนอื่นให้ลบเราออกทันที
      // (ไม่ต้องรอให้ presence หมดอายุ 45 วิ)
      if (selfId) {
        opts.will = {
          topic: topicGone(selfId),
          payload: JSON.stringify({ userId: selfId, ts: Date.now() }),
          qos: 1,
          retain: false
        };
      }
      client = local
        ? new LocalClient(opts, () => {
          // LAN ใช้ไม่ได้ (เช่น เปิด URL ที่ไม่ใช่เซิร์ฟเวอร์ของเรา) -> ถอยไป broker สาธารณะ
          localFailures += 1;
          if (localFailures >= 2) forceMqtt = true;
        })
        : mqtt.connect(broker.url, opts);
    } catch (e) {
      console.error('[Signaling] connect error:', e);
      scheduleReconnect();
      return;
    }

    client.on('connect', () => {
      if (gen !== clientGen) return;
      connected = true;
      brokerFailures = 0;
      localFailures = 0;
      retryDelay = 1000;
      console.log('[Signaling] connected to', local ? 'LAN server (/ws)' : broker.url);
      setStatus('connected', `เชื่อมต่อ ${broker.name} แล้ว`);
      if (session) { joined = false; subscribeSession(); }
    });

    client.on('message', (topic, payload) => {
      if (gen !== clientGen) return;
      if (!topicMatches(topic)) return; // ข่าวของห้อง/ช่องเดิมที่ค้างอยู่ (เพิ่งเปลี่ยนห้อง) -> ทิ้ง
      const bytes = payload ? new Uint8Array(payload) : new Uint8Array(0);
      const sess = session;
      const key = sess ? sess.key : null;
      const isGone = topic.indexOf('/user/') !== -1; // ข้อความ Last Will เป็น plaintext เสมอ
      rxChain = rxChain
        .then(() => {
          if (!bytes.length) return '';
          if (key && !isGone) return decryptText(key, topic, bytes); // ถอดไม่ได้ = รหัสผ่านผิด/ข้อมูลเสีย -> ทิ้ง
          return dec8.decode(bytes);
        })
        .then((text) => {
          if (!text) return;
          if (session !== sess) return; // เปลี่ยนห้องระหว่างรอถอดรหัส -> ทิ้ง
          routeMessage(topic, text);
        })
        .catch(() => { /* ถอดรหัสไม่สำเร็จ: ทิ้งข้อความนี้ */ });
    });

    client.on('error', (err) => {
      if (gen !== clientGen) return;
      console.warn('[Signaling] broker error:', err && err.message);
    });

    client.on('close', () => {
      if (gen !== clientGen) return; // client ตัวเก่าที่เราเป็นคนปิดเอง
      const wasConnected = connected;
      connected = false;
      joined = false;
      if (announceTimer) { clearInterval(announceTimer); announceTimer = null; }
      if (sweepTimer) { clearInterval(sweepTimer); sweepTimer = null; }
      setStatus(wasConnected ? 'disconnected' : 'connecting', 'ขาดการเชื่อมต่อ');
      scheduleReconnect();
    });

    client.on('offline', () => {
      connected = false;
    });
  }

  function connect() {
    if (client) return;
    openClient();
  }

  function disconnect() {
    clientGen++; // กัน event ของ connection นี้ (close/error) เรียก reconnect หลังจากเราตั้งใจปิด
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    leave();
    if (client) {
      // ใช้ end(false) เพื่อรอให้ publish สุดท้าย (presence/ptt_idle) ถูกส่งออกก่อนปิด
      // ถ้า force=true อาจทิ้ง packet แล้วฝั่งอื่นต้องรอ presence หมดอายุ 45 วิ
      try { client.end(false); } catch (e) { /* noop */ }
      client = null;
    }
    connected = false;
    setStatus('disconnected', 'ปิดการเชื่อมต่อแล้ว');
  }

  return {
    connect, disconnect, join, leave,
    sendSignal, requestPttStart, requestPttStop, updateName,
    on,
    get connected() { return connected; },
    get channel() { return session ? session.channel : null; },
    get lanMode() { return useLocalSignaling(); }
  };
})();
