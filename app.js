/**
 * VOX Walkie-Talkie Push-to-Talk (PTT) Mobile Client
 * Ultra low-latency WebRTC + MQTT signaling (static, รันบน GitHub Pages ได้ 100%)
 */

// Application State
const state = {
  userId: localStorage.getItem('vox_user_id') || 'user_' + Math.random().toString(36).substring(2, 8),
  callSign: localStorage.getItem('vox_call_sign') || 'ALPHA-' + Math.floor(10 + Math.random() * 90),
  channel: localStorage.getItem('vox_channel') || 'CH-1 ALPHA',
  roomName: localStorage.getItem('vox_room_name') || '',
  roomPass: localStorage.getItem('vox_room_pass') || '',
  mode: localStorage.getItem('vox_mode') || 'hold', // 'hold' or 'toggle'
  volume: parseFloat(localStorage.getItem('vox_volume') || '0.85'),

  // Connection & Media
  linkConnected: false,
  localStream: null,
  localAudioTrack: null,
  peers: {}, // { peerId: { pc, stream, audioElement, pendingCandidates } }
  peerAttempts: {}, // { peerId: { count, lastAt } } กัน retry offer วนไม่รู้จบเมื่อต่อไม่ได้

  // PTT State
  isTransmitting: false,
  isChannelLocked: false,
  currentSpeakerId: null,
  currentSpeakerName: null,
  pttRequested: false,
  txStartTime: null,
  txTimerInterval: null,

  // Audio & Visualizer
  audioCtx: null,
  analyser: null,
  visualizerAnimationId: null,
  wakeLock: null
};

// Save userId
localStorage.setItem('vox_user_id', state.userId);

// DOM Elements
const dom = {
  permissionModal: document.getElementById('permissionModal'),
  btnActivateAudio: document.getElementById('btnActivateAudio'),

  pttButton: document.getElementById('pttButton'),
  pttLabel: document.getElementById('pttLabel'),
  pttHint: document.getElementById('pttHint'),
  visualizerCanvas: document.getElementById('visualizerCanvas'),

  statusBanner: document.getElementById('statusBanner'),
  statusTitle: document.getElementById('statusTitle'),
  statusSubtitle: document.getElementById('statusSubtitle'),
  txTimer: document.getElementById('txTimer'),

  netStatusBadge: document.getElementById('netStatusBadge'),
  currentChannelLabel: document.getElementById('currentChannelLabel'),
  onlineCountText: document.getElementById('onlineCountText'),
  userCallSignBadge: document.getElementById('userCallSignBadge'),
  membersList: document.getElementById('membersList'),

  modeHoldPill: document.getElementById('modeHoldPill'),
  modeTogglePill: document.getElementById('modeTogglePill'),
  btnWakeLock: document.getElementById('btnWakeLock'),

  btnSettings: document.getElementById('btnSettings'),
  settingsModal: document.getElementById('settingsModal'),
  btnCloseSettings: document.getElementById('btnCloseSettings'),
  inputCallSign: document.getElementById('inputCallSign'),
  selectChannel: document.getElementById('selectChannel'),
  rangeVolume: document.getElementById('rangeVolume'),
  selectAudioInput: document.getElementById('selectAudioInput'),
  btnSaveSettings: document.getElementById('btnSaveSettings'),

  btnChannelSelect: document.getElementById('btnChannelSelect'),
  channelModal: document.getElementById('channelModal'),
  btnCloseChannel: document.getElementById('btnCloseChannel'),

  inputPrivateRoom: document.getElementById('inputPrivateRoom'),
  inputPrivatePass: document.getElementById('inputPrivatePass'),
  btnJoinPrivate: document.getElementById('btnJoinPrivate'),
  privateHint: document.getElementById('privateHint'),

  btnQrCode: document.getElementById('btnQrCode'),
  qrModal: document.getElementById('qrModal'),
  btnCloseQr: document.getElementById('btnCloseQr'),
  qrCanvas: document.getElementById('qrCanvas'),
  qrUrlText: document.getElementById('qrUrlText'),

  modeChip: document.getElementById('modeChip'),
  btnNetStatus: document.getElementById('btnNetStatus'),
  netStatusDot: document.getElementById('netStatusDot'),
  modeChipDot: document.getElementById('modeChipDot'),
  modeChipText: document.getElementById('modeChipText'),
  modeModal: document.getElementById('modeModal'),
  btnCloseMode: document.getElementById('btnCloseMode'),
  modeCurrentTitle: document.getElementById('modeCurrentTitle'),
  modeCurrentDesc: document.getElementById('modeCurrentDesc'),
  modeConnStatus: document.getElementById('modeConnStatus'),
  btnModeOnline: document.getElementById('btnModeOnline'),
  btnModeLan: document.getElementById('btnModeLan'),
  btnModeAuto: document.getElementById('btnModeAuto'),

  toast: document.getElementById('toast'),
  remoteAudios: document.getElementById('remoteAudios')
};

// คิวส่ง offer — ทำให้การสร้าง offer ไม่ทับซ้อนกับการประมวลผลข้อความเข้า
let offerQueue = Promise.resolve();

// STUN ICE Servers Configuration for WebRTC
// โหมด LAN (ไม่มีอินเทอร์เน็ต): ตัด STUN ออก เพราะเครื่องในวงเดียวกันเจอกันได้
// ผ่าน host candidate อยู่แล้ว — การรอ STUN ที่เข้าไม่ถึงมีแต่จะหน่วงเวลา
const rtcConfig = VOX.lanMode
  ? { iceServers: [] }
  : {
    iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
      { urls: 'stun:stun2.l.google.com:19302' }
    ]
  };

// Show Toast helper
function showToast(msg) {
  dom.toast.textContent = msg;
  dom.toast.classList.add('show');
  setTimeout(() => dom.toast.classList.remove('show'), 2500);
}

// Format Seconds to MM:SS
function formatTime(seconds) {
  const m = Math.floor(seconds / 60).toString().padStart(2, '0');
  const s = Math.floor(seconds % 60).toString().padStart(2, '0');
  return `${m}:${s}`;
}

/**
 * 1. Initialize Audio and Microphone
 */
async function initMedia(deviceId = null) {
  try {
    const audioConstraints = {
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
      sampleRate: 48000
    };
    if (deviceId) {
      audioConstraints.deviceId = { exact: deviceId };
    }

    if (state.localStream) {
      state.localStream.getTracks().forEach(t => t.stop());
    }

    state.localStream = await navigator.mediaDevices.getUserMedia({
      audio: audioConstraints,
      video: false
    });

    // Extract audio track and MUTE it initially
    state.localAudioTrack = state.localStream.getAudioTracks()[0];
    state.localAudioTrack.enabled = false;

    // Setup Web Audio Analyser
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    state.audioCtx = window.audioFX.ensureContext();
    state.analyser = state.audioCtx.createAnalyser();
    state.analyser.fftSize = 64;

    const source = state.audioCtx.createMediaStreamSource(state.localStream);
    source.connect(state.analyser);

    // Populate microphone devices list in Settings
    populateAudioDevices();

    // Start canvas animation loop
    startVisualizer();

    return true;
  } catch (err) {
    console.error('Microphone access denied:', err);
    showToast('กรุณาอนุญาตให้ใช้งานไมโครโฟนบนมือถือ');
    return false;
  }
}

async function populateAudioDevices() {
  try {
    const devices = await navigator.mediaDevices.enumerateDevices();
    const audioInputs = devices.filter(d => d.kind === 'audioinput');
    dom.selectAudioInput.innerHTML = '<option value="">ไมโครโฟนเริ่มต้น (Default)</option>';
    audioInputs.forEach((device, index) => {
      const opt = document.createElement('option');
      opt.value = device.deviceId;
      opt.textContent = device.label || `ไมโครโฟน ${index + 1}`;
      dom.selectAudioInput.appendChild(opt);
    });
  } catch (e) {
    console.warn('Cannot enumerate devices:', e);
  }
}

/**
 * 2. Signaling (MQTT ผ่าน VOX) — แทนที่ WebSocket เดิม
 */
function isPrivateRoom() {
  return !!(state.roomName && state.roomPass);
}

function joinCurrentRoom() {
  VOX.join({
    userId: state.userId,
    name: state.callSign,
    channel: state.channel,
    roomName: state.roomName || null,
    password: state.roomPass || null
  });
}

function refreshRoomUi() {
  const priv = isPrivateRoom();
  dom.currentChannelLabel.textContent = priv ? `🔒 ${state.roomName}` : state.channel;
    if (dom.privateHint) {
    dom.privateHint.textContent = priv
      ? `กำลังอยู่ในห้องส่วนตัว “${state.roomName}” — กลับเป็นช่องสาธารณะได้จากปุ่มช่องสัญญาณด้านบน`
      : 'เข้ารหัสและแยกจากช่องสาธารณะ ทุกคนต้องใช้ชื่อห้อง+รหัสผ่านเดียวกัน';
  }
  if (dom.inputPrivateRoom) dom.inputPrivateRoom.value = priv ? state.roomName : '';
}

let signalingStarted = false;

// ---- โหมดสัญญาณ: แสดงบน chip ใน channel bar + modal สลับโหมด ----
// renderModeUi ถูกเรียกจาก status handler และจากปุ่มใน modeModal
let lastLinkStatus = null;

function renderModeUi() {
  if (!dom.modeChip) return;
  const mode = VOX.mode;          // 'lan' | 'mqtt'
  const forced = VOX.modeForced;  // ผู้ใช้บังคับเองหรือเปล่า
  const st = lastLinkStatus;

  // chip: ข้อความ = โหมดที่ใช้อยู่, สีจุด = สถานะเชื่อมต่อ
  dom.modeChipText.textContent = mode === 'lan' ? '📡 LAN' : '🌐 ONLINE';
  if (st && st.state === 'connected') {
    dom.modeChipDot.style.backgroundColor = 'var(--color-green)';
  } else if (!st || st.state === 'connecting' || st.state === 'reconnecting') {
    // ยังไม่เคยต่อเลย -> แสดงเหลือง (กำลังเชื่อมต่อ) ห้ามแสดงเขียวมั่วๆ
    dom.modeChipDot.style.backgroundColor = 'var(--color-yellow)';
  } else {
    dom.modeChipDot.style.backgroundColor = 'var(--color-red)';
  }
  dom.modeChip.title = `${mode === 'lan' ? 'ระบบสำรอง LAN' : 'ระบบออนไลน์'} — `
    + `${st ? (st.label || st.state) : 'ยังไม่ได้เชื่อมต่อ'} (แตะเพื่อสลับระบบ)`;

  // เนื้อหาใน modal
  if (mode === 'lan') {
    dom.modeCurrentTitle.textContent = '📡 ระบบสำรอง LAN (ในวงเดียวกัน)';
    dom.modeCurrentDesc.textContent = 'คุยกับเซิร์ฟเวอร์ lan_server.py บนเครื่องในวง Wi-Fi เดียวกัน '
      + 'ไม่ต้องพึ่งอินเทอร์เน็ตเลย — เหมาะตอนเน็ตล่มหรืออยู่พื้นที่ห่างไกล ทุกเครื่องต้องเปิด URL เดียวกัน';
  } else {
    dom.modeCurrentTitle.textContent = '🌐 ระบบออนไลน์ (อินเทอร์เน็ต)';
    dom.modeCurrentDesc.textContent = 'คุยกับ broker สาธารณะบนอินเทอร์เน็ต '
      + 'ใช้ได้ทุกที่ทุกเครือข่าย แต่ต้องมีเน็ตเท่านั้น';
  }

  dom.modeConnStatus.textContent = `สถานะ: ${st ? (st.label || st.state) : 'ยังไม่ได้เชื่อมต่อ'}`
    + (forced ? ' • บังคับด้วยมือ' : ' • อัตโนมัติตาม URL');

  dom.btnModeOnline.disabled = mode === 'mqtt';
  dom.btnModeLan.disabled = mode === 'lan';
  dom.btnModeAuto.disabled = !forced;
}

// ---- สถานะอินเทอร์เน็ต (ไอคอนข้างปุ่ม QR) ----
// navigator.onLine บอกแค่ว่า "มีการเชื่อมต่อเครือข่าย" ไม่ได้แปลว่าเน็ตใช้ได้จริง
// จึง probe ไปที่ generate_204 (no-cors, เล็กมาก) เพื่อดูว่าออกไปอินเทอร์เน็ตได้จริงไหม
let netState = 'checking';              // 'online' | 'offline' | 'checking'
const NET_PROBE_URL = 'https://www.gstatic.com/generate_204';

function renderNetIndicator() {
  if (!dom.btnNetStatus) return;
  let color;
  let label;
  if (netState === 'online') {
    color = 'var(--color-green)';
    label = 'อินเทอร์เน็ต: ใช้ได้';
  } else if (netState === 'offline') {
    color = 'var(--color-red)';
    label = 'ไม่มีอินเทอร์เน็ต — ใช้ระบบสำรอง LAN ได้';
  } else {
    color = 'var(--color-yellow)';
    label = 'กำลังตรวจสอบอินเทอร์เน็ต...';
  }
  dom.netStatusDot.style.backgroundColor = color;
  dom.btnNetStatus.style.color = color;
  dom.btnNetStatus.title = label;
  dom.btnNetStatus.setAttribute('aria-label', `สถานะอินเทอร์เน็ต: ${label}`);
}

async function probeInternet() {
  if (navigator.onLine === false) {
    netState = 'offline';
    renderNetIndicator();
    return;
  }
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 5000);
    await fetch(`${NET_PROBE_URL}?_=${Date.now()}`, {
      method: 'GET', mode: 'no-cors', cache: 'no-store', signal: ctrl.signal
    });
    clearTimeout(timer);
    netState = 'online';
  } catch (e) {
    netState = 'offline';       // ยิงออกไปไม่ถึง = ไม่มีเน็ตจริง (แม้ navigator.onLine จะบอกว่ามี)
  }
  renderNetIndicator();
}

window.addEventListener('online', probeInternet);
window.addEventListener('offline', () => {
  netState = 'offline';
  renderNetIndicator();
});

renderNetIndicator();            // เริ่มเป็นเหลือง "กำลังตรวจสอบ" ก่อน probe แรกเสร็จ
probeInternet();
setInterval(probeInternet, 20000); // ทวนทุก 20 วิ (ตอนนี้มีเน็ตแล้วเน็ตหุบ ก็เห็นทัน)

if (dom.btnNetStatus) {
  dom.btnNetStatus.addEventListener('click', () => {
    showToast(dom.btnNetStatus.title);
    probeInternet();             // แตะ = ตรวจสอบใหม่ทันที
  });
}

function connectSignaling() {
  if (signalingStarted) return; // กันผูก handler ซ้ำถ้ากดปุ่มเริ่มต้นใช้งานเร็วกว่าที่ modal จะปิด
  signalingStarted = true;

  VOX.on('status', (s) => {
    state.linkConnected = s.state === 'connected';
    const badge = dom.netStatusBadge;
    if (s.state === 'connected') {
      badge.style.backgroundColor = 'var(--color-green)';
      badge.style.boxShadow = '0 0 8px var(--color-green)';
      badge.title = `สัญญาณเชื่อมต่อผ่าน ${s.broker}`;
    } else if (s.state === 'connecting' || s.state === 'reconnecting') {
      badge.style.backgroundColor = 'var(--color-yellow)';
      badge.style.boxShadow = '0 0 8px var(--color-yellow)';
      badge.title = s.label || 'กำลังเชื่อมต่อ...';
    } else {
      badge.style.backgroundColor = 'var(--color-red)';
      badge.style.boxShadow = '0 0 8px var(--color-red)';
      badge.title = 'ขาดการเชื่อมต่อสัญญาณ';
    }
    lastLinkStatus = s;
    renderModeUi();
  });

  VOX.on('user_list', (msg) => {
    updateMembersList(msg.users);
    ensurePeers(msg.users);
  });

  VOX.on('user_joined', (msg) => {
    console.log('[Peer] User joined:', msg.user);
    window.audioFX.playJoinTone();
    showToast(`${msg.user.name} เข้าร่วมช่องสัญญาณ`);
    maybeOffer(msg.user.id);
  });

  VOX.on('user_left', (msg) => {
    console.log('[Peer] User left:', msg.userId);
    window.audioFX.playLeaveTone();
    closePeerConnection(msg.userId);
  });

  VOX.on('signal', (msg) => handleRtcSignal(msg.sender, msg.data));
  VOX.on('ptt_granted', (msg) => handlePttGranted(msg.speakerId, msg.speakerName));
  VOX.on('ptt_denied', (msg) => handlePttDenied(msg.speakerName));
  VOX.on('ptt_idle', () => handlePttIdle());

  // join() ก่อน connect() เพื่อให้ signaling รู้ userId ตั้งแต่ก่อนเปิด connection
  // (ใช้ตั้งค่า MQTT Last Will ให้คนอื่นรู้ทันทีว่าเราออกจากช่อง)
  joinCurrentRoom();
  VOX.connect();
}

/**
 * 3. WebRTC Peer Connection Logic
 *
 * บทบาท: เดิมมี "เซิร์ฟเวอร์" คอยรู้ว่าใครอยู่ก่อน ตอนนี้ไม่มีแล้ว
 * จึงใช้กติกา ฝ่ายที่ userId น้อยกว่าเป็นคนสร้าง offer เสมอ
 * เพื่อไม่ให้ offer ชนกัน (glare) ทั้งสองฝั่ง
 */
function shouldInitiate(peerId) {
  return state.userId < peerId;
}

function maybeOffer(peerId) {
  if (peerId === state.userId) return;
  if (!shouldInitiate(peerId)) return; // รอ offer จากอีกฝั่ง

  // เคยต่อกับ peer นี้ไม่สำเร็จหลายครั้ง -> หน่วงเวลา ไม่ยิง offer รัวๆ
  const rec = state.peerAttempts[peerId];
  if (rec && rec.count >= 3 && Date.now() - rec.lastAt < 30000) return;

  initiatePeerConnection(peerId, true);
}

function recordPeerFailure(peerId) {
  const rec = state.peerAttempts[peerId] || { count: 0, lastAt: 0 };
  rec.count += 1;
  rec.lastAt = Date.now();
  state.peerAttempts[peerId] = rec;
}

function clearPeerFailure(peerId) {
  delete state.peerAttempts[peerId];
}

function ensurePeers(users) {
  users.forEach((u) => {
    if (u.id === state.userId) return;
    if (!state.peers[u.id]) maybeOffer(u.id);
  });
}

function createPeerConnection(peerId) {
  if (state.peers[peerId]) {
    return state.peers[peerId].pc;
  }

  const pc = new RTCPeerConnection(rtcConfig);

  // Add our local audio track
  if (state.localAudioTrack) {
    pc.addTrack(state.localAudioTrack, state.localStream);
  }

  // ICE Candidates
  pc.onicecandidate = (event) => {
    if (event.candidate) {
      VOX.sendSignal(peerId, state.userId, { candidate: event.candidate });
    }
  };

  // Remote Stream Received
  pc.ontrack = (event) => {
    console.log('[WebRTC] Remote track received from:', peerId);
    let audioEl = document.getElementById(`audio_${peerId}`);
    if (!audioEl) {
      audioEl = document.createElement('audio');
      audioEl.id = `audio_${peerId}`;
      audioEl.autoplay = true;
      audioEl.playsInline = true;
      dom.remoteAudios.appendChild(audioEl);
    }
    audioEl.srcObject = event.streams[0];
    audioEl.volume = state.volume;
    audioEl.play().catch(e => console.warn('Autoplay error:', e));

    if (state.peers[peerId]) {
      state.peers[peerId].audioElement = audioEl;
    }
  };

  pc.onconnectionstatechange = () => {
    console.log(`[WebRTC] Peer ${peerId} state:`, pc.connectionState);
    if (pc.connectionState === 'connected') {
      clearPeerFailure(peerId); // ต่อสำเร็จ -> รีเซ็ตตัวนับ
      return;
    }
    if (pc.connectionState === 'disconnected' || pc.connectionState === 'failed') {
      recordPeerFailure(peerId);
      closePeerConnection(peerId);
    }
  };

  state.peers[peerId] = { pc };
  return pc;
}

async function initiatePeerConnection(peerId, isInitiator) {
  if (!isInitiator) return;
  // ทำต่อคิวเดียวกัน เพื่อกัน offer ชนกับ SDP ที่กำลังจะมาถึง (glare)
  offerQueue = offerQueue.then(() => sendOffer(peerId)).catch((e) => console.error('[WebRTC] Create offer error:', e));
}

async function sendOffer(peerId) {
  if (peerId === state.userId) return;
  const pc = createPeerConnection(peerId);
  if (pc.signalingState !== 'stable') return; // มี offer ค้างอยู่แล้ว
  try {
    const offer = await pc.createOffer({
      offerToReceiveAudio: true,
      offerToReceiveVideo: false
    });
    await pc.setLocalDescription(offer);
    VOX.sendSignal(peerId, state.userId, { sdp: pc.localDescription });
  } catch (e) {
    console.error('[WebRTC] Create offer error:', e);
  }
}

async function handleRtcSignal(senderId, data) {
  const pc = createPeerConnection(senderId);

  try {
    if (data.sdp) {
      // Guard สถานะที่ใช้ไม่ได้ (มักเกิดตอน reconnect แล้ว connection ถูกสร้างใหม่)
      if (data.sdp.type === 'answer' && pc.signalingState !== 'have-local-offer') return;
      if (data.sdp.type === 'offer' && pc.signalingState === 'have-remote-offer') return;

      // Offer ชนกัน (glare): เก็บ offer ของเราไว้ถ้าเราเป็นฝ่ายที่กำหนดไว้ต้องเป็นคน offer
      if (data.sdp.type === 'offer' && pc.signalingState === 'have-local-offer') {
        if (shouldInitiate(senderId)) {
          console.warn('[WebRTC] glare: keep our offer, ignore from', senderId);
          return;
        }
        try {
          await pc.setLocalDescription({ type: 'rollback' });
        } catch (e) {
          console.warn('[WebRTC] rollback unsupported, ignoring remote offer', e);
          return;
        }
      }

      await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));

      // flush ICE ที่มาก่อน remote description
      const rec = state.peers[senderId];
      if (rec && rec.pendingCandidates && rec.pendingCandidates.length) {
        const queued = rec.pendingCandidates.splice(0);
        for (const cand of queued) {
          try { await pc.addIceCandidate(new RTCIceCandidate(cand)); } catch (e) { console.warn('[WebRTC] queued candidate error:', e); }
        }
      }

      if (data.sdp.type === 'offer') {
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        VOX.sendSignal(senderId, state.userId, { sdp: pc.localDescription });
      }
    } else if (data.candidate) {
      if (pc.remoteDescription) {
        await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
      } else {
        // remote description ยังมาไม่ถึง -> เก็บไว้ flush ทีหลัง
        const rec = state.peers[senderId];
        if (rec) {
          rec.pendingCandidates = rec.pendingCandidates || [];
          rec.pendingCandidates.push(data.candidate);
        }
      }
    }
  } catch (e) {
    console.error('[WebRTC] Signaling error:', e);
  }
}

function closePeerConnection(peerId) {
  if (state.peers[peerId]) {
    try {
      state.peers[peerId].pc.close();
    } catch (e) {}
    const audioEl = document.getElementById(`audio_${peerId}`);
    if (audioEl) audioEl.remove();
    delete state.peers[peerId];
  }
}

function cleanupAllPeers() {
  Object.keys(state.peers).forEach(peerId => closePeerConnection(peerId));
}

/**
 * 4. Push-To-Talk (PTT) Arbitration Handlers
 *    การตัดสินว่าใครมีสิทธิ์พูดเกิดขึ้นบน broker (retained state) แทนเซิร์ฟเวอร์ Python
 */
function requestPttStart() {
  if (state.isTransmitting) return;
  if (state.isChannelLocked && state.currentSpeakerId !== state.userId) {
    // Channel is already occupied by someone else
    window.audioFX.playBusyTone();
    if (navigator.vibrate) navigator.vibrate([40, 60, 40]);
    showToast(`ช่องสัญญาณไม่ว่าง (${state.currentSpeakerName} กำลังพูด)`);
    return;
  }

  const res = VOX.requestPttStart();
  if (res && res.ok === false) {
    if (res.reason === 'busy') {
      handlePttDenied(res.speakerName || 'ผู้ใช้อื่น');
    } else {
      showToast('ยังเชื่อมต่อเซิร์ฟสัญญาณไม่ได้ กรุณารอสักครู่');
    }
    return;
  }

  state.pttRequested = true;
}

function requestPttStop() {
  if (!state.isTransmitting && !state.pttRequested) return;
  state.pttRequested = false;
  VOX.requestPttStop();
}

function handlePttGranted(speakerId, speakerName) {
  state.isChannelLocked = true;
  state.currentSpeakerId = speakerId;
  state.currentSpeakerName = speakerName;

  if (speakerId === state.userId) {
    // I am transmitting!
    state.isTransmitting = true;
    if (state.localAudioTrack) {
      state.localAudioTrack.enabled = true; // Unmute mic
    }

    // Play walkie-talkie chirp & vibrate
    window.audioFX.playPttStart();
    if (navigator.vibrate) navigator.vibrate(50);

    // Update UI
    dom.pttButton.classList.add('transmitting');
    dom.pttButton.classList.remove('receiving', 'busy');
    dom.pttLabel.textContent = 'กำลังส่งสัญญาณ...';
    dom.pttHint.textContent = 'TRANSMITTING';

    dom.statusBanner.className = 'status-banner transmitting';
    dom.statusTitle.textContent = 'กำลังส่งสัญญาณเสียง (TRANSMITTING)';
    dom.statusSubtitle.textContent = `นามเรียกขาน: ${state.callSign}`;

    // Start Timer
    startTxTimer();
  } else {
    // Someone else is transmitting (หรือเราเพิ่งถูกแย่งล็อกจากการกดพร้อมกัน)
    if (state.isTransmitting && state.localAudioTrack) {
      state.localAudioTrack.enabled = false; // ปิดไมค์ทันทีถ้าเพิ่งเสียล็อก
    }
    state.isTransmitting = false;
    dom.pttButton.classList.add('receiving');
    dom.pttButton.classList.remove('transmitting', 'busy');
    dom.pttLabel.textContent = 'กำลังรับฟัง...';
    dom.pttHint.textContent = 'RECEIVING';

    dom.statusBanner.className = 'status-banner receiving';
    dom.statusTitle.textContent = 'กำลังรับฟัง (RECEIVING)';
    dom.statusSubtitle.textContent = `ผู้พูด: ${speakerName}`;
    startTxTimer();
  }

  updateMemberSpeakingState(speakerId, true);
}

function handlePttDenied(speakerName) {
  state.pttRequested = false;
  window.audioFX.playBusyTone();
  if (navigator.vibrate) navigator.vibrate([40, 60, 40]);

  dom.statusBanner.className = 'status-banner busy';
  dom.statusTitle.textContent = 'ช่องสัญญาณไม่ว่าง (BUSY)';
  dom.statusSubtitle.textContent = `${speakerName} กำลังใช้งานช่องสัญญาณ`;
  showToast(`ช่องสัญญาณกำลังถูกใช้งานโดย ${speakerName}`);

  setTimeout(() => {
    if (!state.isChannelLocked) {
      dom.statusBanner.className = 'status-banner';
      dom.statusTitle.textContent = 'พร้อมใช้งาน (STANDBY)';
      dom.statusSubtitle.textContent = 'ไม่มีผู้ใช้งานส่งสัญญาณ';
    }
  }, 2000);
}

function handlePttIdle(silent) {
  state.isTransmitting = false;
  state.isChannelLocked = false;
  state.currentSpeakerId = null;
  state.currentSpeakerName = null;
  state.pttRequested = false;

  // Mute local mic
  if (state.localAudioTrack) {
    state.localAudioTrack.enabled = false;
  }

  // Play Roger Beep!
  if (!silent) window.audioFX.playRogerBeep();

  // Reset UI
  dom.pttButton.classList.remove('transmitting', 'receiving', 'busy');
  dom.pttLabel.textContent = state.mode === 'hold' ? 'กดค้างเพื่อพูด' : 'แตะเพื่อพูด';
  dom.pttHint.textContent = state.mode === 'hold' ? 'HOLD TO TALK' : 'TAP TO TALK';

  dom.statusBanner.className = 'status-banner';
  dom.statusTitle.textContent = 'พร้อมใช้งาน (STANDBY)';
  dom.statusSubtitle.textContent = 'ไม่มีผู้ใช้งานส่งสัญญาณ';

  stopTxTimer();
  updateMemberSpeakingState(null, false);
}

// Transmission Timer
function startTxTimer() {
  stopTxTimer();
  state.txStartTime = Date.now();
  dom.txTimer.textContent = '00:00';
  state.txTimerInterval = setInterval(() => {
    const elapsed = Math.floor((Date.now() - state.txStartTime) / 1000);
    dom.txTimer.textContent = formatTime(elapsed);
  }, 500);
}

function stopTxTimer() {
  if (state.txTimerInterval) {
    clearInterval(state.txTimerInterval);
    state.txTimerInterval = null;
  }
  dom.txTimer.textContent = '00:00';
}

/**
 * 5. Visualizer Canvas (Audio Waveform Ring)
 */
function startVisualizer() {
  const canvas = dom.visualizerCanvas;
  const ctx = canvas.getContext('2d');
  const bufferLength = state.analyser ? state.analyser.frequencyBinCount : 32;
  const dataArray = new Uint8Array(bufferLength);

  function draw() {
    state.visualizerAnimationId = requestAnimationFrame(draw);

    ctx.clearRect(0, 0, canvas.width, canvas.height);

    const centerX = canvas.width / 2;
    const centerY = canvas.height / 2;
    const baseRadius = 110;

    let avgVolume = 0;
    if (state.analyser && (state.isTransmitting || state.isChannelLocked)) {
      state.analyser.getByteFrequencyData(dataArray);
      let sum = 0;
      for (let i = 0; i < bufferLength; i++) {
        sum += dataArray[i];
      }
      avgVolume = (sum / bufferLength) / 255; // 0.0 to 1.0
    }

    if (avgVolume > 0.05) {
      // Draw dynamic pulse wave rings
      const pulseRadius = baseRadius + avgVolume * 45;
      const color = state.isTransmitting ? 'rgba(239, 68, 68, ' : 'rgba(6, 182, 212, ';

      ctx.beginPath();
      ctx.arc(centerX, centerY, pulseRadius, 0, 2 * Math.PI);
      ctx.strokeStyle = color + (0.6 * avgVolume) + ')';
      ctx.lineWidth = 4;
      ctx.stroke();

      ctx.beginPath();
      ctx.arc(centerX, centerY, pulseRadius + 14, 0, 2 * Math.PI);
      ctx.strokeStyle = color + (0.3 * avgVolume) + ')';
      ctx.lineWidth = 2;
      ctx.stroke();
    }
  }

  draw();
}

/**
 * 6. User Interface & Channel Participants
 */
function updateMembersList(users) {
  dom.onlineCountText.textContent = `${users.length} ออนไลน์`;
  dom.membersList.innerHTML = '';

  users.forEach(user => {
    const isMe = user.id === state.userId;
    const isSpeaking = user.id === state.currentSpeakerId;

    const chip = document.createElement('div');
    chip.id = `member_${user.id}`;
    chip.className = `member-chip ${isMe ? 'me' : ''} ${isSpeaking ? 'speaking' : ''}`;
    chip.innerHTML = `
      <div class="member-dot"></div>
      <span>${user.name} ${isMe ? '(ฉัน)' : ''}</span>
    `;
    dom.membersList.appendChild(chip);
  });
}

function updateMemberSpeakingState(speakerId, isSpeaking) {
  const allChips = dom.membersList.querySelectorAll('.member-chip');
  allChips.forEach(chip => chip.classList.remove('speaking'));

  if (isSpeaking && speakerId) {
    const target = document.getElementById(`member_${speakerId}`);
    if (target) target.classList.add('speaking');
  }
}

function switchChannel(newChannel) {
  const wasPrivate = isPrivateRoom();
  const channelChanged = state.channel !== newChannel;
  // ช่องเดิมไม่ต้องทำอะไร ยกเว้นกรณีอยู่ห้องส่วนตัว -> ถือเป็นการออกจากห้อง
  if (!channelChanged && !wasPrivate) return;

  if (state.isTransmitting) {
    requestPttStop();
  }

  cleanupAllPeers();
  handlePttIdle(true); // รีเซ็ต UI แบบเงียบ (ไม่เล่น roger beep)
  state.channel = newChannel;
  localStorage.setItem('vox_channel', newChannel);

  // เลือกช่องสาธารณะ = ออกจากห้องส่วนตัว
  state.roomName = '';
  state.roomPass = '';
  localStorage.removeItem('vox_room_name');
  localStorage.removeItem('vox_room_pass');
  if (dom.inputPrivateRoom) dom.inputPrivateRoom.value = '';
  if (dom.inputPrivatePass) dom.inputPrivatePass.value = '';

  dom.selectChannel.value = newChannel;
  refreshRoomUi();

  // VOX.join() จะ unsubscribe ห้อง/ช่องเก่า + เคลียร์ presence/lock ให้เอง
  joinCurrentRoom();

  showToast(wasPrivate && !channelChanged
    ? `ออกจากห้องส่วนตัว กลับมาที่ ${newChannel}`
    : `เปลี่ยนช่องสัญญาณเป็น ${newChannel}`);
}

/**
 * 7. Screen Wake Lock (Keep Screen Awake)
 */
async function toggleWakeLock() {
  if ('wakeLock' in navigator) {
    try {
      if (!state.wakeLock) {
        state.wakeLock = await navigator.wakeLock.request('screen');
        dom.btnWakeLock.style.color = 'var(--color-green)';
        showToast('เปิดการป้องกันจอดับแล้ว');
        state.wakeLock.addEventListener('release', () => {
          state.wakeLock = null;
          dom.btnWakeLock.style.color = 'var(--text-main)';
        });
      } else {
        await state.wakeLock.release();
        state.wakeLock = null;
        dom.btnWakeLock.style.color = 'var(--text-main)';
        showToast('ปิดการป้องกันจอดับแล้ว');
      }
    } catch (err) {
      console.warn('Wake Lock error:', err);
    }
  } else {
    showToast('เบราว์เซอร์นี้ไม่รองรับ Screen Wake Lock');
  }
}

/**
 * 8. Setup Event Listeners
 */
function setupEventListeners() {
  // Activate Audio Modal
  dom.btnActivateAudio.addEventListener('click', async () => {
    window.audioFX.ensureContext();
    const success = await initMedia();
    if (success) {
      dom.permissionModal.style.display = 'none';
      connectSignaling();
      toggleWakeLock(); // Auto-enable keep screen awake
    }
  });

  // PTT Touch & Mouse Handling
  const btn = dom.pttButton;

  // Handle Touch Events (Mobile)
  btn.addEventListener('touchstart', (e) => {
    e.preventDefault();
    if (state.mode === 'hold') {
      requestPttStart();
    } else {
      // Toggle mode
      if (state.isTransmitting) {
        requestPttStop();
      } else {
        requestPttStart();
      }
    }
  }, { passive: false });

  btn.addEventListener('touchend', (e) => {
    e.preventDefault();
    if (state.mode === 'hold') {
      requestPttStop();
    }
  }, { passive: false });

  btn.addEventListener('touchcancel', (e) => {
    e.preventDefault();
    if (state.mode === 'hold') {
      requestPttStop();
    }
  }, { passive: false });

  // Handle Mouse Events (Desktop testing)
  btn.addEventListener('mousedown', (e) => {
    if (e.button !== 0) return;
    if (state.mode === 'hold') {
      requestPttStart();
    } else {
      if (state.isTransmitting) {
        requestPttStop();
      } else {
        requestPttStart();
      }
    }
  });

  btn.addEventListener('mouseup', () => {
    if (state.mode === 'hold') {
      requestPttStop();
    }
  });

  btn.addEventListener('mouseleave', () => {
    if (state.mode === 'hold' && state.isTransmitting) {
      requestPttStop();
    }
  });

  // Keyboard Spacebar shortcut for desktop
  window.addEventListener('keydown', (e) => {
    if (e.code === 'Space' && !e.repeat && document.activeElement.tagName !== 'INPUT') {
      e.preventDefault();
      if (state.mode === 'hold') {
        requestPttStart();
      } else {
        if (state.isTransmitting) requestPttStop();
        else requestPttStart();
      }
    }
  });

  window.addEventListener('keyup', (e) => {
    if (e.code === 'Space' && document.activeElement.tagName !== 'INPUT') {
      e.preventDefault();
      if (state.mode === 'hold') {
        requestPttStop();
      }
    }
  });

  // Mode Selection Pills
  dom.modeHoldPill.addEventListener('click', () => {
    state.mode = 'hold';
    localStorage.setItem('vox_mode', 'hold');
    dom.modeHoldPill.classList.add('active');
    dom.modeTogglePill.classList.remove('active');
    dom.pttLabel.textContent = 'กดค้างเพื่อพูด';
    dom.pttHint.textContent = 'HOLD TO TALK';
  });

  dom.modeTogglePill.addEventListener('click', () => {
    state.mode = 'toggle';
    localStorage.setItem('vox_mode', 'toggle');
    dom.modeTogglePill.classList.add('active');
    dom.modeHoldPill.classList.remove('active');
    dom.pttLabel.textContent = 'แตะเพื่อพูด';
    dom.pttHint.textContent = 'TAP TO TALK';
  });

  // Channel Bar click -> Open Channel Modal
  dom.btnChannelSelect.addEventListener('click', () => {
    dom.channelModal.classList.add('open');
  });

  dom.btnCloseChannel.addEventListener('click', () => {
    dom.channelModal.classList.remove('open');
  });

  // QR Code Modal
  if (dom.btnQrCode) {
    dom.btnQrCode.addEventListener('click', () => {
      const currentUrl = window.location.href.split('#')[0].split('?')[0];
      dom.qrUrlText.textContent = currentUrl;
      if (window.renderQRCode && dom.qrCanvas) {
        window.renderQRCode(dom.qrCanvas, currentUrl);
      }
      dom.qrModal.classList.add('open');
    });

    dom.btnCloseQr.addEventListener('click', () => {
      dom.qrModal.classList.remove('open');
    });
  }

  // ---- โหมดสัญญาณ: แสดงสถานะ + ปุ่มสลับเอง (ระบบออนไลน์ <-> ระบบสำรอง LAN) ----
  if (dom.modeChip) {
    dom.modeChip.addEventListener('click', () => {
      renderModeUi();
      dom.modeModal.classList.add('open');
    });

    dom.btnCloseMode.addEventListener('click', () => {
      dom.modeModal.classList.remove('open');
    });

    const switchMode = (m) => {
      VOX.setMode(m);
      renderModeUi();
      dom.modeModal.classList.remove('open');
      showToast(
        m === 'lan' ? 'สลับเป็นระบบสำรอง LAN แล้ว'
          : m === 'mqtt' ? 'สลับเป็นระบบออนไลน์แล้ว'
            : 'กลับเป็นโหมดอัตโนมัติตาม URL แล้ว'
      );
    };

    dom.btnModeOnline.addEventListener('click', () => switchMode('mqtt'));
    dom.btnModeLan.addEventListener('click', () => switchMode('lan'));
    dom.btnModeAuto.addEventListener('click', () => switchMode('auto'));
    renderModeUi(); // แสดงโหมดที่คาดไว้ตั้งแต่ก่อนเชื่อมต่อ
  }

  dom.channelModal.querySelectorAll('button[data-chan]').forEach(btn => {
    btn.addEventListener('click', () => {
      const chan = btn.getAttribute('data-chan');
      switchChannel(chan);
      dom.channelModal.classList.remove('open');
    });
  });

  // Private Room (ในช่องเลือกช่องสัญญาณ)
  if (dom.btnJoinPrivate) {
    dom.btnJoinPrivate.addEventListener('click', () => {
      const roomName = dom.inputPrivateRoom.value.trim();
      const roomPass = dom.inputPrivatePass.value;
      if (!roomName || !roomPass) {
        showToast('กรุณากรอกชื่อห้องและรหัสผ่าน');
        return;
      }
      state.roomName = roomName;
      state.roomPass = roomPass;
      localStorage.setItem('vox_room_name', roomName);
      localStorage.setItem('vox_room_pass', roomPass);

      cleanupAllPeers();
      handlePttIdle(true);
      refreshRoomUi();
      joinCurrentRoom(); // VOX จะ derive key ใหม่ + เปิด connection ใหม่ถ้าห้องเปลี่ยน
      dom.channelModal.classList.remove('open');
      showToast(`เข้าห้องส่วนตัว “${roomName}” แล้ว`);
    });
  }

  // Settings Modal
  dom.btnSettings.addEventListener('click', () => {
    dom.inputCallSign.value = state.callSign;
    dom.selectChannel.value = state.channel;
    dom.rangeVolume.value = Math.round(state.volume * 100);
    dom.settingsModal.classList.add('open');
  });

  dom.btnCloseSettings.addEventListener('click', () => {
    dom.settingsModal.classList.remove('open');
  });

  dom.btnSaveSettings.addEventListener('click', () => {
    const newName = dom.inputCallSign.value.trim();
    if (newName) {
      state.callSign = newName;
      localStorage.setItem('vox_call_sign', newName);
      dom.userCallSignBadge.textContent = `นามเรียกขาน: ${newName}`;
      VOX.updateName(newName);
    }

    const newChan = dom.selectChannel.value;
    if (newChan !== state.channel) {
      switchChannel(newChan); // จะล้างห้องส่วนตัวให้อัตโนมัติ
    }

    const newVol = parseInt(dom.rangeVolume.value, 10) / 100;
    state.volume = newVol;
    localStorage.setItem('vox_volume', newVol.toString());
    window.audioFX.setVolume(newVol);

    // Apply volume to all remote audio elements
    dom.remoteAudios.querySelectorAll('audio').forEach(a => a.volume = newVol);

    const newDeviceId = dom.selectAudioInput.value;
    if (newDeviceId) {
      initMedia(newDeviceId);
    }

    dom.settingsModal.classList.remove('open');
    showToast('บันทึกการตั้งค่าเรียบร้อย');
  });

  // Screen Wake Lock Button
  dom.btnWakeLock.addEventListener('click', toggleWakeLock);

  // Setup initial UI states
  dom.userCallSignBadge.textContent = `นามเรียกขาน: ${state.callSign}`;
  refreshRoomUi();
  dom.selectChannel.value = state.channel;
  if (state.mode === 'toggle') {
    dom.modeTogglePill.classList.add('active');
    dom.modeHoldPill.classList.remove('active');
    dom.pttLabel.textContent = 'แตะเพื่อพูด';
    dom.pttHint.textContent = 'TAP TO TALK';
  }
}

// Auto-run setup when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
  setupEventListeners();
});

// ปิดหน้า/เปลี่ยนหน้า -> เคลียร์ presence + ปล่อยล็อก PTT ทันที
// (ไม่ต้องรอให้ presence หมดอายุ 45 วิ)
window.addEventListener('pagehide', () => {
  VOX.disconnect();
});
