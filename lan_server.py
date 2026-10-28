#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VOX LAN Server — เซิร์ฟเวอร์สำรองไว้ใช้ "เมื่อไม่มีอินเทอร์เน็ต"

ทำสองอย่างในพอร์ตเดียว:
  1. เสิร์ฟไฟล์แอป (static) ให้ทุกเครื่องใน Wi-Fi วงเดียวกันเปิดได้
  2. รัน signaling ผ่าน WebSocket ที่ /ws — pub/sub + retained + Last Will
     ซึ่งมีความหมายเดียวกับ MQTT ที่ signaling.js ใช้บนอินเทอร์เน็ต
     (ดังนั้นเมื่อเปิดแอปจากเซิร์ฟเวอร์นี้ signaling.js จะสลับมาใช้
      transport ท้องถิ่นอัตโนมัติ ไม่ต้องแตะ broker สาธารณะเลย)

ใช้เฉพาะ Python standard library — ไม่ต้อง pip install อะไรทั้งสิ้น
(ทดสอบกับ Python 3.8+)

วิธีใช้:
    python lan_server.py
แล้วเปิด https://<IP ของเครื่องนี้>:8443 จากมือถือในวงเดียวกัน
หน้าแรกจะขึ้นหน้าเตือนใบรับรอง self-signed -> กด "ขั้นสูง" -> ไปต่อ
จึงจะใช้ไมโครโฟนได้ (เบราว์เซอร์อนุญาตไมโครโฟนเฉพาะ secure context)

หมายเหตุ: ใบรับรอง (cert.pem/key.pem) อยู่ใน .gitignore อยู่แล้ว ห้าม commit
"""

import argparse
import base64
import hashlib
import json
import mimetypes
import os
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

ROOT = os.path.dirname(os.path.abspath(__file__))
WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'


def _fix_stdio():
    """ Windows: คอนโซลบางเครื่องเป็น cp1252 -> พิมพ์ไทยแล้ว crash
    ให้สลับเป็น UTF-8 ถ้า encoding เดิมไม่รองรับไทย (คุมเองไม่ได้ในบาง environment) """
    for stream in (sys.stdout, sys.stderr):
        try:
            enc = stream.encoding or 'utf-8'
            '\u0e1a\u0e23\u0e34\u0e29\u0e31\u0e17'.encode(enc)   # "ภาษา"
            stream.reconfigure(errors='replace')
        except Exception:
            try:
                stream.reconfigure(encoding='utf-8', errors='replace')
            except Exception:
                pass


_fix_stdio()

# ไฟล์/โฟลเดอร์ที่ห้ามเสิร์ฟเด็ดขาด (แม้จะไม่ได้อยู่ใน .gitignore ก็ตาม)
BLOCKED_EXT = ('.pem', '.key', '.crt', '.py', '.pyc')
BLOCKED_DIRS = ('__pycache__',)


# ---------------------------------------------------------------------------
# Topic matching (รองรับ + กับ # เหมือน MQTT)
# ---------------------------------------------------------------------------
def topic_matches(topic_filter, topic):
    f = topic_filter.split('/')
    t = topic.split('/')
    for i, part in enumerate(f):
        if part == '#':
            return True
        if i >= len(t):
            return False
        if part != '+' and part != t[i]:
            return False
    return len(f) == len(t)


# ---------------------------------------------------------------------------
# Broker: pub/sub + retained + Last Will
# ---------------------------------------------------------------------------
class Conn:
    """สถานะการเชื่อมต่อของ client หนึ่งตัว"""

    def __init__(self, handler):
        self.handler = handler
        self.subs = set()        # topic filter ที่ subscribe ไว้
        self.will = None         # (topic, payload)
        self.said_bye = False    # disconnect แบบตั้งใจ -> ห้ามยิง will
        self.write_lock = threading.Lock()
        self.closed = False

    def send(self, obj):
        data = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        with self.write_lock:
            if self.closed:
                return
            try:
                self.handler.send_ws_frame(data, 0x1)
            except Exception:
                self.closed = True


class Broker:
    def __init__(self):
        self.lock = threading.Lock()
        self.retained = {}   # topic -> payload (str หรือ dict {'b64': ...})
        self.conns = set()

    # ---- lifecycle ----
    def attach(self, conn):
        with self.lock:
            self.conns.add(conn)

    def detach(self, conn):
        with self.lock:
            self.conns.discard(conn)
            will = conn.will if not conn.said_bye else None
        if will:
            # client ขาดการเชื่อมต่อแบบกะทันหัน -> บอกคนอื่นให้ลบออกทันที
            self.publish(will[0], will[1], retain=False, sender=None)

    # ---- operations ----
    def subscribe(self, conn, topics):
        retained_now = []
        with self.lock:
            for t in topics:
                conn.subs.add(t)
            for topic, payload in self.retained.items():
                if any(topic_matches(f, topic) for f in conn.subs):
                    retained_now.append((topic, payload))
        conn.send({'t': 'suback', 'topics': list(topics)})
        # ส่ง retained หลัง suback (เหมือน MQTT: retained ตามมาหลัง SUBACK)
        for topic, payload in retained_now:
            conn.send({'t': 'msg', 'topic': topic, 'payload': payload, 'r': True})

    def unsubscribe(self, conn, topics):
        with self.lock:
            for t in topics:
                conn.subs.discard(t)
        conn.send({'t': 'unsuback', 'topics': list(topics)})

    def set_will(self, conn, topic, payload):
        conn.will = (topic, payload) if topic else None

    def publish(self, topic, payload, retain, sender):
        # retained: payload ว่าง = ล้าง retained ออก (เหมือน MQTT)
        with self.lock:
            if retain:
                if payload in ('', None):
                    self.retained.pop(topic, None)
                else:
                    self.retained[topic] = payload
            targets = [c for c in self.conns
                       if any(topic_matches(f, topic) for f in c.subs)]
        # ส่งถึง subscriber ทุกตัว "รวมถึงผู้ส่งเอง" เพื่อให้ได้ echo ของ retained
        # (signaling.js อาศัย echo นี้ตอนปล่อยล็อก PTT)
        for c in targets:
            c.send({'t': 'msg', 'topic': topic, 'payload': payload})

    def handle(self, conn, msg):
        t = msg.get('t')
        if t == 'sub':
            self.subscribe(conn, [str(x) for x in msg.get('topics', [])])
        elif t == 'unsub':
            self.unsubscribe(conn, [str(x) for x in msg.get('topics', [])])
        elif t == 'pub':
            topic = str(msg.get('topic') or '')
            if topic:
                self.publish(topic, msg.get('payload'), bool(msg.get('retain')), conn)
        elif t == 'will':
            self.set_will(conn, str(msg.get('topic') or ''), msg.get('payload'))
        elif t == 'bye':
            conn.said_bye = True
        elif t == 'ping':
            conn.send({'t': 'pong'})

    @property
    def client_count(self):
        with self.lock:
            return len(self.conns)


BROKER = Broker()


# ---------------------------------------------------------------------------
# WebSocket framing (server side, text frames เป็นหลัก)
# ---------------------------------------------------------------------------
def read_ws_frame(rfile):
    """อ่านหนึ่ง frame -> (opcode, payload) หรือ None ถ้า connection ปิด"""
    header = rfile.read(2)
    if len(header) < 2:
        return None
    opcode = header[0] & 0x0F
    masked = header[1] & 0x80
    length = header[1] & 0x7F
    if length == 126:
        raw = rfile.read(2)
        if len(raw) < 2:
            return None
        length = struct.unpack('!H', raw)[0]
    elif length == 127:
        raw = rfile.read(8)
        if len(raw) < 8:
            return None
        length = struct.unpack('!Q', raw)[0]
    mask = rfile.read(4) if masked else None
    if length:
        payload = rfile.read(length)
        if len(payload) < length:
            return None
    else:
        payload = b''
    if mask:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return opcode, payload


# ---------------------------------------------------------------------------
# HTTP handler: static files + /ws
# ---------------------------------------------------------------------------
class VOXHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    server_version = 'VOXLAN/1.0'

    # --- helpers ---
    def send_ws_frame(self, data, opcode=0x1):
        """data: bytes  (server ไม่ต้อง mask)"""
        length = len(data)
        if length < 126:
            header = struct.pack('!BB', 0x80 | opcode, length)
        elif length < 65536:
            header = struct.pack('!BBH', 0x80 | opcode, 126, length)
        else:
            header = struct.pack('!BBQ', 0x80 | opcode, 127, length)
        self.wfile.write(header + data)
        self.wfile.flush()

    def _resolve_path(self, url_path):
        """คืนพาธไฟล์เต็ม หรือ None ถ้าไม่อนุญาต"""
        path = unquote(urlsplit(url_path).path)
        if path in ('', '/'):
            path = '/index.html'
        parts = [p for p in path.split('/') if p]
        if not parts:
            return None
        # กัน path traversal และไฟล์ที่ไม่ควรเผยแพร่
        if any(p == '..' or p.startswith('.') or p in BLOCKED_DIRS for p in parts):
            return None
        if parts[-1].lower().endswith(BLOCKED_EXT):
            return None
        full = os.path.join(ROOT, *parts)
        if not os.path.abspath(full).startswith(ROOT + os.sep):
            return None
        if not os.path.isfile(full):
            return None
        return full

    # --- static ---
    def do_GET(self):
        path = urlsplit(self.path).path
        if path == '/ws' or path.startswith('/ws?'):
            self.handle_ws()
            return
        self.serve_static()

    def do_HEAD(self):
        self.serve_static(head_only=True)

    def serve_static(self, head_only=False):
        full = self._resolve_path(self.path)
        if not full:
            self.send_error(404, 'Not Found')
            return
        try:
            with open(full, 'rb') as fh:
                data = fh.read()
        except OSError:
            self.send_error(404, 'Not Found')
            return

        ctype, _ = mimetypes.guess_type(full)
        if not ctype:
            ctype = 'application/octet-stream'
        if ctype.startswith('text/') or ctype in ('application/javascript', 'application/json'):
            ctype += '; charset=utf-8'

        self.send_response(200)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')  # ทดสอบพัฒนาได้ทันที ไม่ค้าง cache
        self.end_headers()
        if not head_only:
            self.wfile.write(data)

    # --- websocket ---
    def handle_ws(self):
        key = self.headers.get('Sec-WebSocket-Key')
        upgrade = (self.headers.get('Upgrade') or '').lower()
        if not key or upgrade != 'websocket':
            self.send_error(400, 'Bad WebSocket request')
            return

        accept = base64.b64encode(
            hashlib.sha1((key + WS_GUID).encode('ascii')).digest()).decode('ascii')
        self.send_response(101, 'Switching Protocols')
        self.send_header('Upgrade', 'websocket')
        self.send_header('Connection', 'Upgrade')
        self.send_header('Sec-WebSocket-Accept', accept)
        self.end_headers()

        conn = Conn(self)
        BROKER.attach(conn)
        conn.send({'t': 'ready'})
        peer = '%s:%s' % self.client_address[:2]
        print(f'  + client เข้าร่วม ({BROKER.client_count} คนออนไลน์) [{peer}]')

        try:
            while True:
                frame = read_ws_frame(self.rfile)
                if frame is None:
                    break
                opcode, payload = frame
                if opcode == 0x8:            # close
                    try:
                        self.send_ws_frame(struct.pack('!H', 1000), 0x8)
                    except Exception:
                        pass
                    break
                if opcode == 0x9:            # ping -> pong
                    try:
                        self.send_ws_frame(payload, 0xA)
                    except Exception:
                        break
                    continue
                if opcode != 0x1:            # ใช้แค่ text frame
                    continue
                try:
                    msg = json.loads(payload.decode('utf-8'))
                except (ValueError, UnicodeDecodeError):
                    continue
                if isinstance(msg, dict):
                    try:
                        BROKER.handle(conn, msg)
                    except Exception as exc:  # กัน client เดียวทำให้เซิร์ฟเวอร์ล่ม
                        print('  ! error:', exc)
        finally:
            with BROKER.lock:
                was_here = conn in BROKER.conns
            BROKER.detach(conn)
            if was_here:
                print(f'  - client ออก ({BROKER.client_count} คนออนไลน์) [{peer}]')
            try:
                self.close_connection = True
                self.connection.close()
            except Exception:
                pass

    def log_message(self, fmt, *args):
        # ไม่พิมพ์ทุก request (กัน console รก) — เก็บเฉพาะ 404 ไว้ช่วยดีบัก
        if any('404' in str(a) for a in args):
            print('  404:', self.path)


# ---------------------------------------------------------------------------
# TLS (self-signed) เพื่อให้มือถือใช้ไมโครโฟนได้
# ---------------------------------------------------------------------------
def lan_ips():
    ips = []
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))       # ไม่ได้ส่งแพ็กเกตจริง แค่อ่าน routing table
        ips.append(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if ip not in ips:
                ips.append(ip)
    except Exception:
        pass
    return [ip for ip in ips if not ip.startswith('127.')] or ips


def ensure_cert(cert_path, key_path):
    """สร้างใบรับรอง self-signed (มี SAN เป็น IP ของเครื่อง) ถ้ายังไม่มี"""
    if os.path.isfile(cert_path) and os.path.isfile(key_path):
        return True
    openssl = shutil.which('openssl')
    if not openssl:
        return False
    sans = ['DNS:localhost', 'IP:127.0.0.1'] + [f'IP:{ip}' for ip in lan_ips()]
    cmd = [
        openssl, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
        '-days', '825',
        '-keyout', key_path, '-out', cert_path,
        '-subj', '/CN=VOX LAN',
        '-addext', 'subjectAltName=' + ','.join(sans),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print('  ! สร้างใบรับรองไม่สำเร็จ:', (result.stderr or '').strip()[:200])
            return False
        return True
    except Exception as exc:
        print('  ! สร้างใบรับรองไม่สำเร็จ:', exc)
        return False


# ---------------------------------------------------------------------------
# --setup: เตรียมระบบอย่างเดียว (ไม่เปิดเซิร์ฟเวอร์) ใช้กับ setup_lan_backup.bat
# ---------------------------------------------------------------------------
REQUIRED_FILES = [
    'index.html', 'app.js', 'style.css', 'signaling.js', 'audio-fx.js',
    'qrcode.js', 'sw.js', 'manifest.json', 'vendor/mqtt.min.js',
]


def run_setup(args):
    bar = '=' * 60
    print(bar)
    print('  VOX LAN — ตรวจสอบและเตรียมระบบสำรอง')
    print(bar)

    print('\n  [1/3] ตรวจไฟล์แอป')
    missing = []
    for name in REQUIRED_FILES:
        ok = os.path.isfile(os.path.join(ROOT, name))
        print(f'        {"OK  " if ok else "ขาด "} {name}')
        if not ok:
            missing.append(name)
    if missing:
        print(f'\n  ! ขาดไฟล์ {len(missing)} ไฟล์ -> ต้องคัดลอกโฟลเดอร์โปรเจกต์ทั้งหมดมาที่เครื่องนี้')
        print('    (อย่าลืมโฟลเดอร์ vendor/ และ icons/ ด้วย)')
        return 1

    print('\n  [2/3] ตรวจ Python')
    print(f'        OK   Python {sys.version.split()[0]}')

    print('\n  [3/3] เตรียมใบรับรอง HTTPS')
    if ensure_cert(args.cert, args.key):
        print('        OK   cert.pem / key.pem พร้อม -> มือถือเปิดไมโครโฟนได้')
    else:
        print('        !  ไม่พบ openssl -> จะใช้ HTTP แทน')
        print('           ผลคือมือถือเครื่องอื่นเปิดได้แต่ใช้ไมโครโฟนไม่ได้')
        print('           (ถ้าติดตั้ง OpenSSL แล้วรันใหม่ จะสร้างใบรับรองให้เอง)')

    print('')
    print('  เสร็จแล้ว — ขั้นตอนถัดไป:')
    print('      1. ดับเบิลคลิก run_lan_server.bat')
    print('      2. เปิด URL ที่ขึ้นบนจอ จากมือถือในวง Wi-Fi เดียวกัน')
    print('      3. ข้ามหน้าเตือนใบรับรอง แล้วอนุญาตไมโครโฟน')
    print('')
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description='VOX LAN server (สำรองไว้ใช้ตอนไม่มีอินเทอร์เน็ต)')
    ap.add_argument('--host', default='0.0.0.0', help='ที่อยู่ที่จะ bind (default: 0.0.0.0)')
    ap.add_argument('--port', type=int, default=None, help='พอร์ต (default: 8443 ถ้าใช้ HTTPS, 8080 ถ้า HTTP)')
    ap.add_argument('--cert', default=os.path.join(ROOT, 'cert.pem'))
    ap.add_argument('--key', default=os.path.join(ROOT, 'key.pem'))
    ap.add_argument('--http', action='store_true',
                    help='บังคับใช้ HTTP (ไม่แนะนำ: มือถือจะใช้ไมโครโฟนไม่ได้เพราะไม่ใช่ secure context)')
    ap.add_argument('--setup', action='store_true',
                    help='ตรวจไฟล์ + เสร็็จเตรียมใบรับรอง แล้วจบ ไม่เปิดเซิร์ฟเวอร์')
    args = ap.parse_args()

    if args.setup:
        sys.exit(run_setup(args))

    use_tls = not args.http
    if use_tls:
        if not ensure_cert(args.cert, args.key):
            print('  ! ไม่มี openssl -> สลับเป็น HTTP (ทดสอบบนเครื่องเดียวได้ แต่มือถือจะใช้ไมโครโฟนไม่ได้)')
            use_tls = False
    port = args.port if args.port is not None else (8443 if use_tls else 8080)

    try:
        httpd = ThreadingHTTPServer((args.host, port), VOXHandler)
    except OSError as exc:
        print(f'! เปิดพอร์ต {port} ไม่ได้: {exc}')
        print(f'  อาจมีโปรแกรมอื่นใช้พอร์ตนี้อยู่ -> ลองรันใหม่ด้วย --port {port + 1}')
        sys.exit(1)
    httpd.daemon_threads = True
    if use_tls:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(args.cert, args.key)
        httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)

    scheme = 'https' if use_tls else 'http'
    ips = lan_ips()
    bar = '=' * 60
    print(bar)
    print('  VOX LAN — โหมดสำรองไว้ใช้เมื่อไม่มีอินเทอร์เน็ต')
    print('  เปิดจากมือถือในวง Wi-Fi เดียวกัน:')
    for ip in ips:
        print(f'      {scheme}://{ip}:{port}/')
    print(f'      (บนเครื่องนี้: {scheme}://localhost:{port}/)')
    if use_tls:
        print('  * หน้าแรกจะเตือนใบรับรอง -> กด "ขั้นสูง" -> ไปต่อ แล้วอนุญาตไมโครโฟน')
    print('  * กด Ctrl+C เพื่อปิดเซิร์ฟเวอร์')
    print(bar)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('\nปิดเซิร์ฟเวอร์แล้ว')
    finally:
        httpd.server_close()


if __name__ == '__main__':
    main()