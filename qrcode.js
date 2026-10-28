/**
 * VOX QR helper — สร้าง QR Code สำหรับเปิดแอปบนมือถือ
 *
 * ใช้ไลบรารี vendor/qrcode.min.js (qrcode-generator 1.4.4, MIT, ของ Kazuhiko Arase)
 * แทนการเขียน encoder เอง เพราะ QR ที่ถูกต้องต้องมี:
 *   - ค่า error correction (Reed-Solomon) + interleave หลายบล็อก
 *   - alignment pattern (บังคับกับ version >= 2)
 *   - จำนวน data codeword ตรงตามตารางของแต่ละ version/ECL
 * ซึ่งไฟล์เวอร์ชันก่อนหน้าขาดส่วนนี้ จึงสแกนไม่ขึ้น ("QR ไม่สมบูรณ์")
 *
 * หมายเหตุ: API ของไลบรารีเป็น global `qrcode(typeNumber, 'M')`
 * ถ้าโหลดไม่เจอ (offline cache ยังไม่อัปเดต) จะแจ้งเตือนใน console
 */
(function () {
  'use strict';

  // เลือก typeNumber เล็กที่สุดที่ใส่ข้อมูลได้ (1..14 ครอบคลุม URL ยาวได้ถึง ~400 ตัวอักษร)
  function buildQr(text) {
    let lastErr = null;
    for (let type = 1; type <= 14; type++) {
      try {
        const qr = qrcode(type, 'M');
        qr.addData(text);
        qr.make();
        return qr;
      } catch (e) {
        const msg = String((e && e.message) || e);
        // ข้อมูลยาวเกิน capacity ของเวอร์ชันนี้ -> ลองเวอร์ชันถัดไป
        if (msg.indexOf('code length overflow') === -1) throw e;
        lastErr = e;
      }
    }
    throw lastErr || new Error('QR: ข้อมูลยาวเกินขนาดที่รองรับ');
  }

  window.renderQRCode = function (canvas, text) {
    if (typeof qrcode !== 'function') {
      console.warn('[QR] vendor/qrcode.min.js ยังไม่ถูกโหลด');
      return false;
    }
    try {
      const qr = buildQr(String(text || ''));
      const count = qr.getModuleCount();
      const quiet = 4;                    // quiet zone ตามมาตรฐาน QR (4 modules)
      const total = count + quiet * 2;

      // วาดแบบ integer module ให้ขอบคม ไม่เบลอตอนสแกน
      const target = canvas.width || 220;
      const tile = Math.max(1, Math.round(target / total));
      const px = tile * total;
      canvas.width = px;
      canvas.height = px;

      const ctx = canvas.getContext('2d');
      ctx.fillStyle = '#ffffff';
      ctx.fillRect(0, 0, px, px);
      ctx.fillStyle = '#000000';
      for (let r = 0; r < count; r++) {
        for (let c = 0; c < count; c++) {
          if (qr.isDark(r, c)) {
            ctx.fillRect((c + quiet) * tile, (r + quiet) * tile, tile, tile);
          }
        }
      }
      return true;
    } catch (e) {
      console.warn('[QR] สร้าง QR Code ไม่สำเร็จ:', e);
      return false;
    }
  };
})();
