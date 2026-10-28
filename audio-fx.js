/**
 * AudioFX - Web Audio API Sound Synthesizer for Push-to-Talk
 * Synthesizes radio walkie-talkie beeps, roger chimes, and channel alerts
 * without requiring external sound files.
 */
class AudioFX {
  constructor() {
    this.ctx = null;
    this.masterGain = null;
    this.volume = 0.8;
  }

  ensureContext() {
    if (!this.ctx) {
      const AudioCtx = window.AudioContext || window.webkitAudioContext;
      this.ctx = new AudioCtx();
      this.masterGain = this.ctx.createGain();
      this.masterGain.gain.setValueAtTime(this.volume, this.ctx.currentTime);
      this.masterGain.connect(this.ctx.destination);
    }
    if (this.ctx.state === 'suspended') {
      this.ctx.resume();
    }
    return this.ctx;
  }

  setVolume(vol) {
    this.volume = Math.max(0, Math.min(1, vol));
    if (this.masterGain && this.ctx) {
      this.masterGain.gain.setValueAtTime(this.volume, this.ctx.currentTime);
    }
  }

  /**
   * PTT Start Tone: Classic Motorola two-tone walkie-talkie chirp (880Hz -> 1046Hz)
   */
  playPttStart() {
    try {
      const ctx = this.ensureContext();
      const now = ctx.currentTime;

      // Tone 1: 880 Hz (50ms)
      const osc1 = ctx.createOscillator();
      const gain1 = ctx.createGain();
      osc1.type = 'sine';
      osc1.frequency.setValueAtTime(880, now);
      gain1.gain.setValueAtTime(0.3, now);
      gain1.gain.exponentialRampToValueAtTime(0.01, now + 0.05);

      osc1.connect(gain1);
      gain1.connect(this.masterGain);
      osc1.start(now);
      osc1.stop(now + 0.05);

      // Tone 2: 1046.5 Hz (70ms)
      const osc2 = ctx.createOscillator();
      const gain2 = ctx.createGain();
      osc2.type = 'sine';
      osc2.frequency.setValueAtTime(1046.5, now + 0.05);
      gain2.gain.setValueAtTime(0.35, now + 0.05);
      gain2.gain.exponentialRampToValueAtTime(0.01, now + 0.12);

      osc2.connect(gain2);
      gain2.connect(this.masterGain);
      osc2.start(now + 0.05);
      osc2.stop(now + 0.12);
    } catch (e) {
      console.warn('AudioFX playPttStart error:', e);
    }
  }

  /**
   * PTT Release Tone: Classic "Roger Beep" squelch tail (1200Hz)
   */
  playRogerBeep() {
    try {
      const ctx = this.ensureContext();
      const now = ctx.currentTime;

      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = 'sine';
      osc.frequency.setValueAtTime(1200, now);

      gain.gain.setValueAtTime(0.25, now);
      gain.gain.exponentialRampToValueAtTime(0.001, now + 0.08);

      osc.connect(gain);
      gain.connect(this.masterGain);
      osc.start(now);
      osc.stop(now + 0.08);
    } catch (e) {
      console.warn('AudioFX playRogerBeep error:', e);
    }
  }

  /**
   * Channel Busy / Collision Tone: Dual low-buzz pulses (220Hz)
   */
  playBusyTone() {
    try {
      const ctx = this.ensureContext();
      const now = ctx.currentTime;

      [0, 0.1].forEach((delay) => {
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.type = 'sawtooth';
        osc.frequency.setValueAtTime(220, now + delay);

        gain.gain.setValueAtTime(0.2, now + delay);
        gain.gain.exponentialRampToValueAtTime(0.01, now + delay + 0.07);

        osc.connect(gain);
        gain.connect(this.masterGain);
        osc.start(now + delay);
        osc.stop(now + delay + 0.07);
      });
    } catch (e) {
      console.warn('AudioFX playBusyTone error:', e);
    }
  }

  /**
   * Join Tone: Subtle ascending chime
   */
  playJoinTone() {
    try {
      const ctx = this.ensureContext();
      const now = ctx.currentTime;

      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = 'triangle';
      osc.frequency.setValueAtTime(587.33, now); // D5
      osc.frequency.exponentialRampToValueAtTime(880, now + 0.12); // A5

      gain.gain.setValueAtTime(0.15, now);
      gain.gain.exponentialRampToValueAtTime(0.001, now + 0.2);

      osc.connect(gain);
      gain.connect(this.masterGain);
      osc.start(now);
      osc.stop(now + 0.2);
    } catch (e) {
      console.warn('AudioFX playJoinTone error:', e);
    }
  }

  /**
   * Leave Tone: Soft descending chime
   */
  playLeaveTone() {
    try {
      const ctx = this.ensureContext();
      const now = ctx.currentTime;

      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = 'triangle';
      osc.frequency.setValueAtTime(784, now); // G5
      osc.frequency.exponentialRampToValueAtTime(440, now + 0.14); // A4

      gain.gain.setValueAtTime(0.12, now);
      gain.gain.exponentialRampToValueAtTime(0.001, now + 0.18);

      osc.connect(gain);
      gain.connect(this.masterGain);
      osc.start(now);
      osc.stop(now + 0.18);
    } catch (e) {
      console.warn('AudioFX playLeaveTone error:', e);
    }
  }
}

window.audioFX = new AudioFX();
