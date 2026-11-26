//! PCM format descriptors, OpenHome dB volume/balance gain ramping, Songcast PCM conversion,
//! lock-free SPSC audio ring buffer, and pure-Rust channel/sample-rate adaptation.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

use openhome_core::types::{VolumeCharacteristics, VolumeState};

/// Interleaved PCM stream parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PcmSpec {
    /// Sample rate in Hz (e.g. `44_100`, `48_000`, `96_000`).
    pub sample_rate: u32,
    /// Number of interleaved audio channels (e.g. `2` for stereo).
    pub channels: u16,
    /// Source bit depth (e.g. `16`, `24`, `32`).
    pub bits_per_sample: u16,
}

impl PcmSpec {
    /// Create a new [`PcmSpec`].
    pub const fn new(sample_rate: u32, channels: u16, bits_per_sample: u16) -> Self {
        Self {
            sample_rate,
            channels,
            bits_per_sample,
        }
    }

    /// Number of sample frames represented by `interleaved_samples` floats.
    pub fn frames_in(&self, interleaved_samples: usize) -> u64 {
        let ch = usize::from(self.channels.max(1));
        (interleaved_samples / ch) as u64
    }
}

/// One decoded or captured block of interleaved normalized `f32` PCM samples (`-1.0..=1.0`).
#[derive(Debug, Clone, PartialEq)]
pub struct PcmFrame {
    /// Stream format of `samples`.
    pub spec: PcmSpec,
    /// Interleaved normalized `f32` samples.
    pub samples: Vec<f32>,
}

/// Compute linear amplitude gain for OpenHome `Volume` and `Mute` using the OpenHome
/// `VolumeCharacteristics` dB scale (`10^(-(max - v) * milli_db_per_step / 20_000)`).
pub fn volume_gain_with_chars(volume: u32, mute: bool, chars: &VolumeCharacteristics) -> f32 {
    if mute || volume == 0 || chars.volume_max == 0 {
        return 0.0;
    }
    let clamped = volume.min(chars.volume_max);
    if clamped == chars.volume_max {
        return 1.0;
    }
    let steps_below = (chars.volume_max - clamped) as f32;
    let milli_db = chars.volume_milli_db_per_step.max(1) as f32;
    let db = -(steps_below * milli_db) / 1000.0;
    10_f32.powf(db / 20.0)
}

/// Compute linear amplitude gain using default OpenHome `VolumeCharacteristics` (`0..=100`, 0.5 dB/step).
pub fn volume_gain(volume: u32, mute: bool) -> f32 {
    volume_gain_with_chars(volume, mute, &VolumeCharacteristics::default())
}

/// Compute `(left_gain, right_gain)` for a given [`VolumeState`] and [`VolumeCharacteristics`].
pub fn channel_gains(state: &VolumeState, chars: &VolumeCharacteristics) -> (f32, f32) {
    let base = volume_gain_with_chars(state.volume, state.mute, chars);
    if base == 0.0 {
        return (0.0, 0.0);
    }
    if chars.balance_max > 0 && state.balance != 0 {
        let max = chars.balance_max as f32;
        let b = (state.balance as f32 / max).clamp(-1.0, 1.0);
        let left = if b > 0.0 { base * (1.0 - b) } else { base };
        let right = if b < 0.0 { base * (1.0 + b) } else { base };
        (left, right)
    } else {
        (base, base)
    }
}

/// Apply OpenHome [`VolumeState`] in-place to interleaved `f32` samples (stateless convenience).
pub fn apply_volume_in_place(samples: &mut [f32], channels: u16, state: &VolumeState) {
    let chars = VolumeCharacteristics {
        balance_max: if state.balance != 0 { 100 } else { 0 },
        ..VolumeCharacteristics::default()
    };
    let (lg, rg) = channel_gains(state, &chars);
    if lg == 0.0 && rg == 0.0 {
        samples.fill(0.0);
        return;
    }
    if channels == 2 && (lg - rg).abs() > f32::EPSILON {
        for pair in samples.as_chunks_mut::<2>().0 {
            pair[0] *= lg;
            pair[1] *= rg;
        }
    } else if (lg - 1.0).abs() > f32::EPSILON {
        for s in samples.iter_mut() {
            *s *= lg;
        }
    }
}

/// Per-sample smooth gain ramp (~10 ms) eliminating zipper noise on volume/mute changes.
#[derive(Debug, Clone)]
pub struct GainRamp {
    cur_left: f32,
    cur_right: f32,
    target_left: f32,
    target_right: f32,
}

impl GainRamp {
    /// Create a new [`GainRamp`] initialized to `(left, right)`.
    pub const fn new(left: f32, right: f32) -> Self {
        Self {
            cur_left: left,
            cur_right: right,
            target_left: left,
            target_right: right,
        }
    }

    /// Update the target `(left, right)` linear gains (smoothly ramped on next audio block).
    pub fn set_target(&mut self, left: f32, right: f32) {
        self.target_left = left;
        self.target_right = right;
    }

    /// Snap current and target gains immediately (used when idle/between tracks so muted
    /// playback starts at exact silence without a 10 ms fade-down transient).
    pub fn snap_to(&mut self, left: f32, right: f32) {
        self.cur_left = left;
        self.cur_right = right;
        self.target_left = left;
        self.target_right = right;
    }

    /// Apply smoothly ramped gain in-place to interleaved `samples` at `sample_rate`.
    pub fn apply(&mut self, samples: &mut [f32], channels: u16, sample_rate: u32) {
        let ch = usize::from(channels.max(1));
        let frames = samples.len() / ch;
        if frames == 0 {
            return;
        }
        let ramp_frames = ((sample_rate / 100).max(1) as usize).min(frames);
        let needs_ramp = (self.cur_left - self.target_left).abs() > 1e-5
            || (self.cur_right - self.target_right).abs() > 1e-5;

        if !needs_ramp {
            self.cur_left = self.target_left;
            self.cur_right = self.target_right;
            if self.cur_left == 0.0 && self.cur_right == 0.0 {
                samples.fill(0.0);
                return;
            }
            if ch == 2 && (self.cur_left - self.cur_right).abs() > 1e-6 {
                for pair in samples.as_chunks_mut::<2>().0 {
                    pair[0] *= self.cur_left;
                    pair[1] *= self.cur_right;
                }
            } else if (self.cur_left - 1.0).abs() > 1e-6 {
                for s in samples.iter_mut() {
                    *s *= self.cur_left;
                }
            }
            return;
        }

        let start_l = self.cur_left;
        let start_r = self.cur_right;
        let inv = 1.0 / (ramp_frames as f32);
        for f in 0..frames {
            let t = ((f + 1).min(ramp_frames) as f32) * inv;
            let gl = start_l + (self.target_left - start_l) * t;
            let gr = start_r + (self.target_right - start_r) * t;
            let base = f * ch;
            if ch == 2 {
                samples[base] *= gl;
                samples[base + 1] *= gr;
            } else {
                for c in 0..ch {
                    samples[base + c] *= gl;
                }
            }
        }
        self.cur_left = self.target_left;
        self.cur_right = self.target_right;
    }
}

/// Convert big-endian signed integer Songcast PCM bytes (`bit_depth` of `16`, `24`, or `32`)
/// into interleaved normalized `f32` samples in `-1.0..=1.0`.
pub fn songcast_pcm_be_to_f32(bytes: &[u8], bit_depth: u8) -> Vec<f32> {
    let mut out = Vec::new();
    songcast_pcm_be_into_f32(bytes, bit_depth, &mut out);
    out
}

/// Convert big-endian signed integer Songcast PCM bytes into `out` (reusing its allocation).
pub fn songcast_pcm_be_into_f32(bytes: &[u8], bit_depth: u8, out: &mut Vec<f32>) {
    out.clear();
    match bit_depth {
        16 => {
            let chunks = bytes.as_chunks::<2>().0;
            out.reserve(chunks.len());
            out.extend(
                chunks
                    .iter()
                    .map(|&c| i16::from_be_bytes(c) as f32 / 32_768.0),
            );
        }
        24 => {
            let chunks = bytes.as_chunks::<3>().0;
            out.reserve(chunks.len());
            out.extend(chunks.iter().map(|&[b0, b1, b2]| {
                let raw = i32::from_be_bytes([b0, b1, b2, 0]) >> 8;
                raw as f32 / 8_388_608.0
            }));
        }
        32 => {
            let chunks = bytes.as_chunks::<4>().0;
            out.reserve(chunks.len());
            out.extend(
                chunks
                    .iter()
                    .map(|&c| i32::from_be_bytes(c) as f32 / 2_147_483_648.0),
            );
        }
        _ => {}
    }
}

/// Lock-free single-producer, single-consumer `f32` ring buffer built with safe atomics
/// (zero `unsafe` code, channel-aligned capacity so audio frames are never split).
pub struct SpscRingBuffer {
    buf: Box<[AtomicU32]>,
    mask_len: usize,
    channels: usize,
    read_idx: AtomicUsize,
    write_idx: AtomicUsize,
    flush_pending: AtomicBool,
}

impl std::fmt::Debug for SpscRingBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpscRingBuffer")
            .field("capacity", &self.capacity())
            .field("channels", &self.channels)
            .field("len", &self.len())
            .finish()
    }
}

impl SpscRingBuffer {
    /// Create a ring buffer holding at least `min_samples` floats, rounded up to a multiple of `channels`.
    pub fn new(min_samples: usize, channels: u16) -> Self {
        let ch = usize::from(channels.max(1));
        let frames = min_samples.div_ceil(ch).max(1);
        // Reserve one extra frame slot to distinguish full from empty without wrapping ambiguity.
        let mask_len = (frames + 1) * ch;
        let buf = (0..mask_len)
            .map(|_| AtomicU32::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            buf,
            mask_len,
            channels: ch,
            read_idx: AtomicUsize::new(0),
            write_idx: AtomicUsize::new(0),
            flush_pending: AtomicBool::new(false),
        }
    }

    /// Usable sample capacity (always a multiple of `channels`).
    pub fn capacity(&self) -> usize {
        self.mask_len - self.channels
    }

    /// Number of samples currently buffered.
    pub fn len(&self) -> usize {
        if self.flush_pending.load(Ordering::Acquire) {
            return 0;
        }
        let w = self.write_idx.load(Ordering::Acquire);
        let r = self.read_idx.load(Ordering::Acquire);
        if w >= r { w - r } else { self.mask_len - r + w }
    }

    /// True if no samples are buffered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Push as many whole frames from `samples` as fit without blocking. Returns samples written.
    pub fn push(&self, samples: &[f32]) -> usize {
        if self.flush_pending.load(Ordering::Acquire) {
            return 0;
        }
        let w = self.write_idx.load(Ordering::Relaxed);
        let r = self.read_idx.load(Ordering::Acquire);
        let used = if w >= r { w - r } else { self.mask_len - r + w };
        let free = self.capacity().saturating_sub(used);
        let max_write = free.min(samples.len());
        // Always write whole channel-aligned frames.
        let count = (max_write / self.channels) * self.channels;
        if count == 0 {
            return 0;
        }
        let mut pos = w;
        for &s in &samples[..count] {
            self.buf[pos].store(s.to_bits(), Ordering::Relaxed);
            pos += 1;
            if pos == self.mask_len {
                pos = 0;
            }
        }
        self.write_idx.store(pos, Ordering::Release);
        count
    }

    /// Pop samples into `out` (zero-filling any underrun tail). Never blocks or allocates.
    pub fn pop_into(&self, out: &mut [f32]) -> usize {
        if self.flush_pending.swap(false, Ordering::AcqRel) {
            // Consumer exclusively owns read_idx: advance it to write_idx and output silence.
            let w = self.write_idx.load(Ordering::Acquire);
            self.read_idx.store(w, Ordering::Release);
            out.fill(0.0);
            return 0;
        }
        let r = self.read_idx.load(Ordering::Relaxed);
        let w = self.write_idx.load(Ordering::Acquire);
        let avail = if w >= r { w - r } else { self.mask_len - r + w };
        let count = avail.min(out.len());
        let mut pos = r;
        for slot in &mut out[..count] {
            *slot = f32::from_bits(self.buf[pos].load(Ordering::Relaxed));
            pos += 1;
            if pos == self.mask_len {
                pos = 0;
            }
        }
        if count < out.len() {
            out[count..].fill(0.0);
        }
        self.read_idx.store(pos, Ordering::Release);
        count
    }

    /// Request the consumer to discard all buffered samples on its next `pop_into` call
    /// (preserves strict SPSC ownership of `read_idx` so concurrent `pop_into` cannot resurrect samples).
    pub fn clear(&self) {
        self.flush_pending.store(true, Ordering::Release);
    }
}

/// Pure-Rust interleaved channel remixer & linear resampler for adapting source audio to
/// an output device's supported `(sample_rate, channels)`.
#[derive(Debug, Clone)]
pub struct PcmAdapter {
    in_rate: u32,
    in_channels: usize,
    out_rate: u32,
    out_channels: usize,
    phase: f64,
    prev_frame: Vec<f32>,
    has_prev: bool,
}

impl PcmAdapter {
    /// Create a [`PcmAdapter`] from `input` spec to `(out_rate, out_channels)`.
    pub fn new(input: PcmSpec, out_rate: u32, out_channels: u16) -> Self {
        let out_ch = usize::from(out_channels.max(1));
        Self {
            in_rate: input.sample_rate.max(1),
            in_channels: usize::from(input.channels.max(1)),
            out_rate: out_rate.max(1),
            out_channels: out_ch,
            phase: 0.0,
            prev_frame: vec![0.0; out_ch],
            has_prev: false,
        }
    }

    /// True if no channel remixing or resampling is required.
    pub fn is_passthrough(&self) -> bool {
        self.in_rate == self.out_rate && self.in_channels == self.out_channels
    }

    /// Reset resampler interpolation state (e.g. on flush/seek).
    pub fn reset(&mut self) {
        self.phase = 0.0;
        self.prev_frame.fill(0.0);
        self.has_prev = false;
    }

    /// Adapt `input` interleaved samples into `out` (appending to cleared `out`).
    pub fn process(&mut self, input: &[f32], out: &mut Vec<f32>) {
        out.clear();
        let in_frames = input.len() / self.in_channels;
        if in_frames == 0 {
            return;
        }
        if self.is_passthrough() {
            out.extend_from_slice(&input[..in_frames * self.in_channels]);
            return;
        }

        // Step 1: remix channels into a scratch buffer at `in_rate`.
        let mut remixed = Vec::with_capacity(in_frames * self.out_channels);
        for f in 0..in_frames {
            let frame = &input[f * self.in_channels..(f + 1) * self.in_channels];
            match (self.in_channels, self.out_channels) {
                (ic, oc) if ic == oc => remixed.extend_from_slice(frame),
                (1, oc) => {
                    for _ in 0..oc {
                        remixed.push(frame[0]);
                    }
                }
                (ic, 1) => {
                    let sum: f32 = frame.iter().copied().sum();
                    remixed.push(sum / (ic as f32));
                }
                (ic, 2) => {
                    // Fold multichannel (e.g. 5.1) into stereo (FL/FR + center/surround fold).
                    let l = frame[0];
                    let r = frame[1];
                    let extra = if ic > 2 {
                        frame[2..].iter().copied().sum::<f32>() / ((ic - 2) as f32) * 0.5
                    } else {
                        0.0
                    };
                    remixed.push((l + extra).clamp(-1.0, 1.0));
                    remixed.push((r + extra).clamp(-1.0, 1.0));
                }
                (ic, oc) => {
                    let copy = ic.min(oc);
                    remixed.extend_from_slice(&frame[..copy]);
                    if oc > copy {
                        remixed.resize(remixed.len() + (oc - copy), 0.0);
                    }
                }
            }
        }

        if self.in_rate == self.out_rate {
            out.extend_from_slice(&remixed);
            return;
        }

        // Step 2: linear interpolation resampler from `in_rate` to `out_rate`.
        let ratio = (self.in_rate as f64) / (self.out_rate as f64);
        let oc = self.out_channels;
        if !self.has_prev {
            self.prev_frame.copy_from_slice(&remixed[..oc]);
            self.has_prev = true;
        }

        while self.phase < (in_frames as f64) {
            let idx = self.phase.floor() as isize;
            let frac = (self.phase - (idx as f64)) as f32;
            let a = if idx <= 0 {
                &self.prev_frame[..]
            } else {
                let p = ((idx - 1) as usize).min(in_frames - 1) * oc;
                &remixed[p..p + oc]
            };
            let b_idx = (idx.max(0) as usize).min(in_frames - 1) * oc;
            let b = &remixed[b_idx..b_idx + oc];
            for c in 0..oc {
                out.push(a[c] + (b[c] - a[c]) * frac);
            }
            self.phase += ratio;
        }
        self.phase -= in_frames as f64;
        let last = (in_frames - 1) * oc;
        self.prev_frame.copy_from_slice(&remixed[last..last + oc]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_db_curve_balance_and_smooth_ramp() {
        assert_eq!(volume_gain(0, false), 0.0);
        assert_eq!(volume_gain(80, true), 0.0);
        assert_eq!(volume_gain(100, false), 1.0);
        // 50 steps * 0.5 dB = -25 dB ≈ 0.05623
        assert!((volume_gain(50, false) - 0.056234).abs() < 1e-4);

        let mut stereo = vec![1.0f32, -1.0, 0.5, -0.5];
        apply_volume_in_place(
            &mut stereo,
            2,
            &VolumeState {
                volume: 100,
                mute: false,
                balance: 50,
                fade: 0,
                volume_limit: 100,
            },
        );
        assert!((stereo[0] - 0.5).abs() < 1e-6);
        assert!((stereo[1] + 1.0).abs() < 1e-6);

        let mut ramp = GainRamp::new(0.0, 0.0);
        ramp.set_target(1.0, 1.0);
        let mut block = vec![1.0f32; 200];
        ramp.apply(&mut block, 2, 10_000); // 10ms = 100 frames
        assert!(block[0] > 0.0 && block[0] < 0.1);
        assert!((block[198] - 1.0).abs() < 1e-5);
        // Subsequent block is steady-state 1.0
        let mut steady = vec![0.5f32; 8];
        ramp.apply(&mut steady, 2, 10_000);
        assert!((steady[0] - 0.5).abs() < 1e-6);
    }

    #[test]
    fn songcast_be_pcm_converts_16_24_and_32_bit() {
        let s16 = songcast_pcm_be_to_f32(&[0x7F, 0xFF, 0x80, 0x00], 16);
        assert_eq!(s16.len(), 2);
        assert!(s16[0] > 0.999 && (s16[1] + 1.0).abs() < 1e-6);

        let s24 = songcast_pcm_be_to_f32(&[0x7F, 0xFF, 0xFF, 0x80, 0x00, 0x00], 24);
        assert_eq!(s24.len(), 2);
        assert!(s24[0] > 0.999 && (s24[1] + 1.0).abs() < 1e-6);

        let s32 = songcast_pcm_be_to_f32(&[0x40, 0x00, 0x00, 0x00], 32);
        assert_eq!(s32.len(), 1);
        assert!((s32[0] - 0.5).abs() < 1e-6);

        assert!(songcast_pcm_be_to_f32(&[1, 2, 3], 8).is_empty());
    }

    #[test]
    fn spsc_ring_buffer_aligns_channels_wraps_and_clears() {
        let rb = SpscRingBuffer::new(10, 6); // rounds to 2 frames = 12 samples
        assert_eq!(rb.capacity(), 12);
        assert!(rb.is_empty());
        assert!(format!("{rb:?}").contains("SpscRingBuffer"));

        // Push 1 frame (6 samples) + 3 trailing samples -> only 6 written (channel-aligned)
        assert_eq!(rb.push(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]), 6);
        assert_eq!(rb.len(), 6);

        // Push another 6 -> full (12)
        assert_eq!(rb.push(&[10.0, 11.0, 12.0, 13.0, 14.0, 15.0]), 6);
        assert_eq!(rb.push(&[1.0; 6]), 0); // full

        let mut out = vec![0.0f32; 8];
        assert_eq!(rb.pop_into(&mut out), 8);
        assert_eq!(out, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 10.0, 11.0]);

        // Wraparound push & pop + underrun zero-fill
        assert_eq!(rb.push(&[20.0, 21.0, 22.0, 23.0, 24.0, 25.0]), 6);
        let mut tail = vec![9.0f32; 12];
        assert_eq!(rb.pop_into(&mut tail), 10);
        assert_eq!(tail[10], 0.0);
        assert_eq!(tail[11], 0.0);

        rb.push(&[1.0; 6]);
        rb.clear();
        assert!(rb.is_empty());
        assert_eq!(rb.push(&[2.0; 6]), 0); // waits until consumer drains flush_pending
        let mut flushed = vec![9.0f32; 6];
        assert_eq!(rb.pop_into(&mut flushed), 0);
        assert!(flushed.iter().all(|&s| s == 0.0));
        assert_eq!(rb.push(&[2.0; 6]), 6);
    }

    #[test]
    fn pcm_adapter_remixes_and_resamples() {
        let mut out = Vec::new();

        // Passthrough
        let mut pt = PcmAdapter::new(PcmSpec::new(48_000, 2, 16), 48_000, 2);
        assert!(pt.is_passthrough());
        pt.process(&[0.25, -0.25], &mut out);
        assert_eq!(out, vec![0.25, -0.25]);

        // Mono -> Stereo + 44.1kHz -> 48kHz resample
        let mut up = PcmAdapter::new(PcmSpec::new(44_100, 1, 16), 48_000, 2);
        let mono = vec![0.5f32; 441]; // 10ms
        up.process(&mono, &mut out);
        let out_frames = out.len() / 2;
        assert!((478..=482).contains(&out_frames), "got {out_frames} frames");
        assert!((out[0] - 0.5).abs() < 1e-4 && (out[1] - 0.5).abs() < 1e-4);
        up.reset();

        // Stereo -> Mono & 4ch -> Stereo & 2ch -> 4ch
        let mut to_mono = PcmAdapter::new(PcmSpec::new(48_000, 2, 16), 48_000, 1);
        to_mono.process(&[0.2, 0.6], &mut out);
        assert!((out[0] - 0.4).abs() < 1e-5);

        let mut quad_to_stereo = PcmAdapter::new(PcmSpec::new(48_000, 4, 16), 48_000, 2);
        quad_to_stereo.process(&[0.4, -0.4, 0.2, 0.2], &mut out);
        assert_eq!(out.len(), 2);

        let mut stereo_to_quad = PcmAdapter::new(PcmSpec::new(48_000, 2, 16), 48_000, 4);
        stereo_to_quad.process(&[0.3, -0.3], &mut out);
        assert_eq!(out, vec![0.3, -0.3, 0.0, 0.0]);
    }
}
