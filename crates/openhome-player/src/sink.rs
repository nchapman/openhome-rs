//! Pluggable audio output sinks (`NullSink`, `CaptureSink`, and optional `CpalSink`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::PlayerError;
use crate::format::{PcmFrame, PcmSpec};

/// An open audio output stream accepting interleaved normalized `f32` PCM samples.
///
/// Streams are opened, written to, and dropped on the playback worker thread so
/// `!Send` platform handles (such as `cpal::Stream`) work without extra indirection.
pub trait SinkStream {
    /// Return the stream's PCM format.
    fn spec(&self) -> PcmSpec;

    /// Write interleaved normalized `f32` samples to the output device.
    fn write(&mut self, samples: &[f32]) -> Result<(), PlayerError>;

    /// Flush any buffered samples (e.g. on pause, seek, or stop).
    fn flush(&mut self) {}
}

/// Factory for opening audio output streams for a given [`PcmSpec`].
pub trait AudioSink: Send + Sync {
    /// Human-readable sink name (e.g. `"null"`, `"capture"`, `"cpal:default"`).
    fn name(&self) -> &str;

    /// Open a new output stream configured for `spec`.
    fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, PlayerError>;
}

/// Headless sink that discards audio samples (optionally pacing writes in real time).
#[derive(Debug, Clone)]
pub struct NullSink {
    realtime: bool,
}

impl NullSink {
    /// Create an unpaced null sink (consumes audio as fast as decoded; ideal for tests).
    pub const fn unpaced() -> Self {
        Self { realtime: false }
    }

    /// Create a real-time-paced null sink (sleeps according to sample rate; ideal for headless daemons).
    pub const fn realtime() -> Self {
        Self { realtime: true }
    }
}

impl AudioSink for NullSink {
    fn name(&self) -> &str {
        if self.realtime {
            "null:realtime"
        } else {
            "null"
        }
    }

    fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, PlayerError> {
        Ok(Box::new(NullStream {
            spec,
            realtime: self.realtime,
        }))
    }
}

struct NullStream {
    spec: PcmSpec,
    realtime: bool,
}

impl SinkStream for NullStream {
    fn spec(&self) -> PcmSpec {
        self.spec
    }

    fn write(&mut self, samples: &[f32]) -> Result<(), PlayerError> {
        if self.realtime && self.spec.sample_rate > 0 {
            let frames = self.spec.frames_in(samples.len());
            if frames > 0 {
                let micros = frames.saturating_mul(1_000_000) / u64::from(self.spec.sample_rate);
                std::thread::sleep(Duration::from_micros(micros));
            }
        }
        Ok(())
    }
}

/// In-memory recording sink that captures every written [`PcmFrame`] for inspection in tests.
#[derive(Debug, Clone, Default)]
pub struct CaptureSink {
    frames: Arc<Mutex<Vec<PcmFrame>>>,
}

impl CaptureSink {
    /// Create a new empty [`CaptureSink`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a snapshot of all captured [`PcmFrame`]s.
    pub fn frames(&self) -> Vec<PcmFrame> {
        self.frames.lock().unwrap().clone()
    }

    /// Total interleaved sample frames captured across all [`PcmFrame`]s.
    pub fn total_frames(&self) -> u64 {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .map(|f| f.spec.frames_in(f.samples.len()))
            .sum()
    }

    /// Clear all captured frames.
    pub fn clear(&self) {
        self.frames.lock().unwrap().clear();
    }
}

impl AudioSink for CaptureSink {
    fn name(&self) -> &str {
        "capture"
    }

    fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, PlayerError> {
        Ok(Box::new(CaptureStream {
            spec,
            frames: Arc::clone(&self.frames),
        }))
    }
}

struct CaptureStream {
    spec: PcmSpec,
    frames: Arc<Mutex<Vec<PcmFrame>>>,
}

impl SinkStream for CaptureStream {
    fn spec(&self) -> PcmSpec {
        self.spec
    }

    fn write(&mut self, samples: &[f32]) -> Result<(), PlayerError> {
        if !samples.is_empty() {
            self.frames.lock().unwrap().push(PcmFrame {
                spec: self.spec,
                samples: samples.to_vec(),
            });
        }
        Ok(())
    }
}

/// Cross-platform audio output sink backed by `cpal` (CoreAudio, ALSA, WASAPI).
///
/// Uses a lock-free [`crate::format::SpscRingBuffer`] between the playback worker and the OS
/// audio callback, negotiates supported device formats (`F32`/`I16`/`I32`/`U16`), and
/// adapts mismatched sample rates or channel counts via [`crate::format::PcmAdapter`].
#[cfg(feature = "cpal-output")]
#[derive(Debug, Clone, Default)]
pub struct CpalSink {
    device_name: Option<String>,
}

#[cfg(feature = "cpal-output")]
impl CpalSink {
    /// Use the system default output audio device.
    pub fn default_device() -> Self {
        Self { device_name: None }
    }

    /// Select a named output audio device (substring match).
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            device_name: Some(name.into()),
        }
    }
}

#[cfg(feature = "cpal-output")]
fn negotiate_cpal_config(
    device: &cpal::Device,
    spec: PcmSpec,
) -> Result<(cpal::StreamConfig, cpal::SampleFormat), PlayerError> {
    use cpal::traits::DeviceTrait;

    if let Ok(ranges) = device.supported_output_configs() {
        let mut best: Option<(u32, cpal::StreamConfig, cpal::SampleFormat)> = None;
        for range in ranges {
            let fmt = range.sample_format();
            if !matches!(
                fmt,
                cpal::SampleFormat::F32
                    | cpal::SampleFormat::I16
                    | cpal::SampleFormat::I32
                    | cpal::SampleFormat::U16
            ) {
                continue;
            }
            let min_r = range.min_sample_rate().0;
            let max_r = range.max_sample_rate().0;
            let rate = spec.sample_rate.clamp(min_r, max_r);
            let ch = range.channels();
            let rate_pen = rate.abs_diff(spec.sample_rate);
            let ch_pen = u32::from(ch.abs_diff(spec.channels)) * 100_000;
            let fmt_pen = if fmt == cpal::SampleFormat::F32 {
                0
            } else {
                10
            };
            let score = rate_pen.saturating_add(ch_pen).saturating_add(fmt_pen);
            let cfg = range.with_sample_rate(cpal::SampleRate(rate)).into();
            if best.as_ref().is_none_or(|(b, _, _)| score < *b) {
                best = Some((score, cfg, fmt));
            }
        }
        if let Some((_, cfg, fmt)) = best {
            return Ok((cfg, fmt));
        }
    }

    let default = device
        .default_output_config()
        .map_err(|e| PlayerError::Sink(e.to_string()))?;
    let fmt = default.sample_format();
    Ok((default.into(), fmt))
}

#[cfg(feature = "cpal-output")]
impl AudioSink for CpalSink {
    fn name(&self) -> &str {
        self.device_name.as_deref().unwrap_or("cpal:default")
    }

    fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, PlayerError> {
        use std::sync::atomic::{AtomicBool, Ordering};

        use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};

        use crate::format::{PcmAdapter, SpscRingBuffer};

        let host = cpal::default_host();
        let device = match &self.device_name {
            Some(wanted) => host
                .output_devices()
                .map_err(|e| PlayerError::Sink(e.to_string()))?
                .find(|d| d.name().map(|n| n.contains(wanted)).unwrap_or(false))
                .ok_or_else(|| PlayerError::Sink(format!("audio device not found: {wanted}")))?,
            None => host
                .default_output_device()
                .ok_or_else(|| PlayerError::Sink("no default audio output device".into()))?,
        };

        let (config, sample_format) = negotiate_cpal_config(&device, spec)?;
        let out_rate = config.sample_rate.0;
        let out_channels = config.channels;

        // ~250ms channel-aligned lock-free SPSC ring buffer between worker and OS callback.
        let min_samples = (out_rate as usize * usize::from(out_channels) / 4).max(4096);
        let ring = Arc::new(SpscRingBuffer::new(min_samples, out_channels));
        let failed = Arc::new(AtomicBool::new(false));

        let cb_ring = Arc::clone(&ring);
        let err_flag = Arc::clone(&failed);
        let err_fn = move |_err| {
            err_flag.store(true, Ordering::Release);
        };

        let stream = match sample_format {
            cpal::SampleFormat::F32 => device.build_output_stream(
                &config,
                move |out: &mut [f32], _| {
                    cb_ring.pop_into(out);
                },
                err_fn,
                None,
            ),
            cpal::SampleFormat::I16 => {
                let mut scratch = Vec::new();
                device.build_output_stream(
                    &config,
                    move |out: &mut [i16], _| {
                        scratch.resize(out.len(), 0.0);
                        cb_ring.pop_into(&mut scratch);
                        for (dst, &s) in out.iter_mut().zip(scratch.iter()) {
                            *dst = (s.clamp(-1.0, 1.0) * 32_767.0) as i16;
                        }
                    },
                    err_fn,
                    None,
                )
            }
            cpal::SampleFormat::I32 => {
                let mut scratch = Vec::new();
                device.build_output_stream(
                    &config,
                    move |out: &mut [i32], _| {
                        scratch.resize(out.len(), 0.0);
                        cb_ring.pop_into(&mut scratch);
                        for (dst, &s) in out.iter_mut().zip(scratch.iter()) {
                            *dst = (s.clamp(-1.0, 1.0) * 2_147_483_647.0) as i32;
                        }
                    },
                    err_fn,
                    None,
                )
            }
            cpal::SampleFormat::U16 => {
                let mut scratch = Vec::new();
                device.build_output_stream(
                    &config,
                    move |out: &mut [u16], _| {
                        scratch.resize(out.len(), 0.0);
                        cb_ring.pop_into(&mut scratch);
                        for (dst, &s) in out.iter_mut().zip(scratch.iter()) {
                            *dst = ((s.clamp(-1.0, 1.0) * 0.5 + 0.5) * 65_535.0) as u16;
                        }
                    },
                    err_fn,
                    None,
                )
            }
            other => {
                return Err(PlayerError::Sink(format!(
                    "unsupported cpal output sample format: {other:?}"
                )));
            }
        }
        .map_err(|e| PlayerError::Sink(e.to_string()))?;

        stream
            .play()
            .map_err(|e| PlayerError::Sink(e.to_string()))?;

        Ok(Box::new(CpalStream {
            spec,
            _stream: stream,
            ring,
            failed,
            adapter: PcmAdapter::new(spec, out_rate, out_channels),
            adapted_buf: Vec::new(),
        }))
    }
}

#[cfg(feature = "cpal-output")]
struct CpalStream {
    spec: PcmSpec,
    _stream: cpal::Stream,
    ring: Arc<crate::format::SpscRingBuffer>,
    failed: Arc<std::sync::atomic::AtomicBool>,
    adapter: crate::format::PcmAdapter,
    adapted_buf: Vec<f32>,
}

#[cfg(feature = "cpal-output")]
impl SinkStream for CpalStream {
    fn spec(&self) -> PcmSpec {
        self.spec
    }

    fn write(&mut self, samples: &[f32]) -> Result<(), PlayerError> {
        use std::sync::atomic::Ordering;
        use std::time::Instant;

        if self.failed.load(Ordering::Acquire) {
            return Err(PlayerError::Sink("audio output device error".into()));
        }

        let to_write = if self.adapter.is_passthrough() {
            samples
        } else {
            self.adapter.process(samples, &mut self.adapted_buf);
            &self.adapted_buf
        };

        let mut offset = 0;
        let mut last_progress = Instant::now();
        while offset < to_write.len() {
            if self.failed.load(Ordering::Acquire) {
                return Err(PlayerError::Sink("audio output device disconnected".into()));
            }
            let written = self.ring.push(&to_write[offset..]);
            if written > 0 {
                offset += written;
                last_progress = Instant::now();
            } else {
                if last_progress.elapsed() > Duration::from_secs(2) {
                    return Err(PlayerError::Sink("audio output device stalled".into()));
                }
                std::thread::sleep(Duration::from_millis(4));
            }
        }
        Ok(())
    }

    fn flush(&mut self) {
        self.ring.clear();
        self.adapter.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_and_capture_sinks_record_and_clear_frames() {
        let spec = PcmSpec::new(48_000, 2, 16);

        let null = NullSink::unpaced();
        assert_eq!(null.name(), "null");
        let mut ns = null.open(spec).unwrap();
        assert_eq!(ns.spec(), spec);
        ns.write(&[0.1, -0.1, 0.2, -0.2]).unwrap();
        ns.flush();

        let rt = NullSink::realtime();
        assert_eq!(rt.name(), "null:realtime");
        let mut rts = rt.open(spec).unwrap();
        rts.write(&[0.0; 2]).unwrap();

        let cap = CaptureSink::new();
        assert_eq!(cap.name(), "capture");
        let mut cs = cap.open(spec).unwrap();
        cs.write(&[0.25, -0.25, 0.5, -0.5]).unwrap();
        assert_eq!(cap.total_frames(), 2);
        assert_eq!(cap.frames().len(), 1);
        cap.clear();
        assert_eq!(cap.total_frames(), 0);
    }
}
