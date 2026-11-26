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
impl AudioSink for CpalSink {
    fn name(&self) -> &str {
        self.device_name.as_deref().unwrap_or("cpal:default")
    }

    fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, PlayerError> {
        use std::collections::VecDeque;
        use std::sync::Condvar;

        use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};

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

        let config = cpal::StreamConfig {
            channels: spec.channels,
            sample_rate: cpal::SampleRate(spec.sample_rate),
            buffer_size: cpal::BufferSize::Default,
        };

        // ~250ms bounded sample queue between playback worker and OS audio callback.
        let capacity = (spec.sample_rate as usize * usize::from(spec.channels) / 4).max(4096);
        let shared = Arc::new((
            Mutex::new(VecDeque::<f32>::with_capacity(capacity)),
            Condvar::new(),
        ));
        let cb_shared = Arc::clone(&shared);

        let stream = device
            .build_output_stream(
                &config,
                move |out: &mut [f32], _| {
                    let (lock, cvar) = &*cb_shared;
                    if let Ok(mut q) = lock.lock() {
                        for slot in out.iter_mut() {
                            *slot = q.pop_front().unwrap_or(0.0);
                        }
                        cvar.notify_one();
                    } else {
                        out.fill(0.0);
                    }
                },
                |_err| {},
                None,
            )
            .map_err(|e| PlayerError::Sink(e.to_string()))?;

        stream
            .play()
            .map_err(|e| PlayerError::Sink(e.to_string()))?;

        Ok(Box::new(CpalStream {
            spec,
            _stream: stream,
            shared,
            capacity,
        }))
    }
}

#[cfg(feature = "cpal-output")]
struct CpalStream {
    spec: PcmSpec,
    _stream: cpal::Stream,
    shared: Arc<(Mutex<std::collections::VecDeque<f32>>, std::sync::Condvar)>,
    capacity: usize,
}

#[cfg(feature = "cpal-output")]
impl SinkStream for CpalStream {
    fn spec(&self) -> PcmSpec {
        self.spec
    }

    fn write(&mut self, samples: &[f32]) -> Result<(), PlayerError> {
        let (lock, cvar) = &*self.shared;
        let mut offset = 0;
        let mut q = lock.lock().unwrap();
        while offset < samples.len() {
            while q.len() >= self.capacity {
                let (guard, _) = cvar.wait_timeout(q, Duration::from_millis(50)).unwrap();
                q = guard;
            }
            let space = self.capacity - q.len();
            let take = space.min(samples.len() - offset);
            q.extend(&samples[offset..offset + take]);
            offset += take;
        }
        Ok(())
    }

    fn flush(&mut self) {
        let (lock, cvar) = &*self.shared;
        if let Ok(mut q) = lock.lock() {
            q.clear();
            cvar.notify_all();
        }
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
