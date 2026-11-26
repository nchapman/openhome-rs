//! Audio output sink trait plus built-in headless ([`NullSink`]) and test ([`CaptureSink`]) sinks.
//!
//! For real OS hardware output (CoreAudio / ALSA / WASAPI), see [`crate::CpalSink`].

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::AudioError;
use crate::format::{PcmFrame, PcmSpec};

/// Factory for opening an audio output stream for a given [`PcmSpec`].
pub trait AudioSink: Send + Sync {
    /// Human-readable sink name (for diagnostics).
    fn name(&self) -> &str;

    /// Open a new output stream configured for `spec`.
    fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, AudioError>;
}

/// Active audio output stream accepting interleaved normalized `f32` PCM samples.
///
/// Lives exclusively on the audio engine's worker thread so hardware streams that are `!Send`
/// (such as `cpal::Stream`) can implement it directly.
pub trait SinkStream {
    /// Stream format this output was opened with.
    fn spec(&self) -> PcmSpec;

    /// Write interleaved normalized `f32` samples (`-1.0..=1.0`), blocking for backpressure
    /// if the output buffer is full.
    fn write(&mut self, samples: &[f32]) -> Result<(), AudioError>;

    /// Immediately discard any buffered audio (used on pause, seek, stop, or track change).
    fn flush(&mut self) {}
}

/// Headless sink that discards audio, optionally sleeping to pace playback in real time.
#[derive(Debug, Clone, Copy)]
pub struct NullSink {
    realtime: bool,
}

impl NullSink {
    /// Create a sink that discards samples immediately with no pacing (ideal for fast tests).
    pub const fn unpaced() -> Self {
        Self { realtime: false }
    }

    /// Create a sink that sleeps for each written frame's wall-clock duration.
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

    fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, AudioError> {
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

    fn write(&mut self, samples: &[f32]) -> Result<(), AudioError> {
        if self.realtime && self.spec.sample_rate > 0 {
            let frames = self.spec.frames_in(samples.len());
            let micros = frames.saturating_mul(1_000_000) / u64::from(self.spec.sample_rate);
            if micros > 0 {
                std::thread::sleep(Duration::from_micros(micros));
            }
        }
        Ok(())
    }

    fn flush(&mut self) {}
}

/// In-memory capturing sink for deterministic integration and round-trip tests.
#[derive(Debug, Clone, Default)]
pub struct CaptureSink {
    frames: Arc<Mutex<Vec<PcmFrame>>>,
    realtime: bool,
}

impl CaptureSink {
    /// Create a new unpaced [`CaptureSink`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a [`CaptureSink`] that paces writes at real-time speed.
    pub fn realtime() -> Self {
        Self {
            frames: Arc::new(Mutex::new(Vec::new())),
            realtime: true,
        }
    }

    /// Snapshot of all captured [`PcmFrame`] blocks.
    pub fn frames(&self) -> Vec<PcmFrame> {
        self.frames.lock().expect("CaptureSink lock").clone()
    }

    /// Total number of sample frames captured across all blocks.
    pub fn total_frames(&self) -> u64 {
        self.frames
            .lock()
            .expect("CaptureSink lock")
            .iter()
            .map(|f| f.spec.frames_in(f.samples.len()))
            .sum()
    }

    /// Clear all captured frames.
    pub fn clear(&self) {
        self.frames.lock().expect("CaptureSink lock").clear();
    }
}

impl AudioSink for CaptureSink {
    fn name(&self) -> &str {
        "capture"
    }

    fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, AudioError> {
        Ok(Box::new(CaptureStream {
            spec,
            frames: Arc::clone(&self.frames),
            realtime: self.realtime,
        }))
    }
}

struct CaptureStream {
    spec: PcmSpec,
    frames: Arc<Mutex<Vec<PcmFrame>>>,
    realtime: bool,
}

impl SinkStream for CaptureStream {
    fn spec(&self) -> PcmSpec {
        self.spec
    }

    fn write(&mut self, samples: &[f32]) -> Result<(), AudioError> {
        if samples.is_empty() {
            return Ok(());
        }
        self.frames
            .lock()
            .expect("CaptureSink lock")
            .push(PcmFrame {
                spec: self.spec,
                samples: samples.to_vec(),
            });
        if self.realtime && self.spec.sample_rate > 0 {
            let frames = self.spec.frames_in(samples.len());
            let micros = frames.saturating_mul(1_000_000) / u64::from(self.spec.sample_rate);
            if micros > 0 {
                std::thread::sleep(Duration::from_micros(micros));
            }
        }
        Ok(())
    }

    fn flush(&mut self) {}
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
