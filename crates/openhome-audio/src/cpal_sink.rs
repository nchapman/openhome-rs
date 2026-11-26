//! Real OS hardware audio output sink powered by `cpal` (CoreAudio / ALSA / WASAPI).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};

use crate::error::AudioError;
use crate::format::{PcmAdapter, PcmSpec, SpscRingBuffer};
use crate::sink::{AudioSink, SinkStream};

/// OS audio output sink backed by `cpal` (CoreAudio on macOS, ALSA on Linux, WASAPI on Windows).
#[derive(Debug, Clone, Default)]
pub struct CpalSink {
    device_name: Option<String>,
}

impl CpalSink {
    /// Open the system default audio output device.
    pub fn default_device() -> Self {
        Self { device_name: None }
    }

    /// Open a named audio output device (substring match).
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            device_name: Some(name.into()),
        }
    }
}

fn negotiate_cpal_config(
    device: &cpal::Device,
    spec: PcmSpec,
) -> Result<(cpal::StreamConfig, cpal::SampleFormat), AudioError> {
    if let Ok(ranges) = device.supported_output_configs() {
        // Lexicographic priority:
        // 1. Prefer channel counts >= requested (avoid downmixing stereo to mono)
        // 2. Closest channel count to requested
        // 3. Closest sample rate to requested
        // 4. Highest precision sample format: F32 < I32 < I16 < U16
        type Score = (u8, u16, u32, u8);
        let mut best: Option<(Score, cpal::StreamConfig, cpal::SampleFormat)> = None;
        for range in ranges {
            let fmt = range.sample_format();
            let fmt_rank = match fmt {
                cpal::SampleFormat::F32 => 0,
                cpal::SampleFormat::I32 => 1,
                cpal::SampleFormat::I16 => 2,
                cpal::SampleFormat::U16 => 3,
                _ => continue,
            };
            let min_r = range.min_sample_rate().0;
            let max_r = range.max_sample_rate().0;
            let rate = spec.sample_rate.clamp(min_r, max_r);
            let ch = range.channels();
            let downmix_pen = u8::from(ch < spec.channels);
            let ch_diff = ch.abs_diff(spec.channels);
            let rate_diff = rate.abs_diff(spec.sample_rate);
            let score: Score = (downmix_pen, ch_diff, rate_diff, fmt_rank);
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
        .map_err(|e| AudioError::Sink(e.to_string()))?;
    let fmt = default.sample_format();
    Ok((default.into(), fmt))
}

impl AudioSink for CpalSink {
    fn name(&self) -> &str {
        self.device_name.as_deref().unwrap_or("cpal:default")
    }

    fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, AudioError> {
        let host = cpal::default_host();
        let device = match &self.device_name {
            Some(wanted) => host
                .output_devices()
                .map_err(|e| AudioError::Sink(e.to_string()))?
                .find(|d| d.name().map(|n| n.contains(wanted)).unwrap_or(false))
                .ok_or_else(|| AudioError::Sink(format!("audio device not found: {wanted}")))?,
            None => host
                .default_output_device()
                .ok_or_else(|| AudioError::Sink("no default audio output device".into()))?,
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

        // Zero-allocation RT callback chunking (fixed stack buffer, channel-aligned).
        let ch = usize::from(out_channels.max(1));
        let stack_step = if ch <= 512 { (512 / ch) * ch } else { ch };

        let stream = match sample_format {
            cpal::SampleFormat::F32 => device.build_output_stream(
                &config,
                move |out: &mut [f32], _| {
                    cb_ring.pop_into(out);
                    for s in out.iter_mut() {
                        if !s.is_finite() {
                            *s = 0.0;
                        }
                    }
                },
                err_fn,
                None,
            ),
            cpal::SampleFormat::I16 => {
                // Pre-allocated fallback only if an exotic device has >512 channels.
                let mut scratch = if ch > 512 {
                    vec![0.0f32; ch]
                } else {
                    Vec::new()
                };
                device.build_output_stream(
                    &config,
                    move |out: &mut [i16], _| {
                        let mut stack_buf = [0.0f32; 512];
                        let buf: &mut [f32] = if ch <= 512 {
                            &mut stack_buf[..stack_step]
                        } else {
                            &mut scratch[..ch]
                        };
                        for chunk in out.chunks_mut(buf.len()) {
                            let len = chunk.len();
                            cb_ring.pop_into(&mut buf[..len]);
                            for (dst, &s) in chunk.iter_mut().zip(&buf[..len]) {
                                let clean = if s.is_finite() {
                                    s.clamp(-1.0, 1.0)
                                } else {
                                    0.0
                                };
                                *dst = (clean * 32_767.0) as i16;
                            }
                        }
                    },
                    err_fn,
                    None,
                )
            }
            cpal::SampleFormat::I32 => {
                let mut scratch = if ch > 512 {
                    vec![0.0f32; ch]
                } else {
                    Vec::new()
                };
                device.build_output_stream(
                    &config,
                    move |out: &mut [i32], _| {
                        let mut stack_buf = [0.0f32; 512];
                        let buf: &mut [f32] = if ch <= 512 {
                            &mut stack_buf[..stack_step]
                        } else {
                            &mut scratch[..ch]
                        };
                        for chunk in out.chunks_mut(buf.len()) {
                            let len = chunk.len();
                            cb_ring.pop_into(&mut buf[..len]);
                            for (dst, &s) in chunk.iter_mut().zip(&buf[..len]) {
                                let clean = if s.is_finite() {
                                    s.clamp(-1.0, 1.0)
                                } else {
                                    0.0
                                };
                                *dst = (clean * 2_147_483_647.0) as i32;
                            }
                        }
                    },
                    err_fn,
                    None,
                )
            }
            cpal::SampleFormat::U16 => {
                let mut scratch = if ch > 512 {
                    vec![0.0f32; ch]
                } else {
                    Vec::new()
                };
                device.build_output_stream(
                    &config,
                    move |out: &mut [u16], _| {
                        let mut stack_buf = [0.0f32; 512];
                        let buf: &mut [f32] = if ch <= 512 {
                            &mut stack_buf[..stack_step]
                        } else {
                            &mut scratch[..ch]
                        };
                        for chunk in out.chunks_mut(buf.len()) {
                            let len = chunk.len();
                            cb_ring.pop_into(&mut buf[..len]);
                            for (dst, &s) in chunk.iter_mut().zip(&buf[..len]) {
                                let clean = if s.is_finite() {
                                    s.clamp(-1.0, 1.0)
                                } else {
                                    0.0
                                };
                                *dst = ((clean * 0.5 + 0.5) * 65_535.0) as u16;
                            }
                        }
                    },
                    err_fn,
                    None,
                )
            }
            other => {
                return Err(AudioError::Sink(format!(
                    "unsupported cpal output sample format: {other:?}"
                )));
            }
        }
        .map_err(|e| AudioError::Sink(e.to_string()))?;

        stream.play().map_err(|e| AudioError::Sink(e.to_string()))?;

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

struct CpalStream {
    spec: PcmSpec,
    _stream: cpal::Stream,
    ring: Arc<SpscRingBuffer>,
    failed: Arc<AtomicBool>,
    adapter: PcmAdapter,
    adapted_buf: Vec<f32>,
}

impl SinkStream for CpalStream {
    fn spec(&self) -> PcmSpec {
        self.spec
    }

    fn write(&mut self, samples: &[f32]) -> Result<(), AudioError> {
        if self.failed.load(Ordering::Acquire) {
            return Err(AudioError::Sink("audio output device error".into()));
        }

        let to_write = if self.adapter.is_passthrough() {
            let ch = usize::from(self.spec.channels.max(1));
            &samples[..(samples.len() / ch) * ch]
        } else {
            self.adapter.process(samples, &mut self.adapted_buf);
            &self.adapted_buf
        };

        let mut offset = 0;
        let mut last_progress = Instant::now();
        while offset < to_write.len() {
            if self.failed.load(Ordering::Acquire) {
                return Err(AudioError::Sink("audio output device disconnected".into()));
            }
            let written = self.ring.push(&to_write[offset..]);
            if written > 0 {
                offset += written;
                last_progress = Instant::now();
            } else {
                if last_progress.elapsed() > Duration::from_secs(2) {
                    return Err(AudioError::Sink("audio output device stalled".into()));
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

    fn drain(&mut self) {
        let deadline = Instant::now() + Duration::from_millis(500);
        while !self.ring.is_empty()
            && !self.failed.load(Ordering::Acquire)
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
