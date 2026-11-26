//! Symphonia-based audio decoder supporting `http(s)://` URIs (and opt-in local files).

use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{
    CODEC_TYPE_AAC, CODEC_TYPE_ALAC, CODEC_TYPE_FLAC, CODEC_TYPE_MP1, CODEC_TYPE_MP2,
    CODEC_TYPE_MP3, CODEC_TYPE_NULL, CODEC_TYPE_VORBIS, CodecType, Decoder, DecoderOptions,
};
use symphonia::core::errors::Error as SymphError;
use symphonia::core::formats::{FormatOptions, FormatReader, SeekMode, SeekTo};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use symphonia::core::units::{Time, TimeBase};

use crate::error::AudioError;
use crate::format::PcmSpec;

/// Maximum buffered HTTP audio payload size when fetching finite tracks (256 MiB).
const MAX_HTTP_FETCH_BYTES: u64 = 256 * 1024 * 1024;
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_IO_TIMEOUT: Duration = Duration::from_secs(15);

/// Returns `true` if `uri` uses an `http://` or `https://` scheme (case-insensitive).
pub fn is_http_uri(uri: &str) -> bool {
    let trimmed = uri.trim();
    trimmed
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("http://"))
        || trimmed
            .get(..8)
            .is_some_and(|s| s.eq_ignore_ascii_case("https://"))
}

/// Optional cooperative cancellation token checked during HTTP fetches.
#[derive(Debug, Clone)]
pub struct LoadCancel {
    expected: u64,
    latest: Arc<AtomicU64>,
}

impl LoadCancel {
    /// Create a cancellation token active while `latest == expected`.
    pub fn new(expected: u64, latest: Arc<AtomicU64>) -> Self {
        Self { expected, latest }
    }

    /// True if a newer playback generation has superseded this load.
    pub fn is_cancelled(&self) -> bool {
        self.latest.load(Ordering::Acquire) != self.expected
    }
}

/// Codec classification metadata extracted from Symphonia `CodecParameters`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecInfo {
    /// Canonical OpenHome codec name (e.g. `"FLAC"`, `"ALAC"`, `"MP3"`, `"AAC"`, `"Vorbis"`, `"PCM"`).
    pub name: String,
    /// Whether the codec is lossless.
    pub lossless: bool,
}

/// Classify a Symphonia [`CodecType`] into OpenHome `Info.Details` (`codec_name`, `lossless`).
pub fn classify_codec(codec: CodecType) -> CodecInfo {
    match codec {
        CODEC_TYPE_FLAC => CodecInfo {
            name: "FLAC".into(),
            lossless: true,
        },
        CODEC_TYPE_ALAC => CodecInfo {
            name: "ALAC".into(),
            lossless: true,
        },
        CODEC_TYPE_MP3 | CODEC_TYPE_MP2 | CODEC_TYPE_MP1 => CodecInfo {
            name: "MP3".into(),
            lossless: false,
        },
        CODEC_TYPE_AAC => CodecInfo {
            name: "AAC".into(),
            lossless: false,
        },
        CODEC_TYPE_VORBIS => CodecInfo {
            name: "Vorbis".into(),
            lossless: false,
        },
        _ => CodecInfo {
            name: "PCM".into(),
            lossless: true,
        },
    }
}

/// An open, decodable audio track producing interleaved `f32` PCM frames.
pub struct DecodedStream {
    format: Box<dyn FormatReader>,
    decoder: Box<dyn Decoder>,
    track_id: u32,
    spec: PcmSpec,
    time_base: TimeBase,
    start_ts: u64,
    duration_secs: u32,
    bit_rate: u32,
    codec_info: CodecInfo,
    sample_buf: Option<SampleBuffer<f32>>,
    last_buf_spec: Option<(u32, usize)>,
}

impl std::fmt::Debug for DecodedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedStream")
            .field("spec", &self.spec)
            .field("duration_secs", &self.duration_secs)
            .field("bit_rate", &self.bit_rate)
            .field("codec", &self.codec_info.name)
            .finish()
    }
}

impl DecodedStream {
    /// Open and probe an `http://` or `https://` audio URI.
    ///
    /// Local filesystem paths and `file://` URIs are rejected by default for security;
    /// use [`DecodedStream::open_local`] or [`DecodedStream::open_with_options`] when local
    /// file access is explicitly desired.
    pub fn open(uri: &str) -> Result<Self, AudioError> {
        Self::open_with_options(uri, false, None)
    }

    /// Open and probe a local audio file on disk.
    pub fn open_local(path: &Path) -> Result<Self, AudioError> {
        Self::open_with_options(&path.to_string_lossy(), true, None)
    }

    /// Open and probe an audio URI with explicit local-file policy and optional cancellation.
    pub fn open_with_options(
        uri: &str,
        allow_local_files: bool,
        cancel: Option<LoadCancel>,
    ) -> Result<Self, AudioError> {
        let OpenedSource {
            source,
            hint,
            byte_len,
        } = open_media_source(uri, allow_local_files, cancel)?;
        let mss = MediaSourceStream::new(source, Default::default());

        let probed = symphonia::default::get_probe()
            .format(
                &hint,
                mss,
                &FormatOptions {
                    enable_gapless: true,
                    ..Default::default()
                },
                &MetadataOptions::default(),
            )
            .map_err(|e| AudioError::Decode(format!("probe failed for {uri}: {e}")))?;

        let format = probed.format;
        let track = format
            .default_track()
            .filter(|t| t.codec_params.codec != CODEC_TYPE_NULL)
            .or_else(|| {
                format
                    .tracks()
                    .iter()
                    .find(|t| t.codec_params.codec != CODEC_TYPE_NULL)
            })
            .ok_or_else(|| AudioError::Decode(format!("no audio track found in {uri}")))?;

        let track_id = track.id;
        let params = &track.codec_params;
        let codec_info = classify_codec(params.codec);
        let sample_rate = params
            .sample_rate
            .ok_or_else(|| AudioError::Decode("missing sample rate".into()))?;
        let channels = params
            .channels
            .map(|c| c.count() as u16)
            .unwrap_or(2)
            .max(1);
        let bits_per_sample = if codec_info.lossless {
            params
                .bits_per_sample
                .or(params.bits_per_coded_sample)
                .unwrap_or(16) as u16
        } else {
            0
        };

        let time_base = params
            .time_base
            .unwrap_or_else(|| TimeBase::new(1, sample_rate.max(1)));
        let start_ts = params.start_ts;
        let duration_secs = params
            .n_frames
            .map(|frames| time_base.calc_time(frames).seconds as u32)
            .unwrap_or(0);

        let bit_rate = match (byte_len, duration_secs) {
            (Some(bytes), dur) if dur > 0 => ((bytes.saturating_mul(8)) / u64::from(dur)) as u32,
            _ if codec_info.lossless => sample_rate
                .saturating_mul(u32::from(channels))
                .saturating_mul(u32::from(bits_per_sample.max(16))),
            _ => 0,
        };

        let decoder = symphonia::default::get_codecs()
            .make(params, &DecoderOptions::default())
            .map_err(|e| AudioError::Decode(format!("unsupported codec in {uri}: {e}")))?;

        Ok(Self {
            format,
            decoder,
            track_id,
            spec: PcmSpec::new(sample_rate, channels, bits_per_sample),
            time_base,
            start_ts,
            duration_secs,
            bit_rate,
            codec_info,
            sample_buf: None,
            last_buf_spec: None,
        })
    }

    /// Stream PCM specification (updated automatically if mid-stream format changes occur).
    pub fn spec(&self) -> PcmSpec {
        self.spec
    }

    /// Estimated duration in whole seconds (`0` if unknown / live stream).
    pub fn duration_secs(&self) -> u32 {
        self.duration_secs
    }

    /// Estimated bit rate in bits/second.
    pub fn bit_rate(&self) -> u32 {
        self.bit_rate
    }

    /// Canonical OpenHome codec name (`"FLAC"`, `"MP3"`, `"PCM"`, etc.).
    pub fn codec_name(&self) -> &str {
        &self.codec_info.name
    }

    /// Whether the decoded codec is lossless.
    pub fn is_lossless(&self) -> bool {
        self.codec_info.lossless
    }

    /// Seek to `seconds` from the start of the track using the stream's [`TimeBase`].
    pub fn seek(&mut self, seconds: u32) -> Result<u32, AudioError> {
        let seeked = self
            .format
            .seek(
                SeekMode::Coarse,
                SeekTo::Time {
                    time: Time::new(u64::from(seconds), 0.0),
                    track_id: Some(self.track_id),
                },
            )
            .map_err(|e| AudioError::Decode(format!("seek failed: {e}")))?;
        self.decoder.reset();
        let rel_ts = seeked.actual_ts.saturating_sub(self.start_ts);
        let actual_secs = self.time_base.calc_time(rel_ts).seconds as u32;
        Ok(actual_secs)
    }

    /// Decode the next block of interleaved normalized `f32` samples into `out`.
    ///
    /// Returns `Ok(true)` if samples were written to `out`, or `Ok(false)` at clean EOF.
    /// Updates [`DecodedStream::spec`] if the decoded frame's sample rate or channel count changed.
    pub fn next_frame_into(&mut self, out: &mut Vec<f32>) -> Result<bool, AudioError> {
        out.clear();
        loop {
            let packet = match self.format.next_packet() {
                Ok(p) => p,
                Err(SymphError::IoError(err))
                    if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return Ok(false);
                }
                Err(SymphError::ResetRequired) => {
                    self.decoder.reset();
                    continue;
                }
                Err(err) => return Err(AudioError::Decode(err.to_string())),
            };

            if packet.track_id() != self.track_id {
                continue;
            }

            let decoded = match self.decoder.decode(&packet) {
                Ok(buf) => buf,
                Err(SymphError::DecodeError(_)) => continue,
                Err(SymphError::IoError(err))
                    if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return Ok(false);
                }
                Err(err) => return Err(AudioError::Decode(err.to_string())),
            };

            let frames = decoded.frames();
            if frames == 0 {
                continue;
            }

            let sig = *decoded.spec();
            let ch = sig.channels.count().max(1);
            if sig.rate != self.spec.sample_rate || ch as u16 != self.spec.channels {
                self.spec = PcmSpec::new(sig.rate, ch as u16, self.spec.bits_per_sample);
            }

            let cap = decoded.capacity() as u64;
            let need_realloc = self.last_buf_spec != Some((sig.rate, ch))
                || self
                    .sample_buf
                    .as_ref()
                    .is_none_or(|b| b.capacity() < frames * ch);
            if need_realloc {
                self.sample_buf = Some(SampleBuffer::<f32>::new(cap, sig));
                self.last_buf_spec = Some((sig.rate, ch));
            }
            let sbuf = self.sample_buf.as_mut().unwrap();
            sbuf.copy_interleaved_ref(decoded);
            out.extend_from_slice(sbuf.samples());
            return Ok(true);
        }
    }

    /// Decode the next block of interleaved normalized `f32` samples.
    ///
    /// Returns `Ok(None)` at clean end-of-stream.
    pub fn next_frame(&mut self) -> Result<Option<Vec<f32>>, AudioError> {
        let mut buf = Vec::new();
        if self.next_frame_into(&mut buf)? {
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }
}

/// Reader wrapper that aborts with `Interrupted` as soon as a newer playback generation supersedes it.
struct CancellableReader<R> {
    inner: R,
    cancel: Option<LoadCancel>,
}

impl<R: Read> Read for CancellableReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.cancel.as_ref().is_some_and(LoadCancel::is_cancelled) {
            // Note: do NOT use ErrorKind::Interrupted because std::io::Read::read_to_end
            // retries Interrupted in an infinite loop!
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "track load cancelled",
            ));
        }
        self.inner.read(buf)
    }
}

/// A read-only streaming HTTP source implementing `MediaSource` (non-seekable for live/chunked streams).
struct StreamingHttpSource {
    reader: Box<dyn Read + Send + Sync>,
    byte_len: Option<u64>,
}

impl Read for StreamingHttpSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buf)
    }
}

impl Seek for StreamingHttpSource {
    fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "live HTTP stream is not seekable",
        ))
    }
}

impl MediaSource for StreamingHttpSource {
    fn is_seekable(&self) -> bool {
        false
    }

    fn byte_len(&self) -> Option<u64> {
        self.byte_len
    }
}

struct OpenedSource {
    source: Box<dyn MediaSource>,
    hint: Hint,
    byte_len: Option<u64>,
}

fn open_media_source(
    uri: &str,
    allow_local_files: bool,
    cancel: Option<LoadCancel>,
) -> Result<OpenedSource, AudioError> {
    if cancel.as_ref().is_some_and(LoadCancel::is_cancelled) {
        return Err(AudioError::Source("track load cancelled".into()));
    }

    let mut hint = Hint::new();
    let path_part = uri.split('?').next().unwrap_or(uri);
    if let Some(ext) = Path::new(path_part).extension().and_then(|e| e.to_str()) {
        hint.with_extension(ext);
    }

    if is_http_uri(uri) {
        // Use per-phase and per-read timeouts (NOT timeout_global) so live radio and long
        // streams play indefinitely while stalled connections still time out cleanly.
        let agent = ureq::Agent::config_builder()
            .timeout_connect(Some(HTTP_CONNECT_TIMEOUT))
            .timeout_send_request(Some(HTTP_CONNECT_TIMEOUT))
            .timeout_recv_response(Some(HTTP_IO_TIMEOUT))
            .timeout_per_read(Some(HTTP_IO_TIMEOUT))
            .build()
            .new_agent();
        let resp = agent
            .get(uri)
            .call()
            .map_err(|e| AudioError::Source(format!("HTTP GET {uri} failed: {e}")))?;

        let content_len = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        if let Some(ct) = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
        {
            let mime = ct.split(';').next().unwrap_or(ct).trim();
            if !mime.is_empty() {
                hint.mime_type(mime);
            }
        }

        // For finite tracks within budget, buffer into a seekable Cursor so Symphonia can
        // probe container trailers (MP4/FLAC/WAV) and support instant SeekSecondAbsolute.
        // Cap initial pre-allocation to 1 MiB so an untrusted Content-Length cannot force an
        // immediate 256 MiB allocation before any body bytes arrive.
        if let Some(len) = content_len
            && len <= MAX_HTTP_FETCH_BYTES
        {
            let mut data = Vec::with_capacity(len.min(1024 * 1024) as usize);
            let reader = CancellableReader {
                inner: resp.into_body().into_reader().take(MAX_HTTP_FETCH_BYTES),
                cancel,
            };
            let mut reader = reader;
            reader
                .read_to_end(&mut data)
                .map_err(|e| AudioError::Source(format!("HTTP read {uri} failed: {e}")))?;
            let actual_len = data.len() as u64;
            return Ok(OpenedSource {
                source: Box::new(Cursor::new(data)),
                hint,
                byte_len: Some(actual_len),
            });
        }

        // Unbounded / live radio stream -> stream incrementally without buffering to RAM.
        let reader = CancellableReader {
            inner: resp.into_body().into_reader(),
            cancel,
        };
        return Ok(OpenedSource {
            source: Box::new(StreamingHttpSource {
                reader: Box::new(reader),
                byte_len: content_len,
            }),
            hint,
            byte_len: content_len,
        });
    }

    if !allow_local_files {
        return Err(AudioError::Source(format!(
            "unsupported URI scheme (only http:// and https:// are allowed): {uri}"
        )));
    }

    let local_path = uri.strip_prefix("file://").unwrap_or(uri);
    let file = File::open(local_path)
        .map_err(|e| AudioError::Source(format!("cannot open {local_path}: {e}")))?;
    let byte_len = file.metadata().ok().map(|m| m.len());
    Ok(OpenedSource {
        source: Box::new(file),
        hint,
        byte_len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal valid 16-bit stereo PCM WAV file containing `num_frames` frames.
    pub(crate) fn synth_wav(sample_rate: u32, num_frames: usize) -> Vec<u8> {
        let channels: u16 = 2;
        let bits: u16 = 16;
        let block_align = channels * (bits / 8);
        let byte_rate = sample_rate * u32::from(block_align);
        let data_bytes = (num_frames * usize::from(block_align)) as u32;
        let mut out = Vec::with_capacity(44 + data_bytes as usize);
        out.extend_from_slice(b"RIFF");
        out.extend_from_slice(&(36 + data_bytes).to_le_bytes());
        out.extend_from_slice(b"WAVE");
        out.extend_from_slice(b"fmt ");
        out.extend_from_slice(&16u32.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes());
        out.extend_from_slice(&channels.to_le_bytes());
        out.extend_from_slice(&sample_rate.to_le_bytes());
        out.extend_from_slice(&byte_rate.to_le_bytes());
        out.extend_from_slice(&block_align.to_le_bytes());
        out.extend_from_slice(&bits.to_le_bytes());
        out.extend_from_slice(b"data");
        out.extend_from_slice(&data_bytes.to_le_bytes());
        for i in 0..num_frames {
            let sample = ((i as f32 * 0.05).sin() * 16_000.0) as i16;
            out.extend_from_slice(&sample.to_le_bytes());
            out.extend_from_slice(&(-sample).to_le_bytes());
        }
        out
    }

    #[test]
    fn open_rejects_local_files_by_default_and_open_local_decodes_wav() {
        let wav = synth_wav(44_100, 44_100 * 2);
        let path =
            std::env::temp_dir().join(format!("openhome-player-test-{}.wav", std::process::id()));
        std::fs::write(&path, &wav).unwrap();

        // Security default: file:// and bare local paths are rejected by DecodedStream::open().
        assert!(DecodedStream::open(&format!("file://{}", path.display())).is_err());
        assert!(DecodedStream::open(&path.to_string_lossy()).is_err());
        assert!(is_http_uri("HTTP://example.com/a.flac"));
        assert!(is_http_uri("https://example.com/a.flac"));
        assert!(!is_http_uri("🎵🎵🎵/track.flac"));
        assert!(!is_http_uri("file:///etc/passwd"));

        // Explicit local open succeeds.
        let mut stream = DecodedStream::open_local(&path).unwrap();
        assert_eq!(stream.spec(), PcmSpec::new(44_100, 2, 16));
        assert_eq!(stream.duration_secs(), 2);
        assert!(stream.bit_rate() > 0);
        assert_eq!(stream.codec_name(), "PCM");
        assert!(stream.is_lossless());
        assert!(format!("{stream:?}").contains("DecodedStream"));

        let first = stream.next_frame().unwrap().expect("first PCM block");
        assert!(!first.is_empty());

        let seeked = stream.seek(1).unwrap();
        assert!(seeked <= 1);

        let mut total_samples = 0usize;
        while let Some(block) = stream.next_frame().unwrap() {
            total_samples += block.len();
        }
        assert!(total_samples > 0);

        // Codec classification coverage.
        assert_eq!(classify_codec(CODEC_TYPE_FLAC).name, "FLAC");
        assert!(classify_codec(CODEC_TYPE_FLAC).lossless);
        assert_eq!(classify_codec(CODEC_TYPE_ALAC).name, "ALAC");
        assert_eq!(classify_codec(CODEC_TYPE_MP3).name, "MP3");
        assert!(!classify_codec(CODEC_TYPE_MP3).lossless);
        assert_eq!(classify_codec(CODEC_TYPE_AAC).name, "AAC");
        assert_eq!(classify_codec(CODEC_TYPE_VORBIS).name, "Vorbis");

        // Pre-cancelled load fails immediately.
        let latest = Arc::new(AtomicU64::new(5));
        assert!(
            DecodedStream::open_with_options(
                &path.to_string_lossy(),
                true,
                Some(LoadCancel::new(4, latest)),
            )
            .is_err()
        );

        let _ = std::fs::remove_file(&path);
    }
}
