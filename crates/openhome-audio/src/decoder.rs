//! Symphonia-based audio decoder supporting `http(s)://` URIs (and opt-in local files).

use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
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

/// Maximum buffered HTTP audio payload size when the server does not support `Accept-Ranges: bytes` (64 MiB).
const MAX_HTTP_FETCH_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum forward seek distance satisfied by discarding from the current HTTP stream instead of issuing a new `Range` request.
const RANGE_SKIP_THRESHOLD: u64 = 16 * 1024;
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_IO_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_CONSECUTIVE_RESETS: usize = 16;
const MAX_CONSECUTIVE_DECODE_ERRORS: usize = 64;
const MAX_SKIPPED_PACKETS: usize = 4096;

/// Shared HTTP agent so TLS root certificates and keep-alive pools are reused across tracks.
fn http_agent() -> &'static ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT.get_or_init(|| {
        // Use per-phase and per-read timeouts (NOT timeout_global) so live radio and long
        // streams play indefinitely while stalled connections still time out cleanly.
        ureq::Agent::config_builder()
            .timeout_connect(Some(HTTP_CONNECT_TIMEOUT))
            .timeout_send_request(Some(HTTP_CONNECT_TIMEOUT))
            .timeout_recv_response(Some(HTTP_IO_TIMEOUT))
            .timeout_per_read(Some(HTTP_IO_TIMEOUT))
            .build()
            .new_agent()
    })
}

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
    seekable: bool,
    cancel: Option<LoadCancel>,
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
        let uri = uri.trim();
        let OpenedSource {
            source,
            hint,
            byte_len,
            seekable,
        } = open_media_source(uri, allow_local_files, cancel.clone())?;
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
            .filter(|&r| r > 0)
            .ok_or_else(|| AudioError::Decode("missing or zero sample rate".into()))?;
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
            .unwrap_or_else(|| TimeBase::new(1, sample_rate));
        let start_ts = params.start_ts;
        let duration_secs = params
            .n_frames
            .map(|frames| u32::try_from(time_base.calc_time(frames).seconds).unwrap_or(u32::MAX))
            .unwrap_or(0);

        let bit_rate = match (byte_len, duration_secs) {
            (Some(bytes), dur) if dur > 0 => {
                u32::try_from(bytes.saturating_mul(8) / u64::from(dur)).unwrap_or(u32::MAX)
            }
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
            seekable,
            cancel,
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

    /// Whether the underlying media source supports seeking.
    pub fn is_seekable(&self) -> bool {
        self.seekable
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
        let mut consecutive_resets = 0usize;
        let mut consecutive_decode_errors = 0usize;
        let mut skipped_packets = 0usize;

        loop {
            if self.cancel.as_ref().is_some_and(LoadCancel::is_cancelled) {
                return Err(AudioError::Source("track decode cancelled".into()));
            }

            let packet = match self.format.next_packet() {
                Ok(p) => p,
                Err(SymphError::IoError(err))
                    if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return Ok(false);
                }
                Err(SymphError::ResetRequired) => {
                    consecutive_resets += 1;
                    if consecutive_resets > MAX_CONSECUTIVE_RESETS {
                        return Err(AudioError::Decode(
                            "too many consecutive decoder resets".into(),
                        ));
                    }
                    self.decoder.reset();
                    continue;
                }
                Err(err) => return Err(AudioError::Decode(err.to_string())),
            };

            if packet.track_id() != self.track_id {
                skipped_packets += 1;
                if skipped_packets > MAX_SKIPPED_PACKETS {
                    return Err(AudioError::Decode(
                        "too many non-audio packets without audio data".into(),
                    ));
                }
                continue;
            }

            let decoded = match self.decoder.decode(&packet) {
                Ok(buf) => buf,
                Err(SymphError::DecodeError(msg)) => {
                    consecutive_decode_errors += 1;
                    if consecutive_decode_errors > MAX_CONSECUTIVE_DECODE_ERRORS {
                        return Err(AudioError::Decode(format!(
                            "too many consecutive decode errors: {msg}"
                        )));
                    }
                    continue;
                }
                Err(SymphError::IoError(err))
                    if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return Ok(false);
                }
                Err(err) => return Err(AudioError::Decode(err.to_string())),
            };

            let frames = decoded.frames();
            if frames == 0 {
                skipped_packets += 1;
                if skipped_packets > MAX_SKIPPED_PACKETS {
                    return Err(AudioError::Decode(
                        "too many empty decoded audio frames".into(),
                    ));
                }
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

/// Seekable HTTP `MediaSource` backed by HTTP `Range: bytes=...` requests.
///
/// Streams directly from the initial `GET` response with O(1) RAM (no full-track download),
/// and issues `Range` requests on demand when Symphonia probes container trailers or seeks.
struct HttpRangeStream {
    uri: String,
    byte_len: u64,
    pos: u64,
    reader: Option<Box<dyn Read + Send + Sync>>,
    cancel: Option<LoadCancel>,
}

impl HttpRangeStream {
    fn open_range(&mut self) -> std::io::Result<()> {
        if self.cancel.as_ref().is_some_and(LoadCancel::is_cancelled) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "track load cancelled",
            ));
        }
        let resp = http_agent()
            .get(&self.uri)
            .header("Range", &format!("bytes={}-", self.pos))
            .call()
            .map_err(|e| std::io::Error::other(format!("HTTP Range GET failed: {e}")))?;
        let status = resp.status().as_u16();
        if status == 206 || (status == 200 && self.pos == 0) {
            self.reader = Some(Box::new(CancellableReader {
                inner: resp.into_body().into_reader(),
                cancel: self.cancel.clone(),
            }));
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!("HTTP Range request returned status {status}"),
            ))
        }
    }
}

impl Read for HttpRangeStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.cancel.as_ref().is_some_and(LoadCancel::is_cancelled) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "track load cancelled",
            ));
        }
        if self.pos >= self.byte_len || buf.is_empty() {
            return Ok(0);
        }
        if self.reader.is_none() {
            self.open_range()?;
        }
        let n = self.reader.as_mut().unwrap().read(buf)?;
        self.pos = self.pos.saturating_add(n as u64);
        Ok(n)
    }
}

impl Seek for HttpRangeStream {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let target = match pos {
            SeekFrom::Start(n) => i128::from(n),
            SeekFrom::End(off) => i128::from(self.byte_len) + i128::from(off),
            SeekFrom::Current(off) => i128::from(self.pos) + i128::from(off),
        };
        if target < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid seek to a negative position",
            ));
        }
        let target = (target as u64).min(self.byte_len);
        if target == self.pos {
            return Ok(self.pos);
        }

        // Satisfy small forward seeks by draining the active stream to avoid a new HTTP round-trip.
        if target > self.pos
            && target - self.pos <= RANGE_SKIP_THRESHOLD
            && let Some(reader) = self.reader.as_mut()
        {
            let mut remaining = (target - self.pos) as usize;
            let mut discard = [0u8; 4096];
            while remaining > 0 {
                let want = remaining.min(discard.len());
                let n = reader.read(&mut discard[..want])?;
                if n == 0 {
                    break;
                }
                self.pos += n as u64;
                remaining -= n;
            }
            if self.pos == target {
                return Ok(self.pos);
            }
        }

        self.reader = None;
        self.pos = target;
        Ok(self.pos)
    }
}

impl MediaSource for HttpRangeStream {
    fn is_seekable(&self) -> bool {
        true
    }

    fn byte_len(&self) -> Option<u64> {
        Some(self.byte_len)
    }
}

struct OpenedSource {
    source: Box<dyn MediaSource>,
    hint: Hint,
    byte_len: Option<u64>,
    seekable: bool,
}

/// Extract a file-extension hint strictly from the path component of a URI or filesystem path,
/// ignoring authority/domain TLDs (e.g. `http://server.local`), `?query`, and `#fragment`.
fn extract_extension_hint(uri: &str) -> Option<&str> {
    let without_frag = uri.split(['?', '#']).next().unwrap_or(uri);
    let path_only = if is_http_uri(without_frag) {
        let after_scheme = without_frag
            .find("://")
            .map(|i| &without_frag[i + 3..])
            .unwrap_or("");
        after_scheme.find('/').map(|i| &after_scheme[i..])?
    } else {
        without_frag
    };
    Path::new(path_only).extension().and_then(|e| e.to_str())
}

/// Parse a `file://` URI (case-insensitive, handling `file://localhost/...` and `%XX` escapes)
/// or bare filesystem path into a local path string.
fn parse_local_path(uri: &str) -> String {
    let stripped = if uri
        .get(..7)
        .is_some_and(|s| s.eq_ignore_ascii_case("file://"))
    {
        let rest = &uri[7..];
        if rest
            .get(..10)
            .is_some_and(|s| s.eq_ignore_ascii_case("localhost/"))
        {
            &rest[9..]
        } else {
            rest
        }
    } else {
        return uri.to_string();
    };
    percent_decode(stripped)
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
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
    if let Some(ext) = extract_extension_hint(uri) {
        hint.with_extension(ext);
    }

    if is_http_uri(uri) {
        let resp = http_agent()
            .get(uri)
            .call()
            .map_err(|e| AudioError::Source(format!("HTTP GET {uri} failed: {e}")))?;

        let content_len = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        let accepts_ranges = resp
            .headers()
            .get("accept-ranges")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.to_ascii_lowercase().contains("bytes"));

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

        // Preferred path: Range-capable HTTP server -> stream immediately with O(1) RAM
        // and full random-access seeking for arbitrarily large hi-res tracks.
        if accepts_ranges && let Some(len) = content_len.filter(|&l| l > 0) {
            let reader: Box<dyn Read + Send + Sync> = Box::new(CancellableReader {
                inner: resp.into_body().into_reader(),
                cancel: cancel.clone(),
            });
            return Ok(OpenedSource {
                source: Box::new(HttpRangeStream {
                    uri: uri.to_string(),
                    byte_len: len,
                    pos: 0,
                    reader: Some(reader),
                    cancel,
                }),
                hint,
                byte_len: Some(len),
                seekable: true,
            });
        }

        // Fallback for small finite tracks on servers that do not advertise Accept-Ranges:
        // buffer into a seekable Cursor (capping initial pre-allocation to 1 MiB).
        if let Some(len) = content_len
            && len <= MAX_HTTP_FETCH_BYTES
        {
            let mut data = Vec::with_capacity(len.min(1024 * 1024) as usize);
            let mut reader = CancellableReader {
                inner: resp.into_body().into_reader().take(MAX_HTTP_FETCH_BYTES),
                cancel,
            };
            reader
                .read_to_end(&mut data)
                .map_err(|e| AudioError::Source(format!("HTTP read {uri} failed: {e}")))?;
            let actual_len = data.len() as u64;
            return Ok(OpenedSource {
                source: Box::new(Cursor::new(data)),
                hint,
                byte_len: Some(actual_len),
                seekable: true,
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
            seekable: false,
        });
    }

    if !allow_local_files {
        return Err(AudioError::Source(format!(
            "unsupported URI scheme (only http:// and https:// are allowed): {uri}"
        )));
    }

    let local_path = parse_local_path(uri);
    let file = File::open(&local_path)
        .map_err(|e| AudioError::Source(format!("cannot open {local_path}: {e}")))?;
    let byte_len = file.metadata().ok().map(|m| m.len());
    Ok(OpenedSource {
        source: Box::new(file),
        hint,
        byte_len,
        seekable: true,
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

    #[test]
    fn http_range_stream_supports_streaming_seek_and_uri_helpers() {
        use std::io::Write;
        use std::net::TcpListener;

        // URI extension hint and file:// parsing edge cases.
        assert_eq!(extract_extension_hint("http://server.local"), None);
        assert_eq!(
            extract_extension_hint("http://server.local/music/track.flac?token=1#frag"),
            Some("flac")
        );
        assert_eq!(
            parse_local_path("FILE://localhost/tmp/my%20track%2b1%ZZ.wav"),
            "/tmp/my track+1%ZZ.wav"
        );
        assert_eq!(
            parse_local_path("file:///tmp/my%20file.wav"),
            "/tmp/my file.wav"
        );
        assert_eq!(parse_local_path("/tmp/plain.wav"), "/tmp/plain.wav");

        // Direct HttpRangeStream SeekFrom::Start / Current / End / negative / small-skip coverage.
        let mut range_stream = HttpRangeStream {
            uri: "http://127.0.0.1:1/x".into(),
            byte_len: 100,
            pos: 0,
            reader: Some(Box::new(Cursor::new(vec![42u8; 100]))),
            cancel: None,
        };
        assert!(range_stream.is_seekable());
        assert_eq!(range_stream.byte_len(), Some(100));
        // Small forward seek (<= 16 KiB) skips by draining active reader without HTTP round-trip.
        assert_eq!(range_stream.seek(SeekFrom::Start(10)).unwrap(), 10);
        assert_eq!(range_stream.seek(SeekFrom::Current(5)).unwrap(), 15);
        assert_eq!(range_stream.stream_position().unwrap(), 15);
        assert_eq!(range_stream.seek(SeekFrom::End(-10)).unwrap(), 90);
        assert!(range_stream.seek(SeekFrom::Start(0)).is_ok());
        assert!(range_stream.seek(SeekFrom::Current(-1)).is_err());
        let mut one = [0u8; 1];
        range_stream.pos = 100;
        assert_eq!(range_stream.read(&mut one).unwrap(), 0);

        // Spin up a tiny Range-capable HTTP/1.1 server serving a 2-second WAV.
        let wav = Arc::new(synth_wav(44_100, 44_100 * 2));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let wav_srv = Arc::clone(&wav);
        let srv_handle = std::thread::spawn(move || {
            // Serve up to 4 requests (initial GET + Range seeks).
            for _ in 0..4 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut req_buf = [0u8; 2048];
                let n = stream.read(&mut req_buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&req_buf[..n]);
                let range_start = req.lines().find_map(|l| {
                    let lower = l.to_ascii_lowercase();
                    lower
                        .strip_prefix("range: bytes=")
                        .and_then(|r| r.split('-').next())
                        .and_then(|s| s.trim().parse::<usize>().ok())
                });
                if let Some(start) = range_start {
                    let start = start.min(wav_srv.len());
                    let slice = &wav_srv[start..];
                    let hdr = format!(
                        "HTTP/1.1 206 Partial Content\r\n\
                         Content-Type: audio/wav\r\n\
                         Accept-Ranges: bytes\r\n\
                         Content-Range: bytes {start}-{}/{}\r\n\
                         Content-Length: {}\r\n\
                         Connection: close\r\n\r\n",
                        wav_srv.len().saturating_sub(1),
                        wav_srv.len(),
                        slice.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(slice);
                } else {
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\n\
                         Content-Type: audio/wav\r\n\
                         Accept-Ranges: bytes\r\n\
                         Content-Length: {}\r\n\
                         Connection: close\r\n\r\n",
                        wav_srv.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&wav_srv);
                }
            }
        });

        let uri = format!("  http://{addr}/song.wav?x=1#top  ");
        let latest = Arc::new(AtomicU64::new(1));
        let mut stream = DecodedStream::open_with_options(
            &uri,
            false,
            Some(LoadCancel::new(1, Arc::clone(&latest))),
        )
        .unwrap();
        assert!(stream.is_seekable());
        assert_eq!(stream.duration_secs(), 2);

        let first = stream.next_frame().unwrap().expect("first frame");
        assert!(!first.is_empty());

        // Seek forward triggers HTTP 206 Range request.
        let seeked = stream.seek(1).unwrap();
        assert!(seeked <= 1);
        let after_seek = stream.next_frame().unwrap().expect("frame after seek");
        assert!(!after_seek.is_empty());

        // Mid-stream cancellation aborts next_frame cleanly.
        latest.store(2, Ordering::Release);
        assert!(stream.next_frame().is_err());

        drop(stream);
        // Unblock listener if fewer than 4 requests were made.
        let _ = std::net::TcpStream::connect(addr);
        let _ = std::net::TcpStream::connect(addr);
        let _ = std::net::TcpStream::connect(addr);
        let _ = srv_handle.join();
    }
}
