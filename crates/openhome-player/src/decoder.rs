//! Symphonia-based audio decoder supporting local files, `file://`, and `http(s)://` URIs.

use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Duration;

use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{CODEC_TYPE_NULL, Decoder, DecoderOptions};
use symphonia::core::errors::Error as SymphError;
use symphonia::core::formats::{FormatOptions, FormatReader, SeekMode, SeekTo};
use symphonia::core::io::{MediaSource, MediaSourceStream};
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use symphonia::core::units::Time;

use crate::error::PlayerError;
use crate::format::PcmSpec;

/// Maximum buffered HTTP audio payload size when fetching non-rangeable streams (256 MiB).
const MAX_HTTP_FETCH_BYTES: u64 = 256 * 1024 * 1024;
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// An open, decodable audio track producing interleaved `f32` PCM frames.
pub struct DecodedStream {
    format: Box<dyn FormatReader>,
    decoder: Box<dyn Decoder>,
    track_id: u32,
    spec: PcmSpec,
    duration_secs: u32,
    bit_rate: u32,
    sample_buf: Option<SampleBuffer<f32>>,
}

impl std::fmt::Debug for DecodedStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedStream")
            .field("spec", &self.spec)
            .field("duration_secs", &self.duration_secs)
            .field("bit_rate", &self.bit_rate)
            .finish()
    }
}

impl DecodedStream {
    /// Open and probe an audio URI (`http://`, `https://`, `file://`, or local filesystem path).
    pub fn open(uri: &str) -> Result<Self, PlayerError> {
        let (source, hint) = open_media_source(uri)?;
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
            .map_err(|e| PlayerError::Decode(format!("probe failed for {uri}: {e}")))?;

        let format = probed.format;
        let track = format
            .tracks()
            .iter()
            .find(|t| t.codec_params.codec != CODEC_TYPE_NULL)
            .ok_or_else(|| PlayerError::Decode(format!("no audio track found in {uri}")))?;

        let track_id = track.id;
        let params = &track.codec_params;
        let sample_rate = params
            .sample_rate
            .ok_or_else(|| PlayerError::Decode("missing sample rate".into()))?;
        let channels = params
            .channels
            .map(|c| c.count() as u16)
            .unwrap_or(2)
            .max(1);
        let bits_per_sample = params
            .bits_per_sample
            .or(params.bits_per_coded_sample)
            .unwrap_or(16) as u16;

        let duration_secs = params
            .n_frames
            .map(|frames| (frames / u64::from(sample_rate.max(1))) as u32)
            .unwrap_or(0);
        let bit_rate = sample_rate
            .saturating_mul(u32::from(channels))
            .saturating_mul(u32::from(bits_per_sample));

        let decoder = symphonia::default::get_codecs()
            .make(params, &DecoderOptions::default())
            .map_err(|e| PlayerError::Decode(format!("unsupported codec in {uri}: {e}")))?;

        Ok(Self {
            format,
            decoder,
            track_id,
            spec: PcmSpec::new(sample_rate, channels, bits_per_sample),
            duration_secs,
            bit_rate,
            sample_buf: None,
        })
    }

    /// Stream PCM specification.
    pub fn spec(&self) -> PcmSpec {
        self.spec
    }

    /// Estimated duration in whole seconds (`0` if unknown / live stream).
    pub fn duration_secs(&self) -> u32 {
        self.duration_secs
    }

    /// Nominal uncompressed bit rate in bits/second.
    pub fn bit_rate(&self) -> u32 {
        self.bit_rate
    }

    /// Seek to `seconds` from the start of the track.
    pub fn seek(&mut self, seconds: u32) -> Result<u32, PlayerError> {
        let seeked = self
            .format
            .seek(
                SeekMode::Coarse,
                SeekTo::Time {
                    time: Time::new(u64::from(seconds), 0.0),
                    track_id: Some(self.track_id),
                },
            )
            .map_err(|e| PlayerError::Decode(format!("seek failed: {e}")))?;
        self.decoder.reset();
        let actual_secs = if self.spec.sample_rate > 0 {
            (seeked.actual_ts / u64::from(self.spec.sample_rate)) as u32
        } else {
            seconds
        };
        Ok(actual_secs)
    }

    /// Decode the next block of interleaved normalized `f32` samples.
    ///
    /// Returns `Ok(None)` at clean end-of-stream.
    pub fn next_frame(&mut self) -> Result<Option<Vec<f32>>, PlayerError> {
        loop {
            let packet = match self.format.next_packet() {
                Ok(p) => p,
                Err(SymphError::IoError(err))
                    if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    return Ok(None);
                }
                Err(SymphError::ResetRequired) => {
                    self.decoder.reset();
                    continue;
                }
                Err(err) => return Err(PlayerError::Decode(err.to_string())),
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
                    return Ok(None);
                }
                Err(err) => return Err(PlayerError::Decode(err.to_string())),
            };

            let frames = decoded.frames();
            if frames == 0 {
                continue;
            }

            let spec = *decoded.spec();
            let cap = decoded.capacity() as u64;
            let sbuf = self
                .sample_buf
                .get_or_insert_with(|| SampleBuffer::<f32>::new(cap, spec));
            if sbuf.capacity() < frames * spec.channels.count() {
                *sbuf = SampleBuffer::<f32>::new(cap, spec);
            }
            sbuf.copy_interleaved_ref(decoded);
            return Ok(Some(sbuf.samples().to_vec()));
        }
    }
}

/// A read-only streaming HTTP source implementing `MediaSource` (non-seekable fallback for live/chunked streams).
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

fn open_media_source(uri: &str) -> Result<(Box<dyn MediaSource>, Hint), PlayerError> {
    let mut hint = Hint::new();
    let path_part = uri.split('?').next().unwrap_or(uri);
    if let Some(ext) = Path::new(path_part).extension().and_then(|e| e.to_str()) {
        hint.with_extension(ext);
    }

    if uri.starts_with("http://") || uri.starts_with("https://") {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(HTTP_TIMEOUT))
            .build()
            .new_agent();
        let resp = agent
            .get(uri)
            .call()
            .map_err(|e| PlayerError::Source(format!("HTTP GET {uri} failed: {e}")))?;

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
            resp.into_body()
                .into_reader()
                .take(MAX_HTTP_FETCH_BYTES)
                .read_to_end(&mut data)
                .map_err(|e| PlayerError::Source(format!("HTTP read {uri} failed: {e}")))?;
            return Ok((Box::new(Cursor::new(data)), hint));
        }

        // Unbounded / live radio stream -> stream incrementally without buffering to RAM.
        let reader = resp.into_body().into_reader();
        return Ok((
            Box::new(StreamingHttpSource {
                reader: Box::new(reader),
                byte_len: content_len,
            }),
            hint,
        ));
    }

    let local_path = uri.strip_prefix("file://").unwrap_or(uri);
    let file = File::open(local_path)
        .map_err(|e| PlayerError::Source(format!("cannot open {local_path}: {e}")))?;
    Ok((Box::new(file), hint))
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
        out.extend_from_slice(&1u16.to_le_bytes()); // PCM
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
    fn decoded_stream_reads_and_seeks_local_wav() {
        let wav = synth_wav(44_100, 44_100 * 2); // 2 seconds
        let path =
            std::env::temp_dir().join(format!("openhome-player-test-{}.wav", std::process::id()));
        std::fs::write(&path, &wav).unwrap();

        let mut stream = DecodedStream::open(&format!("file://{}", path.display())).unwrap();
        assert_eq!(stream.spec(), PcmSpec::new(44_100, 2, 16));
        assert_eq!(stream.duration_secs(), 2);
        assert!(stream.bit_rate() > 0);
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

        let _ = std::fs::remove_file(&path);
    }
}
