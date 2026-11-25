//! Songcast OHM / OHU wire protocol framing (`"Ohm "` magic, major version 1).
//!
//! Verified byte-for-byte against `ohSongcast/Ohm.h` & `ohPipeline/OpenHome/Av/Songcast/Ohm.h`.
//! Note that unicast (`ohu://`) uses the exact same `"Ohm "` wire format over unicast UDP.

use std::net::{Ipv4Addr, SocketAddrV4};

use crate::error::SongcastError;

/// 4-byte OHM packet magic (`"Ohm "`).
pub const OHM_MAGIC: [u8; 4] = *b"Ohm ";

/// OHM protocol major version (`1`).
pub const OHM_MAJOR: u8 = 1;

/// Fixed 8-byte OHM outer header length.
pub const OHM_HEADER_BYTES: usize = 8;

/// Fixed 50-byte OHM Audio sub-header length (excluding variable-length codec name).
pub const OHM_AUDIO_SUBHEADER_BYTES: usize = 50;

/// Maximum codec name length in `OhmAudio` (`u8::MAX`).
pub const OHM_MAX_CODEC_BYTES: usize = 255;

/// Maximum total OHM packet size representable by the 16-bit `total_bytes` field.
pub const OHM_MAX_PACKET_BYTES: usize = u16::MAX as usize;

/// OHM message type identifiers (`OhmHeader::kMsgType*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OhmMsgType {
    Join = 0,
    Listen = 1,
    Leave = 2,
    Audio = 3,
    Track = 4,
    Metatext = 5,
    Slave = 6,
    Resend = 7,
}

impl TryFrom<u8> for OhmMsgType {
    type Error = SongcastError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Join),
            1 => Ok(Self::Listen),
            2 => Ok(Self::Leave),
            3 => Ok(Self::Audio),
            4 => Ok(Self::Track),
            5 => Ok(Self::Metatext),
            6 => Ok(Self::Slave),
            7 => Ok(Self::Resend),
            other => Err(SongcastError::UnknownMsgType(other)),
        }
    }
}

/// 8-byte OHM packet header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OhmHeader {
    pub major: u8,
    pub msg_type: OhmMsgType,
    /// Total packet size in bytes, including this 8-byte header.
    pub total_bytes: u16,
}

impl OhmHeader {
    pub fn new(msg_type: OhmMsgType, payload_bytes: usize) -> Result<Self, SongcastError> {
        let total =
            OHM_HEADER_BYTES
                .checked_add(payload_bytes)
                .ok_or(SongcastError::PacketTooLarge {
                    max: OHM_MAX_PACKET_BYTES,
                    actual: usize::MAX,
                })?;
        if total > OHM_MAX_PACKET_BYTES {
            return Err(SongcastError::PacketTooLarge {
                max: OHM_MAX_PACKET_BYTES,
                actual: total,
            });
        }
        Ok(Self {
            major: OHM_MAJOR,
            msg_type,
            total_bytes: total as u16,
        })
    }

    pub fn payload_bytes(&self) -> usize {
        (self.total_bytes as usize).saturating_sub(OHM_HEADER_BYTES)
    }

    pub fn encode(&self) -> [u8; OHM_HEADER_BYTES] {
        let mut buf = [0u8; OHM_HEADER_BYTES];
        buf[0..4].copy_from_slice(&OHM_MAGIC);
        buf[4] = self.major;
        buf[5] = self.msg_type as u8;
        buf[6..8].copy_from_slice(&self.total_bytes.to_be_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SongcastError> {
        if bytes.len() < OHM_HEADER_BYTES {
            return Err(SongcastError::PacketTooShort {
                expected: OHM_HEADER_BYTES,
                actual: bytes.len(),
            });
        }
        let magic: [u8; 4] = bytes[0..4].try_into().unwrap();
        if magic != OHM_MAGIC {
            return Err(SongcastError::InvalidMagic {
                expected: OHM_MAGIC,
                actual: magic,
            });
        }
        let major = bytes[4];
        if major != OHM_MAJOR {
            return Err(SongcastError::UnsupportedVersion(major));
        }
        let msg_type = OhmMsgType::try_from(bytes[5])?;
        let total_bytes = u16::from_be_bytes([bytes[6], bytes[7]]);
        if (total_bytes as usize) < OHM_HEADER_BYTES || (total_bytes as usize) > bytes.len() {
            return Err(SongcastError::InvalidLength {
                declared: total_bytes as usize,
                actual: bytes.len(),
            });
        }
        Ok(Self {
            major,
            msg_type,
            total_bytes,
        })
    }
}

/// Bitfield flags carried in `OhmAudio::flags` (`OhmHeaderAudio::kFlag*`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AudioFlags(pub u8);

impl AudioFlags {
    /// End-of-stream / stream halt marker.
    pub const HALT: u8 = 1 << 0;
    /// Audio payload is lossless PCM.
    pub const LOSSLESS: u8 = 1 << 1;
    /// `network_timestamp` is populated by a hardware/software timestamper.
    pub const TIMESTAMPED: u8 = 1 << 2;
    /// Frame is a retransmission in response to an `OhmMessage::Resend`.
    pub const RESENT: u8 = 1 << 3;
    /// Extended timestamping flag (`ohPipeline` `kFlagTimestamped2`).
    pub const TIMESTAMPED2: u8 = 1 << 4;

    pub fn is_halt(self) -> bool {
        (self.0 & Self::HALT) != 0
    }

    pub fn is_lossless(self) -> bool {
        (self.0 & Self::LOSSLESS) != 0
    }

    pub fn is_timestamped(self) -> bool {
        (self.0 & Self::TIMESTAMPED) != 0
    }

    pub fn is_resent(self) -> bool {
        (self.0 & Self::RESENT) != 0
    }

    pub fn is_timestamped2(self) -> bool {
        (self.0 & Self::TIMESTAMPED2) != 0
    }

    pub fn with_resent(self) -> Self {
        Self(self.0 | Self::RESENT)
    }
}

/// Decoded OHM Audio frame (`OhmMsgType::Audio`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OhmAudio {
    pub flags: AudioFlags,
    /// Number of audio samples per channel in `audio`.
    pub samples: u16,
    /// Monotonically increasing frame sequence number.
    pub frame: u32,
    /// Network capture/transmit timestamp in Songcast ticks.
    pub network_timestamp: u32,
    /// Sender pipeline latency in Songcast ticks.
    pub media_latency: u32,
    /// Media timestamp (typically `0` in `ohSongcast` / `ohPipeline`).
    pub media_timestamp: u32,
    /// Sample offset of the start of this frame within the current track.
    pub sample_start: u64,
    /// Total samples in the current track (`0` if unknown / live stream).
    pub samples_total: u64,
    /// Sample rate in Hz (e.g. `44100`, `48000`, `96000`).
    pub sample_rate: u32,
    /// Bit rate in bits/sec.
    pub bit_rate: u32,
    /// Volume offset in 1/1024 dB units (signed 16-bit).
    pub volume_offset: i16,
    /// Bits per sample (e.g. `16`, `24`).
    pub bit_depth: u8,
    /// Channel count (e.g. `2` for stereo).
    pub channels: u8,
    /// Codec name (e.g. `"PCM"`).
    pub codec: String,
    /// Big-endian, interleaved, packed PCM audio bytes.
    pub audio: Vec<u8>,
}

/// Decoded OHM Track metadata packet (`OhmMsgType::Track`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OhmTrack {
    pub sequence: u32,
    pub uri: String,
    pub metadata: String,
}

/// Decoded OHM Metatext packet (`OhmMsgType::Metatext`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OhmMetatext {
    pub sequence: u32,
    pub metatext: String,
}

/// Full OHM / OHU message enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OhmMessage {
    Join,
    Listen,
    Leave,
    Audio(OhmAudio),
    Track(OhmTrack),
    Metatext(OhmMetatext),
    Slave(Vec<SocketAddrV4>),
    Resend(Vec<u32>),
}

impl OhmMessage {
    /// Encode this OHM message into a complete UDP datagram.
    pub fn encode(&self) -> Result<Vec<u8>, SongcastError> {
        match self {
            Self::Join => Ok(OhmHeader::new(OhmMsgType::Join, 0)?.encode().to_vec()),
            Self::Listen => Ok(OhmHeader::new(OhmMsgType::Listen, 0)?.encode().to_vec()),
            Self::Leave => Ok(OhmHeader::new(OhmMsgType::Leave, 0)?.encode().to_vec()),
            Self::Audio(a) => encode_audio(a),
            Self::Track(t) => encode_track(t),
            Self::Metatext(m) => encode_metatext(m),
            Self::Slave(slaves) => encode_slave(slaves),
            Self::Resend(frames) => encode_resend(frames),
        }
    }

    /// Decode an OHM message from a UDP datagram.
    pub fn decode(bytes: &[u8]) -> Result<Self, SongcastError> {
        let hdr = OhmHeader::decode(bytes)?;
        let payload = &bytes[OHM_HEADER_BYTES..hdr.total_bytes as usize];
        match hdr.msg_type {
            OhmMsgType::Join => Ok(Self::Join),
            OhmMsgType::Listen => Ok(Self::Listen),
            OhmMsgType::Leave => Ok(Self::Leave),
            OhmMsgType::Audio => decode_audio(payload).map(Self::Audio),
            OhmMsgType::Track => decode_track(payload).map(Self::Track),
            OhmMsgType::Metatext => decode_metatext(payload).map(Self::Metatext),
            OhmMsgType::Slave => decode_slave(payload).map(Self::Slave),
            OhmMsgType::Resend => decode_resend(payload).map(Self::Resend),
        }
    }
}

fn encode_audio(a: &OhmAudio) -> Result<Vec<u8>, SongcastError> {
    let codec_bytes = a.codec.as_bytes();
    if codec_bytes.len() > OHM_MAX_CODEC_BYTES {
        return Err(SongcastError::PacketTooLarge {
            max: OHM_MAX_CODEC_BYTES,
            actual: codec_bytes.len(),
        });
    }
    let payload_len = OHM_AUDIO_SUBHEADER_BYTES + codec_bytes.len() + a.audio.len();
    let hdr = OhmHeader::new(OhmMsgType::Audio, payload_len)?;
    let mut out = Vec::with_capacity(hdr.total_bytes as usize);
    out.extend_from_slice(&hdr.encode());
    out.push(OHM_AUDIO_SUBHEADER_BYTES as u8);
    out.push(a.flags.0);
    out.extend_from_slice(&a.samples.to_be_bytes());
    out.extend_from_slice(&a.frame.to_be_bytes());
    out.extend_from_slice(&a.network_timestamp.to_be_bytes());
    out.extend_from_slice(&a.media_latency.to_be_bytes());
    out.extend_from_slice(&a.media_timestamp.to_be_bytes());
    out.extend_from_slice(&a.sample_start.to_be_bytes());
    out.extend_from_slice(&a.samples_total.to_be_bytes());
    out.extend_from_slice(&a.sample_rate.to_be_bytes());
    out.extend_from_slice(&a.bit_rate.to_be_bytes());
    out.extend_from_slice(&a.volume_offset.to_be_bytes());
    out.push(a.bit_depth);
    out.push(a.channels);
    out.push(0); // reserved
    out.push(codec_bytes.len() as u8);
    out.extend_from_slice(codec_bytes);
    out.extend_from_slice(&a.audio);
    Ok(out)
}

fn decode_audio(payload: &[u8]) -> Result<OhmAudio, SongcastError> {
    if payload.len() < OHM_AUDIO_SUBHEADER_BYTES {
        return Err(SongcastError::PacketTooShort {
            expected: OHM_AUDIO_SUBHEADER_BYTES,
            actual: payload.len(),
        });
    }
    let sub_hdr_bytes = payload[0];
    if (sub_hdr_bytes as usize) != OHM_AUDIO_SUBHEADER_BYTES {
        return Err(SongcastError::InvalidAudioHeaderLength(sub_hdr_bytes));
    }
    let flags = AudioFlags(payload[1]);
    let samples = u16::from_be_bytes([payload[2], payload[3]]);
    let frame = u32::from_be_bytes(payload[4..8].try_into().unwrap());
    let network_timestamp = u32::from_be_bytes(payload[8..12].try_into().unwrap());
    let media_latency = u32::from_be_bytes(payload[12..16].try_into().unwrap());
    let media_timestamp = u32::from_be_bytes(payload[16..20].try_into().unwrap());
    let sample_start = u64::from_be_bytes(payload[20..28].try_into().unwrap());
    let samples_total = u64::from_be_bytes(payload[28..36].try_into().unwrap());
    let sample_rate = u32::from_be_bytes(payload[36..40].try_into().unwrap());
    let bit_rate = u32::from_be_bytes(payload[40..44].try_into().unwrap());
    let volume_offset = i16::from_be_bytes([payload[44], payload[45]]);
    let bit_depth = payload[46];
    let channels = payload[47];
    let reserved = payload[48];
    if reserved != 0 {
        return Err(SongcastError::InvalidAudioHeaderLength(reserved));
    }
    let codec_len = payload[49] as usize;
    let codec_end = OHM_AUDIO_SUBHEADER_BYTES + codec_len;
    if payload.len() < codec_end {
        return Err(SongcastError::PacketTooShort {
            expected: codec_end,
            actual: payload.len(),
        });
    }
    let codec = std::str::from_utf8(&payload[OHM_AUDIO_SUBHEADER_BYTES..codec_end])?.to_string();
    let audio = payload[codec_end..].to_vec();
    Ok(OhmAudio {
        flags,
        samples,
        frame,
        network_timestamp,
        media_latency,
        media_timestamp,
        sample_start,
        samples_total,
        sample_rate,
        bit_rate,
        volume_offset,
        bit_depth,
        channels,
        codec,
        audio,
    })
}

fn encode_track(t: &OhmTrack) -> Result<Vec<u8>, SongcastError> {
    let uri = t.uri.as_bytes();
    let meta = t.metadata.as_bytes();
    let payload_len = 12 + uri.len() + meta.len();
    let hdr = OhmHeader::new(OhmMsgType::Track, payload_len)?;
    let mut out = Vec::with_capacity(hdr.total_bytes as usize);
    out.extend_from_slice(&hdr.encode());
    out.extend_from_slice(&t.sequence.to_be_bytes());
    out.extend_from_slice(&(uri.len() as u32).to_be_bytes());
    out.extend_from_slice(&(meta.len() as u32).to_be_bytes());
    out.extend_from_slice(uri);
    out.extend_from_slice(meta);
    Ok(out)
}

fn decode_track(payload: &[u8]) -> Result<OhmTrack, SongcastError> {
    if payload.len() < 12 {
        return Err(SongcastError::PacketTooShort {
            expected: 12,
            actual: payload.len(),
        });
    }
    let sequence = u32::from_be_bytes(payload[0..4].try_into().unwrap());
    let uri_len = u32::from_be_bytes(payload[4..8].try_into().unwrap()) as usize;
    let meta_len = u32::from_be_bytes(payload[8..12].try_into().unwrap()) as usize;
    let expected = 12usize
        .checked_add(uri_len)
        .and_then(|v| v.checked_add(meta_len))
        .ok_or(SongcastError::PacketTooShort {
            expected: usize::MAX,
            actual: payload.len(),
        })?;
    if payload.len() < expected {
        return Err(SongcastError::PacketTooShort {
            expected,
            actual: payload.len(),
        });
    }
    let uri = std::str::from_utf8(&payload[12..12 + uri_len])?.to_string();
    let metadata = std::str::from_utf8(&payload[12 + uri_len..expected])?.to_string();
    Ok(OhmTrack {
        sequence,
        uri,
        metadata,
    })
}

fn encode_metatext(m: &OhmMetatext) -> Result<Vec<u8>, SongcastError> {
    let text = m.metatext.as_bytes();
    let payload_len = 8 + text.len();
    let hdr = OhmHeader::new(OhmMsgType::Metatext, payload_len)?;
    let mut out = Vec::with_capacity(hdr.total_bytes as usize);
    out.extend_from_slice(&hdr.encode());
    out.extend_from_slice(&m.sequence.to_be_bytes());
    out.extend_from_slice(&(text.len() as u32).to_be_bytes());
    out.extend_from_slice(text);
    Ok(out)
}

fn decode_metatext(payload: &[u8]) -> Result<OhmMetatext, SongcastError> {
    if payload.len() < 8 {
        return Err(SongcastError::PacketTooShort {
            expected: 8,
            actual: payload.len(),
        });
    }
    let sequence = u32::from_be_bytes(payload[0..4].try_into().unwrap());
    let text_len = u32::from_be_bytes(payload[4..8].try_into().unwrap()) as usize;
    let expected = 8usize
        .checked_add(text_len)
        .ok_or(SongcastError::PacketTooShort {
            expected: usize::MAX,
            actual: payload.len(),
        })?;
    if payload.len() < expected {
        return Err(SongcastError::PacketTooShort {
            expected,
            actual: payload.len(),
        });
    }
    let metatext = std::str::from_utf8(&payload[8..expected])?.to_string();
    Ok(OhmMetatext { sequence, metatext })
}

fn encode_slave(slaves: &[SocketAddrV4]) -> Result<Vec<u8>, SongcastError> {
    let payload_len = 4 + slaves.len() * 6;
    let hdr = OhmHeader::new(OhmMsgType::Slave, payload_len)?;
    let mut out = Vec::with_capacity(hdr.total_bytes as usize);
    out.extend_from_slice(&hdr.encode());
    out.extend_from_slice(&(slaves.len() as u32).to_be_bytes());
    for addr in slaves {
        out.extend_from_slice(&addr.ip().octets());
        out.extend_from_slice(&addr.port().to_be_bytes());
    }
    Ok(out)
}

fn decode_slave(payload: &[u8]) -> Result<Vec<SocketAddrV4>, SongcastError> {
    if payload.len() < 4 {
        return Err(SongcastError::PacketTooShort {
            expected: 4,
            actual: payload.len(),
        });
    }
    let count = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
    let expected = count.checked_mul(6).and_then(|n| n.checked_add(4)).ok_or(
        SongcastError::PacketTooShort {
            expected: usize::MAX,
            actual: payload.len(),
        },
    )?;
    if payload.len() < expected {
        return Err(SongcastError::PacketTooShort {
            expected,
            actual: payload.len(),
        });
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let off = 4 + i * 6;
        let ip = Ipv4Addr::new(
            payload[off],
            payload[off + 1],
            payload[off + 2],
            payload[off + 3],
        );
        let port = u16::from_be_bytes([payload[off + 4], payload[off + 5]]);
        out.push(SocketAddrV4::new(ip, port));
    }
    Ok(out)
}

fn encode_resend(frames: &[u32]) -> Result<Vec<u8>, SongcastError> {
    let payload_len = 4 + frames.len() * 4;
    let hdr = OhmHeader::new(OhmMsgType::Resend, payload_len)?;
    let mut out = Vec::with_capacity(hdr.total_bytes as usize);
    out.extend_from_slice(&hdr.encode());
    out.extend_from_slice(&(frames.len() as u32).to_be_bytes());
    for f in frames {
        out.extend_from_slice(&f.to_be_bytes());
    }
    Ok(out)
}

fn decode_resend(payload: &[u8]) -> Result<Vec<u32>, SongcastError> {
    if payload.len() < 4 {
        return Err(SongcastError::PacketTooShort {
            expected: 4,
            actual: payload.len(),
        });
    }
    let count = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
    let expected = count.checked_mul(4).and_then(|n| n.checked_add(4)).ok_or(
        SongcastError::PacketTooShort {
            expected: usize::MAX,
            actual: payload.len(),
        },
    )?;
    if payload.len() < expected {
        return Err(SongcastError::PacketTooShort {
            expected,
            actual: payload.len(),
        });
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let off = 4 + i * 4;
        out.push(u32::from_be_bytes(
            payload[off..off + 4].try_into().unwrap(),
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_messages_roundtrip_as_8_byte_packets() {
        for msg in [OhmMessage::Join, OhmMessage::Listen, OhmMessage::Leave] {
            let bytes = msg.encode().unwrap();
            assert_eq!(bytes.len(), 8);
            assert_eq!(&bytes[0..4], b"Ohm ");
            assert_eq!(bytes[4], 1);
            assert_eq!(u16::from_be_bytes([bytes[6], bytes[7]]), 8);
            assert_eq!(OhmMessage::decode(&bytes).unwrap(), msg);
        }
    }

    #[test]
    fn audio_track_metatext_slave_and_resend_roundtrip() {
        let audio = OhmMessage::Audio(OhmAudio {
            flags: AudioFlags(AudioFlags::LOSSLESS | AudioFlags::TIMESTAMPED),
            samples: 240,
            frame: 42,
            network_timestamp: 123_456,
            media_latency: 1_228_800,
            media_timestamp: 0,
            sample_start: 48_000,
            samples_total: 4_800_000,
            sample_rate: 48_000,
            bit_rate: 1_536_000,
            volume_offset: -512,
            bit_depth: 16,
            channels: 2,
            codec: "PCM".into(),
            audio: vec![0x12, 0x34, 0x56, 0x78],
        });
        let raw = audio.encode().unwrap();
        assert_eq!(raw.len(), 8 + 50 + 3 + 4);
        assert_eq!(raw[8], 50); // sub-header length
        assert_eq!(OhmMessage::decode(&raw).unwrap(), audio);

        let track = OhmMessage::Track(OhmTrack {
            sequence: 7,
            uri: "http://10.0.0.1/track.flac".into(),
            metadata: "<DIDL-Lite/>".into(),
        });
        assert_eq!(OhmMessage::decode(&track.encode().unwrap()).unwrap(), track);

        let metatext = OhmMessage::Metatext(OhmMetatext {
            sequence: 9,
            metatext: "<DIDL-Lite><item/></DIDL-Lite>".into(),
        });
        assert_eq!(
            OhmMessage::decode(&metatext.encode().unwrap()).unwrap(),
            metatext
        );

        let slaves = OhmMessage::Slave(vec![
            SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 10), 51970),
            SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 11), 51971),
        ]);
        assert_eq!(
            OhmMessage::decode(&slaves.encode().unwrap()).unwrap(),
            slaves
        );

        let resend = OhmMessage::Resend(vec![10, 11, 15]);
        assert_eq!(
            OhmMessage::decode(&resend.encode().unwrap()).unwrap(),
            resend
        );
    }

    #[test]
    fn rejects_malformed_headers_and_truncated_payloads() {
        assert!(matches!(
            OhmHeader::decode(&[0; 4]),
            Err(SongcastError::PacketTooShort { .. })
        ));
        assert!(matches!(
            OhmHeader::decode(b"Bad!\x01\x00\x00\x08"),
            Err(SongcastError::InvalidMagic { .. })
        ));
        assert!(matches!(
            OhmHeader::decode(b"Ohm \x02\x00\x00\x08"),
            Err(SongcastError::UnsupportedVersion(2))
        ));
        assert!(matches!(
            OhmHeader::decode(b"Ohm \x01\x99\x00\x08"),
            Err(SongcastError::UnknownMsgType(0x99))
        ));
        assert!(matches!(
            OhmHeader::decode(b"Ohm \x01\x00\x00\x20"),
            Err(SongcastError::InvalidLength { .. })
        ));
        // Audio packet with invalid sub-header length
        let mut bad_audio = vec![0u8; 8 + 50];
        bad_audio[0..4].copy_from_slice(b"Ohm ");
        bad_audio[4] = 1;
        bad_audio[5] = OhmMsgType::Audio as u8;
        bad_audio[6..8].copy_from_slice(&(58u16).to_be_bytes());
        bad_audio[8] = 49;
        assert!(matches!(
            OhmMessage::decode(&bad_audio),
            Err(SongcastError::InvalidAudioHeaderLength(49))
        ));
        // Truncated Track / Metatext / Slave / Resend
        for msg_type in [
            OhmMsgType::Audio,
            OhmMsgType::Track,
            OhmMsgType::Metatext,
            OhmMsgType::Slave,
            OhmMsgType::Resend,
        ] {
            let hdr = OhmHeader::new(msg_type, 0).unwrap().encode();
            assert!(matches!(
                OhmMessage::decode(&hdr),
                Err(SongcastError::PacketTooShort { .. })
            ));
        }

        let flags = AudioFlags(AudioFlags::HALT | AudioFlags::LOSSLESS | AudioFlags::TIMESTAMPED);
        assert!(flags.is_halt() && flags.is_lossless() && flags.is_timestamped());
        assert!(!flags.is_resent());
        assert!(flags.with_resent().is_resent());
    }
}
