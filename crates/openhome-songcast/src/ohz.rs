//! Songcast OHZ zone & preset discovery wire protocol (`"Ohz "` magic, major version 1).
//!
//! Verified byte-for-byte against `ohSongcast/Ohm.h` (`OhzHeader*`).

use crate::error::SongcastError;

/// 4-byte OHZ packet magic (`"Ohz "`).
pub const OHZ_MAGIC: [u8; 4] = *b"Ohz ";

/// OHZ protocol major version (`1`).
pub const OHZ_MAJOR: u8 = 1;

/// Fixed 8-byte OHZ outer header length.
pub const OHZ_HEADER_BYTES: usize = 8;

/// OHZ message type identifiers (`OhzHeader::kMsgType*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OhzMsgType {
    ZoneQuery = 0,
    ZoneUri = 1,
    PresetQuery = 2,
    PresetInfo = 3,
}

impl TryFrom<u8> for OhzMsgType {
    type Error = SongcastError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::ZoneQuery),
            1 => Ok(Self::ZoneUri),
            2 => Ok(Self::PresetQuery),
            3 => Ok(Self::PresetInfo),
            other => Err(SongcastError::UnknownMsgType(other)),
        }
    }
}

/// 8-byte OHZ packet header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OhzHeader {
    pub major: u8,
    pub msg_type: OhzMsgType,
    pub total_bytes: u16,
}

impl OhzHeader {
    pub fn new(msg_type: OhzMsgType, payload_bytes: usize) -> Result<Self, SongcastError> {
        let total =
            OHZ_HEADER_BYTES
                .checked_add(payload_bytes)
                .ok_or(SongcastError::PacketTooLarge {
                    max: u16::MAX as usize,
                    actual: usize::MAX,
                })?;
        if total > u16::MAX as usize {
            return Err(SongcastError::PacketTooLarge {
                max: u16::MAX as usize,
                actual: total,
            });
        }
        Ok(Self {
            major: OHZ_MAJOR,
            msg_type,
            total_bytes: total as u16,
        })
    }

    pub fn encode(&self) -> [u8; OHZ_HEADER_BYTES] {
        let mut buf = [0u8; OHZ_HEADER_BYTES];
        buf[0..4].copy_from_slice(&OHZ_MAGIC);
        buf[4] = self.major;
        buf[5] = self.msg_type as u8;
        buf[6..8].copy_from_slice(&self.total_bytes.to_be_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SongcastError> {
        if bytes.len() < OHZ_HEADER_BYTES {
            return Err(SongcastError::PacketTooShort {
                expected: OHZ_HEADER_BYTES,
                actual: bytes.len(),
            });
        }
        let magic: [u8; 4] = bytes[0..4].try_into().unwrap();
        if magic != OHZ_MAGIC {
            return Err(SongcastError::InvalidMagic {
                expected: OHZ_MAGIC,
                actual: magic,
            });
        }
        let major = bytes[4];
        if major != OHZ_MAJOR {
            return Err(SongcastError::UnsupportedVersion(major));
        }
        let msg_type = OhzMsgType::try_from(bytes[5])?;
        let total_bytes = u16::from_be_bytes([bytes[6], bytes[7]]);
        if (total_bytes as usize) < OHZ_HEADER_BYTES || (total_bytes as usize) > bytes.len() {
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

/// OHZ zone / preset discovery messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OhzMessage {
    /// Query the current `ohm://` or `ohu://` stream URI for `zone`.
    ZoneQuery { zone: String },
    /// Reply carrying the current stream `uri` for `zone`.
    ZoneUri { zone: String, uri: String },
    /// Query metadata for preset index `preset`.
    PresetQuery { preset: u32 },
    /// Reply carrying DIDL-Lite `metadata` for `preset`.
    PresetInfo { preset: u32, metadata: String },
}

impl OhzMessage {
    pub fn encode(&self) -> Result<Vec<u8>, SongcastError> {
        match self {
            Self::ZoneQuery { zone } => {
                let zb = zone.as_bytes();
                let hdr = OhzHeader::new(OhzMsgType::ZoneQuery, 4 + zb.len())?;
                let mut out = Vec::with_capacity(hdr.total_bytes as usize);
                out.extend_from_slice(&hdr.encode());
                out.extend_from_slice(&(zb.len() as u32).to_be_bytes());
                out.extend_from_slice(zb);
                Ok(out)
            }
            Self::ZoneUri { zone, uri } => {
                let zb = zone.as_bytes();
                let ub = uri.as_bytes();
                let hdr = OhzHeader::new(OhzMsgType::ZoneUri, 8 + zb.len() + ub.len())?;
                let mut out = Vec::with_capacity(hdr.total_bytes as usize);
                out.extend_from_slice(&hdr.encode());
                out.extend_from_slice(&(zb.len() as u32).to_be_bytes());
                out.extend_from_slice(&(ub.len() as u32).to_be_bytes());
                out.extend_from_slice(zb);
                out.extend_from_slice(ub);
                Ok(out)
            }
            Self::PresetQuery { preset } => {
                let hdr = OhzHeader::new(OhzMsgType::PresetQuery, 4)?;
                let mut out = Vec::with_capacity(hdr.total_bytes as usize);
                out.extend_from_slice(&hdr.encode());
                out.extend_from_slice(&preset.to_be_bytes());
                Ok(out)
            }
            Self::PresetInfo { preset, metadata } => {
                let mb = metadata.as_bytes();
                let hdr = OhzHeader::new(OhzMsgType::PresetInfo, 8 + mb.len())?;
                let mut out = Vec::with_capacity(hdr.total_bytes as usize);
                out.extend_from_slice(&hdr.encode());
                out.extend_from_slice(&preset.to_be_bytes());
                out.extend_from_slice(&(mb.len() as u32).to_be_bytes());
                out.extend_from_slice(mb);
                Ok(out)
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SongcastError> {
        let hdr = OhzHeader::decode(bytes)?;
        let payload = &bytes[OHZ_HEADER_BYTES..hdr.total_bytes as usize];
        match hdr.msg_type {
            OhzMsgType::ZoneQuery => {
                if payload.len() < 4 {
                    return Err(SongcastError::PacketTooShort {
                        expected: 4,
                        actual: payload.len(),
                    });
                }
                let zone_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                let expected =
                    4usize
                        .checked_add(zone_len)
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
                let zone = std::str::from_utf8(&payload[4..expected])?.to_string();
                Ok(Self::ZoneQuery { zone })
            }
            OhzMsgType::ZoneUri => {
                if payload.len() < 8 {
                    return Err(SongcastError::PacketTooShort {
                        expected: 8,
                        actual: payload.len(),
                    });
                }
                let zone_len = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
                let uri_len = u32::from_be_bytes(payload[4..8].try_into().unwrap()) as usize;
                let expected = 8usize
                    .checked_add(zone_len)
                    .and_then(|v| v.checked_add(uri_len))
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
                let zone = std::str::from_utf8(&payload[8..8 + zone_len])?.to_string();
                let uri = std::str::from_utf8(&payload[8 + zone_len..expected])?.to_string();
                Ok(Self::ZoneUri { zone, uri })
            }
            OhzMsgType::PresetQuery => {
                if payload.len() < 4 {
                    return Err(SongcastError::PacketTooShort {
                        expected: 4,
                        actual: payload.len(),
                    });
                }
                let preset = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                Ok(Self::PresetQuery { preset })
            }
            OhzMsgType::PresetInfo => {
                if payload.len() < 8 {
                    return Err(SongcastError::PacketTooShort {
                        expected: 8,
                        actual: payload.len(),
                    });
                }
                let preset = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                let meta_len = u32::from_be_bytes(payload[4..8].try_into().unwrap()) as usize;
                let expected =
                    8usize
                        .checked_add(meta_len)
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
                let metadata = std::str::from_utf8(&payload[8..expected])?.to_string();
                Ok(Self::PresetInfo { preset, metadata })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ohz_messages_roundtrip() {
        for msg in [
            OhzMessage::ZoneQuery {
                zone: "uuid:living-room".into(),
            },
            OhzMessage::ZoneUri {
                zone: "uuid:living-room".into(),
                uri: "ohm://239.253.0.1:51970".into(),
            },
            OhzMessage::PresetQuery { preset: 3 },
            OhzMessage::PresetInfo {
                preset: 3,
                metadata: "<DIDL-Lite/>".into(),
            },
        ] {
            let bytes = msg.encode().unwrap();
            assert_eq!(&bytes[0..4], b"Ohz ");
            assert_eq!(bytes[4], 1);
            assert_eq!(OhzMessage::decode(&bytes).unwrap(), msg);
        }
    }

    #[test]
    fn ohz_rejects_malformed_and_truncated() {
        assert!(matches!(
            OhzHeader::decode(&[0; 3]),
            Err(SongcastError::PacketTooShort { .. })
        ));
        assert!(matches!(
            OhzHeader::decode(b"Ohm \x01\x00\x00\x08"),
            Err(SongcastError::InvalidMagic { .. })
        ));
        assert!(matches!(
            OhzHeader::decode(b"Ohz \x09\x00\x00\x08"),
            Err(SongcastError::UnsupportedVersion(9))
        ));
        assert!(matches!(
            OhzHeader::decode(b"Ohz \x01\x09\x00\x08"),
            Err(SongcastError::UnknownMsgType(9))
        ));
        assert!(matches!(
            OhzHeader::decode(b"Ohz \x01\x00\x00\x40"),
            Err(SongcastError::InvalidLength { .. })
        ));
        for msg_type in [
            OhzMsgType::ZoneQuery,
            OhzMsgType::ZoneUri,
            OhzMsgType::PresetQuery,
            OhzMsgType::PresetInfo,
        ] {
            let hdr = OhzHeader::new(msg_type, 0).unwrap().encode();
            assert!(matches!(
                OhzMessage::decode(&hdr),
                Err(SongcastError::PacketTooShort { .. })
            ));
        }
    }
}
