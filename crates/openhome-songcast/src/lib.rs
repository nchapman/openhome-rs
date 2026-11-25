//! Pure-Rust OpenHome Songcast (`OHM` multicast, `OHU` unicast, `OHZ` zone
//! discovery) wire framing and synchronized multi-room audio transport.

use thiserror::Error;

/// 4-byte magic prefix of every Songcast `OHM`/`OHU` UDP datagram (`"Ohm "`).
pub const OHM_MAGIC: [u8; 4] = *b"Ohm ";
/// Current Songcast protocol major version.
pub const OHM_VERSION: u8 = 1;
/// Fixed length of the Songcast `OHM` packet header in bytes.
pub const OHM_HEADER_LEN: usize = 8;

/// Songcast `OHM`/`OHU` message types (`ohSongcast/OhmMsg.h`).
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

/// 8-byte Songcast `OHM`/`OHU` packet header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OhmHeader {
    pub version: u8,
    pub msg_type: OhmMsgType,
    pub total_bytes: u16,
}

impl OhmHeader {
    pub fn encode(self) -> [u8; OHM_HEADER_LEN] {
        let len = self.total_bytes.to_be_bytes();
        [
            OHM_MAGIC[0],
            OHM_MAGIC[1],
            OHM_MAGIC[2],
            OHM_MAGIC[3],
            self.version,
            self.msg_type as u8,
            len[0],
            len[1],
        ]
    }

    pub fn decode(buf: &[u8]) -> Result<Self, SongcastError> {
        if buf.len() < OHM_HEADER_LEN {
            return Err(SongcastError::TruncatedPacket);
        }
        if buf[0..4] != OHM_MAGIC {
            return Err(SongcastError::InvalidMagic);
        }
        let version = buf[4];
        if version != OHM_VERSION {
            return Err(SongcastError::UnsupportedVersion(version));
        }
        let msg_type = OhmMsgType::try_from(buf[5])?;
        let total_bytes = u16::from_be_bytes([buf[6], buf[7]]);
        if (total_bytes as usize) < OHM_HEADER_LEN {
            return Err(SongcastError::InvalidLength(total_bytes));
        }
        Ok(Self {
            version,
            msg_type,
            total_bytes,
        })
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SongcastError {
    #[error("truncated Songcast packet")]
    TruncatedPacket,
    #[error("invalid Songcast OHM magic")]
    InvalidMagic,
    #[error("unsupported Songcast version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown Songcast message type {0}")]
    UnknownMsgType(u8),
    #[error("invalid Songcast packet length {0}")]
    InvalidLength(u16),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ohm_header_roundtrip() {
        let hdr = OhmHeader {
            version: OHM_VERSION,
            msg_type: OhmMsgType::Audio,
            total_bytes: 1024,
        };
        let raw = hdr.encode();
        assert_eq!(&raw[0..4], b"Ohm ");
        assert_eq!(OhmHeader::decode(&raw).unwrap(), hdr);
    }

    #[test]
    fn rejects_bad_magic_and_short_length() {
        assert_eq!(
            OhmHeader::decode(b"BAD!\x01\x03\x00\x08"),
            Err(SongcastError::InvalidMagic)
        );
        assert_eq!(
            OhmHeader::decode(b"Ohm \x01\x03\x00\x04"),
            Err(SongcastError::InvalidLength(4))
        );
    }
}
