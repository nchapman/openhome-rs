//! Error types for `openhome-songcast`.

use thiserror::Error;

/// Errors produced by Songcast wire codecs, URI parsing, and UDP transport.
#[derive(Debug, Error)]
pub enum SongcastError {
    #[error("packet too short: need {expected} bytes, got {actual}")]
    PacketTooShort { expected: usize, actual: usize },

    #[error("invalid packet magic: expected {expected:?}, got {actual:?}")]
    InvalidMagic { expected: [u8; 4], actual: [u8; 4] },

    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    #[error("unknown message type: {0}")]
    UnknownMsgType(u8),

    #[error("invalid packet length: header declares {declared} bytes, buffer has {actual}")]
    InvalidLength { declared: usize, actual: usize },

    #[error("invalid audio sub-header length: {0}")]
    InvalidAudioHeaderLength(u8),

    #[error("packet exceeds maximum UDP frame size ({max} bytes): {actual}")]
    PacketTooLarge { max: usize, actual: usize },

    #[error("invalid UTF-8 in protocol string: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    #[error("invalid Songcast URI: {0}")]
    InvalidUri(String),

    #[error("zone resolution timed out for zone '{0}'")]
    ZoneNotFound(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
