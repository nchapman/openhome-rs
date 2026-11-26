//! Error types for `openhome-audio`.

use thiserror::Error;

/// Errors produced by audio fetching, decoding, or output sink operations.
#[derive(Debug, Error)]
pub enum AudioError {
    /// Failed to fetch or open an audio source URI.
    #[error("source error: {0}")]
    Source(String),
    /// Failed to probe or decode audio data.
    #[error("decode error: {0}")]
    Decode(String),
    /// Audio output sink error.
    #[error("sink error: {0}")]
    Sink(String),
    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
