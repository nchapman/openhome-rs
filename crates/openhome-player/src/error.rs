//! Error types for `openhome-player`.

use thiserror::Error;

/// Errors produced by audio decoding, sink output, or player startup.
#[derive(Debug, Error)]
pub enum PlayerError {
    /// Failed to fetch or open an audio source URI.
    #[error("source error: {0}")]
    Source(String),
    /// Failed to probe or decode audio data.
    #[error("decode error: {0}")]
    Decode(String),
    /// Audio output sink error.
    #[error("sink error: {0}")]
    Sink(String),
    /// Underlying OpenHome server error.
    #[error("server error: {0}")]
    Server(#[from] openhome_server::ServerError),
    /// Underlying Songcast error.
    #[error("songcast error: {0}")]
    Songcast(#[from] openhome_songcast::SongcastError),
    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
