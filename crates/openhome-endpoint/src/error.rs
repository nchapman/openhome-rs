//! Error types for `openhome-endpoint`.

use thiserror::Error;

/// Errors produced by `openhome-endpoint` startup, protocol services, or audio playback.
#[derive(Debug, Error)]
pub enum EndpointError {
    /// Underlying audio decoding or output sink error.
    #[error(transparent)]
    Audio(#[from] openhome_audio::AudioError),
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
