//! Server error types for `openhome-server`.

use openhome_core::Error as CoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("configuration error: {0}")]
    Config(String),
}
