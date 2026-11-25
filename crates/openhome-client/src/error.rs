//! Error types for `openhome-client`.

use openhome_core::{Error as CoreError, Fault};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Fault(#[from] Fault),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("unexpected HTTP status {0}: {1}")]
    HttpStatus(u16, String),
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
    #[error("device does not expose service {0}")]
    MissingService(&'static str),
    #[error("SOAP response missing argument {0}")]
    MissingArgument(&'static str),
    #[error("invalid response value for {field}: {value}")]
    InvalidResponse { field: &'static str, value: String },
    #[error("GENA subscription failed: {0}")]
    SubscriptionFailed(String),
}
