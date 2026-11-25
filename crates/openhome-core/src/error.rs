//! OpenHome error and UPnP/OpenHome SOAP fault definitions.

use thiserror::Error;

/// Standard UPnP and OpenHome SOAP fault codes.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("SOAP fault {code}: {description}")]
pub struct Fault {
    pub code: u16,
    pub description: String,
}

impl Fault {
    pub const INVALID_ACTION: u16 = 401;
    pub const INVALID_ARGS: u16 = 402;
    pub const CONFLICT: u16 = 409;
    pub const ACTION_FAILED: u16 = 501;
    /// OpenHome Playlist: unknown or out-of-range track ID / index.
    pub const INVALID_ID: u16 = 800;
    /// OpenHome Playlist: playlist is at `TracksMax` capacity.
    pub const PLAYLIST_FULL: u16 = 801;

    pub fn new(code: u16, description: impl Into<String>) -> Self {
        Self {
            code,
            description: description.into(),
        }
    }

    pub fn invalid_action(action: &str) -> Self {
        Self::new(Self::INVALID_ACTION, format!("Invalid Action: {action}"))
    }

    pub fn invalid_args(msg: impl Into<String>) -> Self {
        Self::new(Self::INVALID_ARGS, msg)
    }

    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::new(Self::CONFLICT, msg)
    }

    pub fn invalid_id() -> Self {
        Self::new(Self::INVALID_ID, "Invalid Id")
    }

    pub fn playlist_full() -> Self {
        Self::new(Self::PLAYLIST_FULL, "Playlist Full")
    }

    pub fn action_failed(msg: impl Into<String>) -> Self {
        Self::new(Self::ACTION_FAILED, msg)
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("XML error: {0}")]
    Xml(String),
    #[error("invalid base64 IdArray: {0}")]
    InvalidIdArray(String),
    #[error("invalid value for {field}: {value}")]
    InvalidValue { field: &'static str, value: String },
    #[error(transparent)]
    Fault(#[from] Fault),
}
