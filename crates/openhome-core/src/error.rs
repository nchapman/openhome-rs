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
    /// OpenHome Product: source not found.
    pub const SOURCE_NOT_FOUND: u16 = 801;
    /// OpenHome Playlist: index out of range.
    pub const INVALID_INDEX: u16 = 802;
    /// OpenHome Playlist: seek failed.
    pub const SEEK_FAILED: u16 = 803;

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
        Self::new(Self::INVALID_ID, "Id not found")
    }

    pub fn playlist_full() -> Self {
        Self::new(Self::PLAYLIST_FULL, "Playlist full")
    }

    pub fn source_not_found() -> Self {
        Self::new(Self::SOURCE_NOT_FOUND, "Source not found")
    }

    pub fn invalid_index() -> Self {
        Self::new(Self::INVALID_INDEX, "Index not found")
    }

    pub fn seek_failed() -> Self {
        Self::new(Self::SEEK_FAILED, "Seek failed")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_constructors_expose_spec_codes() {
        assert_eq!(Fault::invalid_action("x").code, Fault::INVALID_ACTION);
        assert_eq!(Fault::invalid_args("x").code, Fault::INVALID_ARGS);
        assert_eq!(Fault::conflict("x").code, Fault::CONFLICT);
        assert_eq!(Fault::invalid_id().code, Fault::INVALID_ID);
        assert_eq!(Fault::playlist_full().code, Fault::PLAYLIST_FULL);
        assert_eq!(Fault::source_not_found().code, Fault::SOURCE_NOT_FOUND);
        assert_eq!(Fault::invalid_index().code, Fault::INVALID_INDEX);
        assert_eq!(Fault::seek_failed().code, Fault::SEEK_FAILED);
        assert_eq!(Fault::action_failed("x").code, Fault::ACTION_FAILED);
        assert!(Fault::invalid_id().to_string().contains("800"));
    }
}
