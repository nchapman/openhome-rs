//! Async OpenHome Media control point client (`av.openhome.org`).
//!
//! Will provide SSDP discovery, device XML parsing, typed clients for
//! `Product`, `Playlist`, `Info`, `Time`, `Volume`, `Receiver`, and `Sender`,
//! and GENA event subscriptions.

use openhome_core::Error as CoreError;
use thiserror::Error;

/// Discovered OpenHome device summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredDevice {
    pub udn: String,
    pub friendly_name: String,
    pub location: String,
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error: {0}")]
    Http(String),
}
