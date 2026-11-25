//! Async OpenHome Media renderer server (`av.openhome.org`).
//!
//! Implement the service traits in [`service`] (or start from [`MemoryRenderer`])
//! to expose any audio engine as a compliant OpenHome renderer over SSDP,
//! HTTP/1.1 SOAP, and GENA eventing.

pub mod dispatch;
pub mod error;
pub mod event;
mod http;
pub mod memory;
pub mod server;
pub mod service;
pub mod ssdp;

pub use error::ServerError;
pub use event::{EventHub, SubscribeError};
pub use memory::MemoryRenderer;
pub use server::{OpenHomeServer, OpenHomeServerBuilder};
pub use service::{
    ActionResult, InfoService, PlaylistService, ProductService, ReceiverService, SenderService,
    TimeService, VolumeService,
};
pub use ssdp::SsdpConfig;
