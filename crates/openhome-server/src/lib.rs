//! Async OpenHome Media renderer server (`av.openhome.org`).
//!
//! Implement the service traits in [`service`] to expose any audio engine as a
//! compliant OpenHome renderer over SSDP, HTTP/1.1 SOAP, and GENA eventing.

pub mod service;

pub use service::{
    InfoService, PlaylistService, ProductService, ReceiverService, SenderService, TimeService,
    VolumeService,
};
