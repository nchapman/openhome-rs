//! Async OpenHome Media control point client (`av.openhome.org`).
//!
//! Provides SSDP discovery, UPnP device XML parsing, typed clients for
//! `Product`, `Playlist`, `Info`, `Time`, `Volume`, `Receiver`, and `Sender`,
//! a GENA event subscriber with automatic renewal, and a room/source topology model.

pub mod device;
pub mod discovery;
pub mod error;
pub mod gena;
pub mod http;
pub mod services;
pub mod topology;

pub use device::OpenHomeDevice;
pub use discovery::{DiscoveredDevice, DiscoveryOptions, discover};
pub use error::ClientError;
pub use gena::{EventListener, EventSubscription, GenaEvent};
pub use http::HttpClient;
pub use services::{
    InfoClient, PlaylistClient, ProductClient, ReceiverClient, SenderClient, TimeClient,
    VolumeClient,
};
pub use topology::{RoomTopology, Topology, TopologyDevice};
