//! Core protocol types, XML/SOAP/GENA codecs, and constants for OpenHome Media
//! (`av.openhome.org`).

pub mod error;
pub mod gena;
pub mod id_array;
pub mod scpd;
pub mod soap;
pub mod ssdp;
pub mod types;
pub mod urn;
pub mod xml;

pub use error::{Error, Fault};
pub use types::{
    DeviceAttributes, DeviceDescription, InfoCounters, ManufacturerDetails, ModelDetails,
    ProductDetails, ProductInfo, ReceiverState, SenderStatus, ServiceDescription, Source,
    SourceType, TrackDetails, TrackEntry, TrackTime, TransportState, VolumeCharacteristics,
    VolumeState, format_upnp_bool, parse_upnp_bool,
};
