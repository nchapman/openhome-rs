//! Core protocol types, XML/SOAP/GENA codecs, and constants for OpenHome Media
//! (`av.openhome.org`).

pub mod error;
pub mod gena;
pub mod id_array;
pub mod soap;
pub mod types;
pub mod urn;
pub mod xml;

pub use error::{Error, Fault};
pub use types::{
    DeviceAttributes, InfoCounters, ProductInfo, ReceiverState, SenderStatus, Source, SourceType,
    TrackDetails, TrackEntry, TrackTime, TransportState, VolumeCharacteristics, VolumeState,
};
