//! Self-contained OpenHome Media + Songcast audio endpoint.
//!
//! Assembles [`openhome_server`], [`openhome_songcast`], and [`openhome_audio`] into a
//! ready-to-run OpenHome renderer (`Product`, `Playlist`, `Info`, `Time`, `Volume`, `Receiver`)
//! with zero custom glue required.

pub mod endpoint;
pub mod engine;
pub mod error;

pub use endpoint::{EndpointConfig, OpenHomeEndpoint};
pub use engine::{EngineCommand, EngineEvent, PlaybackEngine};
pub use error::EndpointError;
pub use openhome_audio::{
    AudioError, AudioSink, CaptureSink, CodecInfo, CpalSink, DecodedStream, LoadCancel, NullSink,
    PcmFrame, PcmSpec, SinkStream,
};
