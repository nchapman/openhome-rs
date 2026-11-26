//! Self-contained OpenHome Media + Songcast audio player.
//!
//! `openhome-player` pairs [`openhome_server::OpenHomeServer`] and
//! [`openhome_songcast::SongcastReceiver`] with a pure-Rust Symphonia decoder and pluggable
//! [`AudioSink`] backends ([`NullSink`], [`CaptureSink`], and optional `cpal` output via the
//! `cpal-output` feature).

pub mod decoder;
pub mod engine;
pub mod error;
pub mod format;
pub mod player;
pub mod sink;

pub use decoder::DecodedStream;
pub use engine::{EngineCommand, EngineEvent, PlaybackEngine};
pub use error::PlayerError;
pub use format::{PcmFrame, PcmSpec, apply_volume_in_place, songcast_pcm_be_to_f32, volume_gain};
pub use player::{OpenHomePlayer, PlayerConfig};
pub use sink::{AudioSink, CaptureSink, NullSink, SinkStream};

#[cfg(feature = "cpal-output")]
pub use sink::CpalSink;
