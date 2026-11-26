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

pub use decoder::{CodecInfo, DecodedStream, LoadCancel, classify_codec, is_http_uri};
pub use engine::{EngineCommand, EngineEvent, PlaybackEngine};
pub use error::PlayerError;
pub use format::{
    GainRamp, PcmAdapter, PcmFrame, PcmSpec, SpscRingBuffer, apply_volume_in_place, channel_gains,
    songcast_pcm_be_into_f32, songcast_pcm_be_to_f32, volume_gain, volume_gain_with_chars,
};
pub use player::{OpenHomePlayer, PlayerConfig};
pub use sink::{AudioSink, CaptureSink, NullSink, SinkStream};

#[cfg(feature = "cpal-output")]
pub use sink::CpalSink;
