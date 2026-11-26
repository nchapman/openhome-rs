//! Protocol-agnostic pure-Rust audio decoding, DSP, and hardware output primitives.
//!
//! Designed to be shared across renderers (`openhome-endpoint`, UPnP AV DMRs, etc.)
//! without pulling in any networking or media-control protocol dependencies:
//! - [`decoder`]: HTTP(S)/local audio streaming & decoding via Symphonia (FLAC, ALAC, MP3, AAC, Vorbis, WAV, AIFF, Ogg)
//! - [`mod@format`]: [`PcmSpec`], [`PcmFrame`], dB volume + [`GainRamp`], lock-free [`SpscRingBuffer`], [`PcmAdapter`], big-endian PCM conversion
//! - [`sink`] & [`cpal_sink`]: [`AudioSink`] / [`SinkStream`] traits, [`CpalSink`] (CoreAudio/ALSA/WASAPI), [`NullSink`], and [`CaptureSink`]

pub mod cpal_sink;
pub mod decoder;
pub mod error;
pub mod format;
pub mod sink;

pub use cpal_sink::CpalSink;
pub use decoder::{CodecInfo, DecodedStream, LoadCancel, classify_codec, is_http_uri};
pub use error::AudioError;
pub use format::{
    GainRamp, PcmAdapter, PcmFrame, PcmSpec, SpscRingBuffer, VolumeCharacteristics, VolumeLevel,
    apply_volume_in_place, channel_gains, songcast_pcm_be_into_f32, songcast_pcm_be_to_f32,
    volume_gain, volume_gain_with_chars,
};
pub use sink::{AudioSink, CaptureSink, NullSink, SinkStream};
