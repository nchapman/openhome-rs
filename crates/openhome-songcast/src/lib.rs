//! Pure-Rust OpenHome Songcast (`OHM` / `OHU` / `OHZ`) multi-room audio transport.
//!
//! Wire formats, ports, timers, resend behavior, and tick math are verified against
//! Linn's `ohSongcast` and `ohPipeline`:
//! - [`ohm`]: OHM / OHU (`"Ohm "`, version 1) audio, metadata, slave, and resend framing
//! - [`ohz`]: OHZ (`"Ohz "`, version 1) zone & preset discovery framing
//! - [`uri`]: `ohm://`, `ohu://`, `ohz://` URIs, channel addressing, and `Sender.Metadata` DIDL-Lite
//! - [`clock`]: 44.1 kHz / 48 kHz Songcast tick math and [`ClockSync`] drift estimator
//! - [`reorder`]: [`ReorderBuffer`] for out-of-order frame reassembly and gap detection
//! - [`zone`]: Async [`ZoneResponder`], [`resolve_zone`], and [`query_preset`]
//! - [`sender`]: Async [`SongcastSender`] (multicast + unicast, resend ring buffer)
//! - [`receiver`]: Async [`SongcastReceiver`] (keepalive, resend, clock sync)

pub mod clock;
pub mod error;
pub mod ohm;
pub mod ohz;
pub mod receiver;
pub mod reorder;
pub mod sender;
pub mod uri;
pub mod zone;

pub use clock::{
    ClockSync, DEFAULT_LATENCY_MS, TICKS_PER_SEC_44K, TICKS_PER_SEC_48K, duration_to_ticks,
    latency_ms_to_ticks, songcast_ticks_per_sec, ticks_to_duration,
};
pub use error::SongcastError;
pub use ohm::{
    AudioFlags, OHM_AUDIO_SUBHEADER_BYTES, OHM_HEADER_BYTES, OHM_MAGIC, OHM_MAJOR, OhmAudio,
    OhmHeader, OhmMessage, OhmMetatext, OhmMsgType, OhmTrack,
};
pub use ohz::{OHZ_HEADER_BYTES, OHZ_MAGIC, OHZ_MAJOR, OhzHeader, OhzMessage, OhzMsgType};
pub use receiver::{ReceiverConfig, ReceiverEvent, SongcastReceiver};
pub use reorder::{
    DEFAULT_MAX_BACKLOG_FRAMES, MAX_RESEND_FRAMES_PER_REQ, PushOutcome, ReorderBuffer,
};
pub use sender::{
    AudioFormat, DEFAULT_HISTORY_FRAMES, DEFAULT_RECEIVER_EXPIRY, MAX_SLAVES, SenderConfig,
    SongcastSender,
};
pub use uri::{
    NULL_URI, OHM_PORT, OHZ_MULTICAST_ADDR, OHZ_MULTICAST_IP, OHZ_PORT, SongcastUri,
    encode_sender_metadata, sender_channel_addr, zone_discovery_uri,
};
pub use zone::{ZoneResponder, query_preset, resolve_zone};
