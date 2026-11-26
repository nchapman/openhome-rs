//! Async Songcast OHM/OHU receiver (`SongcastReceiver`).
//!
//! Connects to an `ohm://`, `ohu://`, or `ohz://` URI, manages `Join`/`Listen`/`Leave`
//! keepalives, requests missing frames via `OhmMessage::Resend` using [`ReorderBuffer`],
//! estimates synchronized playout times via [`ClockSync`], and streams [`ReceiverEvent`]s
//! to an audio playback engine.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::clock::ClockSync;
use crate::error::SongcastError;
use crate::ohm::{OhmAudio, OhmMessage, OhmMetatext, OhmTrack};
use crate::reorder::{
    DEFAULT_MAX_BACKLOG_FRAMES, MAX_RESEND_FRAMES_PER_REQ, PushOutcome, ReorderBuffer,
};
use crate::sender::MAX_SLAVES;
use crate::uri::SongcastUri;
use crate::zone::{bind_udp_v4, resolve_zone_on};

const RECEIVER_EVENT_CHANNEL_CAPACITY: usize = 256;

/// Default `Join` retry interval (`ohSongcast`: `kTimerJoinTimeoutMs = 300ms`).
pub const DEFAULT_JOIN_INTERVAL: Duration = Duration::from_millis(300);

/// Default `Listen` keepalive interval (`ohSongcast`: primary `~1.25–2.5s`).
pub const DEFAULT_LISTEN_INTERVAL: Duration = Duration::from_millis(2_000);

/// Default `Resend` poll interval when gaps exist (`ohSongcast`: `kSubsequentRepairTimeoutMs = 30ms`).
pub const DEFAULT_REPAIR_INTERVAL: Duration = Duration::from_millis(30);

/// Configuration for [`SongcastReceiver`].
#[derive(Debug, Clone)]
pub struct ReceiverConfig {
    /// Local IPv4 interface for multicast membership (default `0.0.0.0`).
    pub interface: Ipv4Addr,
    /// Outbound TTL for `Join`/`Listen`/`Resend` (default `2`, matching `ohPipeline`).
    pub ttl: u32,
    /// Timeout when resolving `ohz://` zone URIs (default `2s`).
    pub zone_resolve_timeout: Duration,
    /// `Join` retry interval before Track+Metatext are received (default `300ms`).
    pub join_interval: Duration,
    /// `Listen` keepalive interval once joined (default `2s`).
    pub listen_interval: Duration,
    /// Interval between `OhmMessage::Resend` retries while repairing a gap (default `30ms`).
    pub repair_interval: Duration,
    /// Maximum out-of-order audio frames buffered before resyncing (default `200`).
    pub max_backlog_frames: usize,
}

impl Default for ReceiverConfig {
    fn default() -> Self {
        Self {
            interface: Ipv4Addr::UNSPECIFIED,
            ttl: 2,
            zone_resolve_timeout: Duration::from_secs(2),
            join_interval: DEFAULT_JOIN_INTERVAL,
            listen_interval: DEFAULT_LISTEN_INTERVAL,
            repair_interval: DEFAULT_REPAIR_INTERVAL,
            max_backlog_frames: DEFAULT_MAX_BACKLOG_FRAMES,
        }
    }
}

/// Events emitted by [`SongcastReceiver`].
#[derive(Debug, Clone)]
pub enum ReceiverEvent {
    /// Contiguous, ordered audio frame with estimated local playout instant and clock skew.
    Audio {
        frame: OhmAudio,
        playout_at: Instant,
        skew_ppm: f64,
    },
    /// Track URI + DIDL-Lite metadata update.
    Track(OhmTrack),
    /// Live metatext update.
    Metatext(OhmMetatext),
    /// Stream discontinuity / backlog overflow resynced to a new frame sequence.
    Resynced,
}

/// Async Songcast receiver.
pub struct SongcastReceiver {
    resolved_uri: SongcastUri,
    events_rx: mpsc::Receiver<ReceiverEvent>,
    shutdown_tx: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for SongcastReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SongcastReceiver")
            .field("resolved_uri", &self.resolved_uri)
            .finish()
    }
}

impl Drop for SongcastReceiver {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl SongcastReceiver {
    /// Connect to a Songcast URI (`ohm://`, `ohu://`, or `ohz://`).
    ///
    /// If `uri` is `SongcastUri::Zone`, resolves it via OHZ before opening the OHM/OHU socket.
    pub async fn connect(uri: &SongcastUri, config: ReceiverConfig) -> Result<Self, SongcastError> {
        let resolved_uri = match uri {
            SongcastUri::Zone { endpoint, zone } => {
                resolve_zone_on(
                    *endpoint,
                    zone,
                    config.interface,
                    config.zone_resolve_timeout,
                )
                .await?
            }
            other => other.clone(),
        };

        let (remote_endpoint, is_multicast, bind_addr) = match resolved_uri {
            SongcastUri::Multicast(mc) => (mc, true, mc),
            SongcastUri::Unicast(uc) => (uc, false, SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            SongcastUri::Null | SongcastUri::Zone { .. } => {
                return Err(SongcastError::InvalidUri(resolved_uri.to_string()));
            }
        };

        let socket = bind_udp_v4(bind_addr, config.interface, config.ttl)?;
        let (events_tx, events_rx) = mpsc::channel(RECEIVER_EVENT_CHANNEL_CAPACITY);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let task = tokio::spawn(async move {
            let join_bytes = OhmMessage::Join.encode().unwrap();
            let listen_bytes = OhmMessage::Listen.encode().unwrap();
            let leave_bytes = OhmMessage::Leave.encode().unwrap();

            let dest = SocketAddr::V4(remote_endpoint);
            let _ = socket.send_to(&join_bytes, dest).await;

            let mut joined_track = false;
            let mut joined_metatext = false;
            let mut last_track_seq: Option<u32> = None;
            let mut last_meta_seq: Option<u32> = None;
            let mut reorder = ReorderBuffer::new(config.max_backlog_frames);
            let mut clock = ClockSync::new(48_000);
            let mut current_rate: u32 = 48_000;
            // Unicast primary relays Audio/Track/Metatext to slaves (ohSongcast Broadcast()).
            let mut slaves: Vec<SocketAddrV4> = Vec::new();

            let mut join_tick = tokio::time::interval(config.join_interval);
            join_tick.reset();
            let mut listen_tick = tokio::time::interval(config.listen_interval);
            listen_tick.reset();
            let mut repair_tick = tokio::time::interval(config.repair_interval);
            repair_tick.reset();

            let mut buf = [0u8; 16 * 1024];

            loop {
                let fully_joined = joined_track && joined_metatext;
                tokio::select! {
                    res = shutdown_rx.changed() => {
                        if res.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = join_tick.tick(), if !fully_joined => {
                        let _ = socket.send_to(&join_bytes, dest).await;
                    }
                    _ = listen_tick.tick(), if fully_joined => {
                        let _ = socket.send_to(&listen_bytes, dest).await;
                    }
                    _ = repair_tick.tick(), if reorder.has_gaps() => {
                        let missing = reorder.missing_frames(MAX_RESEND_FRAMES_PER_REQ);
                        if !missing.is_empty()
                            && let Ok(bytes) = OhmMessage::Resend(missing).encode()
                        {
                            let _ = socket.send_to(&bytes, dest).await;
                        }
                    }
                    recv = socket.recv_from(&mut buf) => {
                        let Ok((len, peer)) = recv else { continue };
                        let arrival = Instant::now();
                        let Ok(msg) = OhmMessage::decode(&buf[..len]) else { continue };

                        match msg {
                            OhmMessage::Slave(mut list) if !is_multicast && peer == dest => {
                                // Only accept Slave relay lists from the unicast sender itself,
                                // bounded to MAX_SLAVES to prevent UDP reflection/amplification.
                                list.truncate(MAX_SLAVES);
                                slaves = list;
                            }
                            OhmMessage::Track(track) => {
                                if !is_multicast && !slaves.is_empty() {
                                    for &slave in &slaves {
                                        let _ = socket.send_to(&buf[..len], SocketAddr::V4(slave)).await;
                                    }
                                }
                                joined_track = true;
                                if last_track_seq != Some(track.sequence) {
                                    last_track_seq = Some(track.sequence);
                                    let _ = events_tx.try_send(ReceiverEvent::Track(track));
                                }
                            }
                            OhmMessage::Metatext(mt) => {
                                if !is_multicast && !slaves.is_empty() {
                                    for &slave in &slaves {
                                        let _ = socket.send_to(&buf[..len], SocketAddr::V4(slave)).await;
                                    }
                                }
                                joined_metatext = true;
                                if last_meta_seq != Some(mt.sequence) {
                                    last_meta_seq = Some(mt.sequence);
                                    let _ = events_tx.try_send(ReceiverEvent::Metatext(mt));
                                }
                            }
                            OhmMessage::Audio(audio) => {
                                if !is_multicast && !slaves.is_empty() {
                                    for &slave in &slaves {
                                        let _ = socket.send_to(&buf[..len], SocketAddr::V4(slave)).await;
                                    }
                                }
                                match reorder.push(audio) {
                                    PushOutcome::Ready(frames) => {
                                        let freshly_in_order = frames.len() == 1;
                                        for frame in frames {
                                            emit_audio(
                                                frame,
                                                arrival,
                                                freshly_in_order,
                                                &mut current_rate,
                                                &mut clock,
                                                &events_tx,
                                            );
                                        }
                                    }
                                    PushOutcome::GapDetected => {
                                        // Trigger immediate resend on first gap detection.
                                        let missing = reorder.missing_frames(MAX_RESEND_FRAMES_PER_REQ);
                                        if !missing.is_empty()
                                            && let Ok(bytes) = OhmMessage::Resend(missing).encode()
                                        {
                                            let _ = socket.send_to(&bytes, dest).await;
                                        }
                                    }
                                    PushOutcome::Duplicate => {}
                                    PushOutcome::Resynced(frame) => {
                                        clock.reset(frame.sample_rate);
                                        current_rate = frame.sample_rate;
                                        let _ = events_tx.try_send(ReceiverEvent::Resynced);
                                        emit_audio(
                                            frame,
                                            arrival,
                                            true,
                                            &mut current_rate,
                                            &mut clock,
                                            &events_tx,
                                        );
                                    }
                                }
                            }
                            OhmMessage::Listen if is_multicast => {
                                // Master/slave suppression: another multicast receiver just sent Listen.
                                listen_tick.reset();
                            }
                            OhmMessage::Resend(_) if is_multicast && reorder.has_gaps() => {
                                // ResendSeen suppression: defer our own resend timer when a peer requests resend.
                                repair_tick.reset();
                            }
                            _ => {}
                        }
                    }
                }
            }

            if !is_multicast {
                let _ = socket.send_to(&leave_bytes, dest).await;
            }
        });

        Ok(Self {
            resolved_uri,
            events_rx,
            shutdown_tx,
            task: Some(task),
        })
    }

    /// Return the resolved stream URI (`ohm://...` or `ohu://...`).
    pub fn resolved_uri(&self) -> &SongcastUri {
        &self.resolved_uri
    }

    /// Receive the next [`ReceiverEvent`] (returns `None` after shutdown).
    pub async fn recv(&mut self) -> Option<ReceiverEvent> {
        self.events_rx.recv().await
    }

    /// Non-blocking poll for the next queued [`ReceiverEvent`].
    pub fn try_recv(&mut self) -> Result<ReceiverEvent, mpsc::error::TryRecvError> {
        self.events_rx.try_recv()
    }

    /// Stop the receiver and (in unicast mode) send `OhmMessage::Leave`.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

fn emit_audio(
    frame: OhmAudio,
    arrival: Instant,
    freshly_in_order: bool,
    current_rate: &mut u32,
    clock: &mut ClockSync,
    events_tx: &mpsc::Sender<ReceiverEvent>,
) {
    if frame.sample_rate != *current_rate && frame.sample_rate > 0 {
        clock.reset(frame.sample_rate);
        *current_rate = frame.sample_rate;
    }
    let is_halt = frame.flags.is_halt();
    // Only feed freshly-arrived, in-order, non-resent frames into ClockSync so
    // late resends and drained reorder backlogs do not corrupt offset/skew.
    let playout_at = if freshly_in_order && !frame.flags.is_resent() && frame.flags.is_timestamped()
    {
        clock.observe(frame.network_timestamp, frame.media_latency, arrival)
    } else {
        arrival
    };
    let skew_ppm = clock.skew_ppm();
    let _ = events_tx.try_send(ReceiverEvent::Audio {
        frame,
        playout_at,
        skew_ppm,
    });
    // Reset ClockSync after a HALT frame so long pauses (>174s) cannot wrap timestamps negative.
    if is_halt {
        clock.reset(*current_rate);
    }
}
