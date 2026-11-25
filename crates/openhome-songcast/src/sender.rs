//! Async Songcast OHM/OHU sender (`SongcastSender`).
//!
//! Audio-engine agnostic: callers push PCM frames via [`SongcastSender::send_audio`] and
//! metadata via [`SongcastSender::set_track`] / [`SongcastSender::set_metatext`].
//! Handles receiver `Join`/`Listen`/`Leave` liveness, unicast slave lists (max 4),
//! automatic `Track`+`Metatext` resend on `Join`, and a bounded resend history ring buffer
//! (`DEFAULT_HISTORY_FRAMES = 100`).

use std::collections::{HashMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::clock::{DEFAULT_LATENCY_MS, duration_to_ticks, latency_ms_to_ticks};
use crate::error::SongcastError;
use crate::ohm::{AudioFlags, OhmAudio, OhmMessage, OhmMetatext, OhmTrack};
use crate::uri::SongcastUri;
use crate::zone::bind_udp_v4;

/// Default resend history ring buffer depth (`OhmSenderDriver::kMaxHistoryFrames`).
pub const DEFAULT_HISTORY_FRAMES: usize = 100;

/// Default receiver liveness expiry (`OhmSender::kTimerExpiryTimeoutMs = 10s`).
pub const DEFAULT_RECEIVER_EXPIRY: Duration = Duration::from_secs(10);

/// Maximum unicast slave count (`OhmReceiver::kMaxSlaveCount = 4`).
pub const MAX_SLAVES: usize = 4;

/// Audio stream format parameters for [`SongcastSender::send_audio`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioFormat {
    pub sample_rate: u32,
    pub bit_depth: u8,
    pub channels: u8,
    pub bit_rate: u32,
    pub lossless: bool,
    pub codec: String,
    pub volume_offset: i16,
    pub samples_total: u64,
}

impl AudioFormat {
    /// Convenience constructor for standard lossless PCM (e.g. 48 kHz / 16-bit / 2ch).
    pub fn pcm(sample_rate: u32, bit_depth: u8, channels: u8) -> Self {
        Self {
            sample_rate,
            bit_depth,
            channels,
            bit_rate: sample_rate * u32::from(bit_depth) * u32::from(channels),
            lossless: true,
            codec: "PCM".to_string(),
            volume_offset: 0,
            samples_total: 0,
        }
    }
}

/// Configuration for [`SongcastSender`].
#[derive(Debug, Clone)]
pub struct SenderConfig {
    /// Local UDP bind address (pass `127.0.0.1:0` for ephemeral unicast, or a multicast
    /// endpoint like `sender_channel_addr(channel)` for OHM multicast).
    pub bind_addr: SocketAddrV4,
    /// Outbound multicast interface IPv4 (used when `bind_addr.ip()` is multicast).
    pub interface: Ipv4Addr,
    /// Multicast TTL (default `1`, matching `ohPipeline` `OhmSender::kTtl`).
    pub ttl: u32,
    /// Sender pipeline latency advertised in `OhmAudio::media_latency` (default `100` ms).
    pub latency_ms: u32,
    /// Maximum audio frames retained for `OhmMessage::Resend` replies (default `100`).
    pub history_frames: usize,
    /// Receiver `Join`/`Listen` liveness timeout (default `10s`).
    pub receiver_expiry: Duration,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0),
            interface: Ipv4Addr::UNSPECIFIED,
            ttl: 1,
            latency_ms: DEFAULT_LATENCY_MS,
            history_frames: DEFAULT_HISTORY_FRAMES,
            receiver_expiry: DEFAULT_RECEIVER_EXPIRY,
        }
    }
}

#[derive(Debug)]
struct SenderState {
    epoch: Instant,
    frame: u32,
    sample_start: u64,
    track: OhmTrack,
    metatext: OhmMetatext,
    /// Active receivers keyed by UDP peer address -> last `Join`/`Listen` time.
    receivers: HashMap<SocketAddrV4, Instant>,
    /// Primary receiver in unicast mode (receives `Slave` list + stream).
    primary: Option<SocketAddrV4>,
    /// Bounded history of pre-encoded wire packets (with `RESENT` flag pre-set) for `OhmMessage::Resend`.
    history: VecDeque<(u32, Vec<u8>)>,
    max_history: usize,
    latency_ms: u32,
    receiver_expiry: Duration,
}

impl SenderState {
    fn prune_expired(&mut self, now: Instant) {
        let expiry = self.receiver_expiry;
        self.receivers
            .retain(|_, last_seen| now.saturating_duration_since(*last_seen) < expiry);
        if let Some(p) = self.primary
            && !self.receivers.contains_key(&p)
        {
            self.primary = self.receivers.keys().next().copied();
        }
    }

    fn slaves(&self) -> Vec<SocketAddrV4> {
        let Some(primary) = self.primary else {
            return Vec::new();
        };
        self.receivers
            .keys()
            .copied()
            .filter(|&a| a != primary)
            .take(MAX_SLAVES)
            .collect()
    }

    fn record_history(&mut self, frame: u32, mut wire: Vec<u8>) {
        // Flip the RESENT flag bit in-place so resends transmit the cached wire bytes directly.
        if let Some(flags_byte) = wire.get_mut(crate::ohm::OHM_HEADER_BYTES + 1) {
            *flags_byte |= AudioFlags::RESENT;
        }
        if self.history.len() >= self.max_history {
            self.history.pop_front();
        }
        self.history.push_back((frame, wire));
    }

    fn lookup_history(&self, frames: &[u32]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for &req in frames {
            if let Some((_, wire)) = self.history.iter().find(|(f, _)| *f == req) {
                out.push(wire.clone());
            }
        }
        out
    }
}

/// Async Songcast sender transmitting OHM/OHU audio, metadata, and resend replies over UDP.
pub struct SongcastSender {
    socket: Arc<UdpSocket>,
    local_addr: SocketAddrV4,
    multicast_dest: Option<SocketAddrV4>,
    state: Arc<Mutex<SenderState>>,
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl SongcastSender {
    /// Bind and start a new [`SongcastSender`].
    pub async fn bind(config: SenderConfig) -> Result<Self, SongcastError> {
        let multicast_dest = config
            .bind_addr
            .ip()
            .is_multicast()
            .then_some(config.bind_addr);
        let socket = Arc::new(bind_udp_v4(config.bind_addr, config.interface, config.ttl)?);
        let local_addr = match socket.local_addr()? {
            SocketAddr::V4(v4) => v4,
            SocketAddr::V6(_) => {
                return Err(SongcastError::InvalidUri(
                    "IPv6 not supported by OHM".into(),
                ));
            }
        };

        let state = Arc::new(Mutex::new(SenderState {
            epoch: Instant::now(),
            frame: 0,
            sample_start: 0,
            track: OhmTrack {
                sequence: 0,
                uri: String::new(),
                metadata: String::new(),
            },
            metatext: OhmMetatext {
                sequence: 0,
                metatext: String::new(),
            },
            receivers: HashMap::new(),
            primary: None,
            history: VecDeque::with_capacity(config.history_frames),
            max_history: config.history_frames.max(1),
            latency_ms: config.latency_ms,
            receiver_expiry: config.receiver_expiry,
        }));

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let sock_bg = socket.clone();
        let state_bg = state.clone();

        let task = tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    recv = sock_bg.recv_from(&mut buf) => {
                        let Ok((len, SocketAddr::V4(peer))) = recv else { continue };
                        let Ok(msg) = OhmMessage::decode(&buf[..len]) else { continue };
                        handle_incoming_control(
                            &sock_bg,
                            &state_bg,
                            multicast_dest,
                            peer,
                            msg,
                        ).await;
                    }
                }
            }
        });

        Ok(Self {
            socket,
            local_addr,
            multicast_dest,
            state,
            shutdown_tx,
            task,
        })
    }

    /// Return the local bound IPv4 address.
    pub fn local_addr(&self) -> SocketAddrV4 {
        self.local_addr
    }

    /// Return the [`SongcastUri`] (`ohm://...` or `ohu://...`) receivers should connect to.
    pub fn uri(&self) -> SongcastUri {
        match self.multicast_dest {
            Some(mc) => SongcastUri::Multicast(mc),
            None => SongcastUri::Unicast(self.local_addr),
        }
    }

    /// Return the number of currently active (non-expired) receivers.
    pub fn active_receivers(&self) -> usize {
        let mut st = self.state.lock().unwrap();
        st.prune_expired(Instant::now());
        st.receivers.len()
    }

    /// Update the current track URI and DIDL-Lite metadata, broadcasting `OhmMessage::Track`
    /// to active receivers.
    pub async fn set_track(
        &self,
        uri: impl Into<String>,
        metadata: impl Into<String>,
    ) -> Result<(), SongcastError> {
        let (track, targets) = {
            let mut st = self.state.lock().unwrap();
            st.prune_expired(Instant::now());
            st.track.sequence = st.track.sequence.wrapping_add(1);
            st.track.uri = uri.into();
            st.track.metadata = metadata.into();
            st.sample_start = 0;
            (st.track.clone(), self.destinations(&st))
        };
        let bytes = OhmMessage::Track(track).encode()?;
        self.send_to_targets(&bytes, &targets).await
    }

    /// Update the current live metatext, broadcasting `OhmMessage::Metatext` to active receivers.
    pub async fn set_metatext(&self, metatext: impl Into<String>) -> Result<(), SongcastError> {
        let (mt, targets) = {
            let mut st = self.state.lock().unwrap();
            st.prune_expired(Instant::now());
            st.metatext.sequence = st.metatext.sequence.wrapping_add(1);
            st.metatext.metatext = metatext.into();
            (st.metatext.clone(), self.destinations(&st))
        };
        let bytes = OhmMessage::Metatext(mt).encode()?;
        self.send_to_targets(&bytes, &targets).await
    }

    /// Advance the frame counter by `250` (matching `ohPipeline`'s `StreamInterrupted`)
    /// so receivers resync cleanly after a discontinuity rather than requesting stale frames.
    pub fn stream_interrupted(&self) {
        let mut st = self.state.lock().unwrap();
        st.frame = st.frame.wrapping_add(250);
        st.history.clear();
    }

    /// Package and transmit one big-endian interleaved PCM audio frame.
    ///
    /// Returns the assigned frame sequence number. If no receivers are active, the frame
    /// counter and `sample_start` still advance (matching `ohSongcast`) without sending UDP.
    pub async fn send_audio(
        &self,
        format: &AudioFormat,
        pcm_be: Vec<u8>,
        halt: bool,
    ) -> Result<u32, SongcastError> {
        let bytes_per_sample_frame =
            (usize::from(format.bit_depth) / 8).max(1) * usize::from(format.channels).max(1);
        let samples = (pcm_be.len() / bytes_per_sample_frame).min(usize::from(u16::MAX)) as u16;

        let (frame_no, maybe_audio, targets) = {
            let mut st = self.state.lock().unwrap();
            let now = Instant::now();
            st.prune_expired(now);

            let targets = self.destinations(&st);
            let frame = st.frame;
            let sample_start = st.sample_start;
            st.sample_start = st.sample_start.wrapping_add(u64::from(samples));
            if targets.is_empty() {
                // Per ohSongcast OhmSenderDriver::SendAudio, frame only advances when actively sending.
                (frame, None, targets)
            } else {
                st.frame = st.frame.wrapping_add(1);

                let mut flags = AudioFlags::TIMESTAMPED;
                if format.lossless {
                    flags |= AudioFlags::LOSSLESS;
                }
                if halt {
                    flags |= AudioFlags::HALT;
                }

                let network_timestamp =
                    duration_to_ticks(now.saturating_duration_since(st.epoch), format.sample_rate);
                let media_latency = latency_ms_to_ticks(st.latency_ms, format.sample_rate);

                let audio = OhmAudio {
                    flags: AudioFlags(flags),
                    samples,
                    frame,
                    network_timestamp,
                    media_latency,
                    media_timestamp: 0,
                    sample_start,
                    samples_total: format.samples_total,
                    sample_rate: format.sample_rate,
                    bit_rate: format.bit_rate,
                    volume_offset: format.volume_offset,
                    bit_depth: format.bit_depth,
                    channels: format.channels,
                    codec: format.codec.clone(),
                    audio: pcm_be,
                };
                (frame, Some(audio), targets)
            }
        };

        if let Some(audio) = maybe_audio {
            let bytes = OhmMessage::Audio(audio).encode()?;
            self.send_to_targets(&bytes, &targets).await?;
            self.state.lock().unwrap().record_history(frame_no, bytes);
        }
        Ok(frame_no)
    }

    fn destinations(&self, st: &SenderState) -> Vec<SocketAddrV4> {
        if st.receivers.is_empty() {
            return Vec::new();
        }
        match self.multicast_dest {
            Some(mc) => vec![mc],
            // Per ohSongcast/ohPipeline, unicast sender streams only to the primary
            // receiver, which fans out to the Slave list.
            None => st.primary.into_iter().collect(),
        }
    }

    async fn send_to_targets(
        &self,
        bytes: &[u8],
        targets: &[SocketAddrV4],
    ) -> Result<(), SongcastError> {
        for &dest in targets {
            self.socket.send_to(bytes, SocketAddr::V4(dest)).await?;
        }
        Ok(())
    }

    /// Shut down the sender background control loop cleanly.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

async fn handle_incoming_control(
    socket: &UdpSocket,
    state: &Mutex<SenderState>,
    multicast_dest: Option<SocketAddrV4>,
    peer: SocketAddrV4,
    msg: OhmMessage,
) {
    let now = Instant::now();
    match msg {
        OhmMessage::Join => {
            let (track, metatext, slave_update) = {
                let mut st = state.lock().unwrap();
                st.prune_expired(now);
                let is_new = !st.receivers.contains_key(&peer);
                if is_new && multicast_dest.is_none() && st.receivers.len() > MAX_SLAVES {
                    return;
                }
                st.receivers.insert(peer, now);
                if st.primary.is_none() {
                    st.primary = Some(peer);
                }
                let slave_update = if multicast_dest.is_none() {
                    st.primary.map(|p| (p, st.slaves()))
                } else {
                    None
                };
                (st.track.clone(), st.metatext.clone(), slave_update)
            };

            // Per ohSongcast/ohPipeline, unicast Join sends Slave list + Track/Metatext
            // to primary (for relay to slaves) and a Listen to a newly joined slave.
            if let Some((primary, slaves)) = slave_update {
                if let Ok(sb) = OhmMessage::Slave(slaves).encode() {
                    let _ = socket.send_to(&sb, SocketAddr::V4(primary)).await;
                }
                if let Ok(tb) = OhmMessage::Track(track).encode() {
                    let _ = socket.send_to(&tb, SocketAddr::V4(primary)).await;
                }
                if let Ok(mb) = OhmMessage::Metatext(metatext).encode() {
                    let _ = socket.send_to(&mb, SocketAddr::V4(primary)).await;
                }
                if peer != primary
                    && let Ok(lb) = OhmMessage::Listen.encode()
                {
                    let _ = socket.send_to(&lb, SocketAddr::V4(peer)).await;
                }
            } else {
                let reply_dest = multicast_dest.unwrap_or(peer);
                if let Ok(tb) = OhmMessage::Track(track).encode() {
                    let _ = socket.send_to(&tb, SocketAddr::V4(reply_dest)).await;
                }
                if let Ok(mb) = OhmMessage::Metatext(metatext).encode() {
                    let _ = socket.send_to(&mb, SocketAddr::V4(reply_dest)).await;
                }
            }
        }
        OhmMessage::Listen => {
            let mut st = state.lock().unwrap();
            st.prune_expired(now);
            if st.receivers.contains_key(&peer) || st.receivers.len() < MAX_SLAVES + 1 {
                st.receivers.insert(peer, now);
                if st.primary.is_none() {
                    st.primary = Some(peer);
                }
            }
        }
        OhmMessage::Leave => {
            let slave_update = {
                let mut st = state.lock().unwrap();
                st.receivers.remove(&peer);
                if st.primary == Some(peer) {
                    st.primary = st.receivers.keys().next().copied();
                }
                if multicast_dest.is_none() {
                    st.primary.map(|p| (p, st.slaves()))
                } else {
                    None
                }
            };
            if let Some((primary, slaves)) = slave_update
                && let Ok(sb) = OhmMessage::Slave(slaves).encode()
            {
                let _ = socket.send_to(&sb, SocketAddr::V4(primary)).await;
            }
        }
        OhmMessage::Resend(frames) => {
            let to_resend = {
                let st = state.lock().unwrap();
                st.lookup_history(&frames)
            };
            let dest = multicast_dest.unwrap_or(peer);
            for bytes in to_resend {
                let _ = socket.send_to(&bytes, SocketAddr::V4(dest)).await;
            }
        }
        _ => {}
    }
}
