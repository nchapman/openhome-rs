//! Dedicated playback engine worker for Playlist decoding and Songcast PCM reception.

use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use openhome_audio::{
    AudioSink, DecodedStream, GainRamp, LoadCancel, PcmAdapter, PcmSpec, SinkStream,
    VolumeCharacteristics as AudioVolumeChars, VolumeLevel, channel_gains as audio_channel_gains,
    f32_into_pcm_be, pcm_be_into_f32,
};
use openhome_core::types::{
    ReceiverState, TrackDetails, TransportState, VolumeCharacteristics, VolumeState,
};
use openhome_songcast::{
    AudioFormat, ReceiverConfig, ReceiverEvent, SongcastReceiver, SongcastSender, SongcastUri,
};
use tokio::sync::mpsc as tokio_mpsc;

fn channel_gains(state: &VolumeState, chars: &VolumeCharacteristics) -> (f32, f32) {
    audio_channel_gains(
        &VolumeLevel {
            volume: state.volume,
            mute: state.mute,
            balance: state.balance,
        },
        &AudioVolumeChars {
            volume_max: chars.volume_max,
            volume_milli_db_per_step: chars.volume_milli_db_per_step,
            balance_max: chars.balance_max,
        },
    )
}

/// Commands sent from OpenHome service handlers into [`PlaybackEngine`].
#[derive(Debug, Clone)]
pub enum EngineCommand {
    /// Open `uri` and begin playing (or stay paused if `paused == true`).
    Load {
        generation: u64,
        uri: String,
        paused: bool,
    },
    /// Resume playback of the currently loaded track.
    Play,
    /// Pause playback and flush the output sink.
    Pause,
    /// Stop playback immediately, flushing and closing any active track / Songcast stream.
    Stop,
    /// Drain any buffered audio at end-of-playlist before closing the sink cleanly.
    DrainAndStop,
    /// Seek the active track to `seconds` from start.
    Seek(u32),
    /// Update live volume/mute/balance gain.
    SetVolume(VolumeState),
    /// Update the active Playlist track's URI and DIDL-Lite metadata for Songcast Sender broadcast.
    SetTrackMetadata { uri: String, metadata: String },
    /// Start receiving and playing a Songcast stream (`ohm://`, `ohu://`, or `ohz://`).
    StartSongcast {
        generation: u64,
        uri: String,
        interface: Ipv4Addr,
    },
    /// Stop any active Songcast stream.
    StopSongcast,
    /// Terminate the worker thread cleanly.
    Shutdown,
}

/// Asynchronous events emitted by [`PlaybackEngine`] back to the OpenHome service layer.
#[derive(Debug, Clone)]
pub enum EngineEvent {
    /// Playlist transport state changed for `generation`.
    StateChanged {
        generation: u64,
        state: TransportState,
    },
    /// Audio format/duration details discovered for the active track or Songcast stream.
    Details {
        generation: u64,
        details: TrackDetails,
    },
    /// Playback elapsed time tick (`seconds`).
    Time { generation: u64, seconds: u32 },
    /// Active playlist track reached clean end-of-stream.
    TrackEnded { generation: u64 },
    /// Songcast `Receiver` transport state changed (`Waiting`, `Playing`, `Stopped`).
    SongcastState {
        generation: u64,
        state: ReceiverState,
    },
    /// Songcast `OhmTrack` metadata update.
    SongcastTrack {
        generation: u64,
        uri: String,
        metadata: String,
    },
    /// Songcast `OhmMetatext` update.
    SongcastMetatext { generation: u64, metatext: String },
    /// Playback or decode error occurred.
    Error { generation: u64, message: String },
}

/// Background audio playback worker owning decoding and [`AudioSink`] writes.
pub struct PlaybackEngine {
    cmd_tx: Sender<EngineCommand>,
    latest_generation: Arc<AtomicU64>,
    owned_generation: bool,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for PlaybackEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaybackEngine").finish()
    }
}

impl PlaybackEngine {
    /// Spawn the playback worker with default options (HTTP-only URIs, default volume curve).
    pub fn spawn(
        sink: Arc<dyn AudioSink>,
        runtime: tokio::runtime::Handle,
    ) -> (Self, tokio_mpsc::UnboundedReceiver<EngineEvent>) {
        let (mut engine, rx) = Self::spawn_with_options(
            sink,
            runtime,
            Arc::new(AtomicU64::new(0)),
            false,
            VolumeCharacteristics::default(),
        );
        engine.owned_generation = true;
        (engine, rx)
    }

    /// Spawn the playback worker with shared generation counter, local-file policy, and volume characteristics.
    pub fn spawn_with_options(
        sink: Arc<dyn AudioSink>,
        runtime: tokio::runtime::Handle,
        latest_generation: Arc<AtomicU64>,
        allow_local_files: bool,
        volume_chars: VolumeCharacteristics,
    ) -> (Self, tokio_mpsc::UnboundedReceiver<EngineEvent>) {
        Self::spawn_with_sender(
            sink,
            runtime,
            latest_generation,
            allow_local_files,
            volume_chars,
            None,
        )
    }

    /// Spawn the playback worker with an optional [`SongcastSender`] for multiroom rebroadcasting.
    pub fn spawn_with_sender(
        sink: Arc<dyn AudioSink>,
        runtime: tokio::runtime::Handle,
        latest_generation: Arc<AtomicU64>,
        allow_local_files: bool,
        volume_chars: VolumeCharacteristics,
        sender: Option<Arc<SongcastSender>>,
    ) -> (Self, tokio_mpsc::UnboundedReceiver<EngineEvent>) {
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let (ev_tx, ev_rx) = tokio_mpsc::unbounded_channel();
        let worker_gen = Arc::clone(&latest_generation);
        let worker = thread::Builder::new()
            .name("openhome-endpoint-worker".into())
            .spawn(move || {
                worker_loop(
                    sink,
                    runtime,
                    cmd_rx,
                    ev_tx,
                    worker_gen,
                    allow_local_files,
                    volume_chars,
                    sender,
                )
            })
            .expect("spawn openhome-endpoint-worker");
        (
            Self {
                cmd_tx,
                latest_generation,
                owned_generation: false,
                worker: Mutex::new(Some(worker)),
            },
            ev_rx,
        )
    }

    /// Send a non-blocking command to the playback worker.
    pub fn send(&self, cmd: EngineCommand) {
        if self.owned_generation
            && let EngineCommand::Load { generation, .. }
            | EngineCommand::StartSongcast { generation, .. } = &cmd
        {
            self.latest_generation.store(*generation, Ordering::Release);
        }
        let _ = self.cmd_tx.send(cmd);
    }

    /// Synchronously shut down and join the playback worker thread.
    ///
    /// Do not call from a Tokio runtime thread; use [`Self::shutdown_async`] instead.
    pub fn shutdown(self) {
        self.latest_generation.fetch_add(1, Ordering::AcqRel);
        let _ = self.cmd_tx.send(EngineCommand::Shutdown);
        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }
    }

    /// Asynchronously shut down and join the playback worker thread without blocking Tokio.
    pub async fn shutdown_async(&self) {
        self.latest_generation.fetch_add(1, Ordering::AcqRel);
        let _ = self.cmd_tx.send(EngineCommand::Shutdown);
        let handle = self.worker.lock().unwrap().take();
        if let Some(worker) = handle {
            let _ = tokio::task::spawn_blocking(move || {
                let _ = worker.join();
            })
            .await;
        }
    }
}

impl Drop for PlaybackEngine {
    fn drop(&mut self) {
        self.latest_generation.fetch_add(1, Ordering::AcqRel);
        let _ = self.cmd_tx.send(EngineCommand::Shutdown);
    }
}

struct ActiveTrack {
    generation: u64,
    stream: DecodedStream,
    sink: Box<dyn SinkStream>,
    paused: bool,
    frames_played: u64,
    last_reported_sec: u32,
}

struct ActiveSongcast {
    generation: u64,
    receiver: SongcastReceiver,
    sink: Option<Box<dyn SinkStream>>,
    last_spec: Option<PcmSpec>,
    last_details: Option<TrackDetails>,
    playing: bool,
    prebuffered: bool,
    drift_adapter: Option<PcmAdapter>,
    drift_scratch: Vec<f32>,
    last_reported_sec: u32,
}

/// Maximum silence pre-buffer lead applied to align the first Songcast audio packet to `playout_at`.
const MAX_SONGCAST_PREBUFFER: Duration = Duration::from_millis(250);

fn send_songcast_halt(
    runtime: &tokio::runtime::Handle,
    sender: &Option<Arc<SongcastSender>>,
    last_fmt: &mut Option<AudioFormat>,
) {
    if let (Some(s), Some(fmt)) = (sender.as_ref(), last_fmt.take())
        && s.active_receivers() > 0
    {
        let _ = runtime.block_on(s.send_audio(&fmt, Vec::new(), true));
    }
}

fn broadcast_playlist_pcm(
    runtime: &tokio::runtime::Handle,
    sender: &Arc<SongcastSender>,
    spec: PcmSpec,
    stream: &DecodedStream,
    samples: &[f32],
    pcm_be_buf: &mut Vec<u8>,
    last_fmt: &mut Option<AudioFormat>,
) {
    if sender.active_receivers() == 0
        || samples.is_empty()
        || spec.sample_rate == 0
        || spec.channels == 0
    {
        return;
    }
    let channels = (spec.channels as u8).max(1);
    // Preserve 24-bit PCM for hi-res sources (matching ohPipeline's 24-bit Songcast cap).
    let bit_depth: u8 = if spec.bits_per_sample >= 24 { 24 } else { 16 };
    let fmt = AudioFormat {
        sample_rate: spec.sample_rate,
        bit_depth,
        channels,
        bit_rate: stream.bit_rate(),
        lossless: stream.is_lossless(),
        codec: stream.codec_name().to_string(),
        volume_offset: 0,
        samples_total: u64::from(stream.duration_secs()) * u64::from(spec.sample_rate),
    };
    *last_fmt = Some(fmt.clone());

    // Chunk into ~5 ms UDP packets capped at <=1360 payload bytes so high-rate / 24-bit
    // streams (96 kHz / 192 kHz) stay below Ethernet MTU without IP fragmentation.
    let bytes_per_frame = usize::from(channels) * usize::from(bit_depth / 8);
    let mtu_max_frames = (1360 / bytes_per_frame.max(1)).max(1);
    let frames_per_pkt = (spec.sample_rate as usize / 200).max(1).min(mtu_max_frames);
    let samples_per_pkt = frames_per_pkt * usize::from(channels);
    for chunk in samples.chunks(samples_per_pkt) {
        f32_into_pcm_be(chunk, bit_depth, pcm_be_buf);
        if !pcm_be_buf.is_empty() {
            let _ = runtime.block_on(sender.send_audio(&fmt, pcm_be_buf.clone(), false));
        }
    }
}

fn shutdown_songcast_nonblocking(runtime: &tokio::runtime::Handle, sc: ActiveSongcast) {
    // Drop the audio sink immediately on the worker thread before spawning async Leave.
    drop(sc.sink);
    let receiver = sc.receiver;
    runtime.spawn(async move {
        receiver.shutdown().await;
    });
}

#[allow(clippy::too_many_arguments)]
fn worker_loop(
    sink_factory: Arc<dyn AudioSink>,
    runtime: tokio::runtime::Handle,
    cmd_rx: Receiver<EngineCommand>,
    ev_tx: tokio_mpsc::UnboundedSender<EngineEvent>,
    latest_generation: Arc<AtomicU64>,
    allow_local_files: bool,
    volume_chars: VolumeCharacteristics,
    sender: Option<Arc<SongcastSender>>,
) {
    let default_vol = VolumeState {
        volume: 50,
        mute: false,
        balance: 0,
        fade: 0,
        volume_limit: volume_chars.volume_max,
    };
    let (init_l, init_r) = channel_gains(&default_vol, &volume_chars);
    let mut gain_ramp = GainRamp::new(init_l, init_r);
    let mut track: Option<ActiveTrack> = None;
    let mut idle_sink: Option<Box<dyn SinkStream>> = None;
    let mut songcast: Option<ActiveSongcast> = None;
    let mut pcm_scratch: Vec<f32> = Vec::with_capacity(8192);
    let mut sender_pcm_be: Vec<u8> = Vec::with_capacity(2048);
    let mut last_sender_fmt: Option<AudioFormat> = None;

    loop {
        let is_active = track.as_ref().is_some_and(|t| !t.paused) || songcast.is_some();
        let next_cmd = if is_active {
            match cmd_rx.try_recv() {
                Ok(c) => Some(c),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => break,
            }
        } else {
            match cmd_rx.recv() {
                Ok(c) => Some(c),
                Err(_) => break,
            }
        };

        if let Some(cmd) = next_cmd {
            match cmd {
                EngineCommand::Shutdown => {
                    send_songcast_halt(&runtime, &sender, &mut last_sender_fmt);
                    drop(track);
                    drop(idle_sink);
                    if let Some(sc) = songcast.take() {
                        shutdown_songcast_nonblocking(&runtime, sc);
                    }
                    break;
                }
                EngineCommand::SetTrackMetadata { uri, metadata } => {
                    if let Some(s) = sender.as_ref() {
                        let _ = runtime.block_on(s.set_track(uri, metadata));
                    }
                }
                EngineCommand::SetVolume(v) => {
                    let (lg, rg) = channel_gains(&v, &volume_chars);
                    if track.as_ref().is_none_or(|t| t.paused) && songcast.is_none() {
                        gain_ramp.snap_to(lg, rg);
                    } else {
                        gain_ramp.set_target(lg, rg);
                    }
                }
                EngineCommand::Stop => {
                    send_songcast_halt(&runtime, &sender, &mut last_sender_fmt);
                    if let Some(mut t) = track.take() {
                        t.sink.flush();
                    }
                    if let Some(mut s) = idle_sink.take() {
                        s.flush();
                    }
                    if let Some(sc) = songcast.take() {
                        shutdown_songcast_nonblocking(&runtime, sc);
                    }
                }
                EngineCommand::DrainAndStop => {
                    send_songcast_halt(&runtime, &sender, &mut last_sender_fmt);
                    if let Some(mut t) = track.take() {
                        t.sink.drain();
                    }
                    if let Some(mut s) = idle_sink.take() {
                        s.drain();
                    }
                    if let Some(sc) = songcast.take() {
                        shutdown_songcast_nonblocking(&runtime, sc);
                    }
                }
                EngineCommand::StopSongcast => {
                    if let Some(sc) = songcast.take() {
                        shutdown_songcast_nonblocking(&runtime, sc);
                    }
                }
                EngineCommand::Pause => {
                    if let Some(t) = track.as_mut() {
                        t.paused = true;
                        t.sink.flush();
                        send_songcast_halt(&runtime, &sender, &mut last_sender_fmt);
                        let _ = ev_tx.send(EngineEvent::StateChanged {
                            generation: t.generation,
                            state: TransportState::Paused,
                        });
                    }
                }
                EngineCommand::Play => {
                    if let Some(t) = track.as_mut() {
                        t.paused = false;
                        let _ = ev_tx.send(EngineEvent::StateChanged {
                            generation: t.generation,
                            state: TransportState::Playing,
                        });
                    }
                }
                EngineCommand::Seek(secs) => {
                    if let Some(t) = track.as_mut() {
                        match t.stream.seek(secs) {
                            Ok(actual) => {
                                t.sink.flush();
                                send_songcast_halt(&runtime, &sender, &mut last_sender_fmt);
                                t.frames_played =
                                    u64::from(actual) * u64::from(t.stream.spec().sample_rate);
                                t.last_reported_sec = actual;
                                let _ = ev_tx.send(EngineEvent::Time {
                                    generation: t.generation,
                                    seconds: actual,
                                });
                            }
                            Err(_) => {
                                // Resync MemoryRenderer to our true position on non-seekable streams.
                                let _ = ev_tx.send(EngineEvent::Time {
                                    generation: t.generation,
                                    seconds: t.last_reported_sec,
                                });
                            }
                        }
                    }
                }
                EngineCommand::Load {
                    generation,
                    uri,
                    paused,
                } => {
                    // Skip superseded Load commands immediately.
                    let latest = latest_generation.load(Ordering::Acquire);
                    if latest != 0 && generation != latest {
                        continue;
                    }

                    if let Some(sc) = songcast.take() {
                        shutdown_songcast_nonblocking(&runtime, sc);
                    }

                    // Preserve an open sink for gapless reuse if the next track has a compatible
                    // (sample_rate, channels) format:
                    // - Manual skip (track was active): flush immediately so old audio does not bleed.
                    // - Natural EOF (idle_sink): do NOT flush so queued tail plays out gaplessly.
                    let (mut reusable_sink, was_gapless) = if let Some(mut old) = track.take() {
                        old.sink.flush();
                        drop(idle_sink.take());
                        (Some(old.sink), false)
                    } else if let Some(s) = idle_sink.take() {
                        (Some(s), true)
                    } else {
                        (None, false)
                    };

                    let _ = ev_tx.send(EngineEvent::StateChanged {
                        generation,
                        state: TransportState::Buffering,
                    });

                    let cancel = if latest != 0 {
                        Some(LoadCancel::new(generation, Arc::clone(&latest_generation)))
                    } else {
                        None
                    };

                    match DecodedStream::open_with_options(&uri, allow_local_files, cancel) {
                        Ok(stream) => {
                            let spec = stream.spec();
                            let sink_res = match reusable_sink.take() {
                                Some(s) if s.spec().is_sink_compatible(&spec) => Ok(s),
                                Some(mut old) => {
                                    // On gapless format change, drain the previous track's tail
                                    // before dropping and reopening the hardware stream (avoids EBUSY).
                                    if was_gapless {
                                        old.drain();
                                    }
                                    drop(old);
                                    sink_factory.open(spec)
                                }
                                None => sink_factory.open(spec),
                            };
                            match sink_res {
                                Ok(sink_stream) => {
                                    let dur = stream.duration_secs();
                                    let _ = ev_tx.send(EngineEvent::Details {
                                        generation,
                                        details: TrackDetails {
                                            duration: dur,
                                            bit_rate: stream.bit_rate(),
                                            bit_depth: u32::from(spec.bits_per_sample),
                                            sample_rate: spec.sample_rate,
                                            lossless: stream.is_lossless(),
                                            codec_name: stream.codec_name().to_string(),
                                        },
                                    });
                                    let _ = ev_tx.send(EngineEvent::Time {
                                        generation,
                                        seconds: 0,
                                    });
                                    let state = if paused {
                                        TransportState::Paused
                                    } else {
                                        TransportState::Playing
                                    };
                                    let _ =
                                        ev_tx.send(EngineEvent::StateChanged { generation, state });
                                    track = Some(ActiveTrack {
                                        generation,
                                        stream,
                                        sink: sink_stream,
                                        paused,
                                        frames_played: 0,
                                        last_reported_sec: 0,
                                    });
                                }
                                Err(err) => {
                                    let _ = ev_tx.send(EngineEvent::Error {
                                        generation,
                                        message: err.to_string(),
                                    });
                                }
                            }
                        }
                        Err(err) => {
                            if let Some(mut old) = reusable_sink.take()
                                && was_gapless
                            {
                                old.drain();
                            }
                            let _ = ev_tx.send(EngineEvent::Error {
                                generation,
                                message: err.to_string(),
                            });
                        }
                    }
                }
                EngineCommand::StartSongcast {
                    generation,
                    uri,
                    interface,
                } => {
                    let latest = latest_generation.load(Ordering::Acquire);
                    if latest != 0 && generation != latest {
                        continue;
                    }

                    send_songcast_halt(&runtime, &sender, &mut last_sender_fmt);
                    track = None;
                    drop(idle_sink.take());
                    if let Some(sc) = songcast.take() {
                        shutdown_songcast_nonblocking(&runtime, sc);
                    }

                    match SongcastUri::parse(&uri) {
                        Ok(sc_uri) => {
                            let cfg = ReceiverConfig {
                                interface,
                                ..ReceiverConfig::default()
                            };
                            match runtime.block_on(SongcastReceiver::connect(&sc_uri, cfg)) {
                                Ok(receiver) => {
                                    let _ = ev_tx.send(EngineEvent::SongcastState {
                                        generation,
                                        state: ReceiverState::Waiting,
                                    });
                                    songcast = Some(ActiveSongcast {
                                        generation,
                                        receiver,
                                        sink: None,
                                        last_spec: None,
                                        last_details: None,
                                        playing: false,
                                        prebuffered: false,
                                        drift_adapter: None,
                                        drift_scratch: Vec::new(),
                                        last_reported_sec: u32::MAX,
                                    });
                                }
                                Err(err) => {
                                    let _ = ev_tx.send(EngineEvent::Error {
                                        generation,
                                        message: err.to_string(),
                                    });
                                }
                            }
                        }
                        Err(err) => {
                            let _ = ev_tx.send(EngineEvent::Error {
                                generation,
                                message: err.to_string(),
                            });
                        }
                    }
                }
            }
            continue;
        }

        // Pump one Playlist audio block if playing.
        if let Some(active) = track.as_mut()
            && !active.paused
        {
            match active.stream.next_frame_into(&mut pcm_scratch) {
                Ok(true) => {
                    let spec = active.stream.spec();
                    // Reopen sink if mid-stream sample rate or channel count changed.
                    if !active.sink.spec().is_sink_compatible(&spec) {
                        let generation = active.generation;
                        let mut owned = track.take().unwrap();
                        owned.sink.drain();
                        drop(owned.sink);
                        match sink_factory.open(spec) {
                            Ok(new_sink) => {
                                owned.sink = new_sink;
                                let _ = ev_tx.send(EngineEvent::Details {
                                    generation,
                                    details: TrackDetails {
                                        duration: owned.stream.duration_secs(),
                                        bit_rate: owned.stream.bit_rate(),
                                        bit_depth: u32::from(spec.bits_per_sample),
                                        sample_rate: spec.sample_rate,
                                        lossless: owned.stream.is_lossless(),
                                        codec_name: owned.stream.codec_name().to_string(),
                                    },
                                });
                                track = Some(owned);
                            }
                            Err(err) => {
                                let _ = ev_tx.send(EngineEvent::Error {
                                    generation,
                                    message: err.to_string(),
                                });
                                continue;
                            }
                        }
                    }

                    let active = track.as_mut().unwrap();
                    // Rebroadcast full-scale unity PCM to Songcast followers BEFORE local volume scaling
                    // so every grouped room controls its own independent volume (matching ohPipeline).
                    if let Some(s) = sender.as_ref() {
                        broadcast_playlist_pcm(
                            &runtime,
                            s,
                            spec,
                            &active.stream,
                            &pcm_scratch,
                            &mut sender_pcm_be,
                            &mut last_sender_fmt,
                        );
                    }
                    gain_ramp.apply(&mut pcm_scratch, spec.channels, spec.sample_rate);
                    if let Err(err) = active.sink.write(&pcm_scratch) {
                        let generation = active.generation;
                        track = None;
                        let _ = ev_tx.send(EngineEvent::Error {
                            generation,
                            message: err.to_string(),
                        });
                        continue;
                    }
                    active.frames_played += spec.frames_in(pcm_scratch.len());
                    if spec.sample_rate > 0 {
                        let sec = (active.frames_played / u64::from(spec.sample_rate)) as u32;
                        if sec != active.last_reported_sec {
                            active.last_reported_sec = sec;
                            let _ = ev_tx.send(EngineEvent::Time {
                                generation: active.generation,
                                seconds: sec,
                            });
                        }
                    }
                }
                Ok(false) => {
                    let owned = track.take().unwrap();
                    let generation = owned.generation;
                    // Hold sink across EOF so the next gapless Load reuses it without clipping
                    // the buffered tail, or DrainAndStop drains it cleanly at end-of-playlist.
                    idle_sink = Some(owned.sink);
                    let _ = ev_tx.send(EngineEvent::TrackEnded { generation });
                }
                Err(err) => {
                    let generation = active.generation;
                    track = None;
                    let _ = ev_tx.send(EngineEvent::Error {
                        generation,
                        message: err.to_string(),
                    });
                }
            }
            continue;
        }

        // Pump bounded Songcast Receiver events per tick so cmd_rx is always responsive
        // even when a real-time hardware sink blocks ~5 ms per write().
        if let Some(sc) = songcast.as_mut() {
            const MAX_SONGCAST_EVENTS_PER_TICK: usize = 4;
            for iter in 0..MAX_SONGCAST_EVENTS_PER_TICK {
                let ev = match sc.receiver.try_recv() {
                    Ok(ev) => Some(ev),
                    Err(tokio_mpsc::error::TryRecvError::Empty) => {
                        if iter > 0 {
                            break;
                        }
                        // Brief wait so commands remain responsive without spinning CPU.
                        match runtime.block_on(async {
                            tokio::time::timeout(Duration::from_millis(10), sc.receiver.recv())
                                .await
                        }) {
                            Ok(opt) => opt,
                            Err(_) => break,
                        }
                    }
                    Err(tokio_mpsc::error::TryRecvError::Disconnected) => None,
                };

                match ev {
                    Some(ReceiverEvent::Audio {
                        frame,
                        playout_at,
                        skew_ppm,
                    }) => {
                        if frame.flags.is_halt() {
                            if let Some(sink) = sc.sink.as_mut() {
                                sink.flush();
                            }
                            sc.prebuffered = false;
                            if let Some(adapter) = sc.drift_adapter.as_mut() {
                                adapter.reset();
                            }
                            if sc.playing {
                                sc.playing = false;
                                let _ = ev_tx.send(EngineEvent::SongcastState {
                                    generation: sc.generation,
                                    state: ReceiverState::Waiting,
                                });
                            }
                            continue;
                        }
                        if frame.sample_rate == 0 || frame.audio.is_empty() {
                            continue;
                        }

                        let spec = PcmSpec::new(
                            frame.sample_rate,
                            u16::from(frame.channels.max(1)),
                            u16::from(frame.bit_depth.max(16)),
                        );
                        if !sc.last_spec.is_some_and(|s| s.is_sink_compatible(&spec)) {
                            // Drop old sink BEFORE opening new one to avoid EBUSY on exclusive devices.
                            sc.sink = None;
                            sc.prebuffered = false;
                            sc.drift_adapter = None;
                            match sink_factory.open(spec) {
                                Ok(s) => {
                                    sc.sink = Some(s);
                                    sc.last_spec = Some(spec);
                                }
                                Err(err) => {
                                    // Tear down Songcast immediately so we don't spam errors on every UDP packet.
                                    let generation = sc.generation;
                                    if let Some(sc_owned) = songcast.take() {
                                        shutdown_songcast_nonblocking(&runtime, sc_owned);
                                    }
                                    let _ = ev_tx.send(EngineEvent::Error {
                                        generation,
                                        message: err.to_string(),
                                    });
                                    break;
                                }
                            }
                        }

                        let details = TrackDetails {
                            duration: if frame.samples_total > 0 {
                                u32::try_from(frame.samples_total / u64::from(frame.sample_rate))
                                    .unwrap_or(u32::MAX)
                            } else {
                                0
                            },
                            bit_rate: frame.bit_rate,
                            bit_depth: u32::from(frame.bit_depth),
                            sample_rate: frame.sample_rate,
                            lossless: frame.flags.is_lossless(),
                            codec_name: if frame.codec.is_empty() {
                                "PCM".into()
                            } else {
                                frame.codec.clone()
                            },
                        };
                        if sc.last_details.as_ref() != Some(&details) {
                            sc.last_details = Some(details.clone());
                            let _ = ev_tx.send(EngineEvent::Details {
                                generation: sc.generation,
                                details,
                            });
                        }

                        if !sc.playing {
                            sc.playing = true;
                            let _ = ev_tx.send(EngineEvent::SongcastState {
                                generation: sc.generation,
                                state: ReceiverState::Playing,
                            });
                        }

                        let sec = (frame.sample_start / u64::from(frame.sample_rate)) as u32;
                        if sec != sc.last_reported_sec {
                            sc.last_reported_sec = sec;
                            let _ = ev_tx.send(EngineEvent::Time {
                                generation: sc.generation,
                                seconds: sec,
                            });
                        }

                        // Pre-buffer silence up to `playout_at` on stream start / resync so the
                        // first real Songcast sample hits the DAC at the scheduled multiroom instant
                        // and absorbs initial network jitter.
                        if !sc.prebuffered {
                            sc.prebuffered = true;
                            let lead = playout_at
                                .saturating_duration_since(Instant::now())
                                .min(MAX_SONGCAST_PREBUFFER);
                            if lead >= Duration::from_millis(2)
                                && let Some(sink) = sc.sink.as_mut()
                            {
                                let lead_frames =
                                    ((lead.as_micros() * u128::from(spec.sample_rate)) / 1_000_000)
                                        as usize;
                                let silence_samples =
                                    lead_frames * usize::from(spec.channels.max(1));
                                if silence_samples > 0 {
                                    pcm_scratch.clear();
                                    pcm_scratch.resize(silence_samples, 0.0);
                                    let _ = sink.write(&pcm_scratch);
                                }
                            }
                        }

                        pcm_be_into_f32(&frame.audio, frame.bit_depth, &mut pcm_scratch);
                        if !pcm_scratch.is_empty() {
                            // Apply gentle clock-drift compensation when sender skew exceeds 2 ppm.
                            let out_samples: &mut [f32] =
                                if skew_ppm.is_finite() && skew_ppm.abs() >= 2.0 {
                                    let adapter = sc.drift_adapter.get_or_insert_with(|| {
                                        PcmAdapter::new(spec, spec.sample_rate, spec.channels)
                                    });
                                    adapter.set_rate_adjust_ppm(skew_ppm);
                                    adapter.process(&pcm_scratch, &mut sc.drift_scratch);
                                    &mut sc.drift_scratch
                                } else {
                                    &mut pcm_scratch
                                };

                            gain_ramp.apply(out_samples, spec.channels, spec.sample_rate);
                            if let Some(sink) = sc.sink.as_mut()
                                && let Err(err) = sink.write(out_samples)
                            {
                                let generation = sc.generation;
                                if let Some(sc_owned) = songcast.take() {
                                    shutdown_songcast_nonblocking(&runtime, sc_owned);
                                }
                                let _ = ev_tx.send(EngineEvent::Error {
                                    generation,
                                    message: err.to_string(),
                                });
                                break;
                            }
                        }
                    }
                    Some(ReceiverEvent::Track(t)) => {
                        let _ = ev_tx.send(EngineEvent::SongcastTrack {
                            generation: sc.generation,
                            uri: t.uri,
                            metadata: t.metadata,
                        });
                    }
                    Some(ReceiverEvent::Metatext(m)) => {
                        let _ = ev_tx.send(EngineEvent::SongcastMetatext {
                            generation: sc.generation,
                            metatext: m.metatext,
                        });
                    }
                    Some(ReceiverEvent::Resynced) => {
                        if let Some(sink) = sc.sink.as_mut() {
                            sink.flush();
                        }
                        sc.prebuffered = false;
                        if let Some(adapter) = sc.drift_adapter.as_mut() {
                            adapter.reset();
                        }
                    }
                    None => {
                        let generation = sc.generation;
                        songcast = None;
                        let _ = ev_tx.send(EngineEvent::SongcastState {
                            generation,
                            state: ReceiverState::Stopped,
                        });
                        break;
                    }
                }
            }
        }
    }
}
