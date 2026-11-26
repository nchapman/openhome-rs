//! Dedicated playback engine worker for Playlist decoding and Songcast PCM reception.

use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use openhome_core::types::{
    ReceiverState, TrackDetails, TransportState, VolumeCharacteristics, VolumeState,
};
use openhome_songcast::{ReceiverConfig, ReceiverEvent, SongcastReceiver, SongcastUri};
use tokio::sync::mpsc as tokio_mpsc;

use crate::decoder::{DecodedStream, LoadCancel};
use crate::format::{GainRamp, PcmSpec, channel_gains, songcast_pcm_be_into_f32};
use crate::sink::{AudioSink, SinkStream};

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
    /// Stop playback and close the active track / Songcast stream.
    Stop,
    /// Seek the active track to `seconds` from start.
    Seek(u32),
    /// Update live volume/mute/balance gain.
    SetVolume(VolumeState),
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
        Self::spawn_with_options(
            sink,
            runtime,
            Arc::new(AtomicU64::new(0)),
            false,
            VolumeCharacteristics::default(),
        )
    }

    /// Spawn the playback worker with shared generation counter, local-file policy, and volume characteristics.
    pub fn spawn_with_options(
        sink: Arc<dyn AudioSink>,
        runtime: tokio::runtime::Handle,
        latest_generation: Arc<AtomicU64>,
        allow_local_files: bool,
        volume_chars: VolumeCharacteristics,
    ) -> (Self, tokio_mpsc::UnboundedReceiver<EngineEvent>) {
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let (ev_tx, ev_rx) = tokio_mpsc::unbounded_channel();
        let worker_gen = Arc::clone(&latest_generation);
        let worker = thread::Builder::new()
            .name("openhome-player-worker".into())
            .spawn(move || {
                worker_loop(
                    sink,
                    runtime,
                    cmd_rx,
                    ev_tx,
                    worker_gen,
                    allow_local_files,
                    volume_chars,
                )
            })
            .expect("spawn openhome-player-worker");
        (
            Self {
                cmd_tx,
                latest_generation,
                worker: Mutex::new(Some(worker)),
            },
            ev_rx,
        )
    }

    /// Send a non-blocking command to the playback worker.
    pub fn send(&self, cmd: EngineCommand) {
        let _ = self.cmd_tx.send(cmd);
    }

    /// Synchronously shut down and join the playback worker thread.
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
    playing: bool,
    last_reported_sec: u32,
}

fn shutdown_songcast_nonblocking(runtime: &tokio::runtime::Handle, sc: ActiveSongcast) {
    // Drop the audio sink immediately on the worker thread before spawning async Leave.
    drop(sc.sink);
    let receiver = sc.receiver;
    runtime.spawn(async move {
        receiver.shutdown().await;
    });
}

fn worker_loop(
    sink_factory: Arc<dyn AudioSink>,
    runtime: tokio::runtime::Handle,
    cmd_rx: Receiver<EngineCommand>,
    ev_tx: tokio_mpsc::UnboundedSender<EngineEvent>,
    latest_generation: Arc<AtomicU64>,
    allow_local_files: bool,
    volume_chars: VolumeCharacteristics,
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
    let mut songcast: Option<ActiveSongcast> = None;
    let mut pcm_scratch: Vec<f32> = Vec::with_capacity(8192);

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
                    drop(track);
                    if let Some(sc) = songcast.take() {
                        shutdown_songcast_nonblocking(&runtime, sc);
                    }
                    break;
                }
                EngineCommand::SetVolume(v) => {
                    let (lg, rg) = channel_gains(&v, &volume_chars);
                    if track.is_none() && songcast.is_none() {
                        gain_ramp.snap_to(lg, rg);
                    } else {
                        gain_ramp.set_target(lg, rg);
                    }
                }
                EngineCommand::Stop => {
                    if let Some(mut t) = track.take() {
                        t.sink.flush();
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

                    // Preserve the open sink for gapless reuse if the next track matches PcmSpec.
                    let mut reusable_sink = track.take().map(|t| t.sink);

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
                                Some(s) if s.spec() == spec => Ok(s),
                                Some(old) => {
                                    // Drop old sink BEFORE opening the new one to avoid EBUSY.
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
                            drop(reusable_sink);
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

                    track = None;
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
                                        playing: false,
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
                    // Reopen sink if mid-stream format changed (e.g. mono MP3/ADTS or chained Ogg).
                    if active.sink.spec() != spec {
                        let generation = active.generation;
                        let mut owned = track.take().unwrap();
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
                    let generation = active.generation;
                    // Keep active sink? No, TrackEnded will trigger Load or Stop; drop track cleanly.
                    track = None;
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

        // Pump Songcast Receiver events: drain non-blockingly first, then wait briefly.
        if let Some(sc) = songcast.as_mut() {
            let mut handled = false;
            loop {
                let ev = match sc.receiver.try_recv() {
                    Ok(ev) => Some(ev),
                    Err(tokio_mpsc::error::TryRecvError::Empty) => {
                        if handled {
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

                handled = true;
                match ev {
                    Some(ReceiverEvent::Audio { frame, .. }) => {
                        if frame.flags.is_halt() {
                            if let Some(sink) = sc.sink.as_mut() {
                                sink.flush();
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
                        if sc.last_spec != Some(spec) {
                            // Drop old sink BEFORE opening new one to avoid EBUSY on exclusive devices.
                            sc.sink = None;
                            match sink_factory.open(spec) {
                                Ok(s) => {
                                    sc.sink = Some(s);
                                    sc.last_spec = Some(spec);
                                    let _ = ev_tx.send(EngineEvent::Details {
                                        generation: sc.generation,
                                        details: TrackDetails {
                                            duration: if frame.samples_total > 0 {
                                                (frame.samples_total / u64::from(frame.sample_rate))
                                                    as u32
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
                                        },
                                    });
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

                        songcast_pcm_be_into_f32(&frame.audio, frame.bit_depth, &mut pcm_scratch);
                        if !pcm_scratch.is_empty() {
                            gain_ramp.apply(&mut pcm_scratch, spec.channels, spec.sample_rate);
                            if let Some(sink) = sc.sink.as_mut()
                                && let Err(err) = sink.write(&pcm_scratch)
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
