//! Dedicated playback engine worker for Playlist decoding and Songcast PCM reception.

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use openhome_core::types::{TrackDetails, TrackTime, TransportState, VolumeState};
use openhome_songcast::{ReceiverConfig, ReceiverEvent, SongcastReceiver, SongcastUri};
use tokio::sync::mpsc as tokio_mpsc;

use crate::decoder::DecodedStream;
use crate::format::{PcmSpec, apply_volume_in_place, songcast_pcm_be_to_f32};
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
    /// Transport state changed for `generation`.
    StateChanged {
        generation: u64,
        state: TransportState,
    },
    /// Audio format/duration details discovered for the active track or Songcast stream.
    Details {
        generation: u64,
        details: TrackDetails,
    },
    /// Playback elapsed time tick.
    Time { generation: u64, time: TrackTime },
    /// Active playlist track reached clean end-of-stream.
    TrackEnded { generation: u64 },
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
    worker: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for PlaybackEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaybackEngine").finish()
    }
}

impl PlaybackEngine {
    /// Spawn the playback worker using `sink` and the provided Tokio runtime handle.
    pub fn spawn(
        sink: Arc<dyn AudioSink>,
        runtime: tokio::runtime::Handle,
    ) -> (Self, tokio_mpsc::UnboundedReceiver<EngineEvent>) {
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let (ev_tx, ev_rx) = tokio_mpsc::unbounded_channel();
        let worker = thread::Builder::new()
            .name("openhome-player-worker".into())
            .spawn(move || worker_loop(sink, runtime, cmd_rx, ev_tx))
            .expect("spawn openhome-player-worker");
        (
            Self {
                cmd_tx,
                worker: Some(worker),
            },
            ev_rx,
        )
    }

    /// Send a non-blocking command to the playback worker.
    pub fn send(&self, cmd: EngineCommand) {
        let _ = self.cmd_tx.send(cmd);
    }

    /// Shut down the playback worker thread cleanly.
    pub fn shutdown(mut self) {
        let _ = self.cmd_tx.send(EngineCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for PlaybackEngine {
    fn drop(&mut self) {
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
}

fn worker_loop(
    sink_factory: Arc<dyn AudioSink>,
    runtime: tokio::runtime::Handle,
    cmd_rx: Receiver<EngineCommand>,
    ev_tx: tokio_mpsc::UnboundedSender<EngineEvent>,
) {
    let mut volume = VolumeState {
        volume: 50,
        mute: false,
        balance: 0,
        fade: 0,
        volume_limit: 100,
    };
    let mut track: Option<ActiveTrack> = None;
    let mut songcast: Option<ActiveSongcast> = None;

    loop {
        // When actively playing a track or Songcast stream, poll commands non-blockingly;
        // otherwise block until the next command arrives (zero idle CPU).
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
                    if let Some(sc) = songcast.take() {
                        runtime.block_on(sc.receiver.shutdown());
                    }
                    break;
                }
                EngineCommand::SetVolume(v) => {
                    volume = v;
                }
                EngineCommand::Stop => {
                    if let Some(mut t) = track.take() {
                        t.sink.flush();
                    }
                    if let Some(sc) = songcast.take() {
                        runtime.block_on(sc.receiver.shutdown());
                    }
                }
                EngineCommand::StopSongcast => {
                    if let Some(sc) = songcast.take() {
                        runtime.block_on(sc.receiver.shutdown());
                    }
                }
                EngineCommand::Pause => {
                    if let Some(t) = track.as_mut() {
                        t.paused = true;
                        t.sink.flush();
                    }
                }
                EngineCommand::Play => {
                    if let Some(t) = track.as_mut() {
                        t.paused = false;
                    }
                }
                EngineCommand::Seek(secs) => {
                    if let Some(t) = track.as_mut() {
                        t.sink.flush();
                        if let Ok(actual) = t.stream.seek(secs) {
                            t.frames_played =
                                u64::from(actual) * u64::from(t.stream.spec().sample_rate);
                            t.last_reported_sec = actual;
                            let _ = ev_tx.send(EngineEvent::Time {
                                generation: t.generation,
                                time: TrackTime {
                                    track_count: 1,
                                    duration: t.stream.duration_secs(),
                                    seconds: actual,
                                },
                            });
                        }
                    }
                }
                EngineCommand::Load {
                    generation,
                    uri,
                    paused,
                } => {
                    if let Some(sc) = songcast.take() {
                        runtime.block_on(sc.receiver.shutdown());
                    }
                    track = None;

                    let _ = ev_tx.send(EngineEvent::StateChanged {
                        generation,
                        state: TransportState::Buffering,
                    });

                    match DecodedStream::open(&uri) {
                        Ok(stream) => {
                            let spec = stream.spec();
                            match sink_factory.open(spec) {
                                Ok(sink_stream) => {
                                    let dur = stream.duration_secs();
                                    let _ = ev_tx.send(EngineEvent::Details {
                                        generation,
                                        details: TrackDetails {
                                            duration: dur,
                                            bit_rate: stream.bit_rate(),
                                            bit_depth: u32::from(spec.bits_per_sample),
                                            sample_rate: spec.sample_rate,
                                            lossless: spec.bits_per_sample >= 16,
                                            codec_name: "PCM".into(),
                                        },
                                    });
                                    let _ = ev_tx.send(EngineEvent::Time {
                                        generation,
                                        time: TrackTime {
                                            track_count: 1,
                                            duration: dur,
                                            seconds: 0,
                                        },
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
                    track = None;
                    if let Some(sc) = songcast.take() {
                        runtime.block_on(sc.receiver.shutdown());
                    }

                    match SongcastUri::parse(&uri) {
                        Ok(sc_uri) => {
                            let cfg = ReceiverConfig {
                                interface,
                                ..ReceiverConfig::default()
                            };
                            match runtime.block_on(SongcastReceiver::connect(&sc_uri, cfg)) {
                                Ok(receiver) => {
                                    songcast = Some(ActiveSongcast {
                                        generation,
                                        receiver,
                                        sink: None,
                                        last_spec: None,
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
            match active.stream.next_frame() {
                Ok(Some(mut samples)) => {
                    let spec = active.stream.spec();
                    apply_volume_in_place(&mut samples, spec.channels, &volume);
                    if let Err(err) = active.sink.write(&samples) {
                        let generation = active.generation;
                        track = None;
                        let _ = ev_tx.send(EngineEvent::Error {
                            generation,
                            message: err.to_string(),
                        });
                        continue;
                    }
                    active.frames_played += spec.frames_in(samples.len());
                    if spec.sample_rate > 0 {
                        let sec = (active.frames_played / u64::from(spec.sample_rate)) as u32;
                        if sec != active.last_reported_sec {
                            active.last_reported_sec = sec;
                            let _ = ev_tx.send(EngineEvent::Time {
                                generation: active.generation,
                                time: TrackTime {
                                    track_count: 1,
                                    duration: active.stream.duration_secs(),
                                    seconds: sec,
                                },
                            });
                        }
                    }
                }
                Ok(None) => {
                    let generation = active.generation;
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

        // Pump Songcast Receiver events with a short timeout so commands stay responsive.
        if let Some(sc) = songcast.as_mut() {
            let ev = runtime.block_on(async {
                tokio::time::timeout(Duration::from_millis(15), sc.receiver.recv()).await
            });
            match ev {
                Ok(Some(ReceiverEvent::Audio { frame, .. })) => {
                    let spec = PcmSpec::new(
                        frame.sample_rate,
                        u16::from(frame.channels.max(1)),
                        u16::from(frame.bit_depth.max(16)),
                    );
                    if sc.last_spec != Some(spec) {
                        match sink_factory.open(spec) {
                            Ok(s) => {
                                sc.sink = Some(s);
                                sc.last_spec = Some(spec);
                                let _ = ev_tx.send(EngineEvent::Details {
                                    generation: sc.generation,
                                    details: TrackDetails {
                                        duration: 0,
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
                                let _ = ev_tx.send(EngineEvent::Error {
                                    generation: sc.generation,
                                    message: err.to_string(),
                                });
                            }
                        }
                    }
                    let mut samples = songcast_pcm_be_to_f32(&frame.audio, frame.bit_depth);
                    if !samples.is_empty() {
                        apply_volume_in_place(&mut samples, spec.channels, &volume);
                        if let Some(sink) = sc.sink.as_mut()
                            && let Err(err) = sink.write(&samples)
                        {
                            let generation = sc.generation;
                            if let Some(sc_owned) = songcast.take() {
                                runtime.block_on(sc_owned.receiver.shutdown());
                            }
                            let _ = ev_tx.send(EngineEvent::Error {
                                generation,
                                message: err.to_string(),
                            });
                            continue;
                        }
                    }
                }
                Ok(Some(ReceiverEvent::Track(t))) => {
                    let _ = ev_tx.send(EngineEvent::SongcastTrack {
                        generation: sc.generation,
                        uri: t.uri,
                        metadata: t.metadata,
                    });
                }
                Ok(Some(ReceiverEvent::Metatext(m))) => {
                    let _ = ev_tx.send(EngineEvent::SongcastMetatext {
                        generation: sc.generation,
                        metatext: m.metatext,
                    });
                }
                Ok(Some(ReceiverEvent::Resynced)) => {
                    if let Some(sink) = sc.sink.as_mut() {
                        sink.flush();
                    }
                }
                Ok(None) => {
                    songcast = None;
                }
                Err(_) => {} // poll timeout -> loop back to check cmd_rx
            }
        }
    }
}
