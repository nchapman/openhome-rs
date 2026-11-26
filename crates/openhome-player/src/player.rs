//! High-level [`OpenHomePlayer`] bridging `openhome-server` services and [`PlaybackEngine`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use openhome_core::types::{
    DeviceAttributes, InfoCounters, ProductInfo, ReceiverState, Source, TrackDetails, TrackEntry,
    TrackTime, TransportState, VolumeCharacteristics, VolumeState,
};
use openhome_server::{
    ActionResult, InfoService, MemoryRenderer, OpenHomeServer, PlaylistService, ProductService,
    ReceiverService, ServerNotifier, SsdpConfig, TimeService, VolumeService,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::engine::{EngineCommand, EngineEvent, PlaybackEngine};
use crate::error::PlayerError;
use crate::sink::{AudioSink, NullSink};

/// Configuration for starting an [`OpenHomePlayer`].
#[derive(Clone)]
pub struct PlayerConfig {
    /// OpenHome room name (e.g. `"Living Room"`).
    pub room: String,
    /// OpenHome product/device name (e.g. `"OpenHome Player"`).
    pub name: String,
    /// Optional explicit UDN (`uuid:...`). If `None`, a stable UUID is generated.
    pub udn: Option<String>,
    /// HTTP bind address (e.g. `0.0.0.0:0` or `127.0.0.1:0`).
    pub bind_addr: SocketAddr,
    /// Optional host/IP advertised in SSDP `LOCATION` and `description.xml`.
    pub advertise_host: Option<String>,
    /// SSDP discovery configuration.
    pub ssdp: SsdpConfig,
    /// Local IPv4 interface for Songcast Receiver sockets.
    pub songcast_interface: Ipv4Addr,
    /// Audio output sink (`NullSink`, `CaptureSink`, or `CpalSink`).
    pub sink: Arc<dyn AudioSink>,
}

impl std::fmt::Debug for PlayerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlayerConfig")
            .field("room", &self.room)
            .field("name", &self.name)
            .field("bind_addr", &self.bind_addr)
            .field("sink", &self.sink.name())
            .finish()
    }
}

impl Default for PlayerConfig {
    fn default() -> Self {
        Self {
            room: "Main Room".into(),
            name: "OpenHome Player".into(),
            udn: None,
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            advertise_host: None,
            ssdp: SsdpConfig::disabled(),
            songcast_interface: Ipv4Addr::UNSPECIFIED,
            sink: Arc::new(NullSink::realtime()),
        }
    }
}

struct PlayerCore {
    state: MemoryRenderer,
    engine: PlaybackEngine,
    generation: AtomicU64,
    songcast_interface: Ipv4Addr,
    notifier: Mutex<Option<ServerNotifier>>,
}

impl PlayerCore {
    fn next_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    fn attach_notifier(&self, notifier: ServerNotifier) {
        *self.notifier.lock().unwrap() = Some(notifier);
    }

    fn notify_playback(&self) {
        if let Some(n) = self.notifier.lock().unwrap().as_ref() {
            n.notify_playlist();
            n.notify_info();
            n.notify_time();
        }
    }

    fn notify_receiver_and_info(&self) {
        if let Some(n) = self.notifier.lock().unwrap().as_ref() {
            n.notify_receiver();
            n.notify_info();
        }
    }

    /// Start decoding the current playlist track (if any) in `PlaybackEngine`.
    fn load_current_track(&self, paused: bool) {
        let id = self.state.current_id();
        if id == 0 {
            self.engine.send(EngineCommand::Stop);
            return;
        }
        if let Ok(entry) = self.state.read(id) {
            let generation = self.next_generation();
            self.engine.send(EngineCommand::Load {
                generation,
                uri: entry.uri,
                paused,
            });
        }
    }

    fn handle_engine_event(&self, ev: EngineEvent) {
        match ev {
            EngineEvent::StateChanged { generation, state } => {
                if generation != self.current_generation() {
                    return;
                }
                self.state.set_transport_state(state);
                self.notify_playback();
            }
            EngineEvent::Details {
                generation,
                details,
            } => {
                if generation != self.current_generation() {
                    return;
                }
                self.state.set_track_details(details);
                self.notify_playback();
            }
            EngineEvent::Time { generation, time } => {
                if generation != self.current_generation() {
                    return;
                }
                self.state.set_time_seconds(time.seconds);
                if let Some(n) = self.notifier.lock().unwrap().as_ref() {
                    n.notify_time();
                }
            }
            EngineEvent::TrackEnded { generation } => {
                if generation != self.current_generation() {
                    return;
                }
                // Advance to next track if one exists (respecting Repeat/Shuffle); otherwise stop.
                let prev_id = self.state.current_id();
                let ids = self.state.id_array().1;
                let can_advance = if let Some(pos) = ids.iter().position(|&id| id == prev_id) {
                    pos + 1 < ids.len() || self.state.repeat()
                } else {
                    false
                };

                if can_advance && self.state.next().is_ok() {
                    self.load_current_track(false);
                } else {
                    let _ = PlaylistService::stop(&self.state);
                    self.engine.send(EngineCommand::Stop);
                }
                self.notify_playback();
            }
            EngineEvent::SongcastTrack {
                generation,
                uri,
                metadata,
            } => {
                if generation != self.current_generation() {
                    return;
                }
                self.state.set_info_track(uri, metadata);
                self.state.set_receiver_state(ReceiverState::Playing);
                self.notify_receiver_and_info();
            }
            EngineEvent::SongcastMetatext {
                generation,
                metatext,
            } => {
                if generation != self.current_generation() {
                    return;
                }
                self.state.set_metatext(metatext);
                self.notify_receiver_and_info();
            }
            EngineEvent::Error { generation, .. } => {
                if generation != self.current_generation() {
                    return;
                }
                let _ = PlaylistService::stop(&self.state);
                self.state.set_receiver_state(ReceiverState::Stopped);
                self.notify_playback();
                self.notify_receiver_and_info();
            }
        }
    }
}

impl ProductService for PlayerCore {
    fn product_info(&self) -> ProductInfo {
        self.state.product_info()
    }

    fn attributes(&self) -> DeviceAttributes {
        self.state.attributes()
    }

    fn sources(&self) -> Vec<Source> {
        self.state.sources()
    }

    fn source_index(&self) -> u32 {
        self.state.source_index()
    }

    fn set_source_index(&self, index: u32) -> ActionResult<()> {
        let prev = self.state.source_index();
        self.state.set_source_index(index)?;
        if index != prev {
            self.next_generation();
            self.engine.send(EngineCommand::Stop);
        }
        Ok(())
    }

    fn standby(&self) -> bool {
        self.state.standby()
    }

    fn set_standby(&self, standby: bool) -> ActionResult<()> {
        self.state.set_standby(standby)?;
        if standby {
            self.next_generation();
            self.engine.send(EngineCommand::Stop);
        }
        Ok(())
    }

    fn source_xml_change_count(&self) -> u32 {
        self.state.source_xml_change_count()
    }
}

impl PlaylistService for PlayerCore {
    fn play(&self) -> ActionResult<()> {
        let was_paused = PlaylistService::transport_state(&self.state) == TransportState::Paused
            && self.state.source_index() == 0;
        PlaylistService::play(&self.state)?;
        if was_paused {
            self.engine.send(EngineCommand::Play);
        } else {
            self.load_current_track(false);
        }
        Ok(())
    }

    fn pause(&self) -> ActionResult<()> {
        self.state.pause()?;
        self.engine.send(EngineCommand::Pause);
        Ok(())
    }

    fn stop(&self) -> ActionResult<()> {
        PlaylistService::stop(&self.state)?;
        self.next_generation();
        self.engine.send(EngineCommand::Stop);
        Ok(())
    }

    fn next(&self) -> ActionResult<()> {
        self.state.next()?;
        let paused = PlaylistService::transport_state(&self.state) != TransportState::Playing;
        if !paused {
            self.load_current_track(false);
        }
        Ok(())
    }

    fn previous(&self) -> ActionResult<()> {
        self.state.previous()?;
        let paused = PlaylistService::transport_state(&self.state) != TransportState::Playing;
        if !paused {
            self.load_current_track(false);
        }
        Ok(())
    }

    fn repeat(&self) -> bool {
        self.state.repeat()
    }

    fn set_repeat(&self, value: bool) -> ActionResult<()> {
        self.state.set_repeat(value)
    }

    fn shuffle(&self) -> bool {
        self.state.shuffle()
    }

    fn set_shuffle(&self, value: bool) -> ActionResult<()> {
        self.state.set_shuffle(value)
    }

    fn seek_second_absolute(&self, seconds: u32) -> ActionResult<()> {
        self.state.seek_second_absolute(seconds)?;
        self.engine.send(EngineCommand::Seek(seconds));
        Ok(())
    }

    fn seek_second_relative(&self, delta_seconds: i32) -> ActionResult<()> {
        self.state.seek_second_relative(delta_seconds)?;
        let target = self.state.time().seconds;
        self.engine.send(EngineCommand::Seek(target));
        Ok(())
    }

    fn seek_id(&self, id: u32) -> ActionResult<()> {
        self.state.seek_id(id)?;
        self.load_current_track(false);
        Ok(())
    }

    fn seek_index(&self, index: u32) -> ActionResult<()> {
        self.state.seek_index(index)?;
        self.load_current_track(false);
        Ok(())
    }

    fn transport_state(&self) -> TransportState {
        PlaylistService::transport_state(&self.state)
    }

    fn current_id(&self) -> u32 {
        self.state.current_id()
    }

    fn read(&self, id: u32) -> ActionResult<TrackEntry> {
        self.state.read(id)
    }

    fn read_list(&self, ids: &[u32]) -> Vec<TrackEntry> {
        self.state.read_list(ids)
    }

    fn insert(&self, after_id: u32, uri: String, metadata: String) -> ActionResult<u32> {
        self.state.insert(after_id, uri, metadata)
    }

    fn delete_id(&self, id: u32) -> ActionResult<()> {
        let was_current = self.state.current_id() == id;
        self.state.delete_id(id)?;
        if was_current {
            self.next_generation();
            self.engine.send(EngineCommand::Stop);
        }
        Ok(())
    }

    fn delete_all(&self) -> ActionResult<()> {
        self.state.delete_all()?;
        self.next_generation();
        self.engine.send(EngineCommand::Stop);
        Ok(())
    }

    fn tracks_max(&self) -> u32 {
        self.state.tracks_max()
    }

    fn id_array(&self) -> (u32, Vec<u32>) {
        self.state.id_array()
    }

    fn id_array_changed(&self, token: u32) -> bool {
        self.state.id_array_changed(token)
    }

    fn protocol_info(&self) -> String {
        PlaylistService::protocol_info(&self.state)
    }
}

impl InfoService for PlayerCore {
    fn counters(&self) -> InfoCounters {
        self.state.counters()
    }

    fn track(&self) -> (String, String) {
        self.state.track()
    }

    fn details(&self) -> TrackDetails {
        self.state.details()
    }

    fn metatext(&self) -> String {
        self.state.metatext()
    }
}

impl TimeService for PlayerCore {
    fn time(&self) -> TrackTime {
        self.state.time()
    }
}

impl VolumeService for PlayerCore {
    fn characteristics(&self) -> VolumeCharacteristics {
        self.state.characteristics()
    }

    fn state(&self) -> VolumeState {
        self.state.state()
    }

    fn set_volume(&self, value: u32) -> ActionResult<()> {
        self.state.set_volume(value)?;
        self.engine
            .send(EngineCommand::SetVolume(self.state.state()));
        Ok(())
    }

    fn volume_inc(&self) -> ActionResult<()> {
        self.state.volume_inc()?;
        self.engine
            .send(EngineCommand::SetVolume(self.state.state()));
        Ok(())
    }

    fn volume_dec(&self) -> ActionResult<()> {
        self.state.volume_dec()?;
        self.engine
            .send(EngineCommand::SetVolume(self.state.state()));
        Ok(())
    }

    fn set_mute(&self, mute: bool) -> ActionResult<()> {
        self.state.set_mute(mute)?;
        self.engine
            .send(EngineCommand::SetVolume(self.state.state()));
        Ok(())
    }

    fn set_balance(&self, value: i32) -> ActionResult<()> {
        self.state.set_balance(value)?;
        self.engine
            .send(EngineCommand::SetVolume(self.state.state()));
        Ok(())
    }

    fn set_fade(&self, value: i32) -> ActionResult<()> {
        self.state.set_fade(value)?;
        self.engine
            .send(EngineCommand::SetVolume(self.state.state()));
        Ok(())
    }
}

impl ReceiverService for PlayerCore {
    fn play(&self) -> ActionResult<()> {
        self.state.play_receiver()?;
        let (uri, _) = self.state.sender();
        if !uri.is_empty() {
            let generation = self.next_generation();
            self.engine.send(EngineCommand::StartSongcast {
                generation,
                uri,
                interface: self.songcast_interface,
            });
        }
        Ok(())
    }

    fn stop(&self) -> ActionResult<()> {
        self.state.stop_receiver()?;
        self.next_generation();
        self.engine.send(EngineCommand::StopSongcast);
        Ok(())
    }

    fn set_sender(&self, uri: String, metadata: String) -> ActionResult<()> {
        self.state.set_sender(uri, metadata)
    }

    fn sender(&self) -> (String, String) {
        self.state.sender()
    }

    fn protocol_info(&self) -> String {
        ReceiverService::protocol_info(&self.state)
    }

    fn transport_state(&self) -> ReceiverState {
        ReceiverService::transport_state(&self.state)
    }
}

trait ReceiverStateExt {
    fn play_receiver(&self) -> ActionResult<()>;
    fn stop_receiver(&self) -> ActionResult<()>;
}

impl ReceiverStateExt for MemoryRenderer {
    fn play_receiver(&self) -> ActionResult<()> {
        ReceiverService::play(self)
    }

    fn stop_receiver(&self) -> ActionResult<()> {
        ReceiverService::stop(self)
    }
}

/// Running self-contained OpenHome Media + Songcast player.
pub struct OpenHomePlayer {
    server: Option<OpenHomeServer>,
    local_addr: SocketAddr,
    description_url: String,
    core: Arc<PlayerCore>,
    pump_stop_tx: watch::Sender<bool>,
    pump_task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for OpenHomePlayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenHomePlayer")
            .field("local_addr", &self.local_addr)
            .field("description_url", &self.description_url)
            .finish()
    }
}

impl OpenHomePlayer {
    /// Start a new [`OpenHomePlayer`] with the given [`PlayerConfig`].
    pub async fn start(config: PlayerConfig) -> Result<Self, PlayerError> {
        let runtime = tokio::runtime::Handle::current();
        let (engine, mut ev_rx) = PlaybackEngine::spawn(config.sink, runtime);
        let state = MemoryRenderer::new(&config.room, &config.name);
        engine.send(EngineCommand::SetVolume(state.state()));

        let core = Arc::new(PlayerCore {
            state,
            engine,
            generation: AtomicU64::new(0),
            songcast_interface: config.songcast_interface,
            notifier: Mutex::new(None),
        });

        let mut builder = OpenHomeServer::builder(Arc::clone(&core) as Arc<dyn ProductService>)
            .playlist(Arc::clone(&core) as Arc<dyn PlaylistService>)
            .info(Arc::clone(&core) as Arc<dyn InfoService>)
            .time(Arc::clone(&core) as Arc<dyn TimeService>)
            .volume(Arc::clone(&core) as Arc<dyn VolumeService>)
            .receiver(Arc::clone(&core) as Arc<dyn ReceiverService>)
            .bind_addr(config.bind_addr)
            .ssdp(config.ssdp);

        if let Some(udn) = config.udn {
            builder = builder.udn(udn);
        }
        if let Some(host) = config.advertise_host {
            builder = builder.advertise_host(host);
        }

        let server = builder.start().await?;
        core.attach_notifier(server.notifier());

        let (pump_stop_tx, mut pump_stop_rx) = watch::channel(false);
        let pump_core = Arc::clone(&core);
        let pump_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    res = pump_stop_rx.changed() => {
                        if res.is_err() || *pump_stop_rx.borrow() {
                            break;
                        }
                    }
                    ev = ev_rx.recv() => {
                        let Some(ev) = ev else { break };
                        pump_core.handle_engine_event(ev);
                    }
                }
            }
        });

        let local_addr = server.local_addr();
        let description_url = server.description_url().to_string();

        Ok(Self {
            server: Some(server),
            local_addr,
            description_url,
            core,
            pump_stop_tx,
            pump_task: Some(pump_task),
        })
    }

    /// HTTP socket address the OpenHome server is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Full `description.xml` URL advertised to OpenHome control points.
    pub fn description_url(&self) -> &str {
        &self.description_url
    }

    /// Gracefully shut down the HTTP/SSDP server, event pump, and audio playback worker.
    pub async fn shutdown(mut self) {
        self.core.engine.send(EngineCommand::Shutdown);
        let _ = self.pump_stop_tx.send(true);
        if let Some(task) = self.pump_task.take() {
            let _ = task.await;
        }
        if let Some(server) = self.server.take() {
            server.shutdown().await;
        }
    }
}

impl Drop for OpenHomePlayer {
    fn drop(&mut self) {
        self.core.engine.send(EngineCommand::Shutdown);
        let _ = self.pump_stop_tx.send(true);
    }
}
