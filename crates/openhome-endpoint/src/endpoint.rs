//! High-level [`OpenHomeEndpoint`] bridging `openhome-server` services and [`PlaybackEngine`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use openhome_audio::{AudioSink, NullSink, is_http_uri};
use openhome_core::Fault;
use openhome_core::types::{
    DeviceAttributes, InfoCounters, ProductInfo, ReceiverState, SenderStatus, Source, SourceType,
    TrackDetails, TrackEntry, TrackTime, TransportState, VolumeCharacteristics, VolumeState,
};
use openhome_server::{
    ActionResult, InfoService, MemoryRenderer, OpenHomeServer, PlaylistService, ProductService,
    ReceiverService, SenderService, ServerNotifier, SsdpConfig, TimeService, VolumeService,
};
use openhome_songcast::{SenderConfig, SongcastSender, SongcastUri, encode_sender_metadata};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::engine::{EngineCommand, EngineEvent, PlaybackEngine};
use crate::error::EndpointError;

/// Exact UPnP `ProtocolInfo` list matching the codecs/containers enabled in `symphonia`.
/// (Omits raw headerless `audio/L16` so media servers don't send unprobeable PCM.)
const PLAYER_PROTOCOL_INFO: &str = "http-get:*:audio/flac:*,\
http-get:*:audio/x-flac:*,\
http-get:*:audio/mpeg:*,\
http-get:*:audio/mp4:*,\
http-get:*:audio/x-m4a:*,\
http-get:*:audio/aac:*,\
http-get:*:audio/alac:*,\
http-get:*:audio/wav:*,\
http-get:*:audio/x-wav:*,\
http-get:*:audio/aiff:*,\
http-get:*:audio/x-aiff:*,\
http-get:*:audio/ogg:*,\
http-get:*:application/ogg:*";

/// Callback invoked when the playback engine encounters a decode, network, or sink error.
pub type ErrorCallback = Arc<dyn Fn(&str) + Send + Sync>;

/// Configuration for starting an [`OpenHomeEndpoint`].
#[derive(Clone)]
pub struct EndpointConfig {
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
    /// Local IPv4 interface for Songcast Receiver/Sender sockets.
    pub songcast_interface: Ipv4Addr,
    /// Whether to expose the OpenHome `Sender:1` service and rebroadcast Playlist audio
    /// over Songcast so other rooms can group with this endpoint as multiroom leader.
    pub enable_sender: bool,
    /// Optional explicit UDP bind address for the Songcast Sender (defaults to ephemeral unicast).
    pub songcast_sender_addr: Option<SocketAddrV4>,
    /// Whether `Playlist.Insert` is allowed to open `file://` or local filesystem paths.
    ///
    /// Defaults to `false` (only `http://` and `https://` URIs are accepted) so unauthenticated
    /// LAN callers cannot access local files or hang on special device nodes.
    pub allow_local_files: bool,
    /// Audio output sink (`NullSink`, `CaptureSink`, or `CpalSink`).
    pub sink: Arc<dyn AudioSink>,
    /// Optional callback invoked when the playback engine encounters a decode, network, or sink error.
    pub on_error: Option<ErrorCallback>,
}

impl std::fmt::Debug for EndpointConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EndpointConfig")
            .field("room", &self.room)
            .field("name", &self.name)
            .field("bind_addr", &self.bind_addr)
            .field("allow_local_files", &self.allow_local_files)
            .field("sink", &self.sink.name())
            .finish()
    }
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            room: "Main Room".into(),
            name: "OpenHome Player".into(),
            udn: None,
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            advertise_host: None,
            ssdp: SsdpConfig::disabled(),
            songcast_interface: Ipv4Addr::UNSPECIFIED,
            enable_sender: true,
            songcast_sender_addr: None,
            allow_local_files: false,
            sink: Arc::new(NullSink::realtime()),
            on_error: None,
        }
    }
}

struct EndpointCore {
    state: MemoryRenderer,
    engine: PlaybackEngine,
    generation: Arc<AtomicU64>,
    loaded_track_id: AtomicU32,
    songcast_interface: Ipv4Addr,
    sender_enabled: bool,
    allow_local_files: bool,
    on_error: Option<ErrorCallback>,
    notifier: OnceLock<ServerNotifier>,
}

impl EndpointCore {
    fn next_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn attach_notifier(&self, notifier: ServerNotifier) {
        let _ = self.notifier.set(notifier);
    }

    fn is_playlist_active(&self) -> bool {
        let idx = self.state.source_index() as usize;
        self.state
            .sources()
            .get(idx)
            .is_some_and(|s| s.source_type == SourceType::Playlist)
    }

    fn notify_playback(&self) {
        if let Some(n) = self.notifier.get() {
            n.notify_playlist();
            n.notify_info();
            n.notify_time();
        }
    }

    fn notify_receiver_and_info(&self) {
        if let Some(n) = self.notifier.get() {
            n.notify_receiver();
            n.notify_info();
        }
    }

    /// Synchronize OpenHome `Sender:1` (`Status` & `Audio`) with Playlist/Standby/Source state,
    /// matching `ohPipeline`'s `Sender::NotifyPipelineState` & `OhmSender::UpdateStatus`.
    fn sync_sender_state(&self) {
        if !self.sender_enabled {
            return;
        }
        let status = if self.state.standby() || !self.is_playlist_active() {
            SenderStatus::Disabled
        } else {
            SenderStatus::Enabled
        };
        let audio = status == SenderStatus::Enabled
            && PlaylistService::transport_state(&self.state) == TransportState::Playing;
        if self.state.status() != status || self.state.audio() != audio {
            let pres = self.state.presentation_url();
            let meta = self.state.metadata();
            let attrs = SenderService::attributes(&self.state);
            self.state.set_sender_info(pres, meta, audio, status, attrs);
            if let Some(n) = self.notifier.get() {
                n.notify_sender();
            }
        }
    }

    fn stop_engine(&self) {
        self.next_generation();
        self.loaded_track_id.store(0, Ordering::Release);
        self.engine.send(EngineCommand::Stop);
        self.sync_sender_state();
    }

    /// Start decoding the current playlist track (if any) in `PlaybackEngine`.
    fn load_current_track(&self, paused: bool) {
        let id = self.state.current_id();
        if id == 0 {
            self.stop_engine();
            return;
        }
        if let Ok(entry) = self.state.read(id) {
            if !paused {
                // Transition to Buffering synchronously so SOAP/GENA does not flicker Playing->Buffering->Playing.
                self.state.set_transport_state(TransportState::Buffering);
            }
            let generation = self.next_generation();
            self.loaded_track_id.store(id, Ordering::Release);
            if self.sender_enabled {
                self.engine.send(EngineCommand::SetTrackMetadata {
                    uri: entry.uri.clone(),
                    metadata: entry.metadata,
                });
            }
            self.engine.send(EngineCommand::Load {
                generation,
                uri: entry.uri,
                paused,
            });
            self.sync_sender_state();
        }
    }

    fn handle_engine_event(&self, ev: EngineEvent) {
        match ev {
            EngineEvent::StateChanged { generation, state } => {
                if generation != self.current_generation() {
                    return;
                }
                self.state.set_transport_state(state);
                self.sync_sender_state();
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
            EngineEvent::Time {
                generation,
                seconds,
            } => {
                if generation != self.current_generation() {
                    return;
                }
                self.state.set_time_seconds(seconds);
                if let Some(n) = self.notifier.get() {
                    n.notify_time();
                }
            }
            EngineEvent::TrackEnded { generation } => {
                if generation != self.current_generation() {
                    return;
                }
                if self.state.advance_on_track_end().is_some() {
                    self.load_current_track(false);
                } else {
                    self.loaded_track_id.store(0, Ordering::Release);
                    self.engine.send(EngineCommand::DrainAndStop);
                }
                self.notify_playback();
            }
            EngineEvent::SongcastState { generation, state } => {
                if generation != self.current_generation() {
                    return;
                }
                self.state.set_receiver_state(state);
                self.notify_receiver_and_info();
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
                self.notify_receiver_and_info();
                if let Some(n) = self.notifier.get() {
                    n.notify_time();
                }
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
            EngineEvent::Error {
                generation,
                message,
            } => {
                if generation != self.current_generation() {
                    return;
                }
                if let Some(cb) = self.on_error.as_ref() {
                    cb(&message);
                }
                self.loaded_track_id.store(0, Ordering::Release);
                let _ = PlaylistService::stop(&self.state);
                self.state.set_receiver_state(ReceiverState::Stopped);
                self.notify_playback();
                self.notify_receiver_and_info();
            }
        }
    }
}

impl ProductService for EndpointCore {
    fn product_info(&self) -> ProductInfo {
        self.state.product_info()
    }

    fn attributes(&self) -> DeviceAttributes {
        ProductService::attributes(&self.state)
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
            self.stop_engine();
        }
        self.sync_sender_state();
        Ok(())
    }

    fn standby(&self) -> bool {
        self.state.standby()
    }

    fn set_standby(&self, standby: bool) -> ActionResult<()> {
        self.state.set_standby(standby)?;
        if standby {
            self.stop_engine();
        }
        self.sync_sender_state();
        Ok(())
    }

    fn source_xml_change_count(&self) -> u32 {
        self.state.source_xml_change_count()
    }
}

impl PlaylistService for EndpointCore {
    fn play(&self) -> ActionResult<()> {
        let was_paused = PlaylistService::transport_state(&self.state) == TransportState::Paused
            && self.is_playlist_active();
        PlaylistService::play(&self.state)?;
        let current_id = self.state.current_id();
        if was_paused
            && current_id != 0
            && self.loaded_track_id.load(Ordering::Acquire) == current_id
        {
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
        self.stop_engine();
        Ok(())
    }

    fn next(&self) -> ActionResult<()> {
        self.state.next()?;
        match PlaylistService::transport_state(&self.state) {
            TransportState::Playing | TransportState::Buffering => self.load_current_track(false),
            TransportState::Stopped => self.stop_engine(),
            TransportState::Paused => {
                // Invalidate loaded_track_id so a subsequent Play loads the newly selected track.
                self.stop_engine();
            }
        }
        Ok(())
    }

    fn previous(&self) -> ActionResult<()> {
        let prev_id = self.state.current_id();
        self.state.previous()?;
        let new_id = self.state.current_id();
        match PlaylistService::transport_state(&self.state) {
            TransportState::Playing | TransportState::Buffering => {
                if new_id != 0
                    && new_id == prev_id
                    && self.loaded_track_id.load(Ordering::Acquire) == new_id
                {
                    // At track 0 without repeat: restart track from 0s without re-downloading.
                    self.engine.send(EngineCommand::Seek(0));
                } else {
                    self.load_current_track(false);
                }
            }
            TransportState::Stopped => {
                if new_id != prev_id {
                    self.stop_engine();
                }
            }
            TransportState::Paused => {
                if new_id != prev_id {
                    self.stop_engine();
                } else if new_id != 0 && self.loaded_track_id.load(Ordering::Acquire) == new_id {
                    // Restarting track 0 while paused resets elapsed time to 0s; seek decoder too.
                    self.engine.send(EngineCommand::Seek(0));
                }
            }
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
        if !self.allow_local_files && !is_http_uri(&uri) {
            return Err(Fault::invalid_args(
                "Only http:// and https:// track URIs are allowed",
            ));
        }
        self.state.insert(after_id, uri, metadata)
    }

    fn delete_id(&self, id: u32) -> ActionResult<()> {
        let was_current = self.state.current_id() == id;
        self.state.delete_id(id)?;
        if was_current {
            if matches!(
                PlaylistService::transport_state(&self.state),
                TransportState::Playing | TransportState::Buffering
            ) && self.state.current_id() != 0
            {
                self.load_current_track(false);
            } else {
                self.stop_engine();
            }
        }
        Ok(())
    }

    fn delete_all(&self) -> ActionResult<()> {
        self.state.delete_all()?;
        self.stop_engine();
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

impl InfoService for EndpointCore {
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

impl TimeService for EndpointCore {
    fn time(&self) -> TrackTime {
        self.state.time()
    }
}

impl VolumeService for EndpointCore {
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

impl ReceiverService for EndpointCore {
    fn play(&self) -> ActionResult<()> {
        ReceiverService::play(&self.state)?;
        let (uri, _) = self.state.sender();
        self.loaded_track_id.store(0, Ordering::Release);
        if !uri.is_empty() {
            // Enter Waiting until the first Songcast audio packet arrives.
            self.state.set_receiver_state(ReceiverState::Waiting);
            let generation = self.next_generation();
            self.engine.send(EngineCommand::StartSongcast {
                generation,
                uri,
                interface: self.songcast_interface,
            });
        } else {
            // Even without a sender URI, switching to Receiver stops any active Playlist audio
            // and must not leave ReceiverState stuck in Playing.
            self.state.set_receiver_state(ReceiverState::Stopped);
            self.stop_engine();
        }
        Ok(())
    }

    fn stop(&self) -> ActionResult<()> {
        ReceiverService::stop(&self.state)?;
        self.next_generation();
        self.engine.send(EngineCommand::StopSongcast);
        Ok(())
    }

    fn set_sender(&self, uri: String, metadata: String) -> ActionResult<()> {
        if !uri.is_empty() {
            SongcastUri::parse(&uri)
                .map_err(|e| Fault::invalid_args(format!("Invalid Songcast sender URI: {e}")))?;
        }
        let was_active = ReceiverService::transport_state(&self.state) != ReceiverState::Stopped;
        self.state.set_sender(uri.clone(), metadata)?;
        if was_active {
            if uri.is_empty() {
                self.next_generation();
                self.state.set_receiver_state(ReceiverState::Stopped);
                self.engine.send(EngineCommand::StopSongcast);
            } else {
                self.state.set_receiver_state(ReceiverState::Waiting);
                let generation = self.next_generation();
                self.engine.send(EngineCommand::StartSongcast {
                    generation,
                    uri,
                    interface: self.songcast_interface,
                });
            }
        }
        Ok(())
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

impl SenderService for EndpointCore {
    fn presentation_url(&self) -> String {
        self.state.presentation_url()
    }

    fn metadata(&self) -> String {
        self.state.metadata()
    }

    fn audio(&self) -> bool {
        self.state.audio()
    }

    fn status(&self) -> SenderStatus {
        self.state.status()
    }

    fn attributes(&self) -> String {
        SenderService::attributes(&self.state)
    }
}

fn default_outbound_ipv4() -> Ipv4Addr {
    if let Ok(sock) = UdpSocket::bind("0.0.0.0:0")
        && sock.connect("192.0.2.1:80").is_ok()
        && let Ok(SocketAddr::V4(v4)) = sock.local_addr()
        && !v4.ip().is_unspecified()
    {
        return *v4.ip();
    }
    Ipv4Addr::LOCALHOST
}

/// Running self-contained OpenHome Media + Songcast endpoint.
pub struct OpenHomeEndpoint {
    server: Option<OpenHomeServer>,
    local_addr: SocketAddr,
    description_url: String,
    sender_uri: Option<String>,
    sender: Option<Arc<SongcastSender>>,
    core: Arc<EndpointCore>,
    pump_stop_tx: watch::Sender<bool>,
    pump_task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for OpenHomeEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenHomeEndpoint")
            .field("local_addr", &self.local_addr)
            .field("description_url", &self.description_url)
            .field("sender_uri", &self.sender_uri)
            .finish()
    }
}

impl OpenHomeEndpoint {
    /// Start a new [`OpenHomeEndpoint`] with the given [`EndpointConfig`].
    pub async fn start(config: EndpointConfig) -> Result<Self, EndpointError> {
        let runtime = tokio::runtime::Handle::current();
        let state = MemoryRenderer::new(&config.room, &config.name);

        let sender = if config.enable_sender {
            let bind_addr = config
                .songcast_sender_addr
                .unwrap_or_else(|| SocketAddrV4::new(config.songcast_interface, 0));
            let advertise_ip = config
                .advertise_host
                .as_deref()
                .and_then(|h| h.parse::<Ipv4Addr>().ok())
                .or_else(|| match config.bind_addr.ip() {
                    IpAddr::V4(ip) if !ip.is_unspecified() => Some(ip),
                    _ => None,
                })
                .unwrap_or_else(default_outbound_ipv4);
            let sc_sender = SongcastSender::bind(SenderConfig {
                bind_addr,
                advertise_ip: Some(advertise_ip),
                interface: config.songcast_interface,
                ..SenderConfig::default()
            })
            .await?;
            let sc_uri = sc_sender.uri();
            let uri_str = sc_uri.to_string();
            let meta = encode_sender_metadata(
                &config.room,
                &uri_str,
                matches!(sc_uri, SongcastUri::Multicast(_)),
                None,
            );
            state.set_sender_info("", meta, false, SenderStatus::Enabled, "");
            Some(Arc::new(sc_sender))
        } else {
            None
        };
        let sender_uri = sender.as_ref().map(|s| s.uri().to_string());

        state.set_attributes(DeviceAttributes {
            info: true,
            time: true,
            volume: true,
            sender: sender.is_some(),
            receiver: true,
            credentials: false,
        });
        state.set_playlist_protocol_info(PLAYER_PROTOCOL_INFO);

        let generation = Arc::new(AtomicU64::new(0));
        let (engine, mut ev_rx) = PlaybackEngine::spawn_with_sender(
            config.sink,
            runtime,
            Arc::clone(&generation),
            config.allow_local_files,
            state.characteristics(),
            sender.clone(),
        );
        engine.send(EngineCommand::SetVolume(state.state()));

        let core = Arc::new(EndpointCore {
            state,
            engine,
            generation,
            loaded_track_id: AtomicU32::new(0),
            songcast_interface: config.songcast_interface,
            sender_enabled: sender.is_some(),
            allow_local_files: config.allow_local_files,
            on_error: config.on_error,
            notifier: OnceLock::new(),
        });

        let mut builder = OpenHomeServer::builder(Arc::clone(&core) as Arc<dyn ProductService>)
            .playlist(Arc::clone(&core) as Arc<dyn PlaylistService>)
            .info(Arc::clone(&core) as Arc<dyn InfoService>)
            .time(Arc::clone(&core) as Arc<dyn TimeService>)
            .volume(Arc::clone(&core) as Arc<dyn VolumeService>)
            .receiver(Arc::clone(&core) as Arc<dyn ReceiverService>)
            .bind_addr(config.bind_addr)
            .ssdp(config.ssdp);

        if sender.is_some() {
            builder = builder.sender(Arc::clone(&core) as Arc<dyn SenderService>);
        }
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
            sender_uri,
            sender,
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

    /// Live Songcast Sender URI (`ohu://...` or `ohm://...`) if `enable_sender` is enabled.
    pub fn sender_uri(&self) -> Option<&str> {
        self.sender_uri.as_deref()
    }

    /// Gracefully shut down the HTTP/SSDP server, event pump, audio playback worker, and Songcast sender.
    pub async fn shutdown(mut self) {
        if let Some(server) = self.server.take() {
            server.shutdown().await;
        }
        let _ = self.pump_stop_tx.send(true);
        if let Some(task) = self.pump_task.take() {
            let _ = task.await;
        }
        self.core.engine.shutdown_async().await;
        if let Some(sender) = self.sender.take()
            && let Ok(owned) = Arc::try_unwrap(sender)
        {
            owned.shutdown().await;
        }
    }
}

impl Drop for OpenHomeEndpoint {
    fn drop(&mut self) {
        self.core.engine.send(EngineCommand::Shutdown);
        let _ = self.pump_stop_tx.send(true);
    }
}
