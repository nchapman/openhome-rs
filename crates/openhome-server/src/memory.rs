//! Thread-safe in-memory OpenHome renderer backend ([`MemoryRenderer`]).
//!
//! Implements all seven OpenHome service traits (`Product:1`, `Playlist:1`,
//! `Info:1`, `Time:1`, `Volume:1`, `Receiver:1`, `Sender:1`) with faithful
//! OpenHome state-machine semantics and control-point compatibility rules.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use openhome_core::{
    DeviceAttributes, Fault, InfoCounters, ProductInfo, ReceiverState, SenderStatus, Source,
    SourceType, TrackDetails, TrackEntry, TrackTime, TransportState, VolumeCharacteristics,
    VolumeState,
};

use crate::server::{OpenHomeServer, OpenHomeServerBuilder};
use crate::service::{
    ActionResult, InfoService, PlaylistService, ProductService, ReceiverService, SenderService,
    TimeService, VolumeService,
};

const DEFAULT_PLAYLIST_PROTOCOL_INFO: &str = "http-get:*:audio/flac:*,\
http-get:*:audio/x-flac:*,\
http-get:*:audio/mpeg:*,\
http-get:*:audio/mp4:*,\
http-get:*:audio/aac:*,\
http-get:*:audio/wav:*,\
http-get:*:audio/L16:*,\
http-get:*:audio/ogg:*";

const DEFAULT_RECEIVER_PROTOCOL_INFO: &str = "ohz:*:*:*,ohm:*:*:*,ohu:*:*:*";

const MAX_URI_BYTES: usize = 8 * 1024;
const MAX_METADATA_BYTES: usize = 64 * 1024;
const MAX_PLAYLIST_TOTAL_BYTES: usize = 64 * 1024 * 1024;
const MAX_READ_LIST_IDS: usize = 1024;

struct Inner {
    product_info: ProductInfo,
    attributes: DeviceAttributes,
    sources: Vec<Source>,
    source_index: u32,
    source_xml_change_count: u32,
    standby: bool,

    // Playlist
    tracks: Vec<TrackEntry>,
    total_track_bytes: usize,
    next_track_id: u32,
    id_array_token: u32,
    tracks_max: u32,
    current_id: u32,
    transport_state: TransportState,
    repeat: bool,
    shuffle: bool,
    playlist_protocol_info: String,

    // Info & Time
    counters: InfoCounters,
    current_uri: String,
    current_metadata: String,
    details: TrackDetails,
    metatext: String,
    time: TrackTime,

    // Volume
    volume_chars: VolumeCharacteristics,
    volume_state: VolumeState,

    // Receiver (Songcast)
    receiver_uri: String,
    receiver_metadata: String,
    receiver_state: ReceiverState,
    receiver_protocol_info: String,

    // Sender (Songcast)
    sender_presentation_url: String,
    sender_metadata: String,
    sender_audio: bool,
    sender_status: SenderStatus,
    sender_attributes: String,
}

impl Inner {
    fn is_source_type_active(&self, ty: &SourceType) -> bool {
        self.sources
            .get(self.source_index as usize)
            .is_some_and(|s| &s.source_type == ty)
    }

    fn select_source_type(&mut self, ty: &SourceType) -> bool {
        if let Some(idx) = self.sources.iter().position(|s| &s.source_type == ty) {
            self.set_source_index_inner(idx as u32);
            true
        } else {
            false
        }
    }

    fn set_source_index_inner(&mut self, idx: u32) {
        if self.source_index != idx {
            // Stop previous active source transport cleanly.
            if self.is_source_type_active(&SourceType::Playlist) {
                self.transport_state = TransportState::Stopped;
            }
            if self.is_source_type_active(&SourceType::Receiver) {
                self.receiver_state = ReceiverState::Stopped;
            }
            self.source_index = idx;
            self.standby = false;
        }
    }

    fn require_playlist_active(&self) -> ActionResult<()> {
        if self.is_source_type_active(&SourceType::Playlist) {
            Ok(())
        } else {
            Err(Fault::conflict("Playlist source is not active"))
        }
    }

    fn select_track_by_idx(&mut self, pos: usize) {
        let entry = &self.tracks[pos];
        self.current_id = entry.id;
        self.current_uri = entry.uri.clone();
        self.current_metadata = entry.metadata.clone();
        self.counters.track_count = self.counters.track_count.wrapping_add(1);
        self.time.track_count = self.counters.track_count;
        self.time.seconds = 0;
    }

    fn clear_current_track(&mut self) {
        self.current_id = 0;
        self.current_uri.clear();
        self.current_metadata.clear();
        self.time.seconds = 0;
    }
}

/// Complete in-memory OpenHome renderer implementing all 7 OpenHome service traits.
#[derive(Clone)]
pub struct MemoryRenderer {
    inner: Arc<RwLock<Inner>>,
}

impl MemoryRenderer {
    /// Create a new renderer with `Playlist` and `Receiver` (Songcast) sources enabled.
    pub fn new(room: impl Into<String>, name: impl Into<String>) -> Self {
        let room = room.into();
        let name = name.into();
        let volume_chars = VolumeCharacteristics::default();
        let volume_state = VolumeState {
            volume: 50,
            mute: false,
            balance: 0,
            fade: 0,
            volume_limit: volume_chars.volume_max,
        };

        let inner = Inner {
            product_info: ProductInfo {
                manufacturer_name: "openhome-rs".into(),
                manufacturer_info: "Pure-Rust OpenHome Media implementation".into(),
                manufacturer_url: "https://github.com/nchapman/openhome-rs".into(),
                manufacturer_image_uri: String::new(),
                model_name: "OpenHome RS".into(),
                model_info: "Pure-Rust OpenHome Renderer".into(),
                model_url: "https://github.com/nchapman/openhome-rs".into(),
                model_image_uri: String::new(),
                product_room: room,
                product_name: name,
                product_info: "OpenHome Renderer".into(),
                product_url: String::new(),
                product_image_uri: String::new(),
            },
            attributes: DeviceAttributes {
                info: true,
                time: true,
                volume: true,
                sender: true,
                receiver: true,
                credentials: false,
            },
            sources: vec![
                Source {
                    name: "Playlist".into(),
                    source_type: SourceType::Playlist,
                    visible: true,
                    system_name: "Playlist".into(),
                },
                Source {
                    name: "Songcast".into(),
                    source_type: SourceType::Receiver,
                    // Visible = true for Kazoo compatibility (see docs/oracle.md).
                    visible: true,
                    system_name: "Receiver".into(),
                },
            ],
            source_index: 0,
            source_xml_change_count: 0,
            standby: false,
            tracks: Vec::new(),
            total_track_bytes: 0,
            next_track_id: 1,
            id_array_token: 1,
            tracks_max: 16_384,
            current_id: 0,
            transport_state: TransportState::Stopped,
            repeat: false,
            shuffle: false,
            playlist_protocol_info: DEFAULT_PLAYLIST_PROTOCOL_INFO.into(),
            counters: InfoCounters::default(),
            current_uri: String::new(),
            current_metadata: String::new(),
            details: TrackDetails::default(),
            metatext: String::new(),
            time: TrackTime::default(),
            volume_chars,
            volume_state,
            receiver_uri: String::new(),
            receiver_metadata: String::new(),
            receiver_state: ReceiverState::Stopped,
            receiver_protocol_info: DEFAULT_RECEIVER_PROTOCOL_INFO.into(),
            sender_presentation_url: String::new(),
            sender_metadata: String::new(),
            sender_audio: true,
            sender_status: SenderStatus::Enabled,
            sender_attributes: String::new(),
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    /// Build an [`OpenHomeServerBuilder`] wired with all 7 services backed by this renderer.
    pub fn into_server_builder(&self) -> OpenHomeServerBuilder {
        let arc = Arc::new(self.clone());
        OpenHomeServer::builder(arc.clone())
            .playlist(arc.clone())
            .info(arc.clone())
            .time(arc.clone())
            .volume(arc.clone())
            .receiver(arc.clone())
            .sender(arc)
    }

    /// Customize `TracksMax` capacity (useful for testing `801 Playlist full`).
    pub fn set_tracks_max(&self, tracks_max: u32) {
        self.inner.write().unwrap().tracks_max = tracks_max;
    }

    /// Customize `VolumeCharacteristics` (e.g. enabling balance/fade support).
    pub fn set_volume_characteristics(&self, chars: VolumeCharacteristics) {
        let mut inner = self.inner.write().unwrap();
        inner.volume_chars = chars;
        inner.volume_state.volume_limit = inner.volume_state.volume_limit.min(chars.volume_max);
        inner.volume_state.volume = inner
            .volume_state
            .volume
            .min(inner.volume_state.volume_limit);
    }

    /// Update current track stream details (increments `DetailsCount`).
    pub fn set_track_details(&self, details: TrackDetails) {
        let mut inner = self.inner.write().unwrap();
        inner.time.duration = details.duration;
        inner.details = details;
        inner.counters.details_count = inner.counters.details_count.wrapping_add(1);
    }

    /// Update current radio/stream metatext (increments `MetatextCount`).
    pub fn set_metatext(&self, metatext: impl Into<String>) {
        let mut inner = self.inner.write().unwrap();
        inner.metatext = metatext.into();
        inner.counters.metatext_count = inner.counters.metatext_count.wrapping_add(1);
    }

    /// Update playback progress seconds (`Time.Seconds`).
    pub fn set_time_seconds(&self, seconds: u32) {
        self.inner.write().unwrap().time.seconds = seconds;
    }

    /// Update Songcast `Sender` state.
    pub fn set_sender_info(
        &self,
        presentation_url: impl Into<String>,
        metadata: impl Into<String>,
        audio: bool,
        status: SenderStatus,
        attributes: impl Into<String>,
    ) {
        let mut inner = self.inner.write().unwrap();
        inner.sender_presentation_url = presentation_url.into();
        inner.sender_metadata = metadata.into();
        inner.sender_audio = audio;
        inner.sender_status = status;
        inner.sender_attributes = attributes.into();
    }

    /// Replace the advertised `Product` sources and increment `SourceXmlChangeCount`.
    pub fn set_sources(&self, sources: Vec<Source>) {
        let mut inner = self.inner.write().unwrap();
        inner.sources = sources;
        if inner.source_index as usize >= inner.sources.len() {
            inner.source_index = 0;
        }
        inner.source_xml_change_count = inner.source_xml_change_count.wrapping_add(1);
    }
}

impl ProductService for MemoryRenderer {
    fn product_info(&self) -> ProductInfo {
        self.inner.read().unwrap().product_info.clone()
    }

    fn attributes(&self) -> DeviceAttributes {
        self.inner.read().unwrap().attributes.clone()
    }

    fn sources(&self) -> Vec<Source> {
        self.inner.read().unwrap().sources.clone()
    }

    fn source_index(&self) -> u32 {
        self.inner.read().unwrap().source_index
    }

    fn set_source_index(&self, index: u32) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        if (index as usize) >= inner.sources.len() {
            return Err(Fault::source_not_found());
        }
        inner.set_source_index_inner(index);
        Ok(())
    }

    fn standby(&self) -> bool {
        self.inner.read().unwrap().standby
    }

    fn set_standby(&self, standby: bool) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.standby = standby;
        if standby {
            inner.transport_state = TransportState::Stopped;
            inner.receiver_state = ReceiverState::Stopped;
        }
        Ok(())
    }

    fn source_xml_change_count(&self) -> u32 {
        self.inner.read().unwrap().source_xml_change_count
    }
}

impl PlaylistService for MemoryRenderer {
    fn play(&self) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        // OpenHome quirk: Playlist.Play auto-selects the Playlist source.
        if !inner.is_source_type_active(&SourceType::Playlist) {
            inner.select_source_type(&SourceType::Playlist);
        }
        inner.standby = false;
        if inner.tracks.is_empty() {
            inner.transport_state = TransportState::Stopped;
            return Ok(());
        }
        if inner.current_id == 0 {
            inner.select_track_by_idx(0);
        }
        inner.transport_state = TransportState::Playing;
        Ok(())
    }

    fn pause(&self) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.require_playlist_active()?;
        if inner.transport_state == TransportState::Playing {
            inner.transport_state = TransportState::Paused;
        }
        Ok(())
    }

    fn stop(&self) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.require_playlist_active()?;
        inner.transport_state = TransportState::Stopped;
        inner.time.seconds = 0;
        Ok(())
    }

    fn next(&self) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.require_playlist_active()?;
        if inner.tracks.is_empty() {
            return Ok(());
        }
        match inner.tracks.iter().position(|t| t.id == inner.current_id) {
            None => inner.select_track_by_idx(0),
            Some(cur) if cur + 1 < inner.tracks.len() => inner.select_track_by_idx(cur + 1),
            Some(_) if inner.repeat => inner.select_track_by_idx(0),
            Some(_) => inner.transport_state = TransportState::Stopped,
        }
        Ok(())
    }

    fn previous(&self) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.require_playlist_active()?;
        if inner.tracks.is_empty() {
            return Ok(());
        }
        match inner.tracks.iter().position(|t| t.id == inner.current_id) {
            None => {
                let target = if inner.repeat {
                    inner.tracks.len() - 1
                } else {
                    0
                };
                inner.select_track_by_idx(target);
            }
            Some(cur) if cur > 0 => inner.select_track_by_idx(cur - 1),
            Some(_) if inner.repeat => {
                let last = inner.tracks.len() - 1;
                inner.select_track_by_idx(last);
            }
            Some(_) => inner.time.seconds = 0,
        }
        Ok(())
    }

    fn repeat(&self) -> bool {
        self.inner.read().unwrap().repeat
    }

    fn set_repeat(&self, value: bool) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.require_playlist_active()?;
        inner.repeat = value;
        Ok(())
    }

    fn shuffle(&self) -> bool {
        self.inner.read().unwrap().shuffle
    }

    fn set_shuffle(&self, value: bool) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.require_playlist_active()?;
        inner.shuffle = value;
        Ok(())
    }

    fn seek_second_absolute(&self, seconds: u32) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.require_playlist_active()?;
        if inner.time.duration > 0 && seconds > inner.time.duration {
            return Err(Fault::seek_failed());
        }
        inner.time.seconds = seconds;
        Ok(())
    }

    fn seek_second_relative(&self, delta_seconds: i32) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.require_playlist_active()?;
        let cur = i64::from(inner.time.seconds);
        let next = (cur + i64::from(delta_seconds)).max(0) as u32;
        inner.time.seconds = if inner.time.duration > 0 {
            next.min(inner.time.duration)
        } else {
            next
        };
        Ok(())
    }

    fn seek_id(&self, id: u32) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.require_playlist_active()?;
        let pos = inner
            .tracks
            .iter()
            .position(|t| t.id == id)
            .ok_or_else(Fault::invalid_id)?;
        inner.select_track_by_idx(pos);
        inner.transport_state = TransportState::Playing;
        Ok(())
    }

    fn seek_index(&self, index: u32) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.require_playlist_active()?;
        let pos = index as usize;
        if pos >= inner.tracks.len() {
            return Err(Fault::invalid_index());
        }
        inner.select_track_by_idx(pos);
        inner.transport_state = TransportState::Playing;
        Ok(())
    }

    fn transport_state(&self) -> TransportState {
        let inner = self.inner.read().unwrap();
        if inner.is_source_type_active(&SourceType::Playlist) {
            inner.transport_state
        } else {
            TransportState::Stopped
        }
    }

    fn current_id(&self) -> u32 {
        self.inner.read().unwrap().current_id
    }

    fn read(&self, id: u32) -> ActionResult<TrackEntry> {
        let inner = self.inner.read().unwrap();
        inner
            .tracks
            .iter()
            .find(|t| t.id == id)
            .cloned()
            .ok_or_else(Fault::invalid_id)
    }

    fn read_list(&self, ids: &[u32]) -> Vec<TrackEntry> {
        let inner = self.inner.read().unwrap();
        if ids.is_empty() || inner.tracks.is_empty() {
            return Vec::new();
        }
        let index: HashMap<u32, &TrackEntry> = inner.tracks.iter().map(|t| (t.id, t)).collect();
        let mut seen = HashSet::new();
        let mut out = Vec::with_capacity(ids.len().min(MAX_READ_LIST_IDS));
        for &id in ids.iter().take(MAX_READ_LIST_IDS) {
            if seen.insert(id)
                && let Some(&entry) = index.get(&id)
            {
                out.push(entry.clone());
            }
        }
        out
    }

    fn insert(&self, after_id: u32, uri: String, metadata: String) -> ActionResult<u32> {
        if uri.len() > MAX_URI_BYTES {
            return Err(Fault::invalid_args("Uri exceeds maximum allowed length"));
        }
        if metadata.len() > MAX_METADATA_BYTES {
            return Err(Fault::invalid_args(
                "Metadata exceeds maximum allowed length",
            ));
        }
        let entry_bytes = uri.len() + metadata.len();

        let mut inner = self.inner.write().unwrap();
        if inner.tracks.len() >= inner.tracks_max as usize {
            return Err(Fault::playlist_full());
        }
        if inner.total_track_bytes.saturating_add(entry_bytes) > MAX_PLAYLIST_TOTAL_BYTES {
            return Err(Fault::playlist_full());
        }
        let insert_pos = if after_id == 0 {
            0
        } else {
            inner
                .tracks
                .iter()
                .position(|t| t.id == after_id)
                .map(|p| p + 1)
                .ok_or_else(Fault::invalid_id)?
        };
        let new_id = inner.next_track_id;
        inner.next_track_id = inner.next_track_id.wrapping_add(1).max(1);
        inner.tracks.insert(
            insert_pos,
            TrackEntry {
                id: new_id,
                uri,
                metadata,
            },
        );
        inner.total_track_bytes = inner.total_track_bytes.saturating_add(entry_bytes);
        inner.id_array_token = inner.id_array_token.wrapping_add(1);
        Ok(new_id)
    }

    fn delete_id(&self, id: u32) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        let pos = inner
            .tracks
            .iter()
            .position(|t| t.id == id)
            .ok_or_else(Fault::invalid_id)?;
        let removed = inner.tracks.remove(pos);
        inner.total_track_bytes = inner
            .total_track_bytes
            .saturating_sub(removed.uri.len() + removed.metadata.len());
        inner.id_array_token = inner.id_array_token.wrapping_add(1);
        if inner.current_id == id {
            if inner.tracks.is_empty() {
                inner.clear_current_track();
                inner.transport_state = TransportState::Stopped;
            } else {
                let next_pos = pos.min(inner.tracks.len() - 1);
                inner.select_track_by_idx(next_pos);
            }
        }
        Ok(())
    }

    fn delete_all(&self) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.tracks.clear();
        inner.total_track_bytes = 0;
        inner.clear_current_track();
        inner.transport_state = TransportState::Stopped;
        inner.id_array_token = inner.id_array_token.wrapping_add(1);
        Ok(())
    }

    fn tracks_max(&self) -> u32 {
        self.inner.read().unwrap().tracks_max
    }

    fn id_array(&self) -> (u32, Vec<u32>) {
        let inner = self.inner.read().unwrap();
        let ids = inner.tracks.iter().map(|t| t.id).collect();
        (inner.id_array_token, ids)
    }

    fn id_array_changed(&self, token: u32) -> bool {
        self.inner.read().unwrap().id_array_token != token
    }

    fn protocol_info(&self) -> String {
        self.inner.read().unwrap().playlist_protocol_info.clone()
    }
}

impl InfoService for MemoryRenderer {
    fn counters(&self) -> InfoCounters {
        self.inner.read().unwrap().counters
    }

    fn track(&self) -> (String, String) {
        let inner = self.inner.read().unwrap();
        (inner.current_uri.clone(), inner.current_metadata.clone())
    }

    fn details(&self) -> TrackDetails {
        self.inner.read().unwrap().details.clone()
    }

    fn metatext(&self) -> String {
        self.inner.read().unwrap().metatext.clone()
    }
}

impl TimeService for MemoryRenderer {
    fn time(&self) -> TrackTime {
        self.inner.read().unwrap().time
    }
}

impl VolumeService for MemoryRenderer {
    fn characteristics(&self) -> VolumeCharacteristics {
        self.inner.read().unwrap().volume_chars
    }

    fn state(&self) -> VolumeState {
        self.inner.read().unwrap().volume_state
    }

    fn set_volume(&self, value: u32) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        if value > inner.volume_chars.volume_max {
            return Err(Fault::invalid_args("volume exceeds VolumeMax"));
        }
        let limit = inner.volume_state.volume_limit;
        inner.volume_state.volume = value.min(limit);
        Ok(())
    }

    fn volume_inc(&self) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        let cap = inner
            .volume_state
            .volume_limit
            .min(inner.volume_chars.volume_max);
        inner.volume_state.volume = inner.volume_state.volume.saturating_add(1).min(cap);
        Ok(())
    }

    fn volume_dec(&self) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        inner.volume_state.volume = inner.volume_state.volume.saturating_sub(1);
        Ok(())
    }

    fn set_mute(&self, mute: bool) -> ActionResult<()> {
        self.inner.write().unwrap().volume_state.mute = mute;
        Ok(())
    }

    fn set_balance(&self, value: i32) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        let max = inner.volume_chars.balance_max as i32;
        if value < -max || value > max {
            return Err(Fault::invalid_args("balance out of range"));
        }
        inner.volume_state.balance = value;
        Ok(())
    }

    fn set_fade(&self, value: i32) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        let max = inner.volume_chars.fade_max as i32;
        if value < -max || value > max {
            return Err(Fault::invalid_args("fade out of range"));
        }
        inner.volume_state.fade = value;
        Ok(())
    }
}

impl ReceiverService for MemoryRenderer {
    fn play(&self) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        if !inner.is_source_type_active(&SourceType::Receiver) {
            inner.select_source_type(&SourceType::Receiver);
        }
        inner.standby = false;
        inner.receiver_state = ReceiverState::Playing;
        Ok(())
    }

    fn stop(&self) -> ActionResult<()> {
        let mut inner = self.inner.write().unwrap();
        // Per docs/architecture.md: do not reset Product.SourceIndex on Receiver.Stop.
        inner.receiver_state = ReceiverState::Stopped;
        Ok(())
    }

    fn set_sender(&self, uri: String, metadata: String) -> ActionResult<()> {
        if uri.len() > MAX_URI_BYTES {
            return Err(Fault::invalid_args("Uri exceeds maximum allowed length"));
        }
        if metadata.len() > MAX_METADATA_BYTES {
            return Err(Fault::invalid_args(
                "Metadata exceeds maximum allowed length",
            ));
        }
        let mut inner = self.inner.write().unwrap();
        inner.receiver_uri = uri;
        inner.receiver_metadata = metadata;
        Ok(())
    }

    fn sender(&self) -> (String, String) {
        let inner = self.inner.read().unwrap();
        (inner.receiver_uri.clone(), inner.receiver_metadata.clone())
    }

    fn protocol_info(&self) -> String {
        self.inner.read().unwrap().receiver_protocol_info.clone()
    }

    fn transport_state(&self) -> ReceiverState {
        self.inner.read().unwrap().receiver_state
    }
}

impl SenderService for MemoryRenderer {
    fn presentation_url(&self) -> String {
        self.inner.read().unwrap().sender_presentation_url.clone()
    }

    fn metadata(&self) -> String {
        self.inner.read().unwrap().sender_metadata.clone()
    }

    fn audio(&self) -> bool {
        self.inner.read().unwrap().sender_audio
    }

    fn status(&self) -> SenderStatus {
        self.inner.read().unwrap().sender_status
    }

    fn attributes(&self) -> String {
        self.inner.read().unwrap().sender_attributes.clone()
    }
}
