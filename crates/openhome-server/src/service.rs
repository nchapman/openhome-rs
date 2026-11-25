//! Service traits implemented by an OpenHome renderer backend.

use openhome_core::{
    DeviceAttributes, Fault, InfoCounters, ProductInfo, ReceiverState, SenderStatus, Source,
    TrackDetails, TrackEntry, TrackTime, TransportState, VolumeCharacteristics, VolumeState,
};

pub type ActionResult<T> = Result<T, Fault>;

/// `urn:av-openhome-org:service:Product:1`
pub trait ProductService: Send + Sync {
    fn product_info(&self) -> ProductInfo;
    fn attributes(&self) -> DeviceAttributes;
    fn sources(&self) -> Vec<Source>;
    fn source_index(&self) -> u32;
    fn set_source_index(&self, index: u32) -> ActionResult<()>;
    fn standby(&self) -> bool;
    fn set_standby(&self, standby: bool) -> ActionResult<()>;
}

/// `urn:av-openhome-org:service:Playlist:1`
pub trait PlaylistService: Send + Sync {
    fn play(&self) -> ActionResult<()>;
    fn pause(&self) -> ActionResult<()>;
    fn stop(&self) -> ActionResult<()>;
    fn next(&self) -> ActionResult<()>;
    fn previous(&self) -> ActionResult<()>;
    fn repeat(&self) -> bool;
    fn set_repeat(&self, value: bool) -> ActionResult<()>;
    fn shuffle(&self) -> bool;
    fn set_shuffle(&self, value: bool) -> ActionResult<()>;
    fn seek_second_absolute(&self, seconds: u32) -> ActionResult<()>;
    fn seek_second_relative(&self, delta_seconds: i32) -> ActionResult<()>;
    fn seek_id(&self, id: u32) -> ActionResult<()>;
    fn seek_index(&self, index: u32) -> ActionResult<()>;
    fn transport_state(&self) -> TransportState;
    fn current_id(&self) -> u32;
    fn read(&self, id: u32) -> ActionResult<TrackEntry>;
    fn read_list(&self, ids: &[u32]) -> Vec<TrackEntry>;
    fn insert(&self, after_id: u32, uri: String, metadata: String) -> ActionResult<u32>;
    fn delete_id(&self, id: u32) -> ActionResult<()>;
    fn delete_all(&self) -> ActionResult<()>;
    fn tracks_max(&self) -> u32;
    fn id_array(&self) -> (u32, Vec<u32>);
    fn id_array_changed(&self, token: u32) -> bool;
    fn protocol_info(&self) -> String;
}

/// `urn:av-openhome-org:service:Info:1`
pub trait InfoService: Send + Sync {
    fn counters(&self) -> InfoCounters;
    fn track(&self) -> (String, String);
    fn details(&self) -> TrackDetails;
    fn metatext(&self) -> String;
}

/// `urn:av-openhome-org:service:Time:1`
pub trait TimeService: Send + Sync {
    fn time(&self) -> TrackTime;
}

/// `urn:av-openhome-org:service:Volume:1`
pub trait VolumeService: Send + Sync {
    fn characteristics(&self) -> VolumeCharacteristics;
    fn state(&self) -> VolumeState;
    fn set_volume(&self, value: u32) -> ActionResult<()>;
    fn volume_inc(&self) -> ActionResult<()>;
    fn volume_dec(&self) -> ActionResult<()>;
    fn set_mute(&self, mute: bool) -> ActionResult<()>;
    fn set_balance(&self, value: i32) -> ActionResult<()>;
    fn set_fade(&self, value: i32) -> ActionResult<()>;
}

/// `urn:av-openhome-org:service:Receiver:1` (Pass 2: Songcast)
pub trait ReceiverService: Send + Sync {
    fn play(&self) -> ActionResult<()>;
    fn stop(&self) -> ActionResult<()>;
    fn set_sender(&self, uri: String, metadata: String) -> ActionResult<()>;
    fn sender(&self) -> (String, String);
    fn protocol_info(&self) -> String;
    fn transport_state(&self) -> ReceiverState;
}

/// `urn:av-openhome-org:service:Sender:1` (Pass 2: Songcast)
pub trait SenderService: Send + Sync {
    fn presentation_url(&self) -> String;
    fn metadata(&self) -> String;
    fn audio(&self) -> bool;
    fn status(&self) -> SenderStatus;
    fn attributes(&self) -> String;
}
