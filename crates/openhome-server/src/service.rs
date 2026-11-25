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

    /// Number of times `SourceXml` has changed since boot (`Product.SourceXmlChangeCount`).
    fn source_xml_change_count(&self) -> u32 {
        0
    }

    /// Select the first source whose user-visible `Name` matches `name`.
    fn set_source_index_by_name(&self, name: &str) -> ActionResult<()> {
        let sources = self.sources();
        let idx = sources
            .iter()
            .position(|s| s.name == name)
            .ok_or_else(Fault::source_not_found)?;
        self.set_source_index(idx as u32)
    }

    /// Select the first source whose `SystemName` matches `system_name`.
    fn set_source_by_system_name(&self, system_name: &str) -> ActionResult<()> {
        let sources = self.sources();
        let idx = sources
            .iter()
            .position(|s| s.system_name == system_name)
            .ok_or_else(Fault::source_not_found)?;
        self.set_source_index(idx as u32)
    }
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

    fn balance_inc(&self) -> ActionResult<()> {
        let max = self.characteristics().balance_max as i32;
        if max == 0 {
            return Ok(());
        }
        let next = (self.state().balance + 1).min(max);
        self.set_balance(next)
    }

    fn balance_dec(&self) -> ActionResult<()> {
        let max = self.characteristics().balance_max as i32;
        if max == 0 {
            return Ok(());
        }
        let next = (self.state().balance - 1).max(-max);
        self.set_balance(next)
    }

    fn fade_inc(&self) -> ActionResult<()> {
        let max = self.characteristics().fade_max as i32;
        if max == 0 {
            return Ok(());
        }
        let next = (self.state().fade + 1).min(max);
        self.set_fade(next)
    }

    fn fade_dec(&self) -> ActionResult<()> {
        let max = self.characteristics().fade_max as i32;
        if max == 0 {
            return Ok(());
        }
        let next = (self.state().fade - 1).max(-max);
        self.set_fade(next)
    }
}

/// `urn:av-openhome-org:service:Receiver:1` (Songcast)
pub trait ReceiverService: Send + Sync {
    fn play(&self) -> ActionResult<()>;
    fn stop(&self) -> ActionResult<()>;
    fn set_sender(&self, uri: String, metadata: String) -> ActionResult<()>;
    fn sender(&self) -> (String, String);
    fn protocol_info(&self) -> String;
    fn transport_state(&self) -> ReceiverState;
}

/// `urn:av-openhome-org:service:Sender:1` (Songcast)
pub trait SenderService: Send + Sync {
    fn presentation_url(&self) -> String;
    fn metadata(&self) -> String;
    fn audio(&self) -> bool;
    fn status(&self) -> SenderStatus;
    fn attributes(&self) -> String;
}
