//! Strongly typed OpenHome domain models shared across server, client, and Songcast.

use std::fmt;

/// Source type reported in `Product.SourceXml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceType {
    Playlist,
    Radio,
    Receiver,
    UpnpAv,
    NetAux,
    Analog,
    Digital,
    Hdmi,
    Other(String),
}

impl SourceType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Playlist => "Playlist",
            Self::Radio => "Radio",
            Self::Receiver => "Receiver",
            Self::UpnpAv => "UpnpAv",
            Self::NetAux => "NetAux",
            Self::Analog => "Analog",
            Self::Digital => "Digital",
            Self::Hdmi => "Hdmi",
            Self::Other(s) => s.as_str(),
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "Playlist" => Self::Playlist,
            "Radio" => Self::Radio,
            "Receiver" => Self::Receiver,
            "UpnpAv" => Self::UpnpAv,
            "NetAux" => Self::NetAux,
            "Analog" => Self::Analog,
            "Digital" => Self::Digital,
            "Hdmi" => Self::Hdmi,
            other => Self::Other(other.to_string()),
        }
    }
}

impl fmt::Display for SourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Entry in `Product.SourceXml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub name: String,
    pub source_type: SourceType,
    pub visible: bool,
    pub system_name: String,
}

/// Space-delimited capability tokens advertised by `Product.Attributes`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeviceAttributes {
    pub info: bool,
    pub time: bool,
    pub volume: bool,
    pub sender: bool,
    pub receiver: bool,
    pub credentials: bool,
}

impl DeviceAttributes {
    pub fn to_attribute_string(&self) -> String {
        let mut tokens = Vec::new();
        if self.info {
            tokens.push("Info");
        }
        if self.time {
            tokens.push("Time");
        }
        if self.volume {
            tokens.push("Volume");
        }
        if self.sender {
            tokens.push("Sender");
        }
        if self.receiver {
            tokens.push("Receiver");
        }
        if self.credentials {
            tokens.push("Credentials");
        }
        tokens.join(" ")
    }

    pub fn parse(s: &str) -> Self {
        let mut out = Self::default();
        for tok in s.split_whitespace() {
            match tok {
                "Info" => out.info = true,
                "Time" => out.time = true,
                "Volume" => out.volume = true,
                "Sender" => out.sender = true,
                "Receiver" => out.receiver = true,
                "Credentials" => out.credentials = true,
                _ => {}
            }
        }
        out
    }
}

/// Product identity metadata returned by `Product.Manufacturer`, `Model`, and `Product`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductInfo {
    pub manufacturer_name: String,
    pub manufacturer_info: String,
    pub manufacturer_url: String,
    pub manufacturer_image_uri: String,
    pub model_name: String,
    pub model_info: String,
    pub model_url: String,
    pub model_image_uri: String,
    pub product_room: String,
    pub product_name: String,
    pub product_info: String,
    pub product_url: String,
    pub product_image_uri: String,
}

/// `Playlist.TransportState` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransportState {
    #[default]
    Stopped,
    Playing,
    Paused,
    Buffering,
}

impl TransportState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stopped => "Stopped",
            Self::Playing => "Playing",
            Self::Paused => "Paused",
            Self::Buffering => "Buffering",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Stopped" => Some(Self::Stopped),
            "Playing" => Some(Self::Playing),
            "Paused" => Some(Self::Paused),
            "Buffering" => Some(Self::Buffering),
            _ => None,
        }
    }
}

impl fmt::Display for TransportState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Single entry in `Playlist.ReadList` (`<TrackList><Entry>...</Entry></TrackList>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackEntry {
    pub id: u32,
    pub uri: String,
    pub metadata: String,
}

/// Counters reported by `Info.Counters`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InfoCounters {
    pub track_count: u32,
    pub details_count: u32,
    pub metatext_count: u32,
}

/// Stream format details reported by `Info.Details`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrackDetails {
    pub duration: u32,
    pub bit_rate: u32,
    pub bit_depth: u32,
    pub sample_rate: u32,
    pub lossless: bool,
    pub codec_name: String,
}

/// Playback progress reported by `Time.Time`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrackTime {
    pub track_count: u32,
    pub duration: u32,
    pub seconds: u32,
}

/// Static volume hardware/software characteristics (`Volume.Characteristics`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VolumeCharacteristics {
    pub volume_max: u32,
    pub volume_unity: u32,
    pub volume_steps: u32,
    pub volume_milli_db_per_step: u32,
    pub balance_max: u32,
    pub fade_max: u32,
}

impl Default for VolumeCharacteristics {
    fn default() -> Self {
        Self {
            volume_max: 100,
            volume_unity: 100,
            volume_steps: 100,
            volume_milli_db_per_step: 500,
            balance_max: 0,
            fade_max: 0,
        }
    }
}

/// Dynamic volume state (`Volume`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VolumeState {
    pub volume: u32,
    pub mute: bool,
    pub balance: i32,
    pub fade: i32,
    pub volume_limit: u32,
}

/// `Receiver.TransportState` values (Songcast).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReceiverState {
    #[default]
    Stopped,
    Playing,
    Waiting,
    Buffering,
}

impl ReceiverState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stopped => "Stopped",
            Self::Playing => "Playing",
            Self::Waiting => "Waiting",
            Self::Buffering => "Buffering",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Stopped" => Some(Self::Stopped),
            "Playing" => Some(Self::Playing),
            "Waiting" => Some(Self::Waiting),
            "Buffering" => Some(Self::Buffering),
            _ => None,
        }
    }
}

/// `Sender.Status` values (Songcast).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SenderStatus {
    #[default]
    Disabled,
    Enabled,
    Blocked,
}

impl SenderStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "Disabled",
            Self::Enabled => "Enabled",
            Self::Blocked => "Blocked",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Disabled" => Some(Self::Disabled),
            "Enabled" => Some(Self::Enabled),
            "Blocked" => Some(Self::Blocked),
            _ => None,
        }
    }
}
