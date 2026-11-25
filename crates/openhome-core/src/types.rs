//! Strongly typed OpenHome domain models shared across server, client, and Songcast.

use std::fmt;

use crate::error::Error;

/// Format a boolean using UPnP's canonical `"1"` / `"0"` lexical representation.
pub fn format_upnp_bool(value: bool) -> &'static str {
    if value { "1" } else { "0" }
}

/// Parse a UPnP/OpenHome boolean value (`"1"`, `"0"`, `"true"`, `"false"`, `"yes"`, `"no"`).
pub fn parse_upnp_bool(field: &'static str, value: &str) -> Result<bool, Error> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => Err(Error::InvalidValue {
            field,
            value: value.to_string(),
        }),
    }
}

/// Manufacturer details returned by `Product.Manufacturer`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManufacturerDetails {
    pub name: String,
    pub info: String,
    pub url: String,
    pub image_uri: String,
}

/// Model details returned by `Product.Model`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelDetails {
    pub name: String,
    pub info: String,
    pub url: String,
    pub image_uri: String,
}

/// Product details returned by `Product.Product`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProductDetails {
    pub room: String,
    pub name: String,
    pub info: String,
    pub url: String,
    pub image_uri: String,
}

/// UPnP service descriptor within `description.xml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceDescription {
    pub service_type: String,
    pub service_id: String,
    pub scpd_url: String,
    pub control_url: String,
    pub event_sub_url: String,
}

/// UPnP device descriptor (`description.xml`) for `urn:av-openhome-org:device:Source:1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceDescription {
    pub url_base: Option<String>,
    pub device_type: String,
    pub friendly_name: String,
    pub manufacturer: String,
    pub manufacturer_url: Option<String>,
    pub model_description: Option<String>,
    pub model_name: String,
    pub model_number: Option<String>,
    pub model_url: Option<String>,
    pub serial_number: Option<String>,
    /// Canonical UDN, e.g. `"uuid:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`.
    pub udn: String,
    pub presentation_url: Option<String>,
    pub services: Vec<ServiceDescription>,
}

impl DeviceDescription {
    /// Find a service by its UPnP `serviceType` URN (e.g. `SERVICE_PLAYLIST_1`).
    pub fn find_service(&self, service_type: &str) -> Option<&ServiceDescription> {
        self.services
            .iter()
            .find(|s| s.service_type == service_type)
    }

    /// Find a service by its UPnP `serviceId` (e.g. `SERVICE_ID_PLAYLIST`).
    pub fn find_service_by_id(&self, service_id: &str) -> Option<&ServiceDescription> {
        self.services.iter().find(|s| s.service_id == service_id)
    }
}

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
        match s.trim() {
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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

impl ProductInfo {
    pub fn manufacturer(&self) -> ManufacturerDetails {
        ManufacturerDetails {
            name: self.manufacturer_name.clone(),
            info: self.manufacturer_info.clone(),
            url: self.manufacturer_url.clone(),
            image_uri: self.manufacturer_image_uri.clone(),
        }
    }

    pub fn model(&self) -> ModelDetails {
        ModelDetails {
            name: self.model_name.clone(),
            info: self.model_info.clone(),
            url: self.model_url.clone(),
            image_uri: self.model_image_uri.clone(),
        }
    }

    pub fn product(&self) -> ProductDetails {
        ProductDetails {
            room: self.product_room.clone(),
            name: self.product_name.clone(),
            info: self.product_info.clone(),
            url: self.product_url.clone(),
            image_uri: self.product_image_uri.clone(),
        }
    }
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
        match s.trim() {
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
        match s.trim() {
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
        match s.trim() {
            "Disabled" => Some(Self::Disabled),
            "Enabled" => Some(Self::Enabled),
            "Blocked" => Some(Self::Blocked),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upnp_bool_format_and_parse() {
        assert_eq!(format_upnp_bool(true), "1");
        assert_eq!(format_upnp_bool(false), "0");
        for truthy in ["1", "true", "TRUE", "yes", " Yes "] {
            assert!(parse_upnp_bool("Flag", truthy).unwrap());
        }
        for falsy in ["0", "false", "FALSE", "no", " No "] {
            assert!(!parse_upnp_bool("Flag", falsy).unwrap());
        }
        assert!(parse_upnp_bool("Flag", "maybe").is_err());
    }

    #[test]
    fn enums_and_attributes_parsing() {
        for (raw, expected) in [
            ("Playlist", SourceType::Playlist),
            ("Radio", SourceType::Radio),
            ("Receiver", SourceType::Receiver),
            ("UpnpAv", SourceType::UpnpAv),
            ("NetAux", SourceType::NetAux),
            ("Analog", SourceType::Analog),
            ("Digital", SourceType::Digital),
            ("Hdmi", SourceType::Hdmi),
            ("CustomSrc", SourceType::Other("CustomSrc".into())),
        ] {
            let parsed = SourceType::parse(&format!(" {raw} "));
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_str(), raw);
            assert_eq!(parsed.to_string(), raw);
        }

        let all_attrs =
            DeviceAttributes::parse("Info Time Volume Sender Receiver Credentials Extra");
        assert!(
            all_attrs.info
                && all_attrs.time
                && all_attrs.volume
                && all_attrs.sender
                && all_attrs.receiver
                && all_attrs.credentials
        );
        assert_eq!(
            all_attrs.to_attribute_string(),
            "Info Time Volume Sender Receiver Credentials"
        );

        for (s, v) in [
            ("Stopped", TransportState::Stopped),
            ("Playing", TransportState::Playing),
            ("Paused", TransportState::Paused),
            ("Buffering", TransportState::Buffering),
        ] {
            assert_eq!(TransportState::parse(&format!(" {s} ")), Some(v));
            assert_eq!(v.as_str(), s);
        }
        assert_eq!(TransportState::parse("Bogus"), None);

        for (s, v) in [
            ("Stopped", ReceiverState::Stopped),
            ("Playing", ReceiverState::Playing),
            ("Waiting", ReceiverState::Waiting),
            ("Buffering", ReceiverState::Buffering),
        ] {
            assert_eq!(ReceiverState::parse(&format!(" {s}\n")), Some(v));
            assert_eq!(v.as_str(), s);
        }
        assert_eq!(ReceiverState::parse("Unknown"), None);

        for (s, v) in [
            ("Disabled", SenderStatus::Disabled),
            ("Enabled", SenderStatus::Enabled),
            ("Blocked", SenderStatus::Blocked),
        ] {
            assert_eq!(SenderStatus::parse(&format!(" {s} ")), Some(v));
            assert_eq!(v.as_str(), s);
        }
        assert_eq!(SenderStatus::parse("Unknown"), None);

        let empty_attrs = DeviceAttributes::default();
        assert_eq!(empty_attrs.to_attribute_string(), "");

        let info = ProductInfo {
            manufacturer_name: "Mfg".into(),
            model_name: "Mod".into(),
            product_room: "Den".into(),
            product_name: "Deck".into(),
            ..ProductInfo::default()
        };
        assert_eq!(info.manufacturer().name, "Mfg");
        assert_eq!(info.model().name, "Mod");
        assert_eq!(info.product().room, "Den");

        let vc = VolumeCharacteristics::default();
        assert_eq!(vc.volume_max, 100);
        assert_eq!(vc.balance_max, 0);
        assert_eq!(vc.fade_max, 0);
    }
}
