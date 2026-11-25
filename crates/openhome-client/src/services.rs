//! Strongly typed async SOAP service clients for OpenHome Media services.

use std::collections::HashMap;

use openhome_core::id_array::decode_id_array;
use openhome_core::urn::{
    SERVICE_INFO_1, SERVICE_PLAYLIST_1, SERVICE_PRODUCT_1, SERVICE_RECEIVER_1, SERVICE_SENDER_1,
    SERVICE_TIME_1, SERVICE_VOLUME_1,
};
use openhome_core::xml::{decode_source_xml, decode_track_list, format_id_list};
use openhome_core::{
    DeviceAttributes, InfoCounters, ManufacturerDetails, ModelDetails, ProductDetails,
    ReceiverState, SenderStatus, Source, SourceType, TrackDetails, TrackEntry, TrackTime,
    TransportState, VolumeCharacteristics, VolumeState, format_upnp_bool, parse_upnp_bool,
};

use crate::error::ClientError;
use crate::http::HttpClient;

fn take_str(map: &mut HashMap<String, String>, key: &'static str) -> Result<String, ClientError> {
    map.remove(key).ok_or(ClientError::MissingArgument(key))
}

fn take_u32(map: &mut HashMap<String, String>, key: &'static str) -> Result<u32, ClientError> {
    let raw = take_str(map, key)?;
    raw.trim()
        .parse::<u32>()
        .map_err(|_| ClientError::InvalidResponse {
            field: key,
            value: raw,
        })
}

fn take_i32(map: &mut HashMap<String, String>, key: &'static str) -> Result<i32, ClientError> {
    let raw = take_str(map, key)?;
    raw.trim()
        .parse::<i32>()
        .map_err(|_| ClientError::InvalidResponse {
            field: key,
            value: raw,
        })
}

fn take_bool(map: &mut HashMap<String, String>, key: &'static str) -> Result<bool, ClientError> {
    let raw = take_str(map, key)?;
    parse_upnp_bool(key, &raw).map_err(ClientError::Core)
}

/// Client for `urn:av-openhome-org:service:Product:1`.
#[derive(Debug, Clone)]
pub struct ProductClient {
    http: HttpClient,
    control_url: String,
    service_type: String,
}

impl ProductClient {
    pub fn new(http: HttpClient, control_url: String) -> Self {
        Self::with_service_type(http, control_url, SERVICE_PRODUCT_1.to_string())
    }

    pub fn with_service_type(http: HttpClient, control_url: String, service_type: String) -> Self {
        Self {
            http,
            control_url,
            service_type,
        }
    }

    pub async fn manufacturer(&self) -> Result<ManufacturerDetails, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Manufacturer", &[])
            .await?;
        Ok(ManufacturerDetails {
            name: take_str(&mut out, "Name")?,
            info: take_str(&mut out, "Info")?,
            url: take_str(&mut out, "Url")?,
            image_uri: take_str(&mut out, "ImageUri")?,
        })
    }

    pub async fn model(&self) -> Result<ModelDetails, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Model", &[])
            .await?;
        Ok(ModelDetails {
            name: take_str(&mut out, "Name")?,
            info: take_str(&mut out, "Info")?,
            url: take_str(&mut out, "Url")?,
            image_uri: take_str(&mut out, "ImageUri")?,
        })
    }

    pub async fn product(&self) -> Result<ProductDetails, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Product", &[])
            .await?;
        Ok(ProductDetails {
            room: take_str(&mut out, "Room")?,
            name: take_str(&mut out, "Name")?,
            info: take_str(&mut out, "Info")?,
            url: take_str(&mut out, "Url")?,
            image_uri: take_str(&mut out, "ImageUri")?,
        })
    }

    pub async fn standby(&self) -> Result<bool, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Standby", &[])
            .await?;
        take_bool(&mut out, "Value")
    }

    pub async fn set_standby(&self, standby: bool) -> Result<(), ClientError> {
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetStandby",
                &[("Value", format_upnp_bool(standby))],
            )
            .await?;
        Ok(())
    }

    pub async fn source_count(&self) -> Result<u32, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "SourceCount", &[])
            .await?;
        take_u32(&mut out, "Value")
    }

    pub async fn source_xml(&self) -> Result<Vec<Source>, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "SourceXml", &[])
            .await?;
        let raw = take_str(&mut out, "Value")?;
        Ok(decode_source_xml(&raw)?)
    }

    pub async fn source_index(&self) -> Result<u32, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "SourceIndex", &[])
            .await?;
        take_u32(&mut out, "Value")
    }

    pub async fn set_source_index(&self, index: u32) -> Result<(), ClientError> {
        let val = index.to_string();
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetSourceIndex",
                &[("Value", &val)],
            )
            .await?;
        Ok(())
    }

    pub async fn set_source_index_by_name(&self, name: &str) -> Result<(), ClientError> {
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetSourceIndexByName",
                &[("Value", name)],
            )
            .await?;
        Ok(())
    }

    pub async fn set_source_by_system_name(&self, system_name: &str) -> Result<(), ClientError> {
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetSourceBySystemName",
                &[("Value", system_name)],
            )
            .await?;
        Ok(())
    }

    pub async fn source(&self, index: u32) -> Result<Source, ClientError> {
        let idx = index.to_string();
        let mut out = self
            .http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "Source",
                &[("Index", &idx)],
            )
            .await?;
        let system_name = take_str(&mut out, "SystemName")?;
        let source_type = SourceType::parse(&take_str(&mut out, "Type")?);
        let name = take_str(&mut out, "Name")?;
        let visible = take_bool(&mut out, "Visible")?;
        Ok(Source {
            name,
            source_type,
            visible,
            system_name,
        })
    }

    pub async fn attributes(&self) -> Result<DeviceAttributes, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Attributes", &[])
            .await?;
        let raw = take_str(&mut out, "Value")?;
        Ok(DeviceAttributes::parse(&raw))
    }

    pub async fn source_xml_change_count(&self) -> Result<u32, ClientError> {
        let mut out = self
            .http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SourceXmlChangeCount",
                &[],
            )
            .await?;
        take_u32(&mut out, "Value")
    }
}

/// Client for `urn:av-openhome-org:service:Playlist:1`.
#[derive(Debug, Clone)]
pub struct PlaylistClient {
    http: HttpClient,
    control_url: String,
    service_type: String,
}

impl PlaylistClient {
    pub fn new(http: HttpClient, control_url: String) -> Self {
        Self::with_service_type(http, control_url, SERVICE_PLAYLIST_1.to_string())
    }

    pub fn with_service_type(http: HttpClient, control_url: String, service_type: String) -> Self {
        Self {
            http,
            control_url,
            service_type,
        }
    }

    pub async fn play(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "Play", &[])
            .await?;
        Ok(())
    }

    pub async fn pause(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "Pause", &[])
            .await?;
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "Stop", &[])
            .await?;
        Ok(())
    }

    pub async fn next(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "Next", &[])
            .await?;
        Ok(())
    }

    pub async fn previous(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "Previous", &[])
            .await?;
        Ok(())
    }

    pub async fn repeat(&self) -> Result<bool, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Repeat", &[])
            .await?;
        take_bool(&mut out, "Value")
    }

    pub async fn set_repeat(&self, value: bool) -> Result<(), ClientError> {
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetRepeat",
                &[("Value", format_upnp_bool(value))],
            )
            .await?;
        Ok(())
    }

    pub async fn shuffle(&self) -> Result<bool, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Shuffle", &[])
            .await?;
        take_bool(&mut out, "Value")
    }

    pub async fn set_shuffle(&self, value: bool) -> Result<(), ClientError> {
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetShuffle",
                &[("Value", format_upnp_bool(value))],
            )
            .await?;
        Ok(())
    }

    pub async fn seek_second_absolute(&self, seconds: u32) -> Result<(), ClientError> {
        let val = seconds.to_string();
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SeekSecondAbsolute",
                &[("Value", &val)],
            )
            .await?;
        Ok(())
    }

    pub async fn seek_second_relative(&self, delta_seconds: i32) -> Result<(), ClientError> {
        let val = delta_seconds.to_string();
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SeekSecondRelative",
                &[("Value", &val)],
            )
            .await?;
        Ok(())
    }

    pub async fn seek_id(&self, id: u32) -> Result<(), ClientError> {
        let val = id.to_string();
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SeekId",
                &[("Value", &val)],
            )
            .await?;
        Ok(())
    }

    pub async fn seek_index(&self, index: u32) -> Result<(), ClientError> {
        let val = index.to_string();
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SeekIndex",
                &[("Value", &val)],
            )
            .await?;
        Ok(())
    }

    pub async fn transport_state(&self) -> Result<TransportState, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "TransportState", &[])
            .await?;
        let raw = take_str(&mut out, "Value")?;
        TransportState::parse(&raw).ok_or(ClientError::InvalidResponse {
            field: "TransportState",
            value: raw,
        })
    }

    pub async fn id(&self) -> Result<u32, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Id", &[])
            .await?;
        take_u32(&mut out, "Value")
    }

    pub async fn read(&self, id: u32) -> Result<TrackEntry, ClientError> {
        let id_str = id.to_string();
        let mut out = self
            .http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "Read",
                &[("Id", &id_str)],
            )
            .await?;
        Ok(TrackEntry {
            id,
            uri: take_str(&mut out, "Uri")?,
            metadata: take_str(&mut out, "Metadata")?,
        })
    }

    pub async fn read_list(&self, ids: &[u32]) -> Result<Vec<TrackEntry>, ClientError> {
        let id_list = format_id_list(ids);
        self.read_list_raw(&id_list).await
    }

    /// Invoke `Playlist.ReadList` with a raw `IdList` string (useful for testing
    /// interop quirks like Lumin's `-1` sentinel).
    pub async fn read_list_raw(&self, id_list: &str) -> Result<Vec<TrackEntry>, ClientError> {
        let mut out = self
            .http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "ReadList",
                &[("IdList", id_list)],
            )
            .await?;
        let xml = take_str(&mut out, "TrackList")?;
        Ok(decode_track_list(&xml)?)
    }

    pub async fn insert(
        &self,
        after_id: u32,
        uri: &str,
        metadata: &str,
    ) -> Result<u32, ClientError> {
        let after_str = after_id.to_string();
        let mut out = self
            .http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "Insert",
                &[
                    ("AfterId", &after_str),
                    ("Uri", uri),
                    ("Metadata", metadata),
                ],
            )
            .await?;
        take_u32(&mut out, "NewId")
    }

    pub async fn delete_id(&self, id: u32) -> Result<(), ClientError> {
        let val = id.to_string();
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "DeleteId",
                &[("Value", &val)],
            )
            .await?;
        Ok(())
    }

    pub async fn delete_all(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "DeleteAll", &[])
            .await?;
        Ok(())
    }

    pub async fn tracks_max(&self) -> Result<u32, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "TracksMax", &[])
            .await?;
        take_u32(&mut out, "Value")
    }

    pub async fn id_array(&self) -> Result<(u32, Vec<u32>), ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "IdArray", &[])
            .await?;
        let token = take_u32(&mut out, "Token")?;
        let raw = take_str(&mut out, "Array")?;
        let ids = decode_id_array(&raw)?;
        Ok((token, ids))
    }

    pub async fn id_array_changed(&self, token: u32) -> Result<bool, ClientError> {
        let tok = token.to_string();
        let mut out = self
            .http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "IdArrayChanged",
                &[("Token", &tok)],
            )
            .await?;
        take_bool(&mut out, "Value")
    }

    pub async fn protocol_info(&self) -> Result<String, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "ProtocolInfo", &[])
            .await?;
        take_str(&mut out, "Value")
    }
}

/// Client for `urn:av-openhome-org:service:Info:1`.
#[derive(Debug, Clone)]
pub struct InfoClient {
    http: HttpClient,
    control_url: String,
    service_type: String,
}

impl InfoClient {
    pub fn new(http: HttpClient, control_url: String) -> Self {
        Self::with_service_type(http, control_url, SERVICE_INFO_1.to_string())
    }

    pub fn with_service_type(http: HttpClient, control_url: String, service_type: String) -> Self {
        Self {
            http,
            control_url,
            service_type,
        }
    }

    pub async fn counters(&self) -> Result<InfoCounters, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Counters", &[])
            .await?;
        Ok(InfoCounters {
            track_count: take_u32(&mut out, "TrackCount")?,
            details_count: take_u32(&mut out, "DetailsCount")?,
            metatext_count: take_u32(&mut out, "MetatextCount")?,
        })
    }

    pub async fn track(&self) -> Result<(String, String), ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Track", &[])
            .await?;
        Ok((take_str(&mut out, "Uri")?, take_str(&mut out, "Metadata")?))
    }

    pub async fn details(&self) -> Result<TrackDetails, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Details", &[])
            .await?;
        Ok(TrackDetails {
            duration: take_u32(&mut out, "Duration")?,
            bit_rate: take_u32(&mut out, "BitRate")?,
            bit_depth: take_u32(&mut out, "BitDepth")?,
            sample_rate: take_u32(&mut out, "SampleRate")?,
            lossless: take_bool(&mut out, "Lossless")?,
            codec_name: take_str(&mut out, "CodecName")?,
        })
    }

    pub async fn metatext(&self) -> Result<String, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Metatext", &[])
            .await?;
        take_str(&mut out, "Value")
    }
}

/// Client for `urn:av-openhome-org:service:Time:1`.
#[derive(Debug, Clone)]
pub struct TimeClient {
    http: HttpClient,
    control_url: String,
    service_type: String,
}

impl TimeClient {
    pub fn new(http: HttpClient, control_url: String) -> Self {
        Self::with_service_type(http, control_url, SERVICE_TIME_1.to_string())
    }

    pub fn with_service_type(http: HttpClient, control_url: String, service_type: String) -> Self {
        Self {
            http,
            control_url,
            service_type,
        }
    }

    pub async fn time(&self) -> Result<TrackTime, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Time", &[])
            .await?;
        Ok(TrackTime {
            track_count: take_u32(&mut out, "TrackCount")?,
            duration: take_u32(&mut out, "Duration")?,
            seconds: take_u32(&mut out, "Seconds")?,
        })
    }
}

/// Client for `urn:av-openhome-org:service:Volume:1`.
#[derive(Debug, Clone)]
pub struct VolumeClient {
    http: HttpClient,
    control_url: String,
    service_type: String,
}

impl VolumeClient {
    pub fn new(http: HttpClient, control_url: String) -> Self {
        Self::with_service_type(http, control_url, SERVICE_VOLUME_1.to_string())
    }

    pub fn with_service_type(http: HttpClient, control_url: String, service_type: String) -> Self {
        Self {
            http,
            control_url,
            service_type,
        }
    }

    pub async fn characteristics(&self) -> Result<VolumeCharacteristics, ClientError> {
        let mut out = self
            .http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "Characteristics",
                &[],
            )
            .await?;
        Ok(VolumeCharacteristics {
            volume_max: take_u32(&mut out, "VolumeMax")?,
            volume_unity: take_u32(&mut out, "VolumeUnity")?,
            volume_steps: take_u32(&mut out, "VolumeSteps")?,
            volume_milli_db_per_step: take_u32(&mut out, "VolumeMilliDbPerStep")?,
            balance_max: take_u32(&mut out, "BalanceMax")?,
            fade_max: take_u32(&mut out, "FadeMax")?,
        })
    }

    pub async fn set_volume(&self, value: u32) -> Result<(), ClientError> {
        let val = value.to_string();
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetVolume",
                &[("Value", &val)],
            )
            .await?;
        Ok(())
    }

    pub async fn volume_inc(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "VolumeInc", &[])
            .await?;
        Ok(())
    }

    pub async fn volume_dec(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "VolumeDec", &[])
            .await?;
        Ok(())
    }

    pub async fn volume(&self) -> Result<u32, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Volume", &[])
            .await?;
        take_u32(&mut out, "Value")
    }

    pub async fn set_mute(&self, mute: bool) -> Result<(), ClientError> {
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetMute",
                &[("Value", format_upnp_bool(mute))],
            )
            .await?;
        Ok(())
    }

    pub async fn mute(&self) -> Result<bool, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Mute", &[])
            .await?;
        take_bool(&mut out, "Value")
    }

    pub async fn volume_limit(&self) -> Result<u32, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "VolumeLimit", &[])
            .await?;
        take_u32(&mut out, "Value")
    }

    pub async fn set_balance(&self, value: i32) -> Result<(), ClientError> {
        let val = value.to_string();
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetBalance",
                &[("Value", &val)],
            )
            .await?;
        Ok(())
    }

    pub async fn balance_inc(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "BalanceInc", &[])
            .await?;
        Ok(())
    }

    pub async fn balance_dec(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "BalanceDec", &[])
            .await?;
        Ok(())
    }

    pub async fn balance(&self) -> Result<i32, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Balance", &[])
            .await?;
        take_i32(&mut out, "Value")
    }

    pub async fn set_fade(&self, value: i32) -> Result<(), ClientError> {
        let val = value.to_string();
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetFade",
                &[("Value", &val)],
            )
            .await?;
        Ok(())
    }

    pub async fn fade_inc(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "FadeInc", &[])
            .await?;
        Ok(())
    }

    pub async fn fade_dec(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "FadeDec", &[])
            .await?;
        Ok(())
    }

    pub async fn fade(&self) -> Result<i32, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Fade", &[])
            .await?;
        take_i32(&mut out, "Value")
    }

    /// Fetch the combined dynamic [`VolumeState`] across `Volume`, `Mute`,
    /// `Balance`, `Fade`, and `VolumeLimit`.
    pub async fn state(&self) -> Result<VolumeState, ClientError> {
        let (volume, mute, balance, fade, volume_limit) = tokio::try_join!(
            self.volume(),
            self.mute(),
            self.balance(),
            self.fade(),
            self.volume_limit()
        )?;
        Ok(VolumeState {
            volume,
            mute,
            balance,
            fade,
            volume_limit,
        })
    }
}

/// Client for `urn:av-openhome-org:service:Receiver:1` (Songcast).
#[derive(Debug, Clone)]
pub struct ReceiverClient {
    http: HttpClient,
    control_url: String,
    service_type: String,
}

impl ReceiverClient {
    pub fn new(http: HttpClient, control_url: String) -> Self {
        Self::with_service_type(http, control_url, SERVICE_RECEIVER_1.to_string())
    }

    pub fn with_service_type(http: HttpClient, control_url: String, service_type: String) -> Self {
        Self {
            http,
            control_url,
            service_type,
        }
    }

    pub async fn play(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "Play", &[])
            .await?;
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), ClientError> {
        self.http
            .soap_call(&self.control_url, &self.service_type, "Stop", &[])
            .await?;
        Ok(())
    }

    pub async fn set_sender(&self, uri: &str, metadata: &str) -> Result<(), ClientError> {
        self.http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "SetSender",
                &[("Uri", uri), ("Metadata", metadata)],
            )
            .await?;
        Ok(())
    }

    pub async fn sender(&self) -> Result<(String, String), ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Sender", &[])
            .await?;
        Ok((take_str(&mut out, "Uri")?, take_str(&mut out, "Metadata")?))
    }

    pub async fn protocol_info(&self) -> Result<String, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "ProtocolInfo", &[])
            .await?;
        take_str(&mut out, "Value")
    }

    pub async fn transport_state(&self) -> Result<ReceiverState, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "TransportState", &[])
            .await?;
        let raw = take_str(&mut out, "Value")?;
        ReceiverState::parse(&raw).ok_or(ClientError::InvalidResponse {
            field: "TransportState",
            value: raw,
        })
    }
}

/// Client for `urn:av-openhome-org:service:Sender:1` (Songcast).
#[derive(Debug, Clone)]
pub struct SenderClient {
    http: HttpClient,
    control_url: String,
    service_type: String,
}

impl SenderClient {
    pub fn new(http: HttpClient, control_url: String) -> Self {
        Self::with_service_type(http, control_url, SERVICE_SENDER_1.to_string())
    }

    pub fn with_service_type(http: HttpClient, control_url: String, service_type: String) -> Self {
        Self {
            http,
            control_url,
            service_type,
        }
    }

    pub async fn presentation_url(&self) -> Result<String, ClientError> {
        let mut out = self
            .http
            .soap_call(
                &self.control_url,
                &self.service_type,
                "PresentationUrl",
                &[],
            )
            .await?;
        take_str(&mut out, "Value")
    }

    pub async fn metadata(&self) -> Result<String, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Metadata", &[])
            .await?;
        take_str(&mut out, "Value")
    }

    pub async fn audio(&self) -> Result<bool, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Audio", &[])
            .await?;
        take_bool(&mut out, "Value")
    }

    pub async fn status(&self) -> Result<SenderStatus, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Status", &[])
            .await?;
        let raw = take_str(&mut out, "Value")?;
        SenderStatus::parse(&raw).ok_or(ClientError::InvalidResponse {
            field: "Status",
            value: raw,
        })
    }

    pub async fn attributes(&self) -> Result<String, ClientError> {
        let mut out = self
            .http
            .soap_call(&self.control_url, &self.service_type, "Attributes", &[])
            .await?;
        take_str(&mut out, "Value")
    }
}
