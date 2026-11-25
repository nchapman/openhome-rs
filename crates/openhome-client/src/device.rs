//! High-level [`OpenHomeDevice`] handle parsed from `description.xml`.

use openhome_core::urn::{
    SERVICE_ID_INFO, SERVICE_ID_PLAYLIST, SERVICE_ID_PRODUCT, SERVICE_ID_RECEIVER,
    SERVICE_ID_SENDER, SERVICE_ID_TIME, SERVICE_ID_VOLUME, SERVICE_INFO_1, SERVICE_PLAYLIST_1,
    SERVICE_PRODUCT_1, SERVICE_RECEIVER_1, SERVICE_SENDER_1, SERVICE_TIME_1, SERVICE_VOLUME_1,
};
use openhome_core::xml::decode_device_description;
use openhome_core::{DeviceDescription, ServiceDescription};

use crate::error::ClientError;
use crate::http::{HttpClient, resolve_url};
use crate::services::{
    InfoClient, PlaylistClient, ProductClient, ReceiverClient, SenderClient, TimeClient,
    VolumeClient,
};

/// Connected OpenHome Media device parsed from its UPnP `description.xml`.
#[derive(Debug, Clone)]
pub struct OpenHomeDevice {
    location: String,
    description: DeviceDescription,
    http: HttpClient,
}

impl OpenHomeDevice {
    /// Fetch and parse a device's `description.xml` at `location`.
    pub async fn connect(location: impl Into<String>) -> Result<Self, ClientError> {
        Self::connect_with_http(location, HttpClient::default()).await
    }

    /// Fetch and parse a device's `description.xml` using a custom [`HttpClient`].
    pub async fn connect_with_http(
        location: impl Into<String>,
        http: HttpClient,
    ) -> Result<Self, ClientError> {
        let location = location.into();
        let xml = http.get_text(&location).await?;
        let description = decode_device_description(&xml)?;
        Ok(Self {
            location,
            description,
            http,
        })
    }

    /// Construct from an already-parsed [`DeviceDescription`].
    pub fn from_description(
        location: impl Into<String>,
        description: DeviceDescription,
        http: HttpClient,
    ) -> Self {
        Self {
            location: location.into(),
            description,
            http,
        }
    }

    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn udn(&self) -> &str {
        &self.description.udn
    }

    pub fn friendly_name(&self) -> &str {
        &self.description.friendly_name
    }

    pub fn description(&self) -> &DeviceDescription {
        &self.description
    }

    pub fn http(&self) -> &HttpClient {
        &self.http
    }

    /// Resolve a service's `eventSubURL` by `serviceType` or `serviceId`.
    pub fn event_sub_url(&self, service_type_or_id: &'static str) -> Result<String, ClientError> {
        let svc = self
            .description
            .find_service(service_type_or_id)
            .or_else(|| self.description.find_service_by_id(service_type_or_id))
            .ok_or(ClientError::MissingService(service_type_or_id))?;
        resolve_url(&self.location, &svc.event_sub_url)
    }

    /// Fetch a service's SCPD XML document by `serviceType` or `serviceId`.
    pub async fn scpd_xml(&self, service_type_or_id: &'static str) -> Result<String, ClientError> {
        let svc = self
            .description
            .find_service(service_type_or_id)
            .or_else(|| self.description.find_service_by_id(service_type_or_id))
            .ok_or(ClientError::MissingService(service_type_or_id))?;
        let url = resolve_url(&self.location, &svc.scpd_url)?;
        self.http.get_text(&url).await
    }

    pub fn product(&self) -> Result<ProductClient, ClientError> {
        let svc = self.find_openhome_service(
            SERVICE_PRODUCT_1,
            SERVICE_ID_PRODUCT,
            "urn:av-openhome-org:service:Product:",
        )?;
        let url = resolve_url(&self.location, &svc.control_url)?;
        Ok(ProductClient::with_service_type(
            self.http.clone(),
            url,
            svc.service_type.clone(),
        ))
    }

    pub fn playlist(&self) -> Result<PlaylistClient, ClientError> {
        let svc = self.find_openhome_service(
            SERVICE_PLAYLIST_1,
            SERVICE_ID_PLAYLIST,
            "urn:av-openhome-org:service:Playlist:",
        )?;
        let url = resolve_url(&self.location, &svc.control_url)?;
        Ok(PlaylistClient::with_service_type(
            self.http.clone(),
            url,
            svc.service_type.clone(),
        ))
    }

    pub fn info(&self) -> Result<InfoClient, ClientError> {
        let svc = self.find_openhome_service(
            SERVICE_INFO_1,
            SERVICE_ID_INFO,
            "urn:av-openhome-org:service:Info:",
        )?;
        let url = resolve_url(&self.location, &svc.control_url)?;
        Ok(InfoClient::with_service_type(
            self.http.clone(),
            url,
            svc.service_type.clone(),
        ))
    }

    pub fn time(&self) -> Result<TimeClient, ClientError> {
        let svc = self.find_openhome_service(
            SERVICE_TIME_1,
            SERVICE_ID_TIME,
            "urn:av-openhome-org:service:Time:",
        )?;
        let url = resolve_url(&self.location, &svc.control_url)?;
        Ok(TimeClient::with_service_type(
            self.http.clone(),
            url,
            svc.service_type.clone(),
        ))
    }

    pub fn volume(&self) -> Result<VolumeClient, ClientError> {
        let svc = self.find_openhome_service(
            SERVICE_VOLUME_1,
            SERVICE_ID_VOLUME,
            "urn:av-openhome-org:service:Volume:",
        )?;
        let url = resolve_url(&self.location, &svc.control_url)?;
        Ok(VolumeClient::with_service_type(
            self.http.clone(),
            url,
            svc.service_type.clone(),
        ))
    }

    pub fn receiver(&self) -> Result<ReceiverClient, ClientError> {
        let svc = self.find_openhome_service(
            SERVICE_RECEIVER_1,
            SERVICE_ID_RECEIVER,
            "urn:av-openhome-org:service:Receiver:",
        )?;
        let url = resolve_url(&self.location, &svc.control_url)?;
        Ok(ReceiverClient::with_service_type(
            self.http.clone(),
            url,
            svc.service_type.clone(),
        ))
    }

    pub fn sender(&self) -> Result<SenderClient, ClientError> {
        let svc = self.find_openhome_service(
            SERVICE_SENDER_1,
            SERVICE_ID_SENDER,
            "urn:av-openhome-org:service:Sender:",
        )?;
        let url = resolve_url(&self.location, &svc.control_url)?;
        Ok(SenderClient::with_service_type(
            self.http.clone(),
            url,
            svc.service_type.clone(),
        ))
    }

    fn find_openhome_service(
        &self,
        expected_urn: &'static str,
        service_id: &str,
        type_prefix: &str,
    ) -> Result<&ServiceDescription, ClientError> {
        self.description
            .find_service(expected_urn)
            .or_else(|| self.description.find_service_by_id(service_id))
            .or_else(|| {
                self.description
                    .services
                    .iter()
                    .find(|s| s.service_type.starts_with(type_prefix))
            })
            .ok_or(ClientError::MissingService(expected_urn))
    }
}
