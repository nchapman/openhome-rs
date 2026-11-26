//! High-level [`OpenHomeServer`] and [`OpenHomeServerBuilder`].

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::Arc;

use bytes::Bytes;
use openhome_core::DeviceDescription;
use openhome_core::urn::DEVICE_SOURCE_1;
use openhome_core::xml::encode_device_description;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::dispatch::{
    SLUG_INFO, SLUG_PLAYLIST, SLUG_PRODUCT, SLUG_RECEIVER, SLUG_SENDER, SLUG_TIME, SLUG_VOLUME,
    ServiceRegistry,
};
use crate::error::ServerError;
use crate::event::EventHub;
use crate::http::{HttpState, notify_all_services, serve_http};
use crate::service::{
    InfoService, PlaylistService, ProductService, ReceiverService, SenderService, TimeService,
    VolumeService,
};
use crate::ssdp::{SsdpConfig, SsdpRunner};

const DEFAULT_SERVER_HEADER: &str = "OpenHome-RS/0.1 UPnP/1.1 openhome-server/0.1";

/// Builder for configuring and launching an [`OpenHomeServer`].
pub struct OpenHomeServerBuilder {
    udn: Option<String>,
    friendly_name: Option<String>,
    bind_addr: SocketAddr,
    advertise_host: Option<String>,
    server_header: String,
    ssdp: SsdpConfig,
    product: Arc<dyn ProductService>,
    playlist: Option<Arc<dyn PlaylistService>>,
    info: Option<Arc<dyn InfoService>>,
    time: Option<Arc<dyn TimeService>>,
    volume: Option<Arc<dyn VolumeService>>,
    receiver: Option<Arc<dyn ReceiverService>>,
    sender: Option<Arc<dyn SenderService>>,
}

impl OpenHomeServerBuilder {
    pub fn new(product: Arc<dyn ProductService>) -> Self {
        Self {
            udn: None,
            friendly_name: None,
            bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            advertise_host: None,
            server_header: DEFAULT_SERVER_HEADER.to_string(),
            ssdp: SsdpConfig::default(),
            product,
            playlist: None,
            info: None,
            time: None,
            volume: None,
            receiver: None,
            sender: None,
        }
    }

    /// Set the UPnP UDN (e.g. `"uuid:..."` or a raw UUID string).
    pub fn udn(mut self, udn: impl Into<String>) -> Self {
        self.udn = Some(normalize_udn(&udn.into()));
        self
    }

    /// Set the UPnP `<friendlyName>` advertised in `description.xml`.
    pub fn friendly_name(mut self, name: impl Into<String>) -> Self {
        self.friendly_name = Some(name.into());
        self
    }

    /// Bind address for the HTTP/1.1 SOAP + GENA server (default `0.0.0.0:0`).
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }

    /// Override the host/IP placed in the SSDP `LOCATION` URL.
    pub fn advertise_host(mut self, host: impl Into<String>) -> Self {
        self.advertise_host = Some(host.into());
        self
    }

    /// Override the HTTP/SSDP `SERVER` header string.
    pub fn server_header(mut self, header: impl Into<String>) -> Self {
        self.server_header = header.into();
        self
    }

    /// Configure SSDP discovery (or pass [`SsdpConfig::disabled()`]).
    pub fn ssdp(mut self, config: SsdpConfig) -> Self {
        self.ssdp = config;
        self
    }

    pub fn playlist(mut self, svc: Arc<dyn PlaylistService>) -> Self {
        self.playlist = Some(svc);
        self
    }

    pub fn info(mut self, svc: Arc<dyn InfoService>) -> Self {
        self.info = Some(svc);
        self
    }

    pub fn time(mut self, svc: Arc<dyn TimeService>) -> Self {
        self.time = Some(svc);
        self
    }

    pub fn volume(mut self, svc: Arc<dyn VolumeService>) -> Self {
        self.volume = Some(svc);
        self
    }

    pub fn receiver(mut self, svc: Arc<dyn ReceiverService>) -> Self {
        self.receiver = Some(svc);
        self
    }

    pub fn sender(mut self, svc: Arc<dyn SenderService>) -> Self {
        self.sender = Some(svc);
        self
    }

    /// Start the HTTP server (and SSDP responder if enabled).
    pub async fn start(self) -> Result<OpenHomeServer, ServerError> {
        let udn = self
            .udn
            .unwrap_or_else(|| format!("uuid:{}", Uuid::new_v4()));

        let registry = ServiceRegistry {
            product: self.product,
            playlist: self.playlist,
            info: self.info,
            time: self.time,
            volume: self.volume,
            receiver: self.receiver,
            sender: self.sender,
        };

        let pinfo = registry.product.product_info();
        let friendly_name = self.friendly_name.unwrap_or_else(|| {
            if !pinfo.product_room.is_empty() && !pinfo.product_name.is_empty() {
                format!("{} ({})", pinfo.product_room, pinfo.product_name)
            } else if !pinfo.product_name.is_empty() {
                pinfo.product_name.clone()
            } else {
                "OpenHome Renderer".to_string()
            }
        });

        let description = DeviceDescription {
            url_base: None,
            device_type: DEVICE_SOURCE_1.to_string(),
            friendly_name,
            manufacturer: if pinfo.manufacturer_name.is_empty() {
                "openhome-rs".to_string()
            } else {
                pinfo.manufacturer_name.clone()
            },
            manufacturer_url: non_empty(&pinfo.manufacturer_url),
            model_description: non_empty(&pinfo.model_info),
            model_name: if pinfo.model_name.is_empty() {
                "OpenHome Renderer".to_string()
            } else {
                pinfo.model_name.clone()
            },
            model_number: Some(env!("CARGO_PKG_VERSION").to_string()),
            model_url: non_empty(&pinfo.model_url),
            serial_number: None,
            udn: udn.clone(),
            presentation_url: non_empty(&pinfo.product_url),
            services: registry.service_descriptions(),
        };
        let description_xml = Bytes::from(encode_device_description(&description));

        let listener = TcpListener::bind(self.bind_addr).await?;
        let local_addr = listener.local_addr()?;

        let advertise_host = self
            .advertise_host
            .unwrap_or_else(|| resolve_advertise_host(local_addr.ip()));
        let description_url = format!(
            "http://{}:{}/description.xml",
            format_host(&advertise_host),
            local_addr.port()
        );

        let events = EventHub::new();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_header = Arc::new(self.server_header);
        let http_state = HttpState {
            description_xml,
            registry: registry.clone(),
            events: events.clone(),
            server_header: server_header.clone(),
        };

        let (ssdp_addr, ssdp_task) = if self.ssdp.enabled {
            let (runner, bound_addr) = SsdpRunner::bind(
                &self.ssdp,
                description_url.clone(),
                udn.clone(),
                registry.service_type_urns(),
                (*server_header).clone(),
            )?;
            let task = tokio::spawn(runner.run(shutdown_rx.clone()));
            (Some(bound_addr), Some(task))
        } else {
            (None, None)
        };

        let http_task = tokio::spawn(serve_http(listener, http_state, shutdown_rx));

        Ok(OpenHomeServer {
            udn,
            local_addr,
            ssdp_addr,
            description_url,
            registry,
            events,
            shutdown_tx,
            http_task: Some(http_task),
            ssdp_task,
        })
    }
}

/// Cloneable handle for pushing out-of-band GENA notifications from audio backends
/// without holding a reference to [`OpenHomeServer`].
#[derive(Clone)]
pub struct ServerNotifier {
    registry: ServiceRegistry,
    events: EventHub,
}

impl std::fmt::Debug for ServerNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerNotifier")
            .field("events", &self.events)
            .finish()
    }
}

impl ServerNotifier {
    /// Push updated GENA state for a single service slug (if any subscriber is active).
    pub fn notify_service(&self, slug: &str) {
        if !self.events.has_subscribers_for(slug) {
            return;
        }
        if let Some(snapshot) = self.registry.state_snapshot(slug) {
            self.events.notify_snapshot(slug, &snapshot);
        }
    }

    pub fn notify_product(&self) {
        self.notify_service(SLUG_PRODUCT);
    }

    pub fn notify_playlist(&self) {
        self.notify_service(SLUG_PLAYLIST);
    }

    pub fn notify_info(&self) {
        self.notify_service(SLUG_INFO);
    }

    pub fn notify_time(&self) {
        self.notify_service(SLUG_TIME);
    }

    pub fn notify_volume(&self) {
        self.notify_service(SLUG_VOLUME);
    }

    pub fn notify_receiver(&self) {
        self.notify_service(SLUG_RECEIVER);
    }

    pub fn notify_sender(&self) {
        self.notify_service(SLUG_SENDER);
    }

    pub fn notify_all(&self) {
        notify_all_services(&self.registry, &self.events);
    }
}

/// Running OpenHome Media server handle.
pub struct OpenHomeServer {
    udn: String,
    local_addr: SocketAddr,
    ssdp_addr: Option<SocketAddr>,
    description_url: String,
    registry: ServiceRegistry,
    events: EventHub,
    shutdown_tx: watch::Sender<bool>,
    http_task: Option<JoinHandle<()>>,
    ssdp_task: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for OpenHomeServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenHomeServer")
            .field("udn", &self.udn)
            .field("local_addr", &self.local_addr)
            .field("description_url", &self.description_url)
            .field("events", &self.events)
            .finish()
    }
}

impl std::fmt::Debug for OpenHomeServerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenHomeServerBuilder")
            .field("udn", &self.udn)
            .field("friendly_name", &self.friendly_name)
            .field("bind_addr", &self.bind_addr)
            .finish()
    }
}

impl OpenHomeServer {
    pub fn builder(product: Arc<dyn ProductService>) -> OpenHomeServerBuilder {
        OpenHomeServerBuilder::new(product)
    }

    pub fn udn(&self) -> &str {
        &self.udn
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn ssdp_addr(&self) -> Option<SocketAddr> {
        self.ssdp_addr
    }

    pub fn description_url(&self) -> &str {
        &self.description_url
    }

    pub fn events(&self) -> &EventHub {
        &self.events
    }

    /// Return a lightweight, cloneable [`ServerNotifier`] handle for pushing
    /// out-of-band GENA notifications from background audio tasks.
    pub fn notifier(&self) -> ServerNotifier {
        ServerNotifier {
            registry: self.registry.clone(),
            events: self.events.clone(),
        }
    }

    /// Push any changed state variables for a specific service slug to GENA subscribers.
    pub fn notify_service(&self, slug: &str) {
        if !self.events.has_subscribers_for(slug) {
            return;
        }
        if let Some(snapshot) = self.registry.state_snapshot(slug) {
            self.events.notify_snapshot(slug, &snapshot);
        }
    }

    pub fn notify_product(&self) {
        self.notify_service(SLUG_PRODUCT);
    }

    pub fn notify_playlist(&self) {
        self.notify_service(SLUG_PLAYLIST);
    }

    pub fn notify_info(&self) {
        self.notify_service(SLUG_INFO);
    }

    pub fn notify_time(&self) {
        self.notify_service(SLUG_TIME);
    }

    pub fn notify_volume(&self) {
        self.notify_service(SLUG_VOLUME);
    }

    pub fn notify_receiver(&self) {
        self.notify_service(SLUG_RECEIVER);
    }

    pub fn notify_sender(&self) {
        self.notify_service(SLUG_SENDER);
    }

    /// Push any changed state variables across all registered services.
    pub fn notify_all(&self) {
        notify_all_services(&self.registry, &self.events);
    }

    /// Gracefully stop HTTP serving, emit SSDP `ssdp:byebye`, and close GENA subscriptions.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        self.events.clear();
        if let Some(task) = self.ssdp_task.take() {
            let _ = task.await;
        }
        if let Some(task) = self.http_task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for OpenHomeServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        self.events.clear();
    }
}

fn normalize_udn(raw: &str) -> String {
    if raw.starts_with("uuid:") {
        raw.to_string()
    } else {
        format!("uuid:{raw}")
    }
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn resolve_advertise_host(ip: IpAddr) -> String {
    if !ip.is_unspecified() {
        return ip.to_string();
    }
    // Best-effort outbound IPv4 discovery without sending packets.
    if let Ok(sock) = StdUdpSocket::bind("0.0.0.0:0")
        && sock.connect("192.0.2.1:80").is_ok()
        && let Ok(local) = sock.local_addr()
        && !local.ip().is_unspecified()
    {
        return local.ip().to_string();
    }
    Ipv4Addr::LOCALHOST.to_string()
}

fn format_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryRenderer;

    #[test]
    fn helpers_normalize_udn_format_ipv6_and_filter_empty() {
        assert_eq!(normalize_udn("abc-123"), "uuid:abc-123");
        assert_eq!(normalize_udn("uuid:already"), "uuid:already");
        assert_eq!(non_empty(""), None);
        assert_eq!(non_empty("hi"), Some("hi".into()));
        assert_eq!(format_host("127.0.0.1"), "127.0.0.1");
        assert_eq!(format_host("::1"), "[::1]");
        assert_eq!(format_host("[::1]"), "[::1]");
        assert_eq!(
            resolve_advertise_host(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            "127.0.0.1"
        );
    }

    #[tokio::test]
    async fn builder_setters_notify_helpers_and_shutdown_work() {
        let renderer = MemoryRenderer::new("Studio", "Deck");
        let server = renderer
            .into_server_builder()
            .udn("custom-udn-42")
            .friendly_name("Custom Name")
            .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .advertise_host("127.0.0.1")
            .server_header("TestOS/1.0 UPnP/1.1 Test/1.0")
            .ssdp(SsdpConfig::disabled())
            .start()
            .await
            .unwrap();

        assert_eq!(server.udn(), "uuid:custom-udn-42");
        assert!(server.ssdp_addr().is_none());
        assert!(server.description_url().starts_with("http://127.0.0.1:"));

        // All notify_* helpers succeed cleanly with zero subscribers (on server & ServerNotifier).
        assert!(format!("{server:?}").contains("OpenHomeServer"));
        let notifier = server.notifier();
        assert!(format!("{notifier:?}").contains("ServerNotifier"));
        notifier.notify_product();
        notifier.notify_playlist();
        notifier.notify_info();
        notifier.notify_time();
        notifier.notify_volume();
        notifier.notify_receiver();
        notifier.notify_sender();
        notifier.notify_all();

        server.notify_product();
        server.notify_playlist();
        server.notify_info();
        server.notify_time();
        server.notify_volume();
        server.notify_receiver();
        server.notify_sender();
        server.notify_all();
        server.notify_service("NonExistent");

        server.shutdown().await;
    }
}
