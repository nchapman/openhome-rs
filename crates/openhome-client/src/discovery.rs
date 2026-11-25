//! SSDP discovery for OpenHome Media devices (`urn:av-openhome-org:device:Source:1`).

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openhome_core::DeviceDescription;
use openhome_core::ssdp::{SSDP_ADDR_V4, SsdpMessage, decode_ssdp_packet, encode_msearch};
use openhome_core::urn::DEVICE_SOURCE_1;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::device::OpenHomeDevice;
use crate::error::ClientError;
use crate::http::HttpClient;

const MAX_CONCURRENT_FETCHES: usize = 16;

/// Options controlling SSDP `M-SEARCH` discovery.
#[derive(Debug, Clone)]
pub struct DiscoveryOptions {
    /// Search target (`ST`), defaults to `urn:av-openhome-org:device:Source:1`.
    pub st: String,
    /// `MX` max-wait seconds sent in the `M-SEARCH` header.
    pub mx_secs: u32,
    /// Total duration to wait for SSDP responses.
    pub timeout: Duration,
    /// Destination SSDP socket address (defaults to `239.255.255.250:1900`).
    pub target_addr: SocketAddrV4,
    /// Local UDP bind address for sending `M-SEARCH` and receiving unicast replies.
    pub bind_addr: SocketAddrV4,
    /// Optional IPv4 multicast outbound interface for multi-homed hosts.
    pub interface: Option<Ipv4Addr>,
    /// When `true` (default), filter out non-OpenHome UPnP devices (such as
    /// UPnP AV `MediaRenderer:1` devices coexisting on the same LAN).
    pub openhome_only: bool,
    /// Number of `M-SEARCH` packets to transmit (default `2` for UDP resilience).
    pub send_count: usize,
    /// HTTP client used to fetch each discovered device's `description.xml`.
    pub http: HttpClient,
}

impl Default for DiscoveryOptions {
    fn default() -> Self {
        Self {
            st: DEVICE_SOURCE_1.to_string(),
            mx_secs: 2,
            timeout: Duration::from_secs(3),
            target_addr: SSDP_ADDR_V4,
            bind_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0),
            interface: None,
            openhome_only: true,
            send_count: 2,
            http: HttpClient::default(),
        }
    }
}

/// Summary and full UPnP description of a discovered OpenHome device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredDevice {
    pub udn: String,
    pub friendly_name: String,
    pub location: String,
    pub description: DeviceDescription,
}

impl DiscoveredDevice {
    /// Convert this discovered device into a connected [`OpenHomeDevice`].
    pub fn into_device(self, http: HttpClient) -> OpenHomeDevice {
        OpenHomeDevice::from_description(self.location, self.description, http)
    }
}

/// Perform an SSDP `M-SEARCH` discovery sweep and fetch `description.xml` for
/// every unique responding device.
pub async fn discover(options: DiscoveryOptions) -> Result<Vec<DiscoveredDevice>, ClientError> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    sock.bind(&SocketAddr::V4(options.bind_addr).into())?;
    if options.target_addr.ip().is_multicast() {
        let _ = sock.set_multicast_loop_v4(true);
        let _ = sock.set_multicast_ttl_v4(2);
        if let Some(iface) = options.interface {
            let _ = sock.set_multicast_if_v4(&iface);
        }
    }
    sock.set_nonblocking(true)?;

    let std_sock: std::net::UdpSocket = sock.into();
    let socket = UdpSocket::from_std(std_sock)?;

    let host_header = options.target_addr.to_string();
    let packet = encode_msearch(&host_header, &options.st, options.mx_secs);
    let dest = SocketAddr::V4(options.target_addr);

    for i in 0..options.send_count.max(1) {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let _ = socket.send_to(packet.as_bytes(), dest).await;
    }

    let deadline = Instant::now() + options.timeout;
    let mut locations = Vec::new();
    let mut seen_locations = HashSet::new();
    let mut buf = [0u8; 4096];

    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        match tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await {
            Ok(Ok((len, _peer))) => {
                if let Ok(SsdpMessage::Response(resp)) = decode_ssdp_packet(&buf[..len])
                    && seen_locations.insert(resp.location.clone())
                {
                    locations.push(resp.location);
                }
            }
            Ok(Err(_)) | Err(_) => break,
        }
    }

    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_FETCHES));
    let mut set = JoinSet::new();
    for location in locations {
        let http = options.http.clone();
        let openhome_only = options.openhome_only;
        let sem = sem.clone();
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.ok()?;
            let dev = OpenHomeDevice::connect_with_http(&location, http)
                .await
                .ok()?;
            if openhome_only && !dev.is_openhome_device() {
                return None;
            }
            let desc = dev.description().clone();
            Some(DiscoveredDevice {
                udn: desc.udn.clone(),
                friendly_name: desc.friendly_name.clone(),
                location,
                description: desc,
            })
        });
    }

    let mut devices = Vec::new();
    let mut seen_udns = HashSet::new();
    while let Some(res) = set.join_next().await {
        if let Ok(Some(dev)) = res
            && seen_udns.insert(dev.udn.clone())
        {
            devices.push(dev);
        }
    }

    Ok(devices)
}
