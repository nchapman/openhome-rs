//! Async SSDP responder and `ssdp:alive` / `ssdp:byebye` announcer.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use openhome_core::ssdp::{
    SSDP_ADDR_V4, SsdpMessage, decode_ssdp_packet, encode_notify_alive, encode_notify_byebye,
    encode_search_response, format_usn, search_target_matches,
};
use openhome_core::urn::{DEVICE_SOURCE_1, UPNP_ROOT_DEVICE};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::watch;

use crate::error::ServerError;

/// Configuration for the server's SSDP responder & announcer.
#[derive(Debug, Clone)]
pub struct SsdpConfig {
    /// Whether SSDP discovery is enabled.
    pub enabled: bool,
    /// Multicast (or test unicast) socket address to bind and announce to.
    pub multicast_addr: SocketAddrV4,
    /// Local IPv4 interface address for multicast membership (`0.0.0.0` = default).
    pub interface: Ipv4Addr,
    /// `CACHE-CONTROL: max-age=<secs>` value in SSDP messages.
    pub max_age_secs: u32,
    /// Interval between periodic `ssdp:alive` refreshes.
    pub announce_interval: Duration,
}

impl Default for SsdpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            multicast_addr: SSDP_ADDR_V4,
            interface: Ipv4Addr::UNSPECIFIED,
            max_age_secs: 1800,
            announce_interval: Duration::from_secs(600),
        }
    }
}

impl SsdpConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }
}

pub(crate) struct SsdpRunner {
    socket: UdpSocket,
    announce_dest: SocketAddr,
    host_header: String,
    location: String,
    udn: String,
    targets: Vec<String>,
    server_header: String,
    max_age_secs: u32,
    announce_interval: Duration,
}

impl SsdpRunner {
    pub fn bind(
        config: &SsdpConfig,
        location: String,
        udn: String,
        service_urns: Vec<&'static str>,
        server_header: String,
    ) -> Result<(Self, SocketAddr), ServerError> {
        let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        sock.set_reuse_address(true)?;
        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        {
            sock.set_reuse_port(true)?;
        }

        let bind_addr = if config.multicast_addr.ip().is_multicast() {
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, config.multicast_addr.port())
        } else {
            config.multicast_addr
        };
        sock.bind(&SocketAddr::V4(bind_addr).into())?;

        if config.multicast_addr.ip().is_multicast() {
            let iface = if config.interface.is_unspecified() {
                default_outbound_ipv4().unwrap_or(Ipv4Addr::UNSPECIFIED)
            } else {
                config.interface
            };
            sock.join_multicast_v4(config.multicast_addr.ip(), &iface)?;
            let _ = sock.set_multicast_loop_v4(true);
            let _ = sock.set_multicast_ttl_v4(2);
            if !iface.is_unspecified() {
                let _ = sock.set_multicast_if_v4(&iface);
            }
        }
        sock.set_nonblocking(true)?;

        let std_sock: std::net::UdpSocket = sock.into();
        let local_addr = std_sock.local_addr()?;
        let socket = UdpSocket::from_std(std_sock)?;

        let announce_dest = if config.multicast_addr.ip().is_multicast() {
            SocketAddr::V4(config.multicast_addr)
        } else {
            local_addr
        };

        let mut targets = Vec::with_capacity(3 + service_urns.len());
        targets.push(UPNP_ROOT_DEVICE.to_string());
        targets.push(udn.clone());
        targets.push(DEVICE_SOURCE_1.to_string());
        for urn in service_urns {
            targets.push(urn.to_string());
        }

        let host_header = announce_dest.to_string();

        Ok((
            Self {
                socket,
                announce_dest,
                host_header,
                location,
                udn,
                targets,
                server_header,
                max_age_secs: config.max_age_secs,
                announce_interval: config.announce_interval,
            },
            local_addr,
        ))
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) {
        self.send_alive_all().await;

        let mut interval = tokio::time::interval(self.announce_interval);
        interval.tick().await; // consume immediate first tick

        let mut buf = [0u8; 4096];
        loop {
            tokio::select! {
                res = shutdown.changed() => {
                    if res.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => {
                    self.send_alive_all().await;
                }
                recv = self.socket.recv_from(&mut buf) => {
                    if let Ok((len, peer)) = recv
                        && let Ok(SsdpMessage::MSearch(search)) = decode_ssdp_packet(&buf[..len])
                        && search.man.as_deref().is_some_and(|m| {
                            m.split(';')
                                .next()
                                .unwrap_or("")
                                .trim()
                                .trim_matches('"')
                                .eq_ignore_ascii_case("ssdp:discover")
                        })
                    {
                        self.handle_msearch(&search.st, peer).await;
                    }
                }
            }
        }

        self.send_byebye_all().await;
    }

    async fn send_alive_all(&self) {
        if !self.announce_dest.ip().is_multicast() {
            return;
        }
        for target in &self.targets {
            let usn = format_usn(&self.udn, target);
            let msg = encode_notify_alive(
                &self.host_header,
                &self.location,
                target,
                &usn,
                &self.server_header,
                self.max_age_secs,
            );
            let _ = self
                .socket
                .send_to(msg.as_bytes(), self.announce_dest)
                .await;
        }
    }

    async fn send_byebye_all(&self) {
        if !self.announce_dest.ip().is_multicast() {
            return;
        }
        for target in &self.targets {
            let usn = format_usn(&self.udn, target);
            let msg = encode_notify_byebye(&self.host_header, target, &usn);
            let _ = self
                .socket
                .send_to(msg.as_bytes(), self.announce_dest)
                .await;
        }
    }

    async fn handle_msearch(&self, st: &str, peer: SocketAddr) {
        for target in &self.targets {
            if search_target_matches(st, target) {
                let usn = format_usn(&self.udn, target);
                let resp = encode_search_response(
                    &self.location,
                    target,
                    &usn,
                    &self.server_header,
                    self.max_age_secs,
                );
                let _ = self.socket.send_to(resp.as_bytes(), peer).await;
            }
        }
    }
}

fn default_outbound_ipv4() -> Option<Ipv4Addr> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    sock.connect("192.0.2.1:80").ok()?;
    match sock.local_addr().ok()?.ip() {
        std::net::IpAddr::V4(ip) if !ip.is_unspecified() => Some(ip),
        _ => None,
    }
}
