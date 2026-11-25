//! Async OHZ zone & preset discovery (`ZoneResponder`, `resolve_zone`, `query_preset`).

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::SongcastError;
use crate::ohz::OhzMessage;
use crate::uri::SongcastUri;

/// Shared zone & preset registry answered by [`ZoneResponder`].
#[derive(Debug, Default)]
struct ZoneTable {
    zones: HashMap<String, String>,
    presets: HashMap<u32, String>,
}

/// Async UDP responder for OHZ `ZoneQuery` and `PresetQuery` messages.
pub struct ZoneResponder {
    local_addr: SocketAddr,
    table: Arc<RwLock<ZoneTable>>,
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ZoneResponder {
    /// Bind and start an OHZ responder on `bind_addr`.
    ///
    /// When `bind_addr.ip()` is multicast (e.g. `239.255.255.250:51972`), joins the
    /// multicast group on `interface` (`Ipv4Addr::UNSPECIFIED` for default route) with
    /// `SO_REUSEADDR` enabled. For tests, pass `127.0.0.1:0` to bind an ephemeral loopback port.
    pub async fn bind(bind_addr: SocketAddrV4, interface: Ipv4Addr) -> Result<Self, SongcastError> {
        let socket = bind_udp_v4(bind_addr, interface, 1)?;
        let local_addr = socket.local_addr()?;
        let table = Arc::new(RwLock::new(ZoneTable::default()));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let table_bg = table.clone();
        let reply_multicast = bind_addr.ip().is_multicast();
        let task = tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    recv = socket.recv_from(&mut buf) => {
                        let Ok((len, peer)) = recv else { continue };
                        let Ok(msg) = OhzMessage::decode(&buf[..len]) else { continue };
                        let reply = match msg {
                            OhzMessage::ZoneQuery { zone } => {
                                let uri = table_bg.read().unwrap().zones.get(&zone).cloned();
                                uri.map(|uri| OhzMessage::ZoneUri { zone, uri })
                            }
                            OhzMessage::PresetQuery { preset } => {
                                let metadata = table_bg.read().unwrap().presets.get(&preset).cloned();
                                metadata.map(|metadata| OhzMessage::PresetInfo { preset, metadata })
                            }
                            _ => None,
                        };
                        if let Some(reply) = reply
                            && let Ok(bytes) = reply.encode()
                        {
                            let dest = if reply_multicast {
                                SocketAddr::V4(bind_addr)
                            } else {
                                peer
                            };
                            let _ = socket.send_to(&bytes, dest).await;
                        }
                    }
                }
            }
        });

        Ok(Self {
            local_addr,
            table,
            shutdown_tx,
            task,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Register or update the stream URI (e.g. `ohm://239.253.0.1:51970`) for `zone`.
    pub fn set_zone_uri(&self, zone: impl Into<String>, uri: impl Into<String>) {
        self.table
            .write()
            .unwrap()
            .zones
            .insert(zone.into(), uri.into());
    }

    /// Remove a zone mapping.
    pub fn remove_zone(&self, zone: &str) {
        self.table.write().unwrap().zones.remove(zone);
    }

    /// Register or update DIDL-Lite metadata for a Songcast preset index.
    pub fn set_preset(&self, preset: u32, metadata: impl Into<String>) {
        self.table
            .write()
            .unwrap()
            .presets
            .insert(preset, metadata.into());
    }

    /// Shut down the responder task cleanly.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.task.await;
    }
}

/// Resolve an OHZ zone name to its active [`SongcastUri`] by querying `endpoint`.
///
/// For unicast `endpoint`s, replies are accepted only from `endpoint`.
/// Note: per the OpenHome Songcast specification, multicast OHZ discovery is
/// unauthenticated and intended for trusted local networks only.
pub async fn resolve_zone(
    endpoint: SocketAddrV4,
    zone: &str,
    timeout: Duration,
) -> Result<SongcastUri, SongcastError> {
    let is_multicast = endpoint.ip().is_multicast();
    // Per ohSongcast / ohPipeline, OHZ ZoneUri replies are multicast to the OHZ group
    // (239.255.255.250:51972), so multicast queries must join the group to receive them.
    let sock = if is_multicast {
        bind_udp_v4(endpoint, Ipv4Addr::UNSPECIFIED, 2)?
    } else {
        UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)).await?
    };
    let query = OhzMessage::ZoneQuery {
        zone: zone.to_string(),
    }
    .encode()?;

    let deadline = tokio::time::Instant::now() + timeout;
    let mut buf = [0u8; 4096];
    let mut resend_interval = tokio::time::interval(Duration::from_millis(150));

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(SongcastError::ZoneNotFound(zone.to_string()));
        }
        tokio::select! {
            _ = resend_interval.tick() => {
                sock.send_to(&query, SocketAddr::V4(endpoint)).await?;
            }
            res = tokio::time::timeout(remaining, sock.recv_from(&mut buf)) => {
                match res {
                    Ok(Ok((len, peer))) => {
                        if !is_multicast && peer != SocketAddr::V4(endpoint) {
                            continue;
                        }
                        if let Ok(OhzMessage::ZoneUri { zone: resp_zone, uri }) = OhzMessage::decode(&buf[..len])
                            && resp_zone == zone
                        {
                            return SongcastUri::parse(&uri);
                        }
                    }
                    Ok(Err(e)) => return Err(SongcastError::Io(e)),
                    Err(_) => return Err(SongcastError::ZoneNotFound(zone.to_string())),
                }
            }
        }
    }
}

/// Query an OHZ responder for preset DIDL-Lite metadata.
///
/// Resends the UDP query periodically until `timeout` elapses. For unicast
/// `endpoint`s, replies are accepted only from `endpoint`.
pub async fn query_preset(
    endpoint: SocketAddrV4,
    preset: u32,
    timeout: Duration,
) -> Result<Option<String>, SongcastError> {
    let is_multicast = endpoint.ip().is_multicast();
    let sock = if is_multicast {
        bind_udp_v4(endpoint, Ipv4Addr::UNSPECIFIED, 2)?
    } else {
        UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)).await?
    };
    let query = OhzMessage::PresetQuery { preset }.encode()?;

    let deadline = tokio::time::Instant::now() + timeout;
    let mut buf = [0u8; 4096];
    let mut resend_interval = tokio::time::interval(Duration::from_millis(150));

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(None);
        }
        tokio::select! {
            _ = resend_interval.tick() => {
                sock.send_to(&query, SocketAddr::V4(endpoint)).await?;
            }
            res = tokio::time::timeout(remaining, sock.recv_from(&mut buf)) => {
                match res {
                    Ok(Ok((len, peer))) => {
                        if !is_multicast && peer != SocketAddr::V4(endpoint) {
                            continue;
                        }
                        if let Ok(OhzMessage::PresetInfo {
                            preset: resp_preset,
                            metadata,
                        }) = OhzMessage::decode(&buf[..len])
                            && resp_preset == preset
                        {
                            return Ok(Some(metadata));
                        }
                    }
                    Ok(Err(e)) => return Err(SongcastError::Io(e)),
                    Err(_) => return Ok(None),
                }
            }
        }
    }
}

/// Shared UDP IPv4 socket helper supporting both multicast groups and loopback unicast.
pub(crate) fn bind_udp_v4(
    bind_addr: SocketAddrV4,
    interface: Ipv4Addr,
    ttl: u32,
) -> Result<UdpSocket, SongcastError> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    #[cfg(unix)]
    let _ = sock.set_reuse_port(true);

    if bind_addr.ip().is_multicast() {
        let any = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, bind_addr.port());
        sock.bind(&any.into())?;
        sock.join_multicast_v4(bind_addr.ip(), &interface)?;
        sock.set_multicast_loop_v4(true)?;
        sock.set_multicast_ttl_v4(ttl)?;
        if !interface.is_unspecified() {
            let _ = sock.set_multicast_if_v4(&interface);
        }
    } else {
        sock.bind(&bind_addr.into())?;
    }

    sock.set_nonblocking(true)?;
    let std_sock: std::net::UdpSocket = sock.into();
    Ok(UdpSocket::from_std(std_sock)?)
}
