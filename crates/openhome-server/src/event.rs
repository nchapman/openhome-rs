//! GENA (General Event Notification Architecture) subscription manager and
//! outbound `NOTIFY` publisher for OpenHome services.

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::Request;
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, HOST};
use hyper_util::rt::TokioIo;
use openhome_core::gena::encode_propertyset;
use tokio::net::TcpStream;
use tokio::sync::{Notify, watch};
use uuid::Uuid;

const DEFAULT_TIMEOUT_SECS: u32 = 1800;
const MIN_TIMEOUT_SECS: u32 = 1;
const MAX_TIMEOUT_SECS: u32 = 86_400;
const NOTIFY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_SUBSCRIPTIONS: usize = 256;
const MAX_CALLBACKS_PER_SUB: usize = 4;

/// Reason a GENA subscription request was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeError {
    /// No callback URL passed validation (e.g. non-HTTP or peer IP mismatch).
    InvalidCallbacks,
    /// Active subscription limit reached.
    TooManySubscriptions,
}

struct DeliveryState {
    initial_sent: bool,
    next_seq: u32,
    confirmed_state: BTreeMap<String, String>,
    latest_state: BTreeMap<String, String>,
}

struct Subscription {
    service_slug: String,
    expires_at: Instant,
    delivery: Arc<Mutex<DeliveryState>>,
    wake: Arc<Notify>,
    cancel_tx: watch::Sender<bool>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let _ = self.cancel_tx.send(true);
        self.wake.notify_one();
    }
}

/// Manages active GENA event subscriptions across all registered OpenHome services.
#[derive(Clone, Default)]
pub struct EventHub {
    subs: Arc<RwLock<HashMap<String, Subscription>>>,
}

impl EventHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new GENA subscription for `service_slug` and queue the initial
    /// `SEQ=0` notification carrying the full `initial_state`.
    ///
    /// When `peer_ip` is provided, callback URLs are restricted to literal IP
    /// endpoints matching `peer_ip` to prevent GENA CALLBACK SSRF.
    ///
    /// Returns `Ok((sid, granted_timeout_secs))`.
    pub fn subscribe(
        &self,
        service_slug: &str,
        callbacks: Vec<String>,
        peer_ip: Option<IpAddr>,
        requested_timeout_secs: Option<u32>,
        initial_state: BTreeMap<String, String>,
    ) -> Result<(String, u32), SubscribeError> {
        let valid_callbacks: Vec<String> = callbacks
            .into_iter()
            .filter(|url| is_allowed_callback(url, peer_ip))
            .take(MAX_CALLBACKS_PER_SUB)
            .collect();

        if valid_callbacks.is_empty() {
            return Err(SubscribeError::InvalidCallbacks);
        }

        let timeout_secs = clamp_timeout(requested_timeout_secs);
        let sid = format!("uuid:{}", Uuid::new_v4());

        let delivery = Arc::new(Mutex::new(DeliveryState {
            initial_sent: false,
            next_seq: 0,
            confirmed_state: BTreeMap::new(),
            latest_state: initial_state,
        }));
        let wake = Arc::new(Notify::new());
        let (cancel_tx, cancel_rx) = watch::channel(false);

        let sub = Subscription {
            service_slug: service_slug.to_string(),
            expires_at: Instant::now() + Duration::from_secs(u64::from(timeout_secs)),
            delivery: delivery.clone(),
            wake: wake.clone(),
            cancel_tx,
        };

        {
            let mut map = self.subs.write().expect("EventHub lock poisoned");
            prune_expired(&mut map);
            if map.len() >= MAX_SUBSCRIPTIONS {
                return Err(SubscribeError::TooManySubscriptions);
            }
            map.insert(sid.clone(), sub);
        }

        // Trigger initial SEQ=0 delivery.
        wake.notify_one();

        let sid_for_task = sid.clone();
        tokio::spawn(delivery_loop(
            sid_for_task,
            valid_callbacks,
            delivery,
            wake,
            cancel_rx,
        ));

        Ok((sid, timeout_secs))
    }

    /// Renew an existing subscription by `sid` for `service_slug`.
    /// Returns `Some(granted_timeout_secs)` if found and active, or `None`.
    pub fn renew(
        &self,
        service_slug: &str,
        sid: &str,
        requested_timeout_secs: Option<u32>,
    ) -> Option<u32> {
        let timeout_secs = clamp_timeout(requested_timeout_secs);
        let mut map = self.subs.write().expect("EventHub lock poisoned");
        prune_expired(&mut map);
        let sub = map.get_mut(sid)?;
        if sub.service_slug != service_slug {
            return None;
        }
        sub.expires_at = Instant::now() + Duration::from_secs(u64::from(timeout_secs));
        Some(timeout_secs)
    }

    /// Remove an existing subscription by `sid` for `service_slug`.
    /// Returns `true` if a matching subscription existed.
    pub fn unsubscribe(&self, service_slug: &str, sid: &str) -> bool {
        let mut map = self.subs.write().expect("EventHub lock poisoned");
        prune_expired(&mut map);
        if map
            .get(sid)
            .is_some_and(|sub| sub.service_slug == service_slug)
        {
            map.remove(sid);
            true
        } else {
            false
        }
    }

    /// Update each active subscriber's latest state snapshot for `service_slug`
    /// and wake its delivery task so any unconfirmed changes are emitted.
    pub fn notify_snapshot(&self, service_slug: &str, current_state: &BTreeMap<String, String>) {
        let mut map = self.subs.write().expect("EventHub lock poisoned");
        prune_expired(&mut map);

        for sub in map.values() {
            if sub.service_slug != service_slug {
                continue;
            }
            let should_wake = {
                let mut state = sub.delivery.lock().expect("DeliveryState lock poisoned");
                state.latest_state.clone_from(current_state);
                !state.initial_sent || state.latest_state != state.confirmed_state
            };
            if should_wake {
                sub.wake.notify_one();
            }
        }
    }

    /// Number of currently active (non-expired) subscriptions across all services.
    pub fn active_subscription_count(&self) -> usize {
        let mut map = self.subs.write().expect("EventHub lock poisoned");
        prune_expired(&mut map);
        map.len()
    }

    /// Drop all active subscriptions (stops all delivery loops).
    pub fn clear(&self) {
        let mut map = self.subs.write().expect("EventHub lock poisoned");
        map.clear();
    }
}

fn clamp_timeout(requested: Option<u32>) -> u32 {
    requested
        .unwrap_or(DEFAULT_TIMEOUT_SECS)
        .clamp(MIN_TIMEOUT_SECS, MAX_TIMEOUT_SECS)
}

fn prune_expired(map: &mut HashMap<String, Subscription>) {
    let now = Instant::now();
    map.retain(|_, sub| sub.expires_at > now);
}

fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 => v4,
    }
}

fn is_allowed_callback(url: &str, peer_ip: Option<IpAddr>) -> bool {
    let Some((host_port, _path)) = parse_http_url(url) else {
        return false;
    };
    let Ok(sock_addr) = host_port.parse::<SocketAddr>() else {
        // Reject arbitrary DNS hostnames when preventing SSRF; UPnP control
        // points always advertise literal LAN IP addresses in CALLBACK.
        return false;
    };
    let cb_ip = normalize_ip(sock_addr.ip());
    if cb_ip.is_unspecified() || sock_addr.port() == 0 {
        return false;
    }
    if let Some(peer) = peer_ip
        && cb_ip != normalize_ip(peer)
    {
        return false;
    }
    true
}

async fn delivery_loop(
    sid: String,
    callbacks: Vec<String>,
    delivery: Arc<Mutex<DeliveryState>>,
    wake: Arc<Notify>,
    mut cancel: watch::Receiver<bool>,
) {
    // Yield once so the SUBSCRIBE HTTP response can be flushed before SEQ=0.
    tokio::task::yield_now().await;

    loop {
        if *cancel.borrow() {
            break;
        }

        // Build the next outbound payload by diffing latest_state vs confirmed_state.
        let pending = {
            let state = delivery.lock().expect("DeliveryState lock poisoned");
            if !state.initial_sent {
                Some((0u32, state.latest_state.clone()))
            } else {
                let mut delta = BTreeMap::new();
                for (k, v) in &state.latest_state {
                    if state.confirmed_state.get(k) != Some(v) {
                        delta.insert(k.clone(), v.clone());
                    }
                }
                if delta.is_empty() {
                    None
                } else {
                    Some((state.next_seq, delta))
                }
            }
        };

        let Some((seq, sent_props)) = pending else {
            tokio::select! {
                _ = wake.notified() => continue,
                _ = cancel.changed() => break,
            }
        };

        let body = encode_propertyset(&sent_props);
        let mut delivered = false;
        for url in &callbacks {
            if send_notify(url, &sid, seq, &body).await.is_ok() {
                delivered = true;
                break;
            }
        }

        if delivered {
            let mut state = delivery.lock().expect("DeliveryState lock poisoned");
            if !state.initial_sent {
                state.initial_sent = true;
                state.next_seq = 1;
                state.confirmed_state = sent_props;
            } else {
                // Per UPnP GENA 1.1: sequence wraps from u32::MAX back to 1 (never 0).
                state.next_seq = if seq == u32::MAX { 1 } else { seq + 1 };
                for (k, v) in sent_props {
                    state.confirmed_state.insert(k, v);
                }
            }
        } else {
            // Transient delivery failure: confirmed_state is NOT advanced so the
            // unconfirmed delta is preserved for retry or the next state change.
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                _ = wake.notified() => {}
                _ = cancel.changed() => break,
            }
        }
    }
}

async fn send_notify(url: &str, sid: &str, seq: u32, body: &str) -> Result<(), ()> {
    let (host_port, path) = parse_http_url(url).ok_or(())?;

    let stream = tokio::time::timeout(NOTIFY_TIMEOUT, TcpStream::connect(&host_port))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;

    let io = TokioIo::new(stream);
    let (mut sender, conn) =
        tokio::time::timeout(NOTIFY_TIMEOUT, hyper::client::conn::http1::handshake(io))
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;

    tokio::spawn(async move {
        let _ = conn.await;
    });

    let body_bytes = Bytes::from(body.as_bytes().to_vec());
    let req = Request::builder()
        .method("NOTIFY")
        .uri(path)
        .header(HOST, &host_port)
        .header(CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
        .header(CONTENT_LENGTH, body_bytes.len().to_string())
        .header("NT", "upnp:event")
        .header("NTS", "upnp:propchange")
        .header("SID", sid)
        .header("SEQ", seq.to_string())
        .header("CONNECTION", "close")
        .body(Full::new(body_bytes))
        .map_err(|_| ())?;

    let resp = tokio::time::timeout(NOTIFY_TIMEOUT, sender.send_request(req))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;

    let status = resp.status();
    let limited = Limited::new(resp.into_body(), 64 * 1024);
    let _ = tokio::time::timeout(NOTIFY_TIMEOUT, limited.collect()).await;

    if status.is_success() { Ok(()) } else { Err(()) }
}

fn parse_http_url(url: &str) -> Option<(String, String)> {
    let rest = url.strip_prefix("http://")?;
    let (authority, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return None;
    }
    let has_port = if let Some(after_bracket) = authority.strip_prefix('[') {
        after_bracket
            .split_once(']')
            .is_some_and(|(_, tail)| tail.starts_with(':'))
    } else {
        authority.contains(':')
    };
    let host_port = if has_port {
        authority.to_string()
    } else {
        format!("{authority}:80")
    };
    Some((host_port, path.to_string()))
}
