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

    /// Returns `true` if at least one non-expired subscription exists for `service_slug`.
    pub fn has_subscribers_for(&self, service_slug: &str) -> bool {
        let now = Instant::now();
        let map = self.subs.read().expect("EventHub lock poisoned");
        map.values()
            .any(|sub| sub.service_slug == service_slug && sub.expires_at > now)
    }

    /// Update each active subscriber's latest state snapshot for `service_slug`
    /// and wake its delivery task so any unconfirmed changes are emitted.
    pub fn notify_snapshot(&self, service_slug: &str, current_state: &BTreeMap<String, String>) {
        let now = Instant::now();
        let map = self.subs.read().expect("EventHub lock poisoned");

        for sub in map.values() {
            if sub.service_slug != service_slug || sub.expires_at <= now {
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
    let mut consecutive_failures: u32 = 0;
    let mut cached_conn: Option<(String, hyper::client::conn::http1::SendRequest<Full<Bytes>>)> =
        None;

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

        let body = Bytes::from(encode_propertyset(&sent_props));
        let mut delivered = false;
        for url in &callbacks {
            if send_notify(url, &sid, seq, body.clone(), &mut cached_conn)
                .await
                .is_ok()
            {
                delivered = true;
                break;
            }
        }

        if delivered {
            consecutive_failures = 0;
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
            // unconfirmed delta is preserved for retry with capped exponential backoff.
            let backoff_ms = 250u64
                .saturating_mul(1u64 << consecutive_failures.min(4))
                .min(4_000);
            consecutive_failures = consecutive_failures.saturating_add(1);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                _ = wake.notified() => {}
                _ = cancel.changed() => break,
            }
        }
    }
}

async fn send_notify(
    url: &str,
    sid: &str,
    seq: u32,
    body_bytes: Bytes,
    cached_conn: &mut Option<(String, hyper::client::conn::http1::SendRequest<Full<Bytes>>)>,
) -> Result<(), ()> {
    let (host_port, path) = parse_http_url(url).ok_or(())?;

    // Try reusing an existing keep-alive connection for the same host:port first.
    if let Some((cached_host, mut sender)) = cached_conn.take()
        && cached_host == host_port
        && sender.is_ready()
        && let Ok(keep_alive) =
            send_notify_on(&mut sender, &host_port, &path, sid, seq, body_bytes.clone()).await
    {
        if keep_alive && sender.is_ready() {
            *cached_conn = Some((cached_host, sender));
        }
        return Ok(());
    }

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

    let keep_alive = send_notify_on(&mut sender, &host_port, &path, sid, seq, body_bytes).await?;
    if keep_alive && sender.is_ready() {
        *cached_conn = Some((host_port, sender));
    }
    Ok(())
}

async fn send_notify_on(
    sender: &mut hyper::client::conn::http1::SendRequest<Full<Bytes>>,
    host_port: &str,
    path: &str,
    sid: &str,
    seq: u32,
    body_bytes: Bytes,
) -> Result<bool, ()> {
    let req = Request::builder()
        .method("NOTIFY")
        .uri(path)
        .header(HOST, host_port)
        .header(CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
        .header(CONTENT_LENGTH, body_bytes.len().to_string())
        .header("NT", "upnp:event")
        .header("NTS", "upnp:propchange")
        .header("SID", sid)
        .header("SEQ", seq.to_string())
        .body(Full::new(body_bytes))
        .map_err(|_| ())?;

    let resp = tokio::time::timeout(NOTIFY_TIMEOUT, sender.send_request(req))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;

    let status = resp.status();
    let conn_close = resp
        .headers()
        .get(hyper::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("close"));
    let limited = Limited::new(resp.into_body(), 64 * 1024);
    let _ = tokio::time::timeout(NOTIFY_TIMEOUT, limited.collect()).await;

    if status.is_success() {
        Ok(!conn_close)
    } else {
        Err(())
    }
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

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Response, StatusCode};
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn callback_validation_and_limits() {
        let local4 = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let mapped_local4 = IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped());
        let local6 = IpAddr::V6(Ipv6Addr::LOCALHOST);

        assert!(is_allowed_callback(
            "http://127.0.0.1:8080/cb",
            Some(local4)
        ));
        assert!(is_allowed_callback(
            "http://127.0.0.1:8080/cb",
            Some(mapped_local4)
        ));
        assert!(is_allowed_callback("http://[::1]:8080/cb", Some(local6)));

        // Reject mismatches, hostnames, https, port 0, and unspecified IPs.
        assert!(!is_allowed_callback(
            "http://169.254.169.254:80/latest",
            Some(local4)
        ));
        assert!(!is_allowed_callback(
            "http://localhost:8080/cb",
            Some(local4)
        ));
        assert!(!is_allowed_callback(
            "https://127.0.0.1:8080/cb",
            Some(local4)
        ));
        assert!(!is_allowed_callback("http://127.0.0.1:0/cb", Some(local4)));
        assert!(!is_allowed_callback("http://0.0.0.0:8080/cb", None));
    }

    #[tokio::test]
    async fn event_hub_limits_service_scoping_and_retry_preserves_state() {
        let hub = EventHub::new();

        // Bind a mock callback server that fails the first NOTIFY with 500, then succeeds.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cb_addr = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(u32, String)>();

        let attempts_srv = attempts.clone();
        let srv_task = tokio::spawn(async move {
            for _ in 0..3 {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let attempts_c = attempts_srv.clone();
                let tx_c = tx.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let attempts_c = attempts_c.clone();
                        let tx_c = tx_c.clone();
                        async move {
                            let n = attempts_c.fetch_add(1, Ordering::SeqCst);
                            let seq = req
                                .headers()
                                .get("seq")
                                .and_then(|v| v.to_str().ok())
                                .and_then(|s| s.parse::<u32>().ok())
                                .unwrap_or(999);
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let body_str = String::from_utf8_lossy(&body).into_owned();
                            if n == 0 {
                                // Fail initial attempt transiently.
                                Ok::<_, Infallible>(
                                    Response::builder()
                                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                                        .body(Full::new(Bytes::new()))
                                        .unwrap(),
                                )
                            } else {
                                let _ = tx_c.send((seq, body_str));
                                Ok(Response::builder()
                                    .status(StatusCode::OK)
                                    .body(Full::new(Bytes::new()))
                                    .unwrap())
                            }
                        }
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });

        let mut initial = BTreeMap::new();
        initial.insert("Volume".into(), "40".into());

        let (sid, timeout) = hub
            .subscribe(
                "Volume",
                vec![format!("http://{cb_addr}/cb")],
                Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                Some(999_999), // clamped to MAX_TIMEOUT_SECS
                initial,
            )
            .unwrap();
        assert_eq!(timeout, MAX_TIMEOUT_SECS);

        // Wrong service slug cannot renew or unsubscribe.
        assert!(hub.renew("Playlist", &sid, Some(60)).is_none());
        assert!(!hub.unsubscribe("Playlist", &sid));
        assert_eq!(hub.renew("Volume", &sid, Some(60)), Some(60));

        // Await retry delivery of SEQ=0 via channel (no fixed sleep).
        let (seq0, body0) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("retry delivered SEQ=0 in time")
            .expect("channel open");
        assert!(attempts.load(Ordering::SeqCst) >= 2);
        assert_eq!(seq0, 0);
        assert!(body0.contains("<Volume>40</Volume>"));

        // Now send a state update and confirm SEQ=1 arrives promptly.
        let mut updated = BTreeMap::new();
        updated.insert("Volume".into(), "55".into());
        hub.notify_snapshot("Volume", &updated);
        let (seq1, body1) = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("delta delivered SEQ=1 in time")
            .expect("channel open");
        assert_eq!(seq1, 1);
        assert!(body1.contains("<Volume>55</Volume>"));

        assert!(hub.unsubscribe("Volume", &sid));
        assert_eq!(hub.active_subscription_count(), 0);
        srv_task.abort();
    }

    #[tokio::test]
    async fn event_hub_enforces_max_subscriptions() {
        let hub = EventHub::new();
        for _ in 0..MAX_SUBSCRIPTIONS {
            hub.subscribe(
                "Product",
                vec!["http://127.0.0.1:9/cb".into()],
                Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                Some(60),
                BTreeMap::new(),
            )
            .unwrap();
        }
        let err = hub
            .subscribe(
                "Product",
                vec!["http://127.0.0.1:9/cb".into()],
                Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                Some(60),
                BTreeMap::new(),
            )
            .unwrap_err();
        assert_eq!(err, SubscribeError::TooManySubscriptions);
        hub.clear();
    }
}
