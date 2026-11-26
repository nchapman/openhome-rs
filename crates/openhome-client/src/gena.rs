//! Async GENA event callback listener and subscription manager.

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::header::{CONNECTION, CONTENT_LENGTH};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use openhome_core::gena::decode_propertyset;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::device::OpenHomeDevice;
use crate::error::ClientError;
use crate::http::HttpClient;

const MAX_NOTIFY_BODY_BYTES: usize = 4 * 1024 * 1024;
const EVENT_CHANNEL_CAPACITY: usize = 256;
const CALLBACK_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const CALLBACK_BODY_READ_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CONCURRENT_CALLBACK_CONNS: usize = 128;
const CALLBACK_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// A decoded GENA `NOTIFY` event delivered by an OpenHome service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenaEvent {
    pub sid: String,
    pub seq: u32,
    pub properties: BTreeMap<String, String>,
}

#[derive(Clone)]
struct RouteEntry {
    tx: mpsc::Sender<GenaEvent>,
    expected_sid: Arc<RwLock<Option<String>>>,
}

type RouteMap = Arc<RwLock<HashMap<String, RouteEntry>>>;

/// Local HTTP/1.1 callback server that receives GENA `NOTIFY` events from
/// subscribed OpenHome services.
pub struct EventListener {
    local_addr: SocketAddr,
    advertise_host: String,
    http: HttpClient,
    routes: RouteMap,
    shutdown_tx: watch::Sender<bool>,
    server_task: Option<JoinHandle<()>>,
}

impl EventListener {
    /// Bind an event listener on `bind_addr` (e.g. `127.0.0.1:0` or `0.0.0.0:0`).
    pub async fn bind(bind_addr: SocketAddr) -> Result<Self, ClientError> {
        Self::bind_with_options(bind_addr, None, HttpClient::default()).await
    }

    /// Bind an event listener with an optional explicit `advertise_host` for
    /// GENA `CALLBACK` URLs and a custom [`HttpClient`].
    pub async fn bind_with_options(
        bind_addr: SocketAddr,
        advertise_host: Option<String>,
        http: HttpClient,
    ) -> Result<Self, ClientError> {
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let advertise_host = advertise_host.unwrap_or_else(|| resolve_local_host(local_addr.ip()));

        let routes: RouteMap = Arc::new(RwLock::new(HashMap::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_task = tokio::spawn(serve_callbacks(listener, routes.clone(), shutdown_rx));

        Ok(Self {
            local_addr,
            advertise_host,
            http,
            routes,
            shutdown_tx,
            server_task: Some(server_task),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Subscribe to a service's `eventSubURL` with a requested timeout in seconds.
    ///
    /// Establishes automatic background renewal at half the granted timeout.
    pub async fn subscribe(
        &self,
        event_sub_url: impl Into<String>,
        timeout_secs: u32,
    ) -> Result<EventSubscription, ClientError> {
        let event_sub_url = event_sub_url.into();
        // Unpredictable 128-bit token prevents LAN peers from guessing callback paths.
        let token = Uuid::new_v4().simple().to_string();
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let expected_sid = Arc::new(RwLock::new(None));

        // Register route BEFORE sending SUBSCRIBE so an immediate SEQ=0 NOTIFY
        // can never race ahead of route registration.
        {
            let mut routes = self.routes.write().expect("EventListener lock poisoned");
            routes.insert(
                token.clone(),
                RouteEntry {
                    tx,
                    expected_sid: expected_sid.clone(),
                },
            );
        }

        let callback_url = format!(
            "http://{}:{}/cb/{token}",
            format_host(&self.advertise_host),
            self.local_addr.port()
        );

        let (sid, granted_timeout) = match self
            .http
            .gena_subscribe(&event_sub_url, &callback_url, timeout_secs)
            .await
        {
            Ok(res) => res,
            Err(err) => {
                self.routes
                    .write()
                    .expect("EventListener lock poisoned")
                    .remove(&token);
                return Err(err);
            }
        };

        *expected_sid.write().expect("expected_sid lock poisoned") = Some(sid.clone());

        let (renew_stop_tx, renew_stop_rx) = watch::channel(false);
        let renew_task = tokio::spawn(auto_renew_loop(
            self.http.clone(),
            event_sub_url.clone(),
            sid.clone(),
            granted_timeout,
            self.routes.clone(),
            token.clone(),
            renew_stop_rx,
        ));

        Ok(EventSubscription {
            sid,
            event_sub_url,
            timeout_secs: granted_timeout,
            token,
            http: self.http.clone(),
            routes: self.routes.clone(),
            rx,
            renew_stop_tx,
            renew_task: Some(renew_task),
            unsubscribed: false,
        })
    }

    /// Convenience helper to subscribe to a service on an [`OpenHomeDevice`] by
    /// `serviceType` or `serviceId` URN.
    pub async fn subscribe_service(
        &self,
        device: &OpenHomeDevice,
        service_type_or_id: &'static str,
        timeout_secs: u32,
    ) -> Result<EventSubscription, ClientError> {
        let url = device.event_sub_url(service_type_or_id)?;
        self.subscribe(url, timeout_secs).await
    }

    /// Gracefully shut down the callback HTTP server.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        self.routes
            .write()
            .expect("EventListener lock poisoned")
            .clear();
        if let Some(task) = self.server_task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for EventListener {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        self.routes
            .write()
            .expect("EventListener lock poisoned")
            .clear();
    }
}

impl std::fmt::Debug for EventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventListener")
            .field("local_addr", &self.local_addr)
            .field("advertise_host", &self.advertise_host)
            .finish()
    }
}

/// Active GENA event subscription stream.
pub struct EventSubscription {
    sid: String,
    event_sub_url: String,
    timeout_secs: u32,
    token: String,
    http: HttpClient,
    routes: RouteMap,
    rx: mpsc::Receiver<GenaEvent>,
    renew_stop_tx: watch::Sender<bool>,
    renew_task: Option<JoinHandle<()>>,
    unsubscribed: bool,
}

impl std::fmt::Debug for EventSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventSubscription")
            .field("sid", &self.sid)
            .field("event_sub_url", &self.event_sub_url)
            .field("timeout_secs", &self.timeout_secs)
            .finish()
    }
}

impl EventSubscription {
    pub fn sid(&self) -> &str {
        &self.sid
    }

    pub fn timeout_secs(&self) -> u32 {
        self.timeout_secs
    }

    /// Receive the next GENA event (`SEQ=0` initial full state, then deltas).
    pub async fn recv(&mut self) -> Option<GenaEvent> {
        self.rx.recv().await
    }

    /// Manually renew the subscription for `timeout_secs`.
    pub async fn renew(&mut self, timeout_secs: u32) -> Result<u32, ClientError> {
        let granted = self
            .http
            .gena_renew(&self.event_sub_url, &self.sid, timeout_secs)
            .await?;
        self.timeout_secs = granted;
        Ok(granted)
    }

    /// Send GENA `UNSUBSCRIBE`, stop background renewal, and deregister callback route.
    pub async fn unsubscribe(mut self) -> Result<(), ClientError> {
        self.unsubscribed = true;
        let _ = self.renew_stop_tx.send(true);
        if let Some(task) = self.renew_task.take() {
            let _ = task.await;
        }
        // Send UNSUBSCRIBE before removing the local callback route so an in-flight
        // NOTIFY does not receive 412 and race-cancel the subscription before UNSUBSCRIBE arrives.
        let res = self
            .http
            .gena_unsubscribe(&self.event_sub_url, &self.sid)
            .await;
        self.routes
            .write()
            .expect("EventListener lock poisoned")
            .remove(&self.token);
        res
    }
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        if !self.unsubscribed {
            let _ = self.renew_stop_tx.send(true);
            self.routes
                .write()
                .expect("EventListener lock poisoned")
                .remove(&self.token);
        }
    }
}

async fn auto_renew_loop(
    http: HttpClient,
    event_sub_url: String,
    sid: String,
    mut timeout_secs: u32,
    routes: RouteMap,
    token: String,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut consecutive_failures = 0u8;
    loop {
        let sleep_secs = if consecutive_failures > 0 {
            1
        } else {
            u64::from(timeout_secs / 2).max(1)
        };
        tokio::select! {
            res = stop_rx.changed() => {
                if res.is_err() || *stop_rx.borrow() {
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(sleep_secs)) => {
                match http.gena_renew(&event_sub_url, &sid, timeout_secs).await {
                    Ok(granted) => {
                        timeout_secs = granted;
                        consecutive_failures = 0;
                    }
                    Err(_) => {
                        consecutive_failures += 1;
                        if consecutive_failures >= 3 {
                            // Remove the callback route so `EventSubscription::recv()` returns
                            // `None` instead of hanging forever after fatal renewal failure.
                            if let Ok(mut map) = routes.write() {
                                map.remove(&token);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
}

async fn serve_callbacks(
    listener: TcpListener,
    routes: RouteMap,
    mut shutdown: watch::Receiver<bool>,
) {
    let conn_limit = Arc::new(Semaphore::new(MAX_CONCURRENT_CALLBACK_CONNS));

    loop {
        let permit = tokio::select! {
            res = conn_limit.clone().acquire_owned() => match res {
                Ok(p) => p,
                Err(_) => break,
            },
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
        };

        let accept = tokio::select! {
            res = listener.accept() => res,
            res = shutdown.changed() => {
                if res.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
        };

        let Ok((stream, _peer)) = accept else {
            drop(permit);
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        };
        let routes = routes.clone();
        let mut conn_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let routes = routes.clone();
                async move { Ok::<_, Infallible>(handle_notify(req, routes).await) }
            });
            let conn = http1::Builder::new()
                .keep_alive(true)
                .timer(TokioTimer::new())
                .header_read_timeout(CALLBACK_HEADER_READ_TIMEOUT)
                .serve_connection(io, svc);
            tokio::pin!(conn);
            tokio::select! {
                _ = conn.as_mut() => {}
                res = conn_shutdown.changed() => {
                    if res.is_err() || *conn_shutdown.borrow() {
                        conn.as_mut().graceful_shutdown();
                        let _ = tokio::time::timeout(CALLBACK_SHUTDOWN_DRAIN_TIMEOUT, conn).await;
                    }
                }
            }
        });
    }
}

async fn handle_notify(req: Request<Incoming>, routes: RouteMap) -> Response<Full<Bytes>> {
    if req.method().as_str() != "NOTIFY" {
        return empty_status(StatusCode::METHOD_NOT_ALLOWED);
    }

    let Some(token) = req.uri().path().strip_prefix("/cb/").map(str::to_string) else {
        return empty_status(StatusCode::NOT_FOUND);
    };

    let nt = header_str(&req, "nt");
    let nts = header_str(&req, "nts");
    if !nt.is_some_and(|v| v.eq_ignore_ascii_case("upnp:event"))
        || !nts.is_some_and(|v| v.eq_ignore_ascii_case("upnp:propchange"))
    {
        return empty_status(StatusCode::PRECONDITION_FAILED);
    }

    let Some(sid) = header_str(&req, "sid").map(str::to_string) else {
        return empty_status(StatusCode::PRECONDITION_FAILED);
    };
    let Some(seq) = header_str(&req, "seq").and_then(|s| s.parse::<u32>().ok()) else {
        return empty_status(StatusCode::BAD_REQUEST);
    };

    if let Some(cl) = req
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        && cl > MAX_NOTIFY_BODY_BYTES as u64
    {
        return empty_close_status(StatusCode::PAYLOAD_TOO_LARGE);
    }

    let limited = Limited::new(req.into_body(), MAX_NOTIFY_BODY_BYTES);
    let body_bytes = match tokio::time::timeout(CALLBACK_BODY_READ_TIMEOUT, limited.collect()).await
    {
        Ok(Ok(collected)) => collected.to_bytes(),
        Ok(Err(err)) => {
            let status = if err.is::<LengthLimitError>() {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            };
            return empty_close_status(status);
        }
        Err(_) => return empty_close_status(StatusCode::REQUEST_TIMEOUT),
    };
    let Ok(body_str) = std::str::from_utf8(&body_bytes) else {
        return empty_status(StatusCode::BAD_REQUEST);
    };
    let Ok(properties) = decode_propertyset(body_str) else {
        return empty_status(StatusCode::BAD_REQUEST);
    };

    let entry = routes
        .read()
        .expect("EventListener lock poisoned")
        .get(&token)
        .cloned();

    let Some(entry) = entry else {
        return empty_status(StatusCode::PRECONDITION_FAILED);
    };

    if let Some(expected) = entry
        .expected_sid
        .read()
        .expect("expected_sid lock poisoned")
        .as_deref()
        && expected != sid
    {
        return empty_status(StatusCode::PRECONDITION_FAILED);
    }

    match entry.tx.try_send(GenaEvent {
        sid,
        seq,
        properties,
    }) {
        Ok(()) => empty_status(StatusCode::OK),
        Err(mpsc::error::TrySendError::Full(_)) => empty_status(StatusCode::SERVICE_UNAVAILABLE),
        Err(mpsc::error::TrySendError::Closed(_)) => empty_status(StatusCode::PRECONDITION_FAILED),
    }
}

fn header_str<'a>(req: &'a Request<Incoming>, name: &str) -> Option<&'a str> {
    req.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
}

fn empty_status(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, "0")
        .body(Full::new(Bytes::new()))
        .expect("valid GENA response")
}

fn empty_close_status(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, "0")
        .header(CONNECTION, "close")
        .body(Full::new(Bytes::new()))
        .expect("valid GENA close response")
}

fn resolve_local_host(ip: IpAddr) -> String {
    if !ip.is_unspecified() {
        return ip.to_string();
    }
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
    use tokio::net::TcpStream;

    use super::*;

    async fn send_raw_http(addr: SocketAddr, req: Request<Full<Bytes>>) -> StatusCode {
        let stream = TcpStream::connect(addr).await.unwrap();
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let resp = sender.send_request(req).await.unwrap();
        resp.status()
    }

    #[tokio::test]
    async fn callback_server_rejects_wrong_method_unknown_token_bad_headers_and_mismatched_sid() {
        let listener = EventListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .unwrap();
        let addr = listener.local_addr();

        // Register a known route with an expected SID.
        let (tx, mut rx) = mpsc::channel(16);
        let token = "testtoken123".to_string();
        listener.routes.write().unwrap().insert(
            token.clone(),
            RouteEntry {
                tx,
                expected_sid: Arc::new(RwLock::new(Some("uuid:expected-sid".into()))),
            },
        );

        let body = r#"<e:propertyset xmlns:e="urn:schemas-upnp-org:event-1-0"><e:property><Volume>99</Volume></e:property></e:propertyset>"#;

        // 1. Wrong HTTP method -> 405.
        let req = Request::builder()
            .method("GET")
            .uri(format!("/cb/{token}"))
            .header("HOST", addr.to_string())
            .body(Full::new(Bytes::new()))
            .unwrap();
        assert_eq!(
            send_raw_http(addr, req).await,
            StatusCode::METHOD_NOT_ALLOWED
        );

        // 2. Unknown token -> 412.
        let req = Request::builder()
            .method("NOTIFY")
            .uri("/cb/unknown-token")
            .header("HOST", addr.to_string())
            .header("NT", "upnp:event")
            .header("NTS", "upnp:propchange")
            .header("SID", "uuid:expected-sid")
            .header("SEQ", "0")
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        assert_eq!(
            send_raw_http(addr, req).await,
            StatusCode::PRECONDITION_FAILED
        );

        // 3. Forged / mismatched SID on valid token -> 412 and nothing delivered.
        let req = Request::builder()
            .method("NOTIFY")
            .uri(format!("/cb/{token}"))
            .header("HOST", addr.to_string())
            .header("NT", "upnp:event")
            .header("NTS", "upnp:propchange")
            .header("SID", "uuid:attacker-sid")
            .header("SEQ", "0")
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        assert_eq!(
            send_raw_http(addr, req).await,
            StatusCode::PRECONDITION_FAILED
        );

        // 4. Bad SEQ -> 400.
        let req = Request::builder()
            .method("NOTIFY")
            .uri(format!("/cb/{token}"))
            .header("HOST", addr.to_string())
            .header("NT", "upnp:event")
            .header("NTS", "upnp:propchange")
            .header("SID", "uuid:expected-sid")
            .header("SEQ", "not-a-num")
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        assert_eq!(send_raw_http(addr, req).await, StatusCode::BAD_REQUEST);

        // 5. Case-insensitive NT/NTS with valid SID -> 200 OK and event delivered.
        let req = Request::builder()
            .method("NOTIFY")
            .uri(format!("/cb/{token}"))
            .header("HOST", addr.to_string())
            .header("NT", "UPNP:EVENT")
            .header("NTS", "UPNP:PROPCHANGE")
            .header("SID", "uuid:expected-sid")
            .header("SEQ", "0")
            .body(Full::new(Bytes::from(body)))
            .unwrap();
        assert_eq!(send_raw_http(addr, req).await, StatusCode::OK);

        let ev = rx.try_recv().expect("valid NOTIFY delivered");
        assert_eq!(ev.sid, "uuid:expected-sid");
        assert_eq!(ev.properties.get("Volume").map(String::as_str), Some("99"));

        // Bounded channel backpressure: full -> 503, closed -> 412.
        let (full_tx, full_rx) = mpsc::channel(1);
        full_tx
            .try_send(GenaEvent {
                sid: "uuid:full".into(),
                seq: 0,
                properties: BTreeMap::new(),
            })
            .unwrap();
        listener.routes.write().unwrap().insert(
            "full".into(),
            RouteEntry {
                tx: full_tx,
                expected_sid: Arc::new(RwLock::new(Some("uuid:full".into()))),
            },
        );
        let mk_full_req = || {
            Request::builder()
                .method("NOTIFY")
                .uri("/cb/full")
                .header("HOST", addr.to_string())
                .header("NT", "upnp:event")
                .header("NTS", "upnp:propchange")
                .header("SID", "uuid:full")
                .header("SEQ", "1")
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        };
        assert_eq!(
            send_raw_http(addr, mk_full_req()).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        drop(full_rx);
        assert_eq!(
            send_raw_http(addr, mk_full_req()).await,
            StatusCode::PRECONDITION_FAILED
        );

        // Fatal auto_renew_loop exit removes the route so recv() returns None (no hang).
        let (dead_tx, mut dead_rx) = mpsc::channel(4);
        listener.routes.write().unwrap().insert(
            "dead".into(),
            RouteEntry {
                tx: dead_tx,
                expected_sid: Arc::new(RwLock::new(Some("uuid:dead".into()))),
            },
        );
        let (_stop_tx, stop_rx) = watch::channel(false);
        let renew_handle = tokio::spawn(auto_renew_loop(
            HttpClient::new(Duration::from_millis(100)),
            "http://127.0.0.1:1/evt/Product".into(),
            "uuid:dead".into(),
            1,
            listener.routes.clone(),
            "dead".into(),
            stop_rx,
        ));
        let closed = tokio::time::timeout(Duration::from_secs(6), dead_rx.recv())
            .await
            .expect("dead subscription closes instead of hanging");
        assert!(closed.is_none());
        let _ = renew_handle.await;

        listener.shutdown().await;
    }
}
