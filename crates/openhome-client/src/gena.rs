//! Async GENA event callback listener and subscription manager.

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::CONTENT_LENGTH;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use openhome_core::gena::decode_propertyset;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::device::OpenHomeDevice;
use crate::error::ClientError;
use crate::http::HttpClient;

const MAX_NOTIFY_BODY_BYTES: usize = 4 * 1024 * 1024;

/// A decoded GENA `NOTIFY` event delivered by an OpenHome service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenaEvent {
    pub sid: String,
    pub seq: u32,
    pub properties: BTreeMap<String, String>,
}

#[derive(Clone)]
struct RouteEntry {
    tx: mpsc::UnboundedSender<GenaEvent>,
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
        let (tx, rx) = mpsc::unbounded_channel();
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

/// Active GENA event subscription stream.
pub struct EventSubscription {
    sid: String,
    event_sub_url: String,
    timeout_secs: u32,
    token: String,
    http: HttpClient,
    routes: RouteMap,
    rx: mpsc::UnboundedReceiver<GenaEvent>,
    renew_stop_tx: watch::Sender<bool>,
    renew_task: Option<JoinHandle<()>>,
    unsubscribed: bool,
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
        self.routes
            .write()
            .expect("EventListener lock poisoned")
            .remove(&self.token);
        self.http
            .gena_unsubscribe(&self.event_sub_url, &self.sid)
            .await
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
    mut stop_rx: watch::Receiver<bool>,
) {
    loop {
        let sleep_secs = u64::from(timeout_secs / 2).max(1);
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(sleep_secs)) => {
                match http.gena_renew(&event_sub_url, &sid, timeout_secs).await {
                    Ok(granted) => {
                        timeout_secs = granted;
                    }
                    Err(_) => break,
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
    loop {
        let accept = tokio::select! {
            res = listener.accept() => res,
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
                continue;
            }
        };

        let Ok((stream, _peer)) = accept else {
            continue;
        };
        let routes = routes.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let routes = routes.clone();
                async move { Ok::<_, Infallible>(handle_notify(req, routes).await) }
            });
            let _ = http1::Builder::new().serve_connection(io, svc).await;
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
    if nt != Some("upnp:event") || nts != Some("upnp:propchange") {
        return empty_status(StatusCode::PRECONDITION_FAILED);
    }

    let Some(sid) = header_str(&req, "sid").map(str::to_string) else {
        return empty_status(StatusCode::PRECONDITION_FAILED);
    };
    let Some(seq) = header_str(&req, "seq").and_then(|s| s.parse::<u32>().ok()) else {
        return empty_status(StatusCode::BAD_REQUEST);
    };

    let limited = Limited::new(req.into_body(), MAX_NOTIFY_BODY_BYTES);
    let body_bytes = match limited.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return empty_status(StatusCode::BAD_REQUEST),
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

    if entry
        .tx
        .send(GenaEvent {
            sid,
            seq,
            properties,
        })
        .is_err()
    {
        return empty_status(StatusCode::PRECONDITION_FAILED);
    }

    empty_status(StatusCode::OK)
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
