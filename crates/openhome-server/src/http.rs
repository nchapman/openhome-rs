//! HTTP/1.1 server handling UPnP device description, SCPDs, SOAP control,
//! and GENA `SUBSCRIBE` / `UNSUBSCRIBE`.

use std::convert::Infallible;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::body::Incoming;
use hyper::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, SERVER};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use openhome_core::gena::{format_timeout_header, parse_callback_header, parse_timeout_header};
use openhome_core::soap::encode_fault;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, watch};

use crate::dispatch::ServiceRegistry;
use crate::event::{EventHub, SubscribeError};

const MAX_SOAP_BODY_BYTES: usize = 4 * 1024 * 1024;
/// Maximum time allowed to read HTTP request headers or a single SOAP request body.
const HTTP_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const SOAP_BODY_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum concurrent accepted TCP connections (guards against FD/task exhaustion).
const MAX_CONCURRENT_CONNS: usize = 128;
/// Maximum time to wait for in-flight connections to drain during server shutdown.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub(crate) struct HttpState {
    pub description_xml: Bytes,
    pub registry: ServiceRegistry,
    pub events: EventHub,
    pub server_header: Arc<String>,
}

pub(crate) async fn serve_http(
    listener: TcpListener,
    state: HttpState,
    mut shutdown: watch::Receiver<bool>,
) {
    let conn_limit = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNS));

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

        let Ok((stream, peer)) = accept else {
            drop(permit);
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        };

        let peer_ip = peer.ip();
        let state = state.clone();
        let mut conn_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(handle_request(req, peer_ip, state).await) }
            });

            let conn = http1::Builder::new()
                .keep_alive(true)
                .timer(TokioTimer::new())
                .header_read_timeout(HTTP_HEADER_READ_TIMEOUT)
                .serve_connection(io, svc);
            tokio::pin!(conn);
            tokio::select! {
                _ = conn.as_mut() => {}
                res = conn_shutdown.changed() => {
                    if res.is_err() || *conn_shutdown.borrow() {
                        conn.as_mut().graceful_shutdown();
                        let _ = tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, conn).await;
                    }
                }
            }
        });
    }
}

async fn handle_request(
    req: Request<Incoming>,
    peer_ip: IpAddr,
    state: HttpState,
) -> Response<Full<Bytes>> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    if path == "/description.xml" {
        if method == Method::GET || method == Method::HEAD {
            let body = if method == Method::HEAD {
                Bytes::new()
            } else {
                state.description_xml.clone()
            };
            return xml_response(
                StatusCode::OK,
                body,
                state.description_xml.len(),
                &state.server_header,
            );
        }
        return empty_response(StatusCode::METHOD_NOT_ALLOWED, &state.server_header);
    }

    if let Some(rest) = path.strip_prefix("/scpd/") {
        if method != Method::GET && method != Method::HEAD {
            return empty_response(StatusCode::METHOD_NOT_ALLOWED, &state.server_header);
        }
        let slug = rest.strip_suffix(".xml").unwrap_or(rest);
        if let Some(scpd) = state.registry.scpd_xml(slug) {
            let body = if method == Method::HEAD {
                Bytes::new()
            } else {
                Bytes::from_static(scpd.as_bytes())
            };
            return xml_response(StatusCode::OK, body, scpd.len(), &state.server_header);
        }
        return empty_response(StatusCode::NOT_FOUND, &state.server_header);
    }

    if let Some(slug) = path.strip_prefix("/ctl/") {
        if method != Method::POST {
            return empty_response(StatusCode::METHOD_NOT_ALLOWED, &state.server_header);
        }
        if !state.registry.has_service(slug) {
            return empty_response(StatusCode::NOT_FOUND, &state.server_header);
        }
        return handle_soap(req, slug, state).await;
    }

    if let Some(slug) = path.strip_prefix("/evt/") {
        if !state.registry.has_service(slug) {
            return empty_response(StatusCode::NOT_FOUND, &state.server_header);
        }
        return match method.as_str() {
            "SUBSCRIBE" => handle_subscribe(&req, peer_ip, slug, &state),
            "UNSUBSCRIBE" => handle_unsubscribe(&req, slug, &state),
            _ => empty_response(StatusCode::METHOD_NOT_ALLOWED, &state.server_header),
        };
    }

    empty_response(StatusCode::NOT_FOUND, &state.server_header)
}

async fn handle_soap(
    req: Request<Incoming>,
    slug: &str,
    state: HttpState,
) -> Response<Full<Bytes>> {
    // Reject declared oversized bodies before reading so hostile peers cannot stall a task.
    if let Some(cl) = req
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        && cl > MAX_SOAP_BODY_BYTES as u64
    {
        return empty_close_response(StatusCode::PAYLOAD_TOO_LARGE, &state.server_header);
    }

    let soap_action = req
        .headers()
        .get("soapaction")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let limited = Limited::new(req.into_body(), MAX_SOAP_BODY_BYTES);
    let body_bytes = match tokio::time::timeout(SOAP_BODY_READ_TIMEOUT, limited.collect()).await {
        Ok(Ok(collected)) => collected.to_bytes(),
        Ok(Err(err)) => {
            let status = if err.is::<LengthLimitError>() {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            };
            return empty_close_response(status, &state.server_header);
        }
        Err(_) => return empty_close_response(StatusCode::REQUEST_TIMEOUT, &state.server_header),
    };
    let Ok(body_str) = std::str::from_utf8(&body_bytes) else {
        return empty_response(StatusCode::BAD_REQUEST, &state.server_header);
    };

    match state
        .registry
        .dispatch_soap(slug, soap_action.as_deref(), body_str)
    {
        Ok(output) => {
            if !output.mutated_services.is_empty() {
                notify_services(&state.registry, &state.events, output.mutated_services);
            }
            let bytes = Bytes::from(output.response_xml);
            let len = bytes.len();
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
                .header(CONTENT_LENGTH, len.to_string())
                .header("EXT", "")
                .header(SERVER, state.server_header.as_str())
                .body(Full::new(bytes))
                .expect("valid SOAP response")
        }
        Err(fault) => {
            let xml = encode_fault(&fault);
            let bytes = Bytes::from(xml);
            let len = bytes.len();
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .header(CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
                .header(CONTENT_LENGTH, len.to_string())
                .header("EXT", "")
                .header(SERVER, state.server_header.as_str())
                .body(Full::new(bytes))
                .expect("valid SOAP fault response")
        }
    }
}

fn handle_subscribe(
    req: &Request<Incoming>,
    peer_ip: IpAddr,
    slug: &str,
    state: &HttpState,
) -> Response<Full<Bytes>> {
    let sid = header_str(req, "sid");
    let callback = header_str(req, "callback");
    let nt = header_str(req, "nt");
    let timeout = header_str(req, "timeout").and_then(parse_timeout_header);

    // UPnP DA 1.1: SID and CALLBACK/NT are mutually exclusive.
    if sid.is_some() && (callback.is_some() || nt.is_some()) {
        return empty_response(StatusCode::BAD_REQUEST, &state.server_header);
    }

    if let Some(sid) = sid {
        let Some(granted) = state.events.renew(slug, sid, timeout) else {
            return empty_response(StatusCode::PRECONDITION_FAILED, &state.server_header);
        };
        return subscribe_ok_response(sid, granted, &state.server_header);
    }

    // Initial subscription requires NT: upnp:event and at least one valid HTTP CALLBACK URL.
    if !nt.is_some_and(|v| v.eq_ignore_ascii_case("upnp:event")) {
        return empty_response(StatusCode::PRECONDITION_FAILED, &state.server_header);
    }
    let callbacks = callback.map(parse_callback_header).unwrap_or_default();
    if callbacks.is_empty() {
        return empty_response(StatusCode::PRECONDITION_FAILED, &state.server_header);
    }

    let Some(initial_state) = state.registry.state_snapshot(slug) else {
        return empty_response(StatusCode::NOT_FOUND, &state.server_header);
    };

    match state
        .events
        .subscribe(slug, callbacks, Some(peer_ip), timeout, initial_state)
    {
        Ok((new_sid, granted)) => subscribe_ok_response(&new_sid, granted, &state.server_header),
        Err(SubscribeError::InvalidCallbacks) => {
            empty_response(StatusCode::PRECONDITION_FAILED, &state.server_header)
        }
        Err(SubscribeError::TooManySubscriptions) => {
            empty_response(StatusCode::SERVICE_UNAVAILABLE, &state.server_header)
        }
    }
}

fn handle_unsubscribe(
    req: &Request<Incoming>,
    slug: &str,
    state: &HttpState,
) -> Response<Full<Bytes>> {
    let sid = header_str(req, "sid");
    let callback = header_str(req, "callback");
    let nt = header_str(req, "nt");

    if callback.is_some() || nt.is_some() {
        return empty_response(StatusCode::BAD_REQUEST, &state.server_header);
    }
    let Some(sid) = sid else {
        return empty_response(StatusCode::PRECONDITION_FAILED, &state.server_header);
    };
    if state.events.unsubscribe(slug, sid) {
        empty_response(StatusCode::OK, &state.server_header)
    } else {
        empty_response(StatusCode::PRECONDITION_FAILED, &state.server_header)
    }
}

pub(crate) fn notify_services(registry: &ServiceRegistry, events: &EventHub, slugs: &[&str]) {
    for &slug in slugs {
        if !events.has_subscribers_for(slug) {
            continue;
        }
        if let Some(snapshot) = registry.state_snapshot(slug) {
            events.notify_snapshot(slug, &snapshot);
        }
    }
}

pub(crate) fn notify_all_services(registry: &ServiceRegistry, events: &EventHub) {
    let slugs = registry.registered_slugs();
    notify_services(registry, events, &slugs);
}

fn header_str<'a>(req: &'a Request<Incoming>, name: &str) -> Option<&'a str> {
    req.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
}

fn subscribe_ok_response(
    sid: &str,
    timeout_secs: u32,
    server_header: &str,
) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("SID", sid)
        .header("TIMEOUT", format_timeout_header(timeout_secs))
        .header(SERVER, server_header)
        .header(CONTENT_LENGTH, "0")
        .body(Full::new(Bytes::new()))
        .expect("valid SUBSCRIBE response")
}

fn xml_response(
    status: StatusCode,
    body: Bytes,
    content_length: usize,
    server_header: &str,
) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
        .header(CONTENT_LENGTH, content_length.to_string())
        .header(SERVER, server_header)
        .body(Full::new(body))
        .expect("valid XML response")
}

fn empty_response(status: StatusCode, server_header: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(SERVER, server_header)
        .header(CONTENT_LENGTH, "0")
        .body(Full::new(Bytes::new()))
        .expect("valid empty response")
}

/// Empty error response that closes the connection when the request body was not drained
/// (RFC 9112 §9.6).
fn empty_close_response(status: StatusCode, server_header: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(SERVER, server_header)
        .header(CONTENT_LENGTH, "0")
        .header(CONNECTION, "close")
        .body(Full::new(Bytes::new()))
        .expect("valid empty close response")
}
