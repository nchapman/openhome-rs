//! HTTP/1.1 transport for UPnP descriptions, SOAP actions, and GENA control.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::client::conn::http1::SendRequest;
use hyper::header::{CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use openhome_core::Error as CoreError;
use openhome_core::gena::{format_timeout_header, parse_timeout_header};
use openhome_core::soap::{decode_soap_fault, decode_soap_response, encode_action_request};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::ClientError;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RESPONSE_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_IDLE_CONNS_PER_HOST: usize = 4;
const MAX_POOLED_HOSTS: usize = 32;

type ConnPool = Arc<Mutex<HashMap<String, Vec<SendRequest<Full<Bytes>>>>>>;

/// Lightweight HTTP/1.1 client with keep-alive connection pooling for OpenHome UPnP requests.
#[derive(Debug, Clone)]
pub struct HttpClient {
    timeout: Duration,
    pool: ConnPool,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new(DEFAULT_TIMEOUT)
    }
}

impl HttpClient {
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            pool: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Perform an HTTP `GET` and return the UTF-8 response body.
    pub async fn get_text(&self, url: &str) -> Result<String, ClientError> {
        let parsed = ParsedUrl::parse(url)?;
        let (status, _headers, body) = self
            .send_with_retry(&parsed.connect_addr, || {
                Request::builder()
                    .method(Method::GET)
                    .uri(&parsed.path_and_query)
                    .header(HOST, &parsed.host_header)
                    .body(Full::new(Bytes::new()))
                    .map_err(|e| ClientError::Http(e.to_string()))
            })
            .await?;
        if !status.is_success() {
            return Err(ClientError::HttpStatus(status.as_u16(), url.to_string()));
        }
        String::from_utf8(body.to_vec()).map_err(|e| ClientError::Http(e.to_string()))
    }

    /// Invoke a UPnP/OpenHome SOAP action and return the decoded output arguments.
    pub async fn soap_call(
        &self,
        control_url: &str,
        service_urn: &str,
        action: &str,
        args: &[(&str, &str)],
    ) -> Result<HashMap<String, String>, ClientError> {
        let parsed = ParsedUrl::parse(control_url)?;
        let envelope = encode_action_request(service_urn, action, args);
        let payload = Bytes::from(envelope.into_bytes());
        let soap_action = format!("\"{service_urn}#{action}\"");

        let (status, _headers, body) = self
            .send_with_retry(&parsed.connect_addr, || {
                Request::builder()
                    .method(Method::POST)
                    .uri(&parsed.path_and_query)
                    .header(HOST, &parsed.host_header)
                    .header(CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
                    .header(CONTENT_LENGTH, payload.len().to_string())
                    .header("SOAPACTION", &soap_action)
                    .body(Full::new(payload.clone()))
                    .map_err(|e| ClientError::Http(e.to_string()))
            })
            .await?;
        let body_str =
            String::from_utf8(body.to_vec()).map_err(|e| ClientError::Http(e.to_string()))?;

        if status == StatusCode::INTERNAL_SERVER_ERROR {
            if let Ok(Some(fault)) = decode_soap_fault(&body_str) {
                return Err(ClientError::Fault(fault));
            }
            return Err(ClientError::HttpStatus(status.as_u16(), body_str));
        }

        if !status.is_success() {
            return Err(ClientError::HttpStatus(status.as_u16(), body_str));
        }

        match decode_soap_response(&body_str) {
            Ok((_resp_action, out)) => Ok(out),
            Err(CoreError::Fault(fault)) => Err(ClientError::Fault(fault)),
            Err(err) => Err(ClientError::Core(err)),
        }
    }

    /// Send a GENA `SUBSCRIBE` request to establish a new subscription.
    /// Returns `(sid, granted_timeout_secs)`.
    pub async fn gena_subscribe(
        &self,
        event_sub_url: &str,
        callback_url: &str,
        timeout_secs: u32,
    ) -> Result<(String, u32), ClientError> {
        let parsed = ParsedUrl::parse(event_sub_url)?;
        let callback_header = format!("<{callback_url}>");
        let timeout_header = format_timeout_header(timeout_secs);
        let (status, headers, _body) = self
            .send_with_retry(&parsed.connect_addr, || {
                Request::builder()
                    .method("SUBSCRIBE")
                    .uri(&parsed.path_and_query)
                    .header(HOST, &parsed.host_header)
                    .header("CALLBACK", &callback_header)
                    .header("NT", "upnp:event")
                    .header("TIMEOUT", &timeout_header)
                    .body(Full::new(Bytes::new()))
                    .map_err(|e| ClientError::Http(e.to_string()))
            })
            .await?;
        if !status.is_success() {
            return Err(ClientError::SubscriptionFailed(format!(
                "HTTP {} from {event_sub_url}",
                status.as_u16()
            )));
        }

        let sid = headers
            .get("sid")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ClientError::SubscriptionFailed("missing SID header".into()))?
            .to_string();

        let granted = headers
            .get("timeout")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_timeout_header)
            .unwrap_or(timeout_secs);

        Ok((sid, granted))
    }

    /// Renew an existing GENA subscription by `sid`.
    pub async fn gena_renew(
        &self,
        event_sub_url: &str,
        sid: &str,
        timeout_secs: u32,
    ) -> Result<u32, ClientError> {
        let parsed = ParsedUrl::parse(event_sub_url)?;
        let timeout_header = format_timeout_header(timeout_secs);
        let (status, headers, _body) = self
            .send_with_retry(&parsed.connect_addr, || {
                Request::builder()
                    .method("SUBSCRIBE")
                    .uri(&parsed.path_and_query)
                    .header(HOST, &parsed.host_header)
                    .header("SID", sid)
                    .header("TIMEOUT", &timeout_header)
                    .body(Full::new(Bytes::new()))
                    .map_err(|e| ClientError::Http(e.to_string()))
            })
            .await?;
        if !status.is_success() {
            return Err(ClientError::SubscriptionFailed(format!(
                "renew failed with HTTP {}",
                status.as_u16()
            )));
        }

        Ok(headers
            .get("timeout")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_timeout_header)
            .unwrap_or(timeout_secs))
    }

    /// Cancel an existing GENA subscription by `sid`.
    pub async fn gena_unsubscribe(
        &self,
        event_sub_url: &str,
        sid: &str,
    ) -> Result<(), ClientError> {
        let parsed = ParsedUrl::parse(event_sub_url)?;
        let (status, _headers, _body) = self
            .send_with_retry(&parsed.connect_addr, || {
                Request::builder()
                    .method("UNSUBSCRIBE")
                    .uri(&parsed.path_and_query)
                    .header(HOST, &parsed.host_header)
                    .header("SID", sid)
                    .body(Full::new(Bytes::new()))
                    .map_err(|e| ClientError::Http(e.to_string()))
            })
            .await?;
        if !status.is_success() {
            return Err(ClientError::SubscriptionFailed(format!(
                "unsubscribe failed with HTTP {}",
                status.as_u16()
            )));
        }
        Ok(())
    }

    async fn take_pooled(&self, connect_addr: &str) -> Option<SendRequest<Full<Bytes>>> {
        let mut pool = self.pool.lock().await;
        let conns = pool.get_mut(connect_addr)?;
        while let Some(sender) = conns.pop() {
            if sender.is_ready() {
                return Some(sender);
            }
        }
        None
    }

    async fn return_pooled(&self, connect_addr: &str, sender: SendRequest<Full<Bytes>>) {
        if !sender.is_ready() {
            return;
        }
        let mut pool = self.pool.lock().await;
        if !pool.contains_key(connect_addr) && pool.len() >= MAX_POOLED_HOSTS {
            return;
        }
        let conns = pool.entry(connect_addr.to_string()).or_default();
        if conns.len() < MAX_IDLE_CONNS_PER_HOST {
            conns.push(sender);
        }
    }

    async fn connect(&self, connect_addr: &str) -> Result<SendRequest<Full<Bytes>>, ClientError> {
        let stream = tokio::time::timeout(self.timeout, TcpStream::connect(connect_addr))
            .await
            .map_err(|_| ClientError::Http(format!("connect timeout to {connect_addr}")))??;

        let io = TokioIo::new(stream);
        let (sender, conn) =
            tokio::time::timeout(self.timeout, hyper::client::conn::http1::handshake(io))
                .await
                .map_err(|_| ClientError::Http("HTTP handshake timeout".into()))?
                .map_err(|e| ClientError::Http(e.to_string()))?;

        tokio::spawn(async move {
            let _ = conn.await;
        });

        Ok(sender)
    }

    async fn send_on(
        &self,
        connect_addr: &str,
        mut sender: SendRequest<Full<Bytes>>,
        req: Request<Full<Bytes>>,
    ) -> Result<(StatusCode, hyper::HeaderMap, Bytes), SendOnError> {
        let resp = tokio::time::timeout(self.timeout, sender.send_request(req))
            .await
            .map_err(|_| SendOnError::Fatal(ClientError::Http("HTTP request timeout".into())))?
            .map_err(|e| {
                // Only treat pre-response closed/reset pooled sockets as safely retryable.
                if e.is_closed() || e.is_canceled() || e.is_incomplete_message() {
                    SendOnError::StalePool(ClientError::Http(e.to_string()))
                } else {
                    SendOnError::Fatal(ClientError::Http(e.to_string()))
                }
            })?;

        let status = resp.status();
        let headers = resp.headers().clone();
        let conn_close = headers
            .get(CONNECTION)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("close"));
        let limited = Limited::new(resp.into_body(), MAX_RESPONSE_BODY_BYTES);
        let collected = tokio::time::timeout(self.timeout, limited.collect())
            .await
            .map_err(|_| SendOnError::Fatal(ClientError::Http("HTTP body read timeout".into())))?
            .map_err(|e| SendOnError::Fatal(ClientError::Http(e.to_string())))?;

        if !conn_close {
            self.return_pooled(connect_addr, sender).await;
        }

        Ok((status, headers, collected.to_bytes()))
    }

    async fn send_with_retry<F>(
        &self,
        connect_addr: &str,
        build_req: F,
    ) -> Result<(StatusCode, hyper::HeaderMap, Bytes), ClientError>
    where
        F: Fn() -> Result<Request<Full<Bytes>>, ClientError>,
    {
        // Try a pooled keep-alive connection first; retry on a fresh socket ONLY if
        // the idle connection was closed/reset before receiving a response.
        if let Some(sender) = self.take_pooled(connect_addr).await {
            let req = build_req()?;
            match self.send_on(connect_addr, sender, req).await {
                Ok(res) => return Ok(res),
                Err(SendOnError::StalePool(_)) => {}
                Err(SendOnError::Fatal(err)) => return Err(err),
            }
        }
        let sender = self.connect(connect_addr).await?;
        let req = build_req()?;
        self.send_on(connect_addr, sender, req)
            .await
            .map_err(SendOnError::into_client_error)
    }
}

enum SendOnError {
    StalePool(ClientError),
    Fatal(ClientError),
}

impl SendOnError {
    fn into_client_error(self) -> ClientError {
        match self {
            Self::StalePool(e) | Self::Fatal(e) => e,
        }
    }
}

/// Resolve a UPnP service URL (`controlURL`, `eventSubURL`, `SCPDURL`) against
/// the device's `description.xml` location URL.
pub fn resolve_url(base_url: &str, rel_or_abs: &str) -> Result<String, ClientError> {
    if rel_or_abs.starts_with("http://") {
        return Ok(rel_or_abs.to_string());
    }
    if rel_or_abs.starts_with("https://") {
        return Err(ClientError::InvalidUrl(format!(
            "HTTPS is not supported for UPnP HTTP/1.1 transport: {rel_or_abs}"
        )));
    }
    let parsed = ParsedUrl::parse(base_url)?;
    if rel_or_abs.starts_with('/') {
        Ok(format!("http://{}{}", parsed.host_header, rel_or_abs))
    } else {
        let base_path = parsed
            .path_and_query
            .split(['?', '#'])
            .next()
            .unwrap_or("/");
        let base_dir = match base_path.rfind('/') {
            Some(idx) => &base_path[..=idx],
            None => "/",
        };
        Ok(format!(
            "http://{}{}{}",
            parsed.host_header, base_dir, rel_or_abs
        ))
    }
}

pub(crate) struct ParsedUrl {
    pub connect_addr: String,
    pub host_header: String,
    pub path_and_query: String,
}

impl ParsedUrl {
    pub fn parse(url: &str) -> Result<Self, ClientError> {
        if url.starts_with("https://") {
            return Err(ClientError::InvalidUrl(format!(
                "HTTPS is not supported for UPnP HTTP/1.1 transport: {url}"
            )));
        }
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| ClientError::InvalidUrl(url.to_string()))?;
        let (authority, path_and_query) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, "/"),
        };
        if authority.is_empty() {
            return Err(ClientError::InvalidUrl(url.to_string()));
        }

        let has_port = if let Some(after_bracket) = authority.strip_prefix('[') {
            after_bracket
                .split_once(']')
                .is_some_and(|(_, tail)| tail.starts_with(':'))
        } else {
            authority.contains(':')
        };

        let connect_addr = if has_port {
            authority.to_string()
        } else {
            format!("{authority}:80")
        };

        Ok(Self {
            connect_addr,
            host_header: authority.to_string(),
            path_and_query: path_and_query.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_relative_and_absolute_service_urls() {
        let base = "http://192.168.1.42:49152/upnp/description.xml";
        assert_eq!(
            resolve_url(base, "/ctl/Product").unwrap(),
            "http://192.168.1.42:49152/ctl/Product"
        );
        assert_eq!(
            resolve_url(base, "ctl/Product").unwrap(),
            "http://192.168.1.42:49152/upnp/ctl/Product"
        );
        assert_eq!(
            resolve_url(base, "http://10.0.0.1:8080/ctl").unwrap(),
            "http://10.0.0.1:8080/ctl"
        );
        assert!(matches!(
            resolve_url(base, "https://10.0.0.1/ctl"),
            Err(ClientError::InvalidUrl(_))
        ));

        let no_path = ParsedUrl::parse("http://192.168.1.5").unwrap();
        assert_eq!(no_path.connect_addr, "192.168.1.5:80");
        assert_eq!(no_path.path_and_query, "/");

        let ipv6 = ParsedUrl::parse("http://[::1]:8080/ctl?x=1").unwrap();
        assert_eq!(ipv6.connect_addr, "[::1]:8080");
        assert_eq!(ipv6.path_and_query, "/ctl?x=1");

        let ipv6_default = ParsedUrl::parse("http://[::1]/desc.xml").unwrap();
        assert_eq!(ipv6_default.connect_addr, "[::1]:80");

        assert!(matches!(
            ParsedUrl::parse("https://127.0.0.1:8080/x"),
            Err(ClientError::InvalidUrl(_))
        ));
        assert!(matches!(
            ParsedUrl::parse("http:///no-host"),
            Err(ClientError::InvalidUrl(_))
        ));
        assert_eq!(
            resolve_url("http://192.168.1.50:49152/upnp/desc.xml?v=2", "ctl/Product").unwrap(),
            "http://192.168.1.50:49152/upnp/ctl/Product"
        );
        assert!(matches!(
            ParsedUrl::parse("ftp://127.0.0.1/x"),
            Err(ClientError::InvalidUrl(_))
        ));
    }

    #[tokio::test]
    async fn http_client_handles_non_fault_500_missing_sid_infinite_timeout_and_renew_errors() {
        use hyper::Response;
        use hyper::server::conn::http1;
        use hyper::service::service_fn;
        use std::convert::Infallible;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(|req: Request<hyper::body::Incoming>| async move {
                        let resp = match req.uri().path() {
                            "/soap500" => Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Full::new(Bytes::from_static(b"plain text error")))
                                .unwrap(),
                            "/soap404" => Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Full::new(Bytes::from_static(b"not found")))
                                .unwrap(),
                            "/nosid" => Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::new()))
                                .unwrap(),
                            "/infinite" => Response::builder()
                                .status(StatusCode::OK)
                                .header("SID", "uuid:inf-1")
                                .header("TIMEOUT", "Second-infinite")
                                .body(Full::new(Bytes::new()))
                                .unwrap(),
                            _ => Response::builder()
                                .status(StatusCode::PRECONDITION_FAILED)
                                .body(Full::new(Bytes::new()))
                                .unwrap(),
                        };
                        Ok::<_, Infallible>(resp)
                    });
                    let _ = http1::Builder::new().serve_connection(io, svc).await;
                });
            }
        });

        let client = HttpClient::new(Duration::from_secs(2));
        let base = format!("http://{addr}");

        assert!(matches!(
            client
                .soap_call(&format!("{base}/soap500"), "urn:s", "Act", &[])
                .await,
            Err(ClientError::HttpStatus(500, _))
        ));
        assert!(matches!(
            client
                .soap_call(&format!("{base}/soap404"), "urn:s", "Act", &[])
                .await,
            Err(ClientError::HttpStatus(404, _))
        ));
        assert!(matches!(
            client
                .gena_subscribe(&format!("{base}/nosid"), "http://127.0.0.1:9/cb", 60)
                .await,
            Err(ClientError::SubscriptionFailed(_))
        ));
        let (sid, granted) = client
            .gena_subscribe(&format!("{base}/infinite"), "http://127.0.0.1:9/cb", 120)
            .await
            .unwrap();
        assert_eq!(sid, "uuid:inf-1");
        assert_eq!(granted, 120);
        assert!(matches!(
            client
                .gena_renew(&format!("{base}/fail"), "uuid:inf-1", 60)
                .await,
            Err(ClientError::SubscriptionFailed(_))
        ));
        assert!(matches!(
            client
                .gena_unsubscribe(&format!("{base}/fail"), "uuid:inf-1")
                .await,
            Err(ClientError::SubscriptionFailed(_))
        ));

        srv.abort();
    }
}
