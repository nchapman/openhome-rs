//! HTTP/1.1 transport for UPnP descriptions, SOAP actions, and GENA control.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE, HOST};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use openhome_core::Error as CoreError;
use openhome_core::gena::{format_timeout_header, parse_timeout_header};
use openhome_core::soap::{decode_soap_fault, decode_soap_response, encode_action_request};
use tokio::net::TcpStream;

use crate::error::ClientError;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RESPONSE_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Lightweight HTTP/1.1 client for OpenHome UPnP requests.
#[derive(Debug, Clone)]
pub struct HttpClient {
    timeout: Duration,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

impl HttpClient {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Perform an HTTP `GET` and return the UTF-8 response body.
    pub async fn get_text(&self, url: &str) -> Result<String, ClientError> {
        let parsed = ParsedUrl::parse(url)?;
        let req = Request::builder()
            .method(Method::GET)
            .uri(&parsed.path_and_query)
            .header(HOST, &parsed.host_header)
            .header("CONNECTION", "close")
            .body(Full::new(Bytes::new()))
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let (status, _headers, body) = self.send(&parsed.connect_addr, req).await?;
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

        let req = Request::builder()
            .method(Method::POST)
            .uri(&parsed.path_and_query)
            .header(HOST, &parsed.host_header)
            .header(CONTENT_TYPE, "text/xml; charset=\"utf-8\"")
            .header(CONTENT_LENGTH, payload.len().to_string())
            .header("SOAPACTION", soap_action)
            .header("CONNECTION", "close")
            .body(Full::new(payload))
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let (status, _headers, body) = self.send(&parsed.connect_addr, req).await?;
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
        let req = Request::builder()
            .method("SUBSCRIBE")
            .uri(&parsed.path_and_query)
            .header(HOST, &parsed.host_header)
            .header("CALLBACK", format!("<{callback_url}>"))
            .header("NT", "upnp:event")
            .header("TIMEOUT", format_timeout_header(timeout_secs))
            .header("CONNECTION", "close")
            .body(Full::new(Bytes::new()))
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let (status, headers, _body) = self.send(&parsed.connect_addr, req).await?;
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
        let req = Request::builder()
            .method("SUBSCRIBE")
            .uri(&parsed.path_and_query)
            .header(HOST, &parsed.host_header)
            .header("SID", sid)
            .header("TIMEOUT", format_timeout_header(timeout_secs))
            .header("CONNECTION", "close")
            .body(Full::new(Bytes::new()))
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let (status, headers, _body) = self.send(&parsed.connect_addr, req).await?;
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
        let req = Request::builder()
            .method("UNSUBSCRIBE")
            .uri(&parsed.path_and_query)
            .header(HOST, &parsed.host_header)
            .header("SID", sid)
            .header("CONNECTION", "close")
            .body(Full::new(Bytes::new()))
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let (status, _headers, _body) = self.send(&parsed.connect_addr, req).await?;
        if !status.is_success() {
            return Err(ClientError::SubscriptionFailed(format!(
                "unsubscribe failed with HTTP {}",
                status.as_u16()
            )));
        }
        Ok(())
    }

    async fn send(
        &self,
        connect_addr: &str,
        req: Request<Full<Bytes>>,
    ) -> Result<(StatusCode, hyper::HeaderMap, Bytes), ClientError> {
        let stream = tokio::time::timeout(self.timeout, TcpStream::connect(connect_addr))
            .await
            .map_err(|_| ClientError::Http(format!("connect timeout to {connect_addr}")))??;

        let io = TokioIo::new(stream);
        let (mut sender, conn) =
            tokio::time::timeout(self.timeout, hyper::client::conn::http1::handshake(io))
                .await
                .map_err(|_| ClientError::Http("HTTP handshake timeout".into()))?
                .map_err(|e| ClientError::Http(e.to_string()))?;

        tokio::spawn(async move {
            let _ = conn.await;
        });

        let resp = tokio::time::timeout(self.timeout, sender.send_request(req))
            .await
            .map_err(|_| ClientError::Http("HTTP request timeout".into()))?
            .map_err(|e| ClientError::Http(e.to_string()))?;

        let status = resp.status();
        let headers = resp.headers().clone();
        let limited = Limited::new(resp.into_body(), MAX_RESPONSE_BODY_BYTES);
        let collected = tokio::time::timeout(self.timeout, limited.collect())
            .await
            .map_err(|_| ClientError::Http("HTTP body read timeout".into()))?
            .map_err(|e| ClientError::Http(e.to_string()))?;

        Ok((status, headers, collected.to_bytes()))
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
        let base_dir = match parsed.path_and_query.rfind('/') {
            Some(idx) => &parsed.path_and_query[..=idx],
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
    }
}
