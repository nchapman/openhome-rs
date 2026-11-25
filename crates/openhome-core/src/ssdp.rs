//! Pure SSDP (Simple Service Discovery Protocol) wire formatting and parsing.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};

use crate::error::Error;
use crate::urn::SSDP_ALL;

/// Standard SSDP IPv4 multicast address (`239.255.255.250`).
pub const SSDP_MULTICAST_V4: Ipv4Addr = Ipv4Addr::new(239, 255, 255, 250);
/// Standard SSDP UDP port (`1900`).
pub const SSDP_PORT: u16 = 1900;
/// Standard SSDP multicast socket address (`239.255.255.250:1900`).
pub const SSDP_ADDR_V4: SocketAddrV4 = SocketAddrV4::new(SSDP_MULTICAST_V4, SSDP_PORT);

/// Parsed SSDP `M-SEARCH` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsdpSearch {
    pub st: String,
    pub mx: Option<u32>,
    pub man: Option<String>,
}

/// Parsed SSDP `NOTIFY` advertisement (`ssdp:alive` or `ssdp:byebye`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsdpNotify {
    pub nt: String,
    pub nts: String,
    pub usn: String,
    pub location: Option<String>,
    pub server: Option<String>,
    pub max_age: Option<u32>,
}

/// Parsed SSDP `HTTP/1.1 200 OK` response to `M-SEARCH`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsdpResponse {
    pub st: String,
    pub usn: String,
    pub location: String,
    pub server: Option<String>,
    pub max_age: Option<u32>,
}

/// Decoded SSDP UDP message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SsdpMessage {
    MSearch(SsdpSearch),
    NotifyAlive(SsdpNotify),
    NotifyByeBye(SsdpNotify),
    Response(SsdpResponse),
}

/// Encode an SSDP `M-SEARCH` request datagram.
pub fn encode_msearch(host: &str, st: &str, mx_secs: u32) -> String {
    format!(
        "M-SEARCH * HTTP/1.1\r\n\
         HOST: {host}\r\n\
         MAN: \"ssdp:discover\"\r\n\
         MX: {mx_secs}\r\n\
         ST: {st}\r\n\
         \r\n"
    )
}

/// Encode an SSDP `NOTIFY` `ssdp:alive` advertisement datagram.
pub fn encode_notify_alive(
    host: &str,
    location: &str,
    nt: &str,
    usn: &str,
    server: &str,
    max_age_secs: u32,
) -> String {
    format!(
        "NOTIFY * HTTP/1.1\r\n\
         HOST: {host}\r\n\
         CACHE-CONTROL: max-age={max_age_secs}\r\n\
         LOCATION: {location}\r\n\
         NT: {nt}\r\n\
         NTS: ssdp:alive\r\n\
         SERVER: {server}\r\n\
         USN: {usn}\r\n\
         \r\n"
    )
}

/// Encode an SSDP `NOTIFY` `ssdp:byebye` advertisement datagram.
pub fn encode_notify_byebye(host: &str, nt: &str, usn: &str) -> String {
    format!(
        "NOTIFY * HTTP/1.1\r\n\
         HOST: {host}\r\n\
         NT: {nt}\r\n\
         NTS: ssdp:byebye\r\n\
         USN: {usn}\r\n\
         \r\n"
    )
}

/// Encode an SSDP `HTTP/1.1 200 OK` unicast response to an `M-SEARCH` request.
pub fn encode_search_response(
    location: &str,
    st: &str,
    usn: &str,
    server: &str,
    max_age_secs: u32,
) -> String {
    format!(
        "HTTP/1.1 200 OK\r\n\
         CACHE-CONTROL: max-age={max_age_secs}\r\n\
         EXT:\r\n\
         LOCATION: {location}\r\n\
         SERVER: {server}\r\n\
         ST: {st}\r\n\
         USN: {usn}\r\n\
         \r\n"
    )
}

/// Format the SSDP USN for a given UDN and target (`nt`/`st`).
///
/// Per UPnP Device Architecture 1.1:
/// - When target equals the UDN (`uuid:...`), USN is `uuid:...`.
/// - Otherwise USN is `uuid:...::<target>`.
pub fn format_usn(udn: &str, target: &str) -> String {
    if target == udn {
        udn.to_string()
    } else {
        format!("{udn}::{target}")
    }
}

/// Extract the device UDN prefix from an SSDP `USN` header value.
pub fn udn_from_usn(usn: &str) -> &str {
    usn.split_once("::").map_or(usn, |(udn, _)| udn)
}

/// Return true if an `M-SEARCH` `ST` header matches a given advertised target.
pub fn search_target_matches(st: &str, advertised_target: &str) -> bool {
    st == SSDP_ALL || st == advertised_target
}

/// Decode a raw SSDP UDP packet payload.
pub fn decode_ssdp_packet(packet: &[u8]) -> Result<SsdpMessage, Error> {
    let text = std::str::from_utf8(packet).map_err(|e| Error::InvalidValue {
        field: "ssdp",
        value: e.to_string(),
    })?;
    let mut lines = text.lines();
    let first_line = lines
        .next()
        .ok_or_else(|| Error::InvalidValue {
            field: "ssdp",
            value: "empty packet".into(),
        })?
        .trim();

    let mut headers: HashMap<String, String> = HashMap::new();
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }

    let max_age = headers
        .get("cache-control")
        .and_then(|cc| parse_max_age(cc));

    if first_line.starts_with("M-SEARCH ") {
        let st = headers.remove("st").ok_or_else(|| Error::InvalidValue {
            field: "ST",
            value: String::new(),
        })?;
        let mx = headers.get("mx").and_then(|v| v.parse::<u32>().ok());
        let man = headers.remove("man");
        Ok(SsdpMessage::MSearch(SsdpSearch { st, mx, man }))
    } else if first_line.starts_with("NOTIFY ") {
        let nt = headers.remove("nt").ok_or_else(|| Error::InvalidValue {
            field: "NT",
            value: String::new(),
        })?;
        let nts = headers.remove("nts").ok_or_else(|| Error::InvalidValue {
            field: "NTS",
            value: String::new(),
        })?;
        let usn = headers.remove("usn").ok_or_else(|| Error::InvalidValue {
            field: "USN",
            value: String::new(),
        })?;
        let notify = SsdpNotify {
            nt,
            nts: nts.clone(),
            usn,
            location: headers.remove("location"),
            server: headers.remove("server"),
            max_age,
        };
        match nts.to_ascii_lowercase().as_str() {
            "ssdp:alive" => Ok(SsdpMessage::NotifyAlive(notify)),
            "ssdp:byebye" => Ok(SsdpMessage::NotifyByeBye(notify)),
            other => Err(Error::InvalidValue {
                field: "NTS",
                value: other.to_string(),
            }),
        }
    } else if first_line.starts_with("HTTP/1.1 200") || first_line.starts_with("HTTP/1.0 200") {
        let st = headers.remove("st").ok_or_else(|| Error::InvalidValue {
            field: "ST",
            value: String::new(),
        })?;
        let usn = headers.remove("usn").ok_or_else(|| Error::InvalidValue {
            field: "USN",
            value: String::new(),
        })?;
        let location = headers
            .remove("location")
            .ok_or_else(|| Error::InvalidValue {
                field: "LOCATION",
                value: String::new(),
            })?;
        Ok(SsdpMessage::Response(SsdpResponse {
            st,
            usn,
            location,
            server: headers.remove("server"),
            max_age,
        }))
    } else {
        Err(Error::InvalidValue {
            field: "ssdp",
            value: first_line.to_string(),
        })
    }
}

fn parse_max_age(cache_control: &str) -> Option<u32> {
    for part in cache_control.split(',') {
        let trimmed = part.trim();
        if let Some((k, v)) = trimmed.split_once('=')
            && k.trim().eq_ignore_ascii_case("max-age")
        {
            return v.trim().parse::<u32>().ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::urn::DEVICE_SOURCE_1;

    #[test]
    fn msearch_notify_and_response_roundtrip() {
        let msearch = encode_msearch("239.255.255.250:1900", DEVICE_SOURCE_1, 3);
        assert_eq!(
            decode_ssdp_packet(msearch.as_bytes()).unwrap(),
            SsdpMessage::MSearch(SsdpSearch {
                st: DEVICE_SOURCE_1.into(),
                mx: Some(3),
                man: Some("\"ssdp:discover\"".into()),
            })
        );

        let udn = "uuid:1234";
        let usn = format_usn(udn, DEVICE_SOURCE_1);
        assert_eq!(udn_from_usn(&usn), udn);
        assert_eq!(format_usn(udn, udn), udn);

        let alive = encode_notify_alive(
            "239.255.255.250:1900",
            "http://192.168.1.2:49152/description.xml",
            DEVICE_SOURCE_1,
            &usn,
            "OpenHome-RS/0.1 UPnP/1.1",
            1800,
        );
        assert_eq!(
            decode_ssdp_packet(alive.as_bytes()).unwrap(),
            SsdpMessage::NotifyAlive(SsdpNotify {
                nt: DEVICE_SOURCE_1.into(),
                nts: "ssdp:alive".into(),
                usn: usn.clone(),
                location: Some("http://192.168.1.2:49152/description.xml".into()),
                server: Some("OpenHome-RS/0.1 UPnP/1.1".into()),
                max_age: Some(1800),
            })
        );

        let byebye = encode_notify_byebye("239.255.255.250:1900", DEVICE_SOURCE_1, &usn);
        assert!(matches!(
            decode_ssdp_packet(byebye.as_bytes()).unwrap(),
            SsdpMessage::NotifyByeBye(_)
        ));

        let resp = encode_search_response(
            "http://192.168.1.2:49152/description.xml",
            DEVICE_SOURCE_1,
            &usn,
            "OpenHome-RS/0.1 UPnP/1.1",
            1800,
        );
        assert_eq!(
            decode_ssdp_packet(resp.as_bytes()).unwrap(),
            SsdpMessage::Response(SsdpResponse {
                st: DEVICE_SOURCE_1.into(),
                usn,
                location: "http://192.168.1.2:49152/description.xml".into(),
                server: Some("OpenHome-RS/0.1 UPnP/1.1".into()),
                max_age: Some(1800),
            })
        );

        assert!(search_target_matches(SSDP_ALL, DEVICE_SOURCE_1));
        assert!(search_target_matches(DEVICE_SOURCE_1, DEVICE_SOURCE_1));
        assert!(!search_target_matches("upnp:rootdevice", DEVICE_SOURCE_1));
    }

    #[test]
    fn ssdp_handles_lf_only_lowercase_headers_and_rejects_malformed() {
        let lf_packet = "HTTP/1.1 200 OK\n\
                         cache-control: public, MAX-AGE = 90\n\
                         location: http://10.0.0.5:8080/desc.xml\n\
                         st: urn:av-openhome-org:device:Source:1\n\
                         usn: uuid:999::urn:av-openhome-org:device:Source:1\n\n";
        let msg = decode_ssdp_packet(lf_packet.as_bytes()).unwrap();
        assert_eq!(
            msg,
            SsdpMessage::Response(SsdpResponse {
                st: DEVICE_SOURCE_1.into(),
                usn: "uuid:999::urn:av-openhome-org:device:Source:1".into(),
                location: "http://10.0.0.5:8080/desc.xml".into(),
                server: None,
                max_age: Some(90),
            })
        );

        assert!(decode_ssdp_packet(b"").is_err());
        assert!(decode_ssdp_packet(&[0xff, 0xfe]).is_err());
        assert!(decode_ssdp_packet(b"BOGUS * HTTP/1.1\r\n\r\n").is_err());
        assert!(decode_ssdp_packet(b"M-SEARCH * HTTP/1.1\r\n\r\n").is_err());
        assert!(
            decode_ssdp_packet(b"NOTIFY * HTTP/1.1\r\nNT: x\r\nNTS: ssdp:weird\r\nUSN: u\r\n\r\n")
                .is_err()
        );
    }
}
