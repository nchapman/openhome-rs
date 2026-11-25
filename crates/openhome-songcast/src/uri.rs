//! Songcast URI parsing/formatting (`ohz://`, `ohm://`, `ohu://`), default ports,
//! multicast channel addressing, and `Sender.Metadata` DIDL-Lite helpers.

use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};

use openhome_core::xml::escape_xml;

use crate::error::SongcastError;

/// Default UDP port for OHM/OHU Songcast audio & control streams (`Ohm::kPort`).
pub const OHM_PORT: u16 = 51970;

/// Default UDP port for OHZ zone & preset discovery (`ZoneHandler::kMulticastPort`).
pub const OHZ_PORT: u16 = 51972;

/// Default multicast IPv4 address for OHZ zone discovery (`239.255.255.250`).
pub const OHZ_MULTICAST_IP: Ipv4Addr = Ipv4Addr::new(239, 255, 255, 250);

/// Default OHZ multicast socket address (`239.255.255.250:51972`).
pub const OHZ_MULTICAST_ADDR: SocketAddrV4 = SocketAddrV4::new(OHZ_MULTICAST_IP, OHZ_PORT);

/// Disabled / null Songcast URI advertised by an inactive sender (`ohu://0.0.0.0:0`).
pub const NULL_URI: &str = "ohu://0.0.0.0:0";

/// Compute the standard OHM multicast endpoint (`239.253.<hi>.<lo>:51970`) from a
/// 16-bit Songcast channel number, matching `OhmSender::UpdateChannel()`.
pub fn sender_channel_addr(channel: u16) -> SocketAddrV4 {
    let hi = (channel >> 8) as u8;
    let lo = (channel & 0xff) as u8;
    SocketAddrV4::new(Ipv4Addr::new(239, 253, hi, lo), OHM_PORT)
}

/// Format the canonical OHZ zone discovery URI for a sender UDN/zone name:
/// `ohz://239.255.255.250:51972/<zone>`.
pub fn zone_discovery_uri(zone: &str) -> String {
    format!("ohz://{OHZ_MULTICAST_ADDR}/{zone}")
}

/// Build the DIDL-Lite `Metadata` string advertised by an OpenHome `Sender` service
/// (matching `OhmSender::UpdateMetadata()` in `ohSongcast` / `ohPipeline`).
pub fn encode_sender_metadata(
    title: &str,
    zone_uri: &str,
    multicast: bool,
    album_art_uri: Option<&str>,
) -> String {
    let protocol_info = if multicast { "ohz:*:*:m" } else { "ohz:*:*:u" };
    let mut out = String::with_capacity(256);
    out.push_str(
        r#"<DIDL-Lite xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/" xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/">"#,
    );
    out.push_str(r#"<item id="0" restricted="True"><dc:title>"#);
    out.push_str(&escape_xml(title));
    out.push_str(r#"</dc:title><res protocolInfo=""#);
    out.push_str(protocol_info);
    out.push_str(r#"">"#);
    out.push_str(&escape_xml(zone_uri));
    out.push_str("</res>");
    if let Some(art) = album_art_uri
        && !art.is_empty()
    {
        out.push_str("<upnp:albumArtURI>");
        out.push_str(&escape_xml(art));
        out.push_str("</upnp:albumArtURI>");
    }
    out.push_str("<upnp:class>object.item.audioItem</upnp:class></item></DIDL-Lite>");
    out
}

/// Parsed Songcast URI (`ohm://`, `ohu://`, or `ohz://`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SongcastUri {
    /// Multicast OHM stream (`ohm://<ip>:<port>`).
    Multicast(SocketAddrV4),
    /// Unicast OHU stream (`ohu://<ip>:<port>`).
    Unicast(SocketAddrV4),
    /// OHZ zone indirection (`ohz://<ip>:<port>/<zone>`).
    Zone {
        endpoint: SocketAddrV4,
        zone: String,
    },
    /// Null / disabled endpoint (`ohm://0.0.0.0:0` or `ohu://0.0.0.0:0`).
    Null,
}

impl SongcastUri {
    /// Parse a Songcast URI (`ohm://`, `ohu://`, or `ohz://`).
    pub fn parse(s: &str) -> Result<Self, SongcastError> {
        let trimmed = s.trim();
        let (scheme, rest) = trimmed
            .split_once("://")
            .ok_or_else(|| SongcastError::InvalidUri(trimmed.to_string()))?;

        match scheme.to_ascii_lowercase().as_str() {
            "ohm" => {
                let addr: SocketAddrV4 = rest
                    .trim_end_matches('/')
                    .parse()
                    .map_err(|_| SongcastError::InvalidUri(trimmed.to_string()))?;
                if addr.ip().is_unspecified() && addr.port() == 0 {
                    Ok(Self::Null)
                } else {
                    Ok(Self::Multicast(addr))
                }
            }
            "ohu" => {
                let addr: SocketAddrV4 = rest
                    .trim_end_matches('/')
                    .parse()
                    .map_err(|_| SongcastError::InvalidUri(trimmed.to_string()))?;
                if addr.ip().is_unspecified() && addr.port() == 0 {
                    Ok(Self::Null)
                } else {
                    Ok(Self::Unicast(addr))
                }
            }
            "ohz" => {
                let (addr_part, zone_part) = rest
                    .split_once('/')
                    .ok_or_else(|| SongcastError::InvalidUri(trimmed.to_string()))?;
                if zone_part.is_empty() {
                    return Err(SongcastError::InvalidUri(trimmed.to_string()));
                }
                let endpoint: SocketAddrV4 = addr_part
                    .parse()
                    .map_err(|_| SongcastError::InvalidUri(trimmed.to_string()))?;
                Ok(Self::Zone {
                    endpoint,
                    zone: zone_part.to_string(),
                })
            }
            _ => Err(SongcastError::InvalidUri(trimmed.to_string())),
        }
    }
}

impl fmt::Display for SongcastUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Multicast(addr) => write!(f, "ohm://{addr}"),
            Self::Unicast(addr) => write!(f, "ohu://{addr}"),
            Self::Zone { endpoint, zone } => write!(f, "ohz://{endpoint}/{zone}"),
            Self::Null => f.write_str(NULL_URI),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_addr_and_zone_uri_match_oracle() {
        assert_eq!(
            sender_channel_addr(0x0102),
            SocketAddrV4::new(Ipv4Addr::new(239, 253, 1, 2), 51970)
        );
        assert_eq!(
            zone_discovery_uri("uuid:living-room"),
            "ohz://239.255.255.250:51972/uuid:living-room"
        );
    }

    #[test]
    fn songcast_uri_roundtrip_and_errors() {
        let m = SongcastUri::parse("ohm://239.253.0.1:51970").unwrap();
        assert_eq!(
            m,
            SongcastUri::Multicast(SocketAddrV4::new(Ipv4Addr::new(239, 253, 0, 1), 51970))
        );
        assert_eq!(m.to_string(), "ohm://239.253.0.1:51970");

        let u = SongcastUri::parse("ohu://192.168.1.5:51970").unwrap();
        assert_eq!(
            u,
            SongcastUri::Unicast(SocketAddrV4::new(Ipv4Addr::new(192, 168, 1, 5), 51970))
        );
        assert_eq!(u.to_string(), "ohu://192.168.1.5:51970");

        let z = SongcastUri::parse("ohz://239.255.255.250:51972/uuid:abc").unwrap();
        assert_eq!(
            z,
            SongcastUri::Zone {
                endpoint: OHZ_MULTICAST_ADDR,
                zone: "uuid:abc".into()
            }
        );
        assert_eq!(z.to_string(), "ohz://239.255.255.250:51972/uuid:abc");

        assert_eq!(SongcastUri::parse(NULL_URI).unwrap(), SongcastUri::Null);
        assert_eq!(
            SongcastUri::parse("ohm://0.0.0.0:0").unwrap(),
            SongcastUri::Null
        );
        assert_eq!(SongcastUri::Null.to_string(), NULL_URI);

        assert!(SongcastUri::parse("http://1.2.3.4/x").is_err());
        assert!(SongcastUri::parse("ohz://239.255.255.250:51972/").is_err());
        assert!(SongcastUri::parse("ohz://bad:addr/zone").is_err());
        assert!(SongcastUri::parse("no-scheme").is_err());
    }

    #[test]
    fn sender_metadata_encodes_multicast_and_unicast_didl() {
        let m = encode_sender_metadata(
            "Studio & Deck",
            "ohz://239.255.255.250:51972/uuid:1",
            true,
            Some("http://10.0.0.1/icon.png"),
        );
        assert!(m.contains("Studio &amp; Deck"));
        assert!(m.contains(r#"protocolInfo="ohz:*:*:m""#));
        assert!(m.contains("<upnp:albumArtURI>http://10.0.0.1/icon.png</upnp:albumArtURI>"));

        let u = encode_sender_metadata("Room", "ohz://239.255.255.250:51972/uuid:2", false, None);
        assert!(u.contains(r#"protocolInfo="ohz:*:*:u""#));
        assert!(!u.contains("albumArtURI"));
    }
}
