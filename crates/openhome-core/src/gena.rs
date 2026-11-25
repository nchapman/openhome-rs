//! Direct-property GENA `<e:propertyset>` codec for OpenHome services.

use std::collections::BTreeMap;

use quick_xml::Reader;
use quick_xml::events::Event;

use crate::error::Error;
use crate::urn::UPNP_EVENT_NS;
use crate::xml::escape_xml;

/// Encode OpenHome GENA state variable properties into `<e:propertyset>`.
pub fn encode_propertyset(props: &BTreeMap<String, String>) -> String {
    let mut out = format!(
        r#"<?xml version="1.0" encoding="utf-8"?><e:propertyset xmlns:e="{UPNP_EVENT_NS}">"#
    );
    for (name, value) in props {
        out.push_str("<e:property><");
        out.push_str(name);
        out.push('>');
        out.push_str(&escape_xml(value));
        out.push_str("</");
        out.push_str(name);
        out.push_str("></e:property>");
    }
    out.push_str("</e:propertyset>");
    out
}

/// Parse a GENA `CALLBACK` header containing one or more angle-bracketed URLs
/// such as `<http://192.168.1.10:5000/callback>`.
pub fn parse_callback_header(header: &str) -> Vec<String> {
    let mut urls = Vec::new();
    let mut rest = header;
    while let Some(start) = rest.find('<') {
        rest = &rest[start + 1..];
        if let Some(end) = rest.find('>') {
            let url = rest[..end].trim();
            // Per UPnP GENA 1.1, event callback URLs use HTTP.
            if url.starts_with("http://") {
                urls.push(url.to_string());
            }
            rest = &rest[end + 1..];
        } else {
            break;
        }
    }
    urls
}

/// Parse a GENA `TIMEOUT` header (e.g. `Second-1800`, `Second-infinite`) into seconds.
/// Returns `None` if unparseable or `infinite`.
pub fn parse_timeout_header(header: &str) -> Option<u32> {
    let trimmed = header.trim();
    let rest = trimmed
        .strip_prefix("Second-")
        .or_else(|| trimmed.strip_prefix("second-"))?;
    if rest.eq_ignore_ascii_case("infinite") {
        return None;
    }
    rest.parse::<u32>().ok().filter(|&s| s > 0)
}

/// Format a GENA `TIMEOUT` header value (`Second-<secs>`).
pub fn format_timeout_header(secs: u32) -> String {
    format!("Second-{secs}")
}

/// Decode an OpenHome GENA `<e:propertyset>` XML body into key/value pairs.
pub fn decode_propertyset(xml: &str) -> Result<BTreeMap<String, String>, Error> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);
    let mut in_property = false;
    let mut current_var: Option<String> = None;
    let mut props: BTreeMap<String, String> = BTreeMap::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if !in_property {
                    if local == "property" {
                        in_property = true;
                    }
                } else if current_var.is_none() {
                    props.entry(local.clone()).or_default();
                    current_var = Some(local);
                }
            }
            Ok(Event::Empty(e)) if in_property && current_var.is_none() => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                props.entry(local).or_default();
            }
            Ok(Event::Text(e)) if current_var.is_some() => {
                if let Some(var) = &current_var {
                    let text = e.unescape().map_err(|err| Error::Xml(err.to_string()))?;
                    if let Some(slot) = props.get_mut(var) {
                        slot.push_str(&text);
                    }
                }
            }
            Ok(Event::CData(e)) if current_var.is_some() => {
                if let Some(var) = &current_var {
                    let text = String::from_utf8_lossy(e.as_ref());
                    if let Some(slot) = props.get_mut(var) {
                        slot.push_str(&text);
                    }
                }
            }
            Ok(Event::End(e)) => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if current_var.as_deref() == Some(local.as_str()) {
                    current_var = None;
                } else if local == "property" {
                    in_property = false;
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(Error::Xml(err.to_string())),
            _ => {}
        }
    }
    Ok(props)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn propertyset_roundtrip() {
        let mut props = BTreeMap::new();
        props.insert("TransportState".into(), "Playing".into());
        props.insert("Repeat".into(), "1".into());
        props.insert("IdArray".into(), "AAAAAQAAAAI=".into());
        let xml = encode_propertyset(&props);
        assert_eq!(decode_propertyset(&xml).unwrap(), props);
    }

    #[test]
    fn callback_and_timeout_headers() {
        assert_eq!(
            parse_callback_header("<http://127.0.0.1:8080/evt><http://127.0.0.1:8081/evt>"),
            vec!["http://127.0.0.1:8080/evt", "http://127.0.0.1:8081/evt"]
        );
        assert_eq!(parse_timeout_header("Second-1800"), Some(1800));
        assert_eq!(parse_timeout_header("Second-infinite"), None);
        assert_eq!(format_timeout_header(300), "Second-300");
    }
}
