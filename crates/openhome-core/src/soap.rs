//! SOAP envelope encoding and decoding for OpenHome UPnP actions and faults.

use std::collections::HashMap;

use quick_xml::Reader;
use quick_xml::events::Event;

use crate::error::{Error, Fault};
use crate::urn::{SOAP_ENVELOPE_NS, UPNP_CONTROL_NS};
use crate::xml::escape_xml;

/// Build a SOAP action request envelope.
pub fn encode_action_request(service_urn: &str, action: &str, args: &[(&str, &str)]) -> String {
    let mut out = format!(
        r#"<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="{SOAP_ENVELOPE_NS}" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body><u:{action} xmlns:u="{service_urn}">"#
    );
    for (k, v) in args {
        out.push('<');
        out.push_str(k);
        out.push('>');
        out.push_str(&escape_xml(v));
        out.push_str("</");
        out.push_str(k);
        out.push('>');
    }
    out.push_str(&format!("</u:{action}></s:Body></s:Envelope>"));
    out
}

/// Build a SOAP action response envelope.
pub fn encode_action_response(service_urn: &str, action: &str, args: &[(&str, &str)]) -> String {
    let mut out = format!(
        r#"<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="{SOAP_ENVELOPE_NS}" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body><u:{action}Response xmlns:u="{service_urn}">"#
    );
    for (k, v) in args {
        out.push('<');
        out.push_str(k);
        out.push('>');
        out.push_str(&escape_xml(v));
        out.push_str("</");
        out.push_str(k);
        out.push('>');
    }
    out.push_str(&format!("</u:{action}Response></s:Body></s:Envelope>"));
    out
}

/// Build a UPnP/OpenHome SOAP Fault envelope.
pub fn encode_fault(fault: &Fault) -> String {
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="{SOAP_ENVELOPE_NS}" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body><s:Fault><faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring><detail><UPnPError xmlns="{UPNP_CONTROL_NS}"><errorCode>{}</errorCode><errorDescription>{}</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>"#,
        fault.code,
        escape_xml(&fault.description)
    )
}

/// Parse a `SOAPACTION` HTTP header value such as
/// `"urn:av-openhome-org:service:Playlist:1#Play"` into `(service_urn, action)`.
pub fn parse_soapaction_header(header: &str) -> Option<(&str, &str)> {
    let trimmed = header.trim().trim_matches('"');
    let (service, action) = trimmed.split_once('#')?;
    if service.is_empty() || action.is_empty() {
        return None;
    }
    Some((service, action))
}

/// Decode a UPnP/OpenHome SOAP Fault from an XML envelope, if present.
pub fn decode_soap_fault(xml: &str) -> Result<Option<Fault>, Error> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);
    let mut in_fault = false;
    let mut current_tag: Option<String> = None;
    let mut error_code = String::new();
    let mut error_desc = String::new();
    let mut fault_string = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if local == "Fault" {
                    in_fault = true;
                } else if in_fault {
                    current_tag = Some(local);
                }
            }
            Ok(Event::Text(e)) if in_fault => {
                if let Some(tag) = current_tag.as_deref() {
                    let text = e.unescape().map_err(|err| Error::Xml(err.to_string()))?;
                    match tag {
                        "errorCode" => error_code.push_str(&text),
                        "errorDescription" => error_desc.push_str(&text),
                        "faultstring" => fault_string.push_str(&text),
                        _ => {}
                    }
                }
            }
            Ok(Event::CData(e)) if in_fault => {
                if let Some(tag) = current_tag.as_deref() {
                    let text = String::from_utf8_lossy(e.as_ref());
                    match tag {
                        "errorCode" => error_code.push_str(&text),
                        "errorDescription" => error_desc.push_str(&text),
                        "faultstring" => fault_string.push_str(&text),
                        _ => {}
                    }
                }
            }
            Ok(Event::End(e)) => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if current_tag.as_deref() == Some(local.as_str()) {
                    current_tag = None;
                } else if local == "Fault" {
                    break;
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(Error::Xml(err.to_string())),
            _ => {}
        }
    }

    if !in_fault {
        return Ok(None);
    }

    let code = error_code
        .trim()
        .parse::<u16>()
        .unwrap_or(Fault::ACTION_FAILED);
    let description = if !error_desc.trim().is_empty() {
        error_desc.trim().to_string()
    } else if !fault_string.trim().is_empty() {
        fault_string.trim().to_string()
    } else {
        "UPnPError".to_string()
    };
    Ok(Some(Fault::new(code, description)))
}

/// Decode a SOAP action response envelope, returning `Err(Error::Fault(..))` if
/// the envelope carries a UPnP SOAP Fault.
pub fn decode_soap_response(xml: &str) -> Result<(String, HashMap<String, String>), Error> {
    if let Some(fault) = decode_soap_fault(xml)? {
        return Err(Error::Fault(fault));
    }
    decode_soap_args(xml)
}

/// Parse SOAP action arguments from the first child element inside `<s:Body>`.
pub fn decode_soap_args(xml: &str) -> Result<(String, HashMap<String, String>), Error> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);
    let mut in_body = false;
    let mut action_name: Option<String> = None;
    let mut current_arg: Option<String> = None;
    let mut args: HashMap<String, String> = HashMap::new();
    let mut depth_in_action = 0usize;

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if !in_body {
                    if local == "Body" {
                        in_body = true;
                    }
                } else if action_name.is_none() {
                    action_name = Some(local);
                } else {
                    if depth_in_action == 0 {
                        current_arg = Some(local.clone());
                        args.entry(local).or_default();
                    }
                    depth_in_action += 1;
                }
            }
            Ok(Event::Empty(e)) if in_body && action_name.is_some() && depth_in_action == 0 => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                args.entry(local).or_default();
            }
            Ok(Event::Text(e)) if depth_in_action == 1 => {
                if let Some(arg) = &current_arg {
                    let text = e.unescape().map_err(|err| Error::Xml(err.to_string()))?;
                    if let Some(slot) = args.get_mut(arg) {
                        slot.push_str(&text);
                    }
                }
            }
            Ok(Event::CData(e)) if depth_in_action == 1 => {
                if let Some(arg) = &current_arg {
                    let text = String::from_utf8_lossy(e.as_ref());
                    if let Some(slot) = args.get_mut(arg) {
                        slot.push_str(&text);
                    }
                }
            }
            Ok(Event::End(e)) => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if depth_in_action > 0 {
                    depth_in_action -= 1;
                    if depth_in_action == 0 {
                        current_arg = None;
                    }
                } else if action_name.as_deref() == Some(local.as_str()) {
                    break;
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(Error::Xml(err.to_string())),
            _ => {}
        }
    }

    let action = action_name.ok_or_else(|| Error::Xml("missing SOAP Body action".into()))?;
    Ok((action, args))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::urn::SERVICE_PLAYLIST_1;

    #[test]
    fn soap_request_and_response_roundtrip() {
        let req = encode_action_request(
            SERVICE_PLAYLIST_1,
            "Insert",
            &[
                ("AfterId", "0"),
                ("Uri", "http://example.com/a.flac?x=1&y=2"),
                ("Metadata", "<DIDL-Lite/>"),
            ],
        );
        let (action, args) = decode_soap_args(&req).unwrap();
        assert_eq!(action, "Insert");
        assert_eq!(args.get("AfterId").map(String::as_str), Some("0"));
        assert_eq!(
            args.get("Uri").map(String::as_str),
            Some("http://example.com/a.flac?x=1&y=2")
        );
        assert_eq!(
            args.get("Metadata").map(String::as_str),
            Some("<DIDL-Lite/>")
        );
    }

    #[test]
    fn soap_fault_roundtrip_and_soapaction_parse() {
        let fault = Fault::invalid_id();
        let xml = encode_fault(&fault);
        assert_eq!(decode_soap_fault(&xml).unwrap(), Some(fault.clone()));
        assert!(matches!(decode_soap_response(&xml), Err(Error::Fault(f)) if f == fault));

        assert_eq!(
            parse_soapaction_header("\"urn:av-openhome-org:service:Playlist:1#Insert\""),
            Some(("urn:av-openhome-org:service:Playlist:1", "Insert"))
        );
        assert_eq!(
            parse_soapaction_header("urn:av-openhome-org:service:Volume:1#SetVolume"),
            Some(("urn:av-openhome-org:service:Volume:1", "SetVolume"))
        );
        assert_eq!(parse_soapaction_header("no-hash"), None);
        assert_eq!(parse_soapaction_header("#Play"), None);
        assert_eq!(parse_soapaction_header("urn:svc#"), None);
    }

    #[test]
    fn soap_edge_and_error_cases() {
        // Missing Body/action errors cleanly.
        assert!(decode_soap_args("<s:Envelope><s:Body/></s:Envelope>").is_err());

        // Self-closing empty arg & CDATA arg.
        let req = r#"<s:Envelope><s:Body><u:Insert><AfterId>0</AfterId><Uri><![CDATA[http://x/a&b]]></Uri><Metadata/></u:Insert></s:Body></s:Envelope>"#;
        let (act, args) = decode_soap_args(req).unwrap();
        assert_eq!(act, "Insert");
        assert_eq!(args.get("Uri").map(String::as_str), Some("http://x/a&b"));
        assert_eq!(args.get("Metadata").map(String::as_str), Some(""));

        // Non-fault response returns Ok(None).
        assert_eq!(decode_soap_fault(req).unwrap(), None);

        // Pretty-printed fault trims description; missing errorCode falls back to ACTION_FAILED.
        let fault_xml = r#"<s:Envelope><s:Body><s:Fault><faultstring>
          Custom Fault
        </faultstring></s:Fault></s:Body></s:Envelope>"#;
        let f = decode_soap_fault(fault_xml).unwrap().unwrap();
        assert_eq!(f.code, Fault::ACTION_FAILED);
        assert_eq!(f.description, "Custom Fault");
    }
}
