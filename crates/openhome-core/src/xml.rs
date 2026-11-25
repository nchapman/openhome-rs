//! XML helpers for OpenHome `SourceXml`, `TrackList`, and escaping.

use quick_xml::Reader;
use quick_xml::events::Event;

use crate::error::Error;
use crate::types::{DeviceDescription, ServiceDescription, Source, SourceType, TrackEntry};
use crate::urn::UPNP_DEVICE_NS;

/// Escape XML text and attribute special characters.
pub fn escape_xml(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

/// Format OpenHome `Product.SourceXml`.
pub fn encode_source_xml(sources: &[Source]) -> String {
    let mut out = String::from("<SourceList>");
    for src in sources {
        out.push_str("<Source><Name>");
        out.push_str(&escape_xml(&src.name));
        out.push_str("</Name><Type>");
        out.push_str(&escape_xml(src.source_type.as_str()));
        out.push_str("</Type><Visible>");
        out.push_str(if src.visible { "true" } else { "false" });
        out.push_str("</Visible><SystemName>");
        out.push_str(&escape_xml(&src.system_name));
        out.push_str("</SystemName></Source>");
    }
    out.push_str("</SourceList>");
    out
}

/// Parse OpenHome `Product.SourceXml`.
pub fn decode_source_xml(xml: &str) -> Result<Vec<Source>, Error> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);
    let mut sources = Vec::new();
    let mut in_source = false;
    let mut current_tag: Option<String> = None;
    let mut name = String::new();
    let mut source_type = String::new();
    let mut visible = true;
    let mut system_name = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if tag == "Source" {
                    in_source = true;
                    name.clear();
                    source_type.clear();
                    visible = true;
                    system_name.clear();
                } else if in_source {
                    current_tag = Some(tag);
                }
            }
            Ok(Event::Text(e)) if in_source => {
                if let Some(tag) = current_tag.as_deref() {
                    let text = e.unescape().map_err(|err| Error::Xml(err.to_string()))?;
                    match tag {
                        "Name" => name.push_str(&text),
                        "Type" => source_type.push_str(&text),
                        "Visible" => visible = text.trim() == "true" || text.trim() == "1",
                        "SystemName" => system_name.push_str(&text),
                        _ => {}
                    }
                }
            }
            Ok(Event::CData(e)) if in_source => {
                if let Some(tag) = current_tag.as_deref() {
                    let text = String::from_utf8_lossy(e.as_ref());
                    match tag {
                        "Name" => name.push_str(&text),
                        "Type" => source_type.push_str(&text),
                        "Visible" => visible = text.trim() == "true" || text.trim() == "1",
                        "SystemName" => system_name.push_str(&text),
                        _ => {}
                    }
                }
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if tag == "Source" && in_source {
                    if system_name.is_empty() {
                        system_name = name.clone();
                    }
                    sources.push(Source {
                        name: std::mem::take(&mut name),
                        source_type: SourceType::parse(&source_type),
                        visible,
                        system_name: std::mem::take(&mut system_name),
                    });
                    in_source = false;
                    current_tag = None;
                } else if current_tag.as_deref() == Some(tag.as_str()) {
                    current_tag = None;
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(Error::Xml(err.to_string())),
            _ => {}
        }
    }
    Ok(sources)
}

/// Format OpenHome `Playlist.ReadList` `<TrackList>` payload.
pub fn encode_track_list(entries: &[TrackEntry]) -> String {
    let mut out = String::from("<TrackList>");
    for entry in entries {
        out.push_str("<Entry><Id>");
        out.push_str(&entry.id.to_string());
        out.push_str("</Id><Uri>");
        out.push_str(&escape_xml(&entry.uri));
        out.push_str("</Uri><Metadata>");
        out.push_str(&escape_xml(&entry.metadata));
        out.push_str("</Metadata></Entry>");
    }
    out.push_str("</TrackList>");
    out
}

/// Parse OpenHome `Playlist.ReadList` `<TrackList>` payload.
pub fn decode_track_list(xml: &str) -> Result<Vec<TrackEntry>, Error> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);
    let mut entries = Vec::new();
    let mut in_entry = false;
    let mut current_tag: Option<String> = None;
    let mut id_str = String::new();
    let mut uri = String::new();
    let mut metadata = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if tag == "Entry" {
                    in_entry = true;
                    id_str.clear();
                    uri.clear();
                    metadata.clear();
                } else if in_entry {
                    current_tag = Some(tag);
                }
            }
            Ok(Event::Text(e)) if in_entry => {
                if let Some(tag) = current_tag.as_deref() {
                    let text = e.unescape().map_err(|err| Error::Xml(err.to_string()))?;
                    match tag {
                        "Id" => id_str.push_str(&text),
                        "Uri" => uri.push_str(&text),
                        "Metadata" => metadata.push_str(&text),
                        _ => {}
                    }
                }
            }
            Ok(Event::CData(e)) if in_entry => {
                if let Some(tag) = current_tag.as_deref() {
                    let text = String::from_utf8_lossy(e.as_ref());
                    match tag {
                        "Id" => id_str.push_str(&text),
                        "Uri" => uri.push_str(&text),
                        "Metadata" => metadata.push_str(&text),
                        _ => {}
                    }
                }
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if tag == "Entry" && in_entry {
                    let id = id_str
                        .trim()
                        .parse::<u32>()
                        .map_err(|_| Error::InvalidValue {
                            field: "Id",
                            value: id_str.clone(),
                        })?;
                    entries.push(TrackEntry {
                        id,
                        uri: std::mem::take(&mut uri),
                        metadata: std::mem::take(&mut metadata),
                    });
                    in_entry = false;
                    current_tag = None;
                } else if current_tag.as_deref() == Some(tag.as_str()) {
                    current_tag = None;
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(Error::Xml(err.to_string())),
            _ => {}
        }
    }
    Ok(entries)
}

/// Parse a space-separated `Playlist.ReadList` `IdList` string into valid `u32` IDs.
///
/// Silently skips invalid tokens such as Lumin's `-1` sentinel, matching both
/// `ohPipeline` and `upmpdcli`.
pub fn parse_id_list(id_list: &str) -> Vec<u32> {
    id_list
        .split_whitespace()
        .filter_map(|tok| tok.parse::<u32>().ok())
        .collect()
}

/// Format a slice of track IDs into a space-separated `Playlist.ReadList` `IdList`.
pub fn format_id_list(ids: &[u32]) -> String {
    let mut out = String::new();
    for (idx, id) in ids.iter().enumerate() {
        if idx > 0 {
            out.push(' ');
        }
        out.push_str(&id.to_string());
    }
    out
}

/// Encode a UPnP device description document (`description.xml`).
pub fn encode_device_description(desc: &DeviceDescription) -> String {
    let mut out = format!(
        r#"<?xml version="1.0" encoding="utf-8"?><root xmlns="{UPNP_DEVICE_NS}"><specVersion><major>1</major><minor>0</minor></specVersion><device>"#
    );
    push_tag(&mut out, "deviceType", &desc.device_type);
    push_tag(&mut out, "friendlyName", &desc.friendly_name);
    push_tag(&mut out, "manufacturer", &desc.manufacturer);
    if let Some(v) = &desc.manufacturer_url {
        push_tag(&mut out, "manufacturerURL", v);
    }
    if let Some(v) = &desc.model_description {
        push_tag(&mut out, "modelDescription", v);
    }
    push_tag(&mut out, "modelName", &desc.model_name);
    if let Some(v) = &desc.model_number {
        push_tag(&mut out, "modelNumber", v);
    }
    if let Some(v) = &desc.model_url {
        push_tag(&mut out, "modelURL", v);
    }
    if let Some(v) = &desc.serial_number {
        push_tag(&mut out, "serialNumber", v);
    }
    push_tag(&mut out, "UDN", &desc.udn);
    if let Some(v) = &desc.presentation_url {
        push_tag(&mut out, "presentationURL", v);
    }
    out.push_str("<serviceList>");
    for svc in &desc.services {
        out.push_str("<service>");
        push_tag(&mut out, "serviceType", &svc.service_type);
        push_tag(&mut out, "serviceId", &svc.service_id);
        push_tag(&mut out, "SCPDURL", &svc.scpd_url);
        push_tag(&mut out, "controlURL", &svc.control_url);
        push_tag(&mut out, "eventSubURL", &svc.event_sub_url);
        out.push_str("</service>");
    }
    out.push_str("</serviceList></device></root>");
    out
}

fn push_tag(out: &mut String, tag: &str, value: &str) {
    out.push('<');
    out.push_str(tag);
    out.push('>');
    out.push_str(&escape_xml(value));
    out.push_str("</");
    out.push_str(tag);
    out.push('>');
}

/// Decode a UPnP device description document (`description.xml`).
pub fn decode_device_description(xml: &str) -> Result<DeviceDescription, Error> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(false);

    let mut device_depth = 0usize;
    let mut in_service = false;
    let mut current_tag: Option<String> = None;

    let mut device_type = String::new();
    let mut friendly_name = String::new();
    let mut manufacturer = String::new();
    let mut manufacturer_url: Option<String> = None;
    let mut model_description: Option<String> = None;
    let mut model_name = String::new();
    let mut model_number: Option<String> = None;
    let mut model_url: Option<String> = None;
    let mut serial_number: Option<String> = None;
    let mut udn = String::new();
    let mut presentation_url: Option<String> = None;
    let mut services = Vec::new();

    let mut svc_type = String::new();
    let mut svc_id = String::new();
    let mut scpd_url = String::new();
    let mut control_url = String::new();
    let mut event_sub_url = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if tag == "device" {
                    device_depth += 1;
                } else if device_depth == 1 {
                    if tag == "service" {
                        in_service = true;
                        svc_type.clear();
                        svc_id.clear();
                        scpd_url.clear();
                        control_url.clear();
                        event_sub_url.clear();
                    } else {
                        current_tag = Some(tag);
                    }
                }
            }
            Ok(Event::Text(e)) if device_depth == 1 => {
                if let Some(tag) = current_tag.as_deref() {
                    let text = e.unescape().map_err(|err| Error::Xml(err.to_string()))?;
                    if in_service {
                        match tag {
                            "serviceType" => svc_type.push_str(&text),
                            "serviceId" => svc_id.push_str(&text),
                            "SCPDURL" => scpd_url.push_str(&text),
                            "controlURL" => control_url.push_str(&text),
                            "eventSubURL" => event_sub_url.push_str(&text),
                            _ => {}
                        }
                    } else {
                        match tag {
                            "deviceType" => device_type.push_str(&text),
                            "friendlyName" => friendly_name.push_str(&text),
                            "manufacturer" => manufacturer.push_str(&text),
                            "manufacturerURL" => {
                                manufacturer_url.get_or_insert_default().push_str(&text)
                            }
                            "modelDescription" => {
                                model_description.get_or_insert_default().push_str(&text)
                            }
                            "modelName" => model_name.push_str(&text),
                            "modelNumber" => model_number.get_or_insert_default().push_str(&text),
                            "modelURL" => model_url.get_or_insert_default().push_str(&text),
                            "serialNumber" => serial_number.get_or_insert_default().push_str(&text),
                            "UDN" => udn.push_str(&text),
                            "presentationURL" => {
                                presentation_url.get_or_insert_default().push_str(&text)
                            }
                            _ => {}
                        }
                    }
                }
            }
            Ok(Event::CData(e)) if device_depth == 1 => {
                if let Some(tag) = current_tag.as_deref() {
                    let text = String::from_utf8_lossy(e.as_ref());
                    if in_service {
                        match tag {
                            "serviceType" => svc_type.push_str(&text),
                            "serviceId" => svc_id.push_str(&text),
                            "SCPDURL" => scpd_url.push_str(&text),
                            "controlURL" => control_url.push_str(&text),
                            "eventSubURL" => event_sub_url.push_str(&text),
                            _ => {}
                        }
                    } else {
                        match tag {
                            "deviceType" => device_type.push_str(&text),
                            "friendlyName" => friendly_name.push_str(&text),
                            "manufacturer" => manufacturer.push_str(&text),
                            "manufacturerURL" => {
                                manufacturer_url.get_or_insert_default().push_str(&text)
                            }
                            "modelDescription" => {
                                model_description.get_or_insert_default().push_str(&text)
                            }
                            "modelName" => model_name.push_str(&text),
                            "modelNumber" => model_number.get_or_insert_default().push_str(&text),
                            "modelURL" => model_url.get_or_insert_default().push_str(&text),
                            "serialNumber" => serial_number.get_or_insert_default().push_str(&text),
                            "UDN" => udn.push_str(&text),
                            "presentationURL" => {
                                presentation_url.get_or_insert_default().push_str(&text)
                            }
                            _ => {}
                        }
                    }
                }
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.local_name().as_ref()).into_owned();
                if tag == "device" {
                    device_depth = device_depth.saturating_sub(1);
                    if device_depth == 0 {
                        break;
                    }
                } else if device_depth == 1 {
                    if tag == "service" && in_service {
                        services.push(ServiceDescription {
                            service_type: std::mem::take(&mut svc_type),
                            service_id: std::mem::take(&mut svc_id),
                            scpd_url: std::mem::take(&mut scpd_url),
                            control_url: std::mem::take(&mut control_url),
                            event_sub_url: std::mem::take(&mut event_sub_url),
                        });
                        in_service = false;
                        current_tag = None;
                    } else if current_tag.as_deref() == Some(tag.as_str()) {
                        current_tag = None;
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(err) => return Err(Error::Xml(err.to_string())),
            _ => {}
        }
    }

    if udn.is_empty() || device_type.is_empty() {
        return Err(Error::Xml(
            "device description missing required deviceType or UDN".into(),
        ));
    }

    Ok(DeviceDescription {
        device_type,
        friendly_name,
        manufacturer,
        manufacturer_url,
        model_description,
        model_name,
        model_number,
        model_url,
        serial_number,
        udn,
        presentation_url,
        services,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_xml_roundtrip() {
        let sources = vec![
            Source {
                name: "Playlist".into(),
                source_type: SourceType::Playlist,
                visible: true,
                system_name: "Playlist".into(),
            },
            Source {
                name: "Songcast".into(),
                source_type: SourceType::Receiver,
                visible: true,
                system_name: "Receiver".into(),
            },
        ];
        let xml = encode_source_xml(&sources);
        assert_eq!(decode_source_xml(&xml).unwrap(), sources);
    }

    #[test]
    fn track_list_roundtrip_with_didl_and_special_chars() {
        let entries = vec![TrackEntry {
            id: 42,
            uri: "http://192.168.1.5:8080/track?a=1&b=2".into(),
            metadata: "<DIDL-Lite><item><dc:title>A &amp; B</dc:title></item></DIDL-Lite>".into(),
        }];
        let xml = encode_track_list(&entries);
        assert_eq!(decode_track_list(&xml).unwrap(), entries);
    }

    #[test]
    fn id_list_skips_lumin_negative_one() {
        assert_eq!(parse_id_list("1 -1 2 42 bad 3"), vec![1, 2, 42, 3]);
        assert_eq!(format_id_list(&[1, 2, 42]), "1 2 42");
    }

    #[test]
    fn device_description_roundtrip() {
        let desc = DeviceDescription {
            device_type: crate::urn::DEVICE_SOURCE_1.into(),
            friendly_name: "Living Room & Den".into(),
            manufacturer: "OpenHome RS".into(),
            manufacturer_url: Some("https://example.com".into()),
            model_description: Some("Pure Rust Renderer".into()),
            model_name: "openhome-rs".into(),
            model_number: Some("0.1".into()),
            model_url: None,
            serial_number: Some("12345".into()),
            udn: "uuid:11111111-2222-3333-4444-555555555555".into(),
            presentation_url: Some("/".into()),
            services: vec![ServiceDescription {
                service_type: crate::urn::SERVICE_PRODUCT_1.into(),
                service_id: crate::urn::SERVICE_ID_PRODUCT.into(),
                scpd_url: "/scpd/Product.xml".into(),
                control_url: "/ctl/Product".into(),
                event_sub_url: "/evt/Product".into(),
            }],
        };
        let xml = encode_device_description(&desc);
        assert_eq!(decode_device_description(&xml).unwrap(), desc);
    }
}
