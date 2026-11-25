//! XML helpers for OpenHome `SourceXml`, `TrackList`, and escaping.

use quick_xml::Reader;
use quick_xml::events::Event;

use crate::error::Error;
use crate::types::{Source, SourceType, TrackEntry};

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
}
