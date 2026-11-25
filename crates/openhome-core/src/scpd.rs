//! Embedded UPnP Service Control Protocol Description (SCPD) XML definitions
//! for OpenHome Media services.

use crate::urn::{
    SERVICE_INFO_1, SERVICE_PLAYLIST_1, SERVICE_PRODUCT_1, SERVICE_RECEIVER_1, SERVICE_SENDER_1,
    SERVICE_TIME_1, SERVICE_VOLUME_1,
};

pub const PRODUCT_1_XML: &str = include_str!("../scpd/Product1.xml");
pub const PLAYLIST_1_XML: &str = include_str!("../scpd/Playlist1.xml");
pub const INFO_1_XML: &str = include_str!("../scpd/Info1.xml");
pub const TIME_1_XML: &str = include_str!("../scpd/Time1.xml");
pub const VOLUME_1_XML: &str = include_str!("../scpd/Volume1.xml");
pub const RECEIVER_1_XML: &str = include_str!("../scpd/Receiver1.xml");
pub const SENDER_1_XML: &str = include_str!("../scpd/Sender1.xml");

/// Return the SCPD XML document for an OpenHome service URN, if known.
pub fn scpd_for_service_type(service_type: &str) -> Option<&'static str> {
    match service_type {
        SERVICE_PRODUCT_1 => Some(PRODUCT_1_XML),
        SERVICE_PLAYLIST_1 => Some(PLAYLIST_1_XML),
        SERVICE_INFO_1 => Some(INFO_1_XML),
        SERVICE_TIME_1 => Some(TIME_1_XML),
        SERVICE_VOLUME_1 => Some(VOLUME_1_XML),
        SERVICE_RECEIVER_1 => Some(RECEIVER_1_XML),
        SERVICE_SENDER_1 => Some(SENDER_1_XML),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use quick_xml::Reader;
    use quick_xml::events::Event;

    use super::*;

    fn collect_actions(xml: &str) -> Vec<String> {
        let mut reader = Reader::from_str(xml);
        let mut in_action = false;
        let mut in_name = false;
        let mut in_argument = false;
        let mut actions = Vec::new();
        let mut current = String::new();

        loop {
            match reader.read_event().expect("valid SCPD XML") {
                Event::Start(e) => match e.local_name().as_ref() {
                    b"action" => in_action = true,
                    b"argument" => in_argument = true,
                    b"name" if in_action && !in_argument => {
                        in_name = true;
                        current.clear();
                    }
                    _ => {}
                },
                Event::Text(e) if in_name => {
                    current.push_str(&e.unescape().unwrap());
                }
                Event::End(e) => match e.local_name().as_ref() {
                    b"action" => in_action = false,
                    b"argument" => in_argument = false,
                    b"name" if in_name => {
                        in_name = false;
                        actions.push(std::mem::take(&mut current));
                    }
                    _ => {}
                },
                Event::Eof => break,
                _ => {}
            }
        }
        actions
    }

    #[test]
    fn all_scpds_parse_and_expose_expected_actions() {
        let cases = [
            (SERVICE_PRODUCT_1, 14),
            (SERVICE_PLAYLIST_1, 24),
            (SERVICE_INFO_1, 4),
            (SERVICE_TIME_1, 1),
            (SERVICE_VOLUME_1, 16),
            (SERVICE_RECEIVER_1, 6),
            (SERVICE_SENDER_1, 5),
        ];

        for (urn, expected_count) in cases {
            let xml = scpd_for_service_type(urn).expect("SCPD exists");
            let actions = collect_actions(xml);
            assert_eq!(
                actions.len(),
                expected_count,
                "unexpected action count for {urn}: {actions:?}"
            );
        }
    }
}
