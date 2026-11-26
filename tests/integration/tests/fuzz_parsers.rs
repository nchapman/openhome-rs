//! Property-based and fuzz tests covering every zero-network wire parser, codec,
//! and stateful protocol engine in `openhome-core` and `openhome-songcast`.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::{Duration, Instant};

use openhome_core::error::Fault;
use openhome_core::gena::{
    decode_propertyset, encode_propertyset, parse_callback_header, parse_timeout_header,
};
use openhome_core::id_array::{decode_id_array, encode_id_array};
use openhome_core::soap::{
    decode_soap_args, decode_soap_fault, decode_soap_response, encode_action_request,
    encode_action_response, encode_fault, parse_soapaction_header,
};
use openhome_core::ssdp::decode_ssdp_packet;
use openhome_core::types::{Source, SourceType, TrackEntry};
use openhome_core::urn::SERVICE_PLAYLIST_1;
use openhome_core::xml::{
    decode_device_description, decode_source_xml, decode_track_list, encode_source_xml,
    encode_track_list, parse_id_list,
};
use openhome_songcast::{
    AudioFlags, ClockSync, MAX_RESEND_FRAMES_PER_REQ, OhmAudio, OhmMessage, OhmMetatext, OhmTrack,
    OhzMessage, PushOutcome, ReorderBuffer, SongcastUri,
};
use proptest::prelude::*;

/// Safe XML identifier (valid tag name).
fn xml_ident() -> impl Strategy<Value = String> {
    "[A-Za-z][A-Za-z0-9_]{0,15}".prop_map(String::from)
}

/// Safe XML text (no NUL or forbidden XML 1.0 control chars, but includes `<>&"'` and Unicode).
fn xml_text() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            Just('<'),
            Just('>'),
            Just('&'),
            Just('"'),
            Just('\''),
            Just(' '),
            Just('\n'),
            Just('é'),
            Just('🎵'),
            proptest::char::range('!', '~'),
        ],
        0..48,
    )
    .prop_map(|chars| chars.into_iter().collect())
}

fn ipv4_strategy() -> impl Strategy<Value = Ipv4Addr> {
    any::<[u8; 4]>().prop_map(Ipv4Addr::from)
}

fn socket_v4_strategy() -> impl Strategy<Value = SocketAddrV4> {
    (ipv4_strategy(), any::<u16>()).prop_map(|(ip, port)| SocketAddrV4::new(ip, port))
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        max_shrink_iters: 200,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    // -----------------------------------------------------------------------
    // 1. openhome-core: never-panic fuzzing on arbitrary strings
    // -----------------------------------------------------------------------

    #[test]
    fn core_parsers_never_panic_on_arbitrary_input(s in "\\PC{0,256}") {
        let _ = decode_soap_args(&s);
        let _ = decode_soap_response(&s);
        let _ = decode_soap_fault(&s);
        let _ = parse_soapaction_header(&s);
        let _ = decode_device_description(&s);
        let _ = decode_source_xml(&s);
        let _ = decode_track_list(&s);
        let _ = parse_id_list(&s);
        let _ = decode_id_array(&s);
        let _ = decode_propertyset(&s);
        let _ = parse_callback_header(&s);
        let _ = parse_timeout_header(&s);
        let _ = decode_ssdp_packet(s.as_bytes());
    }

    // -----------------------------------------------------------------------
    // 2. openhome-core: SOAP request / response / fault round-trips
    // -----------------------------------------------------------------------

    #[test]
    fn soap_request_and_response_roundtrip(
        action in xml_ident(),
        args in prop::collection::btree_map(xml_ident(), xml_text(), 0..6),
    ) {
        let pairs: Vec<(&str, &str)> = args.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

        let req_xml = encode_action_request(SERVICE_PLAYLIST_1, &action, &pairs);
        let (dec_action, dec_args) = decode_soap_args(&req_xml).unwrap();
        prop_assert_eq!(dec_action, action.clone());
        for (k, v) in &args {
            prop_assert_eq!(dec_args.get(k).map(String::as_str), Some(v.as_str()));
        }

        let resp_xml = encode_action_response(SERVICE_PLAYLIST_1, &action, &pairs);
        let (resp_action, resp_args) = decode_soap_response(&resp_xml).unwrap();
        prop_assert_eq!(resp_action, format!("{action}Response"));
        for (k, v) in &args {
            prop_assert_eq!(resp_args.get(k).map(String::as_str), Some(v.as_str()));
        }
    }

    #[test]
    fn soap_fault_roundtrip(code in 400u16..999u16, desc in xml_text()) {
        let fault = Fault::new(code, desc.clone());
        let xml = encode_fault(&fault);
        let decoded = decode_soap_fault(&xml).unwrap().expect("must decode fault");
        prop_assert_eq!(decoded.code, code);
        let trimmed = desc.trim();
        let expected_desc = if trimmed.is_empty() {
            "UPnPError"
        } else {
            trimmed
        };
        prop_assert_eq!(decoded.description, expected_desc);
    }

    // -----------------------------------------------------------------------
    // 3. openhome-core: IdArray, GENA propertyset, SourceXml, TrackList round-trips
    // -----------------------------------------------------------------------

    #[test]
    fn id_array_roundtrip(ids in prop::collection::vec(any::<u32>(), 0..64)) {
        let encoded = encode_id_array(&ids);
        let decoded = decode_id_array(&encoded).unwrap();
        prop_assert_eq!(decoded, ids);
    }

    #[test]
    fn gena_propertyset_roundtrip(
        props in prop::collection::btree_map(xml_ident(), xml_text(), 0..8),
    ) {
        let encoded = encode_propertyset(&props);
        let decoded = decode_propertyset(&encoded).unwrap();
        prop_assert_eq!(decoded, props);
    }

    #[test]
    fn source_xml_and_track_list_roundtrip(
        sources in prop::collection::vec(
            (xml_text(), xml_text(), xml_text(), any::<bool>()),
            0..6,
        ),
        tracks in prop::collection::vec(
            (1u32..10_000, xml_text(), xml_text()),
            0..6,
        ),
    ) {
        let src_objs: Vec<Source> = sources
            .into_iter()
            .map(|(name, source_type, system_name, visible)| Source {
                name,
                source_type: SourceType::parse(&source_type),
                system_name,
                visible,
            })
            .collect();
        let src_xml = encode_source_xml(&src_objs);
        let dec_src = decode_source_xml(&src_xml).unwrap();
        // Per Product:1 interop, empty SystemName defaults to Name during decode.
        let expected_src: Vec<Source> = src_objs
            .into_iter()
            .map(|mut s| {
                if s.system_name.is_empty() {
                    s.system_name = s.name.clone();
                }
                s
            })
            .collect();
        prop_assert_eq!(dec_src, expected_src);

        let track_objs: Vec<TrackEntry> = tracks
            .into_iter()
            .map(|(id, uri, metadata)| TrackEntry { id, uri, metadata })
            .collect();
        let track_xml = encode_track_list(&track_objs);
        let dec_tracks = decode_track_list(&track_xml).unwrap();
        prop_assert_eq!(dec_tracks, track_objs);
    }

    // -----------------------------------------------------------------------
    // 4. openhome-songcast: OHM / OHZ / URI never-panic fuzzing (raw + magic-prefixed)
    // -----------------------------------------------------------------------

    #[test]
    fn songcast_decoders_never_panic_on_arbitrary_bytes(
        raw in prop::collection::vec(any::<u8>(), 0..512),
        msg_type in any::<u8>(),
        len_be in any::<u16>(),
    ) {
        let _ = OhmMessage::decode(&raw);
        let _ = OhzMessage::decode(&raw);

        // Also test magic-prefixed packets to penetrate past header checks into every branch.
        let mut ohm_pkt = Vec::with_capacity(8 + raw.len());
        ohm_pkt.extend_from_slice(b"Ohm ");
        ohm_pkt.push(1);
        ohm_pkt.push(msg_type);
        ohm_pkt.extend_from_slice(&len_be.to_be_bytes());
        ohm_pkt.extend_from_slice(&raw);
        let _ = OhmMessage::decode(&ohm_pkt);

        let mut ohz_pkt = Vec::with_capacity(8 + raw.len());
        ohz_pkt.extend_from_slice(b"Ohz ");
        ohz_pkt.push(1);
        ohz_pkt.push(msg_type);
        ohz_pkt.extend_from_slice(&len_be.to_be_bytes());
        ohz_pkt.extend_from_slice(&raw);
        let _ = OhzMessage::decode(&ohz_pkt);
    }

    #[test]
    fn songcast_uri_never_panics_and_roundtrips(
        garbage in "\\PC{0,128}",
        uc in socket_v4_strategy(),
        mc_octet3 in any::<u8>(),
        mc_octet4 in any::<u8>(),
        mc_port in any::<u16>(),
        zone_name in "[A-Za-z0-9._-]{1,24}",
    ) {
        let _ = SongcastUri::parse(&garbage);

        let mc = SocketAddrV4::new(Ipv4Addr::new(239, 255, mc_octet3, mc_octet4), mc_port);
        let uris = [
            SongcastUri::Null,
            SongcastUri::Multicast(mc),
            SongcastUri::Unicast(uc),
            SongcastUri::Zone { endpoint: mc, zone: zone_name },
        ];
        for uri in uris {
            let s = uri.to_string();
            let parsed = SongcastUri::parse(&s).unwrap();
            prop_assert_eq!(parsed, uri);
        }
    }

    #[test]
    fn ohm_messages_roundtrip(
        frame in any::<u32>(),
        ts in any::<u32>(),
        latency in any::<u32>(),
        flags_bits in any::<u8>(),
        audio in prop::collection::vec(any::<u8>(), 0..128),
        uri in "\\PC{0,64}",
        metadata in "\\PC{0,64}",
        metatext in "\\PC{0,64}",
        slaves in prop::collection::vec(socket_v4_strategy(), 0..8),
        resend in prop::collection::vec(any::<u32>(), 0..16),
    ) {
        let audio_msg = OhmMessage::Audio(OhmAudio {
            flags: AudioFlags(flags_bits),
            samples: 16,
            frame,
            network_timestamp: ts,
            media_latency: latency,
            media_timestamp: ts.wrapping_add(100),
            sample_start: 0,
            samples_total: 0,
            sample_rate: 48_000,
            bit_rate: 1_536_000,
            volume_offset: 0,
            bit_depth: 16,
            channels: 2,
            codec: "PCM".into(),
            audio,
        });
        let encoded = audio_msg.encode().unwrap();
        prop_assert_eq!(OhmMessage::decode(&encoded).unwrap(), audio_msg);

        let track_msg = OhmMessage::Track(OhmTrack {
            sequence: frame,
            uri,
            metadata,
        });
        let enc_t = track_msg.encode().unwrap();
        prop_assert_eq!(OhmMessage::decode(&enc_t).unwrap(), track_msg);

        let meta_msg = OhmMessage::Metatext(OhmMetatext {
            sequence: frame,
            metatext,
        });
        let enc_m = meta_msg.encode().unwrap();
        prop_assert_eq!(OhmMessage::decode(&enc_m).unwrap(), meta_msg);

        let slave_msg = OhmMessage::Slave(slaves);
        let enc_s = slave_msg.encode().unwrap();
        prop_assert_eq!(OhmMessage::decode(&enc_s).unwrap(), slave_msg);

        let resend_msg = OhmMessage::Resend(resend);
        let enc_r = resend_msg.encode().unwrap();
        prop_assert_eq!(OhmMessage::decode(&enc_r).unwrap(), resend_msg);
    }

    #[test]
    fn ohz_messages_roundtrip(
        zone in "[A-Za-z0-9._-]{0,32}",
        uri in "[a-z0-9:/._-]{0,64}",
        preset in any::<u32>(),
        metadata in "\\PC{0,64}",
    ) {
        let msgs = [
            OhzMessage::ZoneQuery { zone: zone.clone() },
            OhzMessage::ZoneUri { zone, uri },
            OhzMessage::PresetQuery { preset },
            OhzMessage::PresetInfo { preset, metadata },
        ];
        for msg in msgs {
            let encoded = msg.encode().unwrap();
            prop_assert_eq!(OhzMessage::decode(&encoded).unwrap(), msg);
        }
    }

    // -----------------------------------------------------------------------
    // 5. Stateful ReorderBuffer & ClockSync property invariants
    // -----------------------------------------------------------------------

    #[test]
    fn reorder_buffer_invariants_under_arbitrary_frame_streams(
        max_backlog in 1usize..=32,
        base in prop_oneof![Just(0u32), Just(u32::MAX - 8), any::<u32>()],
        deltas in prop::collection::vec((-40i32..60i32, any::<bool>()), 1..120),
    ) {
        let mut buf = ReorderBuffer::new(max_backlog);
        for (delta, resent) in deltas {
            let frame_num = base.wrapping_add_signed(delta);
            let flags = AudioFlags(if resent { AudioFlags::RESENT } else { 0 });
            let pkt = OhmAudio {
                flags,
                samples: 16,
                frame: frame_num,
                network_timestamp: 0,
                media_latency: 0,
                media_timestamp: 0,
                sample_start: 0,
                samples_total: 0,
                sample_rate: 48_000,
                bit_rate: 0,
                volume_offset: 0,
                bit_depth: 16,
                channels: 2,
                codec: String::new(),
                audio: Vec::new(),
            };

            match buf.push(pkt) {
                PushOutcome::Ready(frames) => {
                    prop_assert!(!frames.is_empty());
                    for w in frames.windows(2) {
                        prop_assert_eq!(w[1].frame, w[0].frame.wrapping_add(1));
                    }
                }
                PushOutcome::GapDetected | PushOutcome::Duplicate | PushOutcome::Resynced(_) => {}
            }

            let missing = buf.missing_frames(MAX_RESEND_FRAMES_PER_REQ + 10);
            prop_assert!(missing.len() <= MAX_RESEND_FRAMES_PER_REQ);
        }
    }

    #[test]
    fn clock_sync_never_panics_or_produces_nan_across_u32_wrap(
        rate in prop_oneof![Just(44_100u32), Just(48_000u32), Just(96_000u32), Just(192_000u32)],
        start_ts in prop_oneof![Just(u32::MAX - 500_000), any::<u32>()],
        steps in prop::collection::vec((0u32..500_000, 0u32..20_000_000, 0u64..20_000), 1..60),
    ) {
        let mut clock = ClockSync::new(rate);
        let mut ts = start_ts;
        let mut now = Instant::now();
        for (dt_ticks, latency, dt_us) in steps {
            ts = ts.wrapping_add(dt_ticks);
            now += Duration::from_micros(dt_us);
            let _playout = clock.observe(ts, latency, now);
            prop_assert!(clock.skew_ppm().is_finite());
        }
    }
}

// Targeted adversarial XML / binary edge cases (deterministic, zero-flake).
#[test]
fn adversarial_xml_and_binary_inputs_fail_cleanly() {
    // Deeply nested XML (500 levels) must not stack-overflow or panic.
    let deep = format!("{}{}", "<a>".repeat(500), "</a>".repeat(500));
    let _ = decode_soap_args(&deep);
    let _ = decode_device_description(&deep);
    let _ = decode_source_xml(&deep);
    let _ = decode_track_list(&deep);
    let _ = decode_propertyset(&deep);

    // Unclosed CDATA & invalid entity references.
    for nasty in [
        "<s:Envelope><![CDATA[unclosed",
        "<SourceList><Source><Name>&bogus;&#xFFFFFFFF;</Name></Source></SourceList>",
        "<e:propertyset><e:property><A>\0\x01\x02</A></e:property></e:propertyset>",
        "M-SEARCH * HTTP/1.1\r\n\r\n\0\0",
    ] {
        let _ = decode_soap_args(nasty);
        let _ = decode_source_xml(nasty);
        let _ = decode_propertyset(nasty);
        let _ = decode_ssdp_packet(nasty.as_bytes());
    }

    // Empty property map roundtrip.
    let empty: BTreeMap<String, String> = BTreeMap::new();
    assert_eq!(
        decode_propertyset(&encode_propertyset(&empty)).unwrap(),
        empty
    );
}
