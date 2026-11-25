//! End-to-end round-trip integration tests exercising `openhome-client` against
//! `openhome-server` over real loopback TCP (HTTP/1.1 SOAP + GENA) and UDP (SSDP).

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use openhome_client::{
    ClientError, DiscoveryOptions, EventListener, OpenHomeDevice, Topology, discover,
};
use openhome_core::id_array::decode_id_array;
use openhome_core::urn::{
    DEVICE_SOURCE_1, SERVICE_INFO_1, SERVICE_PLAYLIST_1, SERVICE_PRODUCT_1, SERVICE_RECEIVER_1,
    SERVICE_SENDER_1, SERVICE_TIME_1, SERVICE_VOLUME_1,
};
use openhome_core::{
    Fault, ReceiverState, SenderStatus, SourceType, TrackDetails, TransportState,
    VolumeCharacteristics,
};
use openhome_server::{MemoryRenderer, OpenHomeServer, SsdpConfig};

async fn start_test_server(room: &str, name: &str) -> (MemoryRenderer, OpenHomeServer) {
    let renderer = MemoryRenderer::new(room, name);
    let server = renderer
        .into_server_builder()
        .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .ssdp(SsdpConfig::disabled())
        .start()
        .await
        .expect("start server");
    (renderer, server)
}

fn assert_fault(res: Result<impl std::fmt::Debug, ClientError>, expected_code: u16) {
    match res {
        Err(ClientError::Fault(f)) => {
            assert_eq!(f.code, expected_code, "unexpected fault: {f}");
        }
        other => panic!("expected SOAP fault {expected_code}, got {other:?}"),
    }
}

#[tokio::test]
async fn description_xml_and_all_scpds_roundtrip() {
    let (_renderer, server) = start_test_server("Living Room", "HiFi").await;
    let device = OpenHomeDevice::connect(server.description_url())
        .await
        .expect("connect to device");

    assert_eq!(device.description().device_type, DEVICE_SOURCE_1);
    assert_eq!(device.udn(), server.udn());
    assert_eq!(device.friendly_name(), "Living Room (HiFi)");
    assert_eq!(device.description().services.len(), 7);

    for urn in [
        SERVICE_PRODUCT_1,
        SERVICE_PLAYLIST_1,
        SERVICE_INFO_1,
        SERVICE_TIME_1,
        SERVICE_VOLUME_1,
        SERVICE_RECEIVER_1,
        SERVICE_SENDER_1,
    ] {
        let scpd = device.scpd_xml(urn).await.expect("fetch SCPD");
        assert!(scpd.contains("<scpd "), "invalid SCPD for {urn}");
        assert!(scpd.contains("<actionList>"), "missing actionList in {urn}");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn product_service_full_roundtrip() {
    let (_renderer, server) = start_test_server("Studio", "Reference DAC").await;
    let device = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();
    let product = device.product().unwrap();

    let mfg = product.manufacturer().await.unwrap();
    assert_eq!(mfg.name, "openhome-rs");

    let model = product.model().await.unwrap();
    assert_eq!(model.name, "OpenHome RS");

    let prod = product.product().await.unwrap();
    assert_eq!(prod.room, "Studio");
    assert_eq!(prod.name, "Reference DAC");

    let attrs = product.attributes().await.unwrap();
    assert!(attrs.info && attrs.time && attrs.volume && attrs.receiver && attrs.sender);
    assert!(!attrs.credentials);

    assert!(!product.standby().await.unwrap());
    product.set_standby(true).await.unwrap();
    assert!(product.standby().await.unwrap());
    product.set_standby(false).await.unwrap();
    assert!(!product.standby().await.unwrap());

    assert_eq!(product.source_count().await.unwrap(), 2);
    let sources = product.source_xml().await.unwrap();
    assert_eq!(sources.len(), 2);
    assert_eq!(sources[0].source_type, SourceType::Playlist);
    assert_eq!(sources[1].source_type, SourceType::Receiver);
    // Kazoo compatibility: Receiver must be visible.
    assert!(sources[1].visible);

    let src1 = product.source(1).await.unwrap();
    assert_eq!(src1.name, "Songcast");
    assert_eq!(src1.system_name, "Receiver");

    assert_eq!(product.source_index().await.unwrap(), 0);
    product.set_source_index(1).await.unwrap();
    assert_eq!(product.source_index().await.unwrap(), 1);

    product.set_source_index_by_name("Playlist").await.unwrap();
    assert_eq!(product.source_index().await.unwrap(), 0);

    product.set_source_by_system_name("Receiver").await.unwrap();
    assert_eq!(product.source_index().await.unwrap(), 1);

    assert_eq!(product.source_xml_change_count().await.unwrap(), 0);

    // Out-of-range / unknown source faults (801).
    assert_fault(product.set_source_index(99).await, Fault::SOURCE_NOT_FOUND);
    assert_fault(
        product.set_source_index_by_name("Nonexistent").await,
        Fault::SOURCE_NOT_FOUND,
    );
    assert_fault(product.source(99).await, Fault::SOURCE_NOT_FOUND);

    server.shutdown().await;
}

#[tokio::test]
async fn playlist_service_full_roundtrip_and_interop_quirks() {
    let (renderer, server) = start_test_server("Living Room", "Stream").await;
    renderer.set_tracks_max(3);
    renderer.set_track_details(TrackDetails {
        duration: 240,
        bit_rate: 1411,
        bit_depth: 16,
        sample_rate: 44100,
        lossless: true,
        codec_name: "FLAC".into(),
    });

    let device = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();
    let product = device.product().unwrap();
    let playlist = device.playlist().unwrap();
    let time = device.time().unwrap();

    assert_eq!(playlist.tracks_max().await.unwrap(), 3);
    assert!(
        playlist
            .protocol_info()
            .await
            .unwrap()
            .contains("audio/flac")
    );

    let (tok0, ids0) = playlist.id_array().await.unwrap();
    assert!(ids0.is_empty());
    assert!(!playlist.id_array_changed(tok0).await.unwrap());

    let didl1 = "<DIDL-Lite><item><dc:title>Track 1 &amp; Intro</dc:title></item></DIDL-Lite>";
    let id1 = playlist
        .insert(0, "http://music.local/1.flac?a=1&b=2", didl1)
        .await
        .unwrap();
    let id2 = playlist
        .insert(id1, "http://music.local/2.flac", "<DIDL-Lite>2</DIDL-Lite>")
        .await
        .unwrap();
    let id3 = playlist
        .insert(id2, "http://music.local/3.flac", "<DIDL-Lite>3</DIDL-Lite>")
        .await
        .unwrap();

    // Playlist full (801)
    assert_fault(
        playlist.insert(id3, "http://music.local/4.flac", "").await,
        Fault::PLAYLIST_FULL,
    );

    // Unknown AfterId (800)
    playlist.delete_id(id3).await.unwrap();
    assert_fault(
        playlist.insert(9999, "http://music.local/x.flac", "").await,
        Fault::INVALID_ID,
    );

    assert!(playlist.id_array_changed(tok0).await.unwrap());
    let (tok1, ids1) = playlist.id_array().await.unwrap();
    assert_eq!(ids1, vec![id1, id2]);
    assert!(!playlist.id_array_changed(tok1).await.unwrap());

    // Read & ReadList (including Lumin `-1` and unknown ID skip quirk)
    let entry1 = playlist.read(id1).await.unwrap();
    assert_eq!(entry1.uri, "http://music.local/1.flac?a=1&b=2");
    assert_eq!(entry1.metadata, didl1);
    assert_fault(playlist.read(9999).await, Fault::INVALID_ID);

    let list = playlist
        .read_list_raw(&format!("{id1} -1 9999 {id2}"))
        .await
        .unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].id, id1);
    assert_eq!(list[1].id, id2);

    // Playback transport & seeking
    playlist.play().await.unwrap();
    assert_eq!(
        playlist.transport_state().await.unwrap(),
        TransportState::Playing
    );
    assert_eq!(playlist.id().await.unwrap(), id1);

    playlist.pause().await.unwrap();
    assert_eq!(
        playlist.transport_state().await.unwrap(),
        TransportState::Paused
    );

    playlist.seek_second_absolute(45).await.unwrap();
    assert_eq!(time.time().await.unwrap().seconds, 45);
    playlist.seek_second_relative(-15).await.unwrap();
    assert_eq!(time.time().await.unwrap().seconds, 30);

    playlist.next().await.unwrap();
    assert_eq!(playlist.id().await.unwrap(), id2);
    playlist.previous().await.unwrap();
    assert_eq!(playlist.id().await.unwrap(), id1);

    playlist.seek_id(id2).await.unwrap();
    assert_eq!(playlist.id().await.unwrap(), id2);
    playlist.seek_index(0).await.unwrap();
    assert_eq!(playlist.id().await.unwrap(), id1);
    assert_fault(playlist.seek_id(9999).await, Fault::INVALID_ID);
    assert_fault(playlist.seek_index(99).await, Fault::INVALID_INDEX);

    playlist.set_repeat(true).await.unwrap();
    assert!(playlist.repeat().await.unwrap());
    playlist.set_shuffle(true).await.unwrap();
    assert!(playlist.shuffle().await.unwrap());

    // Inactive source rule: switch to Receiver -> transport mutations return 409 Conflict,
    // except Play which auto-selects Playlist.
    product.set_source_by_system_name("Receiver").await.unwrap();
    assert_fault(playlist.pause().await, Fault::CONFLICT);
    assert_fault(playlist.next().await, Fault::CONFLICT);
    assert_fault(playlist.seek_id(id1).await, Fault::CONFLICT);

    playlist.play().await.unwrap();
    assert_eq!(product.source_index().await.unwrap(), 0);
    assert_eq!(
        playlist.transport_state().await.unwrap(),
        TransportState::Playing
    );

    playlist.delete_all().await.unwrap();
    let (_, ids_empty) = playlist.id_array().await.unwrap();
    assert!(ids_empty.is_empty());
    assert_eq!(
        playlist.transport_state().await.unwrap(),
        TransportState::Stopped
    );

    server.shutdown().await;
}

#[tokio::test]
async fn info_and_time_services_roundtrip() {
    let (renderer, server) = start_test_server("Den", "Player").await;
    let device = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();
    let playlist = device.playlist().unwrap();
    let info = device.info().unwrap();
    let time = device.time().unwrap();

    let id = playlist
        .insert(
            0,
            "http://music.local/jazz.flac",
            "<DIDL-Lite>Jazz</DIDL-Lite>",
        )
        .await
        .unwrap();
    playlist.seek_id(id).await.unwrap();

    renderer.set_track_details(TrackDetails {
        duration: 312,
        bit_rate: 2822,
        bit_depth: 24,
        sample_rate: 96_000,
        lossless: true,
        codec_name: "FLAC".into(),
    });
    renderer.set_metatext("<DIDL-Lite>Now Playing Live</DIDL-Lite>");
    renderer.set_time_seconds(87);

    let counters = info.counters().await.unwrap();
    assert_eq!(counters.track_count, 1);
    assert_eq!(counters.details_count, 1);
    assert_eq!(counters.metatext_count, 1);

    let (uri, meta) = info.track().await.unwrap();
    assert_eq!(uri, "http://music.local/jazz.flac");
    assert_eq!(meta, "<DIDL-Lite>Jazz</DIDL-Lite>");

    let details = info.details().await.unwrap();
    assert_eq!(details.duration, 312);
    assert_eq!(details.bit_depth, 24);
    assert_eq!(details.sample_rate, 96_000);
    assert!(details.lossless);
    assert_eq!(details.codec_name, "FLAC");

    assert_eq!(
        info.metatext().await.unwrap(),
        "<DIDL-Lite>Now Playing Live</DIDL-Lite>"
    );

    let t = time.time().await.unwrap();
    assert_eq!(t.track_count, 1);
    assert_eq!(t.duration, 312);
    assert_eq!(t.seconds, 87);

    server.shutdown().await;
}

#[tokio::test]
async fn volume_service_roundtrip_and_balance_fade_quirks() {
    let (renderer, server) = start_test_server("Lounge", "Amp").await;
    let device = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();
    let vol = device.volume().unwrap();

    let chars = vol.characteristics().await.unwrap();
    assert_eq!(chars.volume_max, 100);
    assert_eq!(chars.balance_max, 0);
    assert_eq!(chars.fade_max, 0);

    vol.set_volume(65).await.unwrap();
    assert_eq!(vol.volume().await.unwrap(), 65);
    vol.volume_inc().await.unwrap();
    assert_eq!(vol.volume().await.unwrap(), 66);
    vol.volume_dec().await.unwrap();
    assert_eq!(vol.volume().await.unwrap(), 65);

    assert_fault(vol.set_volume(101).await, Fault::INVALID_ARGS);

    vol.set_mute(true).await.unwrap();
    assert!(vol.mute().await.unwrap());
    vol.set_mute(false).await.unwrap();
    assert!(!vol.mute().await.unwrap());

    // Balance/Fade unsupported (max == 0): accept 0, reject non-zero with SOAP 402.
    vol.set_balance(0).await.unwrap();
    assert_fault(vol.set_balance(1).await, Fault::INVALID_ARGS);
    vol.set_fade(0).await.unwrap();
    assert_fault(vol.set_fade(-1).await, Fault::INVALID_ARGS);
    vol.balance_inc().await.unwrap();
    vol.fade_dec().await.unwrap();

    // Now enable balance/fade range and verify inc/dec + bounds.
    renderer.set_volume_characteristics(VolumeCharacteristics {
        balance_max: 10,
        fade_max: 10,
        ..VolumeCharacteristics::default()
    });
    vol.set_balance(4).await.unwrap();
    vol.balance_inc().await.unwrap();
    assert_eq!(vol.balance().await.unwrap(), 5);
    vol.balance_dec().await.unwrap();
    assert_eq!(vol.balance().await.unwrap(), 4);
    assert_fault(vol.set_balance(11).await, Fault::INVALID_ARGS);

    vol.set_fade(-3).await.unwrap();
    vol.fade_dec().await.unwrap();
    assert_eq!(vol.fade().await.unwrap(), -4);

    let st = vol.state().await.unwrap();
    assert_eq!(st.volume, 65);
    assert_eq!(st.balance, 4);
    assert_eq!(st.fade, -4);
    assert_eq!(st.volume_limit, 100);

    server.shutdown().await;
}

#[tokio::test]
async fn receiver_and_sender_roundtrip_and_bubbleupnp_quirk() {
    let (renderer, server) = start_test_server("Kitchen", "Speaker").await;
    renderer.set_sender_info(
        "http://192.168.1.50/",
        "<DIDL-Lite>Sender Meta</DIDL-Lite>",
        true,
        SenderStatus::Enabled,
        "ohm",
    );

    let device = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();
    let product = device.product().unwrap();
    let receiver = device.receiver().unwrap();
    let sender = device.sender().unwrap();

    assert!(receiver.protocol_info().await.unwrap().contains("ohm:"));
    assert_eq!(
        receiver.transport_state().await.unwrap(),
        ReceiverState::Stopped
    );

    receiver
        .set_sender(
            "ohz://239.255.255.250:51972/abc",
            "<DIDL-Lite>Zone</DIDL-Lite>",
        )
        .await
        .unwrap();
    let (uri, meta) = receiver.sender().await.unwrap();
    assert_eq!(uri, "ohz://239.255.255.250:51972/abc");
    assert_eq!(meta, "<DIDL-Lite>Zone</DIDL-Lite>");

    // Receiver.Play auto-selects Receiver source (index 1).
    receiver.play().await.unwrap();
    assert_eq!(product.source_index().await.unwrap(), 1);
    assert_eq!(
        receiver.transport_state().await.unwrap(),
        ReceiverState::Playing
    );

    // BubbleUPnP compatibility: Receiver.Stop must NOT reset Product.SourceIndex.
    receiver.stop().await.unwrap();
    assert_eq!(
        receiver.transport_state().await.unwrap(),
        ReceiverState::Stopped
    );
    assert_eq!(product.source_index().await.unwrap(), 1);

    assert_eq!(
        sender.presentation_url().await.unwrap(),
        "http://192.168.1.50/"
    );
    assert_eq!(
        sender.metadata().await.unwrap(),
        "<DIDL-Lite>Sender Meta</DIDL-Lite>"
    );
    assert!(sender.audio().await.unwrap());
    assert_eq!(sender.status().await.unwrap(), SenderStatus::Enabled);
    assert_eq!(sender.attributes().await.unwrap(), "ohm");

    server.shutdown().await;
}

#[tokio::test]
async fn gena_eventing_initial_delta_renewal_and_unsubscribe() {
    let (renderer, server) = start_test_server("Office", "Desk").await;
    let device = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();
    let playlist = device.playlist().unwrap();
    let volume = device.volume().unwrap();

    let listener = EventListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();

    let mut pl_sub = listener
        .subscribe_service(&device, SERVICE_PLAYLIST_1, 300)
        .await
        .unwrap();
    let mut vol_sub = listener
        .subscribe_service(&device, SERVICE_VOLUME_1, 300)
        .await
        .unwrap();
    let mut info_sub = listener
        .subscribe_service(&device, SERVICE_INFO_1, 300)
        .await
        .unwrap();

    assert_eq!(server.events().active_subscription_count(), 3);

    // 1. Initial SEQ=0 events contain full state snapshots.
    let pl_init = tokio::time::timeout(Duration::from_secs(2), pl_sub.recv())
        .await
        .expect("playlist SEQ=0 timeout")
        .expect("playlist SEQ=0 event");
    assert_eq!(pl_init.seq, 0);
    assert_eq!(pl_init.sid, pl_sub.sid());
    assert_eq!(
        pl_init.properties.get("TransportState").map(String::as_str),
        Some("Stopped")
    );
    assert!(pl_init.properties.contains_key("IdArray"));
    assert!(pl_init.properties.contains_key("TracksMax"));

    let vol_init = tokio::time::timeout(Duration::from_secs(2), vol_sub.recv())
        .await
        .expect("volume SEQ=0 timeout")
        .expect("volume SEQ=0 event");
    assert_eq!(vol_init.seq, 0);
    assert_eq!(
        vol_init.properties.get("Volume").map(String::as_str),
        Some("50")
    );
    assert_eq!(vol_init.properties.len(), 11);

    let info_init = tokio::time::timeout(Duration::from_secs(2), info_sub.recv())
        .await
        .expect("info SEQ=0 timeout")
        .expect("info SEQ=0 event");
    assert_eq!(info_init.seq, 0);

    // 2. SOAP mutation emits delta-only SEQ=1 notification.
    let id1 = playlist
        .insert(0, "http://music.local/song.flac", "<DIDL-Lite/>")
        .await
        .unwrap();
    let pl_delta = tokio::time::timeout(Duration::from_secs(2), pl_sub.recv())
        .await
        .expect("playlist SEQ=1 timeout")
        .expect("playlist SEQ=1 event");
    assert_eq!(pl_delta.seq, 1);
    assert_eq!(pl_delta.properties.len(), 1);
    let ids = decode_id_array(pl_delta.properties.get("IdArray").unwrap()).unwrap();
    assert_eq!(ids, vec![id1]);

    volume.set_volume(73).await.unwrap();
    let vol_delta = tokio::time::timeout(Duration::from_secs(2), vol_sub.recv())
        .await
        .expect("volume SEQ=1 timeout")
        .expect("volume SEQ=1 event");
    assert_eq!(vol_delta.seq, 1);
    assert_eq!(vol_delta.properties.len(), 1);
    assert_eq!(
        vol_delta.properties.get("Volume").map(String::as_str),
        Some("73")
    );

    // 3. Out-of-band server backend change + notify_info() delivers only changed Info properties.
    renderer.set_metatext("Live Stream Title");
    server.notify_info();
    let info_delta = tokio::time::timeout(Duration::from_secs(2), info_sub.recv())
        .await
        .expect("info SEQ=1 timeout")
        .expect("info SEQ=1 event");
    assert_eq!(info_delta.seq, 1);
    assert_eq!(
        info_delta.properties.get("Metatext").map(String::as_str),
        Some("Live Stream Title")
    );
    assert_eq!(
        info_delta
            .properties
            .get("MetatextCount")
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(info_delta.properties.len(), 2);

    // 4. Manual renewal & clean unsubscribe.
    let renewed = vol_sub.renew(600).await.unwrap();
    assert_eq!(renewed, 600);

    vol_sub.unsubscribe().await.unwrap();
    pl_sub.unsubscribe().await.unwrap();
    info_sub.unsubscribe().await.unwrap();
    assert_eq!(server.events().active_subscription_count(), 0);

    listener.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn ssdp_discovery_and_topology_roundtrip() {
    // Bind server 1 with loopback SSDP on an ephemeral UDP port for deterministic CI testing.
    let r1 = MemoryRenderer::new("Living Room", "Main");
    let s1 = r1
        .into_server_builder()
        .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .ssdp(SsdpConfig {
            enabled: true,
            multicast_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            interface: Ipv4Addr::LOCALHOST,
            ..SsdpConfig::default()
        })
        .start()
        .await
        .unwrap();

    let ssdp_port = s1.ssdp_addr().expect("SSDP enabled").port();

    let found = discover(DiscoveryOptions {
        st: DEVICE_SOURCE_1.into(),
        mx_secs: 1,
        timeout: Duration::from_millis(300),
        target_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, ssdp_port),
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        send_count: 1,
        ..DiscoveryOptions::default()
    })
    .await
    .unwrap();

    assert_eq!(found.len(), 1);
    assert_eq!(found[0].udn, s1.udn());
    assert_eq!(found[0].friendly_name, "Living Room (Main)");

    // Build a multi-room Topology snapshot with a second renderer.
    let (_r2, s2) = start_test_server("Kitchen", "Mini").await;
    let d1 = found
        .into_iter()
        .next()
        .unwrap()
        .into_device(Default::default());
    let d2 = OpenHomeDevice::connect(s2.description_url()).await.unwrap();

    let topo = Topology::from_devices(&[d1, d2]).await.unwrap();
    assert_eq!(topo.room_count(), 2);
    let living = topo.room("Living Room").expect("Living Room present");
    assert_eq!(living.devices.len(), 1);
    assert_eq!(
        living.devices[0].active_source().map(|s| s.name.as_str()),
        Some("Playlist")
    );
    assert_eq!(topo.senders().count(), 2);
    assert_eq!(topo.receivers().count(), 2);

    s1.shutdown().await;
    s2.shutdown().await;
}

#[tokio::test]
async fn regression_next_unselected_readlist_dedup_insert_limits_and_ssrf_guard() {
    let (_renderer, server) = start_test_server("Lab", "Test").await;
    let device = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();
    let playlist = device.playlist().unwrap();

    let id1 = playlist
        .insert(0, "http://music.local/a.flac", "<DIDL-Lite>A</DIDL-Lite>")
        .await
        .unwrap();
    let id2 = playlist
        .insert(id1, "http://music.local/b.flac", "<DIDL-Lite>B</DIDL-Lite>")
        .await
        .unwrap();

    // Regression: Next with no current selection selects the first track, not the second.
    assert_eq!(playlist.id().await.unwrap(), 0);
    playlist.next().await.unwrap();
    assert_eq!(playlist.id().await.unwrap(), id1);
    playlist.next().await.unwrap();
    assert_eq!(playlist.id().await.unwrap(), id2);

    // Regression: duplicate IDs in ReadList are deduplicated without amplification.
    let list = playlist
        .read_list_raw(&format!("{id1} {id1} {id1} {id2} {id1}"))
        .await
        .unwrap();
    assert_eq!(list.len(), 2);
    assert_eq!(list[0].id, id1);
    assert_eq!(list[1].id, id2);

    // Regression: oversized Uri is rejected with SOAP 402.
    let huge_uri = format!("http://music.local/{}", "a".repeat(16 * 1024));
    assert_fault(playlist.insert(0, &huge_uri, "").await, Fault::INVALID_ARGS);

    // Regression: GENA SUBSCRIBE with non-peer CALLBACK host (SSRF attempt) is rejected.
    let evt_url = device.event_sub_url(SERVICE_PLAYLIST_1).unwrap();
    let ssrf = device
        .http()
        .gena_subscribe(&evt_url, "http://169.254.169.254:80/latest/meta-data", 300)
        .await;
    assert!(matches!(ssrf, Err(ClientError::SubscriptionFailed(_))));
    assert_eq!(server.events().active_subscription_count(), 0);

    server.shutdown().await;
}
