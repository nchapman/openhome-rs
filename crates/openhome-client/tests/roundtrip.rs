//! End-to-end round-trip integration tests exercising `openhome-client` against
//! `openhome-server` over real loopback TCP (HTTP/1.1 SOAP + GENA) and UDP (SSDP).

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use openhome_client::{
    ClientError, DiscoveryOptions, EventListener, HttpClient, OpenHomeDevice, ProductClient,
    Topology, discover,
};
use openhome_core::id_array::decode_id_array;
use openhome_core::soap::decode_soap_fault;
use openhome_core::ssdp::{SsdpMessage, decode_ssdp_packet, encode_msearch};
use openhome_core::urn::{
    DEVICE_SOURCE_1, SERVICE_INFO_1, SERVICE_PLAYLIST_1, SERVICE_PRODUCT_1, SERVICE_PRODUCT_2,
    SERVICE_RECEIVER_1, SERVICE_SENDER_1, SERVICE_TIME_1, SERVICE_VOLUME_1, SSDP_ALL,
    UPNP_ROOT_DEVICE,
};
use openhome_core::{
    DeviceAttributes, DeviceDescription, Fault, ProductInfo, ReceiverState, SenderStatus,
    ServiceDescription, Source, SourceType, TrackDetails, TransportState, VolumeCharacteristics,
};
use openhome_server::{MemoryRenderer, OpenHomeServer, ProductService, SsdpConfig};
use tokio::net::{TcpStream, UdpSocket};

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

async fn raw_http(
    addr: SocketAddr,
    req: Request<Full<Bytes>>,
) -> (StatusCode, hyper::HeaderMap, Bytes) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, headers, body)
}

#[tokio::test]
async fn raw_http_status_codes_and_gena_protocol_errors() {
    let (_renderer, server) = start_test_server("Lab", "Http").await;
    let addr = server.local_addr();
    let host = addr.to_string();

    // HEAD /description.xml -> 200 OK, non-zero Content-Length, empty body.
    let (status, headers, body) = raw_http(
        addr,
        Request::builder()
            .method("HEAD")
            .uri("/description.xml")
            .header("HOST", &host)
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    let cl: usize = headers
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(cl > 100);

    // 404 on unknown paths and unknown SCPD.
    let (status, _, _) = raw_http(
        addr,
        Request::builder()
            .method("GET")
            .uri("/does-not-exist")
            .header("HOST", &host)
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _, _) = raw_http(
        addr,
        Request::builder()
            .method("GET")
            .uri("/scpd/Bogus.xml")
            .header("HOST", &host)
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // 405 on wrong HTTP methods.
    let (status, _, _) = raw_http(
        addr,
        Request::builder()
            .method("GET")
            .uri("/ctl/Product")
            .header("HOST", &host)
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);

    let (status, _, _) = raw_http(
        addr,
        Request::builder()
            .method("POST")
            .uri("/evt/Product")
            .header("HOST", &host)
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);

    // SUBSCRIBE without NT -> 412; with SID + CALLBACK -> 400; UNSUBSCRIBE unknown SID -> 412.
    let (status, _, _) = raw_http(
        addr,
        Request::builder()
            .method("SUBSCRIBE")
            .uri("/evt/Product")
            .header("HOST", &host)
            .header("CALLBACK", "<http://127.0.0.1:9999/cb>")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED);

    let (status, _, _) = raw_http(
        addr,
        Request::builder()
            .method("SUBSCRIBE")
            .uri("/evt/Product")
            .header("HOST", &host)
            .header("SID", "uuid:abc")
            .header("CALLBACK", "<http://127.0.0.1:9999/cb>")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _, _) = raw_http(
        addr,
        Request::builder()
            .method("UNSUBSCRIBE")
            .uri("/evt/Product")
            .header("HOST", &host)
            .header("SID", "uuid:does-not-exist")
            .body(Full::new(Bytes::new()))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED);

    // Malformed SOAP body returns HTTP 500 with UPnP SOAP Fault 402.
    let (status, _, body) = raw_http(
        addr,
        Request::builder()
            .method("POST")
            .uri("/ctl/Playlist")
            .header("HOST", &host)
            .body(Full::new(Bytes::from_static(b"<invalid/>")))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let fault = decode_soap_fault(std::str::from_utf8(&body).unwrap())
        .unwrap()
        .expect("SOAP fault payload");
    assert_eq!(fault.code, Fault::INVALID_ARGS);

    server.shutdown().await;
}

struct MinimalProduct {
    room: String,
    name: String,
}

impl ProductService for MinimalProduct {
    fn product_info(&self) -> ProductInfo {
        ProductInfo {
            product_room: self.room.clone(),
            product_name: self.name.clone(),
            ..ProductInfo::default()
        }
    }

    fn attributes(&self) -> DeviceAttributes {
        DeviceAttributes::default()
    }

    fn sources(&self) -> Vec<Source> {
        vec![Source {
            name: "Aux".into(),
            source_type: SourceType::Analog,
            visible: true,
            system_name: "Aux".into(),
        }]
    }

    fn source_index(&self) -> u32 {
        0
    }

    fn set_source_index(&self, index: u32) -> openhome_server::ActionResult<()> {
        if index == 0 {
            Ok(())
        } else {
            Err(Fault::source_not_found())
        }
    }

    fn standby(&self) -> bool {
        true
    }

    fn set_standby(&self, _standby: bool) -> openhome_server::ActionResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn partial_service_server_and_missing_service_errors() {
    let product = Arc::new(MinimalProduct {
        room: "Bedroom".into(),
        name: "Preamp".into(),
    });
    let server = OpenHomeServer::builder(product)
        .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .ssdp(SsdpConfig::disabled())
        .start()
        .await
        .unwrap();

    let device = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();
    assert_eq!(device.description().services.len(), 1);
    assert!(device.is_openhome_device());
    assert!(device.product().is_ok());
    assert!(matches!(
        device.playlist(),
        Err(ClientError::MissingService(SERVICE_PLAYLIST_1))
    ));
    assert!(matches!(
        device.volume(),
        Err(ClientError::MissingService(SERVICE_VOLUME_1))
    ));

    // Unregistered services return 404 on /ctl/, /evt/, /scpd/.
    let addr = server.local_addr();
    let host = addr.to_string();
    for (method, path) in [
        ("POST", "/ctl/Playlist"),
        ("SUBSCRIBE", "/evt/Playlist"),
        ("GET", "/scpd/Playlist.xml"),
    ] {
        let (status, _, _) = raw_http(
            addr,
            Request::builder()
                .method(method)
                .uri(path)
                .header("HOST", &host)
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn ssdp_search_matrix_all_rootdevice_service_and_invalid_man() {
    let renderer = MemoryRenderer::new("Hall", "Streamer");
    let server = renderer
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

    let ssdp_addr = server.ssdp_addr().unwrap();
    let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    async fn query_until(
        sock: &UdpSocket,
        dest: SocketAddr,
        packet: &str,
        expected_min: usize,
    ) -> Vec<openhome_core::ssdp::SsdpResponse> {
        sock.send_to(packet.as_bytes(), dest).await.unwrap();
        let mut out = Vec::new();
        let mut buf = [0u8; 4096];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while out.len() < expected_min {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, sock.recv_from(&mut buf)).await {
                Ok(Ok((len, _))) => {
                    if let Ok(SsdpMessage::Response(r)) = decode_ssdp_packet(&buf[..len]) {
                        out.push(r);
                    }
                }
                _ => break,
            }
        }
        out
    }

    // ssdp:all -> 10 targets (rootdevice + udn + Source:1 + 7 services).
    let all = query_until(
        &sock,
        ssdp_addr,
        &encode_msearch(&ssdp_addr.to_string(), SSDP_ALL, 1),
        10,
    )
    .await;
    let sts: std::collections::HashSet<&str> = all.iter().map(|r| r.st.as_str()).collect();
    for expected in [
        UPNP_ROOT_DEVICE,
        server.udn(),
        DEVICE_SOURCE_1,
        SERVICE_PRODUCT_1,
        SERVICE_PLAYLIST_1,
        SERVICE_INFO_1,
        SERVICE_TIME_1,
        SERVICE_VOLUME_1,
        SERVICE_RECEIVER_1,
        SERVICE_SENDER_1,
    ] {
        assert!(sts.contains(expected), "missing SSDP target {expected}");
    }

    // upnp:rootdevice -> response with USN "<udn>::upnp:rootdevice".
    let root = query_until(
        &sock,
        ssdp_addr,
        &encode_msearch(&ssdp_addr.to_string(), UPNP_ROOT_DEVICE, 1),
        1,
    )
    .await;
    assert_eq!(root.len(), 1);
    assert_eq!(root[0].st, UPNP_ROOT_DEVICE);
    assert_eq!(root[0].usn, format!("{}::{UPNP_ROOT_DEVICE}", server.udn()));
    assert_eq!(root[0].max_age, Some(1800));

    // Standard RFC 2774 / UPnP DA 1.1 MAN header with `; ns=01` parameter must be accepted.
    let ns_man = format!(
        "M-SEARCH * HTTP/1.1\r\nHOST: {ssdp_addr}\r\nMAN: \"ssdp:discover\"; ns=01\r\nMX: 1\r\nST: {SERVICE_PLAYLIST_1}\r\n\r\n"
    );
    let pl = query_until(&sock, ssdp_addr, &ns_man, 1).await;
    assert_eq!(pl.len(), 1);
    assert_eq!(pl[0].st, SERVICE_PLAYLIST_1);

    // M-SEARCH without valid MAN: "ssdp:discover" is ignored per UPnP DA 1.1.
    let bad_man =
        format!("M-SEARCH * HTTP/1.1\r\nHOST: {ssdp_addr}\r\nMX: 1\r\nST: {SSDP_ALL}\r\n\r\n");
    sock.send_to(bad_man.as_bytes(), ssdp_addr).await.unwrap();
    let mut buf = [0u8; 4096];
    assert!(
        tokio::time::timeout(Duration::from_millis(200), sock.recv_from(&mut buf))
            .await
            .is_err(),
        "M-SEARCH without MAN should receive no replies"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn gena_auto_renewal_keeps_short_lived_subscription_alive() {
    let (_renderer, server) = start_test_server("Den", "RenewTest").await;
    let device = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();
    let volume = device.volume().unwrap();

    let listener = EventListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();

    // 2-second timeout -> auto_renew_loop renews every 1s, leaving >1s headroom
    // before the initial TTL elapses.
    let subscribed_at = tokio::time::Instant::now();
    let mut sub = listener
        .subscribe_service(&device, SERVICE_VOLUME_1, 2)
        .await
        .unwrap();
    let init = tokio::time::timeout(Duration::from_secs(3), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(init.seq, 0);

    // Wait just past the initial 2s TTL so the subscription can only still be active
    // if the background renewal loop succeeded.
    tokio::time::sleep_until(subscribed_at + Duration::from_millis(2200)).await;
    assert_eq!(server.events().active_subscription_count(), 1);

    volume.set_volume(88).await.unwrap();
    let delta = tokio::time::timeout(Duration::from_secs(3), sub.recv())
        .await
        .expect("subscription still alive after auto-renewal")
        .unwrap();
    assert_eq!(delta.seq, 1);
    assert_eq!(
        delta.properties.get("Volume").map(String::as_str),
        Some("88")
    );

    sub.unsubscribe().await.unwrap();
    listener.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn topology_matrix_url_base_and_product_v2_interop() {
    // Empty topology -> 0 rooms.
    let empty = Topology::from_devices(&[]).await.unwrap();
    assert_eq!(empty.room_count(), 0);

    // Two devices in the SAME room: one full renderer, one Product-only preamp in standby.
    let (_r1, s1) = start_test_server("Cinema", "Streamer").await;
    let minimal = Arc::new(MinimalProduct {
        room: "Cinema".into(),
        name: "Preamp".into(),
    });
    let s2 = OpenHomeServer::builder(minimal)
        .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .ssdp(SsdpConfig::disabled())
        .start()
        .await
        .unwrap();

    let d1 = OpenHomeDevice::connect(s1.description_url()).await.unwrap();
    let d2 = OpenHomeDevice::connect(s2.description_url()).await.unwrap();

    let topo = Topology::from_devices(&[d1.clone(), d2]).await.unwrap();
    assert_eq!(topo.room_count(), 1);
    let cinema = topo.room("Cinema").unwrap();
    assert_eq!(cinema.devices.len(), 2);
    assert_eq!(topo.senders().count(), 1);
    assert_eq!(topo.receivers().count(), 1);
    assert!(cinema.devices.iter().any(|d| d.standby));
    assert!(cinema.devices.iter().any(|d| !d.standby));

    // Product:2 client talking to our Product:1 server works seamlessly (same action set).
    let product_v2 = ProductClient::with_service_type(
        HttpClient::default(),
        format!("http://{}/ctl/Product", s1.local_addr()),
        SERVICE_PRODUCT_2.to_string(),
    );
    assert_eq!(product_v2.product().await.unwrap().room, "Cinema");

    // URLBase resolution: DeviceDescription with URLBase resolves relative controlURL.
    let desc_with_url_base = DeviceDescription {
        url_base: Some(format!("http://{}/", s1.local_addr())),
        device_type: DEVICE_SOURCE_1.into(),
        friendly_name: "URLBase Dev".into(),
        manufacturer: "Test".into(),
        manufacturer_url: None,
        model_description: None,
        model_name: "Test".into(),
        model_number: None,
        model_url: None,
        serial_number: None,
        udn: s1.udn().into(),
        presentation_url: None,
        services: vec![ServiceDescription {
            service_type: SERVICE_PRODUCT_1.into(),
            service_id: openhome_core::urn::SERVICE_ID_PRODUCT.into(),
            scpd_url: "scpd/Product.xml".into(),
            control_url: "ctl/Product".into(),
            event_sub_url: "evt/Product".into(),
        }],
    };
    let dev_url_base = OpenHomeDevice::from_description(
        "http://unreachable-placeholder:1/description.xml",
        desc_with_url_base,
        HttpClient::default(),
    );
    assert_eq!(
        dev_url_base
            .product()
            .unwrap()
            .product()
            .await
            .unwrap()
            .room,
        "Cinema"
    );

    s1.shutdown().await;
    s2.shutdown().await;
}
