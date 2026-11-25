//! End-to-end Songcast integration tests: OHZ zone/preset discovery, OHU/OHM sender ↔ receiver
//! streaming, dropped-frame resend repair, stream discontinuity resync, and unicast slave promotion.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use openhome_songcast::{
    AudioFormat, OhmMessage, ReceiverConfig, ReceiverEvent, SenderConfig, SongcastError,
    SongcastReceiver, SongcastSender, SongcastUri, ZoneResponder, query_preset, resolve_zone,
};
use tokio::net::UdpSocket;

#[tokio::test]
async fn zone_responder_and_preset_discovery() {
    let responder = ZoneResponder::bind(
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        Ipv4Addr::LOCALHOST,
    )
    .await
    .unwrap();
    let SocketAddr::V4(ohz_addr) = responder.local_addr() else {
        panic!("expected IPv4");
    };

    responder.set_zone_uri("uuid:studio", "ohu://127.0.0.1:51970");
    responder.set_preset(1, "<DIDL-Lite><item>Jazz</item></DIDL-Lite>");

    let uri = resolve_zone(ohz_addr, "uuid:studio", Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        uri,
        SongcastUri::Unicast(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 51970))
    );

    let preset = query_preset(ohz_addr, 1, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        preset.as_deref(),
        Some("<DIDL-Lite><item>Jazz</item></DIDL-Lite>")
    );

    // Unknown preset returns Ok(None), unknown/removed zone returns ZoneNotFound.
    assert_eq!(
        query_preset(ohz_addr, 99, Duration::from_millis(120))
            .await
            .unwrap(),
        None
    );
    responder.remove_zone("uuid:studio");
    let err = resolve_zone(ohz_addr, "uuid:studio", Duration::from_millis(180))
        .await
        .unwrap_err();
    assert!(matches!(err, SongcastError::ZoneNotFound(_)));

    responder.shutdown().await;
}

#[tokio::test]
async fn sender_receiver_via_ohz_zone_streams_audio_metadata_and_leaves() {
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        ..SenderConfig::default()
    })
    .await
    .unwrap();

    sender
        .set_track("http://127.0.0.1/track1.flac", "<DIDL-Lite>T1</DIDL-Lite>")
        .await
        .unwrap();
    sender.set_metatext("Now Playing: T1").await.unwrap();

    // Publish sender via OHZ ZoneResponder.
    let ohz = ZoneResponder::bind(
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        Ipv4Addr::LOCALHOST,
    )
    .await
    .unwrap();
    let SocketAddr::V4(ohz_addr) = ohz.local_addr() else {
        panic!("expected IPv4");
    };
    ohz.set_zone_uri("uuid:living-room", sender.uri().to_string());

    let zone_uri = SongcastUri::Zone {
        endpoint: ohz_addr,
        zone: "uuid:living-room".into(),
    };
    let mut rx = SongcastReceiver::connect(
        &zone_uri,
        ReceiverConfig {
            join_interval: Duration::from_millis(50),
            listen_interval: Duration::from_millis(200),
            ..ReceiverConfig::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(rx.resolved_uri(), &sender.uri());

    // Wait for initial Track + Metatext delivered in response to Join.
    let mut got_track = false;
    let mut got_meta = false;
    while !(got_track && got_meta) {
        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("initial metadata arrived")
            .unwrap();
        match ev {
            ReceiverEvent::Track(t) => {
                assert_eq!(t.uri, "http://127.0.0.1/track1.flac");
                assert_eq!(t.metadata, "<DIDL-Lite>T1</DIDL-Lite>");
                got_track = true;
            }
            ReceiverEvent::Metatext(m) => {
                assert_eq!(m.metatext, "Now Playing: T1");
                got_meta = true;
            }
            _ => {}
        }
    }
    assert_eq!(sender.active_receivers(), 1);

    // Send 4 stereo 16-bit PCM frames and verify exact order and payload.
    let fmt = AudioFormat::pcm(48_000, 16, 2);
    for i in 0..4u8 {
        let pcm = vec![i; 960]; // 240 samples * 2ch * 2B
        let f = sender.send_audio(&fmt, pcm, i == 3).await.unwrap();
        assert_eq!(f, u32::from(i));
    }

    for i in 0..4u8 {
        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("audio frame arrived")
            .unwrap();
        match ev {
            ReceiverEvent::Audio { frame, .. } => {
                assert_eq!(frame.frame, u32::from(i));
                assert_eq!(frame.samples, 240);
                assert_eq!(frame.sample_rate, 48_000);
                assert_eq!(frame.audio, vec![i; 960]);
                assert_eq!(frame.flags.is_halt(), i == 3);
                assert!(frame.flags.is_lossless());
                assert!(frame.flags.is_timestamped());
            }
            other => panic!("expected Audio, got {other:?}"),
        }
    }

    // Live track update while streaming.
    sender
        .set_track("http://127.0.0.1/track2.flac", "<DIDL-Lite>T2</DIDL-Lite>")
        .await
        .unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        ev,
        ReceiverEvent::Track(t) if t.uri == "http://127.0.0.1/track2.flac"
    ));

    // Shutting down receiver sends Leave; wait briefly for sender to process Leave.
    rx.shutdown().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() != 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(sender.active_receivers(), 0);

    sender.shutdown().await;
    ohz.shutdown().await;
}

#[tokio::test]
async fn dropped_frame_is_repaired_via_ohm_resend_end_to_end() {
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        ..SenderConfig::default()
    })
    .await
    .unwrap();
    let sender_addr = SocketAddr::V4(sender.local_addr());

    // Start a tiny UDP proxy between sender and receiver that drops audio frame #1 ONCE
    // (when not marked RESENT), forcing SongcastReceiver -> OhmMessage::Resend([1]) ->
    // SongcastSender history -> Audio(frame=1, RESENT) -> ordered delivery [0, 1, 2].
    let proxy = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let SocketAddr::V4(proxy_addr) = proxy.local_addr().unwrap() else {
        panic!("expected IPv4");
    };
    let proxy_task = tokio::spawn(async move {
        let mut rx_peer: Option<SocketAddr> = None;
        let mut dropped_once = false;
        let mut buf = [0u8; 16 * 1024];
        loop {
            let Ok((len, src)) = proxy.recv_from(&mut buf).await else {
                break;
            };
            if src == sender_addr {
                if let Ok(OhmMessage::Audio(ref a)) = OhmMessage::decode(&buf[..len])
                    && a.frame == 1
                    && !a.flags.is_resent()
                    && !dropped_once
                {
                    dropped_once = true;
                    continue; // simulate UDP packet loss!
                }
                if let Some(peer) = rx_peer {
                    let _ = proxy.send_to(&buf[..len], peer).await;
                }
            } else {
                rx_peer = Some(src);
                let _ = proxy.send_to(&buf[..len], sender_addr).await;
            }
        }
    });

    let mut rx = SongcastReceiver::connect(
        &SongcastUri::Unicast(proxy_addr),
        ReceiverConfig {
            join_interval: Duration::from_millis(40),
            repair_interval: Duration::from_millis(20),
            ..ReceiverConfig::default()
        },
    )
    .await
    .unwrap();

    // Wait for sender to register the proxy peer.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(sender.active_receivers(), 1);

    // Send frames 0, 1 (dropped once!), 2.
    let fmt = AudioFormat::pcm(48_000, 16, 2);
    for i in 0..3u8 {
        sender.send_audio(&fmt, vec![i; 64], false).await.unwrap();
    }

    // Receiver must deliver frames 0, 1 (with RESENT flag!), 2 in exact ascending order!
    let mut frames = Vec::new();
    while frames.len() < 3 {
        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("repaired frame arrived")
            .unwrap();
        if let ReceiverEvent::Audio { frame, .. } = ev {
            frames.push(frame);
        }
    }
    assert_eq!(frames[0].frame, 0);
    assert!(!frames[0].flags.is_resent());
    assert_eq!(frames[1].frame, 1);
    assert!(frames[1].flags.is_resent());
    assert_eq!(frames[1].audio, vec![1; 64]);
    assert_eq!(frames[2].frame, 2);
    assert!(!frames[2].flags.is_resent());

    rx.shutdown().await;
    proxy_task.abort();
    sender.shutdown().await;
}

#[tokio::test]
async fn stream_interrupted_resyncs_receiver_and_handles_sample_rate_change() {
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        ..SenderConfig::default()
    })
    .await
    .unwrap();

    let mut rx = SongcastReceiver::connect(&sender.uri(), ReceiverConfig::default())
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let fmt48 = AudioFormat::pcm(48_000, 16, 2);
    sender.send_audio(&fmt48, vec![1; 64], false).await.unwrap();

    // Wait for frame 0.
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if matches!(ev, ReceiverEvent::Audio { ref frame, .. } if frame.frame == 0) {
            break;
        }
    }

    // Interrupt stream (+250 frames > 200 backlog) and switch to 44.1 kHz.
    sender.stream_interrupted();
    let fmt44 = AudioFormat::pcm(44_100, 24, 2);
    let new_frame = sender.send_audio(&fmt44, vec![2; 96], false).await.unwrap();
    assert_eq!(new_frame, 251);

    let mut saw_resync = false;
    loop {
        let ev = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        match ev {
            ReceiverEvent::Resynced => saw_resync = true,
            ReceiverEvent::Audio { frame, .. } if frame.frame == 251 => {
                assert!(saw_resync, "expected Resynced before frame 251");
                assert_eq!(frame.sample_rate, 44_100);
                assert_eq!(frame.bit_depth, 24);
                break;
            }
            _ => {}
        }
    }

    rx.shutdown().await;
    sender.shutdown().await;
}

#[tokio::test]
async fn unicast_multiple_receivers_slave_list_and_null_uri_error() {
    assert!(matches!(
        SongcastReceiver::connect(&SongcastUri::Null, ReceiverConfig::default()).await,
        Err(SongcastError::InvalidUri(_))
    ));

    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        ..SenderConfig::default()
    })
    .await
    .unwrap();

    // Join two raw UDP peers to inspect Slave list + primary promotion on Leave.
    let p1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let p2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dest = SocketAddr::V4(sender.local_addr());

    let join = OhmMessage::Join.encode().unwrap();
    let listen = OhmMessage::Listen.encode().unwrap();
    let leave = OhmMessage::Leave.encode().unwrap();

    p1.send_to(&join, dest).await.unwrap();
    p2.send_to(&join, dest).await.unwrap();
    p2.send_to(&listen, dest).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(sender.active_receivers(), 2);

    // Primary leaves -> p2 promoted and remains active.
    p1.send_to(&leave, dest).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() != 1 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(sender.active_receivers(), 1);

    sender.shutdown().await;
}
