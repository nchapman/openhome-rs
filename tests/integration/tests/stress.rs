//! Deterministic stress and chaos tests spanning `openhome-songcast`, `openhome-server`,
//! `openhome-client`, and `openhome-core`:
//!
//! 1. Songcast UDP network chaos (packet loss, reordering, duplication, garbage datagrams).
//! 2. Songcast burst loss beyond sender resend history -> `ReceiverEvent::Resynced`.
//! 3. Stalled unicast primary (bounded channel saturation) still relaying to healthy slaves.
//! 4. Unicast receiver churn + rapid sample-rate / `HALT` format transitions.
//! 5. High-concurrency multi-controller SOAP mutation storm + GENA convergence.
//! 6. Hostile raw-TCP HTTP peers (oversized body, malformed headers, slow/truncated, TCP drop).
//! 7. Hostile GENA subscribers (`503`/`500`/`412`/TCP drop) + subscribe/unsubscribe churn.
//! 8. `HttpClient` keep-alive pool thrashing against server-side immediate socket closes.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use openhome_client::{EventListener, OpenHomeDevice};
use openhome_core::soap::encode_action_response;
use openhome_core::types::TransportState;
use openhome_core::urn::{SERVICE_PLAYLIST_1, SERVICE_PRODUCT_1, SERVICE_VOLUME_1};
use openhome_server::{
    InfoService, MemoryRenderer, OpenHomeServer, PlaylistService, ProductService, SsdpConfig,
    VolumeService,
};
use openhome_songcast::{
    AudioFormat, OhmMessage, ReceiverConfig, ReceiverEvent, SenderConfig, SongcastReceiver,
    SongcastSender, SongcastUri,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::watch;

/// Deterministic XorShift64 PRNG so chaos tests are 100% reproducible.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn chance(&mut self, percent: u64) -> bool {
        (self.next_u64() % 100) < percent
    }
}

/// Encode a synthetic 64-byte PCM frame tagged with `seq` in the first 4 bytes (BE).
fn tagged_pcm(seq: u32) -> Vec<u8> {
    let mut pcm = vec![0u8; 64];
    pcm[..4].copy_from_slice(&seq.to_be_bytes());
    pcm[4..8].copy_from_slice(&(!seq).to_be_bytes());
    pcm
}

fn decode_pcm_tag(pcm: &[u8]) -> Option<u32> {
    if pcm.len() < 8 {
        return None;
    }
    let tag = u32::from_be_bytes(pcm[..4].try_into().ok()?);
    let inv = u32::from_be_bytes(pcm[4..8].try_into().ok()?);
    (tag == !inv).then_some(tag)
}

async fn start_test_server(room: &str) -> (OpenHomeServer, Arc<MemoryRenderer>) {
    let renderer = Arc::new(MemoryRenderer::new(room, "StressBox"));
    let server = OpenHomeServer::builder(Arc::clone(&renderer) as Arc<dyn ProductService>)
        .playlist(Arc::clone(&renderer) as Arc<dyn PlaylistService>)
        .info(Arc::clone(&renderer) as Arc<dyn InfoService>)
        .volume(Arc::clone(&renderer) as Arc<dyn VolumeService>)
        .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .advertise_host("127.0.0.1")
        .ssdp(SsdpConfig::disabled())
        .start()
        .await
        .expect("start stress OpenHomeServer");
    (server, renderer)
}

// ---------------------------------------------------------------------------
// 1. Songcast UDP network chaos: loss + reorder + duplicate + garbage packets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn songcast_udp_chaos_loss_reorder_duplication_and_garbage() {
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        history_frames: 64,
        ..SenderConfig::default()
    })
    .await
    .unwrap();
    let sender_addr = SocketAddr::V4(sender.local_addr());

    // Bidirectional UDP chaos proxy sitting between receiver and sender.
    let proxy = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let proxy_addr = match proxy.local_addr().unwrap() {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let proxy_task = tokio::spawn({
        let proxy = Arc::clone(&proxy);
        async move {
            let mut rng = Rng::new(0xC0FF_EE42_1337_0001);
            let mut rx_peer: Option<SocketAddr> = None;
            let mut delayed: Option<Vec<u8>> = None;
            let mut buf = vec![0u8; 8192];

            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                    }
                    res = proxy.recv_from(&mut buf) => {
                        let Ok((len, from)) = res else { continue };
                        let pkt = buf[..len].to_vec();

                        if from == sender_addr {
                            let Some(client) = rx_peer else { continue };
                            let original_audio_tag = match OhmMessage::decode(&pkt) {
                                Ok(OhmMessage::Audio(ref a)) if !a.flags.is_resent() => {
                                    decode_pcm_tag(&a.audio)
                                }
                                _ => None,
                            };

                            if let Some(tag) = original_audio_tag {
                                // ~12% random drop on interior frames (keep first & last so
                                // the receiver can detect and repair every dropped interior gap).
                                if tag > 0 && tag < 59 && rng.chance(12) {
                                    continue;
                                }
                                // ~15% reorder: hold this packet and flush any previously held one.
                                if tag < 59 && rng.chance(15) {
                                    if let Some(prev) = delayed.replace(pkt) {
                                        let _ = proxy.send_to(&prev, client).await;
                                    }
                                    continue;
                                }
                                // ~10% duplicate packet.
                                if rng.chance(10) {
                                    let _ = proxy.send_to(&pkt, client).await;
                                }
                                // ~8% inject malformed/truncated datagrams on the same socket.
                                if rng.chance(8) {
                                    let _ = proxy.send_to(b"Ohm\x01\x03\x00\xFF\xFF", client).await;
                                    let _ = proxy.send_to(b"GARBAGE", client).await;
                                }
                            }

                            let _ = proxy.send_to(&pkt, client).await;
                            if let Some(prev) = delayed.take() {
                                let _ = proxy.send_to(&prev, client).await;
                            }
                        } else {
                            // Receiver -> Sender (Join/Listen/Resend/Leave).
                            rx_peer = Some(from);
                            let _ = proxy.send_to(&pkt, sender_addr).await;
                        }
                    }
                }
            }
        }
    });

    let mut rx = SongcastReceiver::connect(
        &SongcastUri::Unicast(proxy_addr),
        ReceiverConfig {
            interface: Ipv4Addr::LOCALHOST,
            repair_interval: Duration::from_millis(20),
            ..ReceiverConfig::default()
        },
    )
    .await
    .unwrap();

    // Wait for Join to reach sender through the proxy.
    let join_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() == 0 && tokio::time::Instant::now() < join_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(sender.active_receivers(), 1);

    let fmt = AudioFormat::pcm(48_000, 16, 2);
    let total_frames = 60u32;
    for seq in 0..total_frames {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(3)).await;
    }

    let mut received_tags = Vec::new();
    let mut last_frame: Option<u32> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while received_tags.len() < total_frames as usize && tokio::time::Instant::now() < deadline {
        if let Ok(Some(ReceiverEvent::Audio { frame, .. })) =
            tokio::time::timeout(Duration::from_millis(150), rx.recv()).await
        {
            if let Some(prev) = last_frame {
                assert_eq!(
                    frame.frame,
                    prev.wrapping_add(1),
                    "delivered frames must be strictly contiguous after resend repair"
                );
            }
            last_frame = Some(frame.frame);
            let tag = decode_pcm_tag(&frame.audio).expect("PCM payload must not be corrupted");
            received_tags.push(tag);
        }
    }

    // All 60 frames (including every dropped/reordered one) must arrive intact and in order.
    let expected: Vec<u32> = (0..total_frames).collect();
    assert_eq!(received_tags, expected);

    let _ = stop_tx.send(true);
    let _ = proxy_task.await;
    rx.shutdown().await;
    sender.shutdown().await;
}

// ---------------------------------------------------------------------------
// 2. Songcast burst loss beyond sender history -> clean Resynced recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn songcast_burst_loss_beyond_history_resyncs_cleanly() {
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        history_frames: 6,
        ..SenderConfig::default()
    })
    .await
    .unwrap();
    let sender_addr = SocketAddr::V4(sender.local_addr());

    let proxy = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let proxy_addr = match proxy.local_addr().unwrap() {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let blackout = Arc::new(AtomicBool::new(false));
    let dropped_during_blackout = Arc::new(AtomicUsize::new(0));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let proxy_task = tokio::spawn({
        let proxy = Arc::clone(&proxy);
        let blackout = Arc::clone(&blackout);
        let dropped_during_blackout = Arc::clone(&dropped_during_blackout);
        async move {
            let mut rx_peer: Option<SocketAddr> = None;
            let mut buf = vec![0u8; 8192];
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                    }
                    res = proxy.recv_from(&mut buf) => {
                        let Ok((len, from)) = res else { continue };
                        if from == sender_addr {
                            if blackout.load(Ordering::Relaxed) {
                                dropped_during_blackout.fetch_add(1, Ordering::Relaxed);
                            } else if let Some(client) = rx_peer {
                                let _ = proxy.send_to(&buf[..len], client).await;
                            }
                        } else {
                            rx_peer = Some(from);
                            let _ = proxy.send_to(&buf[..len], sender_addr).await;
                        }
                    }
                }
            }
        }
    });

    let mut rx = SongcastReceiver::connect(
        &SongcastUri::Unicast(proxy_addr),
        ReceiverConfig {
            interface: Ipv4Addr::LOCALHOST,
            max_backlog_frames: 12,
            ..ReceiverConfig::default()
        },
    )
    .await
    .unwrap();

    let join_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() == 0 && tokio::time::Instant::now() < join_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let fmt = AudioFormat::pcm(44_100, 16, 2);
    // Stream 5 initial frames cleanly and confirm the receiver actually receives them
    // before enabling the blackout.
    for seq in 0..5u32 {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let mut pre_blackout = 0usize;
    let pre_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while pre_blackout < 5 && tokio::time::Instant::now() < pre_deadline {
        if let Ok(Some(ReceiverEvent::Audio { .. })) =
            tokio::time::timeout(Duration::from_millis(150), rx.recv()).await
        {
            pre_blackout += 1;
        }
    }
    assert_eq!(pre_blackout, 5);

    // Blackout for 25 frames (> max_backlog_frames=12 and > history_frames=6),
    // waiting until the proxy actually drops all 25 before restoring forwarding.
    blackout.store(true, Ordering::Relaxed);
    for seq in 5..30u32 {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
    }
    let drop_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while dropped_during_blackout.load(Ordering::Relaxed) < 25
        && tokio::time::Instant::now() < drop_deadline
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(dropped_during_blackout.load(Ordering::Relaxed) >= 25);

    // Restore forwarding and send 10 post-blackout frames.
    blackout.store(false, Ordering::Relaxed);
    for seq in 30..40u32 {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let mut saw_resync = false;
    let mut post_resync_tags = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while post_resync_tags.len() < 10 && tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await {
            match ev {
                ReceiverEvent::Resynced => saw_resync = true,
                ReceiverEvent::Audio { frame, .. } => {
                    if let Some(tag) = decode_pcm_tag(&frame.audio)
                        && tag >= 30
                    {
                        post_resync_tags.push(tag);
                    }
                }
                _ => {}
            }
        }
    }

    assert!(
        saw_resync,
        "expected ReceiverEvent::Resynced after burst loss"
    );
    assert_eq!(post_resync_tags, (30..40u32).collect::<Vec<_>>());

    let _ = stop_tx.send(true);
    let _ = proxy_task.await;
    rx.shutdown().await;
    sender.shutdown().await;
}

// ---------------------------------------------------------------------------
// 3. Stalled unicast primary still relays smoothly to healthy slaves
// ---------------------------------------------------------------------------

#[tokio::test]
async fn songcast_stalled_primary_still_relays_to_healthy_slaves() {
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        ..SenderConfig::default()
    })
    .await
    .unwrap();

    // Primary connects first and NEVER drains `primary.recv()` (saturating its 256-slot channel).
    let primary = SongcastReceiver::connect(
        &sender.uri(),
        ReceiverConfig {
            interface: Ipv4Addr::LOCALHOST,
            ..ReceiverConfig::default()
        },
    )
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() < 1 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let slave1 = SongcastReceiver::connect(
        &sender.uri(),
        ReceiverConfig {
            interface: Ipv4Addr::LOCALHOST,
            ..ReceiverConfig::default()
        },
    )
    .await
    .unwrap();
    let slave2 = SongcastReceiver::connect(
        &sender.uri(),
        ReceiverConfig {
            interface: Ipv4Addr::LOCALHOST,
            ..ReceiverConfig::default()
        },
    )
    .await
    .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() < 3 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(sender.active_receivers(), 3);
    // Give primary a moment to receive OhmMessage::Slave with both slaves.
    tokio::time::sleep(Duration::from_millis(40)).await;

    // Blast 320 frames (> 256 bounded channel capacity) while draining both slaves concurrently.
    let drain = |mut rx: SongcastReceiver| {
        tokio::spawn(async move {
            let mut count = 0usize;
            let mut max_tag = 0u32;
            let until = tokio::time::Instant::now() + Duration::from_secs(3);
            while count < 300 && tokio::time::Instant::now() < until {
                if let Ok(Some(ReceiverEvent::Audio { frame, .. })) =
                    tokio::time::timeout(Duration::from_millis(150), rx.recv()).await
                {
                    count += 1;
                    if let Some(tag) = decode_pcm_tag(&frame.audio) {
                        max_tag = max_tag.max(tag);
                    }
                }
            }
            rx.shutdown().await;
            (count, max_tag)
        })
    };

    let t1 = drain(slave1);
    let t2 = drain(slave2);

    let fmt = AudioFormat::pcm(48_000, 16, 2);
    for seq in 0..320u32 {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
        if seq % 16 == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    let (c1, max1) = t1.await.unwrap();
    let (c2, max2) = t2.await.unwrap();
    assert!(
        c1 >= 280 && max1 >= 299,
        "slave1 should keep receiving past primary channel cap (got count={c1}, max_tag={max1})"
    );
    assert!(
        c2 >= 280 && max2 >= 299,
        "slave2 should keep receiving past primary channel cap (got count={c2}, max_tag={max2})"
    );

    primary.shutdown().await;
    sender.shutdown().await;
}

// ---------------------------------------------------------------------------
// 4. Unicast churn (joins, abrupt drops, leaves) + rapid sample-rate / HALT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn songcast_unicast_churn_and_rapid_format_switches() {
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        receiver_expiry: Duration::from_millis(200),
        ..SenderConfig::default()
    })
    .await
    .unwrap();

    // Connect 5 receivers (1 primary + MAX_SLAVES=4); drop 2 abruptly, shut down 2 cleanly,
    // keep 1 survivor.
    let mut receivers = Vec::new();
    for _ in 0..5 {
        let rx = SongcastReceiver::connect(
            &sender.uri(),
            ReceiverConfig {
                interface: Ipv4Addr::LOCALHOST,
                listen_interval: Duration::from_millis(60),
                ..ReceiverConfig::default()
            },
        )
        .await
        .unwrap();
        receivers.push(rx);
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() < 5 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(sender.active_receivers(), 5);

    let survivor = receivers.pop().unwrap();
    let mut survivor = survivor;

    // Rapid format transitions while churning receivers.
    let formats = [
        (AudioFormat::pcm(44_100, 16, 2), false),
        (AudioFormat::pcm(48_000, 24, 2), false),
        (AudioFormat::pcm(96_000, 24, 2), true), // HALT frame
        (AudioFormat::pcm(44_100, 16, 2), false),
    ];

    for (i, (fmt, halt)) in formats.iter().enumerate() {
        if let Some(rx) = receivers.pop() {
            if i % 2 == 0 {
                rx.shutdown().await; // clean Leave
            } else {
                drop(rx); // abrupt drop (tests Drop + expiry cleanup)
            }
        }
        for k in 0..4u32 {
            let pcm_len = if fmt.bit_depth == 24 { 96 } else { 64 };
            let mut pcm = vec![0u8; pcm_len];
            let tag = ((i as u32) << 8) | k;
            pcm[..4].copy_from_slice(&tag.to_be_bytes());
            pcm[4..8].copy_from_slice(&(!tag).to_be_bytes());
            sender.send_audio(fmt, pcm, *halt).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    // Shut down the last remaining non-survivor.
    for rx in receivers {
        rx.shutdown().await;
    }

    // Wait for abruptly dropped receivers to expire so survivor becomes sole primary.
    let expiry_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() != 1 && tokio::time::Instant::now() < expiry_deadline {
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert_eq!(sender.active_receivers(), 1);

    // Send final confirmation burst at 192 kHz and verify survivor receives it cleanly.
    let final_fmt = AudioFormat::pcm(192_000, 24, 2);
    for seq in 0..5u32 {
        let mut pcm = vec![0u8; 96];
        let tag = 0xDEAD_0000 | seq;
        pcm[..4].copy_from_slice(&tag.to_be_bytes());
        pcm[4..8].copy_from_slice(&(!tag).to_be_bytes());
        sender.send_audio(&final_fmt, pcm, false).await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut saw_final_rate = 0usize;
    let until = tokio::time::Instant::now() + Duration::from_secs(2);
    while saw_final_rate < 5 && tokio::time::Instant::now() < until {
        if let Ok(Some(ReceiverEvent::Audio { frame, .. })) =
            tokio::time::timeout(Duration::from_millis(150), survivor.recv()).await
            && frame.sample_rate == 192_000
        {
            saw_final_rate += 1;
        }
    }
    assert_eq!(saw_final_rate, 5);

    survivor.shutdown().await;
    sender.shutdown().await;
}

// ---------------------------------------------------------------------------
// 5. High-concurrency multi-controller SOAP mutation storm + GENA convergence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_soap_multi_controller_mutation_storm() {
    let (server, _renderer) = start_test_server("Storm Room").await;
    let desc_url = server.description_url();

    // Attach 4 concurrent GENA subscribers on Playlist + Volume.
    let listener = EventListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();
    let base_dev = OpenHomeDevice::connect(desc_url).await.unwrap();
    let pl_sub_url = base_dev
        .event_sub_url(SERVICE_PLAYLIST_1)
        .unwrap()
        .to_string();
    let vol_sub_url = base_dev
        .event_sub_url(SERVICE_VOLUME_1)
        .unwrap()
        .to_string();

    let mut pl_subs = Vec::new();
    let mut vol_subs = Vec::new();
    for _ in 0..4 {
        pl_subs.push(listener.subscribe(&pl_sub_url, 120).await.unwrap());
        vol_subs.push(listener.subscribe(&vol_sub_url, 120).await.unwrap());
    }

    // Spawn 20 concurrent controller tasks issuing ~200 interleaved RPCs.
    let mut handles = Vec::new();
    for worker in 0..20usize {
        let url = desc_url.to_string();
        handles.push(tokio::spawn(async move {
            let dev = OpenHomeDevice::connect(url).await.unwrap();
            let pl = dev.playlist().unwrap();
            let vol = dev.volume().unwrap();
            let prod = dev.product().unwrap();

            let mut inserted = Vec::new();
            for step in 0..4usize {
                let uri = format!("http://media.local/w{worker}_t{step}.flac");
                let id = pl.insert(0, &uri, "<DIDL-Lite/>").await.unwrap();
                inserted.push(id);
                let _ = vol.set_volume(((worker * 3 + step) % 90) as u32).await;
                let _ = pl.set_repeat(step % 2 == 0).await;
                let _ = pl.set_shuffle(worker % 2 == 0).await;
                let _ = prod.standby().await.unwrap();
            }
            // Delete half the tracks we inserted.
            for &id in &inserted[..2] {
                pl.delete_id(id).await.unwrap();
            }
            // Exercise SeekId + Play on one remaining track.
            let _ = pl.seek_id(inserted[2]).await;
            let _ = pl.play().await;
            inserted[2..].to_vec()
        }));
    }

    let mut expected_ids = HashSet::new();
    for h in handles {
        for id in h.await.unwrap() {
            expected_ids.insert(id);
        }
    }

    // Set deterministic final markers so all GENA subscribers must converge on them.
    base_dev.volume().unwrap().set_volume(73).await.unwrap();
    base_dev.playlist().unwrap().stop().await.unwrap();

    let (final_token, final_ids) = base_dev.playlist().unwrap().id_array().await.unwrap();
    let final_set: HashSet<u32> = final_ids.iter().copied().collect();
    assert_eq!(
        final_set, expected_ids,
        "every non-deleted inserted ID must survive concurrent mutation storm"
    );
    assert_eq!(final_ids.len(), 40);
    assert!(final_token >= 40);

    // Verify ReadList returns all 40 tracks cleanly with no corruption.
    let entries = base_dev
        .playlist()
        .unwrap()
        .read_list(&final_ids)
        .await
        .unwrap();
    assert_eq!(entries.len(), 40);

    // All 4 Volume and Playlist GENA subscribers must converge on Volume=73 & TransportState=Stopped.
    for (idx, mut sub) in vol_subs.into_iter().enumerate() {
        let mut converged = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await
                && ev.properties.get("Volume").map(String::as_str) == Some("73")
            {
                converged = true;
                break;
            }
        }
        assert!(converged, "volume subscriber {idx} did not converge on 73");
        sub.unsubscribe().await.unwrap();
    }

    for (idx, mut sub) in pl_subs.into_iter().enumerate() {
        let mut converged = false;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await
                && ev.properties.get("TransportState").map(String::as_str)
                    == Some(TransportState::Stopped.as_str())
            {
                converged = true;
                break;
            }
        }
        assert!(
            converged,
            "playlist subscriber {idx} did not converge on Stopped"
        );
        sub.unsubscribe().await.unwrap();
    }

    listener.shutdown().await;
    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// 6. Hostile raw-TCP HTTP peers against OpenHomeServer
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hostile_http_peers_oversized_malformed_truncated_and_rst() {
    let (server, _renderer) = start_test_server("Fortress").await;
    let addr = server.local_addr();
    let desc_url = server.description_url();

    // Attack 1: Oversized body (> 4 MiB MAX_SOAP_BODY_BYTES) -> 413 Payload Too Large immediately.
    let oversize_status = {
        let mut s = TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST /ctl/Product HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 5242880\r\nSOAPACTION: \"{SERVICE_PRODUCT_1}#Room\"\r\n\r\n"
        );
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = [0u8; 512];
        let n = s.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    };
    assert!(
        oversize_status.contains("413"),
        "expected 413 Payload Too Large, got: {oversize_status}"
    );

    // Attack 2: Malformed request line & bogus headers.
    {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"NOT_A_VALID_HTTP_LINE\r\n\r\n").await.unwrap();
        let mut buf = [0u8; 256];
        let _ = tokio::time::timeout(Duration::from_millis(200), s.read(&mut buf)).await;
    }

    // Attack 3: 15 concurrent truncated / immediate-drop / garbage-XML TCP peers.
    let mut attackers = Vec::new();
    for i in 0..15usize {
        attackers.push(tokio::spawn(async move {
            let Ok(mut s) = TcpStream::connect(addr).await else {
                return;
            };
            match i % 3 {
                0 => {
                    // Partial headers then abrupt drop.
                    let _ = s.write_all(b"POST /ctl/Product HTTP/1.1\r\nHost: 127").await;
                    drop(s);
                }
                1 => {
                    // Valid headers with truncated body then drop.
                    let _ = s
                        .write_all(
                            b"POST /ctl/Product HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 500\r\n\r\n<s:Envel",
                        )
                        .await;
                    drop(s);
                }
                _ => {
                    // Malformed XML SOAP body.
                    let bad_xml = "<<<<<not xml>>>>>\0\x01\x02";
                    let req = format!(
                        "POST /ctl/Product HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nSOAPACTION: \"{SERVICE_PRODUCT_1}#Room\"\r\n\r\n{bad_xml}",
                        bad_xml.len()
                    );
                    let _ = s.write_all(req.as_bytes()).await;
                    let mut buf = [0u8; 512];
                    let _ = tokio::time::timeout(Duration::from_millis(200), s.read(&mut buf)).await;
                }
            }
        }));
    }
    for h in attackers {
        let _ = h.await;
    }

    // Server must remain completely healthy for legitimate control points.
    let dev = OpenHomeDevice::connect(desc_url).await.unwrap();
    assert_eq!(
        dev.product().unwrap().product().await.unwrap().room,
        "Fortress"
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// 7. Hostile GENA subscribers (503/500/412/TCP drop) + subscription churn
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hostile_gena_subscribers_503_500_412_rst_and_subscription_churn() {
    let (server, _renderer) = start_test_server("GenaChaos").await;
    let server_addr = server.local_addr();
    let desc_url = server.description_url();

    // Custom hostile callback server handling /flaky (503 -> 500 -> 200), /gone (412), /drop (RST).
    let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    let flaky_attempts = Arc::new(AtomicUsize::new(0));
    let gone_attempts = Arc::new(AtomicUsize::new(0));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let mock_task = tokio::spawn({
        let flaky_attempts = Arc::clone(&flaky_attempts);
        let gone_attempts = Arc::clone(&gone_attempts);
        async move {
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                    }
                    res = mock_listener.accept() => {
                        let Ok((mut stream, _)) = res else { continue };
                        let flaky_attempts = Arc::clone(&flaky_attempts);
                        let gone_attempts = Arc::clone(&gone_attempts);
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let Ok(n) = stream.read(&mut buf).await else { return };
                            let req = String::from_utf8_lossy(&buf[..n]);
                            let first_line = req.lines().next().unwrap_or("");
                            if first_line.contains("/drop") {
                                // Abruptly drop TCP without responding.
                                drop(stream);
                            } else if first_line.contains("/gone") {
                                gone_attempts.fetch_add(1, Ordering::Relaxed);
                                let _ = stream
                                    .write_all(b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                                    .await;
                            } else if first_line.contains("/flaky") {
                                let attempt = flaky_attempts.fetch_add(1, Ordering::Relaxed);
                                let resp = match attempt {
                                    0 => "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                    1 => "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                    _ => "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                };
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                        });
                    }
                }
            }
        }
    });

    // Helper to send raw GENA SUBSCRIBE to server_addr.
    async fn raw_subscribe(server_addr: SocketAddr, cb: &str) -> String {
        let mut s = TcpStream::connect(server_addr).await.unwrap();
        let req = format!(
            "SUBSCRIBE /evt/Volume HTTP/1.1\r\nHost: {server_addr}\r\nCALLBACK: <{cb}>\r\nNT: upnp:event\r\nTIMEOUT: Second-60\r\n\r\n"
        );
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = [0u8; 1024];
        let n = s.read(&mut buf).await.unwrap();
        let resp = String::from_utf8_lossy(&buf[..n]);
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "unexpected: {resp}");
        resp.lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.eq_ignore_ascii_case("sid").then(|| v.trim().to_string())
            })
            .expect("SID header")
    }

    let _sid_flaky = raw_subscribe(server_addr, &format!("http://{mock_addr}/flaky")).await;
    let _sid_gone = raw_subscribe(server_addr, &format!("http://{mock_addr}/gone")).await;
    let _sid_drop = raw_subscribe(server_addr, &format!("http://{mock_addr}/drop")).await;

    // Also attach a healthy subscriber via openhome-client.
    let listener = EventListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();
    let dev = OpenHomeDevice::connect(desc_url).await.unwrap();
    let vol_sub_url = dev.event_sub_url(SERVICE_VOLUME_1).unwrap().to_string();
    let mut healthy = listener.subscribe(&vol_sub_url, 60).await.unwrap();
    // Drain initial GENA snapshot on healthy subscriber.
    let _ = tokio::time::timeout(Duration::from_secs(1), healthy.recv()).await;

    // Trigger several Volume mutations.
    for v in [25u32, 40, 55] {
        dev.volume().unwrap().set_volume(v).await.unwrap();
    }

    // Healthy subscriber must receive Volume=55 promptly despite hostile peers.
    let mut healthy_ok = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(150), healthy.recv()).await
            && ev.properties.get("Volume").map(String::as_str) == Some("55")
        {
            healthy_ok = true;
            break;
        }
    }
    assert!(healthy_ok, "healthy subscriber starved by hostile peers");

    // Wait for /flaky to retry past 503 -> 500 -> 200 (>= 3 attempts).
    let retry_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while flaky_attempts.load(Ordering::Relaxed) < 3 && tokio::time::Instant::now() < retry_deadline
    {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        flaky_attempts.load(Ordering::Relaxed) >= 3,
        "expected flaky subscriber to retry through 503 -> 500 -> 200"
    );

    // /gone returned 412 on first delivery -> must have been cancelled immediately (<= 1 attempt).
    let gone_before = gone_attempts.load(Ordering::Relaxed);
    assert_eq!(gone_before, 1, "412 must cancel subscription immediately");
    dev.volume().unwrap().set_volume(60).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        gone_attempts.load(Ordering::Relaxed),
        1,
        "cancelled 412 subscription must not receive further deliveries"
    );

    // Rapid subscribe/unsubscribe churn (60 cycles) must not leak subscription slots.
    for _ in 0..60 {
        let sub = listener.subscribe(&vol_sub_url, 60).await.unwrap();
        sub.unsubscribe().await.unwrap();
    }

    healthy.unsubscribe().await.unwrap();
    let _ = stop_tx.send(true);
    let _ = mock_task.await;
    listener.shutdown().await;
    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// 8. HttpClient keep-alive pool thrashing against immediate server-side close
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_client_pool_thrashing_on_server_side_keepalive_closes() {
    // Custom HTTP/1.1 server that lies with `Connection: keep-alive` and immediately
    // drops the TCP connection after every single response, forcing stale-pool retries.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let desc_xml = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <device>
    <deviceType>urn:av-openhome-org:device:Source:1</deviceType>
    <friendlyName>CloseBox</friendlyName>
    <UDN>uuid:closebox-0001</UDN>
    <serviceList>
      <service>
        <serviceType>{SERVICE_PRODUCT_1}</serviceType>
        <serviceId>urn:av-openhome-org:serviceId:Product</serviceId>
        <SCPDURL>/scpd/Product.xml</SCPDURL>
        <controlURL>/ctl/Product</controlURL>
        <eventSubURL>/evt/Product</eventSubURL>
      </service>
    </serviceList>
  </device>
</root>"#
    );
    let soap_ok = encode_action_response(SERVICE_PRODUCT_1, "Standby", &[("Value", "0")]);

    let server_task = tokio::spawn({
        let desc_xml = desc_xml.clone();
        let soap_ok = soap_ok.clone();
        async move {
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            break;
                        }
                    }
                    res = listener.accept() => {
                        let Ok((mut stream, _)) = res else { continue };
                        let desc_xml = desc_xml.clone();
                        let soap_ok = soap_ok.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let Ok(n) = stream.read(&mut buf).await else { return };
                            if n == 0 {
                                return;
                            }
                            let req = String::from_utf8_lossy(&buf[..n]);
                            let body = if req.starts_with("GET /description.xml") {
                                desc_xml.as_bytes()
                            } else {
                                soap_ok.as_bytes()
                            };
                            let header = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/xml; charset=\"utf-8\"\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                                body.len()
                            );
                            let _ = stream.write_all(header.as_bytes()).await;
                            let _ = stream.write_all(body).await;
                            let _ = stream.shutdown().await;
                            // Immediately drop socket despite `Connection: keep-alive`.
                        });
                    }
                }
            }
        }
    });

    let dev = OpenHomeDevice::connect(&format!("http://{addr}/description.xml"))
        .await
        .unwrap();
    let prod = dev.product().unwrap();

    // 25 sequential calls reusing the same pooled HttpClient: every pooled socket is dead
    // on reuse and must be transparently evicted and retried!
    for i in 0..25usize {
        let standby = prod
            .standby()
            .await
            .unwrap_or_else(|e| panic!("sequential call {i} failed on stale keep-alive: {e}"));
        assert!(!standby);
    }

    // 16 concurrent calls through cloned devices sharing the same HttpClient pool.
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let d = dev.clone();
        tasks.push(tokio::spawn(async move {
            let p = d.product().unwrap();
            assert!(!p.standby().await.unwrap());
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let _ = stop_tx.send(true);
    let _ = server_task.await;
}
