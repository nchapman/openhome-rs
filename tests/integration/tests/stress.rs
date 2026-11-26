//! Deterministic stress and chaos tests spanning `openhome-songcast`, `openhome-server`,
//! `openhome-client`, and `openhome-core`:
//!
//! 1. Songcast UDP network chaos (loss, reorder, duplication, garbage) with counter proofs.
//! 2. Songcast burst loss (backlog overflow + sender history exhaustion) -> `Resynced`.
//! 3. Stalled unicast primary (bounded channel saturation) still relaying to healthy slaves.
//! 4. Unicast silent-peer expiry (no `Leave`) + rapid sample-rate / `HALT` transitions.
//! 5. High-concurrency multi-controller SOAP storm + post-storm GENA `SEQ>0` & `IdArray` convergence.
//! 6. Hostile raw-TCP HTTP peers (declared & chunked 413 + `Connection: close`, 400, SOAP 500, drops).
//! 7. Hostile GENA subscribers (`503`/`500`/`412`/TCP drop) + >256-cycle subscription churn.
//! 8. `HttpClient` keep-alive pool thrashing against immediate server-side socket closes.
//! 9. Non-idempotent `POST` mid-flight TCP drop executes once (no silent duplicate retry).
//! 10. Unicast `SongcastReceiver` rejects spoofed `OhmMessage::Slave` from unrelated UDP peers.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use openhome_client::{EventListener, OpenHomeDevice};
use openhome_core::id_array::decode_id_array;
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

/// Encode a synthetic 64-byte PCM frame tagged with `seq` in the first 8 bytes.
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

/// Read a complete HTTP/1.1 request (headers + declared `Content-Length` body) before
/// replying/closing so loopback TCP never emits spurious `ECONNRESET` from unread bytes.
async fn read_full_http_request(stream: &mut TcpStream) -> Option<String> {
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 2048];
    let header_end = loop {
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut tmp))
            .await
            .ok()?
            .ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 64 * 1024 {
            return None;
        }
    };

    let headers_str = String::from_utf8_lossy(&buf[..header_end]);
    let content_len: usize = headers_str
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);

    let total_needed = header_end + content_len;
    while buf.len() < total_needed {
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut tmp))
            .await
            .ok()?
            .ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    Some(String::from_utf8_lossy(&buf).into_owned())
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

    let proxy = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let proxy_addr = match proxy.local_addr().unwrap() {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let drops = Arc::new(AtomicUsize::new(0));
    let reorders = Arc::new(AtomicUsize::new(0));
    let dupes = Arc::new(AtomicUsize::new(0));
    let garbage = Arc::new(AtomicUsize::new(0));
    let resends = Arc::new(AtomicUsize::new(0));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let proxy_task = tokio::spawn({
        let proxy = Arc::clone(&proxy);
        let drops = Arc::clone(&drops);
        let reorders = Arc::clone(&reorders);
        let dupes = Arc::clone(&dupes);
        let garbage = Arc::clone(&garbage);
        let resends = Arc::clone(&resends);
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
                                    drops.fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }
                                // ~15% reorder: hold this packet and flush any previously held one.
                                if tag < 59 && rng.chance(15) {
                                    reorders.fetch_add(1, Ordering::Relaxed);
                                    if let Some(prev) = delayed.replace(pkt) {
                                        let _ = proxy.send_to(&prev, client).await;
                                    }
                                    continue;
                                }
                                // ~10% duplicate packet.
                                if rng.chance(10) {
                                    dupes.fetch_add(1, Ordering::Relaxed);
                                    let _ = proxy.send_to(&pkt, client).await;
                                }
                                // ~8% inject malformed/truncated datagrams on the same socket.
                                if rng.chance(8) {
                                    garbage.fetch_add(1, Ordering::Relaxed);
                                    let _ = proxy.send_to(b"Ohm\x01\x03\x00\xFF\xFF", client).await;
                                    let _ = proxy.send_to(b"GARBAGE", client).await;
                                }
                            }

                            let _ = proxy.send_to(&pkt, client).await;
                            if let Some(prev) = delayed.take() {
                                let _ = proxy.send_to(&prev, client).await;
                            }
                        } else {
                            rx_peer = Some(from);
                            if matches!(OhmMessage::decode(&pkt), Ok(OhmMessage::Resend(_))) {
                                resends.fetch_add(1, Ordering::Relaxed);
                            }
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
    let mut saw_resync = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while received_tags.len() < total_frames as usize && tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await {
            match ev {
                ReceiverEvent::Resynced => saw_resync = true,
                ReceiverEvent::Audio { frame, .. } => {
                    if let Some(prev) = last_frame {
                        assert_eq!(
                            frame.frame,
                            prev.wrapping_add(1),
                            "delivered frames must be strictly contiguous after resend repair"
                        );
                    }
                    last_frame = Some(frame.frame);
                    let tag = decode_pcm_tag(&frame.audio).expect("PCM payload uncorrupted");
                    received_tags.push(tag);
                }
                _ => {}
            }
        }
    }

    assert!(!saw_resync, "repairable chaos must not trigger Resynced");
    assert_eq!(received_tags, (0..total_frames).collect::<Vec<_>>());
    assert!(drops.load(Ordering::Relaxed) > 0, "expected drops > 0");
    assert!(
        reorders.load(Ordering::Relaxed) > 0,
        "expected reorders > 0"
    );
    assert!(dupes.load(Ordering::Relaxed) > 0, "expected dupes > 0");
    assert!(garbage.load(Ordering::Relaxed) > 0, "expected garbage > 0");
    assert!(resends.load(Ordering::Relaxed) > 0, "expected resends > 0");

    let _ = stop_tx.send(true);
    let _ = proxy_task.await;
    rx.shutdown().await;
    sender.shutdown().await;
}

// ---------------------------------------------------------------------------
// 2. Songcast burst loss: backlog overflow AND sender history exhaustion -> Resynced
// ---------------------------------------------------------------------------

#[tokio::test]
async fn songcast_burst_loss_beyond_history_resyncs_cleanly() {
    // Phase A: gap (25) > max_backlog_frames (12) -> immediate backlog-overflow resync.
    // Phase B: gap (6) <= max_backlog_frames (12) but > history_frames (4) -> sender cannot
    // repair missing frames, so streaming 15+ subsequent frames overflows pending backlog -> resync.
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        history_frames: 4,
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
    let dropped_audio = Arc::new(AtomicUsize::new(0));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let proxy_task = tokio::spawn({
        let proxy = Arc::clone(&proxy);
        let blackout = Arc::clone(&blackout);
        let dropped_audio = Arc::clone(&dropped_audio);
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
                            if blackout.load(Ordering::SeqCst) {
                                if matches!(OhmMessage::decode(&buf[..len]), Ok(OhmMessage::Audio(_))) {
                                    dropped_audio.fetch_add(1, Ordering::SeqCst);
                                }
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
            repair_interval: Duration::from_millis(20),
            ..ReceiverConfig::default()
        },
    )
    .await
    .unwrap();

    let join_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() == 0 && tokio::time::Instant::now() < join_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(sender.active_receivers(), 1);

    let fmt = AudioFormat::pcm(44_100, 16, 2);
    // Stream 5 initial frames and confirm receiver gets all 5.
    for seq in 0..5u32 {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let mut pre = 0usize;
    let pre_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while pre < 5 && tokio::time::Instant::now() < pre_deadline {
        if let Ok(Some(ReceiverEvent::Audio { .. })) =
            tokio::time::timeout(Duration::from_millis(150), rx.recv()).await
        {
            pre += 1;
        }
    }
    assert_eq!(pre, 5);

    // Phase A: Drop 25 audio frames (> max_backlog_frames=12).
    dropped_audio.store(0, Ordering::SeqCst);
    blackout.store(true, Ordering::SeqCst);
    for seq in 5..30u32 {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
    }
    let drop_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while dropped_audio.load(Ordering::SeqCst) < 25 && tokio::time::Instant::now() < drop_deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(dropped_audio.load(Ordering::SeqCst), 25);

    blackout.store(false, Ordering::SeqCst);
    for seq in 30..40u32 {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let mut resync_count = 0usize;
    let mut phase_a_tags = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while phase_a_tags.len() < 10 && tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await {
            match ev {
                ReceiverEvent::Resynced => resync_count += 1,
                ReceiverEvent::Audio { frame, .. } => {
                    if let Some(tag) = decode_pcm_tag(&frame.audio)
                        && tag >= 30
                    {
                        phase_a_tags.push(tag);
                    }
                }
                _ => {}
            }
        }
    }
    assert_eq!(resync_count, 1, "expected Phase A Resynced");
    assert_eq!(phase_a_tags, (30..40u32).collect::<Vec<_>>());

    // Phase B: Drop 6 audio frames (<= max_backlog_frames=12, > history_frames=4).
    // Sender cannot repair the earliest missing frames; subsequent frames fill pending
    // backlog until ReorderBuffer resyncs cleanly.
    dropped_audio.store(0, Ordering::SeqCst);
    blackout.store(true, Ordering::SeqCst);
    for seq in 40..46u32 {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
    }
    let drop_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while dropped_audio.load(Ordering::SeqCst) < 6 && tokio::time::Instant::now() < drop_deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(dropped_audio.load(Ordering::SeqCst), 6);

    blackout.store(false, Ordering::SeqCst);
    for seq in 46..66u32 {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    let mut saw_phase_b_tail = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !saw_phase_b_tail && tokio::time::Instant::now() < deadline {
        if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await {
            match ev {
                ReceiverEvent::Resynced => resync_count += 1,
                ReceiverEvent::Audio { frame, .. } if decode_pcm_tag(&frame.audio) == Some(65) => {
                    saw_phase_b_tail = true;
                }
                _ => {}
            }
        }
    }
    assert_eq!(
        resync_count, 2,
        "expected Phase B Resynced after sender history exhaustion"
    );
    assert!(saw_phase_b_tail);

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
        history_frames: 320,
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

    let mut slave1 = SongcastReceiver::connect(
        &sender.uri(),
        ReceiverConfig {
            interface: Ipv4Addr::LOCALHOST,
            ..ReceiverConfig::default()
        },
    )
    .await
    .unwrap();
    let mut slave2 = SongcastReceiver::connect(
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

    // Confirm both slaves are actively receiving relayed packets before blasting 320 frames
    // (eliminates any sleep-based startup race on loaded CI runners).
    let fmt = AudioFormat::pcm(48_000, 16, 2);
    let mut s1_ready = false;
    let mut s2_ready = false;
    let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !(s1_ready && s2_ready) && tokio::time::Instant::now() < ready_deadline {
        sender
            .send_audio(&fmt, tagged_pcm(0xFFFF_0000), false)
            .await
            .unwrap();
        if !s1_ready
            && matches!(
                tokio::time::timeout(Duration::from_millis(20), slave1.recv()).await,
                Ok(Some(ReceiverEvent::Audio { .. }))
            )
        {
            s1_ready = true;
        }
        if !s2_ready
            && matches!(
                tokio::time::timeout(Duration::from_millis(20), slave2.recv()).await,
                Ok(Some(ReceiverEvent::Audio { .. }))
            )
        {
            s2_ready = true;
        }
    }
    assert!(s1_ready && s2_ready, "slaves must receive relay sentinel");

    // Blast 320 frames (> 256 bounded channel capacity on stalled primary) while draining slaves.
    let drain = |mut rx: SongcastReceiver| {
        tokio::spawn(async move {
            let mut count = 0usize;
            let mut max_tag = 0u32;
            let until = tokio::time::Instant::now() + Duration::from_secs(3);
            while max_tag < 319 && tokio::time::Instant::now() < until {
                if let Ok(Some(ReceiverEvent::Audio { frame, .. })) =
                    tokio::time::timeout(Duration::from_millis(150), rx.recv()).await
                    && let Some(tag) = decode_pcm_tag(&frame.audio)
                    && tag < 320
                {
                    count += 1;
                    max_tag = max_tag.max(tag);
                }
            }
            rx.shutdown().await;
            (count, max_tag)
        })
    };

    let t1 = drain(slave1);
    let t2 = drain(slave2);

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
        c1 >= 300 && max1 == 319,
        "slave1 should keep receiving past primary channel cap (got count={c1}, max_tag={max1})"
    );
    assert!(
        c2 >= 300 && max2 == 319,
        "slave2 should keep receiving past primary channel cap (got count={c2}, max_tag={max2})"
    );

    primary.shutdown().await;
    sender.shutdown().await;
}

// ---------------------------------------------------------------------------
// 4. Unicast silent-peer expiry (no Leave!) + rapid sample-rate / HALT switches
// ---------------------------------------------------------------------------

#[tokio::test]
async fn songcast_unicast_churn_and_rapid_format_switches() {
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        receiver_expiry: Duration::from_millis(350),
        ..SenderConfig::default()
    })
    .await
    .unwrap();
    let dest = SocketAddr::V4(sender.local_addr());

    // 1) Raw UDP silent primary that sends Join and NEVER sends Leave (truly tests receiver_expiry!).
    let silent_primary = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    silent_primary
        .send_to(&OhmMessage::Join.encode().unwrap(), dest)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() < 1 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // 2) Connect 4 real receivers (total 5 = 1 primary + MAX_SLAVES=4); 3 leave cleanly, 1 survives.
    let mut receivers = Vec::new();
    for _ in 0..4 {
        let rx = SongcastReceiver::connect(
            &sender.uri(),
            ReceiverConfig {
                interface: Ipv4Addr::LOCALHOST,
                join_interval: Duration::from_millis(50),
                listen_interval: Duration::from_millis(50),
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

    let mut survivor = receivers.pop().unwrap();
    for rx in receivers {
        rx.shutdown().await; // clean Leave -> leaves silent_primary + survivor (2)
    }

    // Wait for silent_primary to expire (receiver_expiry=350ms) and promote survivor to sole primary.
    let expiry_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while sender.active_receivers() != 1 && tokio::time::Instant::now() < expiry_deadline {
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert_eq!(
        sender.active_receivers(),
        1,
        "silent primary must expire via receiver_expiry without Leave"
    );
    drop(silent_primary);

    // Stream across rapid format transitions (44.1k/16b -> 48k/24b -> 96k/24b HALT -> 192k/24b)
    // and assert survivor receives valid frames from EVERY phase including the HALT flag.
    let formats = [
        (AudioFormat::pcm(44_100, 16, 2), false),
        (AudioFormat::pcm(48_000, 24, 2), false),
        (AudioFormat::pcm(96_000, 24, 2), true),
        (AudioFormat::pcm(192_000, 24, 2), false),
    ];

    for (i, (fmt, halt)) in formats.iter().enumerate() {
        for k in 0..4u32 {
            let pcm_len = if fmt.bit_depth == 24 { 96 } else { 64 };
            let mut pcm = vec![0u8; pcm_len];
            let tag = ((i as u32) << 8) | k;
            pcm[..4].copy_from_slice(&tag.to_be_bytes());
            pcm[4..8].copy_from_slice(&(!tag).to_be_bytes());
            sender.send_audio(fmt, pcm, *halt).await.unwrap();
            tokio::time::sleep(Duration::from_millis(4)).await;
        }
    }

    let mut seen_rates = HashSet::new();
    let mut saw_halt = false;
    let until = tokio::time::Instant::now() + Duration::from_secs(2);
    while (seen_rates.len() < 4 || !saw_halt) && tokio::time::Instant::now() < until {
        if let Ok(Some(ReceiverEvent::Audio { frame, .. })) =
            tokio::time::timeout(Duration::from_millis(150), survivor.recv()).await
        {
            assert!(decode_pcm_tag(&frame.audio).is_some());
            seen_rates.insert((frame.sample_rate, frame.bit_depth));
            if frame.flags.is_halt() {
                saw_halt = true;
            }
        }
    }

    assert!(seen_rates.contains(&(44_100, 16)));
    assert!(seen_rates.contains(&(48_000, 24)));
    assert!(seen_rates.contains(&(96_000, 24)));
    assert!(seen_rates.contains(&(192_000, 24)));
    assert!(saw_halt, "expected HALT frame to reach survivor");

    survivor.shutdown().await;
    sender.shutdown().await;
}

// ---------------------------------------------------------------------------
// 5. High-concurrency multi-controller SOAP storm + post-storm GENA convergence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_soap_multi_controller_mutation_storm() {
    let (server, _renderer) = start_test_server("Storm Room").await;
    let desc_url = server.description_url();

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

    // Subscribe 4 Playlist + 4 Volume GENA listeners and DRAIN initial SEQ=0 snapshots
    // BEFORE the storm so post-storm assertions cannot falsely pass on SEQ=0!
    let mut pl_subs = Vec::new();
    let mut vol_subs = Vec::new();
    for _ in 0..4 {
        let mut pl_sub = listener.subscribe(&pl_sub_url, 120).await.unwrap();
        let init_pl = tokio::time::timeout(Duration::from_secs(2), pl_sub.recv())
            .await
            .expect("initial playlist snapshot timeout")
            .expect("initial playlist snapshot");
        assert_eq!(init_pl.seq, 0);
        pl_subs.push(pl_sub);

        let mut vol_sub = listener.subscribe(&vol_sub_url, 120).await.unwrap();
        let init_vol = tokio::time::timeout(Duration::from_secs(2), vol_sub.recv())
            .await
            .expect("initial volume snapshot timeout")
            .expect("initial volume snapshot");
        assert_eq!(init_vol.seq, 0);
        vol_subs.push(vol_sub);
    }

    // Spawn 20 concurrent controller tasks issuing ~200 interleaved RPCs (zero swallowed errors!).
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
                vol.set_volume(((worker * 3 + step) % 90) as u32)
                    .await
                    .unwrap();
                pl.set_repeat(step % 2 == 0).await.unwrap();
                pl.set_shuffle(worker % 2 == 0).await.unwrap();
                let _ = prod.standby().await.unwrap();
            }
            for &id in &inserted[..2] {
                pl.delete_id(id).await.unwrap();
            }
            pl.seek_id(inserted[2]).await.unwrap();
            pl.play().await.unwrap();
            inserted[2..].to_vec()
        }));
    }

    let mut expected_ids = HashSet::new();
    for h in handles {
        for id in h.await.unwrap() {
            expected_ids.insert(id);
        }
    }

    // Deterministic final markers after the storm completes.
    base_dev.volume().unwrap().set_volume(73).await.unwrap();
    base_dev.playlist().unwrap().stop().await.unwrap();

    let (final_token, final_ids) = base_dev.playlist().unwrap().id_array().await.unwrap();
    let final_set: HashSet<u32> = final_ids.iter().copied().collect();
    assert_eq!(final_set, expected_ids);
    assert_eq!(final_ids.len(), 40);
    // Initial token (1) + 80 inserts + 40 deletes = 121.
    assert_eq!(final_token, 121);

    let entries = base_dev
        .playlist()
        .unwrap()
        .read_list(&final_ids)
        .await
        .unwrap();
    assert_eq!(entries.len(), 40);

    // Every Volume subscriber must receive post-storm SEQ>0 converging on Volume=73.
    for (idx, mut sub) in vol_subs.into_iter().enumerate() {
        let mut converged = false;
        let mut last_seq = 0u32;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await
            {
                assert!(ev.seq > last_seq, "GENA SEQ must strictly increase");
                last_seq = ev.seq;
                if ev.properties.get("Volume").map(String::as_str) == Some("73") {
                    converged = true;
                    break;
                }
            }
        }
        assert!(converged, "volume subscriber {idx} did not converge on 73");
        sub.unsubscribe().await.unwrap();
    }

    // Every Playlist subscriber must converge on BOTH TransportState=Stopped AND exact final IdArray!
    for (idx, mut sub) in pl_subs.into_iter().enumerate() {
        let mut last_seq = 0u32;
        let mut latest_state: Option<String> = None;
        let mut latest_ids: Option<Vec<u32>> = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await
            {
                assert!(ev.seq > last_seq, "GENA SEQ must strictly increase");
                last_seq = ev.seq;
                if let Some(st) = ev.properties.get("TransportState") {
                    latest_state = Some(st.clone());
                }
                if let Some(raw_arr) = ev.properties.get("IdArray") {
                    latest_ids = Some(decode_id_array(raw_arr).unwrap());
                }
                if latest_state.as_deref() == Some(TransportState::Stopped.as_str())
                    && latest_ids.as_ref() == Some(&final_ids)
                {
                    break;
                }
            }
        }
        assert_eq!(
            latest_state.as_deref(),
            Some(TransportState::Stopped.as_str()),
            "playlist subscriber {idx} did not converge on Stopped"
        );
        assert_eq!(
            latest_ids.as_ref(),
            Some(&final_ids),
            "playlist subscriber {idx} did not converge on final IdArray"
        );
        sub.unsubscribe().await.unwrap();
    }

    listener.shutdown().await;
    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// 6. Hostile raw-TCP HTTP peers (declared & chunked 413 + Connection: close, 400, 500)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hostile_http_peers_oversized_malformed_truncated_and_rst() {
    let (server, _renderer) = start_test_server("Fortress").await;
    let addr = server.local_addr();
    let desc_url = server.description_url();

    // Attack 1a: Declared oversized Content-Length (> 4 MiB) -> 413 + Connection: close immediately.
    let oversize_resp = {
        let mut s = TcpStream::connect(addr).await.unwrap();
        let req = format!(
            "POST /ctl/Product HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 5242880\r\nSOAPACTION: \"{SERVICE_PRODUCT_1}#Standby\"\r\n\r\n"
        );
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = [0u8; 512];
        let n = s.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    };
    assert!(
        oversize_resp.starts_with("HTTP/1.1 413"),
        "expected HTTP/1.1 413, got: {oversize_resp}"
    );
    assert!(
        oversize_resp
            .to_ascii_lowercase()
            .contains("connection: close"),
        "413 on unread body must include Connection: close: {oversize_resp}"
    );

    // Attack 1b: Chunked streaming oversized body (> 4 MiB) -> 413 + Connection: close.
    let chunked_resp = {
        let mut s = TcpStream::connect(addr).await.unwrap();
        let head = format!(
            "POST /ctl/Product HTTP/1.1\r\nHost: {addr}\r\nTransfer-Encoding: chunked\r\nSOAPACTION: \"{SERVICE_PRODUCT_1}#Standby\"\r\n\r\n"
        );
        s.write_all(head.as_bytes()).await.unwrap();
        let chunk = format!("{:x}\r\n{}\r\n", 256 * 1024, "A".repeat(256 * 1024));
        for _ in 0..18 {
            if s.write_all(chunk.as_bytes()).await.is_err() {
                break;
            }
        }
        let mut buf = [0u8; 512];
        let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    };
    assert!(
        chunked_resp.starts_with("HTTP/1.1 413"),
        "expected HTTP/1.1 413 on chunked overflow, got: {chunked_resp}"
    );
    assert!(
        chunked_resp
            .to_ascii_lowercase()
            .contains("connection: close"),
        "chunked 413 must include Connection: close: {chunked_resp}"
    );

    // Attack 2: Malformed HTTP request line -> 400 Bad Request.
    let bad_http_resp = {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"NOT_A_VALID_HTTP_LINE\r\n\r\n").await.unwrap();
        let mut buf = [0u8; 256];
        let n = tokio::time::timeout(Duration::from_secs(1), s.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        String::from_utf8_lossy(&buf[..n]).to_string()
    };
    assert!(
        bad_http_resp.starts_with("HTTP/1.1 400"),
        "expected 400 Bad Request, got: {bad_http_resp}"
    );

    // Attack 3: 15 concurrent truncated / immediate-drop / garbage-XML TCP peers.
    let mut attackers = Vec::new();
    for i in 0..15usize {
        attackers.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            match i % 3 {
                0 => {
                    let _ = s.write_all(b"POST /ctl/Product HTTP/1.1\r\nHost: 127").await;
                    drop(s);
                }
                1 => {
                    let _ = s
                        .write_all(
                            b"POST /ctl/Product HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 500\r\n\r\n<s:Envel",
                        )
                        .await;
                    drop(s);
                }
                _ => {
                    let bad_xml = "<<<<<not xml>>>>>\0\x01\x02";
                    let req = format!(
                        "POST /ctl/Product HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nSOAPACTION: \"{SERVICE_PRODUCT_1}#Standby\"\r\n\r\n{bad_xml}",
                        bad_xml.len()
                    );
                    s.write_all(req.as_bytes()).await.unwrap();
                    let mut buf = [0u8; 1024];
                    let n = tokio::time::timeout(Duration::from_secs(1), s.read(&mut buf))
                        .await
                        .unwrap()
                        .unwrap();
                    let resp = String::from_utf8_lossy(&buf[..n]);
                    assert!(
                        resp.starts_with("HTTP/1.1 500") && resp.contains("UPnPError"),
                        "malformed SOAP XML must return 500 UPnPError, got: {resp}"
                    );
                }
            }
        }));
    }
    for h in attackers {
        h.await.unwrap();
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
// 7. Hostile GENA subscribers (503/500/412/TCP drop) + >256-cycle churn
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hostile_gena_subscribers_503_500_412_rst_and_subscription_churn() {
    let (server, _renderer) = start_test_server("GenaChaos").await;
    let server_addr = server.local_addr();
    let desc_url = server.description_url();

    let mock_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock_listener.local_addr().unwrap();
    let flaky_attempts = Arc::new(AtomicUsize::new(0));
    let gone_attempts = Arc::new(AtomicUsize::new(0));
    let drop_attempts = Arc::new(AtomicUsize::new(0));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let mock_task = tokio::spawn({
        let flaky_attempts = Arc::clone(&flaky_attempts);
        let gone_attempts = Arc::clone(&gone_attempts);
        let drop_attempts = Arc::clone(&drop_attempts);
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
                        let drop_attempts = Arc::clone(&drop_attempts);
                        tokio::spawn(async move {
                            let Some(req) = read_full_http_request(&mut stream).await else {
                                return;
                            };
                            let first_line = req.lines().next().unwrap_or("");
                            if first_line.contains("/drop") {
                                drop_attempts.fetch_add(1, Ordering::Relaxed);
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

    let listener = EventListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();
    let dev = OpenHomeDevice::connect(desc_url).await.unwrap();
    let vol_sub_url = dev.event_sub_url(SERVICE_VOLUME_1).unwrap().to_string();
    let mut healthy = listener.subscribe(&vol_sub_url, 60).await.unwrap();
    let init = tokio::time::timeout(Duration::from_secs(2), healthy.recv())
        .await
        .expect("initial SEQ=0 timeout")
        .expect("initial SEQ=0 event");
    assert_eq!(init.seq, 0);

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
    assert!(flaky_attempts.load(Ordering::Relaxed) >= 3);
    assert!(
        drop_attempts.load(Ordering::Relaxed) >= 1,
        "expected /drop peer to receive delivery attempt"
    );

    // /gone returned 412 -> cancelled immediately (exactly 1 attempt even after another mutation).
    let gone_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while gone_attempts.load(Ordering::Relaxed) < 1 && tokio::time::Instant::now() < gone_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(gone_attempts.load(Ordering::Relaxed), 1);
    dev.volume().unwrap().set_volume(60).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(gone_attempts.load(Ordering::Relaxed), 1);

    // > MAX_SUBSCRIPTIONS (256) subscribe/unsubscribe churn proves zero slot leaks.
    for _ in 0..270 {
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
                            let Some(req) = read_full_http_request(&mut stream).await else {
                                return;
                            };
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

    for i in 0..25usize {
        let standby = prod
            .standby()
            .await
            .unwrap_or_else(|e| panic!("sequential call {i} failed on stale keep-alive: {e}"));
        assert!(!standby);
    }

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

// ---------------------------------------------------------------------------
// 9. Non-idempotent POST mid-flight drop executes ONCE (no silent retry)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn http_client_never_retries_non_idempotent_post_on_mid_flight_drop() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let post_hits = Arc::new(AtomicUsize::new(0));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let desc_xml = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <device>
    <deviceType>urn:av-openhome-org:device:Source:1</deviceType>
    <friendlyName>DropMidFlight</friendlyName>
    <UDN>uuid:drop-mid-0001</UDN>
    <serviceList>
      <service>
        <serviceType>{SERVICE_PLAYLIST_1}</serviceType>
        <serviceId>urn:av-openhome-org:serviceId:Playlist</serviceId>
        <SCPDURL>/scpd/Playlist.xml</SCPDURL>
        <controlURL>/ctl/Playlist</controlURL>
        <eventSubURL>/evt/Playlist</eventSubURL>
      </service>
    </serviceList>
  </device>
</root>"#
    );

    let server_task = tokio::spawn({
        let desc_xml = desc_xml.clone();
        let post_hits = Arc::clone(&post_hits);
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
                        let post_hits = Arc::clone(&post_hits);
                        tokio::spawn(async move {
                            let Some(req) = read_full_http_request(&mut stream).await else {
                                return;
                            };
                            if req.starts_with("GET /description.xml") {
                                let header = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/xml; charset=\"utf-8\"\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    desc_xml.len()
                                );
                                let _ = stream.write_all(header.as_bytes()).await;
                                let _ = stream.write_all(desc_xml.as_bytes()).await;
                            } else if req.starts_with("POST /ctl/Playlist") {
                                // Simulate server executing Insert, then crashing before sending response.
                                post_hits.fetch_add(1, Ordering::SeqCst);
                                drop(stream);
                            }
                        });
                    }
                }
            }
        }
    });

    let dev = OpenHomeDevice::connect(&format!("http://{addr}/description.xml"))
        .await
        .unwrap();
    let res = dev
        .playlist()
        .unwrap()
        .insert(0, "http://media/once.flac", "<DIDL-Lite/>")
        .await;
    assert!(res.is_err(), "mid-flight drop must return Err");
    assert_eq!(
        post_hits.load(Ordering::SeqCst),
        1,
        "HttpClient must NOT retry a non-idempotent POST after mid-flight drop"
    );

    let _ = stop_tx.send(true);
    let _ = server_task.await;
}

// ---------------------------------------------------------------------------
// 10. Unicast SongcastReceiver rejects spoofed OhmMessage::Slave packets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn songcast_unicast_receiver_ignores_spoofed_slave_list() {
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        ..SenderConfig::default()
    })
    .await
    .unwrap();

    // Use a UDP observer proxy so we can discover the primary receiver's exact UDP socket addr.
    let proxy = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let proxy_addr = match proxy.local_addr().unwrap() {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let sender_addr = SocketAddr::V4(sender.local_addr());
    let (rx_addr_tx, mut rx_addr_rx) = watch::channel::<Option<SocketAddr>>(None);
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let proxy_task = tokio::spawn({
        let proxy = Arc::clone(&proxy);
        async move {
            let mut client: Option<SocketAddr> = None;
            let mut buf = vec![0u8; 4096];
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
                            if let Some(c) = client {
                                let _ = proxy.send_to(&buf[..len], c).await;
                            }
                        } else {
                            if client.is_none() {
                                client = Some(from);
                                let _ = rx_addr_tx.send(Some(from));
                            }
                            let _ = proxy.send_to(&buf[..len], sender_addr).await;
                        }
                    }
                }
            }
        }
    });

    let mut primary = SongcastReceiver::connect(
        &SongcastUri::Unicast(proxy_addr),
        ReceiverConfig {
            interface: Ipv4Addr::LOCALHOST,
            ..ReceiverConfig::default()
        },
    )
    .await
    .unwrap();

    let primary_udp_addr = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(a) = *rx_addr_rx.borrow() {
                return a;
            }
            rx_addr_rx.changed().await.unwrap();
        }
    })
    .await
    .unwrap();

    // Rogue UDP peer sends a spoofed OhmMessage::Slave pointing at `victim`.
    let rogue = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let victim = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let victim_v4 = match victim.local_addr().unwrap() {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    };
    let spoofed = OhmMessage::Slave(vec![victim_v4]).encode().unwrap();
    rogue.send_to(&spoofed, primary_udp_addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Stream audio from the real sender -> primary receives it, victim must receive NOTHING.
    let fmt = AudioFormat::pcm(48_000, 16, 2);
    for seq in 0..5u32 {
        sender
            .send_audio(&fmt, tagged_pcm(seq), false)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(3)).await;
    }

    let mut got_primary = false;
    let until = tokio::time::Instant::now() + Duration::from_secs(1);
    while !got_primary && tokio::time::Instant::now() < until {
        if let Ok(Some(ReceiverEvent::Audio { .. })) =
            tokio::time::timeout(Duration::from_millis(100), primary.recv()).await
        {
            got_primary = true;
        }
    }
    assert!(
        got_primary,
        "primary should still receive audio from sender"
    );

    let mut vbuf = [0u8; 256];
    let victim_got =
        tokio::time::timeout(Duration::from_millis(60), victim.recv_from(&mut vbuf)).await;
    assert!(
        victim_got.is_err(),
        "victim must NOT receive relayed packets from spoofed OhmMessage::Slave"
    );

    let _ = stop_tx.send(true);
    let _ = proxy_task.await;
    primary.shutdown().await;
    sender.shutdown().await;
}
