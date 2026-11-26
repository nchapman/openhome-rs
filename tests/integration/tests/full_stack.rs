//! Full 4-crate end-to-end integration tests wiring together:
//! - `openhome-core` (protocol types, DIDL/XML, SOAP, GENA)
//! - `openhome-server` (`OpenHomeServer`, `MemoryRenderer`, custom `ReceiverService`/`ProductService`)
//! - `openhome-client` (`OpenHomeDevice`, `Topology`, `GenaListener`, typed service clients)
//! - `openhome-songcast` (`SongcastSender`, `SongcastReceiver`, `ZoneResponder`, `SongcastUri`)

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openhome_client::{EventListener, EventSubscription, OpenHomeDevice, Topology};
use openhome_core::{
    DeviceAttributes, Fault, InfoCounters, ProductInfo, ReceiverState, SenderStatus, Source,
    SourceType, TrackDetails,
};
use openhome_server::{
    ActionResult, InfoService, MemoryRenderer, OpenHomeServer, ProductService, ReceiverService,
    SsdpConfig,
};
use openhome_songcast::{
    AudioFormat, ReceiverConfig, ReceiverEvent, SenderConfig, SongcastReceiver, SongcastSender,
    SongcastUri, ZoneResponder, encode_sender_metadata,
};
use tokio::sync::watch;

// ---------------------------------------------------------------------------
// Test harness: Songcast-backed OpenHome Sender & Receiver rooms
// ---------------------------------------------------------------------------

/// A full OpenHome sender room: `OpenHomeServer` + `SongcastSender` + `ZoneResponder`.
struct SenderRoom {
    server: OpenHomeServer,
    sender: SongcastSender,
    zone_responder: ZoneResponder,
    zone_name: String,
}

impl SenderRoom {
    async fn start(room: &str, name: &str, zone_name: &str) -> Self {
        let sender = SongcastSender::bind(SenderConfig {
            bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            ..SenderConfig::default()
        })
        .await
        .expect("bind SongcastSender");

        let zone_responder = ZoneResponder::bind(
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
            Ipv4Addr::LOCALHOST,
        )
        .await
        .expect("bind ZoneResponder");
        let stream_uri = SongcastUri::Unicast(sender.local_addr());
        zone_responder.set_zone_uri(zone_name, stream_uri.to_string());

        let SocketAddr::V4(ohz_v4) = zone_responder.local_addr() else {
            panic!("expected IPv4 ZoneResponder address");
        };
        let zone_uri = SongcastUri::Zone {
            endpoint: ohz_v4,
            zone: zone_name.to_string(),
        };
        let sender_metadata = encode_sender_metadata(
            &format!("{room}: {name}"),
            &zone_uri.to_string(),
            false,
            None,
        );

        let renderer = MemoryRenderer::new(room, name);
        renderer.set_sender_info("", sender_metadata, true, SenderStatus::Enabled, "Sender");

        let server = renderer
            .into_server_builder()
            .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .advertise_host("127.0.0.1")
            .ssdp(SsdpConfig::disabled())
            .start()
            .await
            .expect("start sender OpenHomeServer");

        Self {
            server,
            sender,
            zone_responder,
            zone_name: zone_name.to_string(),
        }
    }

    async fn shutdown(self) {
        self.server.shutdown().await;
        self.sender.shutdown().await;
        self.zone_responder.shutdown().await;
    }
}

/// Recorded audio frame delivered to a `ReceiverRoom`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedFrame {
    frame: u32,
    sample_rate: u32,
    pcm: Vec<u8>,
}

struct ActiveRx {
    stop_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

/// Bridges OpenHome `Receiver:1`, `Product:1`, and `Info:1` to a live `SongcastReceiver`.
struct SongcastReceiverBridge {
    inner: MemoryRenderer,
    state: Mutex<ReceiverBridgeState>,
    frames: Mutex<Vec<CapturedFrame>>,
    server_ref: Mutex<Option<Arc<OpenHomeServer>>>,
}

struct ReceiverBridgeState {
    transport_state: ReceiverState,
    track_uri: String,
    track_metadata: String,
    track_count: u32,
    metatext: String,
    metatext_count: u32,
    active_rx: Option<ActiveRx>,
}

impl SongcastReceiverBridge {
    fn new(room: &str, name: &str) -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryRenderer::new(room, name),
            state: Mutex::new(ReceiverBridgeState {
                transport_state: ReceiverState::Stopped,
                track_uri: String::new(),
                track_metadata: String::new(),
                track_count: 0,
                metatext: String::new(),
                metatext_count: 0,
                active_rx: None,
            }),
            frames: Mutex::new(Vec::new()),
            server_ref: Mutex::new(None),
        })
    }

    fn attach_server(&self, server: Arc<OpenHomeServer>) {
        *self.server_ref.lock().unwrap() = Some(server);
    }

    fn notify_receiver_and_info(&self) {
        if let Some(srv) = self.server_ref.lock().unwrap().as_ref() {
            srv.notify_receiver();
            srv.notify_info();
        }
    }

    fn stop_active_receiver(&self) -> Option<tokio::task::JoinHandle<()>> {
        let prev = {
            let mut st = self.state.lock().unwrap();
            st.transport_state = ReceiverState::Stopped;
            st.active_rx.take()
        };
        if let Some(active) = prev {
            let _ = active.stop_tx.send(true);
            Some(active.task)
        } else {
            None
        }
    }

    fn captured_frames(&self) -> Vec<CapturedFrame> {
        self.frames.lock().unwrap().clone()
    }

    fn clear_captured_frames(&self) {
        self.frames.lock().unwrap().clear();
    }

    fn start_receiver(self: &Arc<Self>, uri_str: String) -> ActionResult<()> {
        let parsed = SongcastUri::parse(&uri_str)
            .map_err(|e| Fault::action_failed(format!("invalid Songcast URI: {e}")))?;

        let prev_task = self.stop_active_receiver();

        let (stop_tx, mut stop_rx) = watch::channel(false);
        let bridge = Arc::clone(self);
        let task = tokio::spawn(async move {
            // Ensure any previous SongcastReceiver has sent Leave and exited before reconnecting.
            if let Some(prev) = prev_task {
                let _ = prev.await;
            }
            if *stop_rx.borrow() {
                return;
            }

            let cfg = ReceiverConfig {
                interface: Ipv4Addr::LOCALHOST,
                ..ReceiverConfig::default()
            };
            let Ok(mut rx) = SongcastReceiver::connect(&parsed, cfg).await else {
                {
                    let mut st = bridge.state.lock().unwrap();
                    st.transport_state = ReceiverState::Stopped;
                }
                bridge.notify_receiver_and_info();
                return;
            };

            loop {
                tokio::select! {
                    biased;
                    res = stop_rx.changed() => {
                        if res.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    maybe_ev = rx.recv() => {
                        let Some(ev) = maybe_ev else { break };
                        match ev {
                            ReceiverEvent::Audio { frame: audio, .. } => {
                                bridge.frames.lock().unwrap().push(CapturedFrame {
                                    frame: audio.frame,
                                    sample_rate: audio.sample_rate,
                                    pcm: audio.audio,
                                });
                                let became_playing = {
                                    let mut st = bridge.state.lock().unwrap();
                                    if st.transport_state != ReceiverState::Playing {
                                        st.transport_state = ReceiverState::Playing;
                                        true
                                    } else {
                                        false
                                    }
                                };
                                if became_playing {
                                    bridge.notify_receiver_and_info();
                                }
                            }
                            ReceiverEvent::Track(track) => {
                                {
                                    let mut st = bridge.state.lock().unwrap();
                                    st.track_uri = track.uri;
                                    st.track_metadata = track.metadata;
                                    st.track_count = st.track_count.wrapping_add(1);
                                }
                                bridge.notify_receiver_and_info();
                            }
                            ReceiverEvent::Metatext(mt) => {
                                {
                                    let mut st = bridge.state.lock().unwrap();
                                    st.metatext = mt.metatext;
                                    st.metatext_count = st.metatext_count.wrapping_add(1);
                                }
                                bridge.notify_receiver_and_info();
                            }
                            ReceiverEvent::Resynced => {
                                {
                                    let mut st = bridge.state.lock().unwrap();
                                    st.transport_state = ReceiverState::Buffering;
                                }
                                bridge.notify_receiver_and_info();
                            }
                        }
                    }
                }
            }

            rx.shutdown().await;
        });

        {
            let mut st = self.state.lock().unwrap();
            st.transport_state = ReceiverState::Buffering;
            st.active_rx = Some(ActiveRx { stop_tx, task });
        }

        Ok(())
    }
}

impl ProductService for SongcastReceiverBridge {
    fn product_info(&self) -> ProductInfo {
        self.inner.product_info()
    }

    fn attributes(&self) -> DeviceAttributes {
        DeviceAttributes {
            info: true,
            receiver: true,
            ..DeviceAttributes::default()
        }
    }

    fn sources(&self) -> Vec<Source> {
        self.inner.sources()
    }

    fn source_index(&self) -> u32 {
        self.inner.source_index()
    }

    fn set_source_index(&self, index: u32) -> ActionResult<()> {
        let sources = self.inner.sources();
        self.inner.set_source_index(index)?;
        // Switching away from the Receiver source stops active Songcast playback.
        if sources
            .get(index as usize)
            .is_some_and(|s| s.source_type != SourceType::Receiver)
        {
            self.stop_active_receiver();
            self.notify_receiver_and_info();
        }
        Ok(())
    }

    fn standby(&self) -> bool {
        self.inner.standby()
    }

    fn set_standby(&self, standby: bool) -> ActionResult<()> {
        if standby {
            self.stop_active_receiver();
        }
        self.inner.set_standby(standby)
    }
}

impl InfoService for SongcastReceiverBridge {
    fn counters(&self) -> InfoCounters {
        let st = self.state.lock().unwrap();
        InfoCounters {
            track_count: st.track_count,
            details_count: 0,
            metatext_count: st.metatext_count,
        }
    }

    fn track(&self) -> (String, String) {
        let st = self.state.lock().unwrap();
        (st.track_uri.clone(), st.track_metadata.clone())
    }

    fn details(&self) -> TrackDetails {
        self.inner.details()
    }

    fn metatext(&self) -> String {
        self.state.lock().unwrap().metatext.clone()
    }
}

/// Wrapper implementing `ReceiverService` for `Arc<SongcastReceiverBridge>` so `play` can spawn.
struct BridgeReceiverSvc(Arc<SongcastReceiverBridge>);

impl ReceiverService for BridgeReceiverSvc {
    fn play(&self) -> ActionResult<()> {
        let (uri, _) = self.0.inner.sender();
        if uri.is_empty() {
            return Err(Fault::action_failed("no sender URI set"));
        }
        self.0.start_receiver(uri)
    }

    fn stop(&self) -> ActionResult<()> {
        self.0.stop_active_receiver();
        Ok(())
    }

    fn set_sender(&self, uri: String, metadata: String) -> ActionResult<()> {
        self.0.inner.set_sender(uri, metadata)
    }

    fn sender(&self) -> (String, String) {
        self.0.inner.sender()
    }

    fn protocol_info(&self) -> String {
        self.0.inner.protocol_info()
    }

    fn transport_state(&self) -> ReceiverState {
        self.0.state.lock().unwrap().transport_state
    }
}

struct ReceiverRoom {
    server: Arc<OpenHomeServer>,
    bridge: Arc<SongcastReceiverBridge>,
}

impl ReceiverRoom {
    async fn start(room: &str, name: &str) -> Self {
        let bridge = SongcastReceiverBridge::new(room, name);
        let rx_svc = Arc::new(BridgeReceiverSvc(Arc::clone(&bridge)));

        let server = OpenHomeServer::builder(Arc::clone(&bridge) as Arc<dyn ProductService>)
            .info(Arc::clone(&bridge) as Arc<dyn InfoService>)
            .receiver(rx_svc)
            .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .advertise_host("127.0.0.1")
            .ssdp(SsdpConfig::disabled())
            .start()
            .await
            .expect("start receiver OpenHomeServer");

        let server = Arc::new(server);
        bridge.attach_server(Arc::clone(&server));
        Self { server, bridge }
    }

    async fn shutdown(self) {
        if let Some(task) = self.bridge.stop_active_receiver() {
            let _ = task.await;
        }
        *self.bridge.server_ref.lock().unwrap() = None;
        Arc::try_unwrap(self.server)
            .ok()
            .expect("no outstanding Arc<OpenHomeServer> refs")
            .shutdown()
            .await;
    }
}

/// Extract the first `<res ...>URI</res>` value from DIDL-Lite sender metadata.
fn extract_didl_res_uri(didl: &str) -> String {
    let res_start = didl.find("<res").expect("DIDL contains <res>");
    let gt = didl[res_start..].find('>').expect("<res> closing >") + res_start + 1;
    let end = didl[gt..].find("</res>").expect("</res>") + gt;
    didl[gt..end].trim().to_string()
}

async fn wait_for_event<F>(sub: &mut EventSubscription, timeout: Duration, mut pred: F)
where
    F: FnMut(&std::collections::BTreeMap<String, String>) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for GENA event");
        let ev = tokio::time::timeout(remaining, sub.recv())
            .await
            .expect("GENA recv timed out")
            .expect("GENA subscription closed unexpectedly");
        if pred(&ev.properties) {
            return;
        }
    }
}

async fn wait_until<F>(timeout: Duration, mut pred: F)
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if pred() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    panic!("condition not satisfied within {timeout:?}");
}

// ---------------------------------------------------------------------------
// Test 1: Full Multi-Room Grouping (Topology -> OHZ -> Primary + Slave -> Promotion)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiroom_songcast_grouping_primary_slave_relay_and_promotion() {
    let room_a = SenderRoom::start("Living Room", "Linn Klimax", "living-room-zone").await;
    let room_b = ReceiverRoom::start("Kitchen", "Linn Majik").await;
    let room_c = ReceiverRoom::start("Study", "Linn Sneaky").await;

    // 1. Control point connects to all 3 rooms and inspects Topology.
    let dev_a = OpenHomeDevice::connect(room_a.server.description_url())
        .await
        .unwrap();
    let dev_b = OpenHomeDevice::connect(room_b.server.description_url())
        .await
        .unwrap();
    let dev_c = OpenHomeDevice::connect(room_c.server.description_url())
        .await
        .unwrap();

    let topo = Topology::from_devices(&[dev_a.clone(), dev_b.clone(), dev_c.clone()])
        .await
        .unwrap();
    assert_eq!(topo.room_count(), 3);
    assert_eq!(topo.senders().count(), 1);
    assert_eq!(topo.senders().next().unwrap().room, "Living Room");
    let receiver_rooms: Vec<&str> = topo.receivers().map(|d| d.room.as_str()).collect();
    assert!(receiver_rooms.contains(&"Kitchen"));
    assert!(receiver_rooms.contains(&"Study"));

    // 2. Subscribe to GENA events on Room B's Receiver and Info services.
    let listener = EventListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();
    let mut rx_sub = listener
        .subscribe_service(&dev_b, openhome_core::urn::SERVICE_RECEIVER_1, 300)
        .await
        .unwrap();
    let mut info_sub = listener
        .subscribe_service(&dev_b, openhome_core::urn::SERVICE_INFO_1, 300)
        .await
        .unwrap();

    // Drain initial SEQ=0 snapshots.
    wait_for_event(&mut rx_sub, Duration::from_secs(2), |p| {
        p.get("TransportState").map(String::as_str) == Some("Stopped")
    })
    .await;
    wait_for_event(&mut info_sub, Duration::from_secs(2), |p| {
        p.contains_key("TrackCount")
    })
    .await;

    // 3. Control point reads Sender:1::Metadata() from Room A and groups Room B & C.
    let sender_svc = dev_a.sender().unwrap();
    let sender_meta = sender_svc.metadata().await.unwrap();
    let songcast_uri = extract_didl_res_uri(&sender_meta);
    assert!(
        songcast_uri.starts_with("ohz://") && songcast_uri.contains(&room_a.zone_name),
        "unexpected sender URI: {songcast_uri}"
    );

    // Switch Room B to Receiver source, SetSender, Play -> joins as primary.
    dev_b
        .product()
        .unwrap()
        .set_source_by_system_name("Receiver")
        .await
        .unwrap();
    let rx_b = dev_b.receiver().unwrap();
    rx_b.set_sender(&songcast_uri, &sender_meta).await.unwrap();
    rx_b.play().await.unwrap();

    wait_until(Duration::from_secs(3), || {
        room_a.sender.active_receivers() == 1
    })
    .await;

    // Switch Room C to Receiver source, SetSender, Play -> joins as slave relayed by Room B!
    dev_c
        .product()
        .unwrap()
        .set_source_by_system_name("Receiver")
        .await
        .unwrap();
    let rx_c = dev_c.receiver().unwrap();
    rx_c.set_sender(&songcast_uri, &sender_meta).await.unwrap();
    rx_c.play().await.unwrap();

    wait_until(Duration::from_secs(3), || {
        room_a.sender.active_receivers() == 2
    })
    .await;

    // 4. Room A updates track/metatext and streams 4 PCM frames.
    room_a
        .sender
        .set_track("http://media.local/album/01.flac", "<DIDL-Lite/>")
        .await
        .unwrap();
    room_a
        .sender
        .set_metatext("Miles Davis - So What")
        .await
        .unwrap();

    let fmt = AudioFormat::pcm(48_000, 16, 2);
    let expected_pcms: Vec<Vec<u8>> = (0..4u8).map(|i| vec![0x10 + i; 192]).collect();
    for pcm in &expected_pcms {
        room_a
            .sender
            .send_audio(&fmt, pcm.clone(), false)
            .await
            .unwrap();
    }

    // Both Room B (primary) and Room C (slave via primary relay) receive bit-exact PCM!
    wait_until(Duration::from_secs(3), || {
        room_b.bridge.captured_frames().len() >= 4 && room_c.bridge.captured_frames().len() >= 4
    })
    .await;

    let got_b: Vec<Vec<u8>> = room_b
        .bridge
        .captured_frames()
        .into_iter()
        .take(4)
        .map(|f| f.pcm)
        .collect();
    let got_c: Vec<Vec<u8>> = room_c
        .bridge
        .captured_frames()
        .into_iter()
        .take(4)
        .map(|f| f.pcm)
        .collect();
    assert_eq!(got_b, expected_pcms);
    assert_eq!(got_c, expected_pcms);

    // Control point observes Playing + Track/Metatext via GENA on Room B.
    wait_for_event(&mut rx_sub, Duration::from_secs(2), |p| {
        p.get("TransportState").map(String::as_str) == Some("Playing")
    })
    .await;
    wait_for_event(&mut info_sub, Duration::from_secs(2), |p| {
        p.get("Metatext").map(String::as_str) == Some("Miles Davis - So What")
    })
    .await;

    // 5. Ungroup Room B (primary): Room B leaves & emits Stopped; Room C is promoted to primary!
    rx_b.stop().await.unwrap();
    wait_for_event(&mut rx_sub, Duration::from_secs(2), |p| {
        p.get("TransportState").map(String::as_str) == Some("Stopped")
    })
    .await;
    wait_until(Duration::from_secs(3), || {
        room_a.sender.active_receivers() == 1
    })
    .await;

    room_c.bridge.clear_captured_frames();
    let promoted_pcm = vec![0xEE; 192];
    room_a
        .sender
        .send_audio(&fmt, promoted_pcm.clone(), false)
        .await
        .unwrap();

    wait_until(Duration::from_secs(3), || {
        !room_c.bridge.captured_frames().is_empty()
    })
    .await;
    assert_eq!(room_c.bridge.captured_frames()[0].pcm, promoted_pcm);

    listener.shutdown().await;
    room_c.shutdown().await;
    room_b.shutdown().await;
    room_a.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test 2: Track Change & Stream Interruption Propagate Through Songcast & GENA
// ---------------------------------------------------------------------------

#[tokio::test]
async fn track_change_and_stream_interruption_propagate_through_songcast_and_gena() {
    let room_a = SenderRoom::start("Lounge", "Sender", "lounge-zone").await;
    let room_b = ReceiverRoom::start("Bedroom", "Receiver").await;

    let dev_a = OpenHomeDevice::connect(room_a.server.description_url())
        .await
        .unwrap();
    let dev_b = OpenHomeDevice::connect(room_b.server.description_url())
        .await
        .unwrap();

    let listener = EventListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();
    let mut info_sub = listener
        .subscribe_service(&dev_b, openhome_core::urn::SERVICE_INFO_1, 300)
        .await
        .unwrap();
    wait_for_event(&mut info_sub, Duration::from_secs(2), |p| {
        p.contains_key("TrackCount")
    })
    .await;

    let sender_meta = dev_a.sender().unwrap().metadata().await.unwrap();
    let uri = extract_didl_res_uri(&sender_meta);
    let rx_b = dev_b.receiver().unwrap();
    rx_b.set_sender(&uri, &sender_meta).await.unwrap();
    rx_b.play().await.unwrap();

    wait_until(Duration::from_secs(3), || {
        room_a.sender.active_receivers() == 1
    })
    .await;

    // Stream Track 1 at 44.1 kHz.
    room_a
        .sender
        .set_track("http://media.local/01.flac", "<DIDL-Lite>T1</DIDL-Lite>")
        .await
        .unwrap();
    let fmt_44k = AudioFormat::pcm(44_100, 16, 2);
    room_a
        .sender
        .send_audio(&fmt_44k, vec![0x11; 192], false)
        .await
        .unwrap();

    wait_until(Duration::from_secs(3), || {
        room_b
            .bridge
            .captured_frames()
            .iter()
            .any(|f| f.sample_rate == 44_100)
    })
    .await;

    // Interrupt stream, switch to Track 2 at 48 kHz, and update metatext.
    room_a.sender.stream_interrupted();
    room_a
        .sender
        .set_track("http://media.local/02.flac", "<DIDL-Lite>T2</DIDL-Lite>")
        .await
        .unwrap();
    room_a
        .sender
        .set_metatext("Track 2 - Hi-Res")
        .await
        .unwrap();
    let fmt_48k = AudioFormat::pcm(48_000, 24, 2);
    room_a
        .sender
        .send_audio(&fmt_48k, vec![0x22; 288], false)
        .await
        .unwrap();

    wait_until(Duration::from_secs(3), || {
        room_b
            .bridge
            .captured_frames()
            .iter()
            .any(|f| f.sample_rate == 48_000 && f.pcm == vec![0x22; 288])
    })
    .await;

    wait_for_event(&mut info_sub, Duration::from_secs(2), |p| {
        p.get("Uri").map(String::as_str) == Some("http://media.local/02.flac")
            || p.get("Metatext").map(String::as_str) == Some("Track 2 - Hi-Res")
    })
    .await;

    let (track_uri, _) = dev_b.info().unwrap().track().await.unwrap();
    assert_eq!(track_uri, "http://media.local/02.flac");
    assert_eq!(
        dev_b.info().unwrap().metatext().await.unwrap(),
        "Track 2 - Hi-Res"
    );

    listener.shutdown().await;
    room_b.shutdown().await;
    room_a.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test 3: Source Switch Away From Receiver Stops Songcast & Rejoin Resumes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn source_switch_away_from_receiver_stops_songcast_and_rejoin_resumes() {
    let room_a = SenderRoom::start("Studio", "Master", "studio-zone").await;
    let room_b = ReceiverRoom::start("Patio", "Slave").await;

    let dev_a = OpenHomeDevice::connect(room_a.server.description_url())
        .await
        .unwrap();
    let dev_b = OpenHomeDevice::connect(room_b.server.description_url())
        .await
        .unwrap();

    let sender_meta = dev_a.sender().unwrap().metadata().await.unwrap();
    let uri = extract_didl_res_uri(&sender_meta);

    let prod_b = dev_b.product().unwrap();
    let rx_b = dev_b.receiver().unwrap();
    prod_b.set_source_by_system_name("Receiver").await.unwrap();
    rx_b.set_sender(&uri, &sender_meta).await.unwrap();
    rx_b.play().await.unwrap();

    wait_until(Duration::from_secs(3), || {
        room_a.sender.active_receivers() == 1
    })
    .await;

    // Switching Product source to Playlist must stop SongcastReceiver and send Leave.
    prod_b.set_source_by_system_name("Playlist").await.unwrap();
    wait_until(Duration::from_secs(3), || {
        room_a.sender.active_receivers() == 0
    })
    .await;
    assert_eq!(
        rx_b.transport_state().await.unwrap(),
        ReceiverState::Stopped
    );

    // Switching back to Receiver and calling Play rejoins and resumes audio.
    prod_b.set_source_by_system_name("Receiver").await.unwrap();
    rx_b.play().await.unwrap();
    wait_until(Duration::from_secs(3), || {
        room_a.sender.active_receivers() == 1
    })
    .await;

    room_b.bridge.clear_captured_frames();
    let fmt = AudioFormat::pcm(48_000, 16, 2);
    let pcm = vec![0x77; 192];
    room_a
        .sender
        .send_audio(&fmt, pcm.clone(), false)
        .await
        .unwrap();
    wait_until(Duration::from_secs(3), || {
        room_b.bridge.captured_frames().iter().any(|f| f.pcm == pcm)
    })
    .await;

    room_b.shutdown().await;
    room_a.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test 4: Multi-Controller Concurrent Mutations & Keep-Alive GENA Fan-Out
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_controller_concurrent_mutations_and_gena_fanout() {
    let renderer = MemoryRenderer::new("Main Room", "Reference");
    let server = renderer
        .into_server_builder()
        .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .advertise_host("127.0.0.1")
        .ssdp(SsdpConfig::disabled())
        .start()
        .await
        .unwrap();

    // Two independent control points with their own GENA listeners & HTTP keep-alive pools.
    let cp1 = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();
    let cp2 = OpenHomeDevice::connect(server.description_url())
        .await
        .unwrap();

    let listener1 = EventListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();
    let listener2 = EventListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .await
        .unwrap();

    let mut vol_sub1 = listener1
        .subscribe_service(&cp1, openhome_core::urn::SERVICE_VOLUME_1, 300)
        .await
        .unwrap();
    let mut vol_sub2 = listener2
        .subscribe_service(&cp2, openhome_core::urn::SERVICE_VOLUME_1, 300)
        .await
        .unwrap();
    let mut pl_sub2 = listener2
        .subscribe_service(&cp2, openhome_core::urn::SERVICE_PLAYLIST_1, 300)
        .await
        .unwrap();

    // Drain initial SEQ=0 events.
    wait_for_event(&mut vol_sub1, Duration::from_secs(2), |p| {
        p.contains_key("Volume")
    })
    .await;
    wait_for_event(&mut vol_sub2, Duration::from_secs(2), |p| {
        p.contains_key("Volume")
    })
    .await;
    wait_for_event(&mut pl_sub2, Duration::from_secs(2), |p| {
        p.contains_key("IdArray")
    })
    .await;

    // Concurrent playlist insertions from CP1 & CP2 + volume mutations.
    let pl1 = cp1.playlist().unwrap();
    let pl2 = cp2.playlist().unwrap();
    let vol1 = cp1.volume().unwrap();
    let vol2 = cp2.volume().unwrap();

    let (r1, r2, r3, r4) = tokio::join!(
        async {
            let mut ids = Vec::new();
            for i in 0..5 {
                let id = pl1
                    .insert(0, &format!("http://cp1/{i}.flac"), "")
                    .await
                    .unwrap();
                ids.push(id);
            }
            ids
        },
        async {
            let mut ids = Vec::new();
            for i in 0..5 {
                let id = pl2
                    .insert(0, &format!("http://cp2/{i}.flac"), "")
                    .await
                    .unwrap();
                ids.push(id);
            }
            ids
        },
        async {
            for v in [10, 20, 30, 40] {
                vol1.set_volume(v).await.unwrap();
            }
        },
        async {
            vol2.set_mute(true).await.unwrap();
            vol2.set_mute(false).await.unwrap();
        }
    );
    let _ = (r3, r4);
    assert_eq!(r1.len(), 5);
    assert_eq!(r2.len(), 5);

    // Final playlist has all 10 tracks with unique IDs.
    let (_token, all_ids) = pl1.id_array().await.unwrap();
    assert_eq!(all_ids.len(), 10);

    // Set a distinct final volume and confirm BOTH controllers observe it via GENA.
    vol1.set_volume(67).await.unwrap();
    wait_for_event(&mut vol_sub1, Duration::from_secs(2), |p| {
        p.get("Volume").map(String::as_str) == Some("67")
    })
    .await;
    wait_for_event(&mut vol_sub2, Duration::from_secs(2), |p| {
        p.get("Volume").map(String::as_str) == Some("67")
    })
    .await;

    // CP2 also observed playlist IdArray updates via GENA.
    wait_for_event(&mut pl_sub2, Duration::from_secs(2), |p| {
        p.get("IdArray")
            .and_then(|b64| openhome_core::id_array::decode_id_array(b64).ok())
            .is_some_and(|ids| ids.len() == 10)
    })
    .await;

    listener1.shutdown().await;
    listener2.shutdown().await;
    server.shutdown().await;
}
