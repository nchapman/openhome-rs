//! End-to-end integration tests for `openhome-player` using `openhome-client`,
//! loopback HTTP WAV serving, `SongcastSender`, and `CaptureSink`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openhome_client::OpenHomeDevice;
use openhome_core::types::{ReceiverState, TransportState};
use openhome_endpoint::{
    AudioError, AudioSink, CaptureSink, EndpointConfig, EngineCommand, OpenHomeEndpoint, PcmSpec,
    PlaybackEngine, SinkStream,
};
use openhome_songcast::{AudioFormat, SenderConfig, SongcastSender};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Build a valid 16-bit stereo PCM WAV file with `num_frames` frames.
fn synth_wav(sample_rate: u32, num_frames: usize, amplitude: f32) -> Vec<u8> {
    let channels: u16 = 2;
    let bits: u16 = 16;
    let block_align = channels * (bits / 8);
    let byte_rate = sample_rate * u32::from(block_align);
    let data_bytes = (num_frames * usize::from(block_align)) as u32;
    let mut out = Vec::with_capacity(44 + data_bytes as usize);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_bytes).to_le_bytes());
    out.extend_from_slice(b"WAVE");
    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&channels.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&bits.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_bytes.to_le_bytes());
    for i in 0..num_frames {
        let s = ((i as f32 * 0.08).sin() * amplitude * 30_000.0) as i16;
        out.extend_from_slice(&s.to_le_bytes());
        out.extend_from_slice(&(-s).to_le_bytes());
    }
    out
}

/// Spawn a minimal loopback HTTP/1.1 server serving `/track1.wav`, `/track2.wav`, and `/stream.wav` (no Content-Length).
async fn spawn_wav_http_server(
    wav1: Vec<u8>,
    wav2: Vec<u8>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let wav1 = Arc::new(wav1);
    let wav2 = Arc::new(wav2);

    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let w1 = Arc::clone(&wav1);
            let w2 = Arc::clone(&wav2);
            tokio::spawn(async move {
                let mut req = [0u8; 1024];
                let Ok(n) = sock.read(&mut req).await else {
                    return;
                };
                let first_line = std::str::from_utf8(&req[..n])
                    .unwrap_or("")
                    .lines()
                    .next()
                    .unwrap_or("");
                let path = first_line.split_whitespace().nth(1).unwrap_or("/");
                match path {
                    "/track1.wav" => {
                        let hdr = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: audio/wav\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            w1.len()
                        );
                        let _ = sock.write_all(hdr.as_bytes()).await;
                        let _ = sock.write_all(&w1).await;
                    }
                    "/track2.wav" => {
                        let hdr = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: audio/wav\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            w2.len()
                        );
                        let _ = sock.write_all(hdr.as_bytes()).await;
                        let _ = sock.write_all(&w2).await;
                    }
                    "/stream.wav" => {
                        // Omit Content-Length to exercise StreamingHttpSource path.
                        let hdr = "HTTP/1.1 200 OK\r\nContent-Type: audio/wav\r\nConnection: close\r\n\r\n";
                        let _ = sock.write_all(hdr.as_bytes()).await;
                        let _ = sock.write_all(&w1).await;
                    }
                    _ => {
                        let _ = sock
                            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                            .await;
                    }
                }
            });
        }
    });

    (addr, handle)
}

async fn wait_until<F>(timeout: Duration, mut pred: F) -> bool
where
    F: FnMut() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if pred() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    pred()
}

#[tokio::test]
async fn playlist_http_wav_playback_auto_advance_volume_and_controls() {
    let wav1 = synth_wav(44_100, 44_100 + 4_410, 0.8); // 1.1s at 44.1kHz (exercises Time tick)
    let wav2 = synth_wav(48_000, 4_800, 0.8); // 0.1s at 48kHz
    let (http_addr, http_task) = spawn_wav_http_server(wav1, wav2).await;

    let capture = Arc::new(CaptureSink::new());
    let cfg = EndpointConfig {
        room: "Studio".into(),
        name: "Test Player".into(),
        udn: Some("uuid:openhome-player-test-1".into()),
        bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        advertise_host: Some("127.0.0.1".into()),
        sink: Arc::clone(&capture) as Arc<dyn AudioSink>,
        ..EndpointConfig::default()
    };
    assert!(format!("{cfg:?}").contains("EndpointConfig"));
    let player = OpenHomeEndpoint::start(cfg).await.unwrap();
    assert!(format!("{player:?}").contains("OpenHomeEndpoint"));

    let dev = OpenHomeDevice::connect(player.description_url())
        .await
        .unwrap();
    let product = dev.product().unwrap();
    let playlist = dev.playlist().unwrap();
    let info = dev.info().unwrap();
    let time = dev.time().unwrap();
    let volume = dev.volume().unwrap();
    let receiver = dev.receiver().unwrap();

    assert_eq!(product.product().await.unwrap().room, "Studio");
    assert_eq!(product.source_count().await.unwrap(), 2);
    let attrs = product.attributes().await.unwrap();
    assert!(attrs.info && attrs.time && attrs.volume && attrs.receiver);
    assert!(
        !attrs.sender,
        "Sender must not be advertised without SenderService"
    );
    assert_eq!(product.source_xml_change_count().await.unwrap(), 0);
    let proto = playlist.protocol_info().await.unwrap();
    assert!(!proto.contains("audio/L16"));
    assert!(proto.contains("audio/aiff"));
    assert!(playlist.tracks_max().await.unwrap() > 0);
    assert!(!receiver.protocol_info().await.unwrap().is_empty());
    assert_eq!(receiver.sender().await.unwrap().0, "");
    assert_eq!(info.counters().await.unwrap().track_count, 0);
    assert_eq!(volume.characteristics().await.unwrap().volume_max, 100);
    volume.set_balance(0).await.unwrap();
    volume.set_fade(0).await.unwrap();

    // Security: local file URIs must be rejected by Playlist.Insert by default.
    assert!(
        playlist
            .insert(0, "file:///etc/passwd", "<DIDL-Lite/>")
            .await
            .is_err()
    );
    assert!(
        playlist
            .insert(0, "/etc/passwd", "<DIDL-Lite/>")
            .await
            .is_err()
    );

    // Full volume so decoded samples retain strong amplitude.
    volume.set_volume(100).await.unwrap();
    volume.volume_dec().await.unwrap();
    volume.volume_inc().await.unwrap();
    assert_eq!(volume.volume().await.unwrap(), 100);
    volume.set_mute(false).await.unwrap();

    let url1 = format!("http://{http_addr}/track1.wav");
    let url2 = format!("http://{http_addr}/track2.wav");
    let id1 = playlist.insert(0, &url1, "<DIDL-Lite/>").await.unwrap();
    let id2 = playlist.insert(id1, &url2, "<DIDL-Lite/>").await.unwrap();
    let (token, ids) = playlist.id_array().await.unwrap();
    assert_eq!(ids, vec![id1, id2]);
    assert!(!playlist.id_array_changed(token).await.unwrap());
    assert_eq!(playlist.read(id1).await.unwrap().uri, url1);
    assert_eq!(playlist.read_list(&[id1, id2]).await.unwrap().len(), 2);

    playlist.play().await.unwrap();

    // Wait for both Track 1 (44.1kHz) and auto-advanced Track 2 (48kHz) to finish decoding.
    let finished = wait_until(Duration::from_secs(5), || {
        let frames = capture.frames();
        let has_44k = frames.iter().any(|f| f.spec == PcmSpec::new(44_100, 2, 16));
        let has_48k = frames.iter().any(|f| f.spec == PcmSpec::new(48_000, 2, 16));
        has_44k && has_48k
    })
    .await;
    assert!(
        finished,
        "expected both 44.1kHz and 48kHz PCM frames in CaptureSink"
    );

    // Verify non-zero audio energy was decoded and written.
    let max_amp = capture
        .frames()
        .iter()
        .flat_map(|f| f.samples.iter())
        .fold(0.0f32, |acc, &s| acc.max(s.abs()));
    assert!(
        max_amp > 0.2,
        "expected audible PCM amplitude, got {max_amp}"
    );

    // Wait for player to auto-stop after end of Track 2.
    let mut stopped = false;
    for _ in 0..100 {
        if playlist.transport_state().await.unwrap() == TransportState::Stopped {
            stopped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(stopped, "expected playlist to stop after last track");
    assert_eq!(info.details().await.unwrap().sample_rate, 48_000);
    assert!(time.time().await.unwrap().track_count >= 1);

    // Exercise streaming HTTP source (no Content-Length) + Mute = true -> zeroed samples.
    capture.clear();
    volume.set_mute(true).await.unwrap();
    let url_stream = format!("http://{http_addr}/stream.wav");
    let id3 = playlist
        .insert(id2, &url_stream, "<DIDL-Lite/>")
        .await
        .unwrap();
    playlist.seek_id(id3).await.unwrap();

    let got_muted = wait_until(Duration::from_secs(5), || capture.total_frames() >= 4_410).await;
    assert!(got_muted, "expected streamed WAV frames under mute");
    let muted_max = capture
        .frames()
        .iter()
        .flat_map(|f| f.samples.iter())
        .fold(0.0f32, |acc, &s| acc.max(s.abs()));
    assert_eq!(muted_max, 0.0, "muted output must be silence");

    // Exercise remaining Playlist, Volume & Product controls cleanly.
    assert!(playlist.tracks_max().await.unwrap() >= 100);
    assert_eq!(playlist.read(id3).await.unwrap().id, id3);
    assert_eq!(playlist.read_list(&[id3]).await.unwrap().len(), 1);
    assert_eq!(product.source_index().await.unwrap(), 0);
    let _ = info.counters().await.unwrap();
    volume.set_mute(false).await.unwrap();
    volume.volume_inc().await.unwrap();
    volume.volume_dec().await.unwrap();
    let _ = volume.set_balance(0).await;
    let _ = volume.set_fade(0).await;
    let _ = product.set_source_index_by_name("Playlist").await;
    playlist.set_repeat(true).await.unwrap();
    assert!(playlist.repeat().await.unwrap());
    playlist.set_shuffle(true).await.unwrap();
    assert!(playlist.shuffle().await.unwrap());
    playlist.seek_index(0).await.unwrap();
    playlist.pause().await.unwrap();
    playlist.seek_second_absolute(0).await.unwrap();
    playlist.play().await.unwrap();
    playlist.seek_second_relative(0).await.unwrap();
    playlist.next().await.unwrap();
    playlist.previous().await.unwrap();
    product.set_standby(true).await.unwrap();
    assert!(product.standby().await.unwrap());
    product.set_standby(false).await.unwrap();
    playlist.delete_id(id3).await.unwrap();
    playlist.delete_all().await.unwrap();
    // Play on an empty queue stops cleanly without error.
    playlist.play().await.unwrap();
    assert_eq!(
        playlist.transport_state().await.unwrap(),
        TransportState::Stopped
    );
    assert!(player.local_addr().port() > 0);
    assert!(format!("{:?}", EndpointConfig::default()).contains("EndpointConfig"));

    // Unreachable URI triggers clean error recovery back to Stopped.
    let bad_id = playlist
        .insert(
            0,
            &format!("http://{http_addr}/missing.wav"),
            "<DIDL-Lite/>",
        )
        .await
        .unwrap();
    playlist.seek_id(bad_id).await.unwrap();
    let mut recovered = false;
    for _ in 0..100 {
        if playlist.transport_state().await.unwrap() == TransportState::Stopped {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        recovered,
        "expected clean recovery to Stopped on 404 URI, got {:?}, track={:?}",
        playlist.transport_state().await.unwrap(),
        info.track().await.unwrap(),
    );

    product.set_source_index(1).await.unwrap();
    product.set_standby(true).await.unwrap();
    assert!(product.standby().await.unwrap());

    player.shutdown().await;
    http_task.abort();
}

#[tokio::test]
async fn songcast_receiver_streams_pcm_and_metadata_to_capture_sink() {
    let capture = Arc::new(CaptureSink::new());
    let player = OpenHomeEndpoint::start(EndpointConfig {
        room: "Lounge".into(),
        name: "Songcast Player".into(),
        bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        advertise_host: Some("127.0.0.1".into()),
        songcast_interface: Ipv4Addr::LOCALHOST,
        sink: Arc::clone(&capture) as Arc<dyn openhome_endpoint::AudioSink>,
        ..EndpointConfig::default()
    })
    .await
    .unwrap();

    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        advertise_ip: Some(Ipv4Addr::LOCALHOST),
        ..SenderConfig::default()
    })
    .await
    .unwrap();

    let dev = OpenHomeDevice::connect(player.description_url())
        .await
        .unwrap();
    let receiver = dev.receiver().unwrap();
    let info = dev.info().unwrap();
    let volume = dev.volume().unwrap();
    volume.set_volume(100).await.unwrap();

    let sc_uri = sender.uri().to_string();
    receiver.set_sender(&sc_uri, "<DIDL-Lite/>").await.unwrap();
    receiver.play().await.unwrap();

    // Send Songcast track, metatext, and 16-bit big-endian PCM frames until received.
    let audio_cfg = AudioFormat::pcm(44_100, 16, 2);
    let mut pcm_be = Vec::with_capacity(256 * 4);
    for i in 0..256 {
        let s = ((i as f32 * 0.1).sin() * 20_000.0) as i16;
        pcm_be.extend_from_slice(&s.to_be_bytes());
        pcm_be.extend_from_slice(&(-s).to_be_bytes());
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let _ = sender
            .set_track(
                "http://sender.local/song.flac",
                "<DIDL-Lite>Song</DIDL-Lite>",
            )
            .await;
        let _ = sender.set_metatext("<DIDL-Lite>Live</DIDL-Lite>").await;
        let _ = sender.send_audio(&audio_cfg, pcm_be.clone(), false).await;
        if capture.total_frames() >= 256
            && info.track().await.unwrap().0 == "http://sender.local/song.flac"
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(
        capture.total_frames() >= 256,
        "expected Songcast PCM frames in CaptureSink, got {}",
        capture.total_frames()
    );
    let max_amp = capture
        .frames()
        .iter()
        .flat_map(|f| f.samples.iter())
        .fold(0.0f32, |acc, &s| acc.max(s.abs()));
    assert!(
        max_amp > 0.2,
        "expected audible Songcast PCM, got {max_amp}"
    );
    assert_eq!(
        info.track().await.unwrap().0,
        "http://sender.local/song.flac"
    );
    assert_eq!(
        info.metatext().await.unwrap(),
        "<DIDL-Lite>Live</DIDL-Lite>"
    );

    // Send a Songcast HALT frame -> Receiver transitions to Waiting, then resume at 48 kHz.
    let _ = sender.send_audio(&audio_cfg, Vec::new(), true).await;
    let mut saw_waiting = false;
    for _ in 0..40 {
        if receiver.transport_state().await.unwrap() == ReceiverState::Waiting {
            saw_waiting = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(saw_waiting, "HALT frame must move Receiver to Waiting");

    // Resume at 48 kHz -> forces Songcast sink reopen for new PcmSpec.
    let audio_48k = AudioFormat::pcm(48_000, 16, 2);
    let mut got_48k = false;
    for _ in 0..40 {
        let _ = sender.send_audio(&audio_48k, pcm_be.clone(), false).await;
        if capture
            .frames()
            .iter()
            .any(|f| f.spec == PcmSpec::new(48_000, 2, 16))
        {
            got_48k = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(got_48k, "Songcast format switch to 48kHz must reopen sink");
    assert_eq!(
        receiver.transport_state().await.unwrap(),
        ReceiverState::Playing
    );

    // Send Songcast HALT frame -> Receiver transitions to Waiting, then resumes Playing on audio.
    let _ = sender.send_audio(&audio_cfg, Vec::new(), true).await;
    let mut saw_waiting = false;
    for _ in 0..80 {
        if receiver.transport_state().await.unwrap() == ReceiverState::Waiting {
            saw_waiting = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(15)).await;
    }
    assert!(
        saw_waiting,
        "expected ReceiverState::Waiting on Songcast HALT"
    );

    // Shutting down on current_thread Tokio runtime with active Songcast must NOT deadlock.
    receiver.stop().await.unwrap();
    assert_eq!(
        receiver.transport_state().await.unwrap(),
        ReceiverState::Stopped
    );

    sender.shutdown().await;
    player.shutdown().await;
}

struct FailingSink {
    fail_on_open: bool,
}

impl AudioSink for FailingSink {
    fn name(&self) -> &str {
        "failing"
    }

    fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, AudioError> {
        if self.fail_on_open {
            return Err(AudioError::Sink("device busy".into()));
        }
        Ok(Box::new(FailingStream { spec }))
    }
}

struct FailingStream {
    spec: PcmSpec,
}

impl SinkStream for FailingStream {
    fn spec(&self) -> PcmSpec {
        self.spec
    }

    fn write(&mut self, _samples: &[f32]) -> Result<(), AudioError> {
        Err(AudioError::Sink("write underrun".into()))
    }
}

#[tokio::test]
async fn engine_error_paths_sink_failures_and_drop_cleanup() {
    let wav = synth_wav(44_100, 4_410, 0.5);
    let path = std::env::temp_dir().join(format!(
        "openhome-player-err-test-{}.wav",
        std::process::id()
    ));
    std::fs::write(&path, &wav).unwrap();
    let uri = format!("file://{}", path.display());

    // 1. Failing sink on open() -> EngineEvent::Error (with allow_local_files = true)
    let (engine, mut ev_rx) = PlaybackEngine::spawn_with_options(
        Arc::new(FailingSink { fail_on_open: true }),
        tokio::runtime::Handle::current(),
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        true,
        openhome_core::types::VolumeCharacteristics::default(),
    );
    assert!(format!("{engine:?}").contains("PlaybackEngine"));
    engine.send(EngineCommand::Load {
        generation: 1,
        uri: uri.clone(),
        paused: false,
    });
    let mut saw_open_err = false;
    for _ in 0..5 {
        if let Some(ev) = tokio::time::timeout(Duration::from_secs(2), ev_rx.recv())
            .await
            .unwrap()
            && matches!(ev, openhome_endpoint::EngineEvent::Error { .. })
        {
            saw_open_err = true;
            break;
        }
    }
    assert!(saw_open_err);

    // 2. Invalid Songcast URI -> EngineEvent::Error
    engine.send(EngineCommand::StartSongcast {
        generation: 2,
        uri: "not-a-songcast-uri".into(),
        interface: Ipv4Addr::LOCALHOST,
    });
    let ev = tokio::time::timeout(Duration::from_secs(2), ev_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        ev,
        openhome_endpoint::EngineEvent::Error { generation: 2, .. }
    ));
    engine.shutdown();

    // 3. Failing sink on write() -> EngineEvent::Error
    let (engine2, mut ev_rx2) = PlaybackEngine::spawn_with_options(
        Arc::new(FailingSink {
            fail_on_open: false,
        }),
        tokio::runtime::Handle::current(),
        Arc::new(std::sync::atomic::AtomicU64::new(0)),
        true,
        openhome_core::types::VolumeCharacteristics::default(),
    );
    engine2.send(EngineCommand::Load {
        generation: 1,
        uri: uri.clone(),
        paused: true,
    });
    engine2.send(EngineCommand::Seek(0));
    engine2.send(EngineCommand::Play);
    let mut saw_write_err = false;
    for _ in 0..10 {
        if let Some(ev) = tokio::time::timeout(Duration::from_secs(2), ev_rx2.recv())
            .await
            .unwrap()
            && matches!(ev, openhome_endpoint::EngineEvent::Error { .. })
        {
            saw_write_err = true;
            break;
        }
    }
    assert!(saw_write_err);
    // Invalid Songcast URI emits EngineEvent::Error cleanly without panicking.
    engine2.send(EngineCommand::StartSongcast {
        generation: 4,
        uri: "not-a-valid-songcast-uri".into(),
        interface: Ipv4Addr::LOCALHOST,
    });
    let mut saw_sc_err = false;
    for _ in 0..40 {
        if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(50), ev_rx2.recv()).await
            && matches!(
                ev,
                openhome_endpoint::EngineEvent::Error { generation: 4, .. }
            )
        {
            saw_sc_err = true;
            break;
        }
    }
    assert!(
        saw_sc_err,
        "invalid Songcast URI must emit EngineEvent::Error"
    );

    drop(engine2);

    // 4. OpenHomeEndpoint dropped without calling shutdown() exits cleanly.
    let player = OpenHomeEndpoint::start(EndpointConfig {
        sink: Arc::new(CaptureSink::new()),
        ..EndpointConfig::default()
    })
    .await
    .unwrap();
    drop(player);

    // Exercise superseded Load skip + Load(paused: true) -> Seek -> Play -> Pause -> Stop.
    let gen_ref = Arc::new(std::sync::atomic::AtomicU64::new(1));
    let (cap_engine, mut cap_rx) = PlaybackEngine::spawn_with_options(
        Arc::new(CaptureSink::new()),
        tokio::runtime::Handle::current(),
        Arc::clone(&gen_ref),
        true,
        openhome_core::types::VolumeCharacteristics::default(),
    );
    // Stale generation Load is skipped immediately without touching disk.
    cap_engine.send(EngineCommand::Load {
        generation: 999,
        uri: "/nonexistent/stale.wav".into(),
        paused: false,
    });
    cap_engine.send(EngineCommand::Load {
        generation: 1,
        uri: path.to_string_lossy().into_owned(),
        paused: true,
    });
    let mut saw_paused = false;
    for _ in 0..40 {
        if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(50), cap_rx.recv()).await
            && matches!(
                ev,
                openhome_endpoint::EngineEvent::StateChanged {
                    generation: 1,
                    state: TransportState::Paused
                }
            )
        {
            saw_paused = true;
            break;
        }
    }
    assert!(saw_paused, "Load(paused=true) must emit Paused");
    cap_engine.send(EngineCommand::Seek(0));
    let mut saw_seek_time = false;
    for _ in 0..40 {
        if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(50), cap_rx.recv()).await
            && matches!(
                ev,
                openhome_endpoint::EngineEvent::Time {
                    generation: 1,
                    seconds: 0
                }
            )
        {
            saw_seek_time = true;
            break;
        }
    }
    assert!(saw_seek_time, "Seek(0) must emit Time(0)");
    cap_engine.send(EngineCommand::Play);
    cap_engine.send(EngineCommand::Pause);
    cap_engine.send(EngineCommand::Stop);
    cap_engine.send(EngineCommand::SetVolume(
        openhome_core::types::VolumeState {
            volume: 80,
            mute: false,
            balance: 0,
            fade: 0,
            volume_limit: 100,
        },
    ));
    tokio::task::spawn_blocking(move || cap_engine.shutdown())
        .await
        .unwrap();

    // Dropping OpenHomeEndpoint without calling shutdown() cleans up its background tasks.
    let drop_ep = OpenHomeEndpoint::start(EndpointConfig {
        sink: Arc::new(openhome_endpoint::NullSink::unpaced()),
        ..EndpointConfig::default()
    })
    .await
    .unwrap();
    drop(drop_ep);

    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn paused_next_loads_new_track_and_current_thread_songcast_shutdown_does_not_deadlock() {
    let wav1 = synth_wav(44_100, 44_100, 0.8); // 1.0s at 44.1kHz
    let wav2 = synth_wav(48_000, 4_800, 0.8); // 0.1s at 48kHz
    let (http_addr, http_task) = spawn_wav_http_server(wav1, wav2).await;

    struct PacedCapture(Arc<CaptureSink>);
    impl AudioSink for PacedCapture {
        fn name(&self) -> &str {
            "paced-capture"
        }
        fn open(&self, spec: PcmSpec) -> Result<Box<dyn SinkStream>, AudioError> {
            struct Stream(Box<dyn SinkStream>);
            impl SinkStream for Stream {
                fn spec(&self) -> PcmSpec {
                    self.0.spec()
                }
                fn write(&mut self, samples: &[f32]) -> Result<(), AudioError> {
                    std::thread::sleep(Duration::from_millis(5));
                    self.0.write(samples)
                }
                fn flush(&mut self) {
                    self.0.flush();
                }
            }
            Ok(Box::new(Stream(self.0.open(spec)?)))
        }
    }

    let capture = Arc::new(CaptureSink::new());
    let player = OpenHomeEndpoint::start(EndpointConfig {
        room: "Den".into(),
        name: "State Test".into(),
        bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        advertise_host: Some("127.0.0.1".into()),
        songcast_interface: Ipv4Addr::LOCALHOST,
        sink: Arc::new(PacedCapture(Arc::clone(&capture))),
        ..EndpointConfig::default()
    })
    .await
    .unwrap();

    let dev = OpenHomeDevice::connect(player.description_url())
        .await
        .unwrap();
    let playlist = dev.playlist().unwrap();
    let receiver = dev.receiver().unwrap();

    let id1 = playlist
        .insert(0, &format!("http://{http_addr}/track1.wav"), "<DIDL-Lite/>")
        .await
        .unwrap();
    let _id2 = playlist
        .insert(
            id1,
            &format!("http://{http_addr}/track2.wav"),
            "<DIDL-Lite/>",
        )
        .await
        .unwrap();

    // Seek to track 1, wait for it to start, pause, advance Next while paused, then Play -> must load 48kHz track 2.
    playlist.seek_id(id1).await.unwrap();
    let started_44k = wait_until(Duration::from_secs(3), || {
        capture
            .frames()
            .iter()
            .any(|f| f.spec == PcmSpec::new(44_100, 2, 16))
    })
    .await;
    assert!(started_44k);
    playlist.pause().await.unwrap();
    capture.clear();
    playlist.next().await.unwrap();
    playlist.play().await.unwrap();

    let played_48k = wait_until(Duration::from_secs(5), || {
        capture
            .frames()
            .iter()
            .any(|f| f.spec == PcmSpec::new(48_000, 2, 16))
    })
    .await;
    assert!(
        played_48k,
        "Play after Next-while-Paused must load track 2 (48kHz)"
    );

    // Exercise Previous at index 0 (Seek(0) without reload), DeleteId of currently playing track,
    // and Next on last track (Repeat=false -> Stopped).
    playlist.seek_id(id1).await.unwrap();
    let started_again = wait_until(Duration::from_secs(3), || {
        capture
            .frames()
            .iter()
            .any(|f| f.spec == PcmSpec::new(44_100, 2, 16))
    })
    .await;
    assert!(started_again);
    playlist.previous().await.unwrap();
    playlist.delete_id(id1).await.unwrap();
    playlist.next().await.unwrap();
    assert_eq!(
        playlist.transport_state().await.unwrap(),
        TransportState::Stopped
    );

    // Start active Songcast and test live SetSender reconnect + empty-URI stop + shutdown.
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        advertise_ip: Some(Ipv4Addr::LOCALHOST),
        ..SenderConfig::default()
    })
    .await
    .unwrap();
    let sc_uri = sender.uri().to_string();
    receiver.set_sender(&sc_uri, "<DIDL-Lite/>").await.unwrap();
    receiver.play().await.unwrap();
    // Reconnect while active via SetSender with non-empty URI.
    receiver.set_sender(&sc_uri, "<DIDL-Lite/>").await.unwrap();
    // Empty URI stops active Songcast cleanly.
    receiver.set_sender("", "").await.unwrap();
    assert_eq!(
        receiver.transport_state().await.unwrap(),
        ReceiverState::Stopped
    );
    // Receiver.Play with empty sender URI stops any active Playlist cleanly.
    receiver.play().await.unwrap();
    receiver.set_sender(&sc_uri, "<DIDL-Lite/>").await.unwrap();
    receiver.play().await.unwrap();

    tokio::time::timeout(Duration::from_secs(3), player.shutdown())
        .await
        .expect("shutdown with active Songcast must not deadlock current_thread runtime");
    sender.shutdown().await;
    http_task.abort();
}

#[tokio::test]
async fn songcast_sink_open_and_write_errors_tear_down_without_storm() {
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        advertise_ip: Some(Ipv4Addr::LOCALHOST),
        ..SenderConfig::default()
    })
    .await
    .unwrap();
    let audio_cfg = AudioFormat::pcm(44_100, 16, 2);
    let pcm_be = vec![0x10u8; 256 * 4];

    // 1. FailingSink on open() during Songcast -> single Error, no error storm.
    let (engine, mut ev_rx) = PlaybackEngine::spawn(
        Arc::new(FailingSink { fail_on_open: true }),
        tokio::runtime::Handle::current(),
    );
    engine.send(EngineCommand::StartSongcast {
        generation: 1,
        uri: sender.uri().to_string(),
        interface: Ipv4Addr::LOCALHOST,
    });
    let mut saw_err = false;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        let _ = sender.send_audio(&audio_cfg, pcm_be.clone(), false).await;
        if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(20), ev_rx.recv()).await
            && matches!(
                ev,
                openhome_endpoint::EngineEvent::Error { generation: 1, .. }
            )
        {
            saw_err = true;
            break;
        }
    }
    assert!(
        saw_err,
        "expected EngineEvent::Error on Songcast sink.open() failure"
    );
    // Ensure no second error is spammed on subsequent audio packets.
    let _ = sender.send_audio(&audio_cfg, pcm_be.clone(), false).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), ev_rx.recv())
            .await
            .is_err(),
        "Songcast must not spam errors after teardown"
    );
    engine.shutdown_async().await;

    // 2. FailingSink on write() during Songcast -> Error + teardown.
    let (engine2, mut ev_rx2) = PlaybackEngine::spawn(
        Arc::new(FailingSink {
            fail_on_open: false,
        }),
        tokio::runtime::Handle::current(),
    );
    engine2.send(EngineCommand::StartSongcast {
        generation: 2,
        uri: sender.uri().to_string(),
        interface: Ipv4Addr::LOCALHOST,
    });
    let mut saw_write_err = false;
    let deadline2 = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline2 {
        let _ = sender.send_audio(&audio_cfg, pcm_be.clone(), false).await;
        if let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(20), ev_rx2.recv()).await
            && matches!(
                ev,
                openhome_endpoint::EngineEvent::Error { generation: 2, .. }
            )
        {
            saw_write_err = true;
            break;
        }
    }
    assert!(
        saw_write_err,
        "expected EngineEvent::Error on Songcast sink.write() failure"
    );
    engine2.shutdown_async().await;

    sender.shutdown().await;
}
