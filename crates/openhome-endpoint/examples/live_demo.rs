//! Live local end-to-end demo:
//! - Starts `OpenHomeEndpoint` with real CoreAudio/ALSA/WASAPI output (`CpalSink`)
//!   and real SSDP multicast discovery
//! - Serves two gentle-volume stereo WAV tracks over local HTTP
//! - Discovers the endpoint via `openhome-client` SSDP, subscribes to GENA events, and drives
//!   Playlist playback (Play, Volume, Pause, Resume, Seek, Next)
//! - Switches to Songcast Receiver and streams live PCM from `SongcastSender`
//!
//! Run with:
//! ```sh
//! cargo run -p openhome-endpoint --example live_demo
//! ```

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use openhome_client::{DiscoveryOptions, EventListener, OpenHomeDevice, discover};
use openhome_core::{SourceType, urn};
use openhome_endpoint::{CpalSink, EndpointConfig, OpenHomeEndpoint};
use openhome_server::SsdpConfig;
use openhome_songcast::{AudioFormat, SenderConfig, SongcastSender};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn generate_chord_wav(sample_rate: u32, duration_secs: f32, freqs_hz: &[f32]) -> Vec<u8> {
    let channels: u16 = 2;
    let num_frames = (sample_rate as f32 * duration_secs) as usize;
    let data_bytes = num_frames * usize::from(channels) * 2;

    let mut out = Vec::with_capacity(44 + data_bytes);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_bytes as u32).to_le_bytes());
    out.extend_from_slice(b"WAVE");
    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes()); // PCM
    out.extend_from_slice(&channels.to_le_bytes());
    out.extend_from_slice(&sample_rate.to_le_bytes());
    let byte_rate = sample_rate * u32::from(channels) * 2;
    out.extend_from_slice(&byte_rate.to_le_bytes());
    let block_align = channels * 2;
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&(data_bytes as u32).to_le_bytes());

    let amp = 0.25 / (freqs_hz.len().max(1) as f32);
    for frame in 0..num_frames {
        let t = frame as f32 / sample_rate as f32;
        // Gentle 40ms fade-in and fade-out to avoid clicks.
        let fade_in = (t / 0.04).min(1.0);
        let fade_out = ((duration_secs - t) / 0.04).clamp(0.0, 1.0);
        let env = fade_in * fade_out;
        let mut s = 0.0f32;
        for &f in freqs_hz {
            s += (2.0 * std::f32::consts::PI * f * t).sin();
        }
        let sample = (s * amp * env * i16::MAX as f32) as i16;
        out.extend_from_slice(&sample.to_le_bytes());
        out.extend_from_slice(&sample.to_le_bytes());
    }
    out
}

async fn spawn_http_wav_server(
    tracks: Vec<(&'static str, Vec<u8>)>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let tracks = Arc::new(tracks);
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let tracks = Arc::clone(&tracks);
            tokio::spawn(async move {
                let mut req_buf = [0u8; 2048];
                let Ok(n) = stream.read(&mut req_buf).await else {
                    return;
                };
                let req = String::from_utf8_lossy(&req_buf[..n]);
                let path = req
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("/");
                if let Some((_, body)) = tracks.iter().find(|(p, _)| *p == path) {
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: audio/wav\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes()).await;
                    let _ = stream.write_all(body).await;
                } else {
                    let _ = stream
                        .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                        .await;
                }
            });
        }
    });
    (addr, handle)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let room = "OpenHome Live Demo";
    let name = "openhome-rs Demo Endpoint";

    println!("=== 1. Starting OpenHomeEndpoint ===");
    let player = OpenHomeEndpoint::start(EndpointConfig {
        room: room.into(),
        name: name.into(),
        bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        ssdp: SsdpConfig::default(),
        sink: Arc::new(CpalSink::default_device()),
        ..EndpointConfig::default()
    })
    .await?;
    println!("  HTTP server : {}", player.local_addr());
    println!("  Description : {}", player.description_url());

    println!("\n=== 2. Discovering via real SSDP ===");
    let location = match discover(DiscoveryOptions {
        timeout: Duration::from_secs(2),
        ..DiscoveryOptions::default()
    })
    .await
    {
        Ok(discovered) => {
            println!("  Found {} OpenHome device(s) on LAN:", discovered.len());
            for d in &discovered {
                println!("    - udn={} location={}", d.udn, d.location);
            }
            match discovered
                .into_iter()
                .find(|d| d.location == player.description_url())
            {
                Some(d) => {
                    println!("  [OK] Discovered our player over SSDP!");
                    d.location
                }
                None => {
                    println!(
                        "  [note] SSDP multicast didn't loop back; connecting directly to description URL"
                    );
                    player.description_url().to_string()
                }
            }
        }
        Err(e) => {
            println!("  [note] SSDP multicast unavailable from this shell ({e});");
            println!(
                "         (On macOS Sequoia+, grant Local Network permission to your terminal app)"
            );
            println!(
                "         Connecting directly to {}",
                player.description_url()
            );
            player.description_url().to_string()
        }
    };
    let device = OpenHomeDevice::connect(&location).await?;

    let product = device.product()?;
    let playlist = device.playlist()?;
    let volume = device.volume()?;
    let receiver = device.receiver()?;
    let info = device.info()?;
    let time = device.time()?;

    println!("  Attributes  : {:?}", product.attributes().await?);
    println!(
        "  Sources     : {:?}",
        product
            .source_xml()
            .await?
            .iter()
            .map(|s| format!("{} ({:?})", s.name, s.source_type))
            .collect::<Vec<_>>()
    );

    println!("\n=== 3. Subscribing to GENA events (Playlist + Volume) ===");
    let listener = EventListener::bind("0.0.0.0:0".parse()?).await?;
    let mut pl_events = listener
        .subscribe_service(&device, urn::SERVICE_PLAYLIST_1, 300)
        .await?;
    let mut vol_events = listener
        .subscribe_service(&device, urn::SERVICE_VOLUME_1, 300)
        .await?;
    let gena_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(ev) = pl_events.recv() => {
                    println!("  [GENA Playlist seq={}] {:?}", ev.seq, ev.properties);
                }
                Some(ev) = vol_events.recv() => {
                    println!("  [GENA Volume   seq={}] {:?}", ev.seq, ev.properties);
                }
                else => break,
            }
        }
    });

    // Serve two pleasant stereo chords over local HTTP.
    let wav1 = generate_chord_wav(44_100, 3.0, &[261.63, 329.63, 392.00]); // C major
    let wav2 = generate_chord_wav(44_100, 3.0, &[293.66, 369.99, 440.00]); // D major
    let (http_addr, http_task) =
        spawn_http_wav_server(vec![("/c_major.wav", wav1), ("/d_major.wav", wav2)]).await;

    println!("\n=== 4. Playlist Playback (C Major -> Pause -> Resume -> D Major) ===");
    volume.set_volume(70).await?;
    let id1 = playlist
        .insert(
            0,
            &format!("http://{http_addr}/c_major.wav"),
            "<DIDL-Lite><item><dc:title>C Major Chord</dc:title></item></DIDL-Lite>",
        )
        .await?;
    let _id2 = playlist
        .insert(
            id1,
            &format!("http://{http_addr}/d_major.wav"),
            "<DIDL-Lite><item><dc:title>D Major Chord</dc:title></item></DIDL-Lite>",
        )
        .await?;

    playlist.play().await?;
    println!("  Playing Track 1 (C Major)...");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let t = time.time().await?;
    let d = info.details().await?;
    println!(
        "  Time={}/{}s, Details={}Hz {}bit {}bps lossless={} codec={}",
        t.seconds, t.duration, d.sample_rate, d.bit_depth, d.bit_rate, d.lossless, d.codec_name
    );

    println!("  Pausing for 600ms...");
    playlist.pause().await?;
    tokio::time::sleep(Duration::from_millis(600)).await;

    println!("  Resuming + skipping to Track 2 (D Major)...");
    playlist.play().await?;
    playlist.next().await?;
    tokio::time::sleep(Duration::from_millis(1800)).await;
    playlist.stop().await?;

    println!("\n=== 5. Songcast Receiver Live Stream (A Major) ===");
    let sender = SongcastSender::bind(SenderConfig {
        bind_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0),
        advertise_ip: Some(Ipv4Addr::LOCALHOST),
        ..SenderConfig::default()
    })
    .await?;

    // Switch Product source to Receiver and join our local Songcast sender.
    let sources = product.source_xml().await?;
    if let Some((idx, _)) = sources
        .iter()
        .enumerate()
        .find(|(_, s)| s.source_type == SourceType::Receiver)
    {
        product.set_source_index(idx as u32).await?;
    }
    receiver
        .set_sender(&sender.uri().to_string(), "<DIDL-Lite/>")
        .await?;
    receiver.play().await?;
    let _ = sender
        .set_track(
            "songcast://demo",
            "<DIDL-Lite><item><dc:title>Songcast A Major</dc:title></item></DIDL-Lite>",
        )
        .await;

    let fmt = AudioFormat::pcm(44_100, 16, 2);
    // Stream ~1.8s of 440Hz/554.37Hz/659.25Hz (A major) in 220-frame (~5ms) Songcast packets.
    let total_frames = 44_100 * 18 / 10;
    let chunk_frames = 220usize;
    let mut sample_idx = 0usize;
    while sample_idx < total_frames {
        let mut pcm_be = Vec::with_capacity(chunk_frames * 4);
        for i in 0..chunk_frames {
            let t = (sample_idx + i) as f32 / 44_100.0;
            let env = (t / 0.04).min(1.0) * ((1.8 - t) / 0.04).clamp(0.0, 1.0);
            let s = ((2.0 * std::f32::consts::PI * 440.0 * t).sin()
                + (2.0 * std::f32::consts::PI * 554.37 * t).sin()
                + (2.0 * std::f32::consts::PI * 659.25 * t).sin())
                * 0.08
                * env;
            let v = (s * i16::MAX as f32) as i16;
            pcm_be.extend_from_slice(&v.to_be_bytes());
            pcm_be.extend_from_slice(&v.to_be_bytes());
        }
        sender.send_audio(&fmt, pcm_be, false).await?;
        sample_idx += chunk_frames;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    receiver.stop().await?;
    sender.shutdown().await;
    gena_task.abort();
    listener.shutdown().await;
    http_task.abort();
    player.shutdown().await;
    println!("\n=== Done! Clean shutdown. ===");
    Ok(())
}
