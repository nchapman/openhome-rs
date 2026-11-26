# openhome-rs

[![CI](https://github.com/nchapman/openhome-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/nchapman/openhome-rs/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![MSRV: 1.89](https://img.shields.io/badge/MSRV-1.89-orange.svg)](Cargo.toml)

A pure-Rust implementation of the [OpenHome Media](https://wiki.openhome.org/wiki/OhMediaDevelopers) (`av.openhome.org`) protocol suite and **Songcast** synchronized multi-room audio — including a protocol-agnostic audio engine and a ready-to-run network renderer.

Build OpenHome control points, embed a compliant renderer into your own audio engine, or run a standalone multi-room speaker that works out of the box with third-party OpenHome apps (Linn, Kazoo, Lumin, BubbleUPnP).

## Why openhome-rs?

- **100% safe, pure Rust** — `#![forbid(unsafe_code)]` enforced workspace-wide; no C++ `ohNet`/`ohPipeline` FFI required.
- **Modular by design** — Use the zero-network protocol core (`openhome-core`), the audio-engine-agnostic server/client/Songcast crates, the protocol-agnostic audio crate (`openhome-audio`), or the batteries-included endpoint (`openhome-endpoint`).
- **Multi-room Songcast built in** — Supports `OHM` multicast, `OHU` unicast (with primary→slave relay and promotion), `OHZ` zone discovery, `playout_at` pre-buffering, and `skew_ppm` clock-drift compensation.
- **Oracle-verified interop** — Cross-checked against Linn's `openhome/ohPipeline`, `openhome/ohSongcast`, and `medoc92/upmpdcli`, including real-world control-point quirks.

## Prerequisites & Platforms

- **Rust**: `1.89+` (Edition 2024)
- **macOS**: CoreAudio (works out of the box)
- **Linux**: ALSA development headers for `cpal` output:
  ```sh
  sudo apt-get install -y libasound2-dev pkg-config
  ```
- **Windows**: WASAPI (via `cpal`)

## Quick Start

### 1. Run a standalone OpenHome + Songcast endpoint

```sh
git clone https://github.com/nchapman/openhome-rs.git
cd openhome-rs
cargo run -p openhome-endpoint -- --room "Living Room" --name "OpenHome Endpoint"
```

Expected output:

```text
openhome-endpoint listening on 0.0.0.0:54321 (room="Living Room", name="OpenHome Endpoint", description=http://192.168.1.42:54321/description.xml, sender=Some("ohu://192.168.1.42:54322"))
```

Your endpoint is now discoverable over SSDP by OpenHome control points (Linn, Kazoo, Lumin, BubbleUPnP) and ready to play HTTP streams (`FLAC`, `ALAC`, `MP3`, `AAC`, `Vorbis`, `WAV`, `AIFF`, `Ogg`) or act as a Songcast multi-room leader/follower.

#### CLI Options (`openhome-endpoint`)

| Flag | Default | Description |
|---|---|---|
| `--room NAME` | `"Living Room"` | OpenHome room name advertised via `Product:1` |
| `--name NAME` | `"OpenHome Endpoint"` | Renderer device name |
| `--udn UDN` | Random UUID v4 | Stable UPnP UDN (e.g. `uuid:...`) |
| `--bind IP:PORT` | `0.0.0.0:0` | HTTP/SOAP/GENA bind address |
| `--advertise-host HOST` | Auto-detected outbound IPv4 | Host/IP advertised in SSDP & `description.xml` |
| `--songcast-interface IPV4` | `0.0.0.0` | Local IPv4 interface for Songcast UDP sockets |
| `--device default\|null\|NAME` | `default` | Audio output: OS default (`cpal`), silent `null`, or named `cpal` device |
| `--no-ssdp` | `false` | Disable SSDP multicast responder |
| `--no-sender` | `false` | Disable OpenHome `Sender:1` multi-room leader rebroadcast |

### 2. Embed an endpoint in Rust

Add `openhome-endpoint` and `tokio` to your `Cargo.toml`, then start an endpoint with either real OS audio (`CpalSink`) or a custom `AudioSink`:

```rust
use std::sync::Arc;
use openhome_endpoint::{CpalSink, EndpointConfig, OpenHomeEndpoint};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = OpenHomeEndpoint::start(EndpointConfig {
        room: "Study".into(),
        name: "Desk DAC".into(),
        sink: Arc::new(CpalSink::default_device()),
        ..EndpointConfig::default()
    })
    .await?;

    println!("Description URL: {}", endpoint.description_url());
    println!("Songcast Sender: {:?}", endpoint.sender_uri());

    tokio::signal::ctrl_c().await?;
    endpoint.shutdown().await;
    Ok(())
}
```

### 3. Discover and control OpenHome renderers

```rust
use openhome_client::{DiscoveryOptions, OpenHomeDevice, discover};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let devices = discover(DiscoveryOptions::default()).await?;
    for found in devices {
        let dev = OpenHomeDevice::connect(&found.location).await?;
        let details = dev.product()?.product().await?;
        println!("Found room {:?} ({}) at {}", details.room, details.name, found.location);

        if let Ok(playlist) = dev.playlist() {
            let state = playlist.transport_state().await?;
            println!("  Playlist state: {state:?}");
        }
    }
    Ok(())
}
```

## Workspace Crates

| Crate | Role | Key Features |
|---|---|---|
| [`openhome-core`](crates/openhome-core) | Zero-network protocol core | Service URNs, data models, `IdArray` base64 codec, `SourceXml` / `TrackList` XML, SOAP envelopes/faults, GENA `<e:propertyset>`, SCPD XMLs (`quick-xml` + `thiserror` only). |
| [`openhome-server`](crates/openhome-server) | Async OpenHome renderer | SSDP responder, HTTP/1.1 SOAP + GENA server, `MemoryRenderer`, and traits for `Product`, `Playlist`, `Info`, `Time`, `Volume`, `Receiver`, and `Sender`. |
| [`openhome-client`](crates/openhome-client) | Async control point | SSDP discovery, device XML parser, typed async clients for all 7 services, GENA event subscriber with auto-renewal, and room/source topology model. |
| [`openhome-songcast`](crates/openhome-songcast) | Multi-room transport | `OHM` multicast, `OHU` unicast + primary/slave relay, `OHZ` zone discovery, reorder/resend buffer, and `ClockSync` skew estimation. |
| [`openhome-audio`](crates/openhome-audio) | Protocol-agnostic audio | Symphonia HTTP Range & local decoding, lock-free SPSC ring buffer, channel/rate/ppm adapter, smooth dB gain ramp, `CpalSink`, `NullSink`, and `CaptureSink`. Zero OpenHome or Tokio dependencies. |
| [`openhome-endpoint`](crates/openhome-endpoint) | Runnable renderer | Wires `openhome-server`, `openhome-songcast`, and `openhome-audio` into `OpenHomeEndpoint` and the `openhome-endpoint` CLI binary. |

## Scope & Known Limitations

- **Unicast `Sender.Metadata` by default**: `OpenHomeEndpoint` advertises a direct unicast `ohu://<ip>:<port>` URI in `Sender.Metadata` rather than an `ohz://239.255.255.250:51972/<UDN>` zone indirection URI (an `OHZ` `ZoneResponder` is available in `openhome-songcast` if you need multicast zone discovery).
- **No `Sender.Status::Blocked` multicast conflict detection**: Unicast sender mode reports `Enabled` / `Disabled`.
- **No `Credentials:1` / cloud streaming plugins**: Playback targets HTTP(S) audio streams and Songcast; `file://` playback is disabled by default (`EndpointConfig::allow_local_files`).
- **PCM audio only**: Decodes PCM formats supported by Symphonia; native DSD / DoP passthrough is not implemented.

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — crate boundaries, trait model, and protocol design
- [`docs/oracle.md`](docs/oracle.md) — reference oracles (`openhome/ohPipeline`, `openhome/ohSongcast`, `upmpdcli`) and interoperability quirks

## Development & Contributing

Contributions, bug reports, and interoperability test reports (especially with hardware renderers or third-party control points) are welcome via [GitHub Issues](https://github.com/nchapman/openhome-rs/issues) and Pull Requests.

Run the full verification suite before opening a PR:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test --workspace
./scripts/coverage.sh
```

Or with [`mise`](https://mise.jdx.dev/): `mise run check`.

## License

Licensed under the Apache License, Version 2.0 — see [`LICENSE`](LICENSE).
