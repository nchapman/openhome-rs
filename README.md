# openhome-rs

Pure-Rust implementation of the [OpenHome Media](https://wiki.openhome.org/wiki/OhMediaDevelopers) (`av.openhome.org`) protocol suite — **client (control point)**, **server (renderer)**, and **Songcast (`OHM` / `OHU` / `OHZ`)** synchronized multi-room audio.

## Workspace Crates

| Crate | Purpose |
|---|---|
| `openhome-core` | Zero-network protocol core: service URNs, data models, `IdArray` base64 big-endian codec, `SourceXml` / `TrackList` XML codecs, SOAP envelope/fault codecs, direct-property GENA `<e:propertyset>` codec, and SCPD XML definitions. |
| `openhome-server` | Async OpenHome renderer stack: SSDP responder, HTTP/1.1 SOAP + GENA server, and service traits (`Product`, `Playlist`, `Info`, `Time`, `Volume`, `Receiver`, `Sender`). |
| `openhome-client` | Async OpenHome control point: SSDP discovery, device description parser, typed async service clients, and GENA event subscriber. |
| `openhome-songcast` | Pure-Rust Songcast multi-room audio transport (`OHM` multicast, `OHU` unicast, `OHZ` zone discovery) and clock-pulling synchronization. |
| `openhome-audio` | Protocol-agnostic pure-Rust audio primitives: Symphonia HTTP/local decoding (FLAC/ALAC/MP3/AAC/Vorbis/WAV/AIFF/Ogg), lock-free SPSC ring buffer, channel/rate adapter, dB gain ramp, and `cpal` OS audio output (`CpalSink`). |
| `openhome-endpoint` | Self-contained OpenHome Media + Songcast audio endpoint assembling `openhome-server`, `openhome-songcast`, and `openhome-audio` into a runnable renderer (`cargo run -p openhome-endpoint`). |

## Roadmap

- **Pass 1 — Core OpenHome (`Product:1`, `Playlist:1`, `Info:1`, `Time:1`, `Volume:1`)**:
  Full server + client + GENA eventing + round-trip integration suite validated against `upmpdcli` and Linn's `ohPipeline`.
- **Pass 2 — Songcast (`Sender:1`, `Receiver:1`, `OHM` / `OHU` / `OHZ`)**:
  Synchronized multi-room audio sender and receiver with clock-pulling drift compensation, validated against Linn's `ohSongcast` and `ohPipeline`.

## Quick Check

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Documentation

- [`docs/architecture.md`](docs/architecture.md) — crate boundaries, trait model, and Pass 1 / Pass 2 design
- [`docs/oracle.md`](docs/oracle.md) — reference oracles (`openhome/ohPipeline`, `openhome/ohSongcast`, `upmpdcli`) and interoperability quirks

## License

Apache-2.0 — see [`LICENSE`](LICENSE).
