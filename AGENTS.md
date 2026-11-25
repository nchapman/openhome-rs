# openhome-rs Guidelines

## Workspace Layout
- `crates/openhome-core`: Zero-network protocol types, XML/SOAP/GENA/IdArray codecs, SCPD XMLs. Keep dependencies minimal (`quick-xml`, `thiserror`).
- `crates/openhome-server`: Async OpenHome renderer (SSDP, HTTP/1.1 SOAP, GENA eventing, service traits).
- `crates/openhome-client`: Async OpenHome control point (SSDP discovery, typed clients, GENA subscriber).
- `crates/openhome-songcast`: Songcast `OHM`/`OHU`/`OHZ` wire protocol, sender/receiver, clock sync.

## Oracles
Consult [`docs/oracle.md`](docs/oracle.md) whenever verifying service XMLs, SOAP fault codes, GENA property names, control-point quirks (Kazoo, Lumin, BubbleUPnP), or Songcast packet layouts.
- Authoritative spec & Songcast reference: `openhome/ohPipeline` + `openhome/ohSongcast`
- Battle-tested interop reference: `medoc92/upmpdcli`

## Verification
Managed via [`mise.toml`](mise.toml). Run before committing:
```sh
mise run check
```
This runs formatting (`fmt:check`), strict clippy (`lint`), rustdoc (`doc`), and tests with per-crate line coverage thresholds (`cov` via [`scripts/coverage.sh`](scripts/coverage.sh): ≥90% workspace total, ≥92% `openhome-core`/`openhome-songcast`, ≥90% `openhome-server`/`openhome-client`).
- Quick local tidy loop: `mise run tidy` (`fmt`, `lint`, `test`).
- HTML / missing-lines coverage views: `mise run cov:html` and `mise run cov:uncovered`.
- `unsafe_code = "forbid"` is enforced workspace-wide.
- Prefer round-trip `openhome-client` ↔ `openhome-server` integration tests over mocked wire internals.
