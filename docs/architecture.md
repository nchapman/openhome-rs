# Architecture

## Crate Boundaries

```text
                    ┌──────────────────┐
                    │  openhome-core   │  (no networking, no async runtime)
                    │  types · xml ·   │
                    │  soap · gena ·   │
                    │  scpd · faults   │
                    └────────┬─────────┘
             ┌───────────────┼───────────────┐
             ▼               ▼               ▼
    ┌─────────────────┐ ┌─────────────────┐ ┌───────────────────┐
    │ openhome-server │ │ openhome-client │ │ openhome-songcast │
    │ SSDP · HTTP     │ │ SSDP · HTTP     │ │ OHM · OHU · OHZ   │
    │ SOAP dispatch   │ │ typed clients   │ │ sender · receiver │
    │ GENA publisher  │ │ GENA subscriber │ │ clock sync        │
    │ service traits  │ │ topology model  │ │                   │
    └─────────────────┘ └─────────────────┘ └───────────────────┘
```

## Wire Protocol Essentials

- **Device type**: `urn:av-openhome-org:device:Source:1` (kept as a separate UPnP device with its own UDN when coexisting with a UPnP AV `MediaRenderer:1` device, matching `upmpdcli`).
- **Services (Pass 1)**:
  - `urn:av-openhome-org:service:Product:1` (`Product:1` for maximum control-point compatibility, including Lumin)
  - `urn:av-openhome-org:service:Playlist:1`
  - `urn:av-openhome-org:service:Info:1`
  - `urn:av-openhome-org:service:Time:1`
  - `urn:av-openhome-org:service:Volume:1`
- **Services (Pass 2 — Songcast)**:
  - `urn:av-openhome-org:service:Receiver:1`
  - `urn:av-openhome-org:service:Sender:1`
  - Optional `urn:av-openhome-org:service:Transport:1`
- **GENA Eventing**: OpenHome services publish direct state-variable properties inside `<e:propertyset xmlns:e="urn:schemas-upnp-org:event-1-0"><e:property><Var>value</Var></e:property>...</e:propertyset>` (not UPnP AV's `<LastChange>` wrapper). Initial `SEQ=0` sends all state variables; subsequent notifications send changed variables only.
- **Data Encodings**:
  - Booleans: `"1"` / `"0"`
  - `IdArray`: base64-encoded sequence of big-endian `u32` track IDs
  - `TrackList`: `<TrackList><Entry><Id>...</Id><Uri>...</Uri><Metadata>...</Metadata></Entry></TrackList>`
  - `SourceXml`: `<SourceList><Source><Name>...</Name><Type>...</Type><Visible>true|false</Visible><SystemName>...</SystemName></Source></SourceList>`

## Pass 2 Songcast Design

`openhome-songcast` implements Linn's documented `OHM` (multicast UDP), `OHU` (unicast UDP), and `OHZ` (zone discovery) protocols:
- Every packet starts with the 8-byte `Ohm` header (`"Ohm "` magic, major version `1`, message type, 16-bit big-endian total length).
- Audio packets carry PCM/encoded frames, sample rate, bit depth, channels, track sample counter, and network timestamp for clock-pulling synchronization.
- `openhome-server` wires `Sender:1` and `Receiver:1` directly to `Product` sources (`Visible = true` on Receiver for Kazoo compatibility; `Stop` on Receiver does not reset `SourceIndex` for BubbleUPnP compatibility).
