# Reference Oracles

We use two complementary open-source reference implementations as our protocol oracles:

## 1. Authoritative Spec & Songcast Oracle — Linn OpenHome
- **`https://github.com/openhome/ohPipeline`**
  - Authoritative OpenHome service XML definitions (`OpenHome/Av/ServiceXml/OpenHome/*.xml`)
  - Reference state machines for `Product`, `Playlist`, `Info`, `Time`, `Volume`, `Transport`, `Sender`, and `Receiver` (`OpenHome/Av/`)
- **`https://github.com/openhome/ohSongcast`**
  - Reference `OHM` (multicast), `OHU` (unicast), and `OHZ` wire framing (`Ohm.h`, `OhmMsg.h`, `OhmMsg.cpp`, `OhmProtocolMulticast.cpp`, `OhmProtocolUnicast.cpp`, `OhmSender.cpp`, `OhmReceiver.cpp`)

## 2. Interoperability Oracle — `upmpdcli`
- **`https://framagit.org/medoc92/upmpdcli`** (by Jean-François Dockes)
  - Decade-long battle-tested Linux OpenHome renderer (`src/ohproduct.cxx`, `src/ohplaylist.cxx`, `src/ohinfo.cxx`, `src/ohtime.cxx`, `src/ohvolume.cxx`, `src/ohreceiver.cxx`, `src/ohservice.cxx`, `src/OH*.xml`).
  - Key real-world control-point quirks documented in `upmpdcli`:
    - **Separate UPnP devices**: expose `urn:av-openhome-org:device:Source:1` and `urn:schemas-upnp-org:device:MediaRenderer:1` with distinct UDNs.
    - **Lumin compatibility**: advertise `Product:1` (not `Product:2`) and omit `Credentials` unless explicitly enabled; ignore `Id = -1` in `Playlist.ReadList`.
    - **Kazoo compatibility**: mark `Receiver` source `<Visible>true</Visible>` in `SourceXml`.
    - **BubbleUPnP compatibility**: do not auto-reset `Product.SourceIndex` when `Receiver.Stop` is called.
    - **Playlist faults**: `800` for unknown ID (`Read`, `Insert` with missing `AfterId != 0`, `DeleteId`, `SeekId`), `801` when playlist is full (`TracksMax`), `409` when an inactive source receives transport mutations (except `Play`, which auto-selects `Playlist`).
    - **Volume balance/fade**: accept `0` and reject non-zero with SOAP `402` when balance/fade are unsupported (`BalanceMax = 0`, `FadeMax = 0`).

## Local Clone Helper

```sh
mkdir -p /tmp/openhome-oracle && cd /tmp/openhome-oracle
git clone --depth 1 https://framagit.org/medoc92/upmpdcli.git
git clone --depth 1 https://github.com/openhome/ohPipeline.git
git clone --depth 1 https://github.com/openhome/ohSongcast.git
```
