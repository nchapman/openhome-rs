//! OpenHome room/source topology model inspired by `openhome/ohTopology`.
//!
//! Aggregates one or more [`OpenHomeDevice`] instances by their `Product.Room`
//! so control points can display rooms, active sources, standby state, and
//! Songcast Sender/Receiver capabilities.

use std::collections::BTreeMap;
use std::sync::Arc;

use openhome_core::{DeviceAttributes, Source};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::device::OpenHomeDevice;
use crate::error::ClientError;

const MAX_CONCURRENT_DEVICES: usize = 16;

/// Snapshot of a single OpenHome device within a room topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyDevice {
    pub udn: String,
    pub location: String,
    pub friendly_name: String,
    pub room: String,
    pub product_name: String,
    pub standby: bool,
    pub sources: Vec<Source>,
    pub source_index: u32,
    pub attributes: DeviceAttributes,
}

impl TopologyDevice {
    /// Currently selected [`Source`], if `source_index` is in bounds.
    pub fn active_source(&self) -> Option<&Source> {
        self.sources.get(self.source_index as usize)
    }

    /// Visible sources (respecting each source's `<Visible>` flag).
    pub fn visible_sources(&self) -> impl Iterator<Item = (usize, &Source)> {
        self.sources
            .iter()
            .enumerate()
            .filter(|(_, src)| src.visible)
    }
}

/// An OpenHome room grouping all devices sharing the same `Product.Room` name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomTopology {
    pub name: String,
    pub devices: Vec<TopologyDevice>,
}

/// Snapshot of all rooms and devices discovered on the network.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Topology {
    rooms: BTreeMap<String, RoomTopology>,
}

impl Topology {
    /// Query `Product:1` metadata on every device in `devices` concurrently and group them by room.
    /// Returns an error if any device fails; use [`Self::from_devices_lossy`] to tolerate offline rooms.
    pub async fn from_devices(devices: &[OpenHomeDevice]) -> Result<Self, ClientError> {
        let (topo, errors) = Self::from_devices_lossy(devices).await;
        if let Some((_, err)) = errors.into_iter().next() {
            return Err(err);
        }
        Ok(topo)
    }

    /// Query `Product:1` metadata on every device concurrently, returning the partial
    /// [`Topology`] for healthy rooms alongside `(udn, error)` pairs for any unreachable devices.
    pub async fn from_devices_lossy(
        devices: &[OpenHomeDevice],
    ) -> (Self, Vec<(String, ClientError)>) {
        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_DEVICES));
        let mut set = JoinSet::new();

        for (idx, dev) in devices.iter().cloned().enumerate() {
            let sem = sem.clone();
            set.spawn(async move {
                let udn = dev.udn().to_string();
                let res: Result<TopologyDevice, ClientError> = async {
                    let _permit = sem
                        .acquire_owned()
                        .await
                        .map_err(|_| ClientError::Http("topology semaphore closed".into()))?;
                    let product = dev.product()?;
                    let (details, standby, sources, source_index, attributes) = tokio::try_join!(
                        product.product(),
                        product.standby(),
                        product.source_xml(),
                        product.source_index(),
                        product.attributes(),
                    )?;

                    let room_name = if details.room.is_empty() {
                        dev.friendly_name().to_string()
                    } else {
                        details.room.clone()
                    };

                    Ok(TopologyDevice {
                        udn: dev.udn().to_string(),
                        location: dev.location().to_string(),
                        friendly_name: dev.friendly_name().to_string(),
                        room: room_name,
                        product_name: details.name,
                        standby,
                        sources,
                        source_index,
                        attributes,
                    })
                }
                .await;
                (idx, udn, res)
            });
        }

        let mut entries = Vec::with_capacity(devices.len());
        let mut errors = Vec::new();
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok((idx, _udn, Ok(entry))) => entries.push((idx, entry)),
                Ok((_idx, udn, Err(err))) => errors.push((udn, err)),
                Err(err) => errors.push((String::new(), ClientError::Http(err.to_string()))),
            }
        }
        // Preserve caller-provided ordering within each room.
        entries.sort_by_key(|(idx, _)| *idx);

        let mut rooms: BTreeMap<String, RoomTopology> = BTreeMap::new();
        for (_, entry) in entries {
            let room_name = entry.room.clone();
            rooms
                .entry(room_name.clone())
                .or_insert_with(|| RoomTopology {
                    name: room_name,
                    devices: Vec::new(),
                })
                .devices
                .push(entry);
        }

        (Self { rooms }, errors)
    }

    pub fn rooms(&self) -> impl Iterator<Item = &RoomTopology> {
        self.rooms.values()
    }

    pub fn room(&self, name: &str) -> Option<&RoomTopology> {
        self.rooms.get(name)
    }

    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    /// Return all devices advertising the Songcast `Sender` attribute.
    pub fn senders(&self) -> impl Iterator<Item = &TopologyDevice> {
        self.rooms
            .values()
            .flat_map(|r| r.devices.iter())
            .filter(|d| d.attributes.sender)
    }

    /// Return all devices advertising the Songcast `Receiver` attribute.
    pub fn receivers(&self) -> impl Iterator<Item = &TopologyDevice> {
        self.rooms
            .values()
            .flat_map(|r| r.devices.iter())
            .filter(|d| d.attributes.receiver)
    }
}

#[cfg(test)]
mod tests {
    use openhome_core::SourceType;

    use super::*;

    #[test]
    fn topology_accessors_visible_sources_and_oob_active_source() {
        let entry = TopologyDevice {
            udn: "uuid:deck".into(),
            location: "http://127.0.0.1:80/desc.xml".into(),
            friendly_name: "Deck".into(),
            room: "Study".into(),
            product_name: "Deck".into(),
            standby: false,
            sources: vec![
                Source {
                    name: "Playlist".into(),
                    source_type: SourceType::Playlist,
                    visible: true,
                    system_name: "Playlist".into(),
                },
                Source {
                    name: "Hidden".into(),
                    source_type: SourceType::NetAux,
                    visible: false,
                    system_name: "Hidden".into(),
                },
            ],
            source_index: 99, // OOB -> active_source() == None
            attributes: DeviceAttributes::default(),
        };

        assert!(entry.active_source().is_none());
        let visible: Vec<&str> = entry
            .visible_sources()
            .map(|(_, s)| s.name.as_str())
            .collect();
        assert_eq!(visible, vec!["Playlist"]);

        let mut rooms = BTreeMap::new();
        rooms.insert(
            "Study".into(),
            RoomTopology {
                name: "Study".into(),
                devices: vec![entry],
            },
        );
        let topo = Topology { rooms };
        assert_eq!(topo.rooms().count(), 1);
        assert!(topo.room("Study").is_some());
        assert!(topo.room("Missing").is_none());
        assert_eq!(topo.senders().count(), 0);
        assert_eq!(topo.receivers().count(), 0);
    }
}
