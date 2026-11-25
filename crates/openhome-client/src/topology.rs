//! OpenHome room/source topology model inspired by `openhome/ohTopology`.
//!
//! Aggregates one or more [`OpenHomeDevice`] instances by their `Product.Room`
//! so control points can display rooms, active sources, standby state, and
//! Songcast Sender/Receiver capabilities.

use std::collections::BTreeMap;

use openhome_core::{DeviceAttributes, Source};

use crate::device::OpenHomeDevice;
use crate::error::ClientError;

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
    /// Query `Product:1` metadata on every device in `devices` and group them by room.
    pub async fn from_devices(devices: &[OpenHomeDevice]) -> Result<Self, ClientError> {
        let mut rooms: BTreeMap<String, RoomTopology> = BTreeMap::new();

        for dev in devices {
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

            let entry = TopologyDevice {
                udn: dev.udn().to_string(),
                location: dev.location().to_string(),
                friendly_name: dev.friendly_name().to_string(),
                room: room_name.clone(),
                product_name: details.name,
                standby,
                sources,
                source_index,
                attributes,
            };

            rooms
                .entry(room_name.clone())
                .or_insert_with(|| RoomTopology {
                    name: room_name,
                    devices: Vec::new(),
                })
                .devices
                .push(entry);
        }

        Ok(Self { rooms })
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
