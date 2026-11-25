//! SOAP action dispatch, service registry, and GENA state-snapshot builders.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use openhome_core::id_array::encode_id_array;
use openhome_core::scpd;
use openhome_core::soap::{decode_soap_args, encode_action_response, parse_soapaction_header};
use openhome_core::urn::{
    SERVICE_ID_INFO, SERVICE_ID_PLAYLIST, SERVICE_ID_PRODUCT, SERVICE_ID_RECEIVER,
    SERVICE_ID_SENDER, SERVICE_ID_TIME, SERVICE_ID_VOLUME, SERVICE_INFO_1, SERVICE_PLAYLIST_1,
    SERVICE_PRODUCT_1, SERVICE_RECEIVER_1, SERVICE_SENDER_1, SERVICE_TIME_1, SERVICE_VOLUME_1,
};
use openhome_core::xml::{encode_source_xml, encode_track_list, parse_id_list};
use openhome_core::{Fault, ServiceDescription, format_upnp_bool, parse_upnp_bool};

use crate::service::{
    InfoService, PlaylistService, ProductService, ReceiverService, SenderService, TimeService,
    VolumeService,
};

pub const SLUG_PRODUCT: &str = "Product";
pub const SLUG_PLAYLIST: &str = "Playlist";
pub const SLUG_INFO: &str = "Info";
pub const SLUG_TIME: &str = "Time";
pub const SLUG_VOLUME: &str = "Volume";
pub const SLUG_RECEIVER: &str = "Receiver";
pub const SLUG_SENDER: &str = "Sender";

/// Holds all registered OpenHome service backends for a single renderer device.
#[derive(Clone)]
pub struct ServiceRegistry {
    pub product: Arc<dyn ProductService>,
    pub playlist: Option<Arc<dyn PlaylistService>>,
    pub info: Option<Arc<dyn InfoService>>,
    pub time: Option<Arc<dyn TimeService>>,
    pub volume: Option<Arc<dyn VolumeService>>,
    pub receiver: Option<Arc<dyn ReceiverService>>,
    pub sender: Option<Arc<dyn SenderService>>,
}

/// Outcome of a dispatched SOAP action.
pub struct DispatchOutput {
    pub response_xml: String,
    pub mutated: bool,
}

impl ServiceRegistry {
    /// Return UPnP `<service>` descriptors for all registered services.
    pub fn service_descriptions(&self) -> Vec<ServiceDescription> {
        let mut out = vec![make_service_desc(
            SERVICE_PRODUCT_1,
            SERVICE_ID_PRODUCT,
            SLUG_PRODUCT,
        )];
        if self.playlist.is_some() {
            out.push(make_service_desc(
                SERVICE_PLAYLIST_1,
                SERVICE_ID_PLAYLIST,
                SLUG_PLAYLIST,
            ));
        }
        if self.info.is_some() {
            out.push(make_service_desc(
                SERVICE_INFO_1,
                SERVICE_ID_INFO,
                SLUG_INFO,
            ));
        }
        if self.time.is_some() {
            out.push(make_service_desc(
                SERVICE_TIME_1,
                SERVICE_ID_TIME,
                SLUG_TIME,
            ));
        }
        if self.volume.is_some() {
            out.push(make_service_desc(
                SERVICE_VOLUME_1,
                SERVICE_ID_VOLUME,
                SLUG_VOLUME,
            ));
        }
        if self.receiver.is_some() {
            out.push(make_service_desc(
                SERVICE_RECEIVER_1,
                SERVICE_ID_RECEIVER,
                SLUG_RECEIVER,
            ));
        }
        if self.sender.is_some() {
            out.push(make_service_desc(
                SERVICE_SENDER_1,
                SERVICE_ID_SENDER,
                SLUG_SENDER,
            ));
        }
        out
    }

    /// Return all service type URNs registered on this device (for SSDP).
    pub fn service_type_urns(&self) -> Vec<&'static str> {
        let mut out = vec![SERVICE_PRODUCT_1];
        if self.playlist.is_some() {
            out.push(SERVICE_PLAYLIST_1);
        }
        if self.info.is_some() {
            out.push(SERVICE_INFO_1);
        }
        if self.time.is_some() {
            out.push(SERVICE_TIME_1);
        }
        if self.volume.is_some() {
            out.push(SERVICE_VOLUME_1);
        }
        if self.receiver.is_some() {
            out.push(SERVICE_RECEIVER_1);
        }
        if self.sender.is_some() {
            out.push(SERVICE_SENDER_1);
        }
        out
    }

    /// Return all registered service slugs.
    pub fn registered_slugs(&self) -> Vec<&'static str> {
        let mut out = vec![SLUG_PRODUCT];
        if self.playlist.is_some() {
            out.push(SLUG_PLAYLIST);
        }
        if self.info.is_some() {
            out.push(SLUG_INFO);
        }
        if self.time.is_some() {
            out.push(SLUG_TIME);
        }
        if self.volume.is_some() {
            out.push(SLUG_VOLUME);
        }
        if self.receiver.is_some() {
            out.push(SLUG_RECEIVER);
        }
        if self.sender.is_some() {
            out.push(SLUG_SENDER);
        }
        out
    }

    /// Return whether a service slug is registered on this device.
    pub fn has_service(&self, slug: &str) -> bool {
        match slug {
            SLUG_PRODUCT => true,
            SLUG_PLAYLIST => self.playlist.is_some(),
            SLUG_INFO => self.info.is_some(),
            SLUG_TIME => self.time.is_some(),
            SLUG_VOLUME => self.volume.is_some(),
            SLUG_RECEIVER => self.receiver.is_some(),
            SLUG_SENDER => self.sender.is_some(),
            _ => false,
        }
    }

    /// Return the SCPD XML document for a registered service slug.
    pub fn scpd_xml(&self, slug: &str) -> Option<&'static str> {
        match slug {
            SLUG_PRODUCT => Some(scpd::PRODUCT_1_XML),
            SLUG_PLAYLIST if self.playlist.is_some() => Some(scpd::PLAYLIST_1_XML),
            SLUG_INFO if self.info.is_some() => Some(scpd::INFO_1_XML),
            SLUG_TIME if self.time.is_some() => Some(scpd::TIME_1_XML),
            SLUG_VOLUME if self.volume.is_some() => Some(scpd::VOLUME_1_XML),
            SLUG_RECEIVER if self.receiver.is_some() => Some(scpd::RECEIVER_1_XML),
            SLUG_SENDER if self.sender.is_some() => Some(scpd::SENDER_1_XML),
            _ => None,
        }
    }

    /// Build the full GENA state snapshot for a registered service slug.
    pub fn state_snapshot(&self, slug: &str) -> Option<BTreeMap<String, String>> {
        match slug {
            SLUG_PRODUCT => Some(product_snapshot(self.product.as_ref())),
            SLUG_PLAYLIST => self.playlist.as_deref().map(playlist_snapshot),
            SLUG_INFO => self.info.as_deref().map(info_snapshot),
            SLUG_TIME => self.time.as_deref().map(time_snapshot),
            SLUG_VOLUME => self.volume.as_deref().map(volume_snapshot),
            SLUG_RECEIVER => self.receiver.as_deref().map(receiver_snapshot),
            SLUG_SENDER => self.sender.as_deref().map(sender_snapshot),
            _ => None,
        }
    }

    /// Dispatch a SOAP action request to the target service slug.
    pub fn dispatch_soap(
        &self,
        slug: &str,
        soap_action_header: Option<&str>,
        body_xml: &str,
    ) -> Result<DispatchOutput, Fault> {
        let (body_action, args) = decode_soap_args(body_xml)
            .map_err(|e| Fault::invalid_args(format!("malformed SOAP body: {e}")))?;

        let action = soap_action_header
            .and_then(parse_soapaction_header)
            .map(|(_, a)| a)
            .unwrap_or(body_action.as_str());

        match slug {
            SLUG_PRODUCT => dispatch_product(self.product.as_ref(), action, &args),
            SLUG_PLAYLIST => {
                let svc = self
                    .playlist
                    .as_deref()
                    .ok_or_else(|| Fault::invalid_action(action))?;
                dispatch_playlist(svc, action, &args)
            }
            SLUG_INFO => {
                let svc = self
                    .info
                    .as_deref()
                    .ok_or_else(|| Fault::invalid_action(action))?;
                dispatch_info(svc, action)
            }
            SLUG_TIME => {
                let svc = self
                    .time
                    .as_deref()
                    .ok_or_else(|| Fault::invalid_action(action))?;
                dispatch_time(svc, action)
            }
            SLUG_VOLUME => {
                let svc = self
                    .volume
                    .as_deref()
                    .ok_or_else(|| Fault::invalid_action(action))?;
                dispatch_volume(svc, action, &args)
            }
            SLUG_RECEIVER => {
                let svc = self
                    .receiver
                    .as_deref()
                    .ok_or_else(|| Fault::invalid_action(action))?;
                dispatch_receiver(svc, action, &args)
            }
            SLUG_SENDER => {
                let svc = self
                    .sender
                    .as_deref()
                    .ok_or_else(|| Fault::invalid_action(action))?;
                dispatch_sender(svc, action)
            }
            _ => Err(Fault::invalid_action(action)),
        }
    }
}

fn make_service_desc(service_type: &str, service_id: &str, slug: &str) -> ServiceDescription {
    ServiceDescription {
        service_type: service_type.to_string(),
        service_id: service_id.to_string(),
        scpd_url: format!("/scpd/{slug}.xml"),
        control_url: format!("/ctl/{slug}"),
        event_sub_url: format!("/evt/{slug}"),
    }
}

fn req_str<'a>(args: &'a HashMap<String, String>, name: &str) -> Result<&'a str, Fault> {
    args.get(name)
        .map(String::as_str)
        .ok_or_else(|| Fault::invalid_args(format!("missing argument {name}")))
}

fn req_u32(args: &HashMap<String, String>, name: &str) -> Result<u32, Fault> {
    req_str(args, name)?
        .trim()
        .parse::<u32>()
        .map_err(|_| Fault::invalid_args(format!("invalid ui4 for {name}")))
}

fn req_i32(args: &HashMap<String, String>, name: &str) -> Result<i32, Fault> {
    req_str(args, name)?
        .trim()
        .parse::<i32>()
        .map_err(|_| Fault::invalid_args(format!("invalid i4 for {name}")))
}

fn req_bool(args: &HashMap<String, String>, name: &'static str) -> Result<bool, Fault> {
    let raw = req_str(args, name)?;
    parse_upnp_bool(name, raw).map_err(|_| Fault::invalid_args(format!("invalid bool for {name}")))
}

fn ok_resp(
    urn: &str,
    action: &str,
    out: &[(&str, &str)],
    mutated: bool,
) -> Result<DispatchOutput, Fault> {
    Ok(DispatchOutput {
        response_xml: encode_action_response(urn, action, out),
        mutated,
    })
}

fn dispatch_product(
    svc: &dyn ProductService,
    action: &str,
    args: &HashMap<String, String>,
) -> Result<DispatchOutput, Fault> {
    match action {
        "Manufacturer" => {
            let info = svc.product_info();
            ok_resp(
                SERVICE_PRODUCT_1,
                action,
                &[
                    ("Name", &info.manufacturer_name),
                    ("Info", &info.manufacturer_info),
                    ("Url", &info.manufacturer_url),
                    ("ImageUri", &info.manufacturer_image_uri),
                ],
                false,
            )
        }
        "Model" => {
            let info = svc.product_info();
            ok_resp(
                SERVICE_PRODUCT_1,
                action,
                &[
                    ("Name", &info.model_name),
                    ("Info", &info.model_info),
                    ("Url", &info.model_url),
                    ("ImageUri", &info.model_image_uri),
                ],
                false,
            )
        }
        "Product" => {
            let info = svc.product_info();
            ok_resp(
                SERVICE_PRODUCT_1,
                action,
                &[
                    ("Room", &info.product_room),
                    ("Name", &info.product_name),
                    ("Info", &info.product_info),
                    ("Url", &info.product_url),
                    ("ImageUri", &info.product_image_uri),
                ],
                false,
            )
        }
        "Standby" => ok_resp(
            SERVICE_PRODUCT_1,
            action,
            &[("Value", format_upnp_bool(svc.standby()))],
            false,
        ),
        "SetStandby" => {
            let val = req_bool(args, "Value")?;
            svc.set_standby(val)?;
            ok_resp(SERVICE_PRODUCT_1, action, &[], true)
        }
        "SourceCount" => {
            let count = svc.sources().len().to_string();
            ok_resp(SERVICE_PRODUCT_1, action, &[("Value", &count)], false)
        }
        "SourceXml" => {
            let xml = encode_source_xml(&svc.sources());
            ok_resp(SERVICE_PRODUCT_1, action, &[("Value", &xml)], false)
        }
        "SourceIndex" => {
            let idx = svc.source_index().to_string();
            ok_resp(SERVICE_PRODUCT_1, action, &[("Value", &idx)], false)
        }
        "SetSourceIndex" => {
            let idx = req_u32(args, "Value")?;
            svc.set_source_index(idx)?;
            ok_resp(SERVICE_PRODUCT_1, action, &[], true)
        }
        "SetSourceIndexByName" => {
            let name = req_str(args, "Value")?;
            svc.set_source_index_by_name(name)?;
            ok_resp(SERVICE_PRODUCT_1, action, &[], true)
        }
        "SetSourceBySystemName" => {
            let sys = req_str(args, "Value")?;
            svc.set_source_by_system_name(sys)?;
            ok_resp(SERVICE_PRODUCT_1, action, &[], true)
        }
        "Source" => {
            let idx = req_u32(args, "Index")?;
            let sources = svc.sources();
            let src = sources
                .get(idx as usize)
                .ok_or_else(Fault::source_not_found)?;
            ok_resp(
                SERVICE_PRODUCT_1,
                action,
                &[
                    ("SystemName", &src.system_name),
                    ("Type", src.source_type.as_str()),
                    ("Name", &src.name),
                    ("Visible", format_upnp_bool(src.visible)),
                ],
                false,
            )
        }
        "Attributes" => {
            let attrs = svc.attributes().to_attribute_string();
            ok_resp(SERVICE_PRODUCT_1, action, &[("Value", &attrs)], false)
        }
        "SourceXmlChangeCount" => {
            let count = svc.source_xml_change_count().to_string();
            ok_resp(SERVICE_PRODUCT_1, action, &[("Value", &count)], false)
        }
        other => Err(Fault::invalid_action(other)),
    }
}

fn dispatch_playlist(
    svc: &dyn PlaylistService,
    action: &str,
    args: &HashMap<String, String>,
) -> Result<DispatchOutput, Fault> {
    match action {
        "Play" => {
            svc.play()?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "Pause" => {
            svc.pause()?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "Stop" => {
            svc.stop()?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "Next" => {
            svc.next()?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "Previous" => {
            svc.previous()?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "SetRepeat" => {
            let val = req_bool(args, "Value")?;
            svc.set_repeat(val)?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "Repeat" => ok_resp(
            SERVICE_PLAYLIST_1,
            action,
            &[("Value", format_upnp_bool(svc.repeat()))],
            false,
        ),
        "SetShuffle" => {
            let val = req_bool(args, "Value")?;
            svc.set_shuffle(val)?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "Shuffle" => ok_resp(
            SERVICE_PLAYLIST_1,
            action,
            &[("Value", format_upnp_bool(svc.shuffle()))],
            false,
        ),
        "SeekSecondAbsolute" => {
            let secs = req_u32(args, "Value")?;
            svc.seek_second_absolute(secs)?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "SeekSecondRelative" => {
            let delta = req_i32(args, "Value")?;
            svc.seek_second_relative(delta)?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "SeekId" => {
            let id = req_u32(args, "Value")?;
            svc.seek_id(id)?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "SeekIndex" => {
            let index = req_u32(args, "Value")?;
            svc.seek_index(index)?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "TransportState" => ok_resp(
            SERVICE_PLAYLIST_1,
            action,
            &[("Value", svc.transport_state().as_str())],
            false,
        ),
        "Id" => {
            let id = svc.current_id().to_string();
            ok_resp(SERVICE_PLAYLIST_1, action, &[("Value", &id)], false)
        }
        "Read" => {
            let id = req_u32(args, "Id")?;
            let entry = svc.read(id)?;
            ok_resp(
                SERVICE_PLAYLIST_1,
                action,
                &[("Uri", &entry.uri), ("Metadata", &entry.metadata)],
                false,
            )
        }
        "ReadList" => {
            let id_list = req_str(args, "IdList")?;
            let ids = parse_id_list(id_list);
            let entries = svc.read_list(&ids);
            let track_list = encode_track_list(&entries);
            ok_resp(
                SERVICE_PLAYLIST_1,
                action,
                &[("TrackList", &track_list)],
                false,
            )
        }
        "Insert" => {
            let after_id = req_u32(args, "AfterId")?;
            let uri = req_str(args, "Uri")?.to_string();
            let metadata = args.get("Metadata").cloned().unwrap_or_default();
            let new_id = svc.insert(after_id, uri, metadata)?.to_string();
            ok_resp(SERVICE_PLAYLIST_1, action, &[("NewId", &new_id)], true)
        }
        "DeleteId" => {
            let id = req_u32(args, "Value")?;
            svc.delete_id(id)?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "DeleteAll" => {
            svc.delete_all()?;
            ok_resp(SERVICE_PLAYLIST_1, action, &[], true)
        }
        "TracksMax" => {
            let max = svc.tracks_max().to_string();
            ok_resp(SERVICE_PLAYLIST_1, action, &[("Value", &max)], false)
        }
        "IdArray" => {
            let (token, ids) = svc.id_array();
            let token_str = token.to_string();
            let array = encode_id_array(&ids);
            ok_resp(
                SERVICE_PLAYLIST_1,
                action,
                &[("Token", &token_str), ("Array", &array)],
                false,
            )
        }
        "IdArrayChanged" => {
            let token = req_u32(args, "Token")?;
            let changed = svc.id_array_changed(token);
            ok_resp(
                SERVICE_PLAYLIST_1,
                action,
                &[("Value", format_upnp_bool(changed))],
                false,
            )
        }
        "ProtocolInfo" => {
            let info = svc.protocol_info();
            ok_resp(SERVICE_PLAYLIST_1, action, &[("Value", &info)], false)
        }
        other => Err(Fault::invalid_action(other)),
    }
}

fn dispatch_info(svc: &dyn InfoService, action: &str) -> Result<DispatchOutput, Fault> {
    match action {
        "Counters" => {
            let c = svc.counters();
            let tc = c.track_count.to_string();
            let dc = c.details_count.to_string();
            let mc = c.metatext_count.to_string();
            ok_resp(
                SERVICE_INFO_1,
                action,
                &[
                    ("TrackCount", &tc),
                    ("DetailsCount", &dc),
                    ("MetatextCount", &mc),
                ],
                false,
            )
        }
        "Track" => {
            let (uri, metadata) = svc.track();
            ok_resp(
                SERVICE_INFO_1,
                action,
                &[("Uri", &uri), ("Metadata", &metadata)],
                false,
            )
        }
        "Details" => {
            let d = svc.details();
            let duration = d.duration.to_string();
            let bit_rate = d.bit_rate.to_string();
            let bit_depth = d.bit_depth.to_string();
            let sample_rate = d.sample_rate.to_string();
            ok_resp(
                SERVICE_INFO_1,
                action,
                &[
                    ("Duration", &duration),
                    ("BitRate", &bit_rate),
                    ("BitDepth", &bit_depth),
                    ("SampleRate", &sample_rate),
                    ("Lossless", format_upnp_bool(d.lossless)),
                    ("CodecName", &d.codec_name),
                ],
                false,
            )
        }
        "Metatext" => {
            let mt = svc.metatext();
            ok_resp(SERVICE_INFO_1, action, &[("Value", &mt)], false)
        }
        other => Err(Fault::invalid_action(other)),
    }
}

fn dispatch_time(svc: &dyn TimeService, action: &str) -> Result<DispatchOutput, Fault> {
    match action {
        "Time" => {
            let t = svc.time();
            let tc = t.track_count.to_string();
            let dur = t.duration.to_string();
            let sec = t.seconds.to_string();
            ok_resp(
                SERVICE_TIME_1,
                action,
                &[("TrackCount", &tc), ("Duration", &dur), ("Seconds", &sec)],
                false,
            )
        }
        other => Err(Fault::invalid_action(other)),
    }
}

fn dispatch_volume(
    svc: &dyn VolumeService,
    action: &str,
    args: &HashMap<String, String>,
) -> Result<DispatchOutput, Fault> {
    match action {
        "Characteristics" => {
            let c = svc.characteristics();
            let vmax = c.volume_max.to_string();
            let vunity = c.volume_unity.to_string();
            let vsteps = c.volume_steps.to_string();
            let vmdb = c.volume_milli_db_per_step.to_string();
            let bmax = c.balance_max.to_string();
            let fmax = c.fade_max.to_string();
            ok_resp(
                SERVICE_VOLUME_1,
                action,
                &[
                    ("VolumeMax", &vmax),
                    ("VolumeUnity", &vunity),
                    ("VolumeSteps", &vsteps),
                    ("VolumeMilliDbPerStep", &vmdb),
                    ("BalanceMax", &bmax),
                    ("FadeMax", &fmax),
                ],
                false,
            )
        }
        "SetVolume" => {
            let val = req_u32(args, "Value")?;
            svc.set_volume(val)?;
            ok_resp(SERVICE_VOLUME_1, action, &[], true)
        }
        "VolumeInc" => {
            svc.volume_inc()?;
            ok_resp(SERVICE_VOLUME_1, action, &[], true)
        }
        "VolumeDec" => {
            svc.volume_dec()?;
            ok_resp(SERVICE_VOLUME_1, action, &[], true)
        }
        "Volume" => {
            let val = svc.state().volume.to_string();
            ok_resp(SERVICE_VOLUME_1, action, &[("Value", &val)], false)
        }
        "SetMute" => {
            let mute = req_bool(args, "Value")?;
            svc.set_mute(mute)?;
            ok_resp(SERVICE_VOLUME_1, action, &[], true)
        }
        "Mute" => ok_resp(
            SERVICE_VOLUME_1,
            action,
            &[("Value", format_upnp_bool(svc.state().mute))],
            false,
        ),
        "VolumeLimit" => {
            let val = svc.state().volume_limit.to_string();
            ok_resp(SERVICE_VOLUME_1, action, &[("Value", &val)], false)
        }
        "SetBalance" => {
            let val = req_i32(args, "Value")?;
            svc.set_balance(val)?;
            ok_resp(SERVICE_VOLUME_1, action, &[], true)
        }
        "BalanceInc" => {
            svc.balance_inc()?;
            ok_resp(SERVICE_VOLUME_1, action, &[], true)
        }
        "BalanceDec" => {
            svc.balance_dec()?;
            ok_resp(SERVICE_VOLUME_1, action, &[], true)
        }
        "Balance" => {
            let val = svc.state().balance.to_string();
            ok_resp(SERVICE_VOLUME_1, action, &[("Value", &val)], false)
        }
        "SetFade" => {
            let val = req_i32(args, "Value")?;
            svc.set_fade(val)?;
            ok_resp(SERVICE_VOLUME_1, action, &[], true)
        }
        "FadeInc" => {
            svc.fade_inc()?;
            ok_resp(SERVICE_VOLUME_1, action, &[], true)
        }
        "FadeDec" => {
            svc.fade_dec()?;
            ok_resp(SERVICE_VOLUME_1, action, &[], true)
        }
        "Fade" => {
            let val = svc.state().fade.to_string();
            ok_resp(SERVICE_VOLUME_1, action, &[("Value", &val)], false)
        }
        other => Err(Fault::invalid_action(other)),
    }
}

fn dispatch_receiver(
    svc: &dyn ReceiverService,
    action: &str,
    args: &HashMap<String, String>,
) -> Result<DispatchOutput, Fault> {
    match action {
        "Play" => {
            svc.play()?;
            ok_resp(SERVICE_RECEIVER_1, action, &[], true)
        }
        "Stop" => {
            svc.stop()?;
            ok_resp(SERVICE_RECEIVER_1, action, &[], true)
        }
        "SetSender" => {
            let uri = req_str(args, "Uri")?.to_string();
            let metadata = args.get("Metadata").cloned().unwrap_or_default();
            svc.set_sender(uri, metadata)?;
            ok_resp(SERVICE_RECEIVER_1, action, &[], true)
        }
        "Sender" => {
            let (uri, metadata) = svc.sender();
            ok_resp(
                SERVICE_RECEIVER_1,
                action,
                &[("Uri", &uri), ("Metadata", &metadata)],
                false,
            )
        }
        "ProtocolInfo" => {
            let info = svc.protocol_info();
            ok_resp(SERVICE_RECEIVER_1, action, &[("Value", &info)], false)
        }
        "TransportState" => ok_resp(
            SERVICE_RECEIVER_1,
            action,
            &[("Value", svc.transport_state().as_str())],
            false,
        ),
        other => Err(Fault::invalid_action(other)),
    }
}

fn dispatch_sender(svc: &dyn SenderService, action: &str) -> Result<DispatchOutput, Fault> {
    match action {
        "PresentationUrl" => {
            let v = svc.presentation_url();
            ok_resp(SERVICE_SENDER_1, action, &[("Value", &v)], false)
        }
        "Metadata" => {
            let v = svc.metadata();
            ok_resp(SERVICE_SENDER_1, action, &[("Value", &v)], false)
        }
        "Audio" => ok_resp(
            SERVICE_SENDER_1,
            action,
            &[("Value", format_upnp_bool(svc.audio()))],
            false,
        ),
        "Status" => ok_resp(
            SERVICE_SENDER_1,
            action,
            &[("Value", svc.status().as_str())],
            false,
        ),
        "Attributes" => {
            let v = svc.attributes();
            ok_resp(SERVICE_SENDER_1, action, &[("Value", &v)], false)
        }
        other => Err(Fault::invalid_action(other)),
    }
}

pub fn product_snapshot(svc: &dyn ProductService) -> BTreeMap<String, String> {
    let info = svc.product_info();
    let sources = svc.sources();
    let mut map = BTreeMap::new();
    map.insert("ManufacturerName".into(), info.manufacturer_name);
    map.insert("ManufacturerInfo".into(), info.manufacturer_info);
    map.insert("ManufacturerUrl".into(), info.manufacturer_url);
    map.insert("ManufacturerImageUri".into(), info.manufacturer_image_uri);
    map.insert("ModelName".into(), info.model_name);
    map.insert("ModelInfo".into(), info.model_info);
    map.insert("ModelUrl".into(), info.model_url);
    map.insert("ModelImageUri".into(), info.model_image_uri);
    map.insert("ProductRoom".into(), info.product_room);
    map.insert("ProductName".into(), info.product_name);
    map.insert("ProductInfo".into(), info.product_info);
    map.insert("ProductUrl".into(), info.product_url);
    map.insert("ProductImageUri".into(), info.product_image_uri);
    map.insert("Standby".into(), format_upnp_bool(svc.standby()).into());
    map.insert("SourceIndex".into(), svc.source_index().to_string());
    map.insert("SourceCount".into(), sources.len().to_string());
    map.insert("SourceXml".into(), encode_source_xml(&sources));
    map.insert("Attributes".into(), svc.attributes().to_attribute_string());
    map
}

pub fn playlist_snapshot(svc: &dyn PlaylistService) -> BTreeMap<String, String> {
    let (_, ids) = svc.id_array();
    let mut map = BTreeMap::new();
    map.insert(
        "TransportState".into(),
        svc.transport_state().as_str().into(),
    );
    map.insert("Repeat".into(), format_upnp_bool(svc.repeat()).into());
    map.insert("Shuffle".into(), format_upnp_bool(svc.shuffle()).into());
    map.insert("Id".into(), svc.current_id().to_string());
    map.insert("TracksMax".into(), svc.tracks_max().to_string());
    map.insert("ProtocolInfo".into(), svc.protocol_info());
    map.insert("IdArray".into(), encode_id_array(&ids));
    map
}

pub fn info_snapshot(svc: &dyn InfoService) -> BTreeMap<String, String> {
    let c = svc.counters();
    let (uri, metadata) = svc.track();
    let d = svc.details();
    let mut map = BTreeMap::new();
    map.insert("TrackCount".into(), c.track_count.to_string());
    map.insert("DetailsCount".into(), c.details_count.to_string());
    map.insert("MetatextCount".into(), c.metatext_count.to_string());
    map.insert("Uri".into(), uri);
    map.insert("Metadata".into(), metadata);
    map.insert("Duration".into(), d.duration.to_string());
    map.insert("BitRate".into(), d.bit_rate.to_string());
    map.insert("BitDepth".into(), d.bit_depth.to_string());
    map.insert("SampleRate".into(), d.sample_rate.to_string());
    map.insert("Lossless".into(), format_upnp_bool(d.lossless).into());
    map.insert("CodecName".into(), d.codec_name);
    map.insert("Metatext".into(), svc.metatext());
    map
}

pub fn time_snapshot(svc: &dyn TimeService) -> BTreeMap<String, String> {
    let t = svc.time();
    let mut map = BTreeMap::new();
    map.insert("TrackCount".into(), t.track_count.to_string());
    map.insert("Duration".into(), t.duration.to_string());
    map.insert("Seconds".into(), t.seconds.to_string());
    map
}

pub fn volume_snapshot(svc: &dyn VolumeService) -> BTreeMap<String, String> {
    let c = svc.characteristics();
    let s = svc.state();
    let mut map = BTreeMap::new();
    map.insert("Volume".into(), s.volume.to_string());
    map.insert("Mute".into(), format_upnp_bool(s.mute).into());
    map.insert("Balance".into(), s.balance.to_string());
    map.insert("Fade".into(), s.fade.to_string());
    map.insert("VolumeLimit".into(), s.volume_limit.to_string());
    map.insert("VolumeMax".into(), c.volume_max.to_string());
    map.insert("VolumeUnity".into(), c.volume_unity.to_string());
    map.insert("VolumeSteps".into(), c.volume_steps.to_string());
    map.insert(
        "VolumeMilliDbPerStep".into(),
        c.volume_milli_db_per_step.to_string(),
    );
    map.insert("BalanceMax".into(), c.balance_max.to_string());
    map.insert("FadeMax".into(), c.fade_max.to_string());
    map
}

pub fn receiver_snapshot(svc: &dyn ReceiverService) -> BTreeMap<String, String> {
    let (uri, metadata) = svc.sender();
    let mut map = BTreeMap::new();
    map.insert("Uri".into(), uri);
    map.insert("Metadata".into(), metadata);
    map.insert(
        "TransportState".into(),
        svc.transport_state().as_str().into(),
    );
    map.insert("ProtocolInfo".into(), svc.protocol_info());
    map
}

pub fn sender_snapshot(svc: &dyn SenderService) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    map.insert("PresentationUrl".into(), svc.presentation_url());
    map.insert("Metadata".into(), svc.metadata());
    map.insert("Audio".into(), format_upnp_bool(svc.audio()).into());
    map.insert("Status".into(), svc.status().as_str().into());
    map.insert("Attributes".into(), svc.attributes());
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryRenderer;
    use openhome_core::soap::encode_action_request;

    fn product_only_registry() -> ServiceRegistry {
        let renderer = Arc::new(MemoryRenderer::new("Room", "Name"));
        ServiceRegistry {
            product: renderer,
            playlist: None,
            info: None,
            time: None,
            volume: None,
            receiver: None,
            sender: None,
        }
    }

    #[test]
    fn partial_registry_reports_only_registered_services() {
        let reg = product_only_registry();
        assert!(reg.has_service(SLUG_PRODUCT));
        assert!(!reg.has_service(SLUG_PLAYLIST));
        assert!(!reg.has_service(SLUG_VOLUME));
        assert_eq!(reg.service_descriptions().len(), 1);
        assert!(reg.scpd_xml(SLUG_PRODUCT).is_some());
        assert!(reg.scpd_xml(SLUG_PLAYLIST).is_none());
        assert!(reg.state_snapshot(SLUG_PRODUCT).is_some());
        assert!(reg.state_snapshot(SLUG_PLAYLIST).is_none());

        let req = encode_action_request(SERVICE_PLAYLIST_1, "Play", &[]);
        let err = reg
            .dispatch_soap(SLUG_PLAYLIST, None, &req)
            .err()
            .expect("unregistered service returns fault");
        assert_eq!(err.code, Fault::INVALID_ACTION);
    }

    #[test]
    fn dispatch_soap_error_paths_and_header_precedence() {
        let renderer = Arc::new(MemoryRenderer::new("Room", "Name"));
        let reg = ServiceRegistry {
            product: renderer.clone(),
            playlist: Some(renderer.clone()),
            info: None,
            time: None,
            volume: Some(renderer),
            receiver: None,
            sender: None,
        };

        // Malformed XML -> 402 Invalid Args.
        let err = reg
            .dispatch_soap(SLUG_PRODUCT, None, "<not-soap/>")
            .err()
            .unwrap();
        assert_eq!(err.code, Fault::INVALID_ARGS);

        // Missing required argument -> 402.
        let missing_arg = encode_action_request(SERVICE_VOLUME_1, "SetVolume", &[]);
        let err = reg
            .dispatch_soap(SLUG_VOLUME, None, &missing_arg)
            .err()
            .unwrap();
        assert_eq!(err.code, Fault::INVALID_ARGS);

        // Non-numeric ui4 / bad bool -> 402.
        let bad_u32 = encode_action_request(SERVICE_VOLUME_1, "SetVolume", &[("Value", "abc")]);
        assert_eq!(
            reg.dispatch_soap(SLUG_VOLUME, None, &bad_u32)
                .err()
                .unwrap()
                .code,
            Fault::INVALID_ARGS
        );
        let bad_bool = encode_action_request(SERVICE_VOLUME_1, "SetMute", &[("Value", "maybe")]);
        assert_eq!(
            reg.dispatch_soap(SLUG_VOLUME, None, &bad_bool)
                .err()
                .unwrap()
                .code,
            Fault::INVALID_ARGS
        );

        // Unknown action -> 401.
        let unknown = encode_action_request(SERVICE_PRODUCT_1, "BogusAction", &[]);
        assert_eq!(
            reg.dispatch_soap(SLUG_PRODUCT, None, &unknown)
                .err()
                .unwrap()
                .code,
            Fault::INVALID_ACTION
        );

        // SOAPACTION header takes precedence over body action element name.
        let body = encode_action_request(SERVICE_PRODUCT_1, "IgnoredBodyName", &[]);
        let out = reg
            .dispatch_soap(
                SLUG_PRODUCT,
                Some("\"urn:av-openhome-org:service:Product:1#Standby\""),
                &body,
            )
            .unwrap();
        assert!(out.response_xml.contains("StandbyResponse"));
        assert!(!out.mutated);
    }
}
