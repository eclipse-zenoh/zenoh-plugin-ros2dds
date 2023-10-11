use crate::dds_types::DDSRawSample;
use crate::{ChannelEvent, ROS_DISCOVERY_INFO_PUSH_INTERVAL_MS};
//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use crate::dds_utils::{ddsrt_iov_len_from_usize, delete_dds_entity, get_guid};
use crate::gid::Gid;
use async_std::task;
use cdr::{CdrLe, Infinite};
use cyclors::qos::{
    Durability, History, IgnoreLocal, IgnoreLocalKind, Qos, Reliability, DDS_INFINITE_TIME,
};
use cyclors::*;
use flume::{unbounded, Receiver, Sender};
use futures::select;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::{
    collections::HashMap,
    ffi::{CStr, CString},
    mem::MaybeUninit,
};
use zenoh::buffers::ZBuf;
use zenoh::prelude::HasReader;
use zenoh_core::zwrite;
use zenoh_util::{TimedEvent, Timer};

pub const ROS_DISCOVERY_INFO_TOPIC_NAME: &str = "ros_discovery_info";
const ROS_DISCOVERY_INFO_TOPIC_TYPE: &str = "rmw_dds_common::msg::dds_::ParticipantEntitiesInfo_";

pub struct RosDiscoveryInfoMgr {
    reader: dds_entity_t,
    writer: dds_entity_t,
    // This bridge Node fullname, as used as index in participant_entities_info.node_entities_info_seq
    node_fullname: String,
    // The ParticipantEntitiesInfo to publish on "ros_discovery_info" topic when changed,
    // plus a bool indicating if it changed
    participant_entities_state: Arc<RwLock<(ParticipantEntitiesInfo, bool)>>,
}

impl Drop for RosDiscoveryInfoMgr {
    fn drop(&mut self) {
        if let Err(e) = delete_dds_entity(self.reader) {
            log::warn!(
                "Error dropping DDS reader on {}: {}",
                ROS_DISCOVERY_INFO_TOPIC_NAME,
                e
            );
        }
        if let Err(e) = delete_dds_entity(self.writer) {
            log::warn!(
                "Error dropping DDS writer on {}: {}",
                ROS_DISCOVERY_INFO_TOPIC_NAME,
                e
            );
        }
    }
}

impl RosDiscoveryInfoMgr {
    pub fn new(
        participant: dds_entity_t,
        namespace: &str,
        node_name: &str,
    ) -> Result<RosDiscoveryInfoMgr, String> {
        let cton = CString::new(ROS_DISCOVERY_INFO_TOPIC_NAME)
            .unwrap()
            .into_raw();
        let ctyn = CString::new(ROS_DISCOVERY_INFO_TOPIC_TYPE)
            .unwrap()
            .into_raw();

        unsafe {
            // Create topic (for reader/writer creation)
            let t = cdds_create_blob_topic(participant, cton, ctyn, true);

            // Create reader
            let mut qos = Qos::default();
            qos.reliability = Some(Reliability {
                kind: qos::ReliabilityKind::RELIABLE,
                max_blocking_time: DDS_INFINITE_TIME,
            });
            qos.durability = Some(Durability {
                kind: qos::DurabilityKind::TRANSIENT_LOCAL,
            });
            // Note: KEEP_ALL to not loose any sample (topic is keyless). A periodic task should take samples from history.
            qos.history = Some(History {
                kind: qos::HistoryKind::KEEP_ALL,
                depth: 0,
            });
            qos.ignore_local = Some(IgnoreLocal {
                kind: IgnoreLocalKind::PARTICIPANT,
            });
            let qos_native = qos.to_qos_native();
            let reader = dds_create_reader(participant, t, qos_native, std::ptr::null());
            Qos::delete_qos_native(qos_native);
            if reader < 0 {
                return Err(format!(
                    "Error creating DDS Reader on {}: {}",
                    ROS_DISCOVERY_INFO_TOPIC_NAME,
                    CStr::from_ptr(dds_strretcode(-reader))
                        .to_str()
                        .unwrap_or("unrecoverable DDS retcode")
                ));
            }

            // Create writer
            let mut qos = Qos::default();
            qos.reliability = Some(Reliability {
                kind: qos::ReliabilityKind::RELIABLE,
                max_blocking_time: DDS_INFINITE_TIME,
            });
            qos.durability = Some(Durability {
                kind: qos::DurabilityKind::TRANSIENT_LOCAL,
            });
            qos.history = Some(History {
                kind: qos::HistoryKind::KEEP_LAST,
                depth: 1,
            });
            qos.ignore_local = Some(IgnoreLocal {
                kind: IgnoreLocalKind::PARTICIPANT,
            });
            let qos_native = qos.to_qos_native();
            let writer = dds_create_writer(participant, t, qos_native, std::ptr::null());
            Qos::delete_qos_native(qos_native);
            if writer < 0 {
                return Err(format!(
                    "Error creating DDS Writer on {}: {}",
                    ROS_DISCOVERY_INFO_TOPIC_NAME,
                    CStr::from_ptr(dds_strretcode(-writer))
                        .to_str()
                        .unwrap_or("unrecoverable DDS retcode")
                ));
            }

            drop(CString::from_raw(cton));
            drop(CString::from_raw(ctyn));

            let gid = get_guid(&participant)?;
            let mut participant_entities_info = ParticipantEntitiesInfo::new(gid);
            let node_info = NodeEntitiesInfo::new(namespace.to_string(), node_name.to_string());
            let node_fullname = node_info.to_string();
            participant_entities_info
                .node_entities_info_seq
                .insert(node_fullname.clone(), node_info);

            Ok(RosDiscoveryInfoMgr {
                reader,
                writer,
                node_fullname,
                participant_entities_state: Arc::new(RwLock::new((
                    participant_entities_info,
                    true,
                ))),
            })
        }
    }

    pub async fn run(&self) {
        let writer = self.writer;
        let participant_entities_state = self.participant_entities_state.clone();
        task::spawn(async move {
            // Timer for periodic write of "ros_discovery_info" topic
            let timer = Timer::default();
            let (tx, ros_disco_timer_rcv): (Sender<()>, Receiver<()>) = unbounded();
            let ros_disco_timer_event = TimedEvent::periodic(
                Duration::from_millis(ROS_DISCOVERY_INFO_PUSH_INTERVAL_MS),
                ChannelEvent { tx },
            );
            timer.add_async(ros_disco_timer_event).await;

            loop {
                select!(
                    _ = ros_disco_timer_rcv.recv_async() => {
                        let (ref msg, ref mut has_changed) = *zwrite!(participant_entities_state);
                        if *has_changed {
                            log::debug!("Publish update on 'ros_discovery_info' with {} writers and {} readers",
                                msg.node_entities_info_seq.values().next().map_or(0, |n| n.writer_gid_seq.len()),
                                msg.node_entities_info_seq.values().next().map_or(0, |n| n.reader_gid_seq.len())
                            );
                            log::trace!("Publish update on 'ros_discovery_info': {msg:?}");
                            Self::write(writer, msg).unwrap_or_else(|e|
                                log::error!("Failed to publish update on 'ros_discovery_info' topic: {e}")
                            );
                            *has_changed = false;
                        }

                    }
                )
            }
        });
    }

    pub fn add_dds_writer(&self, gid: Gid) {
        let (ref mut info, ref mut has_changed) = *zwrite!(self.participant_entities_state);
        info.node_entities_info_seq
            .get_mut(&self.node_fullname)
            .unwrap()
            .writer_gid_seq
            .insert(gid);
        *has_changed = true;
    }

    pub fn remove_dds_writer(&self, gid: Gid) {
        let (ref mut info, ref mut has_changed) = *zwrite!(self.participant_entities_state);
        info.node_entities_info_seq
            .get_mut(&self.node_fullname)
            .unwrap()
            .writer_gid_seq
            .remove(&gid);
        *has_changed = true;
    }

    pub fn add_dds_reader(&self, gid: Gid) {
        let (ref mut info, ref mut has_changed) = *zwrite!(self.participant_entities_state);
        info.node_entities_info_seq
            .get_mut(&self.node_fullname)
            .unwrap()
            .reader_gid_seq
            .insert(gid);
        *has_changed = true;
    }

    pub fn remove_dds_reader(&self, gid: Gid) {
        let (ref mut info, ref mut has_changed) = *zwrite!(self.participant_entities_state);
        info.node_entities_info_seq
            .get_mut(&self.node_fullname)
            .unwrap()
            .reader_gid_seq
            .remove(&gid);
        *has_changed = true;
    }

    pub fn read(&self) -> Vec<ParticipantEntitiesInfo> {
        unsafe {
            let mut zp: *mut ddsi_serdata = std::ptr::null_mut();
            #[allow(clippy::uninit_assumed_init)]
            let mut si = MaybeUninit::<[dds_sample_info_t; 1]>::uninit();
            // Place read samples into a map indexed by Participant gid.
            // Thus we only keep the last (not deserialized) update for each
            let mut map: HashMap<String, DDSRawSample> = HashMap::new();
            while dds_takecdr(
                self.reader,
                &mut zp,
                1,
                si.as_mut_ptr() as *mut dds_sample_info_t,
                DDS_ANY_STATE,
            ) > 0
            {
                let si = si.assume_init();
                if si[0].valid_data {
                    let raw_sample = DDSRawSample::create(zp);

                    // No need to deserialize the full payload. Just read the Participant gid (first 16 bytes of the payload)
                    let gid = hex::encode(&raw_sample.payload_as_slice()[0..16]);

                    map.insert(gid, raw_sample);
                }
                ddsi_serdata_unref(zp);
            }

            map.values()
                .filter_map(|sample| {
                    log::trace!("Deserialize ParticipantEntitiesInfo: {:?}", sample);
                    match cdr::deserialize_from::<_, ParticipantEntitiesInfo, _>(
                        ZBuf::from(sample).reader(),
                        cdr::size::Infinite,
                    ) {
                        Ok(i) => Some(i),
                        Err(e) => {
                            log::warn!(
                                "Error receiving ParticipantEntitiesInfo on ros_discovery_info: {}",
                                e
                            );
                            None
                        }
                    }
                })
                .collect()
        }
    }

    fn write(writer: dds_entity_t, info: &ParticipantEntitiesInfo) -> Result<(), String> {
        unsafe {
            let buf = cdr::serialize::<_, _, CdrLe>(info, Infinite)
                .map_err(|e| format!("Error serializing ParticipantEntitiesInfo: {e}"))?;

            let mut sertype: *const ddsi_sertype = std::ptr::null_mut();
            let ret = dds_get_entity_sertype(writer, &mut sertype);
            if ret < 0 {
                return Err(format!(
                    "Error creating payload for ParticipantEntitiesInfo: {}",
                    CStr::from_ptr(dds_strretcode(ret))
                        .to_str()
                        .unwrap_or("unrecoverable DDS retcode")
                ));
            }

            // As per the Vec documentation (see https://doc.rust-lang.org/std/vec/struct.Vec.html#method.into_raw_parts)
            // the only way to correctly releasing it is to create a vec using from_raw_parts
            // and then have its destructor do the cleanup.
            // Thus, while tempting to just pass the raw pointer to cyclone and then free it from C,
            // that is not necessarily safe or guaranteed to be leak free.
            // TODO replace when stable https://github.com/rust-lang/rust/issues/65816
            let (ptr, len, capacity) = crate::vec_into_raw_parts(buf);
            let size: ddsrt_iov_len_t = ddsrt_iov_len_from_usize(len)?;

            let data_out = ddsrt_iovec_t {
                iov_base: ptr as *mut std::ffi::c_void,
                iov_len: size,
            };

            let fwdp = ddsi_serdata_from_ser_iov(
                sertype,
                ddsi_serdata_kind_SDK_DATA,
                1,
                &data_out,
                size as usize,
            );
            dds_writecdr(writer, fwdp);
            drop(Vec::from_raw_parts(ptr, len, capacity));
            Ok(())
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct NodeEntitiesInfo {
    pub node_namespace: String,
    pub node_name: String,
    #[serde(
        serialize_with = "serialize_ros_gids",
        deserialize_with = "deserialize_ros_gids"
    )]
    pub reader_gid_seq: HashSet<Gid>,
    #[serde(
        serialize_with = "serialize_ros_gids",
        deserialize_with = "deserialize_ros_gids"
    )]
    pub writer_gid_seq: HashSet<Gid>,
}

impl NodeEntitiesInfo {
    pub fn new(node_namespace: String, node_name: String) -> NodeEntitiesInfo {
        NodeEntitiesInfo {
            node_namespace,
            node_name,
            reader_gid_seq: HashSet::new(),
            writer_gid_seq: HashSet::new(),
        }
    }

    pub fn full_name(&self) -> String {
        format!(
            "{}/{}",
            if &self.node_namespace == "/" {
                ""
            } else {
                &self.node_namespace
            },
            self.node_name,
        )
    }
}

impl std::fmt::Display for NodeEntitiesInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}",
            if &self.node_namespace == "/" {
                ""
            } else {
                &self.node_namespace
            },
            self.node_name,
        )?;
        Ok(())
    }
}

impl std::fmt::Debug for NodeEntitiesInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Node {}/{} :",
            if &self.node_namespace == "/" {
                ""
            } else {
                &self.node_namespace
            },
            self.node_name,
        )?;
        writeln!(f, "  {} pubs:", self.writer_gid_seq.len())?;
        for i in &self.writer_gid_seq {
            writeln!(f, "    {}", i)?;
        }
        writeln!(f, "  {} subs:", self.reader_gid_seq.len())?;
        for i in &self.reader_gid_seq {
            writeln!(f, "    {}", i)?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ParticipantEntitiesInfo {
    #[serde(
        serialize_with = "serialize_ros_gid",
        deserialize_with = "deserialize_ros_gid"
    )]
    pub gid: Gid,
    #[serde(
        serialize_with = "serialize_node_entities_info_seq",
        deserialize_with = "deserialize_node_entities_info_seq"
    )]
    pub node_entities_info_seq: HashMap<String, NodeEntitiesInfo>,
}

impl ParticipantEntitiesInfo {
    pub fn new(gid: Gid) -> Self {
        ParticipantEntitiesInfo {
            gid,
            node_entities_info_seq: HashMap::new(),
        }
    }
}

impl std::fmt::Display for ParticipantEntitiesInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "participant {} with nodes: [", self.gid)?;
        for (name, _) in self.node_entities_info_seq.iter().take(1) {
            write!(f, "{}", name)?;
        }
        for (name, _) in self.node_entities_info_seq.iter().skip(1) {
            write!(f, ", {}", name)?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

impl std::fmt::Debug for ParticipantEntitiesInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "participant {} :", self.gid)?;
        for i in self.node_entities_info_seq.values() {
            write!(f, "{i:?}")?;
        }
        Ok(())
    }
}

const BYTES_8: [u8; 8] = [0u8, 0, 0, 0, 0, 0, 0, 0];

fn serialize_ros_gid<S>(gid: &Gid, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if serializer.is_human_readable() {
        gid.serialize(serializer)
    } else {
        // Data size for gid in ROS messages in 24 bytes, while a DDS gid is 16 bytes.
        // Rely on "impl Serialize for Gid" for the 16 bytes, and add the last 8 bytes.
        Serialize::serialize(&(gid, &BYTES_8), serializer)
    }
}

fn deserialize_ros_gid<'de, D>(deserializer: D) -> Result<Gid, D::Error>
where
    D: Deserializer<'de>,
{
    if deserializer.is_human_readable() {
        // Rely on impl<'de> Deserialize<'de> for Gid
        Deserialize::deserialize(deserializer)
    } else {
        // Data size for gid in ROS messages in 24 bytes, while a DDS gid is 16 bytes.
        // Rely on "impl<'de> Deserialize<'de> for Gid" for the 16 bytes, and ignore the last 8 bytes
        let (result, _ignore): (Gid, [u8; 8]) = Deserialize::deserialize(deserializer)?;
        Ok(result)
    }
}

fn serialize_ros_gids<S>(gids: &HashSet<Gid>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let is_human_readable = serializer.is_human_readable();
    let mut seq: <S as Serializer>::SerializeSeq = serializer.serialize_seq(Some(gids.len()))?;
    for gid in gids {
        if is_human_readable {
            seq.serialize_element(gid)?;
        } else {
            // Data size for gid in ROS messages in 24 bytes, while a DDS gid is 16 bytes.
            // Rely on "impl Serialize for Gid" for the 16 bytes, and add the last 8 bytes.
            seq.serialize_element(&(gid, &BYTES_8))?;
        }
    }
    seq.end()
}

fn deserialize_ros_gids<'de, D>(deserializer: D) -> Result<HashSet<Gid>, D::Error>
where
    D: Deserializer<'de>,
{
    if deserializer.is_human_readable() {
        Deserialize::deserialize(deserializer)
    } else {
        // Data size for gid in ROS messages in 24 bytes, while a DDS gid is 16 bytes.
        // Deserialize as Vec<[u8; 24]>, consider 16 bytes only for each
        let ros_gids: Vec<[u8; 24]> = Deserialize::deserialize(deserializer)?;
        // NOTE: a DDS gid is 16 bytes only. ignore the last 8 bytes
        Ok(ros_gids
            .iter()
            .map(|ros_gid| {
                TryInto::<&[u8; 16]>::try_into(&ros_gid[..16])
                    .unwrap()
                    .into()
            })
            .collect())
    }
}

fn serialize_node_entities_info_seq<S>(
    entities: &HashMap<String, NodeEntitiesInfo>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(entities.len()))?;
    for entity in entities.values() {
        seq.serialize_element(entity)?;
    }
    seq.end()
}

fn deserialize_node_entities_info_seq<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, NodeEntitiesInfo>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut entities: Vec<NodeEntitiesInfo> = Deserialize::deserialize(deserializer)?;
    let mut map: HashMap<String, NodeEntitiesInfo> = HashMap::with_capacity(entities.len());
    for entity in entities.drain(..) {
        map.insert(entity.full_name(), entity);
    }
    Ok(map)
}

mod tests {

    #[test]
    fn test_serde() {
        use super::*;
        use std::str::FromStr;

        // ros_discovery_message sent by a component_container node started as such:
        //   - ros2 run rclcpp_components component_container --ros-args --remap __ns:=/TEST
        //   - ros2 component load /TEST/ComponentManager composition composition::Listener
        //   - ros2 component load /TEST/ComponentManager composition composition::Talker
        let ros_discovery_info_cdr: Vec<u8> = hex::decode(
            "000100000110de17b1eaf995400c9ac8000001c1000000000000000003000000\
            060000002f5445535400000011000000436f6d706f6e656e744d616e61676572\
            00000000040000000110de17b1eaf995400c9ac8000007040000000000000000\
            0110de17b1eaf995400c9ac80000090400000000000000000110de17b1eaf995\
            400c9ac800000b0400000000000000000110de17b1eaf995400c9ac800000d04\
            0000000000000000040000000110de17b1eaf995400c9ac80000060300000000\
            000000000110de17b1eaf995400c9ac80000080300000000000000000110de17\
            b1eaf995400c9ac800000a0300000000000000000110de17b1eaf995400c9ac8\
            00000c030000000000000000020000002f000000090000006c697374656e6572\
            00000000080000000110de17b1eaf995400c9ac8000010040000000000000000\
            0110de17b1eaf995400c9ac80000120400000000000000000110de17b1eaf995\
            400c9ac80000140400000000000000000110de17b1eaf995400c9ac800001604\
            00000000000000000110de17b1eaf995400c9ac8000018040000000000000000\
            0110de17b1eaf995400c9ac800001a0400000000000000000110de17b1eaf995\
            400c9ac800001c0400000000000000000110de17b1eaf995400c9ac800001d04\
            0000000000000000080000000110de17b1eaf995400c9ac800000e0300000000\
            000000000110de17b1eaf995400c9ac800000f0300000000000000000110de17\
            b1eaf995400c9ac80000110300000000000000000110de17b1eaf995400c9ac8\
            0000130300000000000000000110de17b1eaf995400c9ac80000150300000000\
            000000000110de17b1eaf995400c9ac80000170300000000000000000110de17\
            b1eaf995400c9ac80000190300000000000000000110de17b1eaf995400c9ac8\
            00001b030000000000000000020000002f0000000700000074616c6b65720000\
            070000000110de17b1eaf995400c9ac80000200400000000000000000110de17\
            b1eaf995400c9ac80000220400000000000000000110de17b1eaf995400c9ac8\
            0000240400000000000000000110de17b1eaf995400c9ac80000260400000000\
            000000000110de17b1eaf995400c9ac80000280400000000000000000110de17\
            b1eaf995400c9ac800002a0400000000000000000110de17b1eaf995400c9ac8\
            00002c040000000000000000090000000110de17b1eaf995400c9ac800001e03\
            00000000000000000110de17b1eaf995400c9ac800001f030000000000000000\
            0110de17b1eaf995400c9ac80000210300000000000000000110de17b1eaf995\
            400c9ac80000230300000000000000000110de17b1eaf995400c9ac800002503\
            00000000000000000110de17b1eaf995400c9ac8000027030000000000000000\
            0110de17b1eaf995400c9ac80000290300000000000000000110de17b1eaf995\
            400c9ac800002b0300000000000000000110de17b1eaf995400c9ac800002d03\
            0000000000000000",
        )
        .unwrap();

        let part_info: ParticipantEntitiesInfo = cdr::deserialize(&ros_discovery_info_cdr).unwrap();
        println!("{:?}", part_info);

        assert_eq!(
            part_info.gid,
            Gid::from_str("0110de17b1eaf995400c9ac8000001c1").unwrap()
        );
        assert_eq!(part_info.node_entities_info_seq.len(), 3);

        let node_componentmgr = part_info
            .node_entities_info_seq
            .get("/TEST/ComponentManager")
            .unwrap();
        assert_eq!(node_componentmgr.node_namespace, "/TEST");
        assert_eq!(node_componentmgr.node_name, "ComponentManager");
        assert_eq!(node_componentmgr.reader_gid_seq.len(), 4);
        assert_eq!(node_componentmgr.writer_gid_seq.len(), 4);

        let node_listener = part_info.node_entities_info_seq.get("/listener").unwrap();
        assert_eq!(node_listener.node_namespace, "/");
        assert_eq!(node_listener.node_name, "listener");
        assert_eq!(node_listener.reader_gid_seq.len(), 8);
        assert_eq!(node_listener.writer_gid_seq.len(), 8);

        let node_talker = part_info.node_entities_info_seq.get("/talker").unwrap();
        assert_eq!(node_talker.node_namespace, "/");
        assert_eq!(node_talker.node_name, "talker");
        assert_eq!(node_talker.reader_gid_seq.len(), 7);
        assert_eq!(node_talker.writer_gid_seq.len(), 9);
    }
}
