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

use std::{
    collections::HashMap,
    fmt::{self, Debug},
};

use zenoh::{
    bytes::{Encoding, ZBytes},
    key_expr::{
        format::{kedefine, keformat},
        keyexpr, OwnedKeyExpr,
    },
    query::Query,
};

use crate::{
    dds_discovery::{DdsEntity, DdsParticipant},
    events::ROS2DiscoveryEvent,
    gid::Gid,
    node_info::*,
    ros_discovery::{NodeEntitiesInfo, ParticipantEntitiesInfo},
};

kedefine!(
    pub(crate) ke_admin_participant: "dds/${pgid:*}",
    pub(crate) ke_admin_writer: "dds/${pgid:*}/writer/${wgid:*}/${topic:**}",
    pub(crate) ke_admin_reader: "dds/${pgid:*}/reader/${wgid:*}/${topic:**}",
    pub(crate) ke_admin_node: "node/${node_id:**}",
);

#[derive(Default)]
pub struct DiscoveredEntities {
    participants: HashMap<Gid, DdsParticipant>,
    writers: HashMap<Gid, DdsEntity>,
    readers: HashMap<Gid, DdsEntity>,
    ros_participant_info: HashMap<Gid, ParticipantEntitiesInfo>,
    nodes_info: HashMap<Gid, HashMap<String, NodeInfo>>,
    admin_space: HashMap<OwnedKeyExpr, EntityRef>,
}

impl Debug for DiscoveredEntities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "participants: {:?}",
            self.participants.keys().collect::<Vec<&Gid>>()
        )?;
        writeln!(
            f,
            "writers: {:?}",
            self.writers.keys().collect::<Vec<&Gid>>()
        )?;
        writeln!(
            f,
            "readers: {:?}",
            self.readers.keys().collect::<Vec<&Gid>>()
        )?;
        writeln!(f, "ros_participant_info: {:?}", self.ros_participant_info)?;
        writeln!(f, "nodes_info: {:?}", self.nodes_info)?;
        writeln!(
            f,
            "admin_space: {:?}",
            self.admin_space.keys().collect::<Vec<&OwnedKeyExpr>>()
        )
    }
}

#[derive(Debug)]
enum EntityRef {
    Participant(Gid),
    Writer(Gid),
    Reader(Gid),
    Node(Gid, String),
}

impl DiscoveredEntities {
    #[inline]
    pub fn add_participant(&mut self, participant: DdsParticipant) {
        self.admin_space.insert(
            keformat!(ke_admin_participant::formatter(), pgid = participant.key).unwrap(),
            EntityRef::Participant(participant.key),
        );
        self.participants.insert(participant.key, participant);
    }

    #[inline]
    pub fn remove_participant(&mut self, gid: &Gid) -> Vec<ROS2DiscoveryEvent> {
        let mut events: Vec<ROS2DiscoveryEvent> = Vec::new();
        // Remove Participant from participants list and from admin_space
        self.participants.remove(gid);
        self.admin_space
            .remove(&keformat!(ke_admin_participant::formatter(), pgid = gid).unwrap());
        // Remove associated NodeInfos
        if let Some(nodes) = self.nodes_info.remove(gid) {
            for (name, mut node) in nodes {
                tracing::info!("Undiscovered ROS Node {}", name);
                self.admin_space.remove(
                    &keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                        .unwrap(),
                );
                // return undiscovery events for this node
                events.append(&mut node.remove_all_entities());
            }
        }
        events
    }

    #[inline]
    pub fn add_writer(&mut self, writer: DdsEntity) -> Option<ROS2DiscoveryEvent> {
        // insert in admin space
        self.admin_space.insert(
            keformat!(
                ke_admin_writer::formatter(),
                pgid = writer.participant_key,
                wgid = writer.key,
                topic = &writer.topic_name,
            )
            .unwrap(),
            EntityRef::Writer(writer.key),
        );

        // Check if this Writer is present in some NodeInfo.undiscovered_writer list
        let mut event: Option<ROS2DiscoveryEvent> = None;
        for nodes_map in self.nodes_info.values_mut() {
            for node in nodes_map.values_mut() {
                if let Some(i) = node
                    .undiscovered_writer
                    .iter()
                    .position(|gid| gid == &writer.key)
                {
                    // update the NodeInfo with this Writer's info
                    node.undiscovered_writer.remove(i);
                    event = node.update_with_writer(&writer);
                    break;
                }
            }
            if event.is_some() {
                break;
            }
        }

        // insert in Writers list
        self.writers.insert(writer.key, writer);
        event
    }

    #[inline]
    pub fn get_writer(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.writers.get(gid)
    }

    #[inline]
    pub fn remove_writer(&mut self, gid: &Gid) -> Option<ROS2DiscoveryEvent> {
        if let Some(writer) = self.writers.remove(gid) {
            self.admin_space.remove(
                &keformat!(
                    ke_admin_writer::formatter(),
                    pgid = writer.participant_key,
                    wgid = writer.key,
                    topic = &writer.topic_name,
                )
                .unwrap(),
            );

            // Remove the Writer from any NodeInfo that might use it, possibly leading to a UndiscoveredX event
            for nodes_map in self.nodes_info.values_mut() {
                for node in nodes_map.values_mut() {
                    if let Some(e) = node.remove_writer(gid) {
                        // A Reader can be used by only 1 Node, no need to go on with loops
                        return Some(e);
                    }
                }
            }
        }
        None
    }

    #[inline]
    pub fn add_reader(&mut self, reader: DdsEntity) -> Option<ROS2DiscoveryEvent> {
        // insert in admin space
        self.admin_space.insert(
            keformat!(
                ke_admin_reader::formatter(),
                pgid = reader.participant_key,
                wgid = reader.key,
                topic = &reader.topic_name,
            )
            .unwrap(),
            EntityRef::Reader(reader.key),
        );

        // Check if this Reader is present in some NodeInfo.undiscovered_reader list
        let mut event = None;
        for nodes_map in self.nodes_info.values_mut() {
            for node in nodes_map.values_mut() {
                if let Some(i) = node
                    .undiscovered_reader
                    .iter()
                    .position(|gid| gid == &reader.key)
                {
                    // update the NodeInfo with this Reader's info
                    node.undiscovered_reader.remove(i);
                    event = node.update_with_reader(&reader);
                    break;
                }
            }
            if event.is_some() {
                break;
            }
        }

        // insert in Readers list
        self.readers.insert(reader.key, reader);
        event
    }

    #[inline]
    pub fn get_reader(&self, gid: &Gid) -> Option<&DdsEntity> {
        self.readers.get(gid)
    }

    #[inline]
    pub fn remove_reader(&mut self, gid: &Gid) -> Option<ROS2DiscoveryEvent> {
        if let Some(reader) = self.readers.remove(gid) {
            self.admin_space.remove(
                &keformat!(
                    ke_admin_reader::formatter(),
                    pgid = reader.participant_key,
                    wgid = reader.key,
                    topic = &reader.topic_name,
                )
                .unwrap(),
            );

            // Remove the Reader from any NodeInfo that might use it, possibly leading to a UndiscoveredX event
            for nodes_map in self.nodes_info.values_mut() {
                for node in nodes_map.values_mut() {
                    if let Some(e) = node.remove_reader(gid) {
                        // A Reader can be used by only 1 Node, no need to go on with loops
                        return Some(e);
                    }
                }
            }
        }
        None
    }

    pub fn update_participant_info(
        &mut self,
        ros_info: ParticipantEntitiesInfo,
    ) -> Vec<ROS2DiscoveryEvent> {
        let mut events: Vec<ROS2DiscoveryEvent> = Vec::new();
        let Self {
            writers,
            readers,
            nodes_info,
            admin_space,
            ..
        } = self;
        let nodes_map = nodes_info.entry(ros_info.gid).or_insert_with(HashMap::new);

        // Remove nodes that are no longer present in ParticipantEntitiesInfo
        nodes_map.retain(|name, node| {
            if !ros_info.node_entities_info_seq.contains_key(name) {
                tracing::info!("Undiscovered ROS Node {}", name);
                admin_space.remove(
                    &keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                        .unwrap(),
                );
                // return undiscovery events for this node
                events.append(&mut node.remove_all_entities());
                false
            } else {
                true
            }
        });

        // For each declared node in this ros_node_info
        for (name, ros_node_info) in &ros_info.node_entities_info_seq {
            // If node was not yet discovered, add a new NodeInfo
            if !nodes_map.contains_key(name) {
                tracing::info!("Discovered ROS Node {}", name);
                match NodeInfo::create(
                    ros_node_info.node_namespace.clone(),
                    ros_node_info.node_name.clone(),
                    ros_info.gid,
                ) {
                    Ok(node) => {
                        self.admin_space.insert(
                            keformat!(ke_admin_node::formatter(), node_id = node.id_as_keyexpr(),)
                                .unwrap(),
                            EntityRef::Node(ros_info.gid, node.fullname().to_string()),
                        );
                        nodes_map.insert(node.fullname().to_string(), node);
                    }
                    Err(e) => {
                        tracing::warn!("ROS Node has incompatible name: {e}");
                        break;
                    }
                }
            };

            // Update NodeInfo, adding resulting events to the list
            let node = nodes_map.get_mut(name).unwrap();
            events.append(&mut Self::update_node_info(
                node,
                ros_node_info,
                readers,
                writers,
            ));
        }

        // Save ParticipantEntitiesInfo
        self.ros_participant_info.insert(ros_info.gid, ros_info);
        events
    }

    pub fn update_node_info(
        node: &mut NodeInfo,
        ros_node_info: &NodeEntitiesInfo,
        readers: &mut HashMap<Gid, DdsEntity>,
        writers: &mut HashMap<Gid, DdsEntity>,
    ) -> Vec<ROS2DiscoveryEvent> {
        let mut events = Vec::new();
        // For each declared Reader
        for rgid in &ros_node_info.reader_gid_seq {
            if let Some(entity) = readers.get(rgid) {
                tracing::trace!(
                    "ROS Node {ros_node_info} declares a Reader on {}",
                    entity.topic_name
                );
                if let Some(e) = node.update_with_reader(entity) {
                    tracing::debug!(
                        "ROS Node {ros_node_info} declares a new Reader on {}",
                        entity.topic_name
                    );
                    events.push(e)
                };
            } else {
                tracing::debug!(
                    "ROS Node {ros_node_info} declares a not yet discovered DDS Reader: {rgid}"
                );
                node.undiscovered_reader.push(*rgid);
            }
        }
        // For each declared Writer
        for wgid in &ros_node_info.writer_gid_seq {
            if let Some(entity) = writers.get(wgid) {
                tracing::trace!(
                    "ROS Node {ros_node_info} declares Writer on {}",
                    entity.topic_name
                );
                if let Some(e) = node.update_with_writer(entity) {
                    tracing::debug!(
                        "ROS Node {ros_node_info} declares a new Writer on {}",
                        entity.topic_name
                    );
                    events.push(e)
                };
            } else {
                tracing::debug!(
                    "ROS Node {ros_node_info} declares a not yet discovered DDS Writer: {wgid}"
                );
                node.undiscovered_writer.push(*wgid);
            }
        }
        events
    }

    fn get_entity_json_value(
        &self,
        entity_ref: &EntityRef,
    ) -> Result<Option<serde_json::Value>, serde_json::Error> {
        match entity_ref {
            EntityRef::Participant(gid) => self
                .participants
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Writer(gid) => self
                .writers
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Reader(gid) => self
                .readers
                .get(gid)
                .map(serde_json::to_value)
                .map(remove_null_qos_values)
                .transpose(),
            EntityRef::Node(gid, name) => self
                .nodes_info
                .get(gid)
                .and_then(|map| map.get(name))
                .map(serde_json::to_value)
                .transpose(),
        }
    }

    pub async fn treat_admin_query(&self, query: &Query, admin_keyexpr_prefix: &keyexpr) {
        let selector = query.selector();

        // get the list of sub-key expressions that will match the same stored keys than
        // the selector, if those keys had the admin_keyexpr_prefix.
        let sub_kes = selector.key_expr().strip_prefix(admin_keyexpr_prefix);
        if sub_kes.is_empty() {
            tracing::error!("Received query for admin space: '{}' - but it's not prefixed by admin_keyexpr_prefix='{}'", selector, admin_keyexpr_prefix);
            return;
        }

        // For all sub-key expression
        for sub_ke in sub_kes {
            if sub_ke.is_wild() {
                // iterate over all admin space to find matching keys and reply for each
                for (ke, entity_ref) in self.admin_space.iter() {
                    if sub_ke.intersects(ke) {
                        self.send_admin_reply(query, admin_keyexpr_prefix, ke, entity_ref)
                            .await;
                    }
                }
            } else {
                // sub_ke correspond to 1 key - just get it and reply
                if let Some(entity_ref) = self.admin_space.get(sub_ke) {
                    self.send_admin_reply(query, admin_keyexpr_prefix, sub_ke, entity_ref)
                        .await;
                }
            }
        }
    }

    async fn send_admin_reply(
        &self,
        query: &Query,
        admin_keyexpr_prefix: &keyexpr,
        key_expr: &keyexpr,
        entity_ref: &EntityRef,
    ) {
        match self.get_entity_json_value(entity_ref) {
            Ok(Some(v)) => {
                let admin_keyexpr = admin_keyexpr_prefix / key_expr;
                match serde_json::to_vec(&v) {
                    Ok(bytes) => {
                        if let Err(e) = query
                            .reply(admin_keyexpr, ZBytes::from(bytes))
                            .encoding(Encoding::APPLICATION_JSON)
                            .await
                        {
                            tracing::warn!("Error replying to admin query {:?}: {}", query, e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Error transforming JSON to admin query {:?}: {}", query, e);
                    }
                }
            }
            Ok(None) => {
                tracing::error!("INTERNAL ERROR: Dangling {:?} for {}", entity_ref, key_expr)
            }
            Err(e) => {
                tracing::error!("INTERNAL ERROR serializing admin value as JSON: {}", e)
            }
        }
    }
}

// Remove any null QoS values from a serde_json::Value
fn remove_null_qos_values(
    value: Result<serde_json::Value, serde_json::Error>,
) -> Result<serde_json::Value, serde_json::Error> {
    match value {
        Ok(value) => match value {
            serde_json::Value::Object(mut obj) => {
                let qos = obj.get_mut("qos");
                if let Some(qos) = qos {
                    if qos.is_object() {
                        qos.as_object_mut().unwrap().retain(|_, v| !v.is_null());
                    }
                }
                Ok(serde_json::Value::Object(obj))
            }
            _ => Ok(value),
        },
        Err(error) => Err(error),
    }
}
