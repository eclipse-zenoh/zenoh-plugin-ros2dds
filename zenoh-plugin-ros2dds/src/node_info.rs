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
    collections::{hash_map::Entry, HashMap, HashSet},
    ops::Range,
};

use serde::{ser::SerializeSeq, Serialize, Serializer};
use zenoh::key_expr::{keyexpr, KeyExpr};

use crate::{dds_discovery::DdsEntity, events::ROS2DiscoveryEvent, gid::Gid, ros2_utils::*};

#[derive(Clone, Debug, Serialize)]
pub struct MsgPub {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    // List of DDS Writers declared by 1 Node for a same topic
    // Issue #27: usually only 1 Writer, but may happen that 1 Node declares several Publishers
    //            on the same topic. In this case `ros2 node info <node_id>` still shows only 1 Publisher,
    //            but all the writers can be seen with `ros2 topic info <topic_name> -v`.
    //            Hence the choice here to aggregate all the Writers in 1 Publisher representation.
    //            The Publisher is declared undiscovered only when all its Writers are undiscovered.
    #[serde(skip)]
    pub writers: HashSet<Gid>,
}

impl MsgPub {
    pub fn create(name: String, typ: String, writer: Gid) -> Result<MsgPub, String> {
        check_ros_name(&name)?;
        Ok(MsgPub {
            name,
            typ,
            writers: HashSet::from([writer]),
        })
    }

    pub fn name_as_keyexpr(&self) -> &keyexpr {
        unsafe { keyexpr::from_str_unchecked(&self.name[1..]) }
    }
}

impl std::fmt::Display for MsgPub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Publisher {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct MsgSub {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    // List of DDS Readers declared by 1 Node for a same topic
    // Issue #27: usually only 1 Reader, but may happen that 1 Node declares several Subscribers
    //            on the same topic. In this case `ros2 node info <node_id>` still shows only 1 Subscriber,
    //            but all the writers can be seen with `ros2 topic info <topic_name> -v`.
    //            Hence the choice here to aggregate all the Readers in 1 Subscriber representation.
    //            The Subscriber is declared undiscovered only when all its Readers are undiscovered.
    #[serde(skip)]
    pub readers: HashSet<Gid>,
}

impl MsgSub {
    pub fn create(name: String, typ: String, reader: Gid) -> Result<MsgSub, String> {
        check_ros_name(&name)?;
        Ok(MsgSub {
            name,
            typ,
            readers: HashSet::from([reader]),
        })
    }

    pub fn name_as_keyexpr(&self) -> &keyexpr {
        unsafe { keyexpr::from_str_unchecked(&self.name[1..]) }
    }
}

impl std::fmt::Display for MsgSub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subscriber {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct ServiceSrvEntities {
    pub req_reader: Gid,
    pub rep_writer: Gid,
}

impl ServiceSrvEntities {
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.req_reader != Gid::NOT_DISCOVERED && self.rep_writer != Gid::NOT_DISCOVERED
    }
}

impl std::fmt::Debug for ServiceSrvEntities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{reqR:{:?}, repW:{:?}}}",
            self.req_reader, self.rep_writer
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ServiceSrv {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip)]
    pub entities: ServiceSrvEntities,
}

impl ServiceSrv {
    pub fn create(name: String, typ: String) -> Result<ServiceSrv, String> {
        check_ros_name(&name)?;
        Ok(ServiceSrv {
            name,
            typ,
            entities: ServiceSrvEntities::default(),
        })
    }

    pub fn name_as_keyexpr(&self) -> &keyexpr {
        unsafe { keyexpr::from_str_unchecked(&self.name[1..]) }
    }

    #[inline]
    pub fn is_complete(&self) -> bool {
        self.entities.is_complete()
    }
}

impl std::fmt::Display for ServiceSrv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Service Server {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct ServiceCliEntities {
    pub req_writer: Gid,
    pub rep_reader: Gid,
}

impl ServiceCliEntities {
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.rep_reader != Gid::NOT_DISCOVERED && self.req_writer != Gid::NOT_DISCOVERED
    }
}

impl std::fmt::Debug for ServiceCliEntities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{reqW:{:?}, repR:{:?}}}",
            self.req_writer, self.rep_reader
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ServiceCli {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip)]
    pub entities: ServiceCliEntities,
}

impl ServiceCli {
    pub fn create(name: String, typ: String) -> Result<ServiceCli, String> {
        check_ros_name(&name)?;
        Ok(ServiceCli {
            name,
            typ,
            entities: ServiceCliEntities::default(),
        })
    }

    pub fn name_as_keyexpr(&self) -> &keyexpr {
        unsafe { keyexpr::from_str_unchecked(&self.name[1..]) }
    }

    #[inline]
    pub fn is_complete(&self) -> bool {
        self.entities.is_complete()
    }
}

impl std::fmt::Display for ServiceCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Service Client {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct ActionSrvEntities {
    pub send_goal: ServiceSrvEntities,
    pub cancel_goal: ServiceSrvEntities,
    pub get_result: ServiceSrvEntities,
    pub status_writer: Gid,
    pub feedback_writer: Gid,
}

impl ActionSrvEntities {
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.send_goal.is_complete()
            && self.cancel_goal.is_complete()
            && self.get_result.is_complete()
            && self.status_writer != Gid::NOT_DISCOVERED
            && self.feedback_writer != Gid::NOT_DISCOVERED
    }
}

impl std::fmt::Debug for ActionSrvEntities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{send_goal{:?}, cancel_goal{:?}, get_result{:?}, statusW:{:?}, feedbackW:{:?}}}",
            self.send_goal,
            self.cancel_goal,
            self.get_result,
            self.status_writer,
            self.feedback_writer,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ActionSrv {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip)]
    pub entities: ActionSrvEntities,
}

impl ActionSrv {
    pub fn create(name: String, typ: String) -> Result<ActionSrv, String> {
        check_ros_name(&name)?;
        Ok(ActionSrv {
            name,
            typ,
            entities: ActionSrvEntities::default(),
        })
    }

    pub fn name_as_keyexpr(&self) -> &keyexpr {
        unsafe { keyexpr::from_str_unchecked(&self.name[1..]) }
    }

    #[inline]
    pub fn is_complete(&self) -> bool {
        self.entities.is_complete()
    }
}

impl std::fmt::Display for ActionSrv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Action Server {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub struct ActionCliEntities {
    pub send_goal: ServiceCliEntities,
    pub cancel_goal: ServiceCliEntities,
    pub get_result: ServiceCliEntities,
    pub status_reader: Gid,
    pub feedback_reader: Gid,
}

impl ActionCliEntities {
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.send_goal.is_complete()
            && self.cancel_goal.is_complete()
            && self.get_result.is_complete()
            && self.status_reader != Gid::NOT_DISCOVERED
            && self.feedback_reader != Gid::NOT_DISCOVERED
    }
}

impl std::fmt::Debug for ActionCliEntities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{send_goal{:?}, cancel_goal{:?}, get_result{:?}, statusR:{:?}, feedbackR:{:?}}}",
            self.send_goal,
            self.cancel_goal,
            self.get_result,
            self.status_reader,
            self.feedback_reader,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ActionCli {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(skip)]
    pub entities: ActionCliEntities,
}

impl ActionCli {
    pub fn create(name: String, typ: String) -> Result<ActionCli, String> {
        check_ros_name(&name)?;
        Ok(ActionCli {
            name,
            typ,
            entities: ActionCliEntities::default(),
        })
    }

    pub fn name_as_keyexpr(&self) -> &keyexpr {
        unsafe { keyexpr::from_str_unchecked(&self.name[1..]) }
    }

    #[inline]
    pub fn is_complete(&self) -> bool {
        self.entities.is_complete()
    }
}

impl std::fmt::Display for ActionCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Action Client {}: {}", self.name, self.typ)?;
        Ok(())
    }
}

#[derive(Serialize)]
pub struct NodeInfo {
    // The node unique id is: <participant_gid>/<namespace>/<name>
    #[serde(skip)]
    pub id: String,
    #[serde(skip)]
    fullname: Range<usize>,
    #[serde(skip)]
    namespace: Range<usize>,
    #[serde(skip)]
    name: Range<usize>,
    #[serde(rename = "publishers", serialize_with = "serialize_hashmap_values")]
    pub msg_pub: HashMap<String, MsgPub>,
    #[serde(rename = "subscribers", serialize_with = "serialize_hashmap_values")]
    pub msg_sub: HashMap<String, MsgSub>,
    #[serde(
        rename = "service_servers",
        serialize_with = "serialize_hashmap_values"
    )]
    pub service_srv: HashMap<String, ServiceSrv>,
    #[serde(
        rename = "service_clients",
        serialize_with = "serialize_hashmap_values"
    )]
    pub service_cli: HashMap<String, ServiceCli>,
    #[serde(rename = "action_servers", serialize_with = "serialize_hashmap_values")]
    pub action_srv: HashMap<String, ActionSrv>,
    #[serde(rename = "action_clients", serialize_with = "serialize_hashmap_values")]
    pub action_cli: HashMap<String, ActionCli>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub undiscovered_reader: Vec<Gid>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub undiscovered_writer: Vec<Gid>,
}

impl std::fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.id)
    }
}

impl std::fmt::Debug for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}  (namespace={}, name={})",
            self.id,
            self.namespace(),
            self.name()
        )
    }
}

impl NodeInfo {
    pub fn create(
        node_namespace: String,
        node_name: String,
        participant: Gid,
    ) -> Result<NodeInfo, String> {
        // Construct id as "<participant_gid></namespace>/<name>", keeping Ranges for namespace and name
        let mut id = participant.to_string();
        let namespace_start: usize = id.len();
        id.push_str(&node_namespace);
        let namespace = Range {
            start: namespace_start,
            end: id.len(),
        };
        if node_namespace != "/" {
            id.push('/')
        }
        let name_start = id.len();
        id.push_str(&node_name);
        let name = Range {
            start: name_start,
            end: id.len(),
        };
        let fullname = Range {
            start: namespace_start,
            end: id.len(),
        };

        // Check is resulting id is a valid key expression
        if let Err(e) = KeyExpr::try_from(&id) {
            return Err(format!(
                "Incompatible ROS Node: '{id}' cannot be converted as a Zenoh key expression: {e}"
            ));
        }

        Ok(NodeInfo {
            id,
            fullname,
            namespace,
            name,
            msg_pub: HashMap::new(),
            msg_sub: HashMap::new(),
            service_srv: HashMap::new(),
            service_cli: HashMap::new(),
            action_srv: HashMap::new(),
            action_cli: HashMap::new(),
            undiscovered_reader: Vec::new(),
            undiscovered_writer: Vec::new(),
        })
    }

    #[inline]
    pub fn fullname(&self) -> &str {
        &self.id[self.fullname.clone()]
    }

    #[inline]
    pub fn namespace(&self) -> &str {
        &self.id[self.namespace.clone()]
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.id[self.name.clone()]
    }

    #[inline]
    pub fn id_as_keyexpr(&self) -> &keyexpr {
        unsafe { keyexpr::from_str_unchecked(&self.id) }
    }

    pub fn update_with_reader(&mut self, entity: &DdsEntity) -> Option<ROS2DiscoveryEvent> {
        let topic_prefix = &entity.topic_name[..3];
        let topic_suffix = &entity.topic_name[2..];
        match topic_prefix {
            "rt/" if topic_suffix.ends_with("/_action/status") => self
                .update_action_cli_status_reader(
                    &topic_suffix[..topic_suffix.len() - 15],
                    &entity.key,
                ),
            "rt/" if topic_suffix.ends_with("/_action/feedback") => self
                .update_action_cli_feedback_reader(
                    &topic_suffix[..topic_suffix.len() - 17],
                    dds_type_to_ros2_action_type(&entity.type_name),
                    &entity.key,
                ),
            "rt/" => self.update_msg_sub(
                topic_suffix,
                dds_type_to_ros2_message_type(&entity.type_name),
                &entity.key,
            ),
            "rq/" if topic_suffix.ends_with("/_action/send_goalRequest") => self
                .update_action_srv_send_req_reader(
                    &topic_suffix[..topic_suffix.len() - 25],
                    dds_type_to_ros2_action_type(&entity.type_name),
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("/_action/cancel_goalRequest") => self
                .update_action_srv_cancel_req_reader(
                    &topic_suffix[..topic_suffix.len() - 27],
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("/_action/get_resultRequest") => self
                .update_action_srv_result_req_reader(
                    &topic_suffix[..topic_suffix.len() - 26],
                    dds_type_to_ros2_action_type(&entity.type_name),
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("Request") => self.update_service_srv_req_reader(
                &topic_suffix[..topic_suffix.len() - 7],
                dds_type_to_ros2_service_type(&entity.type_name),
                &entity.key,
            ),
            "rr/" if topic_suffix.ends_with("/_action/send_goalReply") => self
                .update_action_cli_send_rep_reader(
                    &topic_suffix[..topic_suffix.len() - 23],
                    dds_type_to_ros2_action_type(&entity.type_name),
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("/_action/cancel_goalReply") => self
                .update_action_cli_cancel_rep_reader(
                    &topic_suffix[..topic_suffix.len() - 25],
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("/_action/get_resultReply") => self
                .update_action_cli_result_rep_reader(
                    &topic_suffix[..topic_suffix.len() - 24],
                    dds_type_to_ros2_action_type(&entity.type_name),
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("Reply") => self.update_service_cli_rep_reader(
                &topic_suffix[..topic_suffix.len() - 5],
                dds_type_to_ros2_service_type(&entity.type_name),
                &entity.key,
            ),
            _ => {
                tracing::warn!(
                    r#"ROS Node {self} uses unexpected DDS topic "{}" - ignored"#,
                    entity.topic_name
                );
                None
            }
        }
    }

    pub fn update_with_writer(&mut self, entity: &DdsEntity) -> Option<ROS2DiscoveryEvent> {
        let topic_prefix = &entity.topic_name[..3];
        let topic_suffix = &entity.topic_name[2..];
        match topic_prefix {
            "rt/" if topic_suffix.ends_with("/_action/status") => self
                .update_action_srv_status_writer(
                    &topic_suffix[..topic_suffix.len() - 15],
                    &entity.key,
                ),
            "rt/" if topic_suffix.ends_with("/_action/feedback") => self
                .update_action_srv_feedback_writer(
                    &topic_suffix[..topic_suffix.len() - 17],
                    dds_type_to_ros2_action_type(&entity.type_name),
                    &entity.key,
                ),
            "rt/" => self.update_msg_pub(
                topic_suffix,
                dds_type_to_ros2_message_type(&entity.type_name),
                &entity.key,
            ),
            "rq/" if topic_suffix.ends_with("/_action/send_goalRequest") => self
                .update_action_cli_send_req_writer(
                    &topic_suffix[..topic_suffix.len() - 25],
                    dds_type_to_ros2_action_type(&entity.type_name),
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("/_action/cancel_goalRequest") => self
                .update_action_cli_cancel_req_writer(
                    &topic_suffix[..topic_suffix.len() - 27],
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("/_action/get_resultRequest") => self
                .update_action_cli_result_req_writer(
                    &topic_suffix[..topic_suffix.len() - 26],
                    dds_type_to_ros2_action_type(&entity.type_name),
                    &entity.key,
                ),
            "rq/" if topic_suffix.ends_with("Request") => self.update_service_cli_req_writer(
                &topic_suffix[..topic_suffix.len() - 7],
                dds_type_to_ros2_service_type(&entity.type_name),
                &entity.key,
            ),
            "rr/" if topic_suffix.ends_with("/_action/send_goalReply") => self
                .update_action_srv_send_rep_writer(
                    &topic_suffix[..topic_suffix.len() - 23],
                    dds_type_to_ros2_action_type(&entity.type_name),
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("/_action/cancel_goalReply") => self
                .update_action_srv_cancel_rep_writer(
                    &topic_suffix[..topic_suffix.len() - 25],
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("/_action/get_resultReply") => self
                .update_action_srv_result_rep_writer(
                    &topic_suffix[..topic_suffix.len() - 24],
                    dds_type_to_ros2_action_type(&entity.type_name),
                    &entity.key,
                ),
            "rr/" if topic_suffix.ends_with("Reply") => self.update_service_srv_rep_writer(
                &topic_suffix[..topic_suffix.len() - 5],
                dds_type_to_ros2_service_type(&entity.type_name),
                &entity.key,
            ),
            _ => {
                tracing::warn!(
                    r#"ROS Node {self} uses unexpected DDS topic "{}" - ignored"#,
                    entity.topic_name
                );
                None
            }
        }
    }

    // Update MsgPub, returing a ROS2DiscoveryEvent::DiscoveredMsgSub if new or changed
    fn update_msg_pub(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredMsgPub;
        let node_fullname = self.fullname().to_string();
        match self.msg_pub.entry(name.into()) {
            Entry::Vacant(e) => match MsgPub::create(name.into(), typ, *writer) {
                Ok(t) => {
                    e.insert(t.clone());
                    Some(DiscoveredMsgPub(node_fullname, t))
                }
                Err(e) => {
                    tracing::error!(
                        "ROS Node {node_fullname} declared an incompatible Publisher: {e} - ignored"
                    );
                    None
                }
            },
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result: Option<ROS2DiscoveryEvent> = None;
                if v.typ != typ {
                    tracing::error!(
                        r#"ROS Node {node_fullname} declares 2 Publishers on same topic {name} but with different types: {} vs {typ} - Publisher with 2nd type ignored""#,
                        v.typ
                    );
                } else if v.writers.insert(*writer) && v.writers.len() == 1 {
                    // Send DiscoveredMsgPub event only for the 1st discovered Writer
                    result = Some(DiscoveredMsgPub(node_fullname, v.clone()));
                }
                result
            }
        }
    }

    // Update MsgSub, returing a ROS2DiscoveryEvent::DiscoveredMsgSub if new or changed
    fn update_msg_sub(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredMsgSub;
        let node_fullname = self.fullname().to_string();
        match self.msg_sub.entry(name.into()) {
            Entry::Vacant(e) => match MsgSub::create(name.into(), typ, *reader) {
                Ok(t) => {
                    e.insert(t.clone());
                    Some(DiscoveredMsgSub(node_fullname, t))
                }
                Err(e) => {
                    tracing::error!(
                        "ROS Node {node_fullname} declared an incompatible Subscriber: {e} - ignored"
                    );
                    None
                }
            },
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result: Option<ROS2DiscoveryEvent> = None;
                if v.typ != typ {
                    tracing::error!(
                        r#"ROS Node {node_fullname} declares 2 Subscriber on same topic {name} but with different types: {} vs {typ} - Publisher with 2nd type ignored""#,
                        v.typ
                    );
                } else if v.readers.insert(*reader) && v.readers.len() == 1 {
                    // Send DiscoveredMsgSub event only for the 1st discovered Reader
                    result = Some(DiscoveredMsgSub(node_fullname, v.clone()));
                }
                result
            }
        }
    }

    // Update ServiceSrv, returing a ROS2DiscoveryEvent::DiscoveredServiceSrv if new and complete or changed
    fn update_service_srv_req_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredServiceSrv;
        let node_fullname = self.fullname().to_string();
        match self.service_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                match ServiceSrv::create(name.into(), typ) {
                    Ok(mut s) => {
                        s.entities.req_reader = *reader;
                        e.insert(s);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Service Server: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    tracing::warn!(
                        r#"ROS declaration of Service Server "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceSrv(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.req_reader != *reader {
                    if v.entities.req_reader != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Service Server "{v}" changed it's Request DDS Reader's GID from {} to {reader}"#,
                            v.entities.req_reader
                        );
                    }
                    v.entities.req_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceSrv(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ServiceSrv, returing a ROS2DiscoveryEvent::DiscoveredServiceSrv if new and complete or changed
    fn update_service_srv_rep_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredServiceSrv;
        let node_fullname = self.fullname().to_string();
        match self.service_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                match ServiceSrv::create(name.into(), typ) {
                    Ok(mut s) => {
                        s.entities.rep_writer = *writer;
                        e.insert(s);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Service Server: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    tracing::warn!(
                        r#"ROS declaration of Service Server "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceSrv(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.rep_writer != *writer {
                    if v.entities.rep_writer != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Service Server "{v}" changed it's Reply DDS Writer's GID from {} to {writer}"#,
                            v.entities.rep_writer
                        );
                    }
                    v.entities.rep_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceSrv(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ServiceCli, returing a ROS2DiscoveryEvent::DiscoveredServiceCli if new and complete or changed
    fn update_service_cli_rep_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredServiceCli;
        let node_fullname = self.fullname().to_string();
        match self.service_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                match ServiceCli::create(name.into(), typ) {
                    Ok(mut s) => {
                        s.entities.rep_reader = *reader;
                        e.insert(s);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Service Client: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    tracing::warn!(
                        r#"ROS declaration of Service Client "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceCli(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.rep_reader != *reader {
                    if v.entities.rep_reader != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Service Client "{v}" changed it's Request DDS Reader's GID from {} to {reader}"#,
                            v.entities.rep_reader
                        );
                    }
                    v.entities.rep_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceCli(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ServiceCli, returing a ROS2DiscoveryEvent::DiscoveredServiceCli if new and complete or changed
    fn update_service_cli_req_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredServiceCli;
        let node_fullname = self.fullname().to_string();
        match self.service_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                match ServiceCli::create(name.into(), typ) {
                    Ok(mut s) => {
                        s.entities.req_writer = *writer;
                        e.insert(s);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Service Client: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    tracing::warn!(
                        r#"ROS declaration of Service Server "{v}" changed it's type to "{typ}""#
                    );
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceCli(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.req_writer != *writer {
                    if v.entities.req_writer != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Service Server "{v}" changed it's Reply DDS Writer's GID from {} to {writer}"#,
                            v.entities.req_writer
                        );
                    }
                    v.entities.req_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredServiceCli(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    fn update_action_srv_send_req_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        let node_fullname = self.fullname().to_string();
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionSrv::create(name.into(), typ) {
                    Ok(mut a) => {
                        a.entities.send_goal.req_reader = *reader;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Server: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        tracing::warn!(
                            r#"ROS declaration of Action Server "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.send_goal.req_reader != *reader {
                    if v.entities.send_goal.req_reader != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's send_goal Request DDS Reader's GID from {} to {reader}"#,
                            v.entities.send_goal.req_reader
                        );
                    }
                    v.entities.send_goal.req_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    fn update_action_srv_send_rep_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        let node_fullname = self.fullname().to_string();
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionSrv::create(name.into(), typ) {
                    Ok(mut a) => {
                        a.entities.send_goal.rep_writer = *writer;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Server: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        tracing::warn!(
                            r#"ROS declaration of Action Server "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.send_goal.rep_writer != *writer {
                    if v.entities.send_goal.rep_writer != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's send_goal Reply DDS Writer's GID from {} to {writer}"#,
                            v.entities.send_goal.rep_writer
                        );
                    }
                    v.entities.send_goal.rep_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_srv_cancel_req_reader(
        &mut self,
        name: &str,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        let node_fullname = self.fullname().to_string();
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionSrv::create(name.into(), String::new()) {
                    Ok(mut a) => {
                        a.entities.cancel_goal.req_reader = *reader;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Server: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.cancel_goal.req_reader != *reader {
                    if v.entities.cancel_goal.req_reader != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's cancel_goal Request DDS Reader's GID from {} to {reader}"#,
                            v.entities.cancel_goal.req_reader
                        );
                    }
                    v.entities.cancel_goal.req_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_srv_cancel_rep_writer(
        &mut self,
        name: &str,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        let node_fullname = self.fullname().to_string();
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionSrv::create(name.into(), String::new()) {
                    Ok(mut a) => {
                        a.entities.cancel_goal.rep_writer = *writer;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Server: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.cancel_goal.rep_writer != *writer {
                    if v.entities.cancel_goal.rep_writer != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's cancel_goal Reply DDS Writer's GID from {} to {writer}"#,
                            v.entities.cancel_goal.rep_writer
                        );
                    }
                    v.entities.cancel_goal.rep_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    fn update_action_srv_result_req_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        let node_fullname = self.fullname().to_string();
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionSrv::create(name.into(), typ) {
                    Ok(mut a) => {
                        a.entities.get_result.req_reader = *reader;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Server: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        tracing::warn!(
                            r#"ROS declaration of Action Server "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.get_result.req_reader != *reader {
                    if v.entities.get_result.req_reader != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's get_result Request DDS Reader's GID from {} to {reader}"#,
                            v.entities.get_result.req_reader
                        );
                    }
                    v.entities.get_result.req_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    fn update_action_srv_result_rep_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        let node_fullname = self.fullname().to_string();
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionSrv::create(name.into(), typ) {
                    Ok(mut a) => {
                        a.entities.get_result.rep_writer = *writer;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Server: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        tracing::warn!(
                            r#"ROS declaration of Action Server "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.get_result.rep_writer != *writer {
                    if v.entities.get_result.rep_writer != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's get_result Reply DDS Writer's GID from {} to {writer}"#,
                            v.entities.get_result.rep_writer
                        );
                    }
                    v.entities.get_result.rep_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    // NOTE: type of Status topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_srv_status_writer(
        &mut self,
        name: &str,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        let node_fullname = self.fullname().to_string();
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionSrv::create(name.into(), String::new()) {
                    Ok(mut a) => {
                        a.entities.status_writer = *writer;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Server: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.status_writer != *writer {
                    if v.entities.status_writer != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's status DDS Writer's GID from {} to {writer}"#,
                            v.entities.status_writer
                        );
                    }
                    v.entities.status_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionSrv, returing a ROS2DiscoveryEvent::DiscoveredActionSrv if new and complete or changed
    fn update_action_srv_feedback_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionSrv;
        let node_fullname = self.fullname().to_string();
        match self.action_srv.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionSrv::create(name.into(), typ) {
                    Ok(mut a) => {
                        a.entities.feedback_writer = *writer;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Server: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        tracing::warn!(
                            r#"ROS declaration of Action Server "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.feedback_writer != *writer {
                    if v.entities.feedback_writer != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Server "{v}" changed it's status DDS Writer's GID from {} to {writer}"#,
                            v.entities.feedback_writer
                        );
                    }
                    v.entities.feedback_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionSrv(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    fn update_action_cli_send_rep_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        let node_fullname = self.fullname().to_string();
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionCli::create(name.into(), typ) {
                    Ok(mut a) => {
                        a.entities.send_goal.rep_reader = *reader;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Client: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        tracing::warn!(
                            r#"ROS declaration of Action Client "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.send_goal.rep_reader != *reader {
                    if v.entities.send_goal.rep_reader != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's send_goal Reply DDS Reader's GID from {} to {reader}"#,
                            v.entities.send_goal.rep_reader
                        );
                    }
                    v.entities.send_goal.rep_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    fn update_action_cli_send_req_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        let node_fullname = self.fullname().to_string();
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionCli::create(name.into(), typ) {
                    Ok(mut a) => {
                        a.entities.send_goal.req_writer = *writer;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Client: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        tracing::warn!(
                            r#"ROS declaration of Action Client "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.send_goal.req_writer != *writer {
                    if v.entities.send_goal.req_writer != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's send_goal Request DDS Writer's GID from {} to {writer}"#,
                            v.entities.send_goal.req_writer
                        );
                    }
                    v.entities.send_goal.req_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_cli_cancel_rep_reader(
        &mut self,
        name: &str,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        let node_fullname = self.fullname().to_string();
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionCli::create(name.into(), String::new()) {
                    Ok(mut a) => {
                        a.entities.cancel_goal.rep_reader = *reader;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Client: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.cancel_goal.rep_reader != *reader {
                    if v.entities.cancel_goal.rep_reader != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's cancel_goal Reply DDS Reader's GID from {} to {reader}"#,
                            v.entities.cancel_goal.rep_reader
                        );
                    }
                    v.entities.cancel_goal.rep_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    // NOTE: type of CancelGoal topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_cli_cancel_req_writer(
        &mut self,
        name: &str,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        let node_fullname = self.fullname().to_string();
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionCli::create(name.into(), String::new()) {
                    Ok(mut a) => {
                        a.entities.cancel_goal.req_writer = *writer;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Client: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.cancel_goal.req_writer != *writer {
                    if v.entities.cancel_goal.req_writer != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's cancel_goal Request DDS Writer's GID from {} to {writer}"#,
                            v.entities.cancel_goal.req_writer
                        );
                    }
                    v.entities.cancel_goal.req_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    fn update_action_cli_result_rep_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        let node_fullname = self.fullname().to_string();
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionCli::create(name.into(), typ) {
                    Ok(mut a) => {
                        a.entities.get_result.rep_reader = *reader;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Client: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        tracing::warn!(
                            r#"ROS declaration of Action Client "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.get_result.rep_reader != *reader {
                    if v.entities.get_result.rep_reader != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's get_result Reply DDS Reader's GID from {} to {reader}"#,
                            v.entities.get_result.rep_reader
                        );
                    }
                    v.entities.get_result.rep_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    fn update_action_cli_result_req_writer(
        &mut self,
        name: &str,
        typ: String,
        writer: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        let node_fullname = self.fullname().to_string();
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionCli::create(name.into(), typ) {
                    Ok(mut a) => {
                        a.entities.get_result.req_writer = *writer;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Client: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        tracing::warn!(
                            r#"ROS declaration of Action Client "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.get_result.req_writer != *writer {
                    if v.entities.get_result.req_writer != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's get_result Request DDS Writer's GID from {} to {writer}"#,
                            v.entities.get_result.req_writer
                        );
                    }
                    v.entities.get_result.req_writer = *writer;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    // NOTE: type of Status topic does not reflect the action type.
    //       Thus we don't update it or we create ActionCli with as an empty String as type.
    fn update_action_cli_status_reader(
        &mut self,
        name: &str,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        let node_fullname = self.fullname().to_string();
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionCli::create(name.into(), String::new()) {
                    Ok(mut a) => {
                        a.entities.status_reader = *reader;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Client: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.entities.status_reader != *reader {
                    if v.entities.status_reader != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's status DDS Reader's GID from {} to {reader}"#,
                            v.entities.status_reader
                        );
                    }
                    v.entities.status_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    // Update ActionCli, returing a ROS2DiscoveryEvent::DiscoveredActionCli if new and complete or changed
    fn update_action_cli_feedback_reader(
        &mut self,
        name: &str,
        typ: String,
        reader: &Gid,
    ) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::DiscoveredActionCli;
        let node_fullname = self.fullname().to_string();
        match self.action_cli.entry(name.into()) {
            Entry::Vacant(e) => {
                match ActionCli::create(name.into(), typ) {
                    Ok(mut a) => {
                        a.entities.feedback_reader = *reader;
                        e.insert(a);
                    }
                    Err(e) => tracing::error!(
                        "ROS Node {self} declared an incompatible Action Client: {e} - ignored"
                    ),
                }
                None // discovery is not complete anyway
            }
            Entry::Occupied(mut e) => {
                let v = e.get_mut();
                let mut result = None;
                if v.typ != typ {
                    if !v.typ.is_empty() {
                        tracing::warn!(
                            r#"ROS declaration of Action Client "{v}" changed it's type to "{typ}""#
                        );
                    }
                    v.typ = typ;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname.clone(), v.clone()))
                    };
                }
                if v.entities.feedback_reader != *reader {
                    if v.entities.feedback_reader != Gid::NOT_DISCOVERED {
                        tracing::debug!(
                            r#"ROS declaration of Action Client "{v}" changed it's status DDS Reader's GID from {} to {reader}"#,
                            v.entities.feedback_reader
                        );
                    }
                    v.entities.feedback_reader = *reader;
                    if v.is_complete() {
                        result = Some(DiscoveredActionCli(node_fullname, v.clone()))
                    };
                }
                result
            }
        }
    }

    //
    pub fn remove_all_entities(&mut self) -> Vec<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::*;
        let node_fullname = self.fullname().to_string();
        let mut events = Vec::new();

        for (_, v) in self.msg_pub.drain() {
            events.push(UndiscoveredMsgPub(node_fullname.clone(), v))
        }
        for (_, v) in self.msg_sub.drain() {
            events.push(UndiscoveredMsgSub(node_fullname.clone(), v))
        }
        for (_, v) in self.service_srv.drain() {
            events.push(UndiscoveredServiceSrv(node_fullname.clone(), v))
        }
        for (_, v) in self.service_cli.drain() {
            events.push(UndiscoveredServiceCli(node_fullname.clone(), v))
        }
        for (_, v) in self.action_srv.drain() {
            events.push(UndiscoveredActionSrv(node_fullname.clone(), v))
        }
        for (_, v) in self.action_cli.drain() {
            events.push(UndiscoveredActionCli(node_fullname.clone(), v))
        }
        self.undiscovered_reader.resize(0, Gid::NOT_DISCOVERED);
        self.undiscovered_writer.resize(0, Gid::NOT_DISCOVERED);

        events
    }

    // Remove a DDS Reader possibly used by this node, and returns an UndiscoveredX event if
    // this Reader was used by some Subscription, Service or Action
    pub fn remove_reader(&mut self, reader: &Gid) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::*;
        let node_fullname = self.fullname().to_string();
        // Search in Subscribers list if one is using the writer
        if let Some(name) = self.msg_sub.iter_mut().find_map(|(name, sub)| {
            if sub.readers.remove(reader) && sub.readers.is_empty() {
                // found Subscriber using the reader: remove the reader from list
                // and if the list is empty return the Subscriber name to "undiscover" it
                Some(name.clone())
            } else {
                None
            }
        }) {
            // Return undiscovery event for this Subscriber, since all its DDS Writer have been undiscovered
            return Some(UndiscoveredMsgSub(
                node_fullname,
                self.msg_sub.remove(&name).unwrap(),
            ));
        }
        if let Some((name, _)) = self
            .service_srv
            .iter()
            .find(|(_, v)| v.entities.req_reader == *reader)
        {
            return Some(UndiscoveredServiceSrv(
                node_fullname,
                self.service_srv.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self
            .service_cli
            .iter()
            .find(|(_, v)| v.entities.rep_reader == *reader)
        {
            return Some(UndiscoveredServiceCli(
                node_fullname,
                self.service_cli.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self.action_srv.iter().find(|(_, v)| {
            v.entities.send_goal.req_reader == *reader
                || v.entities.cancel_goal.req_reader == *reader
                || v.entities.get_result.req_reader == *reader
        }) {
            return Some(UndiscoveredActionSrv(
                node_fullname,
                self.action_srv.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self.action_cli.iter().find(|(_, v)| {
            v.entities.send_goal.rep_reader == *reader
                || v.entities.cancel_goal.rep_reader == *reader
                || v.entities.get_result.rep_reader == *reader
                || v.entities.status_reader == *reader
                || v.entities.feedback_reader == *reader
        }) {
            return Some(UndiscoveredActionCli(
                node_fullname,
                self.action_cli.remove(&name.clone()).unwrap(),
            ));
        }
        self.undiscovered_reader.retain(|gid| gid != reader);
        None
    }

    // Remove a DDS Writer possibly used by this node, and returns an UndiscoveredX event if
    // this Writer was used by some Publication, Service or Action
    pub fn remove_writer(&mut self, writer: &Gid) -> Option<ROS2DiscoveryEvent> {
        use ROS2DiscoveryEvent::*;
        let node_fullname = self.fullname().to_string();
        // Search in Publishers list if one is using the writer
        if let Some(name) = self.msg_pub.iter_mut().find_map(|(name, publ)| {
            if publ.writers.remove(writer) && publ.writers.is_empty() {
                // found Publisher using the writer: remove the writer from list
                // and if the list is empty return the Publisher name to "undiscover" it
                Some(name.clone())
            } else {
                None
            }
        }) {
            // Return undiscovery event for this Publisher, since all its DDS Writer have been undiscovered
            return Some(UndiscoveredMsgPub(
                node_fullname,
                self.msg_pub.remove(&name).unwrap(),
            ));
        }
        if let Some((name, _)) = self
            .service_srv
            .iter()
            .find(|(_, v)| v.entities.rep_writer == *writer)
        {
            return Some(UndiscoveredServiceSrv(
                node_fullname,
                self.service_srv.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self
            .service_cli
            .iter()
            .find(|(_, v)| v.entities.req_writer == *writer)
        {
            return Some(UndiscoveredServiceCli(
                node_fullname,
                self.service_cli.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self.action_srv.iter().find(|(_, v)| {
            v.entities.send_goal.rep_writer == *writer
                || v.entities.cancel_goal.rep_writer == *writer
                || v.entities.get_result.rep_writer == *writer
                || v.entities.status_writer == *writer
                || v.entities.feedback_writer == *writer
        }) {
            return Some(UndiscoveredActionSrv(
                node_fullname,
                self.action_srv.remove(&name.clone()).unwrap(),
            ));
        }
        if let Some((name, _)) = self.action_cli.iter().find(|(_, v)| {
            v.entities.send_goal.req_writer == *writer
                || v.entities.cancel_goal.req_writer == *writer
                || v.entities.get_result.req_writer == *writer
        }) {
            return Some(UndiscoveredActionCli(
                node_fullname,
                self.action_cli.remove(&name.clone()).unwrap(),
            ));
        }
        self.undiscovered_writer.retain(|gid| gid != writer);
        None
    }
}

fn serialize_hashmap_values<S, T: Serialize>(
    map: &HashMap<String, T>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq: <S as Serializer>::SerializeSeq = serializer.serialize_seq(Some(map.len()))?;
    for x in map.values() {
        seq.serialize_element(x)?;
    }
    seq.end()
}
