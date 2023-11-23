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

use cyclors::dds_entity_t;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashSet, fmt};
use zenoh::buffers::{ZBuf, ZSlice};
use zenoh::liveliness::LivelinessToken;
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh::query::Reply;
use zenoh_core::SyncResolve;

use crate::dds_types::{DDSRawSample, TypeInfo};
use crate::dds_utils::serialize_entity_guid;
use crate::dds_utils::{
    create_dds_reader, create_dds_writer, dds_write, delete_dds_entity, get_guid,
};
use crate::liveliness_mgt::new_ke_liveliness_service_cli;
use crate::ros2_utils::{
    is_service_for_action, new_service_id, ros2_service_type_to_reply_dds_type,
    ros2_service_type_to_request_dds_type, QOS_DEFAULT_SERVICE,
};
use crate::routes_mgr::Context;
use crate::LOG_PAYLOAD;

// a route for a Service Client exposed in Zenoh as a Queryier
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RouteServiceCli<'a> {
    // the ROS2 Service name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    #[serde(serialize_with = "crate::config::serialize_duration_as_f32")]
    queries_timeout: Duration,
    is_active: bool,
    // the local DDS Reader receiving client's requests and routing them to Zenoh
    #[serde(serialize_with = "serialize_entity_guid")]
    req_reader: dds_entity_t,
    // the local DDS Writer sending replies to the client
    #[serde(serialize_with = "serialize_entity_guid")]
    rep_writer: dds_entity_t,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken<'a>>,
    // the list of remote routes served by this route ("<plugin_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route
    local_nodes: HashSet<String>,
}

impl Drop for RouteServiceCli<'_> {
    fn drop(&mut self) {
        // remove reader's GID from ros_discovery_info message
        match get_guid(&self.req_reader) {
            Ok(gid) => self.context.ros_discovery_mgr.remove_dds_reader(gid),
            Err(e) => log::warn!("{self}: {e}"),
        }
        // remove writer's GID from ros_discovery_info message
        match get_guid(&self.rep_writer) {
            Ok(gid) => self.context.ros_discovery_mgr.remove_dds_writer(gid),
            Err(e) => log::warn!("{self}: {e}"),
        }

        if let Err(e) = delete_dds_entity(self.req_reader) {
            log::warn!("{}: error deleting DDS Reader:  {}", self, e);
        }
        if let Err(e) = delete_dds_entity(self.rep_writer) {
            log::warn!("{}: error deleting DDS Writer:  {}", self, e);
        }
    }
}

impl fmt::Display for RouteServiceCli<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Service Client (ROS:{} <-> Zenoh:{})",
            self.ros2_name, self.zenoh_key_expr
        )
    }
}

impl RouteServiceCli<'_> {
    #[allow(clippy::too_many_arguments)]
    pub async fn create<'a>(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        type_info: &Option<Arc<TypeInfo>>,
        queries_timeout: Duration,
        context: Context,
    ) -> Result<RouteServiceCli<'a>, String> {
        log::debug!(
            "Route Service Client (ROS:{ros2_name} <-> Zenoh:{zenoh_key_expr}): creation with type {ros2_type}"
        );

        // Default Service QoS
        let mut qos = QOS_DEFAULT_SERVICE.clone();

        // Add DATA_USER QoS similarly to rmw_cyclone_dds here:
        // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/rmw_node.cpp#L5028C17-L5028C17
        let server_id_str = new_service_id(&context.participant)?;
        let user_data = format!("serviceid= {server_id_str};");
        qos.user_data = Some(user_data.into_bytes());
        log::debug!(
            "Route Service Client (ROS:{ros2_name} <-> Zenoh:{zenoh_key_expr}): using id '{server_id_str}' => USER_DATA={:?}", qos.user_data.as_ref().unwrap()
        );

        // create DDS Writer to send replies coming from Zenoh to the Client
        let rep_topic_name = format!("rr{ros2_name}Reply");
        let rep_type_name = ros2_service_type_to_reply_dds_type(&ros2_type);
        let rep_writer = create_dds_writer(
            context.participant,
            rep_topic_name,
            rep_type_name,
            true,
            qos.clone(),
        )?;
        // add writer's GID in ros_discovery_info message
        context
            .ros_discovery_mgr
            .add_dds_writer(get_guid(&rep_writer)?);

        let route_id: String =
            format!("Route Service Client (ROS:{ros2_name} <-> Zenoh:{zenoh_key_expr})",);

        // create DDS Reader to receive requests and route them to Zenoh
        let req_topic_name = format!("rq{ros2_name}Request");
        let req_type_name = ros2_service_type_to_request_dds_type(&ros2_type);
        let zenoh_key_expr2 = zenoh_key_expr.clone();
        let zsession2 = context.zsession.clone();
        let req_reader = create_dds_reader(
            context.participant,
            req_topic_name,
            req_type_name,
            type_info,
            true,
            qos,
            None,
            move |sample| {
                do_route_request(
                    &route_id,
                    sample,
                    &zenoh_key_expr2,
                    &zsession2,
                    queries_timeout,
                    rep_writer,
                );
            },
        )?;
        // add reader's GID in ros_discovery_info message
        context
            .ros_discovery_mgr
            .add_dds_reader(get_guid(&req_reader)?);

        Ok(RouteServiceCli {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            queries_timeout,
            is_active: false,
            rep_writer,
            req_reader,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    async fn activate(&mut self) -> Result<(), String> {
        self.is_active = true;

        // if not for an Action (since actions declare their own liveliness)
        if !is_service_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_service_cli(
                &self.context.plugin_id,
                &self.zenoh_key_expr,
                &self.ros2_type,
            )?;
            let ros2_name = self.ros2_name.clone();
            self.liveliness_token = Some(self.context.zsession
                .liveliness()
                .declare_token(liveliness_ke)
                .res_async()
                .await
                .map_err(|e| {
                    format!(
                        "Failed create LivelinessToken associated to route for Service Client {ros2_name}: {e}"
                    )
                })?
            );
        }
        Ok(())
    }

    fn deactivate(&mut self) {
        log::debug!("{self} deactivate");
        // Drop Zenoh Publisher and Liveliness token
        // The DDS Writer remains to be discovered by local ROS nodes
        self.is_active = false;
        self.liveliness_token = None;
    }

    #[inline]
    pub fn add_remote_route(&mut self, plugin_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .insert(format!("{plugin_id}:{zenoh_key_expr}"));
        log::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn remove_remote_route(&mut self, plugin_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .remove(&format!("{plugin_id}:{zenoh_key_expr}"));
        log::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn is_serving_remote_route(&self) -> bool {
        !self.remote_routes.is_empty()
    }

    #[inline]
    pub async fn add_local_node(&mut self, node: String) {
        self.local_nodes.insert(node);
        log::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if 1st local node added, activate the route
        if self.local_nodes.len() == 1 {
            if let Err(e) = self.activate().await {
                log::error!("{self} activation failed: {e}");
            }
        }
    }

    #[inline]
    pub fn remove_local_node(&mut self, node: &str) {
        self.local_nodes.remove(node);
        log::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if last local node removed, deactivate the route
        if self.local_nodes.is_empty() {
            self.deactivate();
        }
    }

    #[inline]
    pub fn is_serving_local_node(&self) -> bool {
        !self.local_nodes.is_empty()
    }

    #[inline]
    pub fn is_unused(&self) -> bool {
        !self.is_serving_local_node() && !self.is_serving_remote_route()
    }
}

fn do_route_request(
    route_id: &str,
    sample: &DDSRawSample,
    zenoh_key_expr: &OwnedKeyExpr,
    zsession: &Arc<Session>,
    queries_timeout: Duration,
    rep_writer: dds_entity_t,
) {
    // request payload is expected to be the Request type encoded as CDR, including a 4 bytes header,
    // the client guid (8 bytes) and a sequence_number (8 bytes). As per rmw_cyclonedds here:
    // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/serdata.hpp#L73
    if sample.len() < 20 {
        log::warn!("{route_id}: received invalid request: {sample:0x?}");
        return;
    }

    let zbuf: ZBuf = sample.into();
    let dds_req_buf = zbuf.contiguous();
    let request_id: [u8; 16] = dds_req_buf[4..20].try_into().unwrap();

    // route request buffer stripped from request_id (client_id + sequence_number)
    let mut zenoh_req_buf = ZBuf::empty();
    let slice: ZSlice = dds_req_buf.into_owned().into();
    // copy CDR Header
    zenoh_req_buf.push_zslice(slice.subslice(0, 4).unwrap());
    // copy Request payload, skiping client_id + sequence_number
    zenoh_req_buf.push_zslice(slice.subslice(20, slice.len()).unwrap());

    if *LOG_PAYLOAD {
        log::debug!("{route_id}: routing request {request_id:02x?} to Zenoh - payload: {zenoh_req_buf:02x?}");
    } else {
        log::trace!(
            "{route_id}: routing request {request_id:02x?} to Zenoh - {} bytes",
            zenoh_req_buf.len()
        );
    }

    let route_id2 = route_id.to_string();
    if let Err(e) = zsession
        .get(zenoh_key_expr)
        .with_value(zenoh_req_buf)
        .timeout(queries_timeout)
        .callback(move |reply| do_route_reply(route_id2.clone(), reply, request_id, rep_writer))
        .res_sync()
    {
        log::warn!("{route_id}: routing request {request_id:02x?} to Zenoh failed: {e}");
    }
}

fn do_route_reply(route_id: String, reply: Reply, request_id: [u8; 16], rep_writer: dds_entity_t) {
    match reply.sample {
        Ok(sample) => {
            let zenoh_rep_buf = sample.payload.contiguous();
            if zenoh_rep_buf.len() < 4 || zenoh_rep_buf[1] > 1 {
                log::warn!(
                    "{route_id}: received invalid reply for {request_id:02x?}: {zenoh_rep_buf:0x?}"
                );
                return;
            }
            // route reply buffer re-inserting request_id (client_id + sequence_number)
            let mut dds_rep_buf: Vec<u8> = Vec::new();
            // copy CDR header
            dds_rep_buf.extend_from_slice(&zenoh_rep_buf[..4]);
            // add request_id
            dds_rep_buf.extend_from_slice(&request_id);
            // add query payoad
            dds_rep_buf.extend_from_slice(&zenoh_rep_buf[4..]);

            if *LOG_PAYLOAD {
                log::debug!("{route_id}: routing reply for {request_id:02x?} to Client - payload: {dds_rep_buf:02x?}");
            } else {
                log::trace!(
                    "{route_id}: routing reply for {request_id:02x?} to Client - {} bytes",
                    dds_rep_buf.len()
                );
            }

            if let Err(e) = dds_write(rep_writer, dds_rep_buf) {
                log::warn!("{route_id}: routing reply for {request_id:02x?}  failed: {e}");
            }
        }
        Err(val) => {
            log::warn!("{route_id}: received error as reply for {request_id:02x?}: {val}");
        }
    }
}
