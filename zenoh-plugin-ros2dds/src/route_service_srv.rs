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
use cyclors::qos::{History, HistoryKind, Qos, Reliability, ReliabilityKind, DDS_INFINITE_TIME};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::RwLock;
use std::{collections::HashSet, fmt};
use zenoh::buffers::{ZBuf, ZSlice};
use zenoh::liveliness::LivelinessToken;
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh::queryable::{Query, Queryable};
use zenoh_core::zwrite;

use crate::dds_utils::{dds_write, get_instance_handle};
use crate::gid::Gid;
use crate::liveliness_mgt::new_ke_liveliness_service_srv;
use crate::ros2_utils::{
    new_service_id, ros2_service_type_to_reply_dds_type, ros2_service_type_to_request_dds_type,
};
use crate::{serialize_option_as_bool, LOG_PAYLOAD};
use crate::{dds_discovery::*, Config};

// a route for a Service Server exposed in Zenoh as a Queryable
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RouteServiceSrv<'a> {
    // the ROS2 Service name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the zenoh session
    #[serde(skip)]
    zsession: &'a Arc<Session>,
    // the config
    #[serde(skip)]
    _config: Arc<Config>,
    // the zenoh queryable used to expose the service server in zenoh.
    // `None` when route is created on a remote announcement and no local ROS2 Service Server discovered yet
    #[serde(rename = "is_active", serialize_with = "serialize_option_as_bool")]
    zenoh_queryable: Option<Queryable<'a, ()>>,
    // the local DDS Writer sending requests to the service server
    #[serde(serialize_with = "serialize_entity_guid")]
    req_writer: dds_entity_t,
    // the local DDS Reader receiving replies from the service server
    #[serde(serialize_with = "serialize_entity_guid")]
    rep_reader: dds_entity_t,
    // the client GUID used in eacch request
    #[serde(skip)]
    client_guid: u64,
    // the ROS sequence number for requests
    #[serde(skip)]
    sequence_number: Arc<AtomicU64>,
    // queries waiting for a reply
    #[serde(skip)]
    queries_in_progress: Arc<RwLock<HashMap<u64, Query>>>,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken<'a>>,
    // the list of remote routes served by this route ("<plugin_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route
    local_nodes: HashSet<String>,
}

impl Drop for RouteServiceSrv<'_> {
    fn drop(&mut self) {
        if let Err(e) = delete_dds_entity(self.req_writer) {
            log::warn!("{}: error deleting DDS Writer:  {}", self, e);
        }
        if let Err(e) = delete_dds_entity(self.rep_reader) {
            log::warn!("{}: error deleting DDS Reader:  {}", self, e);
        }
    }
}

impl fmt::Display for RouteServiceSrv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Service Server (ROS:{} -> Zenoh:{})",
            self.ros2_name, self.zenoh_key_expr
        )
    }
}

impl RouteServiceSrv<'_> {
    #[allow(clippy::too_many_arguments)]
    pub async fn create<'a>(
        config: Arc<Config>,
        zsession: &'a Arc<Session>,
        participant: dds_entity_t,
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        type_info: &Option<Arc<TypeInfo>>,
    ) -> Result<RouteServiceSrv<'a>, String> {
        log::debug!(
            "Route Service Server ({ros2_name} -> {zenoh_key_expr}): creation with type {ros2_type}"
        );

        // Default Service QoS copied from:
        // https://github.com/ros2/rmw/blob/83445be486deae8c78d275e092eafb4bf380bd49/rmw/include/rmw/qos_profiles.h#L64C44-L64C44
        let mut qos = Qos::default();
        qos.history = Some(History {
            kind: HistoryKind::KEEP_LAST,
            depth: 10,
        });
        qos.reliability = Some(Reliability {
            kind: ReliabilityKind::RELIABLE,
            max_blocking_time: DDS_INFINITE_TIME,
        });

        // Add DATA_USER QoS similarly to rmw_cyclone_dds here:
        // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/rmw_node.cpp#L5028C17-L5028C17
        let client_id_str = new_service_id(&participant)?;
        let user_data = format!("clientid= {client_id_str};");
        qos.user_data = Some(user_data.into_bytes());
        log::debug!(
            "Route Service Server ({ros2_name} -> {zenoh_key_expr}): using id '{client_id_str}' => USER_DATA={:?}", qos.user_data.as_ref().unwrap()
        );

        // create DDS Writer to send requests coming from Zenoh to the Service
        let req_topic_name = format!("rq{ros2_name}Request");
        let req_type_name = ros2_service_type_to_request_dds_type(&ros2_type);
        let req_writer = create_dds_writer(
            participant,
            req_topic_name,
            req_type_name,
            true,
            qos.clone(),
        )?;

        // client_guid used in requests; use dds_instance_handle of writer as rmw_cyclonedds here:
        // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/rmw_node.cpp#L4848
        let client_guid = get_instance_handle(req_writer)?;

        // map of queries in progress
        let queries_in_progress: Arc<RwLock<HashMap<u64, Query>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // create DDS Reader to receive replies and route them to Zenoh
        let rep_topic_name = format!("rr{ros2_name}Reply");
        let rep_type_name = ros2_service_type_to_reply_dds_type(&ros2_type);
        let queries_in_progress2 = queries_in_progress.clone();
        let zenoh_key_expr2 = zenoh_key_expr.clone();
        let rep_reader = create_dds_reader(
            participant,
            rep_topic_name,
            rep_type_name,
            type_info,
            true,
            qos,
            None,
            move |sample| {
                do_route_reply(
                    sample,
                    zenoh_key_expr2.clone(),
                    &mut *zwrite!(queries_in_progress2),
                    "",
                    client_guid,
                );
            },
        )?;

        Ok(RouteServiceSrv {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            zsession,
            _config: config,
            zenoh_queryable: None,
            req_writer,
            rep_reader,
            client_guid,
            sequence_number: Arc::new(AtomicU64::default()),
            queries_in_progress,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    async fn activate<'a>(&'a mut self, plugin_id: &keyexpr) -> Result<(), String> {
        // For lifetime issue, redeclare the zenoh key expression that can't be stored in Self
        let declared_ke = self
            .zsession
            .declare_keyexpr(self.zenoh_key_expr.clone())
            .res()
            .await
            .map_err(|e| {
                format!(
                    "Route Publisher (ROS:{} -> Zenoh:{}): failed to declare KeyExpr: {e}",
                    self.ros2_name, self.zenoh_key_expr
                )
            })?;

        // create the zenoh Queryable
        // if Reader is TRANSIENT_LOCAL, use a PublicationCache to store historical data
        let queries_in_progress: Arc<RwLock<HashMap<u64, Query>>> =
            self.queries_in_progress.clone();
        let sequence_number: Arc<AtomicU64> = self.sequence_number.clone();
        let route_id: String = self.to_string();
        let client_guid = self.client_guid;
        let req_writer: i32 = self.req_writer;
        self.zenoh_queryable = Some(
            self.zsession
                .declare_queryable(&self.zenoh_key_expr)
                .callback(move |query| {
                    do_route_request(
                        query,
                        &mut *zwrite!(queries_in_progress),
                        &sequence_number,
                        &route_id,
                        client_guid,
                        req_writer,
                    )
                })
                .res()
                .await
                .map_err(|e| {
                    format!(
                        "Failed create Queryable for key {} (rid={}): {e}",
                        self.zenoh_key_expr, declared_ke
                    )
                })?,
        );

        // create associated LivelinessToken
        let liveliness_ke =
            new_ke_liveliness_service_srv(plugin_id, &self.zenoh_key_expr, &self.ros2_type)?;
        let ros2_name = self.ros2_name.clone();
        self.liveliness_token = Some(self.zsession
            .liveliness()
            .declare_token(liveliness_ke)
            .res()
            .await
            .map_err(|e| {
                format!(
                    "Failed create LivelinessToken associated to route for Service Server {ros2_name}: {e}"
                )
            })?
        );
        Ok(())
    }

    fn deactivate(&mut self) {
        log::debug!("{self} deactivate");
        // Drop Zenoh Publisher and Liveliness token
        // The DDS Writer remains to be discovered by local ROS nodes
        self.zenoh_queryable = None;
        self.liveliness_token = None;
    }

    pub fn dds_req_writer_guid(&self) -> Result<Gid, String> {
        get_guid(&self.req_writer)
    }

    pub fn dds_rep_reader_guid(&self) -> Result<Gid, String> {
        get_guid(&self.rep_reader)
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
    pub async fn add_local_node(&mut self, node: String, plugin_id: &keyexpr) {
        self.local_nodes.insert(node);
        log::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if 1st local node added, activate the route
        if self.local_nodes.len() == 1 {
            if let Err(e) = self.activate(plugin_id).await {
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

const CDR_HEADER_LE: [u8; 4] = [0, 1, 0, 0];

fn do_route_request(
    query: Query,
    queries_in_progress: &mut HashMap<u64, Query>,
    sequence_number: &AtomicU64,
    route_id: &str,
    client_guid: u64,
    req_writer: i32,
) {
    let n = sequence_number.fetch_add(1, Ordering::Relaxed);

    // prepend request payload with a (client_guid, sequence_number) header as per rmw_cyclonedds here:
    // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/serdata.hpp#L73
    let dds_req_buf = if let Some(value) = query.value() {
        // query payload is expected to be the Request type encoded as CDR (including 4 bytes header)
        let zenoh_req_buf = &*(value.payload.contiguous());
        if zenoh_req_buf.len() < 4 || zenoh_req_buf[1] > 1 {
            log::warn!("{route_id}: received invalid request: {zenoh_req_buf:0x?}");
            return;
        }
        let client_guid_bytes = if zenoh_req_buf[1] == 0 {
            client_guid.to_be_bytes()
        } else {
            client_guid.to_le_bytes()
        };

        let mut dds_req_buf: Vec<u8> = Vec::new();
        // copy CDR header
        dds_req_buf.extend_from_slice(&zenoh_req_buf[..4]);
        // add client_id
        dds_req_buf.extend_from_slice(&client_guid_bytes);
        // add sequence_number (endianness depending on header)
        if zenoh_req_buf[1] == 0 {
            dds_req_buf.extend_from_slice(&n.to_be_bytes());
        } else {
            dds_req_buf.extend_from_slice(&n.to_le_bytes());
        }
        // add query payoad
        dds_req_buf.extend_from_slice(&zenoh_req_buf[4..]);
        dds_req_buf
    } else {
        // No query payload - send a request containing just client_guid + sequence_number
        let mut dds_req_buf: Vec<u8> = CDR_HEADER_LE.clone().into();
        dds_req_buf.extend_from_slice(&client_guid.to_le_bytes());
        dds_req_buf.extend_from_slice(&n.to_le_bytes());
        dds_req_buf
    };

    if *LOG_PAYLOAD {
        log::trace!("{route_id}: routing request #{n} to Service - payload: {dds_req_buf:02x?}");
    } else {
        log::trace!("{route_id}: routing request #{n} to Service - {} bytes", dds_req_buf.len());
    }

    queries_in_progress.insert(n, query);
    if let Err(e) = dds_write(req_writer, dds_req_buf) {
        log::warn!("{route_id}: routing request failed: {e}");
        queries_in_progress.remove(&n);
    }
}

fn do_route_reply(
    sample: &DDSRawSample,
    zenoh_key_expr: OwnedKeyExpr,
    queries_in_progress: &mut HashMap<u64, Query>,
    route_id: &str,
    client_guid: u64,
) {
    // reply payload is expected to be the Response type encoded as CDR, including a 4 bytes header,
    // the client guid (8 bytes) and a sequence_number (8 bytes). As per rmw_cyclonedds here:
    // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/serdata.hpp#L73
    if sample.len() < 20 {
        log::warn!("{route_id}: received invalid response: {sample:0x?}");
        return;
    }

    let zbuf: ZBuf = sample.into();
    let dds_rep_buf = zbuf.contiguous();
    let cdr_header = &dds_rep_buf[..4];
    let guid = if dds_rep_buf[1] == 0 {
        u64::from_be_bytes(dds_rep_buf[4..12].try_into().unwrap())
    } else {
        u64::from_le_bytes(dds_rep_buf[4..12].try_into().unwrap())
    };

    if guid != client_guid {
        log::warn!(
            "{route_id}: received response for another client: {guid:0x?} (me: {client_guid:0x?}"
        );
        return;
    }
    let seq_num = if cdr_header[1] == 0 {
        u64::from_be_bytes(dds_rep_buf[12..20].try_into().unwrap())
    } else {
        u64::from_le_bytes(dds_rep_buf[12..20].try_into().unwrap())
    };
    match queries_in_progress.remove(&seq_num) {
        Some(query) => {
            use zenoh_core::SyncResolve;
            let slice: ZSlice = dds_rep_buf.into_owned().into();
            let mut zenoh_rep_buf = ZBuf::empty();
            zenoh_rep_buf.push_zslice(slice.subslice(0, 4).unwrap());
            zenoh_rep_buf.push_zslice(slice.subslice(20, slice.len()).unwrap());

            if *LOG_PAYLOAD {
                log::trace!("{route_id}: routing reply #{seq_num} to Client - payload: {zenoh_rep_buf:02x?}");
            } else {
                log::trace!("{route_id}: routing reply #{seq_num} to Client - {} bytes", zenoh_rep_buf.len());
            }

            if let Err(e) = query
                .reply(Ok(Sample::new(zenoh_key_expr, zenoh_rep_buf)))
                .res_sync()
            {
                log::warn!("{route_id}: routing reply for request #{seq_num} failed: {e}");
            }
        }
        None => log::warn!(
            "{route_id}: received response an unknown query (already dropped?): #{seq_num}"
        ),
    }
}
