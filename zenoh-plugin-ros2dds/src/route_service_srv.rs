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
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use cyclors::dds_entity_t;
use serde::Serialize;
use zenoh::{
    bytes::ZBytes,
    internal::{
        buffers::{Buffer, ZBuf, ZSlice},
        zwrite,
    },
    key_expr::{keyexpr, OwnedKeyExpr},
    liveliness::LivelinessToken,
    query::{Query, Queryable},
    Wait,
};

use crate::{
    dds_types::{DDSRawSample, TypeInfo},
    dds_utils::{
        create_dds_reader, create_dds_writer, dds_write, delete_dds_entity, get_guid,
        get_instance_handle, is_cdr_little_endian, serialize_entity_guid, CDR_HEADER_BE,
        CDR_HEADER_LE,
    },
    liveliness_mgt::new_ke_liveliness_service_srv,
    ros2_utils::{
        is_service_for_action, new_service_id, ros2_service_type_to_reply_dds_type,
        ros2_service_type_to_request_dds_type, CddsRequestHeader, QOS_DEFAULT_SERVICE,
    },
    routes_mgr::Context,
    serialize_option_as_bool, LOG_PAYLOAD,
};

// a route for a Service Server exposed in Zenoh as a Queryable
#[derive(Serialize)]
pub struct RouteServiceSrv {
    // the ROS2 Service name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    // the zenoh queryable used to expose the service server in zenoh.
    // `None` when route is created on a remote announcement and no local ROS2 Service Server discovered yet
    #[serde(rename = "is_active", serialize_with = "serialize_option_as_bool")]
    zenoh_queryable: Option<Queryable<()>>,
    // the local DDS Writer sending requests to the service server
    #[serde(serialize_with = "serialize_entity_guid")]
    req_writer: dds_entity_t,
    // the local DDS Reader receiving replies from the service server
    #[serde(serialize_with = "serialize_entity_guid")]
    rep_reader: dds_entity_t,
    // the client GUID used in each request
    #[serde(skip)]
    client_guid: u64,
    // the ROS sequence number for requests
    #[serde(skip)]
    sequence_number: Arc<AtomicU64>,
    // queries waiting for a reply
    #[serde(skip)]
    queries_in_progress: Arc<RwLock<HashMap<CddsRequestHeader, Query>>>,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken>,
    // the list of remote routes served by this route ("<zenoh_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route
    local_nodes: HashSet<String>,
}

impl Drop for RouteServiceSrv {
    fn drop(&mut self) {
        // remove writer's GID from ros_discovery_info message
        match get_guid(&self.req_writer) {
            Ok(gid) => self.context.ros_discovery_mgr.remove_dds_writer(gid),
            Err(e) => tracing::warn!("{self}: {e}"),
        }
        // remove reader's GID from ros_discovery_info message
        match get_guid(&self.rep_reader) {
            Ok(gid) => self.context.ros_discovery_mgr.remove_dds_reader(gid),
            Err(e) => tracing::warn!("{self}: {e}"),
        }

        if let Err(e) = delete_dds_entity(self.req_writer) {
            tracing::warn!("{}: error deleting DDS Writer:  {}", self, e);
        }
        if let Err(e) = delete_dds_entity(self.rep_reader) {
            tracing::warn!("{}: error deleting DDS Reader:  {}", self, e);
        }
    }
}

impl fmt::Display for RouteServiceSrv {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Service Server (ROS:{} <-> Zenoh:{})",
            self.ros2_name, self.zenoh_key_expr
        )
    }
}

impl RouteServiceSrv {
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        type_info: &Option<Arc<TypeInfo>>,
        context: Context,
    ) -> Result<RouteServiceSrv, String> {
        let route_id = format!("Route Service Server (ROS:{ros2_name} <-> Zenoh:{zenoh_key_expr})");
        tracing::debug!("{route_id}: creation with type {ros2_type}");

        // Default Service QoS
        let mut qos = QOS_DEFAULT_SERVICE.clone();

        // Add DATA_USER QoS similarly to rmw_cyclone_dds here:
        // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/rmw_node.cpp#L5028C17-L5028C17
        let client_id_str = new_service_id(&context.participant)?;
        let user_data = format!("clientid= {client_id_str};");
        qos.user_data = Some(user_data.into_bytes());

        // create DDS Writer to send requests coming from Zenoh to the Service
        let req_topic_name = format!("rq{ros2_name}Request");
        let req_type_name = ros2_service_type_to_request_dds_type(&ros2_type);
        let req_writer = create_dds_writer(
            context.participant,
            req_topic_name,
            req_type_name,
            true,
            qos.clone(),
        )?;
        // add writer's GID in ros_discovery_info message
        context
            .ros_discovery_mgr
            .add_dds_writer(get_guid(&req_writer)?);

        // client_guid used in requests; use dds_instance_handle of writer as rmw_cyclonedds here:
        // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/rmw_node.cpp#L4848
        let client_guid = get_instance_handle(req_writer)?;

        tracing::debug!(
            "{route_id}: (local client_guid={client_guid:02x?})  id='{client_id_str}' => USER_DATA={:?}",
            qos.user_data.as_ref().unwrap()
        );

        // map of queries in progress
        let queries_in_progress: Arc<RwLock<HashMap<CddsRequestHeader, Query>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // create DDS Reader to receive replies and route them to Zenoh
        let rep_topic_name = format!("rr{ros2_name}Reply");
        let rep_type_name = ros2_service_type_to_reply_dds_type(&ros2_type);
        let rep_reader = create_dds_reader(
            context.participant,
            rep_topic_name,
            rep_type_name,
            type_info,
            true,
            qos,
            None,
            {
                let queries_in_progress = queries_in_progress.clone();
                let zenoh_key_expr = zenoh_key_expr.clone();
                move |sample| {
                    route_dds_reply_to_zenoh(
                        sample,
                        zenoh_key_expr.clone(),
                        &mut zwrite!(queries_in_progress),
                        &route_id,
                    );
                }
            },
        )?;
        // add reader's GID in ros_discovery_info message
        context
            .ros_discovery_mgr
            .add_dds_reader(get_guid(&rep_reader)?);

        Ok(RouteServiceSrv {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
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

    // Announce the route over Zenoh via a LivelinessToken
    async fn announce_route(&mut self) -> Result<(), String> {
        // For lifetime issue, redeclare the zenoh key expression that can't be stored in Self
        let declared_ke = self
            .context
            .zsession
            .declare_keyexpr(self.zenoh_key_expr.clone())
            .await
            .map_err(|e| format!("{self}: failed to declare KeyExpr: {e}"))?;

        // create the zenoh Queryable
        // if Reader is TRANSIENT_LOCAL, use a PublicationCache to store historical data
        let queries_in_progress: Arc<RwLock<HashMap<CddsRequestHeader, Query>>> =
            self.queries_in_progress.clone();
        let sequence_number: Arc<AtomicU64> = self.sequence_number.clone();
        let route_id: String = self.to_string();
        let client_guid = self.client_guid;
        let req_writer: i32 = self.req_writer;
        self.zenoh_queryable = Some(
            self.context
                .zsession
                .declare_queryable(&self.zenoh_key_expr)
                .callback(move |query| {
                    route_zenoh_request_to_dds(
                        query,
                        &mut zwrite!(queries_in_progress),
                        &sequence_number,
                        &route_id,
                        client_guid,
                        req_writer,
                    )
                })
                .await
                .map_err(|e| {
                    format!(
                        "Failed create Queryable for key {} (rid={}): {e}",
                        self.zenoh_key_expr, declared_ke
                    )
                })?,
        );

        // if not for an Action (since actions declare their own liveliness)
        if !is_service_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_service_srv(
                &self.context.zsession.zid().into_keyexpr(),
                &self.zenoh_key_expr,
                &self.ros2_type,
            )?;
            tracing::debug!("{self} announce via token {liveliness_ke}");
            let ros2_name = self.ros2_name.clone();
            self.liveliness_token = Some(self.context.zsession
                .liveliness()
                .declare_token(liveliness_ke)
                .await
                .map_err(|e| {
                    format!(
                        "Failed create LivelinessToken associated to route for Service Server {ros2_name}: {e}"
                    )
                })?
            );
        }
        Ok(())
    }

    // Retire the route over Zenoh removing the LivelinessToken
    fn retire_route(&mut self) {
        tracing::debug!("{self} retire");
        // Drop Zenoh Publisher and Liveliness token
        // The DDS Writer remains to be discovered by local ROS nodes
        self.zenoh_queryable = None;
        self.liveliness_token = None;
    }

    #[inline]
    pub fn add_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .insert(format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn remove_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .remove(&format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn is_serving_remote_route(&self) -> bool {
        !self.remote_routes.is_empty()
    }

    #[inline]
    pub async fn add_local_node(&mut self, node: String) {
        self.local_nodes.insert(node);
        tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if 1st local node added, activate the route
        if self.local_nodes.len() == 1 {
            if let Err(e) = self.announce_route().await {
                tracing::error!("{self} activation failed: {e}");
            }
        }
    }

    #[inline]
    pub fn remove_local_node(&mut self, node: &str) {
        self.local_nodes.remove(node);
        tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if last local node removed, deactivate the route
        if self.local_nodes.is_empty() {
            self.retire_route();
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

fn route_zenoh_request_to_dds(
    query: Query,
    queries_in_progress: &mut HashMap<CddsRequestHeader, Query>,
    sequence_number: &AtomicU64,
    route_id: &str,
    client_guid: u64,
    req_writer: i32,
) {
    // Get expected endianness from the query value:
    // if any and if long enoough it shall be the Request type encoded as CDR (including 4 bytes header)
    let is_little_endian = match query.payload() {
        Some(value) if value.len() > 4 => {
            is_cdr_little_endian(value.to_bytes().as_ref()).unwrap_or(true)
        }
        _ => true,
    };

    // Try to get request_id from Query attachment (in case it comes from another bridge).
    // Otherwise, create one using client_guid + sequence_number
    let request_id = query
        .attachment()
        .and_then(|a| CddsRequestHeader::try_from(a).ok())
        .unwrap_or_else(|| {
            CddsRequestHeader::create(
                client_guid,
                sequence_number.fetch_add(1, Ordering::Relaxed),
                is_little_endian,
            )
        });

    // prepend request payload with a (client_guid, sequence_number) header as per rmw_cyclonedds here:
    // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/serdata.hpp#L73
    let dds_req_buf = if let Some(value) = query.payload() {
        // The query comes with some payload. It's expected to be the Request type encoded as CDR (including 4 bytes header)
        let zenoh_req_buf = value.to_bytes();
        if zenoh_req_buf.len() < 4 || zenoh_req_buf[1] > 1 {
            tracing::warn!("{route_id}: received invalid request: {zenoh_req_buf:0x?}");
            return;
        }

        // Send to DDS a buffer made of
        //  - the same CDR header coming with the query
        //  - the request_id as request header as per rmw_cyclonedds here:
        //    https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/serdata.hpp#L73
        //  - the remaining of query payload
        let mut dds_req_buf: Vec<u8> = Vec::new();
        dds_req_buf.extend_from_slice(&zenoh_req_buf[..4]);
        dds_req_buf.extend_from_slice(request_id.as_slice());
        dds_req_buf.extend_from_slice(&zenoh_req_buf[4..]);
        dds_req_buf
    } else {
        // No query payload - send a request containing just client_guid + sequence_number
        // Send to DDS a buffer made of
        //  - a CDR header
        //  - the request_id as request header
        let mut dds_req_buf: Vec<u8> = if request_id.is_little_endian() {
            CDR_HEADER_LE.into()
        } else {
            CDR_HEADER_BE.into()
        };
        dds_req_buf.extend_from_slice(request_id.as_slice());
        dds_req_buf
    };

    if *LOG_PAYLOAD {
        tracing::debug!(
            "{route_id}: routing request {request_id} from Zenoh to DDS - payload: {dds_req_buf:02x?}"
        );
    } else {
        tracing::trace!(
            "{route_id}: routing request {request_id} from Zenoh to DDS - {} bytes",
            dds_req_buf.len()
        );
    }

    queries_in_progress.insert(request_id, query);
    if let Err(e) = dds_write(req_writer, dds_req_buf) {
        tracing::warn!("{route_id}: routing request from Zenoh to DDS failed: {e}");
        queries_in_progress.remove(&request_id);
    }
}

fn route_dds_reply_to_zenoh(
    sample: &DDSRawSample,
    zenoh_key_expr: OwnedKeyExpr,
    queries_in_progress: &mut HashMap<CddsRequestHeader, Query>,
    route_id: &str,
) {
    // Reply payload is expected to be the Response type encoded as CDR, including a 4 bytes CDR header,
    // the 16 bytes request_id (8 bytes client guid + 8 bytes sequence_number), and the reply payload. As per rmw_cyclonedds here:
    // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/serdata.hpp#L73

    let z_bytes: ZBytes = sample.into();
    let slice: ZSlice = ZBuf::from(z_bytes).to_zslice();

    // Decompose the slice into 3 sub-slices (4 bytes header, 16 bytes request_id and payload)
    let (payload, request_id, header) = match (
        slice.subslice(20..slice.len()), // payload from index 20
        slice.subslice(4..20).map(|s| s.as_ref().try_into()), // request_id: 16 bytes from index 4
        slice.subslice(0..4),            // header: 4 bytes
        is_cdr_little_endian(slice.as_ref()), // check endianness flag
    ) {
        (Some(payload), Some(Ok(request_id)), Some(header), Some(is_little_endian)) => {
            let request_id = CddsRequestHeader::from_slice(request_id, is_little_endian);
            (payload, request_id, header)
        }
        _ => {
            tracing::warn!("{route_id}: received invalid request: {sample:0x?} (less than 20 bytes) dropping it");
            return;
        }
    };

    // Check if it's one of my queries in progress. Drop otherwise
    match queries_in_progress.remove(&request_id) {
        Some(query) => {
            // route reply buffer stripped from request_id
            let mut zenoh_rep_buf = ZBuf::empty();
            zenoh_rep_buf.push_zslice(header);
            zenoh_rep_buf.push_zslice(payload);

            if *LOG_PAYLOAD {
                tracing::debug!("{route_id}: routing reply {request_id} from DDS to Zenoh - payload: {zenoh_rep_buf:02x?}");
            } else {
                tracing::trace!(
                    "{route_id}: routing reply {request_id} from DDS to Zenoh - {} bytes",
                    zenoh_rep_buf.len()
                );
            }

            if let Err(e) = query.reply(zenoh_key_expr, zenoh_rep_buf).wait() {
                tracing::warn!("{route_id}: routing reply for request {request_id} from DDS to Zenoh failed: {e}");
            }
        }
        None => tracing::trace!(
            "{route_id}: received response from DDS an unknown query: {request_id} - ignore it"
        ),
    }
}
