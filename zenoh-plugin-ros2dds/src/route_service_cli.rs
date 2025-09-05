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
    collections::HashSet,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use cyclors::dds_entity_t;
use serde::Serialize;
use zenoh::{
    bytes::ZBytes,
    handlers::CallbackDrop,
    internal::buffers::{Buffer, ZBuf, ZSlice},
    key_expr::{keyexpr, OwnedKeyExpr},
    liveliness::LivelinessToken,
    query::{Querier, Reply},
    sample::Locality,
    Wait,
};

use crate::{
    dds_types::{DDSRawSample, TypeInfo},
    dds_utils::{
        create_dds_reader, create_dds_writer, dds_write, delete_dds_entity, get_guid,
        is_cdr_little_endian, serialize_atomic_entity_guid, AtomicDDSEntity, DDS_ENTITY_NULL,
    },
    liveliness_mgt::new_ke_liveliness_service_cli,
    ros2_utils::{
        is_service_for_action, new_service_id, ros2_service_type_to_reply_dds_type,
        ros2_service_type_to_request_dds_type, CddsRequestHeader, QOS_DEFAULT_SERVICE,
    },
    ros_discovery::RosDiscoveryInfoMgr,
    routes_mgr::Context,
    LOG_PAYLOAD,
};

// a route for a Service Client exposed in Zenoh as a Queryier
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RouteServiceCli {
    // the ROS2 Service name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    #[serde(skip)]
    _zenoh_querier: Arc<Querier<'static>>,
    #[serde(serialize_with = "crate::config::serialize_duration_as_f32")]
    queries_timeout: Duration,
    // the local DDS Reader receiving client's requests and routing them to Zenoh
    #[serde(serialize_with = "serialize_atomic_entity_guid")]
    req_reader: Arc<AtomicDDSEntity>,
    // the local DDS Writer sending replies to the client
    #[serde(serialize_with = "serialize_atomic_entity_guid")]
    rep_writer: Arc<AtomicDDSEntity>,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken>,
    // the list of remote routes served by this route ("<zenoh_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route
    local_nodes: HashSet<String>,
}

impl Drop for RouteServiceCli {
    fn drop(&mut self) {
        self.deactivate();
    }
}

impl fmt::Display for RouteServiceCli {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Service Client (ROS:{} <-> Zenoh:{})",
            self.ros2_name, self.zenoh_key_expr
        )
    }
}

impl RouteServiceCli {
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        type_info: Option<Arc<TypeInfo>>,
        queries_timeout: Duration,
        context: Context,
    ) -> Result<RouteServiceCli, String> {
        tracing::debug!(
            "Route Service Client (ROS:{ros2_name} <-> Zenoh:{zenoh_key_expr}): creation with type {ros2_type} (queries_timeout={queries_timeout:#?})"
        );

        let zenoh_querier: Arc<Querier<'static>> = Arc::new(
            context
                .zsession
                .declare_querier(zenoh_key_expr.clone())
                .congestion_control(zenoh::qos::CongestionControl::Block)
                .allowed_destination(Locality::Remote)
                .timeout(queries_timeout)
                .await
                .map_err(|e| format!("Failed create Querier for key {zenoh_key_expr}: {e}",))?,
        );

        let route_id = format!("Route Service Client (ROS:{ros2_name} -> Zenoh:{zenoh_key_expr}");

        // activate/deactivate DDS Reader/Writer on detection/undetection of matching Subscribers
        // (copy/move all required args for the callback)
        let rep_writer: Arc<AtomicDDSEntity> = Arc::new(DDS_ENTITY_NULL.into());
        let req_reader: Arc<AtomicDDSEntity> = Arc::new(DDS_ENTITY_NULL.into());

        zenoh_querier
            .matching_listener()
            .callback({
                let rep_writer = rep_writer.clone();
                let req_reader = req_reader.clone();
                let ros2_name = ros2_name.clone();
                let ros2_type = ros2_type.clone();
                let context = context.clone();
                let zquerier = zenoh_querier.clone();

                move |status| {
                        tracing::debug!("{route_id} MatchingStatus changed: {status:?}");
                        if status.matching() {
                            if let Err(e) = activate(
                                &rep_writer,
                                &req_reader,
                                &ros2_name,
                                &ros2_type,
                                &route_id,
                                &context,
                                &type_info,
                                &zquerier,
                            ) {
                                tracing::error!("{route_id}: failed to activate DDS Reader: {e}");
                            }
                        } else {
                            deactivate(
                                &rep_writer,
                                &req_reader,
                                &route_id,
                                &context.ros_discovery_mgr,
                            )
                        }
                }
            })
            .background()
            .await
            .map_err(|e| format!("Route Service Client (ROS:{ros2_name} <-> Zenoh:{zenoh_key_expr}): failed to listen of matching status changes: {e}",))?;

        Ok(RouteServiceCli {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            _zenoh_querier: zenoh_querier,
            queries_timeout,
            rep_writer,
            req_reader,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    // Announce the route over Zenoh via a LivelinessToken
    async fn announce_route(&mut self) -> Result<(), String> {
        // if not for an Action (since actions declare their own liveliness)
        if !is_service_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_service_cli(
                &self.context.zsession.zid().into_keyexpr(),
                &self.zenoh_key_expr,
                &self.ros2_type,
            )?;
            tracing::debug!("{self}: announce via token {liveliness_ke}");
            let ros2_name = self.ros2_name.clone();
            self.liveliness_token = Some(self.context.zsession
                .liveliness()
                .declare_token(liveliness_ke)
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

    // Retire the route over Zenoh removing the LivelinessToken
    fn retire_route(&mut self) {
        tracing::debug!("{self}: retire");
        // Drop Zenoh Publisher and Liveliness token
        // The DDS Writer remains to be discovered by local ROS nodes
        self.liveliness_token = None;
    }

    fn deactivate(&mut self) {
        let route_id = self.to_string();
        deactivate(
            &self.rep_writer,
            &self.req_reader,
            &route_id,
            &self.context.ros_discovery_mgr,
        );
    }

    #[inline]
    pub fn add_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .insert(format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self}: now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn remove_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .remove(&format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self}: now serving remote routes {:?}", self.remote_routes);
        // if last remote node removed, deactivate the route
        if self.remote_routes.is_empty() {
            self.deactivate();
        }
    }

    #[inline]
    pub fn is_serving_remote_route(&self) -> bool {
        !self.remote_routes.is_empty()
    }

    #[inline]
    pub async fn add_local_node(&mut self, node: String) {
        self.local_nodes.insert(node);
        tracing::debug!("{self}: now serving local nodes {:?}", self.local_nodes);
        // if 1st local node added, announce the route
        if self.local_nodes.len() == 1 {
            if let Err(e) = self.announce_route().await {
                tracing::error!("{self}: announcement failed: {e}");
            }
        }
    }

    #[inline]
    pub fn remove_local_node(&mut self, node: &str) {
        self.local_nodes.remove(node);
        tracing::debug!("{self}: now serving local nodes {:?}", self.local_nodes);
        // if last local node removed, retire the route
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

#[allow(clippy::too_many_arguments)]
fn activate(
    rep_writer: &Arc<AtomicDDSEntity>,
    req_reader: &Arc<AtomicDDSEntity>,
    ros2_name: &str,
    ros2_type: &str,
    route_id: &str,
    context: &Context,
    type_info: &Option<Arc<TypeInfo>>,
    zenoh_querier: &Arc<Querier<'static>>,
) -> Result<(), String> {
    tracing::debug!("{route_id}: activate");
    // Default Service QoS
    let mut qos = QOS_DEFAULT_SERVICE.clone();

    // Add DATA_USER QoS similarly to rmw_cyclone_dds here:
    // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/rmw_node.cpp#L5028C17-L5028C17
    let server_id_str = new_service_id(&context.participant)?;
    let user_data = format!("serviceid= {server_id_str};");
    qos.user_data = Some(user_data.into_bytes());
    tracing::debug!(
        "{route_id}: using id '{server_id_str}' => USER_DATA={:?}",
        qos.user_data.as_ref().unwrap()
    );

    // create DDS Writer to send replies coming from Zenoh to the Client
    let rep_topic_name = format!("rr{}Reply", ros2_name);
    let rep_type_name = ros2_service_type_to_reply_dds_type(ros2_type);
    let dds_writer = create_dds_writer(
        context.participant,
        rep_topic_name,
        rep_type_name,
        true,
        qos.clone(),
    )?;
    let old = rep_writer.swap(dds_writer, Ordering::Relaxed);
    if old != DDS_ENTITY_NULL {
        tracing::warn!(
            "{route_id}: on activation their was already a DDS Reply Writer - overwrite it"
        );
        if let Err(e) = delete_dds_entity(old) {
            tracing::warn!("{route_id}: failed to delete overwritten DDS Reply Writer: {e}");
        }
    }

    // add writer's GID in ros_discovery_info message
    context
        .ros_discovery_mgr
        .add_dds_writer(get_guid(&dds_writer)?);

    // create DDS Reader to receive requests and route them to Zenoh
    let req_topic_name = format!("rq{}Request", ros2_name);
    let req_type_name = ros2_service_type_to_request_dds_type(ros2_type);
    let zquerier = zenoh_querier.clone();
    let route_id2 = route_id.to_owned();
    let dds_reader = create_dds_reader(
        context.participant,
        req_topic_name,
        req_type_name,
        type_info,
        true,
        qos,
        None,
        move |sample| {
            route_dds_request_to_zenoh(&route_id2, sample, &zquerier, dds_writer);
        },
    )?;
    let old = req_reader.swap(dds_reader, Ordering::Relaxed);
    if old != DDS_ENTITY_NULL {
        tracing::warn!(
            "{route_id}: on activation their was already a DDS Request Reader - overwrite it"
        );
        if let Err(e) = delete_dds_entity(old) {
            tracing::warn!("{route_id}: failed to delete overwritten DDS Request Reader: {e}");
        }
    }

    // add reader's GID in ros_discovery_info message
    context
        .ros_discovery_mgr
        .add_dds_reader(get_guid(&dds_reader)?);

    Ok(())
}

fn deactivate(
    rep_writer: &Arc<AtomicDDSEntity>,
    req_reader: &Arc<AtomicDDSEntity>,
    route_id: &str,
    ros_discovery_mgr: &Arc<RosDiscoveryInfoMgr>,
) {
    tracing::debug!("{route_id}: Deactivate");
    let req_reader = req_reader.swap(DDS_ENTITY_NULL, Ordering::Relaxed);
    if req_reader != DDS_ENTITY_NULL {
        // remove reader's GID from ros_discovery_info message
        match get_guid(&req_reader) {
            Ok(gid) => ros_discovery_mgr.remove_dds_reader(gid),
            Err(e) => tracing::warn!("{route_id}: {e}"),
        }
        if let Err(e) = delete_dds_entity(req_reader) {
            tracing::warn!("{route_id}: error deleting DDS Reader: {e}");
        }
    }
    let rep_writer = rep_writer.swap(DDS_ENTITY_NULL, Ordering::Relaxed);
    if rep_writer != DDS_ENTITY_NULL {
        // remove writer's GID from ros_discovery_info message
        match get_guid(&rep_writer) {
            Ok(gid) => ros_discovery_mgr.remove_dds_writer(gid),
            Err(e) => tracing::warn!("{route_id}: {e}"),
        }
        if let Err(e) = delete_dds_entity(rep_writer) {
            tracing::warn!("{route_id}: error deleting DDS Writer: {e}");
        }
    }
}

fn route_dds_request_to_zenoh(
    route_id: &str,
    sample: &DDSRawSample,
    querier: &Arc<Querier<'static>>,
    rep_writer: dds_entity_t,
) {
    // Request payload is expected to be the Request type encoded as CDR, including a 4 bytes CDR header,
    // the 16 bytes request_id (8 bytes client guid + 8 bytes sequence_number), and the request payload. As per rmw_cyclonedds here:
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

    // route request buffer stripped from request_id
    let mut zenoh_req_buf = ZBuf::empty();

    zenoh_req_buf.push_zslice(header);
    zenoh_req_buf.push_zslice(payload);

    if *LOG_PAYLOAD {
        tracing::debug!("{route_id}: routing request {request_id} from DDS to Zenoh - payload: {zenoh_req_buf:02x?}");
    } else {
        tracing::trace!(
            "{route_id}: routing request {request_id} from DDS to Zenoh - {} bytes",
            zenoh_req_buf.len()
        );
    }

    if let Err(e) = querier
        .get()
        .payload(zenoh_req_buf)
        .attachment(request_id.as_attachment())
        .with({
            let route_id1: String = route_id.to_string();
            let route_id2 = route_id.to_string();
            let reply_received1 = Arc::new(AtomicBool::new(false));
            let reply_received2 = reply_received1.clone();
            CallbackDrop {
                callback: move |reply| {
                        if !reply_received1.swap(true, std::sync::atomic::Ordering::Relaxed) {
                            route_zenoh_reply_to_dds(&route_id1, reply, request_id, rep_writer)
                        } else {
                            tracing::warn!("{route_id1}: received more than 1 reply for request {request_id} - dropping the extra replies");
                        }
                    },
                drop: move || {
                    if !reply_received2.load(std::sync::atomic::Ordering::Relaxed) {
                        // There is no way to send an error message as a reply to a ROS Service Client !
                        // (sending an invalid message will make it crash...)
                        // We have no choice but to log the error and let the client hanging without reply, until a timeout (if set by the client)
                        tracing::warn!("{route_id2}: received NO reply for request {request_id} - cannot reply to client, it will hang until timeout");
                    }
                },
            }
        })
        .wait()
    {
        tracing::warn!("{route_id}: routing request {request_id} from DDS to Zenoh failed: {e}");
    }
}

fn route_zenoh_reply_to_dds(
    route_id: &str,
    reply: Reply,
    request_id: CddsRequestHeader,
    rep_writer: dds_entity_t,
) {
    match reply.result() {
        Ok(sample) => {
            let zenoh_rep_buf = sample.payload().to_bytes();
            if zenoh_rep_buf.len() < 4 || zenoh_rep_buf[1] > 1 {
                tracing::warn!(
                    "{route_id}: received invalid reply from Zenoh for {request_id}: {zenoh_rep_buf:0x?}"
                );
                return;
            }
            // route reply buffer re-inserting request_id (client_id + sequence_number)
            let mut dds_rep_buf: Vec<u8> = Vec::new();
            // copy CDR header
            dds_rep_buf.extend_from_slice(&zenoh_rep_buf[..4]);
            // add request_id
            dds_rep_buf.extend_from_slice(request_id.as_slice());
            // add query payoad
            dds_rep_buf.extend_from_slice(&zenoh_rep_buf[4..]);

            if *LOG_PAYLOAD {
                tracing::debug!("{route_id}: routing reply for {request_id} from Zenoh to DDS - payload: {dds_rep_buf:02x?}");
            } else {
                tracing::trace!(
                    "{route_id}: routing reply for {request_id} from Zenoh to DDS - {} bytes",
                    dds_rep_buf.len()
                );
            }

            if let Err(e) = dds_write(rep_writer, dds_rep_buf) {
                tracing::warn!(
                    "{route_id}: routing reply for {request_id} from Zenoh to DDS failed: {e}"
                );
            }
        }
        Err(val) => {
            tracing::warn!("{route_id}: received error as reply for {request_id}: {val:?}");
        }
    }
}
