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

use cyclors::{
    dds_entity_t, dds_get_entity_sertype, dds_strretcode, dds_writecdr, ddsi_serdata_from_ser_iov,
    ddsi_serdata_kind_SDK_DATA, ddsi_sertype, ddsrt_iov_len_t, ddsrt_iovec_t,
};
use serde::Serialize;
use std::collections::HashSet;
use std::{ffi::CStr, fmt, time::Duration};
use zenoh::liveliness::LivelinessToken;
use zenoh::prelude::*;
use zenoh::query::ReplyKeyExpr;
use zenoh::{prelude::r#async::AsyncResolve, subscriber::Subscriber};
use zenoh_ext::{FetchingSubscriber, SubscriberBuilderExt};

use crate::dds_utils::{create_dds_writer, ddsrt_iov_len_from_usize, delete_dds_entity, get_guid};
use crate::liveliness_mgt::new_ke_liveliness_sub;
use crate::qos_helpers::is_transient_local;
use crate::ros2_utils::{is_message_for_action, ros2_message_type_to_dds_type};
use crate::routes_mgr::Context;
use crate::{
    dds_utils::serialize_entity_guid, qos::Qos, vec_into_raw_parts, KE_ANY_1_SEGMENT, LOG_PAYLOAD,
};
use crate::{serialize_option_as_bool, KE_PREFIX_PUB_CACHE};

enum ZSubscriber<'a> {
    Subscriber(Subscriber<'a, ()>),
    FetchingSubscriber(FetchingSubscriber<'a, ()>),
}

// a route from Zenoh to DDS
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RouteSubscriber<'a> {
    // the ROS2 Subscriber name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    // the zenoh subscriber receiving messages to be re-published by the DDS Writer
    // `None` when route is created on a remote announcement and no local ROS2 Subscriber discovered yet
    #[serde(rename = "is_active", serialize_with = "serialize_option_as_bool")]
    zenoh_subscriber: Option<ZSubscriber<'a>>,
    // the local DDS Writer created to serve the route (i.e. re-publish to DDS message coming from zenoh)
    #[serde(serialize_with = "serialize_entity_guid")]
    dds_writer: dds_entity_t,
    // if the Writer is TRANSIENT_LOCAL
    transient_local: bool,
    // queries timeout for historical publication (if TRANSIENT_LOCAL)
    queries_timeout: Duration,
    // if the topic is keyless
    #[serde(skip)]
    keyless: bool,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken<'a>>,
    // the list of remote routes served by this route ("<plugin_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route
    local_nodes: HashSet<String>,
}

impl Drop for RouteSubscriber<'_> {
    fn drop(&mut self) {
        // remove writer's GID from ros_discovery_info message
        match get_guid(&self.dds_writer) {
            Ok(gid) => self.context.ros_discovery_mgr.remove_dds_writer(gid),
            Err(e) => tracing::warn!("{self}: {e}"),
        }

        tracing::debug!("{self}: delete Writer");
        if let Err(e) = delete_dds_entity(self.dds_writer) {
            tracing::warn!("{}: error deleting DDS Reader:  {}", self, e);
        }
    }
}

impl fmt::Display for RouteSubscriber<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Subscriber (Zenoh:{} -> ROS:{})",
            self.zenoh_key_expr, self.ros2_name
        )
    }
}

impl RouteSubscriber<'_> {
    #[allow(clippy::too_many_arguments)]
    pub async fn create<'a>(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        keyless: bool,
        mut writer_qos: Qos,
        context: Context,
    ) -> Result<RouteSubscriber<'a>, String> {
        let transient_local = is_transient_local(&writer_qos);
        tracing::debug!("Route Subscriber ({zenoh_key_expr} -> {ros2_name}): creation with type {ros2_type} (transient_local:{transient_local})");

        let topic_name = format!("rt{ros2_name}");
        let type_name = ros2_message_type_to_dds_type(&ros2_type);
        let queries_timeout = context.config.get_queries_timeout_tl_sub(&ros2_name);

        // force RELIABLE QoS for Writers (#23)
        if let Some(cyclors::qos::Reliability {
            kind: cyclors::qos::ReliabilityKind::BEST_EFFORT,
            ..
        }) = &mut writer_qos.reliability
        {
            // Per DDS specification, the default Reliability value for DataWriters is RELIABLE with max_blocking_time=100ms
            // Thus just use default value.
            writer_qos.reliability = None;
        }

        tracing::debug!(
            "Route Subscriber ({zenoh_key_expr} -> {ros2_name}): create Writer with {writer_qos:?}"
        );
        let dds_writer = create_dds_writer(
            context.participant,
            topic_name,
            type_name,
            keyless,
            writer_qos,
        )?;
        // add writer's GID in ros_discovery_info message
        context
            .ros_discovery_mgr
            .add_dds_writer(get_guid(&dds_writer)?);

        Ok(RouteSubscriber {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            zenoh_subscriber: None,
            dds_writer,
            transient_local,
            queries_timeout,
            keyless,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    // Announce the route over Zenoh via a LivelinessToken
    async fn announce_route(&mut self, discovered_reader_qos: &Qos) -> Result<(), String> {
        tracing::debug!("{self} activate");
        // Callback routing message received by Zenoh subscriber to DDS Writer (if set)
        let ros2_name = self.ros2_name.clone();
        let dds_writer = self.dds_writer;
        let subscriber_callback = move |s: Sample| {
            route_zenoh_message_to_dds(s, &ros2_name, dds_writer);
        };

        // create zenoh subscriber
        // if Writer is TRANSIENT_LOCAL, use a QueryingSubscriber to fetch remote historical messages to write
        self.zenoh_subscriber = if self.transient_local {
            // query all PublicationCaches on "<KE_PREFIX_PUB_CACHE>/*/<routing_keyexpr>"
            let query_selector: Selector =
                (*KE_PREFIX_PUB_CACHE / *KE_ANY_1_SEGMENT / &self.zenoh_key_expr).into();
            tracing::debug!("{self}: query historical messages from everybody for TRANSIENT_LOCAL Reader on {query_selector}");
            let sub = self
                .context
                .zsession
                .declare_subscriber(&self.zenoh_key_expr)
                .callback(subscriber_callback)
                .allowed_origin(Locality::Remote) // Allow only remote publications to avoid loops
                .reliable()
                .querying()
                .query_timeout(self.queries_timeout)
                .query_selector(query_selector)
                .query_accept_replies(ReplyKeyExpr::Any)
                .res()
                .await
                .map_err(|e| format!("{self}: failed to create FetchingSubscriber: {e}",))?;
            Some(ZSubscriber::FetchingSubscriber(sub))
        } else {
            let sub = self
                .context
                .zsession
                .declare_subscriber(&self.zenoh_key_expr)
                .callback(subscriber_callback)
                .allowed_origin(Locality::Remote) // Allow only remote publications to avoid loops
                .reliable()
                .res()
                .await
                .map_err(|e| format!("{self}: failed to create Subscriber: {e}"))?;
            Some(ZSubscriber::Subscriber(sub))
        };

        // if not for an Action (since actions declare their own liveliness)
        if !is_message_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_sub(
                &self.context.plugin_id,
                &self.zenoh_key_expr,
                &self.ros2_type,
                self.keyless,
                discovered_reader_qos,
            )?;
            let ros2_name = self.ros2_name.clone();
            self.liveliness_token = Some(
                self.context.zsession
                    .liveliness()
                    .declare_token(liveliness_ke)
                    .res()
                    .await
                    .map_err(|e| {
                        format!(
                            "Failed create LivelinessToken associated to route for Subscriber {ros2_name} : {e}"
                        )
                    })?,
            );
        }
        Ok(())
    }

    // Retire the route over Zenoh removing the LivelinessToken
    fn retire_route(&mut self) {
        tracing::debug!("{self} deactivate");
        // Drop Zenoh Subscriber and Liveliness token
        // The DDS Writer remains to be discovered by local ROS nodes
        self.zenoh_subscriber = None;
        self.liveliness_token = None;
    }

    /// If this route uses a FetchingSubscriber, query for historical publications
    /// using the specified Selector. Otherwise, do nothing.
    pub async fn query_historical_publications<'a>(&mut self, plugin_id: &keyexpr) {
        if let Some(ZSubscriber::FetchingSubscriber(sub)) = &mut self.zenoh_subscriber {
            // query all PublicationCaches on "<KE_PREFIX_PUB_CACHE>/<plugin_id>/<routing_keyexpr>"
            let query_selector: Selector =
                (*KE_PREFIX_PUB_CACHE / plugin_id / &self.zenoh_key_expr).into();
            tracing::debug!("Route Subscriber (Zenoh:{} -> ROS:{}): query historical messages from {plugin_id} for TRANSIENT_LOCAL Reader on {query_selector}",
                self.zenoh_key_expr, self.ros2_name
            );

            if let Err(e) = sub
                .fetch({
                    let session = &self.context.zsession;
                    let query_selector = query_selector.clone();
                    let queries_timeout = self.queries_timeout;
                    move |cb| {
                        use zenoh_core::SyncResolve;
                        session
                            .get(&query_selector)
                            .target(QueryTarget::All)
                            .consolidation(ConsolidationMode::None)
                            .accept_replies(ReplyKeyExpr::Any)
                            .timeout(queries_timeout)
                            .callback(cb)
                            .res_sync()
                    }
                })
                .res()
                .await
            {
                tracing::warn!(
                    "{}: query for historical publications on {} failed: {}",
                    self,
                    query_selector,
                    e
                );
            }
        }
    }

    #[inline]
    pub fn add_remote_route(&mut self, plugin_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .insert(format!("{plugin_id}:{zenoh_key_expr}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn remove_remote_route(&mut self, plugin_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .remove(&format!("{plugin_id}:{zenoh_key_expr}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn is_serving_remote_route(&self) -> bool {
        !self.remote_routes.is_empty()
    }

    #[inline]
    pub async fn add_local_node(&mut self, entity_key: String, discovered_reader_qos: &Qos) {
        self.local_nodes.insert(entity_key);
        tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if 1st local node added, activate the route
        if self.local_nodes.len() == 1 {
            if let Err(e) = self.announce_route(discovered_reader_qos).await {
                tracing::error!("{self} activation failed: {e}");
            }
        }
    }

    #[inline]
    pub fn remove_local_node(&mut self, entity_key: &str) {
        self.local_nodes.remove(entity_key);
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

fn route_zenoh_message_to_dds(s: Sample, ros2_name: &str, data_writer: dds_entity_t) {
    if *LOG_PAYLOAD {
        tracing::debug!(
            "Route Subscriber (Zenoh:{} -> ROS:{}): routing message - payload: {:02x?}",
            s.key_expr,
            &ros2_name,
            s.value.payload
        );
    } else {
        tracing::trace!(
            "Route Subscriber (Zenoh:{} -> ROS:{}): routing message - {} bytes",
            s.key_expr,
            &ros2_name,
            s.value.payload.len()
        );
    }

    unsafe {
        let bs = s.value.payload.contiguous().into_owned();
        // As per the Vec documentation (see https://doc.rust-lang.org/std/vec/struct.Vec.html#method.into_raw_parts)
        // the only way to correctly releasing it is to create a vec using from_raw_parts
        // and then have its destructor do the cleanup.
        // Thus, while tempting to just pass the raw pointer to cyclone and then free it from C,
        // that is not necessarily safe or guaranteed to be leak free.
        // TODO replace when stable https://github.com/rust-lang/rust/issues/65816
        let (ptr, len, capacity) = vec_into_raw_parts(bs);
        let size: ddsrt_iov_len_t = match ddsrt_iov_len_from_usize(len) {
            Ok(s) => s,
            Err(_) => {
                tracing::warn!(
                    "Route Subscriber (Zenoh:{} -> ROS:{}): can't route message; excessive payload size ({})",
                    s.key_expr,
                    ros2_name,
                    len
                );
                return;
            }
        };

        let data_out = ddsrt_iovec_t {
            iov_base: ptr as *mut std::ffi::c_void,
            iov_len: size,
        };

        let mut sertype_ptr: *const ddsi_sertype = std::ptr::null_mut();
        let ret = dds_get_entity_sertype(data_writer, &mut sertype_ptr);
        if ret < 0 {
            tracing::warn!(
                "Route Subscriber (Zenoh:{} -> ROS:{}): can't route message; sertype lookup failed ({})",
                s.key_expr,
                ros2_name,
                CStr::from_ptr(dds_strretcode(ret))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            );
            return;
        }

        let fwdp = ddsi_serdata_from_ser_iov(
            sertype_ptr,
            ddsi_serdata_kind_SDK_DATA,
            1,
            &data_out,
            size as usize,
        );

        let ret = dds_writecdr(data_writer, fwdp);
        if ret < 0 {
            tracing::warn!(
                "Route Subscriber (Zenoh:{} -> ROS:{}): DDS write({data_writer}) failed: {}",
                s.key_expr,
                ros2_name,
                CStr::from_ptr(dds_strretcode(ret))
                    .to_str()
                    .unwrap_or("unrecoverable DDS retcode")
            );
            return;
        }

        drop(Vec::from_raw_parts(ptr, len, capacity));
    }
}
