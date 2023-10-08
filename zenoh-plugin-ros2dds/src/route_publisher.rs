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

use cyclors::qos::{HistoryKind, Qos};
use cyclors::DDS_LENGTH_UNLIMITED;
use serde::{Serialize, Serializer};
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashSet, fmt};
use zenoh::liveliness::LivelinessToken;
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh::publication::Publisher;
use zenoh_core::SyncResolve;
use zenoh_ext::{PublicationCache, SessionExt};

use crate::dds_types::{DDSRawSample, TypeInfo};
use crate::dds_utils::{
    create_dds_reader, delete_dds_entity, get_guid, serialize_atomic_entity_guid, AtomicDDSEntity,
    DDS_ENTITY_NULL,
};
use crate::liveliness_mgt::new_ke_liveliness_pub;
use crate::ros2_utils::{is_message_for_action, ros2_message_type_to_dds_type};
use crate::routes_mgr::Context;
use crate::{qos_helpers::*, Config};
use crate::{KE_PREFIX_PUB_CACHE, LOG_PAYLOAD};

pub struct ZPublisher<'a> {
    publisher: Publisher<'static>,
    _cache: Option<PublicationCache<'a>>,
    cache_size: usize,
}

impl<'a> Deref for ZPublisher<'a> {
    type Target = Publisher<'static>;

    fn deref(&self) -> &Self::Target {
        &self.publisher
    }
}

// a route from DDS to Zenoh
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RoutePublisher<'a> {
    // the ROS2 Publisher name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context<'a>,
    // the zenoh publisher used to re-publish to zenoh the data received by the DDS Reader
    // `None` when route is created on a remote announcement and no local ROS2 Subscriber discovered yet
    #[serde(
        rename = "publication_cache_size",
        serialize_with = "serialize_pub_cache"
    )]
    zenoh_publisher: ZPublisher<'a>,
    // the local DDS Reader created to serve the route (i.e. re-publish to zenoh data coming from DDS)
    #[serde(serialize_with = "serialize_atomic_entity_guid")]
    dds_reader: AtomicDDSEntity,
    // TypeInfo for Reader creation (if available)
    #[serde(skip)]
    type_info: Option<Arc<TypeInfo>>,
    // if the topic is keyless
    #[serde(skip)]
    keyless: bool,
    // the QoS for the DDS Reader to be created.
    // those are either the QoS announced by a remote bridge on a Reader discovery,
    // either the QoS adapted from a local disovered Writer
    #[serde(skip)]
    reader_qos: Qos,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken<'a>>,
    // the list of remote routes served by this route ("<plugin_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route
    local_nodes: HashSet<String>,
}

impl Drop for RoutePublisher<'_> {
    fn drop(&mut self) {
        self.deactivate_dds_reader();
    }
}

impl fmt::Display for RoutePublisher<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Publisher (ROS:{} -> Zenoh:{})",
            self.ros2_name, self.zenoh_key_expr
        )
    }
}

impl RoutePublisher<'_> {
    #[allow(clippy::too_many_arguments)]
    pub async fn create<'a>(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        type_info: &Option<Arc<TypeInfo>>,
        keyless: bool,
        reader_qos: Qos,
        context: &Context<'a>,
    ) -> Result<RoutePublisher<'a>, String> {
        log::debug!(
            "Route Publisher ({ros2_name} -> {zenoh_key_expr}): creation with type {ros2_type}"
        );

        // create the zenoh Publisher
        // if Reader shall be TRANSIENT_LOCAL, use a PublicationCache to store historical data
        let transient_local = is_transient_local(&reader_qos);
        let (cache, cache_size) = if transient_local {
            #[allow(non_upper_case_globals)]
            let history_qos = get_history_or_default(&reader_qos);
            let durability_service_qos = get_durability_service_or_default(&reader_qos);
            let mut history = match (history_qos.kind, history_qos.depth) {
                (HistoryKind::KEEP_LAST, n) => {
                    if keyless {
                        // only 1 instance => history=n
                        n as usize
                    } else if durability_service_qos.max_instances == DDS_LENGTH_UNLIMITED {
                        // No limit! => history=MAX
                        usize::MAX
                    } else if durability_service_qos.max_instances > 0 {
                        // Compute cache size as history.depth * durability_service.max_instances
                        // This makes the assumption that the frequency of publication is the same for all instances...
                        // But as we have no way to have 1 cache per-instance, there is no other choice.
                        n.saturating_mul(durability_service_qos.max_instances) as usize
                    } else {
                        n as usize
                    }
                }
                (HistoryKind::KEEP_ALL, _) => usize::MAX,
            };
            // In case there are several Writers served by this route, increase the cache size
            history = history.saturating_mul(context.config.transient_local_cache_multiplier);
            log::debug!(
                "Route Publisher ({ros2_name} -> {zenoh_key_expr}): caching TRANSIENT_LOCAL publications via a PublicationCache with history={history} (computed from Reader's QoS: history=({:?},{}), durability_service.max_instances={})",
                history_qos.kind, history_qos.depth, durability_service_qos.max_instances
            );
            (
                Some(
                    context
                        .zsession
                        .declare_publication_cache(&zenoh_key_expr)
                        .history(history)
                        .queryable_prefix(*KE_PREFIX_PUB_CACHE / &context.plugin_id)
                        .queryable_allowed_origin(Locality::Remote) // Note: don't reply to queries from local QueryingSubscribers
                        .res_async()
                        .await
                        .map_err(|e| {
                            format!("Failed create PublicationCache for key {zenoh_key_expr}: {e}",)
                        })?,
                ),
                history,
            )
        } else {
            (None, 0)
        };

        // CongestionControl to be used when re-publishing over zenoh: Blocking if Writer is RELIABLE (since we don't know what is remote Reader's QoS)
        let congestion_ctrl = match (
            context.config.reliable_routes_blocking,
            is_reliable(&reader_qos),
        ) {
            (true, true) => CongestionControl::Block,
            _ => CongestionControl::Drop,
        };

        let publisher: Publisher<'static> = context
            .zsession
            .declare_publisher(zenoh_key_expr.clone())
            .congestion_control(congestion_ctrl)
            .res_async()
            .await
            .map_err(|e| format!("Failed create Publisher for key {zenoh_key_expr}: {e}",))?;

        Ok(RoutePublisher {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context: context.clone(),
            zenoh_publisher: ZPublisher {
                publisher,
                _cache: cache,
                cache_size,
            },
            dds_reader: DDS_ENTITY_NULL.into(),
            type_info: type_info.clone(),
            reader_qos,
            keyless,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    fn activate_dds_reader(&mut self) -> Result<(), String> {
        let topic_name = format!("rt{}", self.ros2_name);
        let type_name = ros2_message_type_to_dds_type(&self.ros2_type);
        let read_period = get_read_period(&self.context.config, &self.zenoh_key_expr);
        let route_id = self.to_string();
        let publisher = self.zenoh_publisher.deref().clone();

        // create matching DDS Reader that forwards data coming from DDS to Zenoh
        let dds_reader = create_dds_reader(
            self.context.participant,
            topic_name,
            type_name,
            &self.type_info,
            self.keyless,
            self.reader_qos.clone(),
            read_period,
            move |sample: &DDSRawSample| {
                do_route_message(
                    sample, &publisher, // &ke,
                    &route_id,
                );
            },
        )?;
        self.dds_reader.swap(dds_reader, Ordering::Relaxed);

        // add reader's GID in ros_discovery_info message
        self.context
            .ros_discovery_mgr
            .add_dds_reader(get_guid(&dds_reader)?);

        Ok(())
    }

    fn deactivate_dds_reader(&mut self) {
        let dds_reader = self.dds_reader.swap(DDS_ENTITY_NULL, Ordering::Relaxed);
        if dds_reader != DDS_ENTITY_NULL {
            // remove reader's GID from ros_discovery_info message
            match get_guid(&dds_reader) {
                Ok(gid) => self.context.ros_discovery_mgr.remove_dds_reader(gid),
                Err(e) => log::warn!("{self}: {e}"),
            }
            if let Err(e) = delete_dds_entity(dds_reader) {
                log::warn!("{}: error deleting DDS Reader:  {}", self, e);
            }
        }
    }

    async fn announce_route(&mut self, discovered_writer_qos: &Qos) -> Result<(), String> {
        // only if not for an Action (since actions declare their own liveliness)
        if !is_message_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_pub(
                &self.context.plugin_id,
                &self.zenoh_key_expr,
                &self.ros2_type,
                self.keyless,
                discovered_writer_qos,
            )?;
            let ros2_name = self.ros2_name.clone();
            self.liveliness_token = Some(self.context.zsession
                .liveliness()
                .declare_token(liveliness_ke)
                .res_async()
                .await
                .map_err(|e| {
                    format!(
                        "Failed create LivelinessToken associated to route for Publisher {ros2_name}: {e}"
                    )
                })?
            );
        }
        Ok(())
    }

    fn retire_route(&mut self) {
        self.liveliness_token = None;
    }

    #[inline]
    pub fn add_remote_route(&mut self, plugin_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .insert(format!("{plugin_id}:{zenoh_key_expr}"));
        log::debug!("{self} now serving remote routes {:?}", self.remote_routes);
        // if 1st remote route added, activate the DDS Reader
        if self.remote_routes.len() == 1 {
            if let Err(e) = self.activate_dds_reader() {
                log::error!("{self} activation of DDS Reader failed: {e}");
            }
        }
    }

    #[inline]
    pub fn remove_remote_route(&mut self, plugin_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .remove(&format!("{plugin_id}:{zenoh_key_expr}"));
        log::debug!("{self} now serving remote routes {:?}", self.remote_routes);
        // if last remote route removed, deactivate the DDS Reader
        if self.remote_routes.is_empty() {
            self.deactivate_dds_reader();
        }
    }

    #[inline]
    pub fn is_serving_remote_route(&self) -> bool {
        !self.remote_routes.is_empty()
    }

    #[inline]
    pub async fn add_local_node(&mut self, node: String, discovered_writer_qos: &Qos) {
        self.local_nodes.insert(node);
        log::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if 1st local node added, announce the route
        if self.local_nodes.len() == 1 {
            if let Err(e) = self.announce_route(discovered_writer_qos).await {
                log::error!("{self} announcement failed: {e}");
            }
        }
    }

    #[inline]
    pub fn remove_local_node(&mut self, node: &str) {
        self.local_nodes.remove(node);
        log::debug!("{self} now serving local nodes {:?}", self.local_nodes);
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

pub fn serialize_pub_cache<S>(zpub: &ZPublisher, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_u64(zpub.cache_size as u64)
}

// Return the read period if keyexpr matches one of the "pub_max_frequencies" option
fn get_read_period(config: &Config, ke: &keyexpr) -> Option<Duration> {
    for (re, freq) in &config.pub_max_frequencies {
        if re.is_match(ke) {
            return Some(Duration::from_secs_f32(1f32 / freq));
        }
    }
    None
}

fn do_route_message(sample: &DDSRawSample, publisher: &Publisher, route_id: &str) {
    if *LOG_PAYLOAD {
        log::trace!("{route_id}: routing message - payload: {:02x?}", sample);
    } else {
        log::trace!("{route_id}: routing message - {} bytes", sample.len());
    }
    if let Err(e) = publisher.put(sample).res_sync() {
        log::error!("{route_id}: failed to route message: {e}");
    }
}
