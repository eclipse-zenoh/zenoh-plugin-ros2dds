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
use cyclors::{dds_entity_t, DDS_LENGTH_UNLIMITED};
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashSet, fmt};
use zenoh::liveliness::LivelinessToken;
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh_ext::{PublicationCache, SessionExt};

use crate::dds_discovery::create_forwarding_dds_reader;
use crate::dds_types::TypeInfo;
use crate::dds_utils::{delete_dds_entity, get_guid, serialize_entity_guid};
use crate::gid::Gid;
use crate::liveliness_mgt::new_ke_liveliness_pub;
use crate::ros2_utils::{is_message_for_action, ros2_message_type_to_dds_type};
use crate::routes_mgr::Context;
use crate::{qos_helpers::*, Config};
use crate::{serialize_option_as_bool, KE_PREFIX_PUB_CACHE};

enum ZPublisher<'a> {
    Publisher(KeyExpr<'a>),
    PublicationCache(PublicationCache<'a>),
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
    #[serde(rename = "is_active", serialize_with = "serialize_option_as_bool")]
    zenoh_publisher: Option<ZPublisher<'a>>,
    // the local DDS Reader created to serve the route (i.e. re-publish to zenoh data coming from DDS)
    #[serde(serialize_with = "serialize_entity_guid")]
    dds_reader: dds_entity_t,
    // if the Reader is TRANSIENT_LOCAL
    transient_local: bool,
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

impl Drop for RoutePublisher<'_> {
    fn drop(&mut self) {
        if let Err(e) = delete_dds_entity(self.dds_reader) {
            log::warn!("{}: error deleting DDS Reader:  {}", self, e);
        }
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
        let transient_local = is_transient_local(&reader_qos);
        log::debug!(
            "Route Publisher ({ros2_name} -> {zenoh_key_expr}): creation with type {ros2_type}"
        );

        // declare the zenoh key expression (for wire optimization)
        let declared_ke = context.zsession
            .declare_keyexpr(zenoh_key_expr.clone())
            .res()
            .await
            .map_err(|e| {
                format!("Route Publisher ({ros2_name} -> {zenoh_key_expr}): failed to declare KeyExpr: {e}")
            })?;

        // CongestionControl to be used when re-publishing over zenoh: Blocking if Writer is RELIABLE (since we don't know what is remote Reader's QoS)
        let congestion_ctrl = match (
            context.config.reliable_routes_blocking,
            is_reliable(&reader_qos),
        ) {
            (true, true) => CongestionControl::Block,
            _ => CongestionControl::Drop,
        };

        let topic_name = format!("rt{ros2_name}");
        let type_name = ros2_message_type_to_dds_type(&ros2_type);
        let read_period = get_read_period(&context.config, &zenoh_key_expr);

        // create matching DDS Reader that forwards data coming from DDS to Zenoh
        let dds_reader = create_forwarding_dds_reader(
            context.participant,
            topic_name,
            type_name,
            type_info,
            keyless,
            reader_qos.clone(),
            declared_ke.clone(),
            context.zsession.clone(),
            read_period,
            congestion_ctrl,
        )?;

        Ok(RoutePublisher {
            ros2_name,
            ros2_type,
            dds_reader,
            zenoh_key_expr,
            context: context.clone(),
            zenoh_publisher: None,
            transient_local,
            keyless,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    async fn activate<'a>(&'a mut self, discovered_writer_qos: &Qos) -> Result<(), String> {
        // For lifetime issue, redeclare the zenoh key expression that can't be stored in Self
        let declared_ke = self
            .context
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

        // create the zenoh Publisher
        // if Reader is TRANSIENT_LOCAL, use a PublicationCache to store historical data
        self.zenoh_publisher = if self.transient_local {
            #[allow(non_upper_case_globals)]
            let history_qos = get_history_or_default(discovered_writer_qos);
            let durability_service_qos = get_durability_service_or_default(discovered_writer_qos);
            let mut history = match (history_qos.kind, history_qos.depth) {
                (HistoryKind::KEEP_LAST, n) => {
                    if self.keyless {
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
            history = history.saturating_mul(self.context.config.transient_local_cache_multiplier);
            log::debug!(
                "{self}: caching TRANSIENT_LOCAL publications via a PublicationCache with history={history} (computed from Reader's QoS: history=({:?},{}), durability_service.max_instances={})",
                history_qos.kind, history_qos.depth, durability_service_qos.max_instances
            );
            let pub_cache = self
                .context
                .zsession
                .declare_publication_cache(&declared_ke)
                .history(history)
                .queryable_prefix(*KE_PREFIX_PUB_CACHE / &self.context.plugin_id)
                .queryable_allowed_origin(Locality::Remote) // Note: don't reply to queries from local QueryingSubscribers
                .res()
                .await
                .map_err(|e| {
                    format!(
                        "Failed create PublicationCache for key {} (rid={}): {e}",
                        self.zenoh_key_expr, declared_ke
                    )
                })?;
            Some(ZPublisher::PublicationCache(pub_cache))
        } else {
            if let Err(e) = self
                .context
                .zsession
                .declare_publisher(declared_ke.clone())
                .res()
                .await
            {
                log::warn!(
                    "Failed to declare publisher for key {} (rid={}): {}",
                    self.zenoh_key_expr,
                    declared_ke,
                    e
                );
            }
            Some(ZPublisher::Publisher(declared_ke.clone()))
        };

        // if not for an Action (since actions declare their own liveliness)
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
                .res()
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

    fn deactivate(&mut self) {
        log::debug!("{self} deactivate");
        // Drop Zenoh Publisher and Liveliness token
        // The DDS Writer remains to be discovered by local ROS nodes
        self.zenoh_publisher = None;
        self.liveliness_token = None;
    }

    #[inline]
    pub fn dds_reader_guid(&self) -> Result<Gid, String> {
        get_guid(&self.dds_reader)
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
    pub async fn add_local_node(&mut self, node: String, discovered_writer_qos: &Qos) {
        self.local_nodes.insert(node);
        log::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if 1st local node added, activate the route
        if self.local_nodes.len() == 1 {
            if let Err(e) = self.activate(discovered_writer_qos).await {
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

// Return the read period if keyexpr matches one of the "pub_max_frequencies" option
fn get_read_period(config: &Config, ke: &keyexpr) -> Option<Duration> {
    for (re, freq) in &config.pub_max_frequencies {
        if re.is_match(ke) {
            return Some(Duration::from_secs_f32(1f32 / freq));
        }
    }
    None
}
