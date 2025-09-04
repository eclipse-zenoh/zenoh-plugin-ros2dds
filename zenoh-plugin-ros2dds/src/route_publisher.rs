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
    ops::Deref,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use cyclors::{
    qos::{HistoryKind, Qos},
    DDS_LENGTH_UNLIMITED,
};
use serde::{Serialize, Serializer};
use zenoh::{
    key_expr::{keyexpr, OwnedKeyExpr},
    liveliness::LivelinessToken,
    qos::{CongestionControl, Priority, Reliability},
    sample::Locality,
    Wait,
};
use zenoh_ext::{AdvancedPublisher, AdvancedPublisherBuilderExt, CacheConfig};

use crate::{
    dds_types::{DDSRawSample, TypeInfo},
    dds_utils::{
        create_dds_reader, delete_dds_entity, get_guid, serialize_atomic_entity_guid,
        AtomicDDSEntity, DDS_ENTITY_NULL,
    },
    liveliness_mgt::new_ke_liveliness_pub,
    qos_helpers::*,
    ros2_utils::{is_message_for_action, ros2_message_type_to_dds_type},
    ros_discovery::RosDiscoveryInfoMgr,
    routes_mgr::Context,
    Config, LOG_PAYLOAD,
};

pub struct ZPublisher {
    publisher: Arc<AdvancedPublisher<'static>>,
    cache_size: usize,
}

impl Deref for ZPublisher {
    type Target = Arc<AdvancedPublisher<'static>>;

    fn deref(&self) -> &Self::Target {
        &self.publisher
    }
}

// a route from DDS to Zenoh
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RoutePublisher {
    // the ROS2 Publisher name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    // the zenoh publisher used to re-publish to zenoh the message received by the DDS Reader
    // `None` when route is created on a remote announcement and no local ROS2 Subscriber discovered yet
    #[serde(
        rename = "publication_cache_size",
        serialize_with = "serialize_pub_cache"
    )]
    zenoh_publisher: ZPublisher,
    // the local DDS Reader created to serve the route (i.e. re-publish to zenoh message coming from DDS)
    #[serde(serialize_with = "serialize_atomic_entity_guid")]
    dds_reader: Arc<AtomicDDSEntity>,
    // the Zenoh Priority for publications
    #[serde(serialize_with = "serialize_priority")]
    priority: Priority,
    // TypeInfo for Reader creation (if available)
    #[serde(skip)]
    _type_info: Option<Arc<TypeInfo>>,
    // if the topic is keyless
    #[serde(skip)]
    keyless: bool,
    // the QoS for the DDS Reader to be created.
    // those are either the QoS announced by a remote bridge on a Reader discovery,
    // either the QoS adapted from a local discovered Writer
    #[serde(skip)]
    _reader_qos: Qos,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken>,
    // the list of remote routes served by this route ("<zenoh_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route
    local_nodes: HashSet<String>,
}

impl Drop for RoutePublisher {
    fn drop(&mut self) {
        self.deactivate_dds_reader();
    }
}

impl fmt::Display for RoutePublisher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Publisher (ROS:{} -> Zenoh:{})",
            self.ros2_name, self.zenoh_key_expr
        )
    }
}

impl RoutePublisher {
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        type_info: &Option<Arc<TypeInfo>>,
        keyless: bool,
        reader_qos: Qos,
        context: Context,
    ) -> Result<RoutePublisher, String> {
        tracing::debug!(
            "Route Publisher ({ros2_name} -> {zenoh_key_expr}): creation with type {ros2_type}"
        );

        // create the zenoh Publisher
        // if Reader shall be TRANSIENT_LOCAL, use a PublicationCache to store historical messages
        let transient_local: bool = is_transient_local(&reader_qos);
        let cache_size: usize = if transient_local {
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
            tracing::debug!(
                "Route Publisher ({ros2_name} -> {zenoh_key_expr}): caching TRANSIENT_LOCAL publications with history={history} (computed from Reader's QoS: history=({:?},{}), durability_service.max_instances={})",
                history_qos.kind, history_qos.depth, durability_service_qos.max_instances
            );
            history
        } else {
            0
        };

        // CongestionControl to be used when re-publishing over zenoh: Blocking if Writer is RELIABLE (since we don't know what is remote Reader's QoS)
        let congestion_ctrl = match (
            context.config.reliable_routes_blocking,
            is_reliable(&reader_qos),
        ) {
            (true, true) => CongestionControl::Block,
            _ => CongestionControl::Drop,
        };

        // Priority if configured for this topic
        let (priority, is_express) = context
            .config
            .get_pub_priority_and_express(&ros2_name)
            .unwrap_or_default();
        tracing::debug!(
            "Route Publisher ({ros2_name} -> {zenoh_key_expr}): congestion_ctrl {:?}, priority {:?}, express:{}",
            congestion_ctrl,
            priority,
            is_express
        );

        let mut publisher_builder = context
            .zsession
            .declare_publisher(zenoh_key_expr.clone())
            .advanced();
        if transient_local {
            publisher_builder = publisher_builder
                .cache(CacheConfig::default().max_samples(cache_size))
                .publisher_detection();
        }

        let publisher: Arc<AdvancedPublisher<'static>> = Arc::new(
            publisher_builder
                .reliability(Reliability::Reliable)
                .allowed_destination(Locality::Remote)
                .congestion_control(congestion_ctrl)
                .express(is_express)
                .priority(priority)
                .await
                .map_err(|e| format!("Failed create Publisher for key {zenoh_key_expr}: {e}",))?,
        );

        // activate/deactivate DDS Reader on detection/undetection of matching Subscribers
        // (copy/move all required args for the callback)
        let dds_reader: Arc<AtomicDDSEntity> = Arc::new(DDS_ENTITY_NULL.into());

        publisher
            .matching_listener()
            .callback({
                let dds_reader = dds_reader.clone();
                let ros2_name = ros2_name.clone();
                let ros2_type = ros2_type.clone();
                let zenoh_key_expr = zenoh_key_expr.clone();
                let route_id =
                    format!("Route Publisher (ROS:{ros2_name} -> Zenoh:{zenoh_key_expr})");
                let context = context.clone();
                let reader_qos = reader_qos.clone();
                let type_info = type_info.clone();
                let publisher = publisher.clone();

                move |status| {
                    tracing::debug!("{route_id} MatchingStatus changed: {status:?}");
                    if status.matching() {
                        if let Err(e) = activate_dds_reader(
                            &dds_reader,
                            &ros2_name,
                            &ros2_type,
                            &route_id,
                            &context,
                            keyless,
                            &reader_qos,
                            &type_info,
                            &publisher,
                        ) {
                            tracing::error!("{route_id}: failed to activate DDS Reader: {e}");
                        }
                    } else {
                        deactivate_dds_reader(&dds_reader, &route_id, &context.ros_discovery_mgr)
                    }
                }
            })
            .background()
            .await
            .map_err(|e| format!("Failed to listen of matching status changes: {e}",))?;

        Ok(RoutePublisher {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            zenoh_publisher: ZPublisher {
                publisher,
                cache_size,
            },
            dds_reader,
            priority,
            _type_info: type_info.clone(),
            _reader_qos: reader_qos,
            keyless,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    fn deactivate_dds_reader(&mut self) {
        let dds_reader = self.dds_reader.swap(DDS_ENTITY_NULL, Ordering::Relaxed);
        if dds_reader != DDS_ENTITY_NULL {
            // remove reader's GID from ros_discovery_info message
            match get_guid(&dds_reader) {
                Ok(gid) => self.context.ros_discovery_mgr.remove_dds_reader(gid),
                Err(e) => tracing::warn!("{self}: {e}"),
            }
            if let Err(e) = delete_dds_entity(dds_reader) {
                tracing::warn!("{}: error deleting DDS Reader:  {}", self, e);
            }
        }
    }

    async fn announce_route(&mut self, discovered_writer_qos: &Qos) -> Result<(), String> {
        // only if not for an Action (since actions declare their own liveliness)
        if !is_message_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_pub(
                &self.context.zsession.zid().into_keyexpr(),
                &self.zenoh_key_expr,
                &self.ros2_type,
                self.keyless,
                discovered_writer_qos,
            )?;
            let ros2_name = self.ros2_name.clone();
            self.liveliness_token = Some(self.context.zsession
                .liveliness()
                .declare_token(liveliness_ke)
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
        if self.local_nodes.insert(node) {
            tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
            // if 1st local node added, announce the route
            if self.local_nodes.len() == 1 {
                if let Err(e) = self.announce_route(discovered_writer_qos).await {
                    tracing::error!("{self} announcement failed: {e}");
                }
            }
        }
    }

    #[inline]
    pub fn remove_local_node(&mut self, node: &str) {
        if self.local_nodes.remove(node) {
            tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
            // if last local node removed, retire the route
            if self.local_nodes.is_empty() {
                self.retire_route();
            }
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

fn serialize_priority<S>(p: &Priority, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_u8(*p as u8)
}

// Return the read period if name matches one of the "pub_max_frequencies" option
fn get_read_period(config: &Config, ros2_name: &str) -> Option<Duration> {
    config
        .get_pub_max_frequencies(ros2_name)
        .map(|f| Duration::from_secs_f32(1f32 / f))
}

#[allow(clippy::too_many_arguments)]
fn activate_dds_reader(
    dds_reader: &Arc<AtomicDDSEntity>,
    ros2_name: &str,
    ros2_type: &str,
    route_id: &str,
    context: &Context,
    keyless: bool,
    reader_qos: &Qos,
    type_info: &Option<Arc<TypeInfo>>,
    publisher: &Arc<AdvancedPublisher<'static>>,
) -> Result<(), String> {
    tracing::debug!("{route_id}: create Reader with {reader_qos:?}");
    let topic_name: String = format!("rt{}", ros2_name);
    let type_name = ros2_message_type_to_dds_type(ros2_type);
    let read_period = get_read_period(&context.config, ros2_name);

    // create matching DDS Reader that forwards message coming from DDS to Zenoh
    let reader = create_dds_reader(
        context.participant,
        topic_name.clone(),
        type_name,
        type_info,
        keyless,
        reader_qos.clone(),
        read_period,
        {
            let route_id = route_id.to_string();
            let publisher = publisher.clone();
            move |sample: &DDSRawSample| {
                route_dds_message_to_zenoh(sample, &publisher, &route_id);
            }
        },
    )?;
    let old = dds_reader.deref().swap(reader, Ordering::Relaxed);
    // add reader's GID in ros_discovery_info message
    context.ros_discovery_mgr.add_dds_reader(get_guid(&reader)?);

    if old != DDS_ENTITY_NULL {
        tracing::warn!("{route_id}: on activation their was already a DDS Reader - overwrite it");
        if let Err(e) = delete_dds_entity(old) {
            tracing::warn!("{route_id}: failed to delete overwritten DDS Reader: {e}");
        }
    }

    Ok(())
}

fn deactivate_dds_reader(
    dds_reader: &Arc<AtomicDDSEntity>,
    route_id: &str,
    ros_discovery_mgr: &Arc<RosDiscoveryInfoMgr>,
) {
    tracing::debug!("{route_id}: delete Reader");
    let reader = dds_reader.swap(DDS_ENTITY_NULL, Ordering::Relaxed);
    if reader != DDS_ENTITY_NULL {
        // remove reader's GID from ros_discovery_info message
        match get_guid(&reader) {
            Ok(gid) => ros_discovery_mgr.remove_dds_reader(gid),
            Err(e) => tracing::warn!("{route_id}: {e}"),
        }
        if let Err(e) = delete_dds_entity(reader) {
            tracing::warn!("{route_id}: error deleting DDS Reader:  {e}");
        }
    }
}

fn route_dds_message_to_zenoh(
    sample: &DDSRawSample,
    publisher: &Arc<AdvancedPublisher>,
    route_id: &str,
) {
    if *LOG_PAYLOAD {
        tracing::debug!("{route_id}: routing message - payload: {:02x?}", sample);
    } else {
        tracing::trace!("{route_id}: routing message - {} bytes", sample.len());
    }
    if let Err(e) = publisher.put(sample).wait() {
        tracing::error!("{route_id}: failed to route message: {e}");
    }
}
