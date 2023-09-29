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
use crate::config::Config;
use crate::discovered_entities::DiscoveredEntities;
use crate::events::ROS2AnnouncementEvent;
use crate::events::ROS2DiscoveryEvent;
use crate::qos_helpers::adapt_reader_qos_for_writer;
use crate::qos_helpers::adapt_writer_qos_for_reader;
use crate::ros_discovery::RosDiscoveryInfoMgr;
use crate::route_publisher::RoutePublisher;
use crate::route_subscriber::RouteSubscriber;
use cyclors::dds_entity_t;
use cyclors::qos::Qos;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use zenoh::prelude::keyexpr;
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::OwnedKeyExpr;
use zenoh::queryable::Query;
use zenoh::sample::Sample;
use zenoh::Session;
use zenoh_core::zread;

use crate::ke_for_sure;

lazy_static::lazy_static!(
    static ref KE_PREFIX_ROUTE_PUBLISHER: &'static keyexpr = ke_for_sure!("route/topic/pub");
    static ref KE_PREFIX_ROUTE_SUBSCRIBER: &'static keyexpr = ke_for_sure!("route/topic/sub");
    static ref KE_PREFIX_ROUTE_SERVICE_SRV: &'static keyexpr = ke_for_sure!("route/service/srv");
    static ref KE_PREFIX_ROUTE_SERVICE_CLI: &'static keyexpr = ke_for_sure!("route/service/cli");
);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RouteStatus {
    Routed(OwnedKeyExpr), // Routing is active, with the zenoh key expression used for the route
    NotAllowed,           // Routing was not allowed per configuration
    CreationFailure(String), // The route creation failed
    _QoSConflict,         // A route was already established but with conflicting QoS
}

#[derive(Debug)]
enum RouteRef {
    PublisherRoute(String),
    SubscriberRoute(String),
}

pub struct RoutesMgr<'a> {
    plugin_id: OwnedKeyExpr,
    config: Arc<Config>,
    zsession: &'a Arc<Session>,
    participant: dds_entity_t,
    discovered_entities: Arc<RwLock<DiscoveredEntities>>,
    // maps of established routes - ecah map indexed by topic/service/action name
    routes_publishers: HashMap<String, RoutePublisher<'a>>,
    routes_subscribers: HashMap<String, RouteSubscriber<'a>>,
    // ros_discovery_info read/write manager
    ros_discovery_mgr: Arc<RosDiscoveryInfoMgr>,
    admin_prefix: OwnedKeyExpr,
    // admin space: index is the admin_keyexpr (relative to admin_prefix)
    admin_space: HashMap<OwnedKeyExpr, RouteRef>,
}

impl<'a> RoutesMgr<'a> {
    pub fn new(
        plugin_id: OwnedKeyExpr,
        config: Arc<Config>,
        zsession: &'a Arc<Session>,
        participant: dds_entity_t,
        discovered_entities: Arc<RwLock<DiscoveredEntities>>,
        ros_discovery_mgr: Arc<RosDiscoveryInfoMgr>,
        admin_prefix: OwnedKeyExpr,
    ) -> RoutesMgr<'a> {
        RoutesMgr {
            plugin_id,
            config,
            zsession,
            participant,
            discovered_entities,
            routes_publishers: HashMap::new(),
            routes_subscribers: HashMap::new(),
            ros_discovery_mgr,
            admin_prefix,
            admin_space: HashMap::new(),
        }
    }

    pub async fn on_ros_discovery_event(
        &mut self,
        event: ROS2DiscoveryEvent,
    ) -> Result<(), String> {
        use ROS2DiscoveryEvent::*;
        match event {
            DiscoveredMsgPub(node, iface) => {
                let plugin_id = self.plugin_id.clone();
                // Retrieve info on DDS Writer
                let entity = {
                    let entities = zread!(self.discovered_entities);
                    entities
                        .get_writer(&iface.writer)
                        .ok_or(format!(
                            "Failed to get DDS info for {iface} Writer {}. Already deleted ?",
                            iface.writer
                        ))?
                        .clone()
                };
                // Get route (create it if not yet exists)
                let route = self
                    .get_or_create_route_publisher(
                        iface.name,
                        iface.typ,
                        entity.keyless,
                        adapt_writer_qos_for_reader(&entity.qos),
                    )
                    .await?;
                route
                    .add_local_node(node.into(), &plugin_id, &entity.qos)
                    .await;
            }

            UndiscoveredMsgPub(node, iface) => {
                if let Entry::Occupied(mut entry) = self.routes_publishers.entry(iface.name.clone())
                {
                    let route = entry.get_mut();
                    route.remove_local_node(&node);
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_PUBLISHER / iface.name_as_keyexpr()));
                        let route = entry.remove();
                        // remove reader's GID in ros_discovery_msg
                        self.ros_discovery_mgr
                            .remove_dds_reader(route.dds_reader_guid().map_err(|e| {
                                format!("Failed to update ros_discovery_info message: {e}")
                            })?);
                        log::info!("{route} removed");
                    }
                }
            }

            DiscoveredMsgSub(node, iface) => {
                // Retrieve info on DDS Reader
                let entity = {
                    let entities = zread!(self.discovered_entities);
                    entities
                        .get_reader(&iface.reader)
                        .ok_or(format!(
                            "Failed to get DDS info for {iface} Reader {}. Already deleted ?",
                            iface.reader
                        ))?
                        .clone()
                };
                let plugin_id = self.plugin_id.clone();
                let config = self.config.clone();
                // Get route (create it if not yet exists)
                let route = self
                    .get_or_create_route_subscriber(
                        iface.name,
                        iface.typ,
                        entity.keyless,
                        adapt_reader_qos_for_writer(&entity.qos),
                    )
                    .await?;
                route
                    .add_local_node(node.into(), &config, &plugin_id, &entity.qos)
                    .await;
            }

            UndiscoveredMsgSub(node, iface) => {
                if let Entry::Occupied(mut entry) =
                    self.routes_subscribers.entry(iface.name.clone())
                {
                    let route = entry.get_mut();
                    route.remove_local_node(&node);
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_SUBSCRIBER / iface.name_as_keyexpr()));
                        let route = entry.remove();
                        // remove writer's GID in ros_discovery_msg
                        self.ros_discovery_mgr
                            .remove_dds_writer(route.dds_writer_guid().map_err(|e| {
                                format!("Failed to update ros_discovery_info message: {e}")
                            })?);
                        log::info!("{route} removed");
                    }
                }
            }
            DiscoveredServiceSrv(_node, iface) => {
                log::info!("... TODO: create Service Server route for {}", iface.name);
            }
            UndiscoveredServiceSrv(_node, iface) => {
                log::info!("... TODO: delete Service Server route for {}", iface.name);
            }
            DiscoveredServiceCli(_node, iface) => {
                log::info!("... TODO: create Service Client route for {}", iface.name);
            }
            UndiscoveredServiceCli(_node, iface) => {
                log::info!("... TODO: delete Service Client route for {}", iface.name);
            }
            DiscoveredActionSrv(_node, iface) => {
                log::info!("... TODO: create Action Server route for {}", iface.name);
            }
            UndiscoveredActionSrv(_node, iface) => {
                log::info!("... TODO: delete Action Server route for {}", iface.name);
            }
            DiscoveredActionCli(_node, iface) => {
                log::info!("... TODO: create Action Client route for {}", iface.name);
            }
            UndiscoveredActionCli(_node, iface) => {
                log::info!("... TODO: delete Action Client route for {}", iface.name);
            }
        }
        Ok(())
    }

    pub async fn on_ros_announcement_event(
        &mut self,
        event: ROS2AnnouncementEvent,
    ) -> Result<(), String> {
        use ROS2AnnouncementEvent::*;
        match event {
            AnnouncedMsgPub {
                plugin_id,
                zenoh_key_expr,
                ros2_type,
                keyless,
                writer_qos,
            } => {
                // On remote Publisher route announcement, prepare a Subscriber route
                // with an associated DDS Writer allowing local ROS2 Nodes to discover it
                let route = self
                    .get_or_create_route_subscriber(
                        format!("/{zenoh_key_expr}"),
                        ros2_type,
                        keyless,
                        writer_qos,
                    )
                    .await?;
                route.add_remote_route(&plugin_id, &zenoh_key_expr);
            }

            RetiredMsgPub {
                plugin_id,
                zenoh_key_expr,
            } => {
                if let Entry::Occupied(mut entry) =
                    self.routes_subscribers.entry(format!("/{zenoh_key_expr}"))
                {
                    let route = entry.get_mut();
                    route.remove_remote_route(&plugin_id, &zenoh_key_expr);
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_SUBSCRIBER / &zenoh_key_expr));
                        let route = entry.remove();
                        // remove writer's GID in ros_discovery_msg
                        self.ros_discovery_mgr
                            .remove_dds_writer(route.dds_writer_guid().map_err(|e| {
                                format!("Failed to update ros_discovery_info message: {e}")
                            })?);
                        log::info!("{route} removed");
                    }
                }
            }

            AnnouncedMsgSub {
                plugin_id,
                zenoh_key_expr,
                ros2_type,
                keyless,
                reader_qos,
            } => {
                // On remote Subscriber route announcement, prepare a Publisher route
                // with an associated DDS Reader allowing local ROS2 Nodes to discover it
                let route = self
                    .get_or_create_route_publisher(
                        format!("/{zenoh_key_expr}"),
                        ros2_type,
                        keyless,
                        reader_qos,
                    )
                    .await?;
                route.add_remote_route(&plugin_id, &zenoh_key_expr);
            }

            RetiredMsgSub {
                plugin_id,
                zenoh_key_expr,
            } => {
                if let Entry::Occupied(mut entry) =
                    self.routes_publishers.entry(format!("/{zenoh_key_expr}"))
                {
                    let route = entry.get_mut();
                    route.remove_remote_route(&plugin_id, &zenoh_key_expr);
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_PUBLISHER / &zenoh_key_expr));
                        let route = entry.remove();
                        // remove reader's GID in ros_discovery_msg
                        self.ros_discovery_mgr
                            .remove_dds_reader(route.dds_reader_guid().map_err(|e| {
                                format!("Failed to update ros_discovery_info message: {e}")
                            })?);
                        log::info!("{route} removed");
                    }
                }
            }

            _ => log::info!("... TODO: manage {event:?}"),
        }
        Ok(())
    }

    pub async fn query_historical_all_publications(&mut self, plugin_id: &keyexpr) {
        for route in self.routes_subscribers.values_mut() {
            route
                .query_historical_publications(&plugin_id, self.config.queries_timeout)
                .await;
        }
    }

    async fn get_or_create_route_publisher(
        &mut self,
        ros2_name: String,
        ros2_type: String,
        keyless: bool,
        reader_qos: Qos,
    ) -> Result<&mut RoutePublisher<'a>, String> {
        match self.routes_publishers.entry(ros2_name.clone()) {
            Entry::Vacant(entry) => {
                // ROS2 topic name => Zenoh key expr : strip '/' prefix
                let zenoh_key_expr = ke_for_sure!(&ros2_name[1..]);
                // create route
                let route = RoutePublisher::create(
                    self.config.clone(),
                    &self.zsession,
                    self.participant,
                    ros2_name.clone(),
                    ros2_type,
                    zenoh_key_expr.to_owned(),
                    &None,
                    keyless,
                    reader_qos,
                )
                .await?;
                log::info!("{route} created");

                // insert reference in admin_space
                let admin_ke = *KE_PREFIX_ROUTE_PUBLISHER / zenoh_key_expr;
                self.admin_space
                    .insert(admin_ke, RouteRef::PublisherRoute(ros2_name));

                // insert reader's GID in ros_discovery_msg
                self.ros_discovery_mgr.add_dds_reader(
                    route
                        .dds_reader_guid()
                        .map_err(|e| format!("Failed to update ros_discovery_info message: {e}"))?,
                );

                Ok(entry.insert(route))
            }
            Entry::Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    async fn get_or_create_route_subscriber(
        &mut self,
        ros2_name: String,
        ros2_type: String,
        keyless: bool,
        writer_qos: Qos,
    ) -> Result<&mut RouteSubscriber<'a>, String> {
        match self.routes_subscribers.entry(ros2_name.clone()) {
            Entry::Vacant(entry) => {
                // ROS2 topic name => Zenoh key expr : strip '/' prefix
                let zenoh_key_expr = ke_for_sure!(&ros2_name[1..]);
                // create route
                let route = RouteSubscriber::create(
                    self.config.clone(),
                    &self.zsession,
                    self.participant,
                    ros2_name.clone(),
                    ros2_type,
                    zenoh_key_expr.to_owned(),
                    keyless,
                    writer_qos,
                )
                .await?;
                log::info!("{route} created");

                // insert reference in admin_space
                let admin_ke = *KE_PREFIX_ROUTE_SUBSCRIBER / zenoh_key_expr;
                self.admin_space
                    .insert(admin_ke, RouteRef::SubscriberRoute(ros2_name));

                // insert writer's GID in ros_discovery_msg
                self.ros_discovery_mgr.add_dds_writer(
                    route
                        .dds_writer_guid()
                        .map_err(|e| format!("Failed to update ros_discovery_info message: {e}"))?,
                );

                Ok(entry.insert(route))
            }
            Entry::Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    pub async fn treat_admin_query(&self, query: &Query) {
        let selector = query.selector();

        // get the list of sub-key expressions that will match the same stored keys than
        // the selector, if those keys had the admin_keyexpr_prefix.
        let sub_kes = selector.key_expr.strip_prefix(&self.admin_prefix);
        if sub_kes.is_empty() {
            log::error!("Received query for admin space: '{}' - but it's not prefixed by admin_keyexpr_prefix='{}'", selector, &self.admin_prefix);
            return;
        }

        // For all sub-key expression
        for sub_ke in sub_kes {
            if sub_ke.is_wild() {
                // iterate over all admin space to find matching keys and reply for each
                for (ke, route_ref) in self.admin_space.iter() {
                    if sub_ke.intersects(ke) {
                        self.send_admin_reply(query, ke, route_ref).await;
                    }
                }
            } else {
                // sub_ke correspond to 1 key - just get it and reply
                if let Some(route_ref) = self.admin_space.get(sub_ke) {
                    self.send_admin_reply(query, sub_ke, route_ref).await;
                }
            }
        }
    }

    async fn send_admin_reply(&self, query: &Query, key_expr: &keyexpr, route_ref: &RouteRef) {
        match self.get_entity_json_value(route_ref) {
            Ok(Some(v)) => {
                let admin_keyexpr = &self.admin_prefix / &key_expr;
                if let Err(e) = query
                    .reply(Ok(Sample::new(admin_keyexpr, v)))
                    .res_async()
                    .await
                {
                    log::warn!("Error replying to admin query {:?}: {}", query, e);
                }
            }
            Ok(None) => log::error!("INTERNAL ERROR: Dangling {:?} for {}", route_ref, key_expr),
            Err(e) => {
                log::error!("INTERNAL ERROR serializing admin value as JSON: {}", e)
            }
        }
    }

    fn get_entity_json_value(
        &self,
        route_ref: &RouteRef,
    ) -> Result<Option<serde_json::Value>, serde_json::Error> {
        match route_ref {
            RouteRef::PublisherRoute(ke) => self
                .routes_publishers
                .get(ke)
                .map(serde_json::to_value)
                .transpose(),
            RouteRef::SubscriberRoute(ke) => self
                .routes_subscribers
                .get(ke)
                .map(serde_json::to_value)
                .transpose(),
        }
    }
}
