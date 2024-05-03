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
use async_trait::async_trait;
use cyclors::*;
use events::ROS2AnnouncementEvent;
use flume::{unbounded, Receiver, Sender};
use futures::select;
use serde::Serializer;
use std::collections::HashMap;
use std::env;
use std::mem::ManuallyDrop;
use std::sync::Arc;
use zenoh::liveliness::LivelinessToken;
use zenoh::plugins::{RunningPlugin, RunningPluginTrait, ZenohPlugin};
use zenoh::prelude::r#async::AsyncResolve;
use zenoh::prelude::*;
use zenoh::queryable::Query;
use zenoh::runtime::Runtime;
use zenoh::Result as ZResult;
use zenoh::Session;
use zenoh_core::zerror;
use zenoh_ext::SubscriberBuilderExt;
use zenoh_plugin_trait::{plugin_long_version, plugin_version, Plugin, PluginControl};
use zenoh_util::Timed;

pub mod config;
mod dds_discovery;
mod dds_types;
mod dds_utils;
mod discovered_entities;
mod discovery_mgr;
mod events;
mod gid;
mod liveliness_mgt;
mod node_info;
mod qos_helpers;
mod ros2_utils;
mod ros_discovery;
mod route_action_cli;
mod route_action_srv;
mod route_publisher;
mod route_service_cli;
mod route_service_srv;
mod route_subscriber;
mod routes_mgr;
use config::Config;

use crate::dds_utils::get_guid;
use crate::discovery_mgr::DiscoveryMgr;
use crate::events::ROS2DiscoveryEvent;
use crate::liveliness_mgt::*;
use crate::ros_discovery::RosDiscoveryInfoMgr;
use crate::routes_mgr::RoutesMgr;

#[macro_export]
macro_rules! ke_for_sure {
    ($val:expr) => {
        unsafe { keyexpr::from_str_unchecked($val) }
    };
}

lazy_static::lazy_static!(
    pub static ref VERSION_JSON_VALUE: Value =
        serde_json::Value::String(ROS2Plugin::PLUGIN_LONG_VERSION.to_owned()).into();

    static ref LOG_PAYLOAD: bool = std::env::var("Z_LOG_PAYLOAD").is_ok();

    static ref KE_ANY_1_SEGMENT: &'static keyexpr = ke_for_sure!("*");
    static ref KE_ANY_N_SEGMENT: &'static keyexpr = ke_for_sure!("**");

    static ref KE_PREFIX_PUB_CACHE: &'static keyexpr = ke_for_sure!("@ros2_pub_cache");
);

zenoh::kedefine!(
    // Admin space key expressions of plugin's version
    pub ke_admin_version: "${plugin_status_key:**}/__version__",

    // Admin prefix of this bridge
    pub ke_admin_prefix: "@ros2/${plugin_id:*}/",
);

// CycloneDDS' localhost-only: set network interface address (shortened form of config would be
// possible, too, but I think it is clearer to spell it out completely).
// Empty configuration fragments are ignored, so it is safe to unconditionally append a comma.
const CYCLONEDDS_CONFIG_LOCALHOST_ONLY: &str = r#"<CycloneDDS><Domain><General><Interfaces><NetworkInterface address="127.0.0.1"/></Interfaces></General></Domain></CycloneDDS>,"#;

// CycloneDDS' enable-shm: enable usage of Iceoryx shared memory
#[cfg(feature = "dds_shm")]
const CYCLONEDDS_CONFIG_ENABLE_SHM: &str = r#"<CycloneDDS><Domain><SharedMemory><Enable>true</Enable></SharedMemory></Domain></CycloneDDS>,"#;

// interval between each read/write on "ros_discovery_info" topic
const ROS_DISCOVERY_INFO_POLL_INTERVAL_MS: u64 = 100;
const ROS_DISCOVERY_INFO_PUSH_INTERVAL_MS: u64 = 100;

#[cfg(feature = "dynamic_plugin")]
zenoh_plugin_trait::declare_plugin!(ROS2Plugin);

#[allow(clippy::upper_case_acronyms)]
pub struct ROS2Plugin;

impl ZenohPlugin for ROS2Plugin {}
impl Plugin for ROS2Plugin {
    type StartArgs = Runtime;
    type Instance = RunningPlugin;

    const PLUGIN_VERSION: &'static str = plugin_version!();
    const PLUGIN_LONG_VERSION: &'static str = plugin_long_version!();
    const DEFAULT_NAME: &'static str = "ros2dds";

    fn start(name: &str, runtime: &Self::StartArgs) -> ZResult<zenoh::plugins::RunningPlugin> {
        // Try to initiate login.
        // Required in case of dynamic lib, otherwise no logs.
        // But cannot be done twice in case of static link.
        zenoh_util::try_init_log_from_env();

        let runtime_conf = runtime.config().lock();
        let plugin_conf = runtime_conf
            .plugin(name)
            .ok_or_else(|| zerror!("Plugin `{}`: missing config", name))?;
        let config: Config = serde_json::from_value(plugin_conf.clone())
            .map_err(|e| zerror!("Plugin `{}` configuration error: {}", name, e))?;
        async_std::task::spawn(run(runtime.clone(), config));
        Ok(Box::new(ROS2Plugin))
    }
}
impl PluginControl for ROS2Plugin {}
impl RunningPluginTrait for ROS2Plugin {}

pub async fn run(runtime: Runtime, config: Config) {
    // Try to initiate login.
    // Required in case of dynamic lib, otherwise no logs.
    // But cannot be done twice in case of static link.
    zenoh_util::try_init_log_from_env();
    tracing::debug!("ROS2 plugin {}", ROS2Plugin::PLUGIN_VERSION);
    tracing::info!("ROS2 plugin {config:?}");

    // Check config validity
    if !regex::Regex::new("/[A-Za-z0-9_/]*")
        .unwrap()
        .is_match(&config.namespace)
    {
        tracing::error!(
            r#"Configuration error: invalid namespace "{}" must contain only alphanumeric, '_' or '/' characters and start with '/'"#,
            config.namespace
        );
        return;
    }
    if !regex::Regex::new("[A-Za-z0-9_]+")
        .unwrap()
        .is_match(&config.nodename)
    {
        tracing::error!(
            r#"Configuration error: invalid nodename "{}" must contain only alphanumeric or '_' characters"#,
            config.nodename
        );
        return;
    }

    // open zenoh-net Session
    let zsession = match zenoh::init(runtime).res_async().await {
        Ok(session) => Arc::new(session),
        Err(e) => {
            tracing::error!("Unable to init zenoh session for DDS plugin : {:?}", e);
            return;
        }
    };

    let plugin_id = if let Some(ref id) = config.id {
        if id.contains('/') {
            tracing::error!("The 'id' configuration must not contain any '/' character");
            return;
        }
        id.clone()
    } else {
        zsession.zid().into_keyexpr().to_owned()
    };

    // Declare plugin's liveliness token
    let ke_liveliness =
        zenoh::keformat!(ke_liveliness_plugin::formatter(), plugin_id = &plugin_id).unwrap();
    let member = match zsession
        .liveliness()
        .declare_token(ke_liveliness)
        .res_async()
        .await
    {
        Ok(member) => member,
        Err(e) => {
            tracing::error!(
                "Unable to declare liveliness token for DDS plugin : {:?}",
                e
            );
            return;
        }
    };

    // if "ros_localhost_only" is set, configure CycloneDDS to use only localhost interface
    if config.ros_localhost_only {
        env::set_var(
            "CYCLONEDDS_URI",
            format!(
                "{}{}",
                CYCLONEDDS_CONFIG_LOCALHOST_ONLY,
                env::var("CYCLONEDDS_URI").unwrap_or_default()
            ),
        );
    }

    // if "enable_shm" is set, configure CycloneDDS to use Iceoryx shared memory
    #[cfg(feature = "dds_shm")]
    {
        if config.shm_enabled {
            env::set_var(
                "CYCLONEDDS_URI",
                format!(
                    "{}{}",
                    CYCLONEDDS_CONFIG_ENABLE_SHM,
                    env::var("CYCLONEDDS_URI").unwrap_or_default()
                ),
            );
        }
    }

    // create DDS Participant
    tracing::debug!(
        "Create DDS Participant on domain {} with CYCLONEDDS_URI='{}'",
        config.domain,
        env::var("CYCLONEDDS_URI").unwrap_or_default()
    );
    let participant =
        unsafe { dds_create_participant(config.domain, std::ptr::null(), std::ptr::null()) };
    tracing::debug!(
        "ROS2 plugin {} using DDS Participant {} created",
        plugin_id,
        get_guid(&participant).unwrap()
    );

    let mut ros2_plugin = ROS2PluginRuntime {
        config: Arc::new(config),
        zsession,
        participant,
        _member: member,
        plugin_id,
        admin_space: HashMap::<OwnedKeyExpr, AdminRef>::new(),
    };

    ros2_plugin.run().await;
}

pub struct ROS2PluginRuntime<'a> {
    config: Arc<Config>,
    // Note: &'a Arc<Session> here to keep the ownership of Session outside this struct
    // and be able to store the publishers/subscribers it creates in this same struct.
    zsession: Arc<Session>,
    participant: dds_entity_t,
    _member: LivelinessToken<'a>,
    plugin_id: OwnedKeyExpr,
    // admin space: index is the admin_keyexpr
    // value is the JSon string to return to queries.
    admin_space: HashMap<OwnedKeyExpr, AdminRef>,
}

// An reference used in admin space to point to a struct (DdsEntity or Route) stored in another map
#[derive(Debug)]
enum AdminRef {
    Config,
    Version,
}

impl<'a> ROS2PluginRuntime<'a> {
    async fn run(&mut self) {
        // Subscribe to all liveliness info from other ROS2 plugins
        let ke_liveliness_all = zenoh::keformat!(
            ke_liveliness_all::formatter(),
            plugin_id = "*",
            remaining = "**"
        )
        .unwrap();
        let liveliness_subscriber = self
            .zsession
            .liveliness()
            .declare_subscriber(ke_liveliness_all)
            .querying()
            .with(flume::unbounded())
            .res_async()
            .await
            .expect("Failed to create Liveliness Subscriber");

        // declare admin space queryable
        let admin_prefix =
            zenoh::keformat!(ke_admin_prefix::formatter(), plugin_id = &self.plugin_id).unwrap();
        let admin_keyexpr_expr = (&admin_prefix) / *KE_ANY_N_SEGMENT;
        tracing::debug!("Declare admin space on {}", admin_keyexpr_expr);
        let admin_queryable = self
            .zsession
            .declare_queryable(admin_keyexpr_expr)
            .res_async()
            .await
            .expect("Failed to create AdminSpace queryable");

        // add plugin's config and version in admin space
        self.admin_space
            .insert(&admin_prefix / ke_for_sure!("config"), AdminRef::Config);
        self.admin_space
            .insert(&admin_prefix / ke_for_sure!("version"), AdminRef::Version);

        // Create and start the RosDiscoveryInfoMgr (managing ros_discovery_info topic)
        let ros_discovery_mgr = Arc::new(
            RosDiscoveryInfoMgr::new(
                self.participant,
                &self.config.namespace,
                &self.config.nodename,
            )
            .expect("Failed to create RosDiscoveryInfoMgr"),
        );
        ros_discovery_mgr.run().await;

        // Create and start DiscoveryManager
        let (tx, discovery_rcv): (Sender<ROS2DiscoveryEvent>, Receiver<ROS2DiscoveryEvent>) =
            unbounded();
        let mut discovery_mgr = DiscoveryMgr::create(self.participant, ros_discovery_mgr.clone());
        discovery_mgr.run(tx).await;

        // Create RoutesManager
        let mut routes_mgr = RoutesMgr::new(
            self.plugin_id.clone(),
            self.config.clone(),
            self.zsession.clone(),
            self.participant,
            discovery_mgr.discovered_entities.clone(),
            ros_discovery_mgr,
            admin_prefix.clone(),
        );

        loop {
            select!(
                evt = discovery_rcv.recv_async() => {
                    match evt {
                        Ok(evt) => {
                            if self.is_allowed(&evt) {
                                tracing::info!("{evt} - Allowed");
                                // pass ROS2DiscoveryEvent to RoutesMgr
                                if let Err(e) = routes_mgr.on_ros_discovery_event(evt).await {
                                    tracing::warn!("Error updating route: {e}");
                                }
                            } else {
                                tracing::debug!("{evt} - Denied per config");
                            }
                        }
                        Err(e) => tracing::error!("Internal Error: received from DiscoveryMgr: {e}")
                    }
                },

                liveliness_event = liveliness_subscriber.recv_async() => {
                    match liveliness_event
                    {
                        Ok(evt) => {
                            let ke = evt.key_expr.as_keyexpr();
                            if let Ok(parsed) = ke_liveliness_all::parse(ke) {
                                let plugin_id = parsed.plugin_id();
                                if plugin_id == self.plugin_id.as_ref() {
                                    // ignore own announcements
                                    continue;
                                }
                                match (parsed.remaining(), evt.kind)  {
                                    // New remote bridge detected
                                    (None, SampleKind::Put) => {
                                        tracing::info!("New ROS 2 bridge detected: {}", plugin_id);
                                        // make each routes for a TRANSIENT_LOCAL Subscriber to query historical publications from this new plugin
                                        routes_mgr.query_all_historical_publications(plugin_id).await;
                                    }
                                    // New remote bridge left
                                    (None, SampleKind::Delete) => tracing::info!("Remote ROS 2 bridge left: {}", plugin_id),
                                    // the liveliness token corresponds to a ROS2 announcement
                                    (Some(remaining), _) => {
                                        // parse it and pass ROS2AnnouncementEvent to RoutesMgr
                                        match self.parse_announcement_event(ke, &remaining.as_str()[..3], evt.kind) {
                                            Ok(evt) => {
                                                tracing::info!("Remote bridge {plugin_id} {evt}");
                                                routes_mgr.on_ros_announcement_event(evt).await
                                                    .unwrap_or_else(|e| tracing::warn!("Error treating announcement event: {e}"));
                                            },
                                            Err(e) =>
                                                tracing::warn!("Received unexpected liveliness key expression '{ke}': {e}")
                                        }
                                    }
                                }
                            } else {
                                tracing::warn!("Received unexpected liveliness key expression '{ke}'");
                            }
                        },
                        Err(e) => tracing::warn!("Error receiving liveliness event: {e}")
                    }
                },

                get_request = admin_queryable.recv_async() => {
                    if let Ok(query) = get_request {
                        self.treat_admin_query(&query).await;
                        // pass query to discovery_mgr
                        discovery_mgr.treat_admin_query(&query, &admin_prefix);
                        // pass query to discovery_mgr
                        routes_mgr.treat_admin_query(&query).await;
                    } else {
                        tracing::warn!("AdminSpace queryable was closed!");
                    }
                }
            )
        }
    }

    fn parse_announcement_event(
        &self,
        liveliness_ke: &keyexpr,
        iface_kind: &str,
        sample_kind: SampleKind,
    ) -> Result<ROS2AnnouncementEvent, String> {
        use ROS2AnnouncementEvent::*;
        tracing::debug!("Received liveliness event: {sample_kind} on {liveliness_ke}");
        match (iface_kind, sample_kind) {
            ("MP/", SampleKind::Put) => parse_ke_liveliness_pub(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(
                    |(plugin_id, zenoh_key_expr, ros2_type, keyless, writer_qos)| AnnouncedMsgPub {
                        plugin_id,
                        zenoh_key_expr,
                        ros2_type,
                        keyless,
                        writer_qos,
                    },
                ),
            ("MP/", SampleKind::Delete) => parse_ke_liveliness_pub(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(plugin_id, zenoh_key_expr, ..)| RetiredMsgPub {
                    plugin_id,
                    zenoh_key_expr,
                }),
            ("MS/", SampleKind::Put) => parse_ke_liveliness_sub(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(
                    |(plugin_id, zenoh_key_expr, ros2_type, keyless, reader_qos)| AnnouncedMsgSub {
                        plugin_id,
                        zenoh_key_expr,
                        ros2_type,
                        keyless,
                        reader_qos,
                    },
                ),
            ("MS/", SampleKind::Delete) => parse_ke_liveliness_sub(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(plugin_id, zenoh_key_expr, ..)| RetiredMsgSub {
                    plugin_id,
                    zenoh_key_expr,
                }),
            ("SS/", SampleKind::Put) => parse_ke_liveliness_service_srv(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(
                    |(plugin_id, zenoh_key_expr, ros2_type)| AnnouncedServiceSrv {
                        plugin_id,
                        zenoh_key_expr,
                        ros2_type,
                    },
                ),
            ("SS/", SampleKind::Delete) => parse_ke_liveliness_service_srv(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(plugin_id, zenoh_key_expr, ..)| RetiredServiceSrv {
                    plugin_id,
                    zenoh_key_expr,
                }),
            ("SC/", SampleKind::Put) => parse_ke_liveliness_service_cli(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(
                    |(plugin_id, zenoh_key_expr, ros2_type)| AnnouncedServiceCli {
                        plugin_id,
                        zenoh_key_expr,
                        ros2_type,
                    },
                ),
            ("SC/", SampleKind::Delete) => parse_ke_liveliness_service_cli(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(plugin_id, zenoh_key_expr, ..)| RetiredServiceCli {
                    plugin_id,
                    zenoh_key_expr,
                }),
            ("AS/", SampleKind::Put) => parse_ke_liveliness_action_srv(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(
                    |(plugin_id, zenoh_key_expr, ros2_type)| AnnouncedActionSrv {
                        plugin_id,
                        zenoh_key_expr,
                        ros2_type,
                    },
                ),
            ("AS/", SampleKind::Delete) => parse_ke_liveliness_action_srv(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(plugin_id, zenoh_key_expr, ..)| RetiredActionSrv {
                    plugin_id,
                    zenoh_key_expr,
                }),
            ("AC/", SampleKind::Put) => parse_ke_liveliness_action_cli(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(
                    |(plugin_id, zenoh_key_expr, ros2_type)| AnnouncedActionCli {
                        plugin_id,
                        zenoh_key_expr,
                        ros2_type,
                    },
                ),
            ("AC/", SampleKind::Delete) => parse_ke_liveliness_action_cli(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(plugin_id, zenoh_key_expr, ..)| RetiredActionCli {
                    plugin_id,
                    zenoh_key_expr,
                }),
            _ => Err(format!("invalid ROS2 interface kind: {iface_kind}")),
        }
    }

    fn is_allowed(&self, evt: &ROS2DiscoveryEvent) -> bool {
        if let Some(allowance) = &self.config.allowance {
            use ROS2DiscoveryEvent::*;
            match evt {
                DiscoveredMsgPub(_, iface) | UndiscoveredMsgPub(_, iface) => {
                    allowance.is_publisher_allowed(&iface.name)
                }
                DiscoveredMsgSub(_, iface) | UndiscoveredMsgSub(_, iface) => {
                    allowance.is_subscriber_allowed(&iface.name)
                }
                DiscoveredServiceSrv(_, iface) | UndiscoveredServiceSrv(_, iface) => {
                    allowance.is_service_srv_allowed(&iface.name)
                }
                DiscoveredServiceCli(_, iface) | UndiscoveredServiceCli(_, iface) => {
                    allowance.is_service_cli_allowed(&iface.name)
                }
                DiscoveredActionSrv(_, iface) | UndiscoveredActionSrv(_, iface) => {
                    allowance.is_action_srv_allowed(&iface.name)
                }
                DiscoveredActionCli(_, iface) | UndiscoveredActionCli(_, iface) => {
                    allowance.is_action_cli_allowed(&iface.name)
                }
            }
        } else {
            // no allow/deny configured => allow all
            true
        }
    }

    async fn treat_admin_query(&self, query: &Query) {
        let query_ke = query.selector().key_expr;
        if query_ke.is_wild() {
            // iterate over all admin space to find matching keys and reply for each
            for (ke, admin_ref) in self.admin_space.iter() {
                if query_ke.intersects(ke) {
                    self.send_admin_reply(query, ke, admin_ref).await;
                }
            }
        } else {
            // sub_ke correspond to 1 key - just get it and reply
            let own_ke: OwnedKeyExpr = query_ke.into();
            if let Some(admin_ref) = self.admin_space.get(&own_ke) {
                self.send_admin_reply(query, &own_ke, admin_ref).await;
            }
        }
    }

    async fn send_admin_reply(&self, query: &Query, key_expr: &keyexpr, admin_ref: &AdminRef) {
        let value: Value = match admin_ref {
            AdminRef::Version => VERSION_JSON_VALUE.clone(),
            AdminRef::Config => match serde_json::to_value(&*self.config) {
                Ok(v) => v.into(),
                Err(e) => {
                    tracing::error!("INTERNAL ERROR serializing config as JSON: {}", e);
                    return;
                }
            },
        };
        if let Err(e) = query
            .reply(Ok(Sample::new(key_expr.to_owned(), value)))
            .res_async()
            .await
        {
            tracing::warn!("Error replying to admin query {:?}: {}", query, e);
        }
    }
}

//TODO replace when stable https://github.com/rust-lang/rust/issues/65816
#[inline]
pub fn vec_into_raw_parts<T>(v: Vec<T>) -> (*mut T, usize, usize) {
    let mut me = ManuallyDrop::new(v);
    (me.as_mut_ptr(), me.len(), me.capacity())
}

struct ChannelEvent {
    tx: Sender<()>,
}

#[async_trait]
impl Timed for ChannelEvent {
    async fn run(&mut self) {
        if self.tx.send(()).is_err() {
            tracing::warn!("Error sending periodic timer notification on channel");
        };
    }
}

pub(crate) fn serialize_option_as_bool<S, T>(opt: &Option<T>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_bool(opt.is_some())
}
