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
#![allow(deprecated)]

use std::{
    collections::HashMap,
    env,
    future::Future,
    mem::ManuallyDrop,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use cyclors::*;
use events::ROS2AnnouncementEvent;
use flume::{unbounded, Receiver, Sender};
use futures::select;
use serde::Serializer;
use tokio::task::JoinHandle;
use zenoh::{
    bytes::{Encoding, ZBytes},
    internal::{
        plugins::{RunningPlugin, RunningPluginTrait, ZenohPlugin},
        runtime::DynamicRuntime,
        zerror, Timed,
    },
    key_expr::{
        format::{kedefine, keformat},
        keyexpr, OwnedKeyExpr,
    },
    liveliness::LivelinessToken,
    query::Query,
    sample::SampleKind,
    Result as ZResult, Session,
};
use zenoh_ext::SubscriberBuilderExt;
use zenoh_plugin_trait::{plugin_long_version, plugin_version, Plugin, PluginControl};

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
use config::{Config, RosAutomaticDiscoveryRange};

use crate::{
    dds_utils::get_guid,
    discovery_mgr::DiscoveryMgr,
    events::ROS2DiscoveryEvent,
    liveliness_mgt::*,
    ros2_utils::{key_expr_to_ros2_name, ros_distro_is_less_than},
    ros_discovery::RosDiscoveryInfoMgr,
    routes_mgr::RoutesMgr,
};

lazy_static::lazy_static! {
    static ref WORK_THREAD_NUM: AtomicUsize = AtomicUsize::new(config::DEFAULT_WORK_THREAD_NUM);
    static ref MAX_BLOCK_THREAD_NUM: AtomicUsize = AtomicUsize::new(config::DEFAULT_MAX_BLOCK_THREAD_NUM);
    // The global runtime is used in the dynamic plugins, which we can't get the current runtime
    static ref TOKIO_RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_multi_thread()
               .worker_threads(WORK_THREAD_NUM.load(Ordering::SeqCst))
               .max_blocking_threads(MAX_BLOCK_THREAD_NUM.load(Ordering::SeqCst))
               .enable_all()
               .build()
               .expect("Unable to create runtime");
}
#[inline(always)]
pub(crate) fn spawn_runtime<F>(task: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // Check whether able to get the current runtime
    match tokio::runtime::Handle::try_current() {
        Ok(rt) => {
            // Able to get the current runtime (standalone binary), use the current runtime
            rt.spawn(task)
        }
        Err(_) => {
            // Unable to get the current runtime (dynamic plugins), reuse the global runtime
            TOKIO_RUNTIME.spawn(task)
        }
    }
}

lazy_static::lazy_static!(


    static ref LOG_PAYLOAD: bool = std::env::var("Z_LOG_PAYLOAD").is_ok();

    static ref KE_ANY_1_SEGMENT: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("*") };
    static ref KE_ANY_N_SEGMENT: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("**") };

    static ref KE_PREFIX_PUB_CACHE: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("@ros2_pub_cache") };
);

kedefine!(
    // Admin space key expressions of plugin's version
    pub ke_admin_version: "${plugin_status_key:**}/__version__",

    // Admin prefix of this bridge
    pub ke_admin_prefix: "@/${zenoh_id:*}/ros2/",
);

// CycloneDDS' localhost-only: set network interface address (shortened form of config would be
// possible, too, but I think it is clearer to spell it out completely).
// Empty configuration fragments are ignored, so it is safe to unconditionally append a comma.
const CYCLONEDDS_CONFIG_LOCALHOST_ONLY: &str = r#"<CycloneDDS><Domain><General>
                                                      <Interfaces><NetworkInterface address="127.0.0.1"/></Interfaces>
                                                  </General></Domain></CycloneDDS>,"#;

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
    type StartArgs = DynamicRuntime;
    type Instance = RunningPlugin;

    const PLUGIN_VERSION: &'static str = plugin_version!();
    const PLUGIN_LONG_VERSION: &'static str = plugin_long_version!();
    const DEFAULT_NAME: &'static str = "ros2dds";

    fn start(name: &str, runtime: &Self::StartArgs) -> ZResult<RunningPlugin> {
        // Try to initiate login.
        // Required in case of dynamic lib, otherwise no logs.
        // But cannot be done twice in case of static link.
        zenoh::try_init_log_from_env();

        let runtime_conf = runtime.get_config();
        let plugin_conf = runtime_conf
            .get_plugin_config(name)
            .map_err(|_| zerror!("Plugin `{}`: missing config", name))?;
        let config: Config = serde_json::from_value(plugin_conf.clone())
            .map_err(|e| zerror!("Plugin `{}` configuration error: {}", name, e))?;
        WORK_THREAD_NUM.store(config.work_thread_num, Ordering::SeqCst);
        MAX_BLOCK_THREAD_NUM.store(config.max_block_thread_num, Ordering::SeqCst);

        spawn_runtime(run(runtime.clone(), config));

        Ok(Box::new(ROS2Plugin))
    }
}
impl PluginControl for ROS2Plugin {}
impl RunningPluginTrait for ROS2Plugin {}

fn create_cyclonedds_config(
    ros_automatic_discovery_range: RosAutomaticDiscoveryRange,
    ros_static_peers: Vec<String>,
) -> String {
    let mut config = String::new();
    // Refer to https://github.com/ros2/rmw_cyclonedds/blob/c9e7001e6bf5373bdf1931535354b52eeddb2053/rmw_cyclonedds_cpp/src/rmw_node.cpp#L1134
    let add_localhost_as_static_peer: bool;
    let add_static_peers: bool;
    let disable_multicast: bool;
    match ros_automatic_discovery_range {
        RosAutomaticDiscoveryRange::Subnet => {
            add_localhost_as_static_peer = false;
            add_static_peers = true;
            disable_multicast = false;
        }
        RosAutomaticDiscoveryRange::SystemDefault => {
            add_localhost_as_static_peer = false;
            add_static_peers = false;
            disable_multicast = false;
        }
        RosAutomaticDiscoveryRange::Localhost => {
            add_localhost_as_static_peer = true;
            add_static_peers = true;
            disable_multicast = true;
        }
        RosAutomaticDiscoveryRange::Off => {
            add_localhost_as_static_peer = false;
            add_static_peers = false;
            disable_multicast = true;
        }
    };
    if add_localhost_as_static_peer || add_static_peers || disable_multicast {
        config += "<CycloneDDS><Domain>";

        if disable_multicast {
            config += "<General><AllowMulticast>false</AllowMulticast></General>";
        }

        let discovery_off = disable_multicast && !add_localhost_as_static_peer && !add_static_peers;
        if discovery_off {
            config += "<Discovery><ParticipantIndex>none</ParticipantIndex>";
            config += &format!("<Tag>ros_discovery_off_{}</Tag>", std::process::id());
        } else {
            config += "<Discovery><ParticipantIndex>auto</ParticipantIndex>";
            config += "<MaxAutoParticipantIndex>32</MaxAutoParticipantIndex>";
        }

        if (add_static_peers && !ros_static_peers.is_empty()) || add_localhost_as_static_peer {
            config += "<Peers>";
            if add_localhost_as_static_peer {
                config += "<Peer address=\"localhost\"/>";
            }
            for peer in ros_static_peers {
                config += "<Peer address=\"";
                config += &peer;
                config += "\"/>";
            }
            config += "</Peers>";
        }

        config += "</Discovery></Domain></CycloneDDS>,";
    }
    config
}

pub async fn run(runtime: DynamicRuntime, config: Config) {
    // Try to initiate login.
    // Required in case of dynamic lib, otherwise no logs.
    // But cannot be done twice in case of static link.
    zenoh::try_init_log_from_env();
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
    let zsession = match zenoh::session::init(runtime).await {
        Ok(session) => Arc::new(session),
        Err(e) => {
            tracing::error!("Unable to init zenoh session for DDS plugin : {:?}", e);
            return;
        }
    };

    // Declare plugin's liveliness token
    let ke_liveliness = keformat!(
        ke_liveliness_plugin::formatter(),
        zenoh_id = zsession.zid().into_keyexpr()
    )
    .unwrap();
    let member = match zsession.liveliness().declare_token(ke_liveliness).await {
        Ok(member) => member,
        Err(e) => {
            tracing::error!(
                "Unable to declare liveliness token for DDS plugin : {:?}",
                e
            );
            return;
        }
    };

    // Dynamic Discovery is changed after iron. Need to check the ROS 2 version.
    // https://docs.ros.org/en/rolling/Tutorials/Advanced/Improved-Dynamic-Discovery.html
    if ros_distro_is_less_than("iron") {
        if config.ros_automatic_discovery_range.is_some() {
            tracing::warn!("ROS_AUTOMATIC_DISCOVERY_RANGE will be ignored since it's not supported before ROS 2 Iron");
        }
        if config.ros_static_peers.is_some() {
            tracing::warn!(
                "ROS_STATIC_PEERS will be ignored since it's not supported before ROS 2 Iron"
            );
        }
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
    } else {
        let (ros_automatic_discovery_range, ros_static_peers) = if config.ros_localhost_only {
            // If ROS_LOCALHOST_ONLY is set, need to transform into new environmental variables
            // Refer to https://github.com/ros2/ros2_documentation/pull/3519#discussion_r1186541935
            tracing::warn!("ROS_LOCALHOST_ONLY is deprecated but still honored if it is enabled. Use ROS_AUTOMATIC_DISCOVERY_RANGE and ROS_STATIC_PEERS instead.");
            tracing::warn!("'localhost_only' is enabled, 'automatic_discovery_range' and 'static_peers' will be ignored.");
            (Some(RosAutomaticDiscoveryRange::Localhost), None)
        } else {
            (
                config.ros_automatic_discovery_range,
                config.ros_static_peers.clone(),
            )
        };
        env::set_var(
            "CYCLONEDDS_URI",
            format!(
                "{}{}",
                create_cyclonedds_config(
                    ros_automatic_discovery_range.unwrap_or(RosAutomaticDiscoveryRange::Subnet),
                    ros_static_peers.unwrap_or(Vec::new())
                ),
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
        zsession.zid(),
        get_guid(&participant).unwrap()
    );

    let mut ros2_plugin = ROS2PluginRuntime {
        config: Arc::new(config),
        zsession,
        participant,
        _member: member,
        admin_space: HashMap::<OwnedKeyExpr, AdminRef>::new(),
    };

    ros2_plugin.run().await;
}

pub struct ROS2PluginRuntime {
    config: Arc<Config>,
    // Note: &'a Arc<Session> here to keep the ownership of Session outside this struct
    // and be able to store the publishers/subscribers it creates in this same struct.
    zsession: Arc<Session>,
    participant: dds_entity_t,
    _member: LivelinessToken,
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

impl ROS2PluginRuntime {
    async fn run(&mut self) {
        // Subscribe to all liveliness info from other ROS2 plugins
        let ke_liveliness_all = keformat!(
            ke_liveliness_all::formatter(),
            zenoh_id = "*",
            remaining = "**"
        )
        .unwrap();
        let liveliness_subscriber = self
            .zsession
            .liveliness()
            .declare_subscriber(ke_liveliness_all)
            .querying()
            .with(flume::unbounded())
            .await
            .expect("Failed to create Liveliness Subscriber");

        // declare admin space queryable
        let admin_prefix = keformat!(
            ke_admin_prefix::formatter(),
            zenoh_id = &self.zsession.zid().into_keyexpr()
        )
        .unwrap();
        let admin_keyexpr_expr = (&admin_prefix) / *KE_ANY_N_SEGMENT;
        tracing::debug!("Declare admin space on {}", admin_keyexpr_expr);
        let admin_queryable = self
            .zsession
            .declare_queryable(admin_keyexpr_expr)
            .await
            .expect("Failed to create AdminSpace queryable");

        // add plugin's config and version in admin space
        self.admin_space.insert(
            &admin_prefix / unsafe { keyexpr::from_str_unchecked("config") },
            AdminRef::Config,
        );
        self.admin_space.insert(
            &admin_prefix / unsafe { keyexpr::from_str_unchecked("version") },
            AdminRef::Version,
        );

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
                            let ke = evt.key_expr().as_keyexpr();
                            if let Ok(parsed) = ke_liveliness_all::parse(ke) {
                                let zenoh_id = parsed.zenoh_id();
                                if zenoh_id == &*self.zsession.zid().into_keyexpr() {
                                    // ignore own announcements
                                    continue;
                                }
                                match (parsed.remaining(), evt.kind())  {
                                    // New remote bridge detected
                                    (None, SampleKind::Put) => {
                                        tracing::info!("New ROS 2 bridge detected: {}", zenoh_id);
                                    }
                                    // New remote bridge left
                                    (None, SampleKind::Delete) => tracing::info!("Remote ROS 2 bridge left: {}", zenoh_id),
                                    // the liveliness token corresponds to a ROS2 announcement
                                    (Some(remaining), _) => {
                                        // parse it and pass ROS2AnnouncementEvent to RoutesMgr
                                        match self.parse_announcement_event(ke, &remaining.as_str()[..3], evt.kind()) {
                                            Ok(evt) => {
                                                if self.is_announcement_allowed(&evt) {
                                                    tracing::info!("Remote bridge {zenoh_id} {evt} - Allowed");
                                                    routes_mgr.on_ros_announcement_event(evt).await
                                                        .unwrap_or_else(|e| tracing::warn!("Error treating announcement event: {e}"));
                                                } else {
                                                    tracing::debug!("Remote bridge {zenoh_id} {evt} - Matching entity denied per config");
                                                }
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
                    |(zenoh_id, zenoh_key_expr, ros2_type, keyless, writer_qos)| AnnouncedMsgPub {
                        zenoh_id,
                        zenoh_key_expr,
                        ros2_type,
                        keyless,
                        writer_qos,
                    },
                ),
            ("MP/", SampleKind::Delete) => parse_ke_liveliness_pub(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(zenoh_id, zenoh_key_expr, ..)| RetiredMsgPub {
                    zenoh_id,
                    zenoh_key_expr,
                }),
            ("MS/", SampleKind::Put) => parse_ke_liveliness_sub(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(
                    |(zenoh_id, zenoh_key_expr, ros2_type, keyless, reader_qos)| AnnouncedMsgSub {
                        zenoh_id,
                        zenoh_key_expr,
                        ros2_type,
                        keyless,
                        reader_qos,
                    },
                ),
            ("MS/", SampleKind::Delete) => parse_ke_liveliness_sub(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(zenoh_id, zenoh_key_expr, ..)| RetiredMsgSub {
                    zenoh_id,
                    zenoh_key_expr,
                }),
            ("SS/", SampleKind::Put) => parse_ke_liveliness_service_srv(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(
                    |(zenoh_id, zenoh_key_expr, ros2_type)| AnnouncedServiceSrv {
                        zenoh_id,
                        zenoh_key_expr,
                        ros2_type,
                    },
                ),
            ("SS/", SampleKind::Delete) => parse_ke_liveliness_service_srv(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(zenoh_id, zenoh_key_expr, ..)| RetiredServiceSrv {
                    zenoh_id,
                    zenoh_key_expr,
                }),
            ("SC/", SampleKind::Put) => parse_ke_liveliness_service_cli(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(
                    |(zenoh_id, zenoh_key_expr, ros2_type)| AnnouncedServiceCli {
                        zenoh_id,
                        zenoh_key_expr,
                        ros2_type,
                    },
                ),
            ("SC/", SampleKind::Delete) => parse_ke_liveliness_service_cli(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(zenoh_id, zenoh_key_expr, ..)| RetiredServiceCli {
                    zenoh_id,
                    zenoh_key_expr,
                }),
            ("AS/", SampleKind::Put) => parse_ke_liveliness_action_srv(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(zenoh_id, zenoh_key_expr, ros2_type)| AnnouncedActionSrv {
                    zenoh_id,
                    zenoh_key_expr,
                    ros2_type,
                }),
            ("AS/", SampleKind::Delete) => parse_ke_liveliness_action_srv(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(zenoh_id, zenoh_key_expr, ..)| RetiredActionSrv {
                    zenoh_id,
                    zenoh_key_expr,
                }),
            ("AC/", SampleKind::Put) => parse_ke_liveliness_action_cli(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(zenoh_id, zenoh_key_expr, ros2_type)| AnnouncedActionCli {
                    zenoh_id,
                    zenoh_key_expr,
                    ros2_type,
                }),
            ("AC/", SampleKind::Delete) => parse_ke_liveliness_action_cli(liveliness_ke)
                .map_err(|e| format!("Received invalid liveliness token: {e}"))
                .map(|(zenoh_id, zenoh_key_expr, ..)| RetiredActionCli {
                    zenoh_id,
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

    // Check if a remote announcement by another bridge is allowed, depending on the matching entity allowance in config.
    // E.g. a remote announcement of a Publisher on /abc is allowed only if a Subscriber on /abc is allowed in the local config.
    fn is_announcement_allowed(&self, evt: &ROS2AnnouncementEvent) -> bool {
        if let Some(allowance) = &self.config.allowance {
            use ROS2AnnouncementEvent::*;
            match evt {
                AnnouncedMsgPub { zenoh_key_expr, .. } | RetiredMsgPub { zenoh_key_expr, .. } => {
                    allowance
                        .is_subscriber_allowed(&key_expr_to_ros2_name(zenoh_key_expr, &self.config))
                }
                AnnouncedMsgSub { zenoh_key_expr, .. } | RetiredMsgSub { zenoh_key_expr, .. } => {
                    allowance
                        .is_publisher_allowed(&key_expr_to_ros2_name(zenoh_key_expr, &self.config))
                }
                AnnouncedServiceSrv { zenoh_key_expr, .. }
                | RetiredServiceSrv { zenoh_key_expr, .. } => allowance
                    .is_service_cli_allowed(&key_expr_to_ros2_name(zenoh_key_expr, &self.config)),
                AnnouncedServiceCli { zenoh_key_expr, .. }
                | RetiredServiceCli { zenoh_key_expr, .. } => allowance
                    .is_service_srv_allowed(&key_expr_to_ros2_name(zenoh_key_expr, &self.config)),
                AnnouncedActionSrv { zenoh_key_expr, .. }
                | RetiredActionSrv { zenoh_key_expr, .. } => allowance
                    .is_action_cli_allowed(&key_expr_to_ros2_name(zenoh_key_expr, &self.config)),
                AnnouncedActionCli { zenoh_key_expr, .. }
                | RetiredActionCli { zenoh_key_expr, .. } => allowance
                    .is_action_srv_allowed(&key_expr_to_ros2_name(zenoh_key_expr, &self.config)),
            }
        } else {
            // no allow/deny configured => allow all
            true
        }
    }

    async fn treat_admin_query(&self, query: &Query) {
        let query_ke = query.key_expr();
        if query_ke.is_wild() {
            // iterate over all admin space to find matching keys and reply for each
            for (ke, admin_ref) in self.admin_space.iter() {
                if query_ke.intersects(ke) {
                    self.send_admin_reply(query, ke, admin_ref).await;
                }
            }
        } else {
            // sub_ke correspond to 1 key - just get it and reply
            let own_ke: OwnedKeyExpr = query_ke.to_owned().into();
            if let Some(admin_ref) = self.admin_space.get(&own_ke) {
                self.send_admin_reply(query, &own_ke, admin_ref).await;
            }
        }
    }

    async fn send_admin_reply(&self, query: &Query, key_expr: &keyexpr, admin_ref: &AdminRef) {
        let z_bytes: ZBytes = match admin_ref {
            AdminRef::Version => match serde_json::to_value(ROS2Plugin::PLUGIN_LONG_VERSION) {
                Ok(v) => match serde_json::to_vec(&v) {
                    Ok(bytes) => ZBytes::from(bytes),
                    Err(e) => {
                        tracing::warn!("Error transforming JSON to ZBytes: {}", e);
                        return;
                    }
                },
                Err(e) => {
                    tracing::error!("INTERNAL ERROR serializing config as JSON: {}", e);
                    return;
                }
            },
            AdminRef::Config => match serde_json::to_value(&*self.config) {
                Ok(v) => match serde_json::to_vec(&v) {
                    Ok(bytes) => ZBytes::from(bytes),
                    Err(e) => {
                        tracing::warn!("Error transforming JSON to ZBytes: {}", e);
                        return;
                    }
                },
                Err(e) => {
                    tracing::error!("INTERNAL ERROR serializing config as JSON: {}", e);
                    return;
                }
            },
        };
        if let Err(e) = query
            .reply(key_expr.to_owned(), z_bytes)
            .encoding(Encoding::APPLICATION_JSON)
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
