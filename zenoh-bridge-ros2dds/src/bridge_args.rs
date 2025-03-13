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
use clap::builder::FalseyValueParser;
use zenoh::config::Config;
use zenoh_plugin_trait::Plugin;

use crate::zenoh_args::CommonArgs;

//
// All Bridge arguments
//
#[derive(clap::Parser, Clone, Debug)]
#[command(version=zenoh_plugin_ros2dds::ROS2Plugin::PLUGIN_VERSION,
    long_version=zenoh_plugin_ros2dds::ROS2Plugin::PLUGIN_LONG_VERSION,
    about="Zenoh bridge for ROS 2 with a DDS RMW",
)]
pub struct BridgeArgs {
    #[command(flatten)]
    pub session_args: CommonArgs,
    /// A ROS 2 namespace to be used by the "zenoh_bridge_dds" node'
    #[arg(short, long)]
    pub namespace: Option<String>,
    /// The DDS Domain ID. Default to $ROS_DOMAIN_ID environment variable if defined, or to 0 otherwise.
    #[arg(short, long, env("ROS_DOMAIN_ID"))]
    pub domain: Option<u32>,
    /// Configure CycloneDDS to use only the localhost interface. If not set, a $ROS_LOCALHOST_ONLY=1 environment variable activates this option.
    /// When this flag is not active, CycloneDDS will pick the interface defined in "$CYCLONEDDS_URI" configuration, or automatically choose one.
    #[arg(
        long,
        env("ROS_LOCALHOST_ONLY"),
        value_parser(FalseyValueParser::new()),
        verbatim_doc_comment
    )]
    pub ros_localhost_only: bool,
    /// Configure CycloneDDS to apply ROS_AUTOMATIC_DISCOVREY_RANGE. The argument only takes effect after ROS 2 Iron.
    #[arg(long, env("ROS_AUTOMATIC_DISCOVERY_RANGE"))]
    pub ros_automatic_discovery_range: Option<String>,
    /// Configure CycloneDDS to apply ROS_STATIC_PEERS. The argument only takes effect after ROS 2 Iron.
    #[arg(long, env("ROS_STATIC_PEERS"))]
    pub ros_static_peers: Option<String>,
    /// Configure CycloneDDS to use Iceoryx shared memory. If not set, CycloneDDS will instead use any shared memory settings defined in "$CYCLONEDDS_URI" configuration.
    #[cfg(feature = "dds_shm")]
    #[arg(long)]
    pub dds_enable_shm: bool,
    /// Specifies a maximum frequency of publications routing over zenoh for a set of Publishers.
    /// The string must have the format "<regex>=<float>":
    ///   - "regex" is a regular expression matching a Publisher interface name
    ///   - "float" is the maximum frequency in Hertz; if publication rate is higher, downsampling will occur when routing.
    ///
    /// Repeat this option to configure several topics expressions with a max frequency.
    #[arg(long, value_name = "REGEX=FLOAT", verbatim_doc_comment)]
    pub pub_max_frequency: Vec<String>,
    /// A float in seconds that will be used as a timeout when the bridge queries any other remote bridge
    /// for discovery information and for historical data for TRANSIENT_LOCAL DDS Readers it serves
    /// (i.e. if the query to the remote bridge exceed the timeout, some historical samples might be not routed to the Readers, but the route will not be blocked forever).
    /// This value overwrites the value possibly set in configuration file under 'plugins/ros2dds/queries_timeout/default' key [default: 5.0].
    #[arg(long, value_name = "FLOAT", verbatim_doc_comment)]
    pub queries_timeout_default: Option<f32>,

    /// Configures HTTP interface for the REST API (disabled by default, setting this option enables it). Accepted values:
    ///  - a port number
    ///  - a string with format `<local_ip>:<port_number>` (to bind the HTTP server to a specific interface).
    #[arg(short, long, value_name = "PORT | IP:PORT", verbatim_doc_comment)]
    pub rest_http_port: Option<String>,
    /// Experimental!! Run a watchdog thread that monitors the bridge's async executor and
    /// reports as error log any stalled status during the specified period [default: 1.0 second]
    #[arg(short, long, value_name = "FLOAT", default_missing_value = "1.0")]
    pub watchdog: Option<Option<f32>>,

    /// ROS command line arguments as specified in https://design.ros2.org/articles/ros_command_line_arguments.html
    /// Supported capabilities:
    ///   -r, --remap <from:=to> : remapping is supported only for '__ns' and '__node'
    #[arg(
        long,
        value_name = " list of ROS args until '--' ",
        verbatim_doc_comment
    )]
    pub ros_args: (),
}

impl From<BridgeArgs> for Config {
    fn from(value: BridgeArgs) -> Self {
        (&value).into()
    }
}

impl From<&BridgeArgs> for Config {
    fn from(args: &BridgeArgs) -> Self {
        let mut config = (&args.session_args).into();

        insert_json5_option(&mut config, "plugins/ros2dds/namespace", &args.namespace);
        if let Some(domain) = args.domain {
            insert_json5(&mut config, "plugins/ros2dds/domain", &domain);
        }
        insert_json5(
            &mut config,
            "plugins/ros2dds/ros_localhost_only",
            &args.ros_localhost_only,
        );
        insert_json5_option(
            &mut config,
            "plugins/ros2dds/ros_automatic_discovery_range",
            &args.ros_automatic_discovery_range,
        );
        insert_json5_option(
            &mut config,
            "plugins/ros2dds/ros_static_peers",
            &args.ros_static_peers,
        );
        #[cfg(feature = "dds_shm")]
        {
            insert_json5(
                &mut config,
                "plugins/ros2dds/shm_enabled",
                &args.dds_enable_shm,
            );
        }
        insert_json5_list(
            &mut config,
            "plugins/ros2dds/pub_max_frequencies",
            &args.pub_max_frequency,
        );
        insert_json5_option(
            &mut config,
            "plugins/ros2dds/queries_timeout/default",
            &args.queries_timeout_default,
        );

        insert_json5_option(&mut config, "plugins/rest/http_port", &args.rest_http_port);

        config
    }
}

pub(crate) fn insert_json5<T>(config: &mut Config, key: &str, value: &T)
where
    T: Sized + serde::Serialize,
{
    config
        .insert_json5(key, &serde_json::to_string(value).unwrap())
        .unwrap();
}

pub(crate) fn insert_json5_option<T>(config: &mut Config, key: &str, value: &Option<T>)
where
    T: Sized + serde::Serialize,
{
    if let Some(v) = value {
        config
            .insert_json5(key, &serde_json::to_string(v).unwrap())
            .unwrap();
    }
}

pub(crate) fn insert_json5_list<T>(config: &mut Config, key: &str, values: &Vec<T>)
where
    T: Sized + serde::Serialize,
{
    if !values.is_empty() {
        config
            .insert_json5(key, &serde_json::to_string(values).unwrap())
            .unwrap();
    }
}
