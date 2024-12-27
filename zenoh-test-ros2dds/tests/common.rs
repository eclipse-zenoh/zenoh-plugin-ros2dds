//
// Copyright (c) 2024 ZettaScale Technology
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
use std::time;

use zenoh::{
    config::Config,
    internal::{plugins::PluginsManager, runtime::RuntimeBuilder},
};
use zenoh_config::ModeDependentValue;

pub static DEFAULT_TIMEOUT: time::Duration = time::Duration::from_secs(60);

pub fn init_env() {
    std::env::set_var("RMW_IMPLEMENTATION", "rmw_cyclonedds_cpp");
}

pub async fn create_bridge() {
    let mut plugins_mgr = PluginsManager::static_plugins_only();
    plugins_mgr.declare_static_plugin::<zenoh_plugin_ros2dds::ROS2Plugin, &str>("ros2dds", true);
    let mut config = Config::default();
    config.insert_json5("plugins/ros2dds", "{}").unwrap();
    config
        .timestamping
        .set_enabled(Some(ModeDependentValue::Unique(true)))
        .unwrap();
    config.adminspace.set_enabled(true).unwrap();
    config.plugins_loading.set_enabled(true).unwrap();
    let mut runtime = RuntimeBuilder::new(config)
        .plugins_manager(plugins_mgr)
        .build()
        .await
        .unwrap();
    runtime.start().await.unwrap();
}
