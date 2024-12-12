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

use std::{sync::mpsc::channel, time::Duration};

use futures::StreamExt;
use r2r::{self, QosProfile};
use zenoh::{
    config::Config,
    internal::{plugins::PluginsManager, runtime::RuntimeBuilder},
};
use zenoh_config::ModeDependentValue;

// The test topic
const TEST_TOPIC: &str = "test_topic";
// The test TEST_PAYLOAD
const TEST_PAYLOAD: &str = "Hello World";

fn init_env() {
    std::env::set_var("RMW_IMPLEMENTATION", "rmw_cyclonedds_cpp");
}

async fn create_bridge() {
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

#[tokio::test(flavor = "multi_thread")]
async fn test_zenoh_pub_ros_sub() {
    init_env();
    let (tx, rx) = channel();

    // Create zenoh-bridge-ros2dds
    tokio::spawn(create_bridge());

    // ROS subscriber
    let ctx = r2r::Context::create().unwrap();
    let mut node = r2r::Node::create(ctx, "ros_sub", "").unwrap();
    let subscriber = node
        .subscribe::<r2r::std_msgs::msg::String>(&format!("/{}", TEST_TOPIC), QosProfile::default())
        .unwrap();

    // Zenoh publisher
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let publisher = session.declare_publisher(TEST_TOPIC).await.unwrap();

    // Wait for the environment to be ready
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Publish Zenoh message
    let buf = cdr::serialize::<_, _, cdr::CdrLe>(TEST_PAYLOAD, cdr::size::Infinite).unwrap();
    publisher.put(buf).await.unwrap();

    // Check ROS subscriber will receive the data
    tokio::spawn(async move {
        subscriber
            .for_each(|msg| {
                tx.send(msg.data).unwrap();
                futures::future::ready(())
            })
            .await
    });
    node.spin_once(std::time::Duration::from_millis(100));
    let data = rx
        .recv_timeout(Duration::from_secs(3))
        .expect("Receiver timeout");
    assert_eq!(data, TEST_PAYLOAD);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ros_pub_zenoh_sub() {
    init_env();
    // Create zenoh-bridge-ros2dds
    tokio::spawn(create_bridge());

    // Zenoh subscriber
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();
    let subscriber = session.declare_subscriber(TEST_TOPIC).await.unwrap();

    // ROS publisher
    let ctx = r2r::Context::create().unwrap();
    let mut node = r2r::Node::create(ctx, "ros_pub", "").unwrap();
    let publisher = node
        .create_publisher(&format!("/{}", TEST_TOPIC), QosProfile::default())
        .unwrap();
    let msg = r2r::std_msgs::msg::String {
        data: TEST_PAYLOAD.into(),
    };

    // Wait for the environment to be ready
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Publish ROS message
    publisher.publish(&msg).unwrap();

    // Check Zenoh subscriber will receive the data
    tokio::time::timeout(Duration::from_secs(3), async {
        let sample = subscriber.recv_async().await.unwrap();
        let result: Result<String, _> =
            cdr::deserialize_from(sample.payload().reader(), cdr::size::Infinite);
        let recv_data = result.expect("Fail to receive data");
        assert_eq!(recv_data, TEST_PAYLOAD);
    })
    .await
    .expect("Timeout: Zenoh subscriber didn't receive any ROS message.");
}
