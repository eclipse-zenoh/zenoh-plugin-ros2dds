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

pub mod common;

use std::time::Duration;

use r2r::{self, QosProfile};
use serde_derive::{Deserialize, Serialize};
use zenoh::Wait;

// The test service
const TEST_SERVICE_R2Z: &str = "test_service_r2z";

#[derive(Serialize, Deserialize, PartialEq, Clone)]
struct AddTwoIntsRequest {
    a: i64,
    b: i64,
}
#[derive(Serialize, Deserialize, PartialEq, Clone)]
struct AddTwoIntsReply {
    sum: i64,
}

#[test]
fn test_ros_client_zenoh_service() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let (sender, receiver) = std::sync::mpsc::channel();

    rt.block_on(async {
        common::init_env();
        // Create zenoh-bridge-ros2dds
        tokio::spawn(common::create_bridge());

        let a = 1;
        let b = 2;

        // Zenoh service
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let _queryable = session
            .declare_queryable(TEST_SERVICE_R2Z)
            .callback(|query| {
                let request: AddTwoIntsRequest =
                    cdr::deserialize(&query.payload().unwrap().to_bytes()).unwrap();
                let response = AddTwoIntsReply {
                    sum: request.a + request.b,
                };
                let data = cdr::serialize::<_, _, cdr::CdrLe>(&response, cdr::Infinite).unwrap();
                query.reply(TEST_SERVICE_R2Z, data).wait().unwrap();
            })
            .await
            .unwrap();

        // ROS client
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "ros_client", "").unwrap();
        let client = node
            .create_client::<r2r::example_interfaces::srv::AddTwoInts::Service>(
                &format!("/{}", TEST_SERVICE_R2Z),
                QosProfile::default(),
            )
            .unwrap();

        // Node spin
        let _handler = tokio::task::spawn_blocking(move || loop {
            node.spin_once(std::time::Duration::from_millis(100));
        });

        // Wait for the environment to be ready
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Send the request and then process the response
        let my_req = r2r::example_interfaces::srv::AddTwoInts::Request { a, b };
        let resp = client.request(&my_req).unwrap().await.unwrap();
        assert_eq!(resp.sum, a + b);

        // Tell the main test thread, we're completed
        sender.send(()).unwrap();
    });

    let test_result = receiver.recv_timeout(common::DEFAULT_TIMEOUT);
    // Stop the tokio runtime
    // Note that we should shutdown the runtime before doing any check that might panic the test.
    // Otherwise, the tasks inside the runtime will never be completed.
    rt.shutdown_background();
    match test_result {
        Ok(_) => {
            println!("Test passed");
        }
        Err(_) => {
            panic!("Test failed due to timeout.....");
        }
    }
}
