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

use common::DEFAULT_TIMEOUT;
use futures::StreamExt;
use r2r::{self};
use serde_derive::{Deserialize, Serialize};
use zenoh::Wait;

// The test action
const TEST_ACTION_R2Z: &str = "test_action_r2z";

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct FibonacciSendGoal {
    pub goal_id: [u8; 16],
    pub goal: i32,
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct ActionSendGoalResponse {
    pub accept: bool,
    pub sec: i32,
    pub nanosec: u32,
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct ActionResultRequest {
    pub goal_id: [u8; 16],
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct FibonacciResult {
    pub status: i8,
    pub sequence: Vec<i32>,
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct FibonacciFeedback {
    pub goal_id: [u8; 16],
    pub sequence: Vec<i32>,
}

#[test]
fn test_ros_client_zenoh_action() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let (sender, receiver) = std::sync::mpsc::channel();

    rt.spawn(async move {
        common::init_env();
        // Create zenoh-bridge-ros2dds
        tokio::spawn(common::create_bridge());

        // We send request 5 and expect result [0, 1, 1, 2, 3, 5]
        let action_request = 5;
        let action_result = vec![0, 1, 1, 2, 3, 5];
        let feedback = vec![0, 1, 1, 2, 3];

        // Zenoh action server
        // Note that we just create send_goal, get_result and feedback to implement the minimal action server
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let send_goal_expr = TEST_ACTION_R2Z.to_string() + "/_action/send_goal";
        let get_result_expr = TEST_ACTION_R2Z.to_string() + "/_action/get_result";
        let feedback_expr = TEST_ACTION_R2Z.to_string() + "/_action/feedback";
        // feedback publisher
        let feedback_publisher = session
            .declare_publisher(feedback_expr.clone())
            .await
            .unwrap();
        // send_goal
        let _send_goal_server = session
            .declare_queryable(send_goal_expr.clone())
            .callback(move |query| {
                let send_goal: FibonacciSendGoal =
                    cdr::deserialize(&query.payload().unwrap().to_bytes()).unwrap();
                println!(
                    "Receive goal request: order {:?}, goal id {:?}",
                    send_goal.goal, send_goal.goal_id
                );
                assert_eq!(send_goal.goal, action_request);

                // Reply to the action client
                let send_goal_response = ActionSendGoalResponse {
                    accept: true,
                    sec: 0,
                    nanosec: 0,
                };
                let payload =
                    cdr::serialize::<_, _, cdr::CdrLe>(&send_goal_response, cdr::Infinite).unwrap();
                query.reply(&send_goal_expr, payload).wait().unwrap();
            })
            .await
            .unwrap();
        // get_result
        let feedback_seq = feedback.clone();
        let result_seq = action_result.clone();
        let _get_result_server = session
            .declare_queryable(get_result_expr.clone())
            .callback(move |query| {
                let req_result: ActionResultRequest =
                    cdr::deserialize(&query.payload().unwrap().to_bytes()).unwrap();
                // Publish feedback
                let buf = cdr::serialize::<_, _, cdr::CdrLe>(
                    &FibonacciFeedback {
                        goal_id: req_result.goal_id,
                        sequence: feedback_seq.clone(),
                    },
                    cdr::Infinite,
                )
                .unwrap();
                feedback_publisher.put(buf).wait().unwrap();
                // Reply the get result
                let get_result_response = FibonacciResult {
                    status: 4,
                    sequence: result_seq.clone(),
                };
                let payload =
                    cdr::serialize::<_, _, cdr::CdrLe>(&get_result_response, cdr::Infinite)
                        .unwrap();
                query.reply(&get_result_expr, payload).wait().unwrap();
            })
            .await
            .unwrap();

        // ROS action client
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "ros_action_client", "").unwrap();
        let client = node
            .create_action_client::<r2r::example_interfaces::action::Fibonacci::Action>(
                TEST_ACTION_R2Z,
            )
            .unwrap();

        // Node spin
        let _handler = tokio::task::spawn_blocking(move || loop {
            node.spin_once(std::time::Duration::from_millis(100));
        });

        // Wait for the environment to be ready
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Send ROS 2 action request
        let my_goal = r2r::example_interfaces::action::Fibonacci::Goal {
            order: action_request,
        };
        let (_goal, result_fut, mut feedback_fut) =
            client.send_goal_request(my_goal).unwrap().await.unwrap();
        let feedback_result = feedback_fut.next().await.unwrap();
        println!("Receive feedback {:?}", feedback_result);
        assert_eq!(feedback_result.sequence, feedback);
        let (goal_status, result) = result_fut.await.unwrap();
        println!("Receive result {:?}, status {:?}", result, goal_status);
        assert_eq!(result.sequence, action_result);

        // Tell the main test thread, we're completed
        sender.send(()).unwrap();
    });

    let test_result = receiver.recv_timeout(DEFAULT_TIMEOUT);
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
