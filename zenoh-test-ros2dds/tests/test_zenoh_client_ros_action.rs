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

use futures::StreamExt;
use r2r::{self};
use serde_derive::{Deserialize, Serialize};

// The test action
const TEST_ACTION_Z2R: &str = "test_action_z2r";

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
fn test_zenoh_client_ros_action() {
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
        // Random goal id
        let goal_id = [1; 16];

        // ROS action server
        // Note that we ignore the feedback and just return back the result
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "ros_action_server", "").unwrap();
        let mut action_server = node
            .create_action_server::<r2r::example_interfaces::action::Fibonacci::Action>(
                TEST_ACTION_Z2R,
            )
            .unwrap();
        let sequence = action_result.clone();
        let feedback_seq = feedback.clone();
        tokio::spawn(async move {
            while let Some(req) = action_server.next().await {
                println!(
                    "Receive goal request: order {}, goal id {}",
                    req.goal.order, req.uuid
                );
                assert_eq!(req.goal.order, action_request);
                let (mut recv_goal, mut _cancel) = req.accept().unwrap();
                let feedback_data = r2r::example_interfaces::action::Fibonacci::Feedback {
                    sequence: feedback_seq.clone(),
                };
                recv_goal.publish_feedback(feedback_data).unwrap();
                recv_goal
                    .succeed(r2r::example_interfaces::action::Fibonacci::Result {
                        sequence: sequence.clone(),
                    })
                    .unwrap();
            }
        });

        // Node spin
        let _handler = tokio::task::spawn_blocking(move || loop {
            node.spin_once(std::time::Duration::from_millis(100));
        });

        // Zenoh action client
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let send_goal_expr = TEST_ACTION_Z2R.to_string() + "/_action/send_goal";
        let get_result_expr = TEST_ACTION_Z2R.to_string() + "/_action/get_result";
        let feedback_expr = TEST_ACTION_Z2R.to_string() + "/_action/feedback";
        let send_goal_client = session.declare_querier(send_goal_expr).await.unwrap();
        let get_result_client = session.declare_querier(get_result_expr).await.unwrap();
        let subscriber = session.declare_subscriber(feedback_expr).await.unwrap();

        // Wait for the environment to be ready
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Send Zenoh action request
        let req = FibonacciSendGoal {
            goal_id,
            goal: action_request,
        };
        let buf = cdr::serialize::<_, _, cdr::CdrLe>(&req, cdr::Infinite).unwrap();
        let recv_handler = send_goal_client.get().payload(buf).await.unwrap();
        let reply_sample = recv_handler.recv().unwrap();
        let reader = reply_sample.result().unwrap().payload().reader();
        let reply: ActionSendGoalResponse =
            cdr::deserialize_from(reader, cdr::size::Infinite).unwrap();
        println!("Receive the result of SendGoal: {:?}", reply.accept);

        // Receive feedback
        let sample = subscriber.recv_async().await.unwrap();
        let feedback_result: FibonacciFeedback =
            cdr::deserialize_from(sample.payload().reader(), cdr::size::Infinite).unwrap();
        println!("Receive feedback {:?}", feedback_result.sequence);
        assert_eq!(feedback_result.sequence, feedback);

        // Get the result from ROS 2 action server
        let req = ActionResultRequest { goal_id };
        let buf = cdr::serialize::<_, _, cdr::CdrLe>(&req, cdr::Infinite).unwrap();
        let recv_handler = get_result_client.get().payload(buf).await.unwrap();
        let reply_sample = recv_handler.recv().unwrap();
        let reader = reply_sample.result().unwrap().payload().reader();
        let reply: FibonacciResult = cdr::deserialize_from(reader, cdr::size::Infinite).unwrap();
        println!(
            "Receive result: {:?}, status {:?}",
            reply.sequence, reply.status
        );
        assert_eq!(reply.sequence, action_result);

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
