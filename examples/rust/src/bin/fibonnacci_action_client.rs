//
// Copyright (c) 2023 ZettaScale Technology
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
use cdr::{CdrLe, Infinite};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, PartialEq, Debug)]
struct Time {
    sec: u32,
    nsec: u32,
}

#[derive(Serialize, PartialEq, Debug)]
struct FibonacciSendGoalRequest {
    goal_id: [u8; 16],
    order: i32,
}

#[derive(Deserialize, PartialEq, Debug)]
struct FibonacciSendGoalResponse {
    accepted: bool,
    stamp: Time,
}

#[derive(Serialize, PartialEq, Debug)]
struct FibonacciGetResultRequest {
    goal_id: [u8; 16],
}

#[derive(Deserialize, PartialEq, Debug)]
struct FibonacciGetResultResponse {
    status: i8,
    sequence: Vec<i32>,
}

#[derive(Deserialize, PartialEq, Debug)]
struct FibonacciFeedback {
    goal_id: [u8; 16],
    partial_sequence: Vec<i32>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let _subscriber = session
        .declare_subscriber("fibonacci/_action/feedback")
        .callback(|sample| {
            match cdr::deserialize_from::<_, FibonacciFeedback, _>(
                sample.payload().reader(),
                cdr::size::Infinite,
            ) {
                Ok(msg) => {
                    println!(
                        "Next number in sequence received: {:?}",
                        msg.partial_sequence
                    );
                }
                Err(e) => log::warn!("Error decoding message: {}", e),
            };
        })
        .await
        .unwrap();

    let goal_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let req = FibonacciSendGoalRequest {
        goal_id: goal_id,
        order: 10,
    };

    let buf = cdr::serialize::<_, _, CdrLe>(&req, Infinite).unwrap();
    println!("Sending goal");
    let replies = session
        .get("fibonacci/_action/send_goal")
        .payload(buf)
        .await
        .unwrap();

    while let Ok(reply) = replies.recv_async().await {
        match cdr::deserialize_from::<_, FibonacciSendGoalResponse, _>(
            reply.result().unwrap().payload().reader(),
            cdr::size::Infinite,
        ) {
            Ok(res) => {
                if res.accepted {
                    println!("Goal accepted by server, waiting for result");
                } else {
                    println!("Goal rejected :(");
                    return;
                }
            }
            Err(e) => log::warn!("Error decoding message: {}", e),
        }
    }

    let req = FibonacciGetResultRequest { goal_id: goal_id };
    let buf = cdr::serialize::<_, _, CdrLe>(&req, Infinite).unwrap();
    let replies = session
        .get("fibonacci/_action/get_result")
        .payload(buf)
        .await
        .unwrap();
    while let Ok(reply) = replies.recv_async().await {
        match cdr::deserialize_from::<_, FibonacciGetResultResponse, _>(
            reply.result().unwrap().payload().reader(),
            cdr::size::Infinite,
        ) {
            Ok(res) => {
                println!("Result: {:?}", res.sequence);
            }
            Err(e) => log::warn!("Error decoding message: {}", e),
        }
    }
}
