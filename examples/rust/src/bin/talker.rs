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
use serde::Serialize;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Serialize, PartialEq, Debug)]
struct Message {
    data: String,
}

#[tokio::main]
async fn main() {
    // Initiate logging
    env_logger::init();

    println!("Opening session...");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let publisher = session.declare_publisher("chatter").await.unwrap();

    for idx in 1..u32::MAX {
        sleep(Duration::from_secs(1)).await;
        let message = Message {
            data: format!("Hello World:{idx:4}"),
        };
        let buf = cdr::serialize::<_, _, CdrLe>(&message, Infinite).unwrap();
        println!("Publishing: '{}')...", message.data);
        publisher.put(buf).await.unwrap();
    }
}
