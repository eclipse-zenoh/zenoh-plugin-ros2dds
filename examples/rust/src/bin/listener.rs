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
use serde::Deserialize;

#[derive(Deserialize, PartialEq, Debug)]
struct Message {
    data: String,
}
#[tokio::main]
async fn main() {
    // Initiate logging
    env_logger::init();

    println!("Opening session...");
    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let subscriber = session.declare_subscriber("chatter").await.unwrap();

    while let Ok(sample) = subscriber.recv_async().await {
        match cdr::deserialize_from::<_, Message, _>(sample.payload().reader(), cdr::size::Infinite)
        {
            Ok(msg) => {
                println!("I heard: [{}]", msg.data);
            }
            Err(e) => log::warn!("Error decoding message: {}", e),
        }
    }
}
