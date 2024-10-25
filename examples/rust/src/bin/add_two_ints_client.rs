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

#[derive(Serialize, PartialEq, Debug)]
struct AddTwoIntsRequest {
    a: i64,
    b: i64,
}

#[derive(Deserialize, PartialEq, Debug)]
struct AddTwoIntsResponse {
    sum: i64,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let req = AddTwoIntsRequest { a: 2, b: 3 };
    let buf = cdr::serialize::<_, _, CdrLe>(&req, Infinite).unwrap();
    let replies = session.get("add_two_ints").payload(buf).await.unwrap();

    while let Ok(reply) = replies.recv_async().await {
        match cdr::deserialize_from::<_, AddTwoIntsResponse, _>(
            reply.result().unwrap().payload().reader(),
            cdr::size::Infinite,
        ) {
            Ok(res) => {
                println!("Result of add_two_ints: {}", res.sum);
            }
            Err(e) => log::warn!("Error decoding message: {}", e),
        }
    }
}
