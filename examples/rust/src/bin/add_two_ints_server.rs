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
struct AddTwoIntsRequest {
    a: i64,
    b: i64,
}

#[derive(Serialize, PartialEq, Debug)]
struct AddTwoIntsResponse {
    sum: i64,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let session = zenoh::open(zenoh::Config::default()).await.unwrap();

    let liveliness = session.liveliness().declare_token("@/f2f0382de97ce1ddc0e6738833feb2b9/@ros2_lv/SS/add_two_ints/example_interfaces§srv§AddTwoInts").await.unwrap();

    let queryable = session
        .declare_queryable("add_two_ints")
        .complete(true)
        .await
        .unwrap();

    while let Ok(query) = queryable.recv_async().await {
        match cdr::deserialize_from::<_, AddTwoIntsRequest, _>(
            query.payload().unwrap().reader(),
            cdr::size::Infinite,
        ) {
            Ok(res) => {
                let reply = AddTwoIntsResponse { sum: res.a + res.b };
                println!("Result of add_two_ints: {}", reply.sum);
                let payload = cdr::serialize::<_, _, CdrLe>(&reply, Infinite).unwrap();
                let key_expr = "add_two_ints";
                query.reply(key_expr, payload).await.unwrap();
            }
            Err(e) => log::warn!("Error decoding message: {}", e),
        }
    }
}
