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
use clap::{App, Arg};
use serde::{Deserialize, Serialize};
use zenoh::config::Config;
use zenoh::prelude::r#async::*;

#[derive(Serialize, PartialEq, Debug)]
struct AddTwoIntsRequest {
    a: i64,
    b: i64,
}

#[derive(Deserialize, PartialEq, Debug)]
struct AddTwoIntsResponse {
    sum: i64,
}

#[async_std::main]
async fn main() {
    env_logger::init();

    let config = parse_args();

    let session = zenoh::open(config).res().await.unwrap();

    let req = AddTwoIntsRequest { a: 2, b: 3 };
    let buf = cdr::serialize::<_, _, CdrLe>(&req, Infinite).unwrap();
    let replies = session
        .get("add_two_ints")
        .with_value(buf)
        .res()
        .await
        .unwrap();

    while let Ok(reply) = replies.recv_async().await {
        match cdr::deserialize_from::<_, AddTwoIntsResponse, _>(
            reply.sample.unwrap().payload.reader(),
            cdr::size::Infinite,
        ) {
            Ok(res) => {
                println!("Result of add_two_ints: {}", res.sum);
            }
            Err(e) => log::warn!("Error decoding message: {}", e),
        }
    }
}

fn parse_args() -> Config {
    let args = App::new("zenoh sub example")
        .arg(Arg::from_usage(
            "-c, --config=[FILE]      'A configuration file.'",
        ))
        .get_matches();

    let config = if let Some(conf_file) = args.value_of("config") {
        Config::from_file(conf_file).unwrap()
    } else {
        Config::default()
    };

    config
}
