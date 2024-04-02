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
use async_std::task::sleep;
use cdr::{CdrLe, Infinite};
use clap::{App, Arg};
use serde::Serialize;
use std::time::Duration;
use zenoh::config::Config;
use zenoh::prelude::r#async::*;

#[derive(Serialize, PartialEq, Debug)]
struct Message {
    data: String,
}

#[async_std::main]
async fn main() {
    // Initiate logging
    env_logger::init();

    let config = parse_args();

    println!("Opening session...");
    let session = zenoh::open(config).res().await.unwrap();

    let key_expr = "chatter";    

    println!("Declaring Publisher on '{key_expr}'...");
    let publisher = session.declare_publisher(key_expr).res().await.unwrap();

    for idx in 0..u32::MAX {
        sleep(Duration::from_secs(1)).await;
        let message = Message {
            data: format!("Hello World:{idx:4}"),
        };
        let buf = cdr::serialize::<_, _, CdrLe>(&message, Infinite).unwrap();
        println!("Putting Data ('{}': '{}')...", &key_expr, message.data);
        publisher.put(buf).res().await.unwrap();
    }
}

fn parse_args() -> Config {
    let args = App::new("zenoh talker example")
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
