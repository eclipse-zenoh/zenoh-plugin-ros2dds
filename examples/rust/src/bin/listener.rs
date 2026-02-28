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
use clap::{App, Arg};
use serde::Deserialize;
use zenoh::config::Config;
use zenoh::prelude::r#async::*;

#[derive(Deserialize, PartialEq, Debug)]
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

    let subscriber = session.declare_subscriber("chatter").res().await.unwrap();

    while let Ok(sample) = subscriber.recv_async().await {
        match cdr::deserialize_from::<_, Message, _>(
            sample.value.payload.reader(),
            cdr::size::Infinite,
        ) {
            Ok(msg) => {
                println!("I heard: [{}]", msg.data);
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
