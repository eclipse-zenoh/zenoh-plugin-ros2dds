//
// Copyright (c) 2022 ZettaScale Technology
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

//! Copied from https://github.com/eclipse-zenoh/zenoh/blob/master/examples/src/lib.rs
//! Usual command line arguments to manage a Zenoh Config

use zenoh::config::Config;

#[derive(clap::ValueEnum, Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Wai {
    Peer,
    Client,
    Router,
}

impl core::fmt::Display for Wai {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        core::fmt::Debug::fmt(&self, f)
    }
}

impl From<Wai> for zenoh::scouting::WhatAmI {
    fn from(val: Wai) -> Self {
        match val {
            Wai::Peer => zenoh::scouting::WhatAmI::Peer,
            Wai::Client => zenoh::scouting::WhatAmI::Client,
            Wai::Router => zenoh::scouting::WhatAmI::Router,
        }
    }
}

#[derive(clap::Parser, Clone, PartialEq, Eq, Hash, Debug)]
pub struct CommonArgs {
    #[arg(short, long)]
    /// A configuration file.
    pub config: Option<String>,
    #[arg(short, long)]
    /// The Zenoh session mode [default: router].
    pub mode: Option<Wai>,
    #[arg(short = 'e', long)]
    /// Endpoints to connect to.
    pub connect: Vec<String>,
    #[arg(short, long)]
    /// Endpoints to listen on.
    pub listen: Vec<String>,
    #[arg(long)]
    /// Disable the multicast-based scouting mechanism.
    pub no_multicast_scouting: bool,
    #[arg(long)]
    /// Disable the multicast-based scouting mechanism.
    pub enable_shm: bool,
}

impl From<CommonArgs> for Config {
    fn from(value: CommonArgs) -> Self {
        (&value).into()
    }
}
impl From<&CommonArgs> for Config {
    fn from(value: &CommonArgs) -> Self {
        let mut config = match &value.config {
            Some(path) => Config::from_file(path).unwrap(),
            None => Config::default(),
        };
        if value.mode.is_some() {
            // apply mode set via command line, overwritting mode set in config file
            config.set_mode(value.mode.map(Into::into)).unwrap();
        } else if config.mode().is_none() {
            // no mode set neither via command line, neither in config file - set Router mode by default
            config
                .set_mode(Some(zenoh::scouting::WhatAmI::Router))
                .unwrap();
        }
        if !value.connect.is_empty() {
            config.connect.endpoints = value.connect.iter().map(|v| v.parse().unwrap()).collect();
        }
        if !value.listen.is_empty() {
            config.listen.endpoints = value.listen.iter().map(|v| v.parse().unwrap()).collect();
        }
        if value.no_multicast_scouting {
            config.scouting.multicast.set_enabled(Some(false)).unwrap();
        }
        if value.enable_shm {
            #[cfg(feature = "shared-memory")]
            config.transport.shared_memory.set_enabled(true).unwrap();
            #[cfg(not(feature = "shared-memory"))]
            {
                println!("enable-shm argument: SHM cannot be enabled, because Zenoh is compiled without shared-memory feature!");
                std::process::exit(-1);
            }
        }
        config
    }
}
