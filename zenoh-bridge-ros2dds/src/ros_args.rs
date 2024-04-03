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

use zenoh::config::Config;

use crate::bridge_args::insert_json5;

#[derive(clap::Parser, Clone, PartialEq, Eq, Hash, Debug)]
pub struct RosArgs {
    /// Name remapping
    #[arg(short, long, value_name = "FROM:=TO", value_parser = parse_remap)]
    pub remap: Vec<Remap>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum RemapFrom {
    Namespace,
    Node,
    _Name(String),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Remap {
    from: RemapFrom,
    to: String,
}

fn parse_remap(s: &str) -> Result<Remap, String> {
    match s.split_once(":=") {
        Some(("__ns", to)) if !to.is_empty() => Ok(Remap {
            from: RemapFrom::Namespace,
            to: to.into(),
        }),
        Some(("__node", to)) if !to.is_empty() => Ok(Remap {
            from: RemapFrom::Node,
            to: to.into(),
        }),
        Some((from, to)) if !from.is_empty() && !to.is_empty() => {
            Err("only remapping for '__ns' and '__node' are currently supported".into())
        }
        _ => Err("valid value must have format 'fromp:=to'".into()),
    }
}

impl RosArgs {
    pub fn update_config(&self, config: &mut Config) {
        for r in &self.remap {
            match r.from {
                RemapFrom::Namespace => {
                    tracing::info!(
                        "Remapping namespace to '{}' as per ROS command line argument",
                        r.to
                    );
                    insert_json5(config, "plugins/ros2dds/namespace", &r.to);
                }
                RemapFrom::Node => {
                    tracing::info!(
                        "Remapping node name to '{}' as per ROS command line argument",
                        r.to
                    );
                    insert_json5(config, "plugins/ros2dds/nodename", &r.to);
                }
                RemapFrom::_Name(_) => {
                    panic!("Unsupported remapping... only '__node' and '__ns' are supported")
                }
            }
        }
    }
}
