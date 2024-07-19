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

use std::fmt::Display;

use cyclors::qos::Qos;
use zenoh::key_expr::OwnedKeyExpr;

use crate::node_info::*;

/// A (local) discovery event of a ROS2 interface
#[derive(Debug)]
pub enum ROS2DiscoveryEvent {
    DiscoveredMsgPub(String, MsgPub),
    UndiscoveredMsgPub(String, MsgPub),
    DiscoveredMsgSub(String, MsgSub),
    UndiscoveredMsgSub(String, MsgSub),
    DiscoveredServiceSrv(String, ServiceSrv),
    UndiscoveredServiceSrv(String, ServiceSrv),
    DiscoveredServiceCli(String, ServiceCli),
    UndiscoveredServiceCli(String, ServiceCli),
    DiscoveredActionSrv(String, ActionSrv),
    UndiscoveredActionSrv(String, ActionSrv),
    DiscoveredActionCli(String, ActionCli),
    UndiscoveredActionCli(String, ActionCli),
}

impl std::fmt::Display for ROS2DiscoveryEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ROS2DiscoveryEvent::*;
        match self {
            DiscoveredMsgPub(node, iface) => write!(f, "Node {node} declares {iface}"),
            DiscoveredMsgSub(node, iface) => write!(f, "Node {node} declares {iface}"),
            DiscoveredServiceSrv(node, iface) => write!(f, "Node {node} declares {iface}"),
            DiscoveredServiceCli(node, iface) => write!(f, "Node {node} declares {iface}"),
            DiscoveredActionSrv(node, iface) => write!(f, "Node {node} declares {iface}"),
            DiscoveredActionCli(node, iface) => write!(f, "Node {node} declares {iface}"),
            UndiscoveredMsgPub(node, iface) => write!(f, "Node {node} undeclares {iface}"),
            UndiscoveredMsgSub(node, iface) => write!(f, "Node {node} undeclares {iface}"),
            UndiscoveredServiceSrv(node, iface) => write!(f, "Node {node} undeclares {iface}"),
            UndiscoveredServiceCli(node, iface) => write!(f, "Node {node} undeclares {iface}"),
            UndiscoveredActionSrv(node, iface) => write!(f, "Node {node} undeclares {iface}"),
            UndiscoveredActionCli(node, iface) => write!(f, "Node {node} undeclares {iface}"),
        }
    }
}

/// A (remote) announcement/retirement of a ROS2 interface
#[derive(Debug)]
pub enum ROS2AnnouncementEvent {
    AnnouncedMsgPub {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
        ros2_type: String,
        keyless: bool,
        writer_qos: Qos,
    },
    RetiredMsgPub {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
    },
    AnnouncedMsgSub {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
        ros2_type: String,
        keyless: bool,
        reader_qos: Qos,
    },
    RetiredMsgSub {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
    },
    AnnouncedServiceSrv {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
        ros2_type: String,
    },
    RetiredServiceSrv {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
    },
    AnnouncedServiceCli {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
        ros2_type: String,
    },
    RetiredServiceCli {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
    },
    AnnouncedActionSrv {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
        ros2_type: String,
    },
    RetiredActionSrv {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
    },
    AnnouncedActionCli {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
        ros2_type: String,
    },
    RetiredActionCli {
        zenoh_id: OwnedKeyExpr,
        zenoh_key_expr: OwnedKeyExpr,
    },
}

impl Display for ROS2AnnouncementEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ROS2AnnouncementEvent::*;
        match self {
            AnnouncedMsgPub { zenoh_key_expr, .. } => {
                write!(f, "announces Publisher {zenoh_key_expr}")
            }
            AnnouncedMsgSub { zenoh_key_expr, .. } => {
                write!(f, "announces Subscriber {zenoh_key_expr}")
            }
            AnnouncedServiceSrv { zenoh_key_expr, .. } => {
                write!(f, "announces Service Server {zenoh_key_expr}")
            }
            AnnouncedServiceCli { zenoh_key_expr, .. } => {
                write!(f, "announces Service Client {zenoh_key_expr}")
            }
            AnnouncedActionSrv { zenoh_key_expr, .. } => {
                write!(f, "announces Action Server {zenoh_key_expr}")
            }
            AnnouncedActionCli { zenoh_key_expr, .. } => {
                write!(f, "announces Action Client {zenoh_key_expr}")
            }
            RetiredMsgPub { zenoh_key_expr, .. } => write!(f, "retires Publisher {zenoh_key_expr}"),
            RetiredMsgSub { zenoh_key_expr, .. } => {
                write!(f, "retires Subscriber {zenoh_key_expr}")
            }
            RetiredServiceSrv { zenoh_key_expr, .. } => {
                write!(f, "retires Service Server {zenoh_key_expr}")
            }
            RetiredServiceCli { zenoh_key_expr, .. } => {
                write!(f, "retires Service Client {zenoh_key_expr}")
            }
            RetiredActionSrv { zenoh_key_expr, .. } => {
                write!(f, "retires Action Server {zenoh_key_expr}")
            }
            RetiredActionCli { zenoh_key_expr, .. } => {
                write!(f, "retires Action Client {zenoh_key_expr}")
            }
        }
    }
}
