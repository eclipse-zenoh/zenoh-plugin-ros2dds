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

use std::{
    env::VarError,
    sync::atomic::{AtomicU32, Ordering},
};

use cyclors::{
    dds_entity_t,
    qos::{
        Durability, DurabilityKind, History, HistoryKind, IgnoreLocal, IgnoreLocalKind, Qos,
        Reliability, ReliabilityKind, TypeConsistency, TypeConsistencyKind, WriterDataLifecycle,
        DDS_INFINITE_TIME,
    },
};
use zenoh::prelude::{keyexpr, KeyExpr, OwnedKeyExpr};

use crate::{config::Config, dds_utils::get_guid, ke_for_sure};

pub const ROS2_ACTION_CANCEL_GOAL_SRV_TYPE: &str = "action_msgs/srv/CancelGoal";
pub const ROS2_ACTION_STATUS_MSG_TYPE: &str = "action_msgs/msg/GoalStatusArray";

// ROS_DISTRO value assumed if the environment variable is not set
pub const ASSUMED_ROS_DISTRO: &str = "iron";

lazy_static::lazy_static!(
    pub static ref ROS_DISTRO: String = get_ros_distro();

    pub static ref KE_SUFFIX_ACTION_SEND_GOAL: &'static keyexpr = ke_for_sure!("_action/send_goal");
    pub static ref KE_SUFFIX_ACTION_CANCEL_GOAL: &'static keyexpr = ke_for_sure!("_action/cancel_goal");
    pub static ref KE_SUFFIX_ACTION_GET_RESULT: &'static keyexpr = ke_for_sure!("_action/get_result");
    pub static ref KE_SUFFIX_ACTION_FEEDBACK: &'static keyexpr = ke_for_sure!("_action/feedback");
    pub static ref KE_SUFFIX_ACTION_STATUS: &'static keyexpr = ke_for_sure!("_action/status");

    pub static ref QOS_DEFAULT_SERVICE: Qos = ros2_service_default_qos();
    pub static ref QOS_DEFAULT_ACTION_FEEDBACK: Qos = ros2_action_feedback_default_qos();
    pub static ref QOS_DEFAULT_ACTION_STATUS: Qos = ros2_action_status_default_qos();
);

pub fn get_ros_distro() -> String {
    match std::env::var("ROS_DISTRO") {
        Ok(s) if !s.is_empty() => {
            log::debug!("ROS_DISTRO detected: {s}");
            s
        }
        Ok(_) | Err(VarError::NotPresent) => {
            log::warn!(
                "ROS_DISTRO environment variable is not set. \
                Assuming '{ASSUMED_ROS_DISTRO}', but this could lead to errors on 'ros_discovery_info' \
                (see https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/issues/21)"
            );
            ASSUMED_ROS_DISTRO.to_string()
        }
        Err(VarError::NotUnicode(s)) => {
            log::warn!(
                "ROS_DISTRO environment variable is invalid ('{s:?}'). \
                Assuming '{ASSUMED_ROS_DISTRO}', but this could lead to errors on 'ros_discovery_info' \
                (see https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/issues/21)"
            );
            ASSUMED_ROS_DISTRO.to_string()
        }
    }
}

/// Check if the ROS_DISTRO is older than `distro`, comparing the 1st char.
/// None is returned if ROS_DISTRO is not set.
pub fn ros_distro_is_less_than(distro: &str) -> bool {
    assert!(!distro.is_empty());
    ROS_DISTRO.chars().next() < distro.chars().next()
}

/// Convert ROS2 interface name to a Zenoh key expression,
/// prefixing with "namespace" if configured
pub fn ros2_name_to_key_expr(ros2_name: &str, config: &Config) -> OwnedKeyExpr {
    // ros2_name as discovered by the bridge starts with a '/'
    // config.namespace starts with a '/'
    // But a Zenoh key_expr shall not start with a '/'
    if config.namespace == "/" {
        ke_for_sure!(&ros2_name[1..]).to_owned()
    } else {
        ke_for_sure!(&config.namespace[1..]) / ke_for_sure!(&ros2_name[1..])
    }
}

/// Convert a Zenoh key expression to a ROS2 full interface name,
/// removing "namespace" prefix if configured and present in the key expr
pub fn key_expr_to_ros2_name(key_expr: &keyexpr, config: &Config) -> String {
    // Zenoh key_expr never starts with a '/'
    // But the full ROS2 name that is returned shall (full == with a namespace, even if just '/')
    if config.namespace == "/" {
        format!("/{key_expr}")
    } else {
        match key_expr.as_str().strip_prefix(&config.namespace[1..]) {
            Some(s) => s.to_string(),
            None => format!("/{key_expr}"),
        }
    }
}

/// Convert DDS Topic type to ROS2 Message type
pub fn dds_type_to_ros2_message_type(dds_topic: &str) -> String {
    let result = dds_topic.replace("::dds_::", "::").replace("::", "/");
    if result.ends_with('_') {
        result[..result.len() - 1].into()
    } else {
        result
    }
}

/// Convert ROS2 Message type to DDS Topic type
pub fn ros2_message_type_to_dds_type(ros_topic: &str) -> String {
    let mut result = ros_topic.replace('/', "::");
    if let Some(pos) = result.rfind(':') {
        result.insert_str(pos + 1, "dds_::")
    }
    result.push('_');
    result
}

/// Convert DDS Topic type for ROS2 Service to ROS2 Service type
pub fn dds_type_to_ros2_service_type(dds_topic: &str) -> String {
    dds_type_to_ros2_message_type(
        dds_topic
            .strip_suffix("_Request_")
            .or(dds_topic.strip_suffix("_Response_"))
            .unwrap_or(dds_topic),
    )
}

/// Convert ROS2 Service type to DDS Topic type for Request
pub fn ros2_service_type_to_request_dds_type(ros_service: &str) -> String {
    ros2_message_type_to_dds_type(&format!("{ros_service}_Request"))
}

/// Convert ROS2 Service type to DDS Topic type for Reply
pub fn ros2_service_type_to_reply_dds_type(ros_service: &str) -> String {
    ros2_message_type_to_dds_type(&format!("{ros_service}_Response"))
}

/// Convert DDS Topic type for ROS2 Action to ROS2 Action type
/// Warning: can't work for "rt/.../_action/status", "rq/.../_action/cancel_goalRequest"
/// or "rr../_action/cancel_goalReply" topic, since their types are generic
pub fn dds_type_to_ros2_action_type(dds_topic: &str) -> String {
    dds_type_to_ros2_message_type(
        dds_topic
            .strip_suffix("_SendGoal_Request_")
            .or(dds_topic.strip_suffix("_SendGoal_Response_"))
            .or(dds_topic.strip_suffix("_GetResult_Request_"))
            .or(dds_topic.strip_suffix("_GetResult_Response_"))
            .or(dds_topic.strip_suffix("_FeedbackMessage_"))
            .unwrap_or(dds_topic),
    )
}

fn ros2_service_default_qos() -> Qos {
    // Default Service QoS copied from:
    // https://github.com/ros2/rmw/blob/83445be486deae8c78d275e092eafb4bf380bd49/rmw/include/rmw/qos_profiles.h#L64C44-L64C44
    Qos {
        history: Some(History {
            kind: HistoryKind::KEEP_LAST,
            depth: 10,
        }),
        reliability: Some(Reliability {
            kind: ReliabilityKind::RELIABLE,
            max_blocking_time: DDS_INFINITE_TIME,
        }),
        ignore_local: Some(IgnoreLocal {
            kind: IgnoreLocalKind::PARTICIPANT,
        }),
        ..Default::default()
    }
}

fn ros2_action_feedback_default_qos() -> Qos {
    Qos {
        history: Some(History {
            kind: HistoryKind::KEEP_LAST,
            depth: 10,
        }),
        reliability: Some(Reliability {
            kind: ReliabilityKind::RELIABLE,
            max_blocking_time: DDS_INFINITE_TIME,
        }),
        data_representation: Some([0].into()),
        writer_data_lifecycle: Some(WriterDataLifecycle {
            autodispose_unregistered_instances: false,
        }),
        type_consistency: Some(TypeConsistency {
            kind: TypeConsistencyKind::ALLOW_TYPE_COERCION,
            ignore_sequence_bounds: true,
            ignore_string_bounds: true,
            ignore_member_names: false,
            prevent_type_widening: false,
            force_type_validation: false,
        }),
        ignore_local: Some(IgnoreLocal {
            kind: IgnoreLocalKind::PARTICIPANT,
        }),
        ..Default::default()
    }
}

fn ros2_action_status_default_qos() -> Qos {
    // Default Status topic QoS copied from:
    // https://github.com/ros2/rcl/blob/8f7f4f0804a34ee9d9ecd2d7e75a57ce2b7ced5d/rcl_action/include/rcl_action/default_qos.h#L30
    Qos {
        durability: Some(Durability {
            kind: DurabilityKind::TRANSIENT_LOCAL,
        }),
        reliability: Some(Reliability {
            kind: ReliabilityKind::RELIABLE,
            max_blocking_time: DDS_INFINITE_TIME,
        }),
        data_representation: Some([0].into()),
        writer_data_lifecycle: Some(WriterDataLifecycle {
            autodispose_unregistered_instances: false,
        }),
        type_consistency: Some(TypeConsistency {
            kind: TypeConsistencyKind::ALLOW_TYPE_COERCION,
            ignore_sequence_bounds: true,
            ignore_string_bounds: true,
            ignore_member_names: false,
            prevent_type_widening: false,
            force_type_validation: false,
        }),
        ignore_local: Some(IgnoreLocal {
            kind: IgnoreLocalKind::PARTICIPANT,
        }),
        ..Default::default()
    }
}

pub fn is_service_for_action(ros2_service_name: &str) -> bool {
    ros2_service_name.ends_with(KE_SUFFIX_ACTION_SEND_GOAL.as_str())
        || ros2_service_name.ends_with(KE_SUFFIX_ACTION_CANCEL_GOAL.as_str())
        || ros2_service_name.ends_with(KE_SUFFIX_ACTION_GET_RESULT.as_str())
}

pub fn is_message_for_action(ros2_message_name: &str) -> bool {
    ros2_message_name.ends_with(KE_SUFFIX_ACTION_FEEDBACK.as_str())
        || ros2_message_name.ends_with(KE_SUFFIX_ACTION_STATUS.as_str())
}

/// Check if name is a ROS name: starting with '/' and useable as a key expression (removing 1st '/')
#[inline]
pub fn check_ros_name(name: &str) -> Result<(), String> {
    if !name.starts_with('/') || KeyExpr::try_from("&(name[1..])").is_err() {
        Err(format!(
            "'{name}' cannot be converted as a Zenoh key expression"
        ))
    } else {
        Ok(())
    }
}

lazy_static::lazy_static!(
    pub static ref CLIENT_ID_COUNTER: AtomicU32 = AtomicU32::default();
);

/// Create a new id for a Service Client or Server, in the same way than rmw_cyclone_dds
/// The id as a hexadecimal String with '.' separators between each 2 bytes
pub fn new_service_id(participant: &dds_entity_t) -> Result<String, String> {
    // Service client or server id (16 bytes) generated in the same way than rmw_cyclone_dds here:
    // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/rmw_node.cpp#L4908
    let mut id: [u8; 16] = *get_guid(participant)?;
    let counter_be = CLIENT_ID_COUNTER
        .fetch_add(1, Ordering::Relaxed)
        .to_be_bytes();
    id[12..].copy_from_slice(&counter_be);
    let id_str = id
        .iter()
        .map(|b| format!("{b:x}"))
        .collect::<Vec<String>>()
        .join(".");
    Ok(id_str)
}

mod tests {

    #[test]
    fn test_types_conversions() {
        use crate::ros2_utils::*;

        assert_eq!(
            dds_type_to_ros2_message_type("geometry_msgs::msg::dds_::Twist_"),
            "geometry_msgs/msg/Twist"
        );
        assert_eq!(
            dds_type_to_ros2_message_type("rcl_interfaces::msg::dds_::Log_"),
            "rcl_interfaces/msg/Log"
        );

        assert_eq!(
            ros2_message_type_to_dds_type("geometry_msgs/msg/Twist"),
            "geometry_msgs::msg::dds_::Twist_"
        );
        assert_eq!(
            ros2_message_type_to_dds_type("rcl_interfaces/msg/Log"),
            "rcl_interfaces::msg::dds_::Log_"
        );

        assert_eq!(
            dds_type_to_ros2_service_type("example_interfaces::srv::dds_::AddTwoInts_Request_"),
            "example_interfaces/srv/AddTwoInts"
        );
        assert_eq!(
            dds_type_to_ros2_service_type("example_interfaces::srv::dds_::AddTwoInts_Response_"),
            "example_interfaces/srv/AddTwoInts"
        );
        assert_eq!(
            dds_type_to_ros2_service_type("rcl_interfaces::srv::dds_::ListParameters_Request_"),
            "rcl_interfaces/srv/ListParameters"
        );
        assert_eq!(
            dds_type_to_ros2_service_type("rcl_interfaces::srv::dds_::ListParameters_Response_"),
            "rcl_interfaces/srv/ListParameters"
        );

        assert_eq!(
            dds_type_to_ros2_action_type(
                "example_interfaces::action::dds_::Fibonacci_SendGoal_Request_"
            ),
            "example_interfaces/action/Fibonacci"
        );
        assert_eq!(
            dds_type_to_ros2_action_type(
                "example_interfaces::action::dds_::Fibonacci_SendGoal_Response_"
            ),
            "example_interfaces/action/Fibonacci"
        );
        assert_eq!(
            dds_type_to_ros2_action_type(
                "example_interfaces::action::dds_::Fibonacci_GetResult_Request_"
            ),
            "example_interfaces/action/Fibonacci"
        );
        assert_eq!(
            dds_type_to_ros2_action_type(
                "example_interfaces::action::dds_::Fibonacci_GetResult_Response_"
            ),
            "example_interfaces/action/Fibonacci"
        );
        assert_eq!(
            dds_type_to_ros2_action_type(
                "example_interfaces::action::dds_::Fibonacci_FeedbackMessage_"
            ),
            "example_interfaces/action/Fibonacci"
        );
    }
}
