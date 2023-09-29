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

use zenoh::prelude::KeyExpr;

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
///
/// # Examples
pub fn ros2_message_type_to_dds_type(ros_topic: &str) -> String {
    let mut result = ros_topic.replace("/", "::");
    result
        .rfind(':')
        .map(|pos| result.insert_str(pos + 1, "dds_::"));
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

// check if name is a ROS name: starting with '/' and useable as a key expression (removing 1st '/')
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
