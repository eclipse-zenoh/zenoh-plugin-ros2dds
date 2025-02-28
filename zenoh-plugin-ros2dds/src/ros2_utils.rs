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
    str,
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
use zenoh::{
    bytes::ZBytes,
    internal::bail,
    key_expr::{keyexpr, KeyExpr, OwnedKeyExpr},
    Error as ZError,
};

use crate::{config::Config, dds_utils::get_guid};

pub const ROS2_ACTION_CANCEL_GOAL_SRV_TYPE: &str = "action_msgs/srv/CancelGoal";
pub const ROS2_ACTION_STATUS_MSG_TYPE: &str = "action_msgs/msg/GoalStatusArray";
// Type hash for action_msgs/msg/GoalStatusArray in Iron and Jazzy (might change in future versions)
pub const ROS2_ACTION_STATUS_MSG_TYPE_HASH: &str =
    "RIHS01_91a0593bacdcc50ea9bdcf849a938b128412cc1ea821245c663bcd26f83c295e";

// ROS_DISTRO value assumed if the environment variable is not set
pub const ASSUMED_ROS_DISTRO: &str = "iron";

// Separator used by ROS 2 in USER_DATA QoS
pub const USER_DATA_PROPS_SEPARATOR: char = ';';
// Key for type hash used by ROS 2 in USER_DATA QoS
pub const USER_DATA_TYPEHASH_KEY: &str = "typehash=";

lazy_static::lazy_static!(
    pub static ref ROS_DISTRO: String = get_ros_distro();

    pub static ref KE_SUFFIX_ACTION_SEND_GOAL: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("_action/send_goal") };
    pub static ref KE_SUFFIX_ACTION_CANCEL_GOAL: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("_action/cancel_goal") };
    pub static ref KE_SUFFIX_ACTION_GET_RESULT: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("_action/get_result") };
    pub static ref KE_SUFFIX_ACTION_FEEDBACK: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("_action/feedback") };
    pub static ref KE_SUFFIX_ACTION_STATUS: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("_action/status") };

    pub static ref QOS_DEFAULT_SERVICE: Qos = ros2_service_default_qos();
    pub static ref QOS_DEFAULT_ACTION_FEEDBACK: Qos = ros2_action_feedback_default_qos();
    pub static ref QOS_DEFAULT_ACTION_STATUS: Qos = ros2_action_status_default_qos();
);

pub fn get_ros_distro() -> String {
    match std::env::var("ROS_DISTRO") {
        Ok(s) if !s.is_empty() => {
            tracing::debug!("ROS_DISTRO detected: {s}");
            s
        }
        Ok(_) | Err(VarError::NotPresent) => {
            tracing::warn!(
                "ROS_DISTRO environment variable is not set. \
                Assuming '{ASSUMED_ROS_DISTRO}', but this could lead to errors on 'ros_discovery_info' \
                (see https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/issues/21)"
            );
            ASSUMED_ROS_DISTRO.to_string()
        }
        Err(VarError::NotUnicode(s)) => {
            tracing::warn!(
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
        unsafe { keyexpr::from_str_unchecked(&ros2_name[1..]) }.to_owned()
    } else {
        unsafe {
            keyexpr::from_str_unchecked(&config.namespace[1..])
                / keyexpr::from_str_unchecked(&ros2_name[1..])
        }
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

const ATTACHMENT_KEY_REQUEST_HEADER: [u8; 3] = [0x72, 0x71, 0x68]; // "rqh" in ASCII

/// In rmw_cyclonedds_cpp a cdds_request_header sent within each request and reply payload.
/// See https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/serdata.hpp#L73
/// Note that it's different from the rmw_request_id_t defined in RMW interfaces in
/// https://github.com/ros2/rmw/blob/9b3d9d0e3021b7a6e75d8886e3e061a53c36c789/rmw/include/rmw/types.h#L360
#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct CddsRequestHeader {
    // The header contains a u64 GUID (Client's) and a i64 sequence number.
    // Keep those as a single buffer, as it's transfered as such between DDS and Zenoh.
    header: [u8; 16],
    // the sequence number is subject to endianness, we need to keep a flag for it
    is_little_endian: bool,
}

impl CddsRequestHeader {
    pub fn create(client_id: u64, seq_num: u64, is_little_endian: bool) -> CddsRequestHeader {
        let mut header = [0u8; 16];
        if is_little_endian {
            header[..8].copy_from_slice(&client_id.to_le_bytes());
            header[8..].copy_from_slice(&seq_num.to_le_bytes())
        } else {
            header[..8].copy_from_slice(&client_id.to_be_bytes());
            header[8..].copy_from_slice(&seq_num.to_be_bytes())
        }
        CddsRequestHeader {
            header,
            is_little_endian,
        }
    }

    pub fn from_slice(header: [u8; 16], is_little_endian: bool) -> CddsRequestHeader {
        CddsRequestHeader {
            header,
            is_little_endian,
        }
    }

    pub fn is_little_endian(&self) -> bool {
        self.is_little_endian
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.header
    }

    pub fn as_attachment(&self) -> ZBytes {
        // concat header + endianness flag
        let mut buf = [0u8; 17];
        buf[0..16].copy_from_slice(&self.header);
        buf[16] = self.is_little_endian as u8;

        let mut writer = ZBytes::writer();
        writer.append(ZBytes::from(ATTACHMENT_KEY_REQUEST_HEADER));
        writer.append(ZBytes::from(buf));
        writer.finish()
    }
}

impl TryFrom<&ZBytes> for CddsRequestHeader {
    type Error = ZError;

    fn try_from(value: &ZBytes) -> Result<Self, Self::Error> {
        let bytes = value.to_bytes();

        let header = match bytes.get(0..ATTACHMENT_KEY_REQUEST_HEADER.len()) {
            Some(header) => header,
            None => bail!("No 'key request header' bytes found in attachment"),
        };

        if header != ATTACHMENT_KEY_REQUEST_HEADER {
            bail!(
                "Initial {:?} bytes do not match ATTACHMENT_KEY_REQUEST_HEADER",
                ATTACHMENT_KEY_REQUEST_HEADER.len()
            )
        }

        if let Some(buf) = bytes.get(ATTACHMENT_KEY_REQUEST_HEADER.len()..) {
            if buf.len() == 17 {
                let header: [u8; 16] = buf[0..16]
                    .try_into()
                    .expect("Shouldn't happen: buf is 17 bytes");
                Ok(CddsRequestHeader {
                    header,
                    is_little_endian: buf[16] != 0,
                })
            } else {
                bail!("Attachment 'header' is not 16 bytes: {buf:02x?}")
            }
        } else {
            bail!("Could not Read Remaining Attachment Buffer")
        }
    }
}

impl std::fmt::Display for CddsRequestHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // a request header is made of 8 bytes client guid + 8 bytes sequence number
        // display as such for easier understanding
        write!(f, "(")?;
        for i in &self.header[0..8] {
            write!(f, "{i:02x}")?;
        }
        let seq_num = if self.is_little_endian {
            u64::from_le_bytes(
                self.header[8..]
                    .try_into()
                    .expect("Shouldn't happen: self.header is 16 bytes"),
            )
        } else {
            u64::from_be_bytes(
                self.header[8..]
                    .try_into()
                    .expect("Shouldn't happen: self.header is 16 bytes"),
            )
        };
        write!(f, ",{seq_num})",)
    }
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
    let mut qos = Qos {
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
    };
    if !ros_distro_is_less_than("iron") {
        // NOTE: the type hash should be a real one instead of this invalid type hash.
        //       However, `rmw_cyclonedds_cpp` doesn't do any type checking (yet).
        //       And the way to forward the type hash for actions (and services) raise questions
        //       that are described in https://github.com/eclipse-zenoh/zenoh-plugin-ros2dds/pull/349
        insert_type_hash(
            &mut qos,
            "RIHS01_0000000000000000000000000000000000000000000000000000000000000000",
        );
    }
    qos
}

fn ros2_action_status_default_qos() -> Qos {
    // Default Status topic QoS copied from:
    // https://github.com/ros2/rcl/blob/8f7f4f0804a34ee9d9ecd2d7e75a57ce2b7ced5d/rcl_action/include/rcl_action/default_qos.h#L30
    let mut qos = Qos {
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
    };
    if !ros_distro_is_less_than("iron") {
        // add type_hash in USER_DATA QoS
        insert_type_hash(&mut qos, ROS2_ACTION_STATUS_MSG_TYPE_HASH);
    }
    qos
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

// /// For potential use later (type_hash in admin space?)
// pub fn extract_type_hash(qos: &Qos) -> Option<String> {
//     if let Some(v) = &qos.user_data {
//         if let Ok(s) = str::from_utf8(v) {
//             if let Some(mut start) = s.find(USER_DATA_TYPEHASH_KEY) {
//                 start += USER_DATA_TYPEHASH_KEY.len();
//                 if let Some(end) = s[start..].find(USER_DATA_PROPS_SEPARATOR) {
//                     return Some(s[start..(start + end)].into());
//                 }
//             }
//         }
//     }
//     None
// }

pub fn insert_type_hash(qos: &mut Qos, type_hash: &str) {
    let mut s = USER_DATA_TYPEHASH_KEY.to_string();
    s.push_str(type_hash);
    s.push(USER_DATA_PROPS_SEPARATOR);
    match qos.user_data {
        Some(ref mut v) => v.extend(s.into_bytes().iter()),
        None => qos.user_data = Some(s.into_bytes()),
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
