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
use regex::Regex;
use serde::{de, de::Visitor, ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};
use std::env;
use std::fmt;
use std::time::Duration;
use zenoh::prelude::*;

pub const DEFAULT_NAMESPACE: &str = "/";
pub const DEFAULT_NODENAME: &str = "zenoh_bridge_ros2dds";
pub const DEFAULT_DOMAIN: u32 = 0;
pub const DEFAULT_RELIABLE_ROUTES_BLOCKING: bool = true;
pub const DEFAULT_TRANSIENT_LOCAL_CACHE_MULTIPLIER: usize = 10;
pub const DEFAULT_DDS_LOCALHOST_ONLY: bool = false;
pub const DEFAULT_QUERIES_TIMEOUT: f32 = 5.0;

#[derive(Deserialize, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub id: Option<OwnedKeyExpr>,
    #[serde(default = "default_namespace")]
    pub namespace: String,
    #[serde(default = "default_nodename")]
    pub nodename: OwnedKeyExpr,
    #[serde(default = "default_domain")]
    pub domain: u32,
    #[serde(default = "default_localhost_only")]
    pub ros_localhost_only: bool,
    #[serde(default, flatten)]
    pub allowance: Option<Allowance>,
    #[serde(
        default,
        deserialize_with = "deserialize_vec_regex_f32",
        serialize_with = "serialize_vec_regex_f32"
    )]
    pub pub_max_frequencies: Vec<(Regex, f32)>,
    #[serde(default)]
    #[cfg(feature = "dds_shm")]
    pub shm_enabled: bool,
    #[serde(default = "default_transient_local_cache_multiplier")]
    pub transient_local_cache_multiplier: usize,
    #[serde(default)]
    pub queries_timeout: Option<QueriesTimeouts>,
    #[serde(default = "default_reliable_routes_blocking")]
    pub reliable_routes_blocking: bool,
    #[serde(
        default,
        deserialize_with = "deserialize_vec_regex_prio",
        serialize_with = "serialize_vec_regex_prio"
    )]
    pub pub_priorities: Vec<(Regex, Priority)>,
    __required__: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_path")]
    __path__: Option<Vec<String>>,
}

impl Config {
    pub fn get_pub_max_frequencies(&self, ros2_name: &str) -> Option<f32> {
        for (re, freq) in &self.pub_max_frequencies {
            if re.is_match(ros2_name) {
                return Some(*freq);
            }
        }
        None
    }

    pub fn get_pub_priorities(&self, ros2_name: &str) -> Option<Priority> {
        for (re, p) in &self.pub_priorities {
            if re.is_match(ros2_name) {
                return Some(*p);
            }
        }
        None
    }

    pub fn get_queries_timeout_tl_sub(&self, ros2_name: &str) -> Duration {
        if let Some(qt) = &self.queries_timeout {
            for (re, secs) in &qt.transient_local_subscribers {
                if re.is_match(ros2_name) {
                    return Duration::from_secs_f32(*secs);
                }
            }
            return Duration::from_secs_f32(qt.default);
        }
        Duration::from_secs_f32(DEFAULT_QUERIES_TIMEOUT)
    }

    pub fn get_queries_timeout_service(&self, ros2_name: &str) -> Duration {
        if let Some(qt) = &self.queries_timeout {
            for (re, secs) in &qt.services {
                if re.is_match(ros2_name) {
                    return Duration::from_secs_f32(*secs);
                }
            }
            return Duration::from_secs_f32(qt.default);
        }
        Duration::from_secs_f32(DEFAULT_QUERIES_TIMEOUT)
    }

    pub fn get_queries_timeout_action_send_goal(&self, ros2_name: &str) -> Duration {
        match &self.queries_timeout {
            Some(QueriesTimeouts {
                default,
                actions: Some(at),
                ..
            }) => {
                for (re, secs) in &at.send_goal {
                    if re.is_match(ros2_name) {
                        return Duration::from_secs_f32(*secs);
                    }
                }
                Duration::from_secs_f32(*default)
            }
            Some(QueriesTimeouts {
                default,
                actions: None,
                ..
            }) => Duration::from_secs_f32(*default),
            _ => Duration::from_secs_f32(DEFAULT_QUERIES_TIMEOUT),
        }
    }

    pub fn get_queries_timeout_action_cancel_goal(&self, ros2_name: &str) -> Duration {
        match &self.queries_timeout {
            Some(QueriesTimeouts {
                default,
                actions: Some(at),
                ..
            }) => {
                for (re, secs) in &at.cancel_goal {
                    if re.is_match(ros2_name) {
                        return Duration::from_secs_f32(*secs);
                    }
                }
                Duration::from_secs_f32(*default)
            }
            Some(QueriesTimeouts {
                default,
                actions: None,
                ..
            }) => Duration::from_secs_f32(*default),
            _ => Duration::from_secs_f32(DEFAULT_QUERIES_TIMEOUT),
        }
    }

    pub fn get_queries_timeout_action_get_result(&self, ros2_name: &str) -> Duration {
        match &self.queries_timeout {
            Some(QueriesTimeouts {
                default,
                actions: Some(at),
                ..
            }) => {
                for (re, secs) in &at.get_result {
                    if re.is_match(ros2_name) {
                        return Duration::from_secs_f32(*secs);
                    }
                }
                Duration::from_secs_f32(*default)
            }
            Some(QueriesTimeouts {
                default,
                actions: None,
                ..
            }) => Duration::from_secs_f32(*default),
            _ => Duration::from_secs_f32(DEFAULT_QUERIES_TIMEOUT),
        }
    }
}

#[derive(Deserialize, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueriesTimeouts {
    #[serde(default = "default_queries_timeout")]
    default: f32,
    #[serde(
        default,
        deserialize_with = "deserialize_vec_regex_f32",
        serialize_with = "serialize_vec_regex_f32"
    )]
    transient_local_subscribers: Vec<(Regex, f32)>,
    #[serde(
        default,
        deserialize_with = "deserialize_vec_regex_f32",
        serialize_with = "serialize_vec_regex_f32"
    )]
    services: Vec<(Regex, f32)>,
    #[serde(default)]
    actions: Option<ActionsTimeouts>,
}

#[derive(Deserialize, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActionsTimeouts {
    #[serde(
        default,
        deserialize_with = "deserialize_vec_regex_f32",
        serialize_with = "serialize_vec_regex_f32"
    )]
    send_goal: Vec<(Regex, f32)>,
    #[serde(
        default,
        deserialize_with = "deserialize_vec_regex_f32",
        serialize_with = "serialize_vec_regex_f32"
    )]
    cancel_goal: Vec<(Regex, f32)>,
    #[serde(
        default,
        deserialize_with = "deserialize_vec_regex_f32",
        serialize_with = "serialize_vec_regex_f32"
    )]
    get_result: Vec<(Regex, f32)>,
}

#[derive(Deserialize, Debug, Serialize)]
pub enum Allowance {
    #[serde(rename = "allow")]
    Allow(ROS2InterfacesRegex),
    #[serde(rename = "deny")]
    Deny(ROS2InterfacesRegex),
}

impl Allowance {
    pub fn is_publisher_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .publishers
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .publishers
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }

    pub fn is_subscriber_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .subscribers
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .subscribers
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }

    pub fn is_service_srv_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .service_servers
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .service_servers
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }

    pub fn is_service_cli_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .service_clients
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .service_clients
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }

    pub fn is_action_srv_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .action_servers
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .action_servers
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }

    pub fn is_action_cli_allowed(&self, name: &str) -> bool {
        use Allowance::*;
        match self {
            Allow(r) => r
                .action_clients
                .as_ref()
                .map(|re| re.is_match(name))
                .unwrap_or(false),
            Deny(r) => r
                .action_clients
                .as_ref()
                .map(|re| !re.is_match(name))
                .unwrap_or(true),
        }
    }
}

#[derive(Deserialize, Debug, Default, Serialize)]
pub struct ROS2InterfacesRegex {
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        serialize_with = "serialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub publishers: Option<Regex>,
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        serialize_with = "serialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub subscribers: Option<Regex>,
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        serialize_with = "serialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub service_servers: Option<Regex>,
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        serialize_with = "serialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub service_clients: Option<Regex>,
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        serialize_with = "serialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub action_servers: Option<Regex>,
    #[serde(
        default,
        deserialize_with = "deserialize_regex",
        serialize_with = "serialize_regex",
        skip_serializing_if = "Option::is_none"
    )]
    pub action_clients: Option<Regex>,
}

fn default_namespace() -> String {
    DEFAULT_NAMESPACE.to_string()
}

fn default_nodename() -> OwnedKeyExpr {
    unsafe { OwnedKeyExpr::from_string_unchecked(DEFAULT_NODENAME.into()) }
}

fn default_domain() -> u32 {
    if let Ok(s) = env::var("ROS_DOMAIN_ID") {
        s.parse::<u32>().unwrap_or(DEFAULT_DOMAIN)
    } else {
        DEFAULT_DOMAIN
    }
}

fn default_queries_timeout() -> f32 {
    DEFAULT_QUERIES_TIMEOUT
}

fn deserialize_path<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_option(OptPathVisitor)
}

struct OptPathVisitor;

impl<'de> serde::de::Visitor<'de> for OptPathVisitor {
    type Value = Option<Vec<String>>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "none or a string or an array of strings")
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(None)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(PathVisitor).map(Some)
    }
}

struct PathVisitor;

impl<'de> serde::de::Visitor<'de> for PathVisitor {
    type Value = Vec<String>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a string or an array of strings")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(vec![v.into()])
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut v = if let Some(l) = seq.size_hint() {
            Vec::with_capacity(l)
        } else {
            Vec::new()
        };
        while let Some(s) = seq.next_element()? {
            v.push(s);
        }
        Ok(v)
    }
}

fn default_reliable_routes_blocking() -> bool {
    DEFAULT_RELIABLE_ROUTES_BLOCKING
}

fn default_localhost_only() -> bool {
    env::var("ROS_LOCALHOST_ONLY").as_deref() == Ok("1")
}

fn default_transient_local_cache_multiplier() -> usize {
    DEFAULT_TRANSIENT_LOCAL_CACHE_MULTIPLIER
}

fn serialize_regex<S>(r: &Option<Regex>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match r {
        Some(ex) => serializer.serialize_some(ex.as_str()),
        None => serializer.serialize_none(),
    }
}

fn deserialize_regex<'de, D>(deserializer: D) -> Result<Option<Regex>, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(RegexVisitor)
}

// Serde Visitor for Regex deserialization.
// It accepts either a String, either a list of Strings (that are concatenated with `|`)
struct RegexVisitor;

impl<'de> Visitor<'de> for RegexVisitor {
    type Value = Option<Regex>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(r#"either a string or a list of strings"#)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Regex::new(&format!("^{value}$"))
            .map(Some)
            .map_err(|e| de::Error::custom(format!("Invalid regex '{value}': {e}")))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut vec: Vec<String> = Vec::new();
        while let Some(s) = seq.next_element::<String>()? {
            vec.push(format!("^{s}$"));
        }
        if vec.is_empty() {
            return Ok(None);
        };

        let s: String = vec.join("|");
        Regex::new(&s)
            .map(Some)
            .map_err(|e| de::Error::custom(format!("Invalid regex '{s}': {e}")))
    }
}

fn deserialize_vec_regex_f32<'de, D>(deserializer: D) -> Result<Vec<(Regex, f32)>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum AcceptedValues {
        Float(f32),
        List(Vec<String>),
    }

    let values: AcceptedValues = Deserialize::deserialize(deserializer).unwrap();
    match values {
        AcceptedValues::Float(f) => {
            // same float for any string (i.e. matching ".*")
            Ok(vec![(Regex::new(".*").unwrap(), f)])
        }
        AcceptedValues::List(strs) => {
            let mut result: Vec<(Regex, f32)> = Vec::with_capacity(strs.len());
            for s in strs {
                let i = s.find('=').ok_or_else(|| {
                    de::Error::custom(format!(
                        r#"Invalid list of "<regex>=<float>" elements": {s}"#
                    ))
                })?;
                let regex = Regex::new(&s[0..i])
                    .map_err(|e| de::Error::custom(format!("Invalid regex in '{s}': {e}")))?;
                let frequency: f32 = s[i + 1..]
                    .parse()
                    .map_err(|e| de::Error::custom(format!("Invalid float value in '{s}': {e}")))?;
                result.push((regex, frequency));
            }
            Ok(result)
        }
    }
}

fn serialize_vec_regex_f32<S>(v: &Vec<(Regex, f32)>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(v.len()))?;
    for (r, f) in v {
        let s = format!("{}={}", r.as_str(), f);
        seq.serialize_element(&s)?;
    }
    seq.end()
}

fn deserialize_vec_regex_prio<'de, D>(deserializer: D) -> Result<Vec<(Regex, Priority)>, D::Error>
where
    D: Deserializer<'de>,
{
    let strs: Vec<String> = Deserialize::deserialize(deserializer).unwrap();
    let mut result: Vec<(Regex, Priority)> = Vec::with_capacity(strs.len());
    for s in strs {
        let i = s.find('=').ok_or_else(|| {
            de::Error::custom(format!(r#"Invalid list of "<regex>=<int>" elements": {s}"#))
        })?;
        let regex = Regex::new(&s[0..i])
            .map_err(|e| de::Error::custom(format!("Invalid regex in '{s}': {e}")))?;
        let i: u8 = s[i + 1..].parse().map_err(|e| {
            de::Error::custom(format!("Invalid priority (not an integer) in '{s}': {e}"))
        })?;
        let priority = Priority::try_from(i)
            .map_err(|e| de::Error::custom(format!("Invalid priority in '{s}': {e}")))?;
        result.push((regex, priority));
    }
    Ok(result)
}

fn serialize_vec_regex_prio<S>(v: &Vec<(Regex, Priority)>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(v.len()))?;
    for (r, p) in v {
        let s = format!("{}={}", r.as_str(), *p as u8);
        seq.serialize_element(&s)?;
    }
    seq.end()
}

pub fn serialize_duration_as_f32<S>(d: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_f32(d.as_secs_f32())
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn test_allowance() {
        use super::*;

        let allow: Allowance = serde_json::from_str(
            r#"{
                "allow": {
                    "publishers": ["/tf", ".*/pose"],
                    "subscribers": [],
                    "service_servers": [".*"],
                    "action_servers": [".*/rotate_absolute"],
                    "action_clients": [ "" ]
                }
            }"#,
        )
        .unwrap();
        println!("allow: {}", serde_json::to_string(&allow).unwrap());

        assert!(matches!(
            allow,
            Allowance::Allow(ROS2InterfacesRegex {
                publishers: Some(_),
                subscribers: None,
                service_servers: Some(_),
                service_clients: None,
                action_servers: Some(_),
                action_clients: Some(_),
            })
        ));

        assert!(allow.is_publisher_allowed("/tf"));
        assert!(allow.is_publisher_allowed("/x/y/pose"));
        assert!(!allow.is_publisher_allowed("/abc/rotate_absolute"));
        assert!(!allow.is_publisher_allowed("/cmd_vel"));
        assert!(!allow.is_service_cli_allowed("/some_pseudo_random_name"));

        assert!(!allow.is_subscriber_allowed("/tf"));
        assert!(!allow.is_subscriber_allowed("/x/y/pose"));
        assert!(!allow.is_publisher_allowed("/abc/rotate_absolute"));
        assert!(!allow.is_subscriber_allowed("/cmd_vel"));
        assert!(!allow.is_service_cli_allowed("/some_pseudo_random_name"));

        assert!(allow.is_service_srv_allowed("/tf"));
        assert!(allow.is_service_srv_allowed("/x/y/pose"));
        assert!(allow.is_service_srv_allowed("/abc/rotate_absolute"));
        assert!(allow.is_service_srv_allowed("/cmd_vel"));
        assert!(allow.is_service_srv_allowed("/some_pseudo_random_name"));

        assert!(!allow.is_service_cli_allowed("/tf"));
        assert!(!allow.is_service_cli_allowed("/x/y/pose"));
        assert!(!allow.is_service_cli_allowed("/abc/rotate_absolute"));
        assert!(!allow.is_service_cli_allowed("/cmd_vel"));
        assert!(!allow.is_service_cli_allowed("/some_pseudo_random_name"));

        assert!(!allow.is_action_srv_allowed("/tf"));
        assert!(!allow.is_action_srv_allowed("/x/y/pose"));
        assert!(allow.is_action_srv_allowed("/abc/rotate_absolute"));
        assert!(!allow.is_action_srv_allowed("/cmd_vel"));
        assert!(!allow.is_action_srv_allowed("/some_pseudo_random_name"));

        assert!(!allow.is_action_cli_allowed("/tf"));
        assert!(!allow.is_action_cli_allowed("/x/y/pose"));
        assert!(!allow.is_action_cli_allowed("/abc/rotate_absolute"));
        assert!(!allow.is_action_cli_allowed("/cmd_vel"));
        assert!(!allow.is_action_cli_allowed("/some_pseudo_random_name"));

        let deny: Allowance = serde_json::from_str(
            r#"{
                "deny": {
                    "publishers": ["/tf", ".*/pose"],
                    "subscribers": [],
                    "service_servers": [".*"],
                    "action_servers": [".*/rotate_absolute"],
                    "action_clients": [ "" ]
                }
            }"#,
        )
        .unwrap();
        println!("deny: {}", serde_json::to_string(&allow).unwrap());

        assert!(matches!(
            deny,
            Allowance::Deny(ROS2InterfacesRegex {
                publishers: Some(_),
                subscribers: None,
                service_servers: Some(_),
                service_clients: None,
                action_servers: Some(_),
                action_clients: Some(_),
            })
        ));

        assert!(!deny.is_publisher_allowed("/tf"));
        assert!(!deny.is_publisher_allowed("/x/y/pose"));
        assert!(deny.is_publisher_allowed("/abc/rotate_absolute"));
        assert!(deny.is_publisher_allowed("/cmd_vel"));
        assert!(deny.is_service_cli_allowed("/some_pseudo_random_name"));

        assert!(deny.is_subscriber_allowed("/tf"));
        assert!(deny.is_subscriber_allowed("/x/y/pose"));
        assert!(deny.is_publisher_allowed("/abc/rotate_absolute"));
        assert!(deny.is_subscriber_allowed("/cmd_vel"));
        assert!(deny.is_service_cli_allowed("/some_pseudo_random_name"));

        assert!(!deny.is_service_srv_allowed("/tf"));
        assert!(!deny.is_service_srv_allowed("/x/y/pose"));
        assert!(!deny.is_service_srv_allowed("/abc/rotate_absolute"));
        assert!(!deny.is_service_srv_allowed("/cmd_vel"));
        assert!(!deny.is_service_srv_allowed("/some_pseudo_random_name"));

        assert!(deny.is_service_cli_allowed("/tf"));
        assert!(deny.is_service_cli_allowed("/x/y/pose"));
        assert!(deny.is_service_cli_allowed("/abc/rotate_absolute"));
        assert!(deny.is_service_cli_allowed("/cmd_vel"));
        assert!(deny.is_service_cli_allowed("/some_pseudo_random_name"));

        assert!(deny.is_action_srv_allowed("/tf"));
        assert!(deny.is_action_srv_allowed("/x/y/pose"));
        assert!(!deny.is_action_srv_allowed("/abc/rotate_absolute"));
        assert!(deny.is_action_srv_allowed("/cmd_vel"));
        assert!(deny.is_action_srv_allowed("/some_pseudo_random_name"));

        assert!(deny.is_action_cli_allowed("/tf"));
        assert!(deny.is_action_cli_allowed("/x/y/pose"));
        assert!(deny.is_action_cli_allowed("/abc/rotate_absolute"));
        assert!(deny.is_action_cli_allowed("/cmd_vel"));
        assert!(deny.is_action_cli_allowed("/some_pseudo_random_name"));

        let invalid = serde_json::from_str::<Allowance>(
            r#"{
                "allow": {
                    "publishers": ["/tf", ".*/pose"],
                    "subscribers": [],
                    "service_servers": [".*"],
                    "action_servers": [".*/rotate_absolute"],
                    "action_clients": [ "" ]
                },
                "deny": {
                    "subscribers": ["/tf", ".*/pose"],
                    "service_clients": [".*"],
                    "action_servers": [""],
                    "action_clients": [ ".*/rotate_absolute" ]
                },
            }"#,
        );
        assert!(invalid.is_err());
    }

    #[test]
    fn test_path_field() {
        // See: https://github.com/eclipse-zenoh/zenoh-plugin-webserver/issues/19
        let config = serde_json::from_str::<Config>(r#"{"__path__": "/example/path"}"#);

        assert!(config.is_ok());
        let Config {
            __required__,
            __path__,
            ..
        } = config.unwrap();

        assert_eq!(__path__, Some(vec![String::from("/example/path")]));
        assert_eq!(__required__, None);
    }

    #[test]
    fn test_required_field() {
        // See: https://github.com/eclipse-zenoh/zenoh-plugin-webserver/issues/19
        let config = serde_json::from_str::<Config>(r#"{"__required__": true}"#);
        assert!(config.is_ok());
        let Config {
            __required__,
            __path__,
            ..
        } = config.unwrap();

        assert_eq!(__path__, None);
        assert_eq!(__required__, Some(true));
    }

    #[test]
    fn test_path_field_and_required_field() {
        // See: https://github.com/eclipse-zenoh/zenoh-plugin-webserver/issues/19
        let config = serde_json::from_str::<Config>(
            r#"{"__path__": "/example/path", "__required__": true}"#,
        );

        assert!(config.is_ok());
        let Config {
            __required__,
            __path__,
            ..
        } = config.unwrap();

        assert_eq!(__path__, Some(vec![String::from("/example/path")]));
        assert_eq!(__required__, Some(true));
    }

    #[test]
    fn test_no_path_field_and_no_required_field() {
        // See: https://github.com/eclipse-zenoh/zenoh-plugin-webserver/issues/19
        let config = serde_json::from_str::<Config>("{}");

        assert!(config.is_ok());
        let Config {
            __required__,
            __path__,
            ..
        } = config.unwrap();

        assert_eq!(__path__, None);
        assert_eq!(__required__, None);
    }
}
