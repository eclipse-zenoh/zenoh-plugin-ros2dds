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

use cyclors::qos::{
    Durability, DurabilityKind, History, HistoryKind, Qos, Reliability, ReliabilityKind,
    DDS_100MS_DURATION,
};
use zenoh::prelude::{keyexpr, OwnedKeyExpr};

const SLASH_REPLACEMSNT_CHAR: &str = "§";

zenoh::kedefine!(
    // Liveliness tokens key expressions
    pub ke_liveliness_all: "@ros2_lv/${plugin_id:*}/${remaining:**}",
    pub ke_liveliness_plugin: "@ros2_lv/${plugin_id:*}",
    pub(crate) ke_liveliness_pub: "@ros2_lv/${plugin_id:*}/MP/${ke:*}/${typ:*}/${qos_ke:*}",
    pub(crate) ke_liveliness_sub: "@ros2_lv/${plugin_id:*}/MS/${ke:*}/${typ:*}/${qos_ke:*}",
    pub(crate) ke_liveliness_service_srv: "@ros2_lv/${plugin_id:*}/SS/${ke:*}/${typ:*}",
    pub(crate) ke_liveliness_service_cli: "@ros2_lv/${plugin_id:*}/SC/${ke:*}/${typ:*}",
    pub(crate) ke_liveliness_action_srv: "@ros2_lv/${plugin_id:*}/AS/${ke:*}/${typ:*}",
    pub(crate) ke_liveliness_action_cli: "@ros2_lv/${plugin_id:*}/AC/${ke:*}/${typ:*}",
);

pub(crate) fn new_ke_liveliness_pub(
    plugin_id: &keyexpr,
    zenoh_key_expr: &keyexpr,
    ros2_type: &str,
    keyless: bool,
    qos: &Qos,
) -> Result<OwnedKeyExpr, String> {
    let ke = escape_slashes(zenoh_key_expr);
    let typ = escape_slashes(ros2_type);
    let qos_ke = qos_to_key_expr(keyless, qos);
    zenoh::keformat!(ke_liveliness_pub::formatter(), plugin_id, ke, typ, qos_ke)
        .map_err(|e| e.to_string())
}

pub(crate) fn parse_ke_liveliness_pub(
    ke: &keyexpr,
) -> Result<(OwnedKeyExpr, OwnedKeyExpr, String, bool, Qos), String> {
    let parsed = ke_liveliness_pub::parse(ke)
        .map_err(|e| format!("failed to parse liveliness keyexpr {ke}: {e}"))?;
    let plugin_id = parsed.plugin_id().to_owned();
    let zenoh_key_expr = unescape_slashes(parsed.ke());
    let ros2_type = unescape_slashes(parsed.typ());
    let (keyless, qos) = key_expr_to_qos(parsed.qos_ke())
        .map_err(|e| format!("failed to parse liveliness keyexpr {ke}: {e}"))?;
    Ok((
        plugin_id,
        zenoh_key_expr,
        ros2_type.to_string(),
        keyless,
        qos,
    ))
}

pub(crate) fn new_ke_liveliness_sub(
    plugin_id: &keyexpr,
    zenoh_key_expr: &keyexpr,
    ros2_type: &str,
    keyless: bool,
    qos: &Qos,
) -> Result<OwnedKeyExpr, String> {
    let ke = escape_slashes(zenoh_key_expr);
    let typ = escape_slashes(ros2_type);
    let qos_ke = qos_to_key_expr(keyless, qos);
    zenoh::keformat!(ke_liveliness_sub::formatter(), plugin_id, ke, typ, qos_ke)
        .map_err(|e| e.to_string())
}

pub(crate) fn parse_ke_liveliness_sub(
    ke: &keyexpr,
) -> Result<(OwnedKeyExpr, OwnedKeyExpr, String, bool, Qos), String> {
    let parsed = ke_liveliness_sub::parse(ke)
        .map_err(|e| format!("failed to parse liveliness keyexpr {ke}: {e}"))?;
    let plugin_id = parsed.plugin_id().to_owned();
    let zenoh_key_expr = unescape_slashes(parsed.ke());
    let ros2_type = unescape_slashes(parsed.typ());
    let (keyless, qos) = key_expr_to_qos(parsed.qos_ke())
        .map_err(|e| format!("failed to parse liveliness keyexpr {ke}: {e}"))?;
    Ok((
        plugin_id,
        zenoh_key_expr,
        ros2_type.to_string(),
        keyless,
        qos,
    ))
}

pub(crate) fn new_ke_liveliness_service_srv(
    plugin_id: &keyexpr,
    zenoh_key_expr: &keyexpr,
    ros2_type: &str,
) -> Result<OwnedKeyExpr, String> {
    let ke = escape_slashes(zenoh_key_expr);
    let typ = escape_slashes(ros2_type);
    zenoh::keformat!(ke_liveliness_service_srv::formatter(), plugin_id, ke, typ)
        .map_err(|e| e.to_string())
}

pub(crate) fn parse_ke_liveliness_service_srv(
    ke: &keyexpr,
) -> Result<(OwnedKeyExpr, OwnedKeyExpr, String), String> {
    let parsed = ke_liveliness_service_srv::parse(ke)
        .map_err(|e| format!("failed to parse liveliness keyexpr {ke}: {e}"))?;
    let plugin_id = parsed.plugin_id().to_owned();
    let zenoh_key_expr = unescape_slashes(parsed.ke());
    let ros2_type = unescape_slashes(parsed.typ());
    Ok((plugin_id, zenoh_key_expr, ros2_type.to_string()))
}

pub(crate) fn new_ke_liveliness_service_cli(
    plugin_id: &keyexpr,
    zenoh_key_expr: &keyexpr,
    ros2_type: &str,
) -> Result<OwnedKeyExpr, String> {
    let ke = escape_slashes(zenoh_key_expr);
    let typ = escape_slashes(ros2_type);
    zenoh::keformat!(ke_liveliness_service_cli::formatter(), plugin_id, ke, typ)
        .map_err(|e| e.to_string())
}

pub(crate) fn parse_ke_liveliness_service_cli(
    ke: &keyexpr,
) -> Result<(OwnedKeyExpr, OwnedKeyExpr, String), String> {
    let parsed = ke_liveliness_service_cli::parse(ke)
        .map_err(|e| format!("failed to parse liveliness keyexpr {ke}: {e}"))?;
    let plugin_id = parsed.plugin_id().to_owned();
    let zenoh_key_expr = unescape_slashes(parsed.ke());
    let ros2_type = unescape_slashes(parsed.typ());
    Ok((plugin_id, zenoh_key_expr, ros2_type.to_string()))
}

pub(crate) fn new_ke_liveliness_action_srv(
    plugin_id: &keyexpr,
    zenoh_key_expr: &keyexpr,
    ros2_type: &str,
) -> Result<OwnedKeyExpr, String> {
    let ke = escape_slashes(zenoh_key_expr);
    let typ = escape_slashes(ros2_type);
    zenoh::keformat!(ke_liveliness_action_srv::formatter(), plugin_id, ke, typ)
        .map_err(|e| e.to_string())
}

pub(crate) fn parse_ke_liveliness_action_srv(
    ke: &keyexpr,
) -> Result<(OwnedKeyExpr, OwnedKeyExpr, String), String> {
    let parsed = ke_liveliness_action_srv::parse(ke)
        .map_err(|e| format!("failed to parse liveliness keyexpr {ke}: {e}"))?;
    let plugin_id = parsed.plugin_id().to_owned();
    let zenoh_key_expr = unescape_slashes(parsed.ke());
    let ros2_type = unescape_slashes(parsed.typ());
    Ok((plugin_id, zenoh_key_expr, ros2_type.to_string()))
}

pub(crate) fn new_ke_liveliness_action_cli(
    plugin_id: &keyexpr,
    zenoh_key_expr: &keyexpr,
    ros2_type: &str,
) -> Result<OwnedKeyExpr, String> {
    let ke = escape_slashes(zenoh_key_expr);
    let typ = escape_slashes(ros2_type);
    zenoh::keformat!(ke_liveliness_action_cli::formatter(), plugin_id, ke, typ)
        .map_err(|e| e.to_string())
}

pub(crate) fn parse_ke_liveliness_action_cli(
    ke: &keyexpr,
) -> Result<(OwnedKeyExpr, OwnedKeyExpr, String), String> {
    let parsed = ke_liveliness_action_cli::parse(ke)
        .map_err(|e| format!("failed to parse liveliness keyexpr {ke}: {e}"))?;
    let plugin_id = parsed.plugin_id().to_owned();
    let zenoh_key_expr = unescape_slashes(parsed.ke());
    let ros2_type = unescape_slashes(parsed.typ());
    Ok((plugin_id, zenoh_key_expr, ros2_type.to_string()))
}

fn escape_slashes(s: &str) -> OwnedKeyExpr {
    OwnedKeyExpr::try_from(s.replace('/', SLASH_REPLACEMSNT_CHAR)).unwrap()
}

fn unescape_slashes(ke: &keyexpr) -> OwnedKeyExpr {
    OwnedKeyExpr::try_from(ke.as_str().replace(SLASH_REPLACEMSNT_CHAR, "/")).unwrap()
}

// Serialize QoS as a KeyExpr-compatible string (for usage in liveliness keyexpr)
// NOTE: only significant Qos for ROS2 are serialized
// See https://docs.ros.org/en/rolling/Concepts/Intermediate/About-Quality-of-Service-Settings.html
//
// format: "<keyless>:<ReliabilityKind>:<DurabilityKind>:<HistoryKid>,<HistoryDepth>"
// where each element is "" if default QoS, or an integer in case of enum, and 'K' for !keyless
pub fn qos_to_key_expr(keyless: bool, qos: &Qos) -> OwnedKeyExpr {
    use std::io::Write;
    let mut w: Vec<u8> = Vec::new();

    if !keyless {
        write!(w, "K").unwrap();
    }
    write!(w, ":").unwrap();
    if let Some(Reliability { kind, .. }) = &qos.reliability {
        write!(&mut w, "{}", *kind as isize).unwrap();
    }
    write!(w, ":").unwrap();
    if let Some(Durability { kind }) = &qos.durability {
        write!(&mut w, "{}", *kind as isize).unwrap();
    }
    write!(w, ":").unwrap();
    if let Some(History { kind, depth }) = &qos.history {
        write!(&mut w, "{},{}", *kind as isize, depth).unwrap();
    }

    unsafe {
        let s: String = String::from_utf8_unchecked(w);
        OwnedKeyExpr::from_string_unchecked(s)
    }
}

fn key_expr_to_qos(ke: &keyexpr) -> Result<(bool, Qos), String> {
    let elts: Vec<&str> = ke.split(':').collect();
    if elts.len() != 4 {
        return Err(format!("Internal Error: unexpected QoS expression: '{ke}' - 4 elements between : were expected"));
    }
    let mut qos = Qos::default();
    let keyless = elts[0].is_empty();
    if !elts[1].is_empty() {
        match elts[1].parse::<cyclors::dds_reliability_kind_t>() {
            Ok(i) => qos.reliability = Some(Reliability {kind: ReliabilityKind::from(&i), max_blocking_time: DDS_100MS_DURATION }),
            Err(_) => return Err(format!("Internal Error: unexpected QoS expression: '{ke}' - failed to parse Reliability in 2nd element")),
        }
    }
    if !elts[2].is_empty() {
        match elts[2].parse::<cyclors::dds_durability_kind_t>() {
            Ok(i) => qos.durability = Some(Durability {kind: DurabilityKind::from(&i)}),
            Err(_) => return Err(format!("Internal Error: unexpected QoS expression: '{ke}' - failed to parse Durability in 3d element")),
        }
    }
    if !elts[3].is_empty() {
        match elts[3].split_once(',').map(|(s1, s2)|
            (
                s1.parse::<cyclors::dds_history_kind_t>(),
                s2.parse::<i32>(),
            )
        ) {
            Some((Ok(k), Ok(depth))) => qos.history = Some(History {kind: HistoryKind::from(&k), depth }),
            _ => return Err(format!("Internal Error: unexpected QoS expression: '{ke}' - failed to parse History in 4th element")),
        }
    }

    Ok((keyless, qos))
}

mod tests {
    #[test]
    fn test_qos_key_expr() {
        use super::*;

        let mut q = Qos::default();
        assert_eq!(qos_to_key_expr(true, &q).to_string(), ":::");
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(true, &q)),
            Ok((true, q.clone()))
        );
        assert_eq!(qos_to_key_expr(false, &q).to_string(), "K:::");
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(false, &q)),
            Ok((false, q.clone()))
        );

        q.reliability = Some(Reliability {
            kind: ReliabilityKind::RELIABLE,
            max_blocking_time: DDS_100MS_DURATION,
        });
        assert_eq!(
            qos_to_key_expr(true, &q).to_string(),
            format!(":{}::", ReliabilityKind::RELIABLE as u8)
        );
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(true, &q)),
            Ok((true, q.clone()))
        );
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(true, &q)),
            Ok((true, q.clone()))
        );
        q.reliability = None;

        q.durability = Some(Durability {
            kind: DurabilityKind::TRANSIENT_LOCAL,
        });
        assert_eq!(
            qos_to_key_expr(true, &q).to_string(),
            format!("::{}:", DurabilityKind::TRANSIENT_LOCAL as u8)
        );
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(true, &q)),
            Ok((true, q.clone()))
        );
        q.durability = None;

        q.history = Some(History {
            kind: HistoryKind::KEEP_LAST,
            depth: 3,
        });
        assert_eq!(
            qos_to_key_expr(true, &q).to_string(),
            format!(":::{},3", HistoryKind::KEEP_LAST as u8)
        );
        assert_eq!(
            key_expr_to_qos(&qos_to_key_expr(true, &q)),
            Ok((true, q.clone()))
        );
        q.reliability = None;
    }
}
