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
use std::{collections::HashSet, fmt};

use serde::Serialize;
use zenoh::{
    key_expr::{keyexpr, OwnedKeyExpr},
    liveliness::LivelinessToken,
};

use crate::{
    liveliness_mgt::new_ke_liveliness_action_cli, ros2_utils::*,
    route_action_srv::serialize_action_zenoh_key_expr, route_service_cli::RouteServiceCli,
    route_subscriber::RouteSubscriber, routes_mgr::Context,
};

#[derive(Serialize)]
pub struct RouteActionCli {
    // the ROS2 Action name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression prefix used for services/messages routing
    #[serde(
        rename = "zenoh_key_expr",
        serialize_with = "serialize_action_zenoh_key_expr"
    )]
    zenoh_key_expr_prefix: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    is_active: bool,
    #[serde(skip)]
    route_send_goal: RouteServiceCli,
    #[serde(skip)]
    route_cancel_goal: RouteServiceCli,
    #[serde(skip)]
    route_get_result: RouteServiceCli,
    #[serde(skip)]
    route_feedback: RouteSubscriber,
    #[serde(skip)]
    route_status: RouteSubscriber,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken>,
    // the list of remote routes served by this route ("<zenoh_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route
    local_nodes: HashSet<String>,
}

impl fmt::Display for RouteActionCli {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Action Client (ROS:{} <-> Zenoh:{}/*)",
            self.ros2_name, self.zenoh_key_expr_prefix
        )
    }
}

impl RouteActionCli {
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr_prefix: OwnedKeyExpr,
        context: Context,
    ) -> Result<RouteActionCli, String> {
        // configured queries timeout for calls to send_goal service
        let send_goal_queries_timeout = context
            .config
            .get_queries_timeout_action_send_goal(&ros2_name);
        let route_send_goal = RouteServiceCli::create(
            format!("{ros2_name}/{}", *KE_SUFFIX_ACTION_SEND_GOAL),
            format!("{ros2_type}_SendGoal"),
            &zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_SEND_GOAL,
            None,
            send_goal_queries_timeout,
            context.clone(),
        )
        .await?;

        // configured queries timeout for calls to cancel_goal service
        let cancel_goal_queries_timeout = context
            .config
            .get_queries_timeout_action_cancel_goal(&ros2_name);
        let route_cancel_goal = RouteServiceCli::create(
            format!("{ros2_name}/{}", *KE_SUFFIX_ACTION_CANCEL_GOAL),
            ROS2_ACTION_CANCEL_GOAL_SRV_TYPE.to_string(),
            &zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_CANCEL_GOAL,
            None,
            cancel_goal_queries_timeout,
            context.clone(),
        )
        .await?;

        // configured queries timeout for calls to get_result service
        let get_result_queries_timeout = context
            .config
            .get_queries_timeout_action_get_result(&ros2_name);
        let route_get_result = RouteServiceCli::create(
            format!("{ros2_name}/{}", *KE_SUFFIX_ACTION_GET_RESULT),
            format!("{ros2_type}_GetResult"),
            &zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_GET_RESULT,
            None,
            get_result_queries_timeout,
            context.clone(),
        )
        .await?;

        let route_feedback = RouteSubscriber::create(
            format!("{ros2_name}/{}", *KE_SUFFIX_ACTION_FEEDBACK),
            format!("{ros2_type}_FeedbackMessage"),
            &zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_FEEDBACK,
            true,
            QOS_DEFAULT_ACTION_FEEDBACK.clone(),
            context.clone(),
        )
        .await?;

        let route_status = RouteSubscriber::create(
            format!("{ros2_name}/{}", *KE_SUFFIX_ACTION_STATUS),
            ROS2_ACTION_STATUS_MSG_TYPE.to_string(),
            &zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_STATUS,
            true,
            QOS_DEFAULT_ACTION_STATUS.clone(),
            context.clone(),
        )
        .await?;

        Ok(RouteActionCli {
            ros2_name,
            ros2_type,
            zenoh_key_expr_prefix,
            context,
            is_active: false,
            route_send_goal,
            route_cancel_goal,
            route_get_result,
            route_feedback,
            route_status,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    // Announce the route over Zenoh via a LivelinessToken
    async fn announce_route(&mut self) -> Result<(), String> {
        self.is_active = true;

        // create associated LivelinessToken
        let liveliness_ke = new_ke_liveliness_action_cli(
            &self.context.zsession.zid().into_keyexpr(),
            &self.zenoh_key_expr_prefix,
            &self.ros2_type,
        )?;
        tracing::debug!("{self} announce via token {liveliness_ke}");
        let ros2_name = self.ros2_name.clone();
        self.liveliness_token = Some(self.context.zsession
            .liveliness()
            .declare_token(liveliness_ke)
            .await
            .map_err(|e| {
                format!(
                    "Failed create LivelinessToken associated to route for Action Client {ros2_name}: {e}"
                )
            })?
        );
        Ok(())
    }

    // Retire the route over Zenoh removing the LivelinessToken
    fn retire_route(&mut self) {
        tracing::debug!("{self} retire");
        // Drop Zenoh Publisher and Liveliness token
        // The DDS Writer remains to be discovered by local ROS nodes
        self.is_active = false;
        self.liveliness_token = None;
    }

    #[inline]
    pub fn add_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr_prefix: &keyexpr) {
        self.route_send_goal.add_remote_route(
            zenoh_id,
            &(zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_SEND_GOAL),
        );
        self.route_cancel_goal.add_remote_route(
            zenoh_id,
            &(zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_CANCEL_GOAL),
        );
        self.route_get_result.add_remote_route(
            zenoh_id,
            &(zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_GET_RESULT),
        );
        self.route_feedback.add_remote_route(
            zenoh_id,
            &(zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_FEEDBACK),
        );
        self.route_status.add_remote_route(
            zenoh_id,
            &(zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_STATUS),
        );
        self.remote_routes
            .insert(format!("{zenoh_id}:{zenoh_key_expr_prefix}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn remove_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr_prefix: &keyexpr) {
        self.route_send_goal.remove_remote_route(
            zenoh_id,
            &(zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_SEND_GOAL),
        );
        self.route_cancel_goal.remove_remote_route(
            zenoh_id,
            &(zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_CANCEL_GOAL),
        );
        self.route_get_result.remove_remote_route(
            zenoh_id,
            &(zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_GET_RESULT),
        );
        self.route_feedback.remove_remote_route(
            zenoh_id,
            &(zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_FEEDBACK),
        );
        self.route_status.remove_remote_route(
            zenoh_id,
            &(zenoh_key_expr_prefix / *KE_SUFFIX_ACTION_STATUS),
        );
        self.remote_routes
            .remove(&format!("{zenoh_id}:{zenoh_key_expr_prefix}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub async fn add_local_node(&mut self, node: String) {
        futures::join!(
            self.route_send_goal.add_local_node(node.clone()),
            self.route_cancel_goal.add_local_node(node.clone()),
            self.route_get_result.add_local_node(node.clone()),
            self.route_feedback
                .add_local_node(node.clone(), &QOS_DEFAULT_ACTION_FEEDBACK),
            self.route_status
                .add_local_node(node.clone(), &QOS_DEFAULT_ACTION_STATUS),
        );

        self.local_nodes.insert(node);
        tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if 1st local node added, activate the route
        if self.local_nodes.len() == 1 {
            if let Err(e) = self.announce_route().await {
                tracing::error!("{self} activation failed: {e}");
            }
        }
    }

    #[inline]
    pub fn remove_local_node(&mut self, node: &str) {
        self.route_send_goal.remove_local_node(node);
        self.route_cancel_goal.remove_local_node(node);
        self.route_get_result.remove_local_node(node);
        self.route_feedback.remove_local_node(node);
        self.route_status.remove_local_node(node);

        self.local_nodes.remove(node);
        tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        // if last local node removed, deactivate the route
        if self.local_nodes.is_empty() {
            self.retire_route();
        }
    }

    pub fn is_unused(&self) -> bool {
        self.route_send_goal.is_unused()
            && self.route_cancel_goal.is_unused()
            && self.route_get_result.is_unused()
            && self.route_status.is_unused()
            && self.route_feedback.is_unused()
    }
}
