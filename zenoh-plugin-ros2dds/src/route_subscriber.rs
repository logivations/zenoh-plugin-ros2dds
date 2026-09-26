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

use std::{collections::HashSet, fmt, time::Duration};

use serde::Serialize;
use zenoh::{
    key_expr::{keyexpr, OwnedKeyExpr},
    liveliness::LivelinessToken,
    pubsub::Subscriber,
    sample::{Locality, Sample},
    Wait,
};
use zenoh_ext::{AdvancedSubscriber, AdvancedSubscriberBuilderExt, HistoryConfig};

use crate::{
    dds_endpoint::{DdsAccess, DdsEndpoint},
    dds_utils::{dds_write, serialize_local_nodes},
    gid::Gid,
    liveliness_mgt::new_ke_liveliness_sub,
    qos::{History, Qos},
    qos_helpers::is_transient_local,
    ros2_utils::{is_message_for_action, ros2_message_type_to_dds_type},
    route_lifecycle::{Retry, RouteLifecycle},
    routes_mgr::Context,
    serialize_option_as_bool, LOG_PAYLOAD,
};

enum ZSubscriber {
    Subscriber(Subscriber<()>),
    AdvancedSubscriber(AdvancedSubscriber<()>),
}

// a route from Zenoh to DDS
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RouteSubscriber {
    // the ROS2 Subscriber name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    // the zenoh subscriber receiving messages to be re-published by the DDS Writer
    // `None` when route is created on a remote announcement and no local ROS2 Subscriber discovered yet
    #[serde(rename = "is_active", serialize_with = "serialize_option_as_bool")]
    zenoh_subscriber: Option<ZSubscriber>,
    // the local DDS Writer created to serve the route (i.e. re-publish to DDS message coming from zenoh)
    #[serde(serialize_with = "crate::dds_endpoint::serialize_optional")]
    dds_writer: Option<DdsEndpoint>,
    lifecycle: RouteLifecycle,
    announcement_retry: Retry,
    #[serde(skip)]
    writer_qos: Qos,
    #[serde(skip)]
    discovered_reader_qos: Option<Qos>,
    // if the Writer is TRANSIENT_LOCAL
    transient_local: bool,
    // queries timeout for historical publication (if TRANSIENT_LOCAL)
    queries_timeout: Duration,
    // if the topic is keyless
    #[serde(skip)]
    keyless: bool,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken>,
    // the list of remote routes served by this route ("<zenoh_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route, keyed by (participant_gid, node_fullname) to
    // disambiguate same-named nodes across restarts (#702).
    #[serde(flatten, serialize_with = "serialize_local_nodes")]
    local_nodes: HashSet<(Gid, String)>,
}

impl Drop for RouteSubscriber {
    fn drop(&mut self) {
        if let Some(writer) = &self.dds_writer {
            writer.fence();
        }
        self.retire_route();
        self.dds_writer.take();
    }
}

impl fmt::Display for RouteSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Subscriber (Zenoh:{} -> ROS:{})",
            self.zenoh_key_expr, self.ros2_name
        )
    }
}

impl RouteSubscriber {
    pub(crate) fn endpoint_count(&self) -> usize {
        usize::from(self.dds_writer.is_some())
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        keyless: bool,
        mut writer_qos: Qos,
        context: Context,
    ) -> Result<RouteSubscriber, String> {
        let transient_local = is_transient_local(&writer_qos);
        tracing::debug!("Route Subscriber ({zenoh_key_expr} -> {ros2_name}): creation with type {ros2_type} (transient_local:{transient_local})");

        let queries_timeout = context.config.get_queries_timeout_tl_sub(&ros2_name);

        // force RELIABLE QoS for Writers (#23)
        if let Some(cyclors::qos::Reliability {
            kind: cyclors::qos::ReliabilityKind::BEST_EFFORT,
            ..
        }) = &mut writer_qos.reliability
        {
            // Per DDS specification, the default Reliability value for DataWriters is RELIABLE with max_blocking_time=100ms
            // Thus just use default value.
            writer_qos.reliability = None;
        }

        Ok(RouteSubscriber {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            zenoh_subscriber: None,
            dds_writer: None,
            lifecycle: RouteLifecycle::new(),
            announcement_retry: Retry::new(),
            writer_qos,
            discovered_reader_qos: None,
            transient_local,
            queries_timeout,
            keyless,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    // Announce the route over Zenoh via a LivelinessToken
    async fn announce_route(&mut self, discovered_reader_qos: &Qos) -> Result<(), String> {
        tracing::debug!("{self} activate");
        // Callback routing message received by Zenoh subscriber to DDS Writer (if set)
        let ros2_name = self.ros2_name.clone();
        let dds_writer = self
            .dds_writer
            .as_ref()
            .ok_or("DDS proxy not available")?
            .access();
        let subscriber_callback = move |s: Sample| {
            route_zenoh_message_to_dds(s, &ros2_name, &dds_writer);
        };

        // create zenoh subscriber
        // if Writer is TRANSIENT_LOCAL, use a QueryingSubscriber to fetch remote historical messages to write
        let subscriber = if self.transient_local {
            let history_config = match &discovered_reader_qos.history {
                Some(History { depth, .. }) => {
                    let depth: usize = (*depth).try_into().unwrap_or(usize::MAX);
                    HistoryConfig::default()
                        .detect_late_publishers()
                        .max_samples(depth)
                }
                _other => HistoryConfig::default()
                    .detect_late_publishers()
                    .max_samples(1),
            };
            let sub = self
                .context
                .zsession
                .declare_subscriber(&self.zenoh_key_expr)
                .advanced()
                .callback(subscriber_callback)
                .allowed_origin(Locality::Remote) // Allow only remote publications to avoid loops
                .history(history_config)
                .query_timeout(self.queries_timeout)
                .await
                .map_err(|e| format!("{self}: failed to create FetchingSubscriber: {e}",))?;
            Some(ZSubscriber::AdvancedSubscriber(sub))
        } else {
            let sub = self
                .context
                .zsession
                .declare_subscriber(&self.zenoh_key_expr)
                .callback(subscriber_callback)
                .allowed_origin(Locality::Remote) // Allow only remote publications to avoid loops
                .await
                .map_err(|e| format!("{self}: failed to create Subscriber: {e}"))?;
            Some(ZSubscriber::Subscriber(sub))
        };

        // if not for an Action (since actions declare their own liveliness)
        if !is_message_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_sub(
                &self.context.zsession.zid().into_keyexpr(),
                &self.zenoh_key_expr,
                &self.ros2_type,
                self.keyless,
                discovered_reader_qos,
            )?;
            let ros2_name = self.ros2_name.clone();
            self.liveliness_token = Some(
                self.context.zsession
                    .liveliness()
                    .declare_token(liveliness_ke)
                    .await
                    .map_err(|e| {
                        format!(
                            "Failed create LivelinessToken associated to route for Subscriber {ros2_name} : {e}"
                        )
                    })?,
            );
        }
        self.zenoh_subscriber = subscriber;
        Ok(())
    }

    pub(crate) async fn reconcile(&mut self) {
        let route_id = self.to_string();
        self.lifecycle.set_desired(true);
        if let Err(error) = self.lifecycle.reconcile(&mut self.dds_writer, || {
            let mut writer = DdsEndpoint::writer(
                self.context.participant,
                format!("rt{}", self.ros2_name),
                ros2_message_type_to_dds_type(&self.ros2_type),
                self.keyless,
                self.writer_qos.clone(),
            )?;
            writer.advertise(self.context.ros_discovery_mgr.clone());
            Ok(writer)
        }) {
            self.lifecycle.log_activation_failure(&route_id, &error);
        }
        if !self.local_nodes.is_empty()
            && self.dds_writer.is_some()
            && self.zenoh_subscriber.is_none()
            && self.announcement_retry.ready()
        {
            if let Some(qos) = self.discovered_reader_qos.clone() {
                let result = self.announce_route(&qos).await;
                if let Err(error) = self.announcement_retry.record(result) {
                    tracing::error!("{route_id}: announcement failed: {error}");
                }
            }
        }
    }

    // Retire the route over Zenoh removing the LivelinessToken
    fn retire_route(&mut self) {
        tracing::debug!("{self} deactivate");
        // Drop Zenoh Subscriber and Liveliness token
        // The DDS Writer remains to be discovered by local ROS nodes
        match self.zenoh_subscriber.take() {
            Some(ZSubscriber::Subscriber(s)) => {
                if let Err(e) = s.undeclare().wait() {
                    tracing::debug!("Unable to undeclare subscriber: {:?}", e);
                }
            }
            Some(ZSubscriber::AdvancedSubscriber(s)) => {
                if let Err(e) = s.undeclare().wait() {
                    tracing::debug!("Unable to undeclare subscriber: {:?}", e);
                }
            }
            None => {}
        };
        self.liveliness_token = None;
    }

    #[inline]
    pub fn add_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .insert(format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn remove_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .remove(&format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self} now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn is_serving_remote_route(&self) -> bool {
        !self.remote_routes.is_empty()
    }

    #[inline]
    pub async fn add_local_node(&mut self, node_key: (Gid, String), discovered_reader_qos: &Qos) {
        self.local_nodes.insert(node_key);
        self.discovered_reader_qos
            .get_or_insert_with(|| discovered_reader_qos.clone());
        tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
        self.reconcile().await;
    }

    #[inline]
    pub fn remove_local_node(&mut self, node_key: &(Gid, String)) {
        if self.local_nodes.remove(node_key) {
            tracing::debug!("{self} now serving local nodes {:?}", self.local_nodes);
            // if last local node removed, deactivate the route
            if self.local_nodes.is_empty() {
                self.retire_route();
                self.discovered_reader_qos = None;
            }
        }
    }

    #[inline]
    pub fn is_serving_local_node(&self) -> bool {
        !self.local_nodes.is_empty()
    }

    #[inline]
    pub fn is_unused(&self) -> bool {
        !self.is_serving_local_node() && !self.is_serving_remote_route()
    }
}

fn route_zenoh_message_to_dds(s: Sample, ros2_name: &str, data_writer: &DdsAccess) {
    if *LOG_PAYLOAD {
        tracing::debug!(
            "Route Subscriber (Zenoh:{} -> ROS:{}): routing message - payload: {:02x?}",
            s.key_expr(),
            &ros2_name,
            s.payload()
        );
    } else {
        tracing::trace!(
            "Route Subscriber (Zenoh:{} -> ROS:{}): routing message - {} bytes",
            s.key_expr(),
            &ros2_name,
            s.payload().len()
        );
    }

    if let Some(Err(error)) =
        data_writer.with(|writer| dds_write(writer, s.payload().to_bytes().to_vec()))
    {
        tracing::warn!(
            "Route Subscriber (Zenoh:{} -> ROS:{}): DDS write failed: {}",
            s.key_expr(),
            ros2_name,
            error
        );
    }
}
