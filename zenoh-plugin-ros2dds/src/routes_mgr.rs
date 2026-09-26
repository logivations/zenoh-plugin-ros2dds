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
    collections::{hash_map::Entry, HashMap},
    sync::{atomic::Ordering, Arc, RwLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use cyclors::{
    dds_entity_t,
    qos::{IgnoreLocal, Qos},
};
use serde::{Deserialize, Serialize};
use zenoh::{
    bytes::{Encoding, ZBytes},
    internal::zread,
    key_expr::{keyexpr, OwnedKeyExpr},
    query::Query,
    Session,
};

use crate::{
    config::Config,
    discovered_entities::DiscoveredEntities,
    events::{ROS2AnnouncementEvent, ROS2DiscoveryEvent},
    qos_helpers::{adapt_reader_qos_for_writer, adapt_writer_qos_for_reader},
    ros2_utils::{key_expr_to_ros2_name, ros2_name_to_key_expr},
    ros_discovery::RosDiscoveryInfoMgr,
    route_action_cli::RouteActionCli,
    route_action_srv::RouteActionSrv,
    route_publisher::RoutePublisher,
    route_service_cli::RouteServiceCli,
    route_service_srv::RouteServiceSrv,
    route_subscriber::RouteSubscriber,
};

lazy_static::lazy_static!(
    static ref KE_PREFIX_ROUTE_PUBLISHER: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("route/topic/pub") };
    static ref KE_PREFIX_ROUTE_SUBSCRIBER: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("route/topic/sub") };
    static ref KE_PREFIX_ROUTE_SERVICE_SRV: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("route/service/srv") };
    static ref KE_PREFIX_ROUTE_SERVICE_CLI: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("route/service/cli") };
    static ref KE_PREFIX_ROUTE_ACTION_SRV: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("route/action/srv") };
    static ref KE_PREFIX_ROUTE_ACTION_CLI: &'static keyexpr =  unsafe { keyexpr::from_str_unchecked("route/action/cli") };
);

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RouteStatus {
    Routed(OwnedKeyExpr), // Routing is active, with the zenoh key expression used for the route
    NotAllowed,           // Routing was not allowed per configuration
    CreationFailure(String), // The route creation failed
    _QoSConflict,         // A route was already established but with conflicting QoS
}

#[derive(Debug)]
enum RouteRef {
    Publisher(String),
    Subscriber(String),
    ServiceSrv(String),
    ServiceCli(String),
    ActionSrv(String),
    ActionCli(String),
}

// A Context struct to be shared as an Arc amongst all the code
#[derive(Clone)]
pub struct Context {
    pub(crate) config: Arc<Config>,
    pub(crate) zsession: Arc<Session>,
    pub(crate) participant: dds_entity_t,
    // all discovered entities
    pub(crate) discovered_entities: Arc<RwLock<DiscoveredEntities>>,
    // ros_discovery_info read/write manager
    pub(crate) ros_discovery_mgr: Arc<RosDiscoveryInfoMgr>,
    pub(crate) matching_changed: Arc<tokio::sync::Notify>,
}

pub struct RoutesMgr {
    context: Context,
    started: Instant,
    started_unix_ms: u128,
    last_reconciled: Instant,
    reconciliation_sequence: u64,
    // maps of established routes - ecah map indexed by topic/service/action name
    routes_publishers: HashMap<String, RoutePublisher>,
    routes_subscribers: HashMap<String, RouteSubscriber>,
    routes_service_srv: HashMap<String, RouteServiceSrv>,
    routes_service_cli: HashMap<String, RouteServiceCli>,
    routes_action_srv: HashMap<String, RouteActionSrv>,
    routes_action_cli: HashMap<String, RouteActionCli>,
    // admin space key prefix (stripped in map indexes)
    admin_prefix: OwnedKeyExpr,
    // admin space: index is the admin_keyexpr (relative to admin_prefix)
    admin_space: HashMap<OwnedKeyExpr, RouteRef>,
}

impl RoutesMgr {
    pub fn new(
        config: Arc<Config>,
        zsession: Arc<Session>,
        participant: dds_entity_t,
        discovered_entities: Arc<RwLock<DiscoveredEntities>>,
        ros_discovery_mgr: Arc<RosDiscoveryInfoMgr>,
        admin_prefix: OwnedKeyExpr,
    ) -> RoutesMgr {
        let context = Context {
            config,
            zsession,
            participant,
            discovered_entities,
            ros_discovery_mgr,
            matching_changed: Arc::new(tokio::sync::Notify::new()),
        };

        RoutesMgr {
            context,
            started: Instant::now(),
            started_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            last_reconciled: Instant::now(),
            reconciliation_sequence: 0,
            routes_publishers: HashMap::new(),
            routes_subscribers: HashMap::new(),
            routes_service_srv: HashMap::new(),
            routes_service_cli: HashMap::new(),
            routes_action_srv: HashMap::new(),
            routes_action_cli: HashMap::new(),
            admin_prefix,
            admin_space: HashMap::new(),
        }
    }

    pub(crate) fn matching_changed(&self) -> Arc<tokio::sync::Notify> {
        self.context.matching_changed.clone()
    }

    pub(crate) async fn reconcile(&mut self) {
        for route in self.routes_publishers.values_mut() {
            route.reconcile().await;
        }
        for route in self.routes_service_cli.values_mut() {
            route.reconcile().await;
        }
        for route in self.routes_subscribers.values_mut() {
            route.reconcile().await;
        }
        for route in self.routes_service_srv.values_mut() {
            route.reconcile().await;
        }
        for route in self.routes_action_cli.values_mut() {
            route.reconcile().await;
        }
        for route in self.routes_action_srv.values_mut() {
            route.reconcile().await;
        }
        self.last_reconciled = Instant::now();
        self.reconciliation_sequence = self.reconciliation_sequence.saturating_add(1);
    }

    pub async fn on_ros_discovery_event(
        &mut self,
        event: ROS2DiscoveryEvent,
    ) -> Result<(), String> {
        use ROS2DiscoveryEvent::*;
        match event {
            DiscoveredMsgPub(participant, node, iface) => {
                // Pick 1 discovered Writer amongst the possibly multiple ones listed in MsgPub
                let entity = {
                    let entities = zread!(self.context.discovered_entities);
                    iface
                        .writers
                        .iter()
                        .find_map(|w| entities.get_writer(w))
                        .cloned()
                };
                match entity {
                    Some(entity) => {
                        // Get route (create it if not yet exists)
                        let route = self
                            .get_or_create_route_publisher(
                                iface.name,
                                iface.typ,
                                entity.keyless,
                                adapt_writer_qos_for_reader(&entity.qos),
                                true,
                            )
                            .await?;
                        route.add_local_node((participant, node), &entity.qos).await;
                    }
                    None => {
                        return Err(format!(
                            "Failed to get DDS info for any Writer of {iface} ({:?})",
                            iface.writers
                        ))
                    }
                }
            }

            UndiscoveredMsgPub(participant, node, iface) => {
                if let Entry::Occupied(mut entry) = self.routes_publishers.entry(iface.name.clone())
                {
                    let route = entry.get_mut();
                    route.remove_local_node(&(participant, node));
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_PUBLISHER / iface.name_as_keyexpr()));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }

            DiscoveredMsgSub(participant, node, iface) => {
                // Pick 1 discovered Reader amongst the possibly multiple ones listed in MsgSub
                let entity = {
                    let entities = zread!(self.context.discovered_entities);
                    iface
                        .readers
                        .iter()
                        .find_map(|r| entities.get_reader(r))
                        .cloned()
                };
                match entity {
                    Some(entity) => {
                        // Get route (create it if not yet exists)
                        let route = self
                            .get_or_create_route_subscriber(
                                iface.name,
                                iface.typ,
                                entity.keyless,
                                adapt_reader_qos_for_writer(&entity.qos),
                                true,
                            )
                            .await?;
                        route.add_local_node((participant, node), &entity.qos).await;
                    }
                    None => {
                        return Err(format!(
                            "Failed to get DDS info for any Reader of {iface} ({:?})",
                            iface.readers
                        ))
                    }
                }
            }

            UndiscoveredMsgSub(participant, node, iface) => {
                if let Entry::Occupied(mut entry) =
                    self.routes_subscribers.entry(iface.name.clone())
                {
                    let route = entry.get_mut();
                    route.remove_local_node(&(participant, node));
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_SUBSCRIBER / iface.name_as_keyexpr()));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }
            DiscoveredServiceSrv(participant, node, iface) => {
                // Get route (create it if not yet exists)
                let route = self
                    .get_or_create_route_service_srv(iface.name, iface.typ, true)
                    .await?;
                route.add_local_node((participant, node)).await;
            }
            UndiscoveredServiceSrv(participant, node, iface) => {
                if let Entry::Occupied(mut entry) =
                    self.routes_service_srv.entry(iface.name.clone())
                {
                    let route = entry.get_mut();
                    route.remove_local_node(&(participant, node));
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_SERVICE_SRV / iface.name_as_keyexpr()));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }
            DiscoveredServiceCli(participant, node, iface) => {
                // Get route (create it if not yet exists)
                let route = self
                    .get_or_create_route_service_cli(iface.name, iface.typ, true)
                    .await?;
                route.add_local_node((participant, node)).await;
            }
            UndiscoveredServiceCli(participant, node, iface) => {
                if let Entry::Occupied(mut entry) =
                    self.routes_service_cli.entry(iface.name.clone())
                {
                    let route = entry.get_mut();
                    route.remove_local_node(&(participant, node));
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_SERVICE_CLI / iface.name_as_keyexpr()));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }
            DiscoveredActionSrv(participant, node, iface) => {
                // Get route (create it if not yet exists)
                let route = self
                    .get_or_create_route_action_srv(iface.name, iface.typ)
                    .await?;
                route.add_local_node((participant, node)).await;
            }
            UndiscoveredActionSrv(participant, node, iface) => {
                if let Entry::Occupied(mut entry) = self.routes_action_srv.entry(iface.name.clone())
                {
                    let route = entry.get_mut();
                    route.remove_local_node(&(participant, node));
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_ACTION_SRV / iface.name_as_keyexpr()));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }
            DiscoveredActionCli(participant, node, iface) => {
                // Get route (create it if not yet exists)
                let route = self
                    .get_or_create_route_action_cli(iface.name, iface.typ)
                    .await?;
                route.add_local_node((participant, node)).await;
            }
            UndiscoveredActionCli(participant, node, iface) => {
                if let Entry::Occupied(mut entry) = self.routes_action_cli.entry(iface.name.clone())
                {
                    let route = entry.get_mut();
                    route.remove_local_node(&(participant, node));
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_ACTION_CLI / iface.name_as_keyexpr()));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn on_ros_announcement_event(
        &mut self,
        event: ROS2AnnouncementEvent,
    ) -> Result<(), String> {
        use ROS2AnnouncementEvent::*;
        match event {
            AnnouncedMsgPub {
                zenoh_id,
                zenoh_key_expr,
                ros2_type,
                keyless,
                writer_qos,
            } => {
                let mut qos = writer_qos.clone();
                qos.ignore_local = Some(IgnoreLocal {
                    kind: cyclors::qos::IgnoreLocalKind::PARTICIPANT,
                });
                // On remote Publisher route announcement, prepare a Subscriber route
                // with an associated DDS Writer allowing local ROS2 Nodes to discover it
                let route = self
                    .get_or_create_route_subscriber(
                        key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config),
                        ros2_type,
                        keyless,
                        qos,
                        true,
                    )
                    .await?;
                route.add_remote_route(&zenoh_id, &zenoh_key_expr);
            }

            RetiredMsgPub {
                zenoh_id,
                zenoh_key_expr,
            } => {
                // Convert zenoh key_expr to ROS2 name (same as when creating the route)
                let ros2_name = key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config);

                if let Entry::Occupied(mut entry) = self.routes_subscribers.entry(ros2_name) {
                    let route = entry.get_mut();
                    route.remove_remote_route(&zenoh_id, &zenoh_key_expr);
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_SUBSCRIBER / &zenoh_key_expr));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }

            AnnouncedMsgSub {
                zenoh_id,
                zenoh_key_expr,
                ros2_type,
                keyless,
                reader_qos,
            } => {
                let mut qos = reader_qos.clone();
                qos.ignore_local = Some(IgnoreLocal {
                    kind: cyclors::qos::IgnoreLocalKind::PARTICIPANT,
                });
                // On remote Subscriber route announcement, prepare a Publisher route
                // with an associated DDS Reader allowing local ROS2 Nodes to discover it
                let route = self
                    .get_or_create_route_publisher(
                        key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config),
                        ros2_type,
                        keyless,
                        qos,
                        true,
                    )
                    .await?;
                route.add_remote_route(&zenoh_id, &zenoh_key_expr);
            }

            RetiredMsgSub {
                zenoh_id,
                zenoh_key_expr,
            } => {
                // Convert zenoh key_expr to ROS2 name (same as when creating the route)
                let ros2_name = key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config);

                if let Entry::Occupied(mut entry) = self.routes_publishers.entry(ros2_name) {
                    let route = entry.get_mut();
                    route.remove_remote_route(&zenoh_id, &zenoh_key_expr);
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_PUBLISHER / &zenoh_key_expr));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }

            AnnouncedServiceSrv {
                zenoh_id,
                zenoh_key_expr,
                ros2_type,
            } => {
                // On remote Service Server route announcement, prepare a Service Client route
                // with a associated DDS Reader/Writer allowing local ROS2 Nodes to discover it
                let route = self
                    .get_or_create_route_service_cli(
                        key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config),
                        ros2_type,
                        true,
                    )
                    .await?;
                route.add_remote_route(&zenoh_id, &zenoh_key_expr);
            }

            RetiredServiceSrv {
                zenoh_id,
                zenoh_key_expr,
            } => {
                // Convert zenoh key_expr to ROS2 name (same as when creating the route)
                let ros2_name = key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config);

                if let Entry::Occupied(mut entry) = self.routes_service_cli.entry(ros2_name) {
                    let route = entry.get_mut();
                    route.remove_remote_route(&zenoh_id, &zenoh_key_expr);
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_SERVICE_CLI / &zenoh_key_expr));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }

            AnnouncedServiceCli {
                zenoh_id,
                zenoh_key_expr,
                ros2_type,
            } => {
                // On remote Service Client route announcement, prepare a Service Server route
                // with a associated DDS Reader/Writer allowing local ROS2 Nodes to discover it
                let route = self
                    .get_or_create_route_service_srv(
                        key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config),
                        ros2_type,
                        true,
                    )
                    .await?;
                route.add_remote_route(&zenoh_id, &zenoh_key_expr);
            }

            RetiredServiceCli {
                zenoh_id,
                zenoh_key_expr,
            } => {
                // Convert zenoh key_expr to ROS2 name (same as when creating the route)
                let ros2_name = key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config);

                if let Entry::Occupied(mut entry) = self.routes_service_srv.entry(ros2_name) {
                    let route = entry.get_mut();
                    route.remove_remote_route(&zenoh_id, &zenoh_key_expr);
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_SERVICE_SRV / &zenoh_key_expr));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }

            AnnouncedActionSrv {
                zenoh_id,
                zenoh_key_expr,
                ros2_type,
            } => {
                // On remote Action Server route announcement, prepare a Action Client route
                // with a associated DDS Reader/Writer allowing local ROS2 Nodes to discover it
                let route = self
                    .get_or_create_route_action_cli(
                        key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config),
                        ros2_type,
                    )
                    .await?;
                route.add_remote_route(&zenoh_id, &zenoh_key_expr);
            }

            RetiredActionSrv {
                zenoh_id,
                zenoh_key_expr,
            } => {
                // Convert zenoh key_expr to ROS2 name (same as when creating the route)
                let ros2_name = key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config);

                if let Entry::Occupied(mut entry) = self.routes_action_cli.entry(ros2_name) {
                    let route = entry.get_mut();
                    route.remove_remote_route(&zenoh_id, &zenoh_key_expr);
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_ACTION_CLI / &zenoh_key_expr));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }

            AnnouncedActionCli {
                zenoh_id,
                zenoh_key_expr,
                ros2_type,
            } => {
                // On remote Action Client route announcement, prepare a Action Server route
                // with a associated DDS Reader/Writer allowing local ROS2 Nodes to discover it
                let route = self
                    .get_or_create_route_action_srv(
                        key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config),
                        ros2_type,
                    )
                    .await?;
                route.add_remote_route(&zenoh_id, &zenoh_key_expr);
            }

            RetiredActionCli {
                zenoh_id,
                zenoh_key_expr,
            } => {
                // Convert zenoh key_expr to ROS2 name (same as when creating the route)
                let ros2_name = key_expr_to_ros2_name(&zenoh_key_expr, &self.context.config);

                if let Entry::Occupied(mut entry) = self.routes_action_srv.entry(ros2_name) {
                    let route = entry.get_mut();
                    route.remove_remote_route(&zenoh_id, &zenoh_key_expr);
                    if route.is_unused() {
                        self.admin_space
                            .remove(&(*KE_PREFIX_ROUTE_ACTION_SRV / &zenoh_key_expr));
                        let route = entry.remove();
                        tracing::info!("{route} removed");
                    }
                }
            }
        }
        Ok(())
    }

    async fn get_or_create_route_publisher(
        &mut self,
        ros2_name: String,
        ros2_type: String,
        keyless: bool,
        reader_qos: Qos,
        admin_space_ref: bool,
    ) -> Result<&mut RoutePublisher, String> {
        match self.routes_publishers.entry(ros2_name.clone()) {
            Entry::Vacant(entry) => {
                // ROS2 topic name => Zenoh key expr
                let zenoh_key_expr = ros2_name_to_key_expr(&ros2_name, &self.context.config);
                // create route
                let route = RoutePublisher::create(
                    ros2_name.clone(),
                    ros2_type,
                    zenoh_key_expr.clone(),
                    &None,
                    keyless,
                    reader_qos,
                    self.context.clone(),
                )
                .await?;
                tracing::info!("{route} created");

                if admin_space_ref {
                    // insert reference in admin_space
                    let admin_ke = *KE_PREFIX_ROUTE_PUBLISHER / &zenoh_key_expr;
                    self.admin_space
                        .insert(admin_ke, RouteRef::Publisher(ros2_name));
                }

                Ok(entry.insert(route))
            }
            Entry::Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    async fn get_or_create_route_subscriber(
        &mut self,
        ros2_name: String,
        ros2_type: String,
        keyless: bool,
        writer_qos: Qos,
        admin_space_ref: bool,
    ) -> Result<&mut RouteSubscriber, String> {
        match self.routes_subscribers.entry(ros2_name.clone()) {
            Entry::Vacant(entry) => {
                // ROS2 topic name => Zenoh key expr
                let zenoh_key_expr = ros2_name_to_key_expr(&ros2_name, &self.context.config);
                // create route
                let route = RouteSubscriber::create(
                    ros2_name.clone(),
                    ros2_type,
                    zenoh_key_expr.clone(),
                    keyless,
                    writer_qos,
                    self.context.clone(),
                )
                .await?;
                tracing::info!("{route} created");

                if admin_space_ref {
                    // insert reference in admin_space
                    let admin_ke = *KE_PREFIX_ROUTE_SUBSCRIBER / &zenoh_key_expr;
                    self.admin_space
                        .insert(admin_ke, RouteRef::Subscriber(ros2_name));
                }

                Ok(entry.insert(route))
            }
            Entry::Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    async fn get_or_create_route_service_srv(
        &mut self,
        ros2_name: String,
        ros2_type: String,
        admin_space_ref: bool,
    ) -> Result<&mut RouteServiceSrv, String> {
        match self.routes_service_srv.entry(ros2_name.clone()) {
            Entry::Vacant(entry) => {
                // ROS2 topic name => Zenoh key expr
                let zenoh_key_expr = ros2_name_to_key_expr(&ros2_name, &self.context.config);
                // create route
                let route = RouteServiceSrv::create(
                    ros2_name.clone(),
                    ros2_type,
                    zenoh_key_expr.clone(),
                    &None,
                    self.context.clone(),
                )
                .await?;
                tracing::info!("{route} created");

                if admin_space_ref {
                    // insert reference in admin_space
                    let admin_ke = *KE_PREFIX_ROUTE_SERVICE_SRV / &zenoh_key_expr;
                    self.admin_space
                        .insert(admin_ke, RouteRef::ServiceSrv(ros2_name));
                }

                Ok(entry.insert(route))
            }
            Entry::Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    async fn get_or_create_route_service_cli(
        &mut self,
        ros2_name: String,
        ros2_type: String,
        admin_space_ref: bool,
    ) -> Result<&mut RouteServiceCli, String> {
        match self.routes_service_cli.entry(ros2_name.clone()) {
            Entry::Vacant(entry) => {
                // ROS2 topic name => Zenoh key expr : strip '/' prefix
                let zenoh_key_expr = ros2_name_to_key_expr(&ros2_name, &self.context.config);
                // configured queries timeout for services calls
                let queries_timeout = self.context.config.get_queries_timeout_service(&ros2_name);
                // create route
                let route = RouteServiceCli::create(
                    ros2_name.clone(),
                    ros2_type,
                    zenoh_key_expr.clone(),
                    None,
                    queries_timeout,
                    self.context.clone(),
                )
                .await?;
                tracing::info!("{route} created");

                if admin_space_ref {
                    // insert reference in admin_space
                    let admin_ke = *KE_PREFIX_ROUTE_SERVICE_CLI / &zenoh_key_expr;
                    self.admin_space
                        .insert(admin_ke, RouteRef::ServiceCli(ros2_name));
                }

                Ok(entry.insert(route))
            }
            Entry::Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    async fn get_or_create_route_action_srv(
        &mut self,
        ros2_name: String,
        ros2_type: String,
    ) -> Result<&mut RouteActionSrv, String> {
        match self.routes_action_srv.entry(ros2_name.clone()) {
            Entry::Vacant(entry) => {
                // ROS2 topic name => Zenoh key expr : strip '/' prefix
                let zenoh_key_expr = ros2_name_to_key_expr(&ros2_name, &self.context.config);
                // create route
                let route = RouteActionSrv::create(
                    ros2_name.clone(),
                    ros2_type,
                    zenoh_key_expr.clone(),
                    self.context.clone(),
                )
                .await?;
                tracing::info!("{route} created");

                // insert reference in admin_space
                let admin_ke = *KE_PREFIX_ROUTE_ACTION_SRV / &zenoh_key_expr;
                self.admin_space
                    .insert(admin_ke, RouteRef::ActionSrv(ros2_name));

                Ok(entry.insert(route))
            }
            Entry::Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    async fn get_or_create_route_action_cli(
        &mut self,
        ros2_name: String,
        ros2_type: String,
    ) -> Result<&mut RouteActionCli, String> {
        match self.routes_action_cli.entry(ros2_name.clone()) {
            Entry::Vacant(entry) => {
                // ROS2 topic name => Zenoh key expr : strip '/' prefix
                let zenoh_key_expr = ros2_name_to_key_expr(&ros2_name, &self.context.config);
                // create route
                let route = RouteActionCli::create(
                    ros2_name.clone(),
                    ros2_type,
                    zenoh_key_expr.to_owned(),
                    self.context.clone(),
                )
                .await?;
                tracing::info!("{route} created");

                // insert reference in admin_space
                let admin_ke = *KE_PREFIX_ROUTE_ACTION_CLI / &zenoh_key_expr;
                self.admin_space
                    .insert(admin_ke, RouteRef::ActionCli(ros2_name));

                Ok(entry.insert(route))
            }
            Entry::Occupied(entry) => Ok(entry.into_mut()),
        }
    }

    fn lifecycle_health(&self) -> serde_json::Value {
        use crate::dds_endpoint::{CLEANUP_FAILURES, LIVE_ENDPOINTS, LIVE_LISTENERS, LIVE_TOPICS};
        let owned = self
            .routes_publishers
            .values()
            .map(RoutePublisher::endpoint_count)
            .sum::<usize>()
            + self
                .routes_subscribers
                .values()
                .map(RouteSubscriber::endpoint_count)
                .sum::<usize>()
            + self
                .routes_service_cli
                .values()
                .map(RouteServiceCli::endpoint_count)
                .sum::<usize>()
            + self
                .routes_service_srv
                .values()
                .map(RouteServiceSrv::endpoint_count)
                .sum::<usize>()
            + self
                .routes_action_cli
                .values()
                .map(RouteActionCli::endpoint_count)
                .sum::<usize>()
            + self
                .routes_action_srv
                .values()
                .map(RouteActionSrv::endpoint_count)
                .sum::<usize>();
        serde_json::json!({
            "schema_version": 1,
            "build_id": env!("BRIDGE_BUILD_ID"),
            "base_version": env!("CARGO_PKG_VERSION"),
            "pid": std::process::id(),
            "started_unix_ms": self.started_unix_ms,
            "uptime_ms": self.started.elapsed().as_millis(),
            "zid": self.context.zsession.zid().to_string(),
            "participant": crate::dds_utils::get_guid(&self.context.participant).ok(),
            "reconciliation_sequence": self.reconciliation_sequence,
            "reconciliation_age_ms": self.last_reconciled.elapsed().as_millis(),
            "routes": self.admin_space.len(),
            "owned_dds_endpoints": owned,
            "live_dds_endpoints": LIVE_ENDPOINTS.load(Ordering::Acquire),
            "owned_topic_references": LIVE_TOPICS.load(Ordering::Acquire),
            "dds_callback_allocations": LIVE_LISTENERS.load(Ordering::Acquire),
            "owned_matching_listeners": self.routes_publishers.len() + self.routes_service_cli.len()
                + self.routes_action_cli.len() * 3 + self.routes_action_srv.len() * 2,
            "cleanup_failures": CLEANUP_FAILURES.load(Ordering::Acquire),
            "discovery": zread!(self.context.discovered_entities).counts(),
        })
    }

    pub async fn treat_admin_query(&self, query: &Query) {
        let health_key = &self.admin_prefix / unsafe { keyexpr::from_str_unchecked("lifecycle") };
        if query.key_expr().intersects(&health_key) {
            let bytes = serde_json::to_vec(&self.lifecycle_health()).expect("lifecycle JSON value");
            if let Err(error) = query
                .reply(health_key, bytes)
                .encoding(Encoding::APPLICATION_JSON)
                .await
            {
                tracing::warn!("Unable to reply to lifecycle health query: {error}");
            }
        }
        let selector = query.selector();

        // get the list of sub-key expressions that will match the same stored keys than
        // the selector, if those keys had the admin_keyexpr_prefix.
        let sub_kes = selector.key_expr().strip_prefix(&self.admin_prefix);
        if sub_kes.is_empty() {
            tracing::error!("Received query for admin space: '{}' - but it's not prefixed by admin_keyexpr_prefix='{}'", selector, &self.admin_prefix);
            return;
        }

        // For all sub-key expression
        for sub_ke in sub_kes {
            if sub_ke.is_wild() {
                // iterate over all admin space to find matching keys and reply for each
                for (ke, route_ref) in self.admin_space.iter() {
                    if sub_ke.intersects(ke) {
                        self.send_admin_reply(query, ke, route_ref).await;
                    }
                }
            } else {
                // sub_ke correspond to 1 key - just get it and reply
                if let Some(route_ref) = self.admin_space.get(sub_ke) {
                    self.send_admin_reply(query, sub_ke, route_ref).await;
                }
            }
        }
    }

    async fn send_admin_reply(&self, query: &Query, key_expr: &keyexpr, route_ref: &RouteRef) {
        match self.get_entity_json_value(route_ref) {
            Ok(Some(v)) => {
                let admin_keyexpr = &self.admin_prefix / key_expr;
                match serde_json::to_vec(&v) {
                    Ok(bytes) => {
                        if let Err(e) = query
                            .reply(admin_keyexpr, ZBytes::from(bytes))
                            .encoding(Encoding::APPLICATION_JSON)
                            .await
                        {
                            tracing::warn!("Error replying to admin query {:?}: {}", query, e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Error transforming JSON to admin query {:?}: {}", query, e);
                    }
                }
            }
            Ok(None) => {
                tracing::error!("INTERNAL ERROR: Dangling {:?} for {}", route_ref, key_expr)
            }
            Err(e) => {
                tracing::error!("INTERNAL ERROR serializing admin value as JSON: {}", e)
            }
        }
    }

    fn get_entity_json_value(
        &self,
        route_ref: &RouteRef,
    ) -> Result<Option<serde_json::Value>, serde_json::Error> {
        match route_ref {
            RouteRef::Publisher(ke) => self
                .routes_publishers
                .get(ke)
                .map(serde_json::to_value)
                .transpose(),
            RouteRef::Subscriber(ke) => self
                .routes_subscribers
                .get(ke)
                .map(serde_json::to_value)
                .transpose(),
            RouteRef::ServiceSrv(ke) => self
                .routes_service_srv
                .get(ke)
                .map(serde_json::to_value)
                .transpose(),
            RouteRef::ServiceCli(ke) => self
                .routes_service_cli
                .get(ke)
                .map(serde_json::to_value)
                .transpose(),
            RouteRef::ActionSrv(ke) => self
                .routes_action_srv
                .get(ke)
                .map(serde_json::to_value)
                .transpose(),
            RouteRef::ActionCli(ke) => self
                .routes_action_cli
                .get(ke)
                .map(serde_json::to_value)
                .transpose(),
        }
    }
}
