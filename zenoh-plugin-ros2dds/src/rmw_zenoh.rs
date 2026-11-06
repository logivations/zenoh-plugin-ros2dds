//
// Copyright (c) 2026 Logivations GmbH
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//

//! The bridge for ROS 2 nodes that use rmw_zenoh_cpp (`rmw: zenoh`).
//!
//! Towards the other bridges nothing changes: the same key expressions, payloads, request headers
//! and `@/<zid>/@ros2_lv/...` announcements as a bridge on a DDS domain, so a peer bridge on
//! cyclonedds cannot tell the difference. Towards the local nodes the bridge is a second zenoh
//! session, a client of the local rmw_zenoh router: it discovers the nodes' publishers,
//! subscriptions, services and clients from rmw_zenoh's liveliness tokens (`@ros2_lv/<domain>/...`),
//! routes the allowed ones and announces proxies for them in the local ROS graph (as the DDS
//! bridge's readers and writers appear in the DDS graph), e.g. a service server for a local client
//! while a remote server matches, so that `wait_for_service` behaves as with DDS.
//!
//! The two sessions stay separate on purpose: one session joined to both networks would route
//! every local rmw_zenoh entity to the remote router and every other bridge.
//!
//! Zenoh callbacks run on the RX threads that both sessions share (2 by default): nothing may block
//! there. Publications are handed to one ordered sender task per route (dropped when it falls
//! behind), queries and replies are sent from tasks on the plugin's runtime.
//!
//! Not supported (yet): actions (their topics and services are skipped), pub_max_frequencies,
//! the admin space.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use cyclors::qos::{
    Durability, DurabilityKind, History, HistoryKind, Qos, Reliability, ReliabilityKind,
    DDS_100MS_DURATION,
};
use futures::future::{select, Either};
use zenoh::{
    bytes::ZBytes,
    key_expr::{format::keformat, keyexpr, OwnedKeyExpr},
    liveliness::LivelinessToken,
    pubsub::Subscriber,
    qos::{CongestionControl, Reliability as ZReliability},
    query::{ConsolidationMode, Querier, Query, Queryable},
    sample::{Locality, Sample, SampleKind},
    Session,
};
use zenoh_ext::{
    z_deserialize, z_serialize, AdvancedPublisher, AdvancedPublisherBuilderExt, AdvancedSubscriber,
    AdvancedSubscriberBuilderExt, CacheConfig, HistoryConfig,
};

use crate::{
    config::Config,
    dds_utils::is_cdr_little_endian,
    liveliness_mgt::{
        ke_liveliness_plugin, new_ke_liveliness_pub, new_ke_liveliness_service_cli,
        new_ke_liveliness_service_srv, new_ke_liveliness_sub,
    },
    ros2_utils::{
        dds_type_to_ros2_message_type, insert_type_hash, ros2_name_to_key_expr, CddsRequestHeader,
    },
    TOKIO_RUNTIME,
};

/// Prefix of rmw_zenoh's liveliness tokens (graph cache)
const RMW_ADMIN_SPACE: &str = "@ros2_lv";
/// rmw_zenoh replaces '/' in names with this character in its liveliness tokens
const RMW_SLASH_REPLACEMENT: char = '%';
// rmw_zenoh's defaults for a QoS field left empty in a token (rmw_qos_profile_default)
const RMW_DEFAULT_DEPTH: i32 = 10;
/// Publications a route buffers while its sender waits for the network; more are dropped
const ROUTE_QUEUE_SIZE: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Kind {
    Publisher,
    Subscription,
    Service,
    Client,
}

impl Kind {
    fn from_token(s: &str) -> Option<Kind> {
        match s {
            "MP" => Some(Kind::Publisher),
            "MS" => Some(Kind::Subscription),
            "SS" => Some(Kind::Service),
            "SC" => Some(Kind::Client),
            _ => None,
        }
    }

    /// The local proxy the bridge announces for a route of this kind (the counterpart entity)
    fn proxy_token(&self) -> &'static str {
        match self {
            Kind::Publisher => "MS",
            Kind::Subscription => "MP",
            Kind::Service => "SC",
            Kind::Client => "SS",
        }
    }
}

/// An entity of a local node, from its rmw_zenoh liveliness token
/// `@ros2_lv/<domain>/<zid>/<nid>/<id>/<kind>/<enclave>/<namespace>/<node>/<name>/<type>/<hash>/<qos>`
#[derive(Debug, Clone)]
struct RmwEntity {
    zid: String,
    kind: Kind,
    name: String,
    rmw_type: String,
    type_hash: String,
    qos: String,
}

impl RmwEntity {
    fn parse(ke: &str) -> Option<RmwEntity> {
        let parts: Vec<&str> = ke.split('/').collect();
        if parts.len() != 13 || parts[0] != RMW_ADMIN_SPACE {
            // nodes (NN) have no topic part
            return None;
        }
        Some(RmwEntity {
            zid: parts[2].to_string(),
            kind: Kind::from_token(parts[5])?,
            name: demangle(parts[9]),
            rmw_type: demangle(parts[10]),
            type_hash: demangle(parts[11]),
            qos: parts[12].to_string(),
        })
    }
}

fn mangle(s: &str) -> String {
    s.replace('/', &RMW_SLASH_REPLACEMENT.to_string())
}

fn demangle(s: &str) -> String {
    s.replace(RMW_SLASH_REPLACEMENT, "/")
}

/// The fields of rmw_zenoh's QoS token part the bridge needs:
/// `<reliability>:<durability>:<history>,<depth>:<deadline>:<lifespan>:<liveliness>`,
/// rmw enum values, empty for rmw_qos_profile_default's value
#[derive(Debug, Clone, Copy, PartialEq)]
struct RmwQos {
    best_effort: bool,
    transient_local: bool,
    keep_all: bool,
    depth: i32,
}

impl RmwQos {
    fn parse(s: &str) -> RmwQos {
        let elts: Vec<&str> = s.split(':').collect();
        let field = |i: usize| elts.get(i).copied().unwrap_or("");
        let (history, depth) = field(2).split_once(',').unwrap_or((field(2), ""));
        RmwQos {
            // RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT
            best_effort: field(0) == "2",
            // RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL
            transient_local: field(1) == "1",
            // RMW_QOS_POLICY_HISTORY_KEEP_ALL
            keep_all: history == "2",
            depth: depth.parse().unwrap_or(RMW_DEFAULT_DEPTH),
        }
    }

    /// The QoS a DDS bridge announces for such an entity (as it reads it from DDS discovery)
    fn to_dds(self, type_hash: &str) -> Qos {
        let mut qos = Qos {
            reliability: Some(Reliability {
                kind: if self.best_effort {
                    ReliabilityKind::BEST_EFFORT
                } else {
                    ReliabilityKind::RELIABLE
                },
                max_blocking_time: DDS_100MS_DURATION,
            }),
            durability: Some(Durability {
                kind: if self.transient_local {
                    DurabilityKind::TRANSIENT_LOCAL
                } else {
                    DurabilityKind::VOLATILE
                },
            }),
            history: Some(History {
                kind: if self.keep_all {
                    HistoryKind::KEEP_ALL
                } else {
                    HistoryKind::KEEP_LAST
                },
                depth: self.depth,
            }),
            ..Default::default()
        };
        if type_hash.starts_with("RIHS") {
            insert_type_hash(&mut qos, type_hash);
        }
        qos
    }

    fn history_samples(&self) -> usize {
        if self.keep_all {
            usize::MAX
        } else {
            self.depth.max(1) as usize
        }
    }
}

/// rmw_zenoh's attachment of every publication, request and reply:
/// sequence number, source timestamp (ns since the epoch) and the source's 16 bytes GID
fn rmw_attachment(sequence_number: i64, gid: &[u8; 16]) -> ZBytes {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or_default();
    z_serialize(&(sequence_number, now, *gid))
}

fn parse_rmw_attachment(attachment: Option<&ZBytes>) -> Option<(i64, i64, [u8; 16])> {
    attachment.and_then(|a| z_deserialize::<(i64, i64, [u8; 16])>(a).ok())
}

/// A publication waiting for its route's sender: payload and attachment
type Publication = (ZBytes, Option<ZBytes>);

/// The ordered sender of a route: publications leave in the order the callback queued them,
/// blocking (congestion control Block) only this task, never a zenoh RX thread
fn spawn_sender(
    route: String,
    publisher: Arc<AdvancedPublisher<'static>>,
) -> flume::Sender<Publication> {
    let (tx, rx) = flume::bounded::<Publication>(ROUTE_QUEUE_SIZE);
    TOKIO_RUNTIME.spawn(async move {
        // ends when the route drops its callback, the only holder of tx
        while let Ok((payload, attachment)) = rx.recv_async().await {
            if let Err(e) = publisher.put(payload).attachment(attachment).await {
                tracing::warn!("{route}: routing failed: {e}");
            }
        }
    });
    tx
}

fn queue(route: &str, tx: &flume::Sender<Publication>, publication: Publication) {
    if let Err(flume::TrySendError::Full(_)) = tx.try_send(publication) {
        tracing::debug!("{route}: {ROUTE_QUEUE_SIZE} publications waiting, dropping one");
    }
}

/// A query waiting for the reply of the other side. The first reply answers it; if none comes
/// (timeout), dropping the last reference answers with an error so the requester does not wait
/// for nothing.
struct PendingQuery {
    route: String,
    query: Mutex<Option<Query>>,
}

impl PendingQuery {
    fn new(route: String, query: Query) -> Arc<PendingQuery> {
        Arc::new(PendingQuery {
            route,
            query: Mutex::new(Some(query)),
        })
    }

    fn reply(&self, payload: ZBytes, attachment: Option<ZBytes>) {
        let Some(query) = self.query.lock().unwrap().take() else {
            tracing::debug!("{}: dropping an extra reply", self.route);
            return;
        };
        let route = self.route.clone();
        TOKIO_RUNTIME.spawn(async move {
            if let Err(e) = query
                .reply(query.key_expr().clone(), payload)
                .attachment(attachment)
                .await
            {
                tracing::warn!("{route}: replying failed: {e}");
            }
        });
    }

    fn reply_err(&self, payload: ZBytes) {
        if let Some(query) = self.query.lock().unwrap().take() {
            TOKIO_RUNTIME.spawn(async move {
                let _ = query.reply_err(payload).await;
            });
        }
    }
}

impl Drop for PendingQuery {
    fn drop(&mut self) {
        if let Some(query) = self.query.get_mut().unwrap().take() {
            tracing::warn!("{}: no reply for a request (timeout)", self.route);
            TOKIO_RUNTIME.spawn(async move {
                let _ = query
                    .reply_err(ZBytes::from("zenoh bridge: no reply (timeout)"))
                    .await;
            });
        }
    }
}

/// Everything a route holds: dropping it undeclares the zenoh entities and withdraws the tokens.
#[allow(dead_code)]
enum RouteHandles {
    Publisher {
        local_sub: AdvancedSubscriber<()>,
        remote_pub: Arc<AdvancedPublisher<'static>>,
    },
    Subscription {
        remote_sub: RemoteSubscriber,
        local_pub: Arc<AdvancedPublisher<'static>>,
    },
    Service {
        remote_queryable: Queryable<()>,
    },
    Client {
        local_queryable: Queryable<()>,
        querier: Arc<Querier<'static>>,
        /// dropping it stops the task that announces the local proxy while a remote server matches
        _stop: flume::Sender<()>,
    },
}

#[allow(dead_code)]
enum RemoteSubscriber {
    Plain(Subscriber<()>),
    Advanced(AdvancedSubscriber<()>),
}

struct Route {
    /// the liveliness token key expressions of the local entities this route serves
    local_entities: HashSet<String>,
    _handles: RouteHandles,
    _remote_token: LivelinessToken,
    /// the local proxy (a client route manages its own, see RouteHandles::Client)
    _local_token: Option<LivelinessToken>,
}

/// One route per local interface: name, kind and type (rmw_zenoh's data key contains the type)
#[derive(Clone, PartialEq, Eq, Hash)]
struct RouteKey {
    kind: Kind,
    name: String,
    rmw_type: String,
    type_hash: String,
}

impl RouteKey {
    fn of(entity: &RmwEntity) -> RouteKey {
        RouteKey {
            kind: entity.kind,
            name: entity.name.clone(),
            rmw_type: entity.rmw_type.clone(),
            type_hash: entity.type_hash.clone(),
        }
    }
}

impl fmt::Display for RouteKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let what = match self.kind {
            Kind::Publisher => "publisher",
            Kind::Subscription => "subscription",
            Kind::Service => "service",
            Kind::Client => "client",
        };
        write!(f, "rmw_zenoh {what} {} ({})", self.name, self.rmw_type)
    }
}

struct Bridge {
    config: Arc<Config>,
    domain: u32,
    /// the session towards the other bridges (the plugin's runtime)
    remote: Arc<Session>,
    remote_zid: OwnedKeyExpr,
    /// the session towards the local rmw_zenoh router
    local: Session,
    local_zid: String,
    /// GID of the bridge's local publications and requests
    gid: [u8; 16],
    nodename: String,
    next_entity_id: Arc<AtomicU64>,
    routes: HashMap<RouteKey, Route>,
}

pub(crate) async fn run(remote: Arc<Session>, config: Arc<Config>) {
    let domain = config.domain;
    tracing::info!(
        "rmw_zenoh: waiting for the local rmw_zenoh router at {:?} (ROS domain {domain})",
        config.rmw_zenoh_connect
    );
    let local = match open_local_session(&config).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("rmw_zenoh: cannot open the session to the local router: {e}");
            return;
        }
    };
    let local_zid = local.zid().to_string();
    let mut gid = [0u8; 16];
    let zid_bytes = local.zid().to_le_bytes();
    gid[..zid_bytes.len()].copy_from_slice(&zid_bytes);

    // only now, with the local nodes reachable, the other bridges see this one
    let _member = match remote
        .liveliness()
        .declare_token(
            keformat!(
                ke_liveliness_plugin::formatter(),
                zenoh_id = remote.zid().into_keyexpr()
            )
            .unwrap(),
        )
        .await
    {
        Ok(member) => member,
        Err(e) => {
            tracing::error!("rmw_zenoh: unable to declare the plugin's liveliness token: {e:?}");
            return;
        }
    };
    tracing::info!(
        "rmw_zenoh: bridging ROS domain {domain} of the router at {:?} (local zid {local_zid}) as {}",
        config.rmw_zenoh_connect,
        remote.zid()
    );

    let nodename = config.nodename.to_string();
    // the bridge's node in the local graph, owner of its proxy entities
    let node_token = match local
        .liveliness()
        .declare_token(format!(
            "{RMW_ADMIN_SPACE}/{domain}/{local_zid}/0/0/NN/%/%/{nodename}"
        ))
        .await
    {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("rmw_zenoh: cannot declare the bridge node's token: {e}");
            return;
        }
    };

    let entities = match local
        .liveliness()
        .declare_subscriber(format!("{RMW_ADMIN_SPACE}/{domain}/**"))
        .history(true)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("rmw_zenoh: cannot subscribe to the local graph: {e}");
            return;
        }
    };

    let mut bridge = Bridge {
        remote_zid: remote.zid().into_keyexpr(),
        config,
        domain,
        remote,
        local,
        local_zid,
        gid,
        nodename,
        next_entity_id: Arc::new(AtomicU64::new(1)),
        routes: HashMap::new(),
    };

    while let Ok(sample) = entities.recv_async().await {
        let ke = sample.key_expr().as_str();
        let Some(entity) = RmwEntity::parse(ke) else {
            continue;
        };
        if entity.zid == bridge.local_zid {
            // one of the bridge's own proxies
            continue;
        }
        match sample.kind() {
            SampleKind::Put => bridge.on_local_entity(ke, entity).await,
            SampleKind::Delete => bridge.on_local_entity_gone(ke, &entity),
        }
    }
    drop(node_token);
    tracing::info!("rmw_zenoh: bridge stopped");
}

/// A client of the local router. In client mode zenoh connects or fails, it has no background
/// connect: wait for the router (it may start after the bridge); once connected, the session
/// reconnects by itself when the router restarts.
async fn open_local_session(config: &Config) -> Result<Session, String> {
    let mut zconfig = zenoh::Config::default();
    let endpoints = serde_json::to_string(&config.rmw_zenoh_connect).map_err(|e| e.to_string())?;
    for (key, value) in [
        ("mode", r#""client""#),
        ("connect/endpoints", endpoints.as_str()),
        ("connect/timeout_ms", "-1"),
        ("scouting/multicast/enabled", "false"),
    ] {
        zconfig
            .insert_json5(key, value)
            .map_err(|e| format!("{key}={value}: {e}"))?;
    }
    zenoh::open(zconfig).await.map_err(|e| e.to_string())
}

impl Bridge {
    fn is_allowed(&self, entity: &RmwEntity) -> bool {
        let Some(allowance) = &self.config.allowance else {
            return true;
        };
        match entity.kind {
            Kind::Publisher => allowance.is_publisher_allowed(&entity.name),
            Kind::Subscription => allowance.is_subscriber_allowed(&entity.name),
            Kind::Service => allowance.is_service_srv_allowed(&entity.name),
            Kind::Client => allowance.is_service_cli_allowed(&entity.name),
        }
    }

    async fn on_local_entity(&mut self, ke: &str, entity: RmwEntity) {
        if entity.name.contains("/_action/") {
            tracing::debug!(
                "rmw_zenoh: {} skipped, actions are not bridged",
                entity.name
            );
            return;
        }
        if !self.is_allowed(&entity) {
            tracing::trace!("rmw_zenoh: {:?} {} not allowed", entity.kind, entity.name);
            return;
        }
        let key = RouteKey::of(&entity);
        if let Some(route) = self.routes.get_mut(&key) {
            route.local_entities.insert(ke.to_string());
            return;
        }
        match self.create_route(&key, &entity).await {
            Ok(mut route) => {
                tracing::info!(
                    "{key}: route created ({}, qos '{}')",
                    entity.type_hash,
                    entity.qos
                );
                route.local_entities.insert(ke.to_string());
                self.routes.insert(key, route);
            }
            Err(e) => tracing::error!("{key}: failed to create the route: {e}"),
        }
    }

    fn on_local_entity_gone(&mut self, ke: &str, entity: &RmwEntity) {
        let key = RouteKey::of(entity);
        if let Some(route) = self.routes.get_mut(&key) {
            route.local_entities.remove(ke);
            if route.local_entities.is_empty() {
                self.routes.remove(&key);
                tracing::info!("{key}: route removed (no local entity left)");
            }
        }
    }

    /// `<domain>/<name>/<type>/<hash>`: the key of the local entity's data, requests and replies
    fn local_key_expr(&self, entity: &RmwEntity) -> Result<OwnedKeyExpr, String> {
        OwnedKeyExpr::try_from(format!(
            "{}/{}/{}/{}",
            self.domain,
            entity.name.trim_matches('/'),
            entity.rmw_type,
            entity.type_hash
        ))
        .map_err(|e| e.to_string())
    }

    async fn create_route(&self, key: &RouteKey, entity: &RmwEntity) -> Result<Route, String> {
        let local_ke = self.local_key_expr(entity)?;
        let zenoh_ke = ros2_name_to_key_expr(&entity.name, &self.config);
        let ros2_type = dds_type_to_ros2_message_type(&entity.rmw_type);
        let qos = RmwQos::parse(&entity.qos);

        let (handles, remote_token_ke) = match entity.kind {
            Kind::Publisher => (
                self.route_local_publisher(key, &local_ke, &zenoh_ke, qos)
                    .await?,
                new_ke_liveliness_pub(
                    &self.remote_zid,
                    &zenoh_ke,
                    &ros2_type,
                    true,
                    &qos.to_dds(&entity.type_hash),
                )?,
            ),
            Kind::Subscription => (
                self.route_local_subscription(key, &local_ke, &zenoh_ke, qos)
                    .await?,
                new_ke_liveliness_sub(
                    &self.remote_zid,
                    &zenoh_ke,
                    &ros2_type,
                    true,
                    &qos.to_dds(&entity.type_hash),
                )?,
            ),
            Kind::Service => (
                self.route_local_service(key, &local_ke, &zenoh_ke).await?,
                new_ke_liveliness_service_srv(&self.remote_zid, &zenoh_ke, &ros2_type)?,
            ),
            Kind::Client => (
                self.route_local_client(key, entity, &local_ke, &zenoh_ke)
                    .await?,
                new_ke_liveliness_service_cli(&self.remote_zid, &zenoh_ke, &ros2_type)?,
            ),
        };

        // announce the route to the other bridges, as a DDS bridge does for the entity
        let remote_token = self
            .remote
            .liveliness()
            .declare_token(remote_token_ke)
            .await
            .map_err(|e| format!("announcement to the other bridges: {e}"))?;
        // and the proxy to the local graph (a client route announces its own while it matches)
        let local_token = if entity.kind == Kind::Client {
            None
        } else {
            Some(
                self.local
                    .liveliness()
                    .declare_token(self.proxy_token_key_expr(entity))
                    .await
                    .map_err(|e| format!("local proxy token: {e}"))?,
            )
        };

        Ok(Route {
            local_entities: HashSet::new(),
            _handles: handles,
            _remote_token: remote_token,
            _local_token: local_token,
        })
    }

    fn proxy_token_key_expr(&self, entity: &RmwEntity) -> String {
        proxy_token_key_expr(
            self.domain,
            &self.local_zid,
            self.next_entity_id.fetch_add(1, Ordering::Relaxed),
            &self.nodename,
            entity,
        )
    }

    /// local publisher -> other bridges
    async fn route_local_publisher(
        &self,
        key: &RouteKey,
        local_ke: &keyexpr,
        zenoh_ke: &keyexpr,
        qos: RmwQos,
    ) -> Result<RouteHandles, String> {
        let congestion_ctrl = match (self.config.reliable_routes_blocking, qos.best_effort) {
            (true, false) => CongestionControl::Block,
            _ => CongestionControl::Drop,
        };
        let (priority, express) = self
            .config
            .get_pub_priority_and_express(&key.name)
            .unwrap_or_default();
        let mut builder = self
            .remote
            .declare_publisher(zenoh_ke.to_owned())
            .advanced();
        if qos.transient_local {
            let cache = qos
                .history_samples()
                .saturating_mul(self.config.transient_local_cache_multiplier);
            builder = builder
                .cache(CacheConfig::default().max_samples(cache))
                .publisher_detection();
        }
        let remote_pub = Arc::new(
            builder
                .reliability(ZReliability::Reliable)
                .allowed_destination(Locality::Remote)
                .congestion_control(congestion_ctrl)
                .express(express)
                .priority(priority)
                .await
                .map_err(|e| format!("publisher on {zenoh_ke}: {e}"))?,
        );

        let route = key.to_string();
        let sender = spawn_sender(route.clone(), remote_pub.clone());
        let own_gid = self.gid;
        let mut builder = self
            .local
            .declare_subscriber(local_ke.to_owned())
            .advanced()
            .callback(move |sample: Sample| {
                // The history query of a transient local subscriber also reaches the cache of
                // this bridge's own local publisher on the topic (a cache answers any origin):
                // never send the other bridges' samples back to them.
                if parse_rmw_attachment(sample.attachment()).is_some_and(|(_, _, g)| g == own_gid) {
                    return;
                }
                // rmw_zenoh's payload is the CDR message with its 4 bytes header, as a DDS
                // bridge routes it; the attachment stays local
                queue(&route, &sender, (sample.payload().clone(), None));
            })
            // the bridge's own live publications on the same topic would loop back
            .allowed_origin(Locality::Remote);
        if qos.transient_local {
            builder = builder.history(
                HistoryConfig::default()
                    .detect_late_publishers()
                    .max_samples(qos.history_samples()),
            );
        }
        let local_sub = builder
            .await
            .map_err(|e| format!("local subscriber on {local_ke}: {e}"))?;

        Ok(RouteHandles::Publisher {
            local_sub,
            remote_pub,
        })
    }

    /// other bridges -> local subscription
    async fn route_local_subscription(
        &self,
        key: &RouteKey,
        local_ke: &keyexpr,
        zenoh_ke: &keyexpr,
        qos: RmwQos,
    ) -> Result<RouteHandles, String> {
        let mut builder = self.local.declare_publisher(local_ke.to_owned()).advanced();
        if qos.transient_local {
            let cache = qos
                .history_samples()
                .saturating_mul(self.config.transient_local_cache_multiplier);
            builder = builder
                .cache(CacheConfig::default().max_samples(cache))
                .publisher_detection();
        }
        let local_pub = Arc::new(
            builder
                .reliability(if qos.best_effort {
                    ZReliability::BestEffort
                } else {
                    ZReliability::Reliable
                })
                .congestion_control(CongestionControl::Drop)
                .allowed_destination(Locality::Remote)
                .await
                .map_err(|e| format!("local publisher on {local_ke}: {e}"))?,
        );

        let route = key.to_string();
        let sender = spawn_sender(route.clone(), local_pub.clone());
        let gid = self.gid;
        let sequence_number = AtomicI64::new(1);
        let callback = move |sample: Sample| {
            let attachment = rmw_attachment(sequence_number.fetch_add(1, Ordering::Relaxed), &gid);
            queue(
                &route,
                &sender,
                (sample.payload().clone(), Some(attachment)),
            );
        };
        let remote_sub = if qos.transient_local {
            RemoteSubscriber::Advanced(
                self.remote
                    .declare_subscriber(zenoh_ke.to_owned())
                    .advanced()
                    .callback(callback)
                    .allowed_origin(Locality::Remote)
                    .history(
                        HistoryConfig::default()
                            .detect_late_publishers()
                            .max_samples(qos.history_samples()),
                    )
                    .query_timeout(self.config.get_queries_timeout_tl_sub(&key.name))
                    .await
                    .map_err(|e| format!("subscriber on {zenoh_ke}: {e}"))?,
            )
        } else {
            RemoteSubscriber::Plain(
                self.remote
                    .declare_subscriber(zenoh_ke.to_owned())
                    .callback(callback)
                    .allowed_origin(Locality::Remote)
                    .await
                    .map_err(|e| format!("subscriber on {zenoh_ke}: {e}"))?,
            )
        };

        Ok(RouteHandles::Subscription {
            remote_sub,
            local_pub,
        })
    }

    /// requests of the other bridges -> local service
    async fn route_local_service(
        &self,
        key: &RouteKey,
        local_ke: &keyexpr,
        zenoh_ke: &keyexpr,
    ) -> Result<RouteHandles, String> {
        let querier = Arc::new(
            self.local
                .declare_querier(local_ke.to_owned())
                .consolidation(ConsolidationMode::None)
                .allowed_destination(Locality::Remote)
                .timeout(self.config.get_queries_timeout_service(&key.name))
                .await
                .map_err(|e| format!("local querier on {local_ke}: {e}"))?,
        );
        let route = key.to_string();
        let gid = self.gid;
        let sequence_number = Arc::new(AtomicI64::new(1));
        let remote_queryable = self
            .remote
            .declare_queryable(zenoh_ke.to_owned())
            .complete(true)
            .allowed_origin(Locality::Remote)
            .callback(move |query: Query| {
                let attachment =
                    rmw_attachment(sequence_number.fetch_add(1, Ordering::Relaxed), &gid);
                let request = query.payload().cloned().unwrap_or_default();
                let pending = PendingQuery::new(route.clone(), query);
                let querier = querier.clone();
                let route = route.clone();
                TOKIO_RUNTIME.spawn(async move {
                    let result = querier
                        .get()
                        .payload(request)
                        .attachment(attachment)
                        .callback(move |reply| match reply.result() {
                            // the local service replies with its rmw attachment, which stays local
                            Ok(sample) => pending.reply(sample.payload().clone(), None),
                            Err(e) => pending.reply_err(e.payload().clone()),
                        })
                        .await;
                    if let Err(e) = result {
                        tracing::warn!("{route}: request to the local service failed: {e}");
                    }
                });
            })
            .await
            .map_err(|e| format!("queryable on {zenoh_ke}: {e}"))?;
        Ok(RouteHandles::Service { remote_queryable })
    }

    /// requests of a local client -> service behind another bridge
    async fn route_local_client(
        &self,
        key: &RouteKey,
        entity: &RmwEntity,
        local_ke: &keyexpr,
        zenoh_ke: &keyexpr,
    ) -> Result<RouteHandles, String> {
        let querier = Arc::new(
            self.remote
                .declare_querier(zenoh_ke.to_owned())
                // a ROS service has a single replier: hand its reply over as soon as it arrives
                .consolidation(ConsolidationMode::None)
                .allowed_destination(Locality::Remote)
                .timeout(self.config.get_queries_timeout_service(&key.name))
                .await
                .map_err(|e| format!("querier on {zenoh_ke}: {e}"))?,
        );

        let route = key.to_string();
        let remote = querier.clone();
        let local_queryable = self
            .local
            .declare_queryable(local_ke.to_owned())
            .complete(true)
            .allowed_origin(Locality::Remote)
            .callback(move |query: Query| {
                let Some((sequence_number, _, client_gid)) =
                    parse_rmw_attachment(query.attachment())
                else {
                    tracing::warn!("{route}: request without rmw_zenoh attachment, dropped");
                    return;
                };
                let request = query.payload().cloned().unwrap_or_default();
                // the request id a DDS client would send: 8 bytes client GUID + sequence number
                let little_endian = is_cdr_little_endian(&request.to_bytes()).unwrap_or(true);
                let client_id = u64::from_le_bytes(client_gid[..8].try_into().unwrap());
                let request_id =
                    CddsRequestHeader::create(client_id, sequence_number as u64, little_endian);
                let pending = PendingQuery::new(route.clone(), query);
                let querier = remote.clone();
                let route = route.clone();
                TOKIO_RUNTIME.spawn(async move {
                    let result = querier
                        .get()
                        .payload(request)
                        .attachment(request_id.as_attachment())
                        .callback(move |reply| match reply.result() {
                            // rmw_zenoh's client matches the reply by the request's sequence
                            // number
                            Ok(sample) => pending.reply(
                                sample.payload().clone(),
                                Some(rmw_attachment(sequence_number, &client_gid)),
                            ),
                            Err(e) => pending.reply_err(e.payload().clone()),
                        })
                        .await;
                    if let Err(e) = result {
                        tracing::warn!("{route}: request to the other bridges failed: {e}");
                    }
                });
            })
            .await
            .map_err(|e| format!("local queryable on {local_ke}: {e}"))?;

        // The service server proxy, only while a remote server matches: with DDS, a client sees
        // the service when the bridge's DDS server exists, which it does only then.
        let matching = querier
            .matching_listener()
            .await
            .map_err(|e| format!("matching listener on {zenoh_ke}: {e}"))?;
        let (stop, stopped) = flume::bounded::<()>(1);
        let local = self.local.clone();
        let token_ke = proxy_token_key_expr(
            self.domain,
            &self.local_zid,
            self.next_entity_id.fetch_add(1, Ordering::Relaxed),
            &self.nodename,
            entity,
        );
        let route = key.to_string();
        TOKIO_RUNTIME.spawn(async move {
            let mut token: Option<LivelinessToken> = None;
            loop {
                let status = match select(matching.recv_async(), stopped.recv_async()).await {
                    Either::Left((Ok(status), _)) => status,
                    // the route is gone (or the querier): withdraw the proxy
                    _ => break,
                };
                if status.matching() && token.is_none() {
                    match local.liveliness().declare_token(&token_ke).await {
                        Ok(t) => {
                            tracing::debug!("{route}: remote service available");
                            token = Some(t);
                        }
                        Err(e) => tracing::warn!("{route}: local proxy token: {e}"),
                    }
                } else if !status.matching() && token.is_some() {
                    tracing::debug!("{route}: remote service gone");
                    token = None;
                }
            }
            drop(token);
        });

        Ok(RouteHandles::Client {
            local_queryable,
            querier,
            _stop: stop,
        })
    }
}

fn proxy_token_key_expr(
    domain: u32,
    local_zid: &str,
    id: u64,
    nodename: &str,
    entity: &RmwEntity,
) -> String {
    format!(
        "{RMW_ADMIN_SPACE}/{domain}/{local_zid}/0/{id}/{}/%/%/{nodename}/{}/{}/{}/{}",
        entity.kind.proxy_token(),
        mangle(&entity.name),
        mangle(&entity.rmw_type),
        mangle(&entity.type_hash),
        entity.qos
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_entity() {
        let e = RmwEntity::parse(
            "@ros2_lv/3/aac3178e146ba6f1fc6e6a4085e77f21/0/11/MS/%/%/amr_node/%amr3%fork_height/std_msgs::msg::dds_::Float32_/RIHS01_7170d3d8f841f7be3172ce5f4f59f3a4d7f63b0447e8b33327601ad64d83d6e2/:1:,1:,:,:,,",
        )
        .unwrap();
        assert_eq!(e.kind, Kind::Subscription);
        assert_eq!(e.name, "/amr3/fork_height");
        assert_eq!(e.rmw_type, "std_msgs::msg::dds_::Float32_");
        assert_eq!(
            dds_type_to_ros2_message_type(&e.rmw_type),
            "std_msgs/msg/Float32"
        );
        assert_eq!(
            RmwQos::parse(&e.qos),
            RmwQos {
                best_effort: false,
                transient_local: true,
                keep_all: false,
                depth: 1
            }
        );
        // a node has no topic
        assert!(RmwEntity::parse("@ros2_lv/3/aac3/0/0/NN/%/%/amr_node").is_none());
    }

    #[test]
    fn test_qos_defaults() {
        let q = RmwQos::parse("::,:,:,,");
        assert_eq!(
            q,
            RmwQos {
                best_effort: false,
                transient_local: false,
                keep_all: false,
                depth: 10
            }
        );
        let q = RmwQos::parse("2::,5:,:,:,,");
        assert!(q.best_effort);
        assert_eq!(q.depth, 5);
        let dds = q.to_dds("RIHS01_00");
        assert_eq!(dds.reliability.unwrap().kind, ReliabilityKind::BEST_EFFORT);
        assert_eq!(dds.user_data.unwrap(), b"typehash=RIHS01_00;".to_vec());
    }

    #[test]
    fn test_attachment_roundtrip() {
        let gid = [7u8; 16];
        let a = rmw_attachment(42, &gid);
        // rmw_zenoh: int64, int64, then the gid as a sequence (varint length 16 + bytes)
        let bytes = a.to_bytes();
        assert_eq!(bytes.len(), 8 + 8 + 1 + 16);
        assert_eq!(&bytes[..8], &42i64.to_le_bytes());
        assert_eq!(bytes[16], 16);
        let (seq, _, g) = parse_rmw_attachment(Some(&a)).unwrap();
        assert_eq!((seq, g), (42, gid));
    }
}
