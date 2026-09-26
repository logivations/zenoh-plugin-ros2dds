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
    collections::HashSet,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use serde::Serialize;
use zenoh::{
    bytes::ZBytes,
    internal::buffers::{Buffer, ZBuf, ZSlice},
    key_expr::{keyexpr, OwnedKeyExpr},
    liveliness::LivelinessToken,
    query::{Query, Queryable},
    Wait,
};

use crate::{
    dds_endpoint::{DdsAccess, DdsEndpoint},
    dds_types::{DDSRawSample, TypeInfo},
    dds_utils::{
        dds_write, get_instance_handle, is_cdr_little_endian, serialize_local_nodes, CDR_HEADER_LE,
    },
    gid::Gid,
    liveliness_mgt::new_ke_liveliness_service_srv,
    pending_queries::{self, PendingQueries, RETENTION_PARAMETER},
    ros2_utils::{
        is_service_for_action, new_service_id, ros2_service_type_to_reply_dds_type,
        ros2_service_type_to_request_dds_type, CddsRequestHeader, QOS_DEFAULT_SERVICE,
    },
    route_lifecycle::{Retry, RouteLifecycle},
    routes_mgr::Context,
    serialize_option_as_bool, LOG_PAYLOAD,
};

// a route for a Service Server exposed in Zenoh as a Queryable
#[derive(Serialize)]
pub struct RouteServiceSrv {
    // the ROS2 Service name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    // the zenoh queryable used to expose the service server in zenoh.
    // `None` when route is created on a remote announcement and no local ROS2 Service Server discovered yet
    #[serde(rename = "is_active", serialize_with = "serialize_option_as_bool")]
    zenoh_queryable: Option<Queryable<()>>,
    #[serde(flatten, serialize_with = "serialize_proxy")]
    proxy: Option<ServiceServerProxy>,
    lifecycle: RouteLifecycle,
    announcement_retry: Retry,
    #[serde(skip)]
    type_info: Option<Arc<TypeInfo>>,
    // a liveliness token associated to this route, for announcement to other plugins
    #[serde(skip)]
    liveliness_token: Option<LivelinessToken>,
    // the list of remote routes served by this route ("<zenoh_id>:<zenoh_key_expr>"")
    remote_routes: HashSet<String>,
    // the list of nodes served by this route, keyed by (participant_gid, node_fullname) — #702.
    #[serde(flatten, serialize_with = "serialize_local_nodes")]
    local_nodes: HashSet<(Gid, String)>,
}

impl Drop for RouteServiceSrv {
    fn drop(&mut self) {
        // Fence data callbacks before unregistering their source. An already
        // queued query can run after Queryable::drop, but cannot borrow DDS.
        if let Some(proxy) = &self.proxy {
            proxy.req_writer.fence();
        }
        self.zenoh_queryable.take();
        self.proxy.take();
    }
}

impl fmt::Display for RouteServiceSrv {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Service Server (ROS:{} <-> Zenoh:{})",
            self.ros2_name, self.zenoh_key_expr
        )
    }
}

impl RouteServiceSrv {
    pub(crate) fn endpoint_count(&self) -> usize {
        usize::from(self.proxy.is_some()) * 2
    }
    pub(crate) fn is_active(&self) -> bool {
        self.zenoh_queryable.is_some()
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        type_info: &Option<Arc<TypeInfo>>,
        context: Context,
    ) -> Result<RouteServiceSrv, String> {
        let route_id = format!("Route Service Server (ROS:{ros2_name} <-> Zenoh:{zenoh_key_expr})");
        tracing::debug!("{route_id}: creation with type {ros2_type}");

        Ok(RouteServiceSrv {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            zenoh_queryable: None,
            proxy: None,
            lifecycle: RouteLifecycle::new(),
            announcement_retry: Retry::new(),
            type_info: type_info.clone(),
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    // Announce the route over Zenoh via a LivelinessToken
    async fn announce_route(&mut self) -> Result<(), String> {
        // For lifetime issue, redeclare the zenoh key expression that can't be stored in Self
        let declared_ke = self
            .context
            .zsession
            .declare_keyexpr(self.zenoh_key_expr.clone())
            .await
            .map_err(|e| format!("{self}: failed to declare KeyExpr: {e}"))?;

        // create the zenoh Queryable
        // if Reader is TRANSIENT_LOCAL, use a PublicationCache to store historical data
        let proxy = self.proxy.as_ref().ok_or("DDS proxy not available")?;
        let queries_in_progress = proxy.queries_in_progress.clone();
        let sequence_number = proxy.sequence_number.clone();
        let route_id: String = self.to_string();
        let client_guid = proxy.client_guid;
        let req_writer = proxy.req_writer.access();
        let retention = self
            .context
            .config
            .get_incoming_query_retention(&self.ros2_name);
        let queryable = Some(
            self.context
                .zsession
                .declare_queryable(&self.zenoh_key_expr)
                .callback(move |query| {
                    route_zenoh_request_to_dds(
                        query,
                        &queries_in_progress,
                        &sequence_number,
                        &route_id,
                        client_guid,
                        &req_writer,
                        retention,
                    )
                })
                .await
                .map_err(|e| {
                    format!(
                        "Failed create Queryable for key {} (rid={}): {e}",
                        self.zenoh_key_expr, declared_ke
                    )
                })?,
        );

        // if not for an Action (since actions declare their own liveliness)
        if !is_service_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_service_srv(
                &self.context.zsession.zid().into_keyexpr(),
                &self.zenoh_key_expr,
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
                        "Failed create LivelinessToken associated to route for Service Server {ros2_name}: {e}"
                    )
                })?
            );
        }
        self.zenoh_queryable = queryable;
        Ok(())
    }

    pub(crate) async fn reconcile(&mut self) {
        if let Some(proxy) = &self.proxy {
            proxy.queries_in_progress.expire(Instant::now());
        }
        let route_id = self.to_string();
        // A retained server route exposes a local client pair even before a
        // local server appears, so DDS discovery can converge in either order.
        self.lifecycle.set_desired(true);
        if let Err(error) = self.lifecycle.reconcile(&mut self.proxy, || {
            create_proxy(
                &self.ros2_name,
                &self.ros2_type,
                &self.zenoh_key_expr,
                &route_id,
                &self.context,
                &self.type_info,
            )
        }) {
            tracing::error!("{route_id}: activation failed: {error}");
        }
        if !self.local_nodes.is_empty()
            && self.proxy.is_some()
            && self.zenoh_queryable.is_none()
            && self.announcement_retry.ready()
        {
            let result = self.announce_route().await;
            if let Err(error) = self.announcement_retry.record(result) {
                tracing::error!("{route_id}: announcement failed: {error}");
            }
        }
    }

    // Retire the route over Zenoh removing the LivelinessToken
    fn retire_route(&mut self) {
        tracing::debug!("{self} retire");
        // Drop Zenoh Publisher and Liveliness token
        // The DDS Writer remains to be discovered by local ROS nodes
        self.zenoh_queryable = None;
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
    pub async fn add_local_node(&mut self, node_key: (Gid, String)) {
        self.local_nodes.insert(node_key);
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

struct ServiceServerProxy {
    rep_reader: DdsEndpoint,
    req_writer: DdsEndpoint,
    client_guid: u64,
    sequence_number: Arc<AtomicU64>,
    queries_in_progress: Arc<PendingQueries>,
}
impl Drop for ServiceServerProxy {
    fn drop(&mut self) {
        self.req_writer.fence();
        DdsEndpoint::withdraw_pair(&mut self.rep_reader, &mut self.req_writer);
    }
}
fn serialize_proxy<S: serde::Serializer>(
    proxy: &Option<ServiceServerProxy>,
    s: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap;
    let mut map = s.serialize_map(Some(4))?;
    if let Some(proxy) = proxy {
        map.serialize_entry("req_writer", &proxy.req_writer)?;
        map.serialize_entry("rep_reader", &proxy.rep_reader)?;
        let (pending, expired) = proxy.queries_in_progress.counts();
        map.serialize_entry("pending_queries", &pending)?;
        map.serialize_entry("expired_queries", &expired)?;
    } else {
        map.serialize_entry("req_writer", "")?;
        map.serialize_entry("rep_reader", "")?;
        map.serialize_entry("pending_queries", &0usize)?;
        map.serialize_entry("expired_queries", &0u64)?;
    }
    map.end()
}
fn create_proxy(
    ros2_name: &str,
    ros2_type: &str,
    zenoh_key_expr: &OwnedKeyExpr,
    route_id: &str,
    context: &Context,
    type_info: &Option<Arc<TypeInfo>>,
) -> Result<ServiceServerProxy, String> {
    let mut qos = QOS_DEFAULT_SERVICE.clone();
    qos.user_data =
        Some(format!("clientid= {};", new_service_id(&context.participant)?).into_bytes());
    let req_writer = DdsEndpoint::writer(
        context.participant,
        format!("rq{ros2_name}Request"),
        ros2_service_type_to_request_dds_type(ros2_type),
        true,
        qos.clone(),
    )?;
    let client_guid = get_instance_handle(req_writer.entity())?;
    let queries_in_progress = Arc::new(PendingQueries::default());
    let pending = queries_in_progress.clone();
    let route_id = route_id.to_owned();
    let key = zenoh_key_expr.clone();
    let rep_reader = DdsEndpoint::reader(
        context.participant,
        format!("rr{ros2_name}Reply"),
        ros2_service_type_to_reply_dds_type(ros2_type),
        type_info,
        true,
        qos,
        None,
        move |sample| route_dds_reply_to_zenoh(sample, key.clone(), &pending, &route_id),
    )?;
    let mut proxy = ServiceServerProxy {
        rep_reader,
        req_writer,
        client_guid,
        queries_in_progress,
        sequence_number: Arc::new(AtomicU64::new(0)),
    };
    DdsEndpoint::advertise_pair(
        &mut proxy.rep_reader,
        &mut proxy.req_writer,
        context.ros_discovery_mgr.clone(),
    );
    Ok(proxy)
}

fn route_zenoh_request_to_dds(
    query: Query,
    queries_in_progress: &PendingQueries,
    sequence_number: &AtomicU64,
    route_id: &str,
    client_guid: u64,
    req_writer: &DdsAccess,
    retention: Duration,
) {
    // Empty service requests may contain only the four-byte CDR header.
    let is_little_endian = query
        .payload()
        .and_then(|value| is_cdr_little_endian(value.to_bytes().as_ref()))
        .unwrap_or(true);

    // Try to get request_id from Query attachment (in case it comes from another bridge).
    // Otherwise, create one using client_guid + sequence_number
    let request_id = query
        .attachment()
        .and_then(|a| CddsRequestHeader::try_from(a).ok())
        .unwrap_or_else(|| {
            CddsRequestHeader::create(client_guid, sequence_number.fetch_add(1, Ordering::Relaxed))
        });

    // prepend request payload with a (client_guid, sequence_number) header as per rmw_cyclonedds here:
    // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/serdata.hpp#L73
    let dds_req_buf = if let Some(value) = query.payload() {
        // The query comes with some payload. It's expected to be the Request type encoded as CDR (including 4 bytes header)
        let zenoh_req_buf = value.to_bytes();
        if zenoh_req_buf.len() < 4 || zenoh_req_buf[1] > 1 {
            tracing::warn!("{route_id}: received invalid request: {zenoh_req_buf:0x?}");
            return;
        }

        // Send to DDS a buffer made of
        //  - the same CDR header coming with the query
        //  - the request_id as request header as per rmw_cyclonedds here:
        //    https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/serdata.hpp#L73
        //  - the remaining of query payload
        let mut dds_req_buf: Vec<u8> = Vec::new();
        dds_req_buf.extend_from_slice(&zenoh_req_buf[..4]);
        dds_req_buf.extend_from_slice(&request_id.to_bytes(is_little_endian));
        dds_req_buf.extend_from_slice(&zenoh_req_buf[4..]);
        dds_req_buf
    } else {
        // No query payload - send a request containing just client_guid + sequence_number
        // Send to DDS a buffer made of
        //  - a CDR header
        //  - the request_id as request header
        let mut dds_req_buf: Vec<u8> = CDR_HEADER_LE.into();
        dds_req_buf.extend_from_slice(&request_id.to_bytes(is_little_endian));
        dds_req_buf
    };

    if *LOG_PAYLOAD {
        tracing::debug!(
            "{route_id}: routing request {request_id} from Zenoh to DDS - payload: {dds_req_buf:02x?}"
        );
    } else {
        tracing::trace!(
            "{route_id}: routing request {request_id} from Zenoh to DDS - {} bytes",
            dds_req_buf.len()
        );
    }

    let deadline = pending_queries::deadline(
        query.parameters().get(RETENTION_PARAMETER),
        retention,
        Instant::now(),
    );
    let completed = req_writer.with(|writer| {
        let replaced = queries_in_progress.insert(request_id, query, deadline);
        let failed = if let Err(e) = dds_write(writer, dds_req_buf) {
            tracing::warn!("{route_id}: routing request from Zenoh to DDS failed: {e}");
            queries_in_progress.take(&request_id)
        } else {
            None
        };
        (replaced, failed)
    });
    // Both replacement and failed-write cleanup can send a response-final.
    drop(completed);
}

fn route_dds_reply_to_zenoh(
    sample: &DDSRawSample,
    zenoh_key_expr: OwnedKeyExpr,
    queries_in_progress: &PendingQueries,
    route_id: &str,
) {
    // Reply payload is expected to be the Response type encoded as CDR, including a 4 bytes CDR header,
    // the 16 bytes request_id (8 bytes client guid + 8 bytes sequence_number), and the reply payload. As per rmw_cyclonedds here:
    // https://github.com/ros2/rmw_cyclonedds/blob/2263814fab142ac19dd3395971fb1f358d22a653/rmw_cyclonedds_cpp/src/serdata.hpp#L73

    let z_bytes: ZBytes = sample.into();
    let slice: ZSlice = ZBuf::from(z_bytes).to_zslice();

    // Decompose the slice into 3 sub-slices (4 bytes header, 16 bytes request_id and payload)
    let (payload, request_id, header) = match (
        slice.subslice(20..slice.len()), // payload from index 20
        slice.subslice(4..20).map(|s| s.as_ref().try_into()), // request_id: 16 bytes from index 4
        slice.subslice(0..4),            // header: 4 bytes
        is_cdr_little_endian(slice.as_ref()), // check endianness flag
    ) {
        (Some(payload), Some(Ok(request_id)), Some(header), Some(is_little_endian)) => {
            let request_id = CddsRequestHeader::from_slice(request_id, is_little_endian);
            (payload, request_id, header)
        }
        _ => {
            tracing::warn!("{route_id}: received invalid request: {sample:0x?} (less than 20 bytes) dropping it");
            return;
        }
    };

    // Check if it's one of my queries in progress. Drop otherwise
    let query = queries_in_progress.take(&request_id);
    match query {
        Some(query) => {
            // route reply buffer stripped from request_id
            let mut zenoh_rep_buf = ZBuf::empty();
            zenoh_rep_buf.push_zslice(header);
            zenoh_rep_buf.push_zslice(payload);

            if *LOG_PAYLOAD {
                tracing::debug!("{route_id}: routing reply {request_id} from DDS to Zenoh - payload: {zenoh_rep_buf:02x?}");
            } else {
                tracing::trace!(
                    "{route_id}: routing reply {request_id} from DDS to Zenoh - {} bytes",
                    zenoh_rep_buf.len()
                );
            }

            if let Err(e) = query.reply(zenoh_key_expr, zenoh_rep_buf).wait() {
                tracing::warn!("{route_id}: routing reply for request {request_id} from DDS to Zenoh failed: {e}");
            }
        }
        None => tracing::trace!(
            "{route_id}: received response from DDS an unknown query: {request_id} - ignore it"
        ),
    }
}
