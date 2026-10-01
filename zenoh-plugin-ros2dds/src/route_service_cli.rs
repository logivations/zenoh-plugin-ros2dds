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
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use serde::Serialize;
use zenoh::{
    bytes::ZBytes,
    handlers::CallbackDrop,
    internal::buffers::{Buffer, ZBuf, ZSlice},
    key_expr::{keyexpr, OwnedKeyExpr},
    liveliness::LivelinessToken,
    matching::{MatchingListener, MatchingStatus},
    query::{Querier, Reply},
    sample::Locality,
    Wait,
};

use crate::{
    dds_endpoint::{DdsAccess, DdsEndpoint},
    dds_types::{DDSRawSample, TypeInfo},
    dds_utils::{dds_write, is_cdr_little_endian, serialize_local_nodes},
    gid::Gid,
    liveliness_mgt::new_ke_liveliness_service_cli,
    pending_queries::RETENTION_PARAMETER,
    ros2_utils::{
        is_service_for_action, new_service_id, ros2_service_type_to_reply_dds_type,
        ros2_service_type_to_request_dds_type, CddsRequestHeader, QOS_DEFAULT_SERVICE,
    },
    route_lifecycle::{Retry, RouteLifecycle},
    routes_mgr::Context,
    LOG_PAYLOAD,
};

// a route for a Service Client exposed in Zenoh as a Queryier
#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize)]
pub struct RouteServiceCli {
    // the ROS2 Service name
    ros2_name: String,
    // the ROS2 type
    ros2_type: String,
    // the Zenoh key expression used for routing
    zenoh_key_expr: OwnedKeyExpr,
    // the context
    #[serde(skip)]
    context: Context,
    #[serde(skip)]
    _zenoh_querier: Arc<Querier<'static>>,
    #[serde(serialize_with = "crate::config::serialize_duration_as_f32")]
    queries_timeout: Duration,
    #[serde(flatten, serialize_with = "serialize_proxy")]
    proxy: Option<ServiceClientProxy>,
    lifecycle: RouteLifecycle,
    announcement_retry: Retry,
    #[serde(skip)]
    matching_listener: Option<MatchingListener<()>>,
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

impl Drop for RouteServiceCli {
    fn drop(&mut self) {
        self.matching_listener.take();
        self.proxy.take();
    }
}

impl fmt::Display for RouteServiceCli {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Route Service Client (ROS:{} <-> Zenoh:{})",
            self.ros2_name, self.zenoh_key_expr
        )
    }
}

impl RouteServiceCli {
    #[cfg(feature = "lifecycle-test-hooks")]
    pub(crate) fn invalidate_reader_for_test(&self) -> Result<(), String> {
        self.proxy
            .as_ref()
            .ok_or("No service proxy to invalidate")?
            .req_reader
            .invalidate_for_test()
    }
    pub(crate) fn endpoint_count(&self) -> usize {
        usize::from(self.proxy.is_some()) * 2
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        ros2_name: String,
        ros2_type: String,
        zenoh_key_expr: OwnedKeyExpr,
        type_info: Option<Arc<TypeInfo>>,
        queries_timeout: Duration,
        context: Context,
    ) -> Result<RouteServiceCli, String> {
        tracing::debug!(
            "Route Service Client (ROS:{ros2_name} <-> Zenoh:{zenoh_key_expr}): creation with type {ros2_type} (queries_timeout={queries_timeout:#?})"
        );

        let zenoh_querier: Arc<Querier<'static>> = Arc::new(
            context
                .zsession
                .declare_querier(zenoh_key_expr.clone())
                .congestion_control(zenoh::qos::CongestionControl::Block)
                .allowed_destination(Locality::Remote)
                .timeout(queries_timeout)
                .await
                .map_err(|e| format!("Failed create Querier for key {zenoh_key_expr}: {e}",))?,
        );

        let lifecycle = RouteLifecycle::new(context.maintenance.clone());
        let announcement_retry = Retry::new(context.maintenance.clone());
        let observe = lifecycle.observer(context.matching_changed.clone());
        let matching_listener = zenoh_querier
            .matching_listener()
            .callback(move |status: MatchingStatus| observe(status.matching()))
            .await
            .map_err(|e| format!("Failed to declare matching listener: {e}"))?;

        Ok(RouteServiceCli {
            ros2_name,
            ros2_type,
            zenoh_key_expr,
            context,
            _zenoh_querier: zenoh_querier,
            queries_timeout,
            proxy: None,
            lifecycle,
            announcement_retry,
            matching_listener: Some(matching_listener),
            type_info,
            liveliness_token: None,
            remote_routes: HashSet::new(),
            local_nodes: HashSet::new(),
        })
    }

    // Announce the route over Zenoh via a LivelinessToken
    async fn announce_route(&mut self) -> Result<(), String> {
        // if not for an Action (since actions declare their own liveliness)
        if !is_service_for_action(&self.ros2_name) {
            // create associated LivelinessToken
            let liveliness_ke = new_ke_liveliness_service_cli(
                &self.context.zsession.zid().into_keyexpr(),
                &self.zenoh_key_expr,
                &self.ros2_type,
            )?;
            tracing::debug!("{self}: announce via token {liveliness_ke}");
            let ros2_name = self.ros2_name.clone();
            self.liveliness_token = Some(self.context.zsession
                .liveliness()
                .declare_token(liveliness_ke)
                .await
                .map_err(|e| {
                    format!(
                        "Failed create LivelinessToken associated to route for Service Client {ros2_name}: {e}"
                    )
                })?
            );
        }
        Ok(())
    }

    // Retire the route over Zenoh removing the LivelinessToken
    fn retire_route(&mut self) {
        tracing::debug!("{self}: retire");
        // Withdraw the announcement; DDS resources follow retained route demand.
        self.liveliness_token = None;
    }

    pub(crate) async fn reconcile(&mut self) {
        let result = self.lifecycle.reconcile(&mut self.proxy, || {
            create_proxy(
                &self.ros2_name,
                &self.ros2_type,
                &self.context,
                &self.type_info,
                &self._zenoh_querier,
                self.queries_timeout,
            )
        });
        if let Err(error) = result {
            self.lifecycle
                .log_activation_failure(&self.to_string(), &error);
        }
        if !self.local_nodes.is_empty()
            && self.liveliness_token.is_none()
            && !is_service_for_action(&self.ros2_name)
            && self.announcement_retry.ready_or_schedule()
        {
            let result = self.announce_route().await;
            if let Err(error) = self.announcement_retry.record(result) {
                tracing::error!("{self}: announcement failed: {error}");
            }
        }
    }

    #[inline]
    pub fn add_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .insert(format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self}: now serving remote routes {:?}", self.remote_routes);
    }

    #[inline]
    pub fn remove_remote_route(&mut self, zenoh_id: &str, zenoh_key_expr: &keyexpr) {
        self.remote_routes
            .remove(&format!("{zenoh_id}:{zenoh_key_expr}"));
        tracing::debug!("{self}: now serving remote routes {:?}", self.remote_routes);
        // Matching may also come from native queryables, independently of this set.
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
            tracing::debug!("{self}: now serving local nodes {:?}", self.local_nodes);
            // if last local node removed, retire the route
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

struct ServiceClientProxy {
    // Stop requests before destroying the reply writer (field drop order).
    req_reader: DdsEndpoint,
    rep_writer: DdsEndpoint,
}
impl Drop for ServiceClientProxy {
    fn drop(&mut self) {
        self.rep_writer.fence();
        DdsEndpoint::withdraw_pair(&mut self.req_reader, &mut self.rep_writer);
    }
}
fn serialize_proxy<S: serde::Serializer>(
    proxy: &Option<ServiceClientProxy>,
    s: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap;
    let mut map = s.serialize_map(Some(2))?;
    if let Some(proxy) = proxy {
        map.serialize_entry("req_reader", &proxy.req_reader)?;
        map.serialize_entry("rep_writer", &proxy.rep_writer)?;
    } else {
        map.serialize_entry("req_reader", "")?;
        map.serialize_entry("rep_writer", "")?;
    }
    map.end()
}
fn create_proxy(
    ros2_name: &str,
    ros2_type: &str,
    context: &Context,
    type_info: &Option<Arc<TypeInfo>>,
    querier: &Arc<Querier<'static>>,
    queries_timeout: Duration,
) -> Result<ServiceClientProxy, String> {
    let mut qos = QOS_DEFAULT_SERVICE.clone();
    qos.user_data =
        Some(format!("serviceid= {};", new_service_id(&context.participant)?).into_bytes());
    let rep_writer = DdsEndpoint::writer(
        context.participant,
        format!("rr{ros2_name}Reply"),
        ros2_service_type_to_reply_dds_type(ros2_type),
        true,
        qos.clone(),
    )?;
    let access = rep_writer.access();
    let querier = querier.clone();
    let route_id = format!(
        "Route Service Client (ROS:{ros2_name} <-> Zenoh:{})",
        querier.key_expr()
    );
    let req_reader = DdsEndpoint::reader(
        context.participant,
        format!("rq{ros2_name}Request"),
        ros2_service_type_to_request_dds_type(ros2_type),
        type_info,
        true,
        qos,
        None,
        move |sample| {
            route_dds_request_to_zenoh(&route_id, sample, &querier, access.clone(), queries_timeout)
        },
    )?;
    let mut proxy = ServiceClientProxy {
        req_reader,
        rep_writer,
    };
    DdsEndpoint::advertise_pair(
        &mut proxy.req_reader,
        &mut proxy.rep_writer,
        context.ros_discovery_mgr.clone(),
    );
    Ok(proxy)
}

fn route_dds_request_to_zenoh(
    route_id: &str,
    sample: &DDSRawSample,
    querier: &Arc<Querier<'static>>,
    rep_writer: DdsAccess,
    queries_timeout: Duration,
) {
    // Request payload is expected to be the Request type encoded as CDR, including a 4 bytes CDR header,
    // the 16 bytes request_id (8 bytes client guid + 8 bytes sequence_number), and the request payload. As per rmw_cyclonedds here:
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

    // route request buffer stripped from request_id
    let mut zenoh_req_buf = ZBuf::empty();

    let attachment = request_id.as_attachment(header.as_ref()[1] == 1);
    zenoh_req_buf.push_zslice(header);
    zenoh_req_buf.push_zslice(payload);

    if *LOG_PAYLOAD {
        tracing::debug!("{route_id}: routing request {request_id} from DDS to Zenoh - payload: {zenoh_req_buf:02x?}");
    } else {
        tracing::trace!(
            "{route_id}: routing request {request_id} from DDS to Zenoh - {} bytes",
            zenoh_req_buf.len()
        );
    }

    if let Err(e) = querier
        .get()
        .parameters(format!("{RETENTION_PARAMETER}={}", queries_timeout.as_millis()))
        .payload(zenoh_req_buf)
        .attachment(attachment)
        .with({
            let route_id1: String = route_id.to_string();
            let route_id2 = route_id.to_string();
            let reply_received1 = Arc::new(AtomicBool::new(false));
            let reply_received2 = reply_received1.clone();
            CallbackDrop {
                callback: move |reply| {
                        if !reply_received1.swap(true, std::sync::atomic::Ordering::Relaxed) {
                            route_zenoh_reply_to_dds(&route_id1, reply, request_id, &rep_writer)
                        } else {
                            tracing::warn!("{route_id1}: received more than 1 reply for request {request_id} - dropping the extra replies");
                        }
                    },
                drop: move || {
                    if !reply_received2.load(std::sync::atomic::Ordering::Relaxed) {
                        // There is no way to send an error message as a reply to a ROS Service Client !
                        // (sending an invalid message will make it crash...)
                        // We have no choice but to log the error and let the client hanging without reply, until a timeout (if set by the client)
                        tracing::warn!("{route_id2}: received NO reply for request {request_id} - cannot reply to client, it will hang until timeout");
                    }
                },
            }
        })
        .wait()
    {
        tracing::warn!("{route_id}: routing request {request_id} from DDS to Zenoh failed: {e}");
    }
}

fn route_zenoh_reply_to_dds(
    route_id: &str,
    reply: Reply,
    request_id: CddsRequestHeader,
    rep_writer: &DdsAccess,
) {
    match reply.result() {
        Ok(sample) => {
            let zenoh_rep_buf = sample.payload().to_bytes();
            if zenoh_rep_buf.len() < 4 || zenoh_rep_buf[1] > 1 {
                tracing::warn!(
                    "{route_id}: received invalid reply from Zenoh for {request_id}: {zenoh_rep_buf:0x?}"
                );
                return;
            }
            // route reply buffer re-inserting request_id (client_id + sequence_number)
            let mut dds_rep_buf: Vec<u8> = Vec::new();
            // copy CDR header
            dds_rep_buf.extend_from_slice(&zenoh_rep_buf[..4]);
            // add request_id
            dds_rep_buf.extend_from_slice(&request_id.to_bytes(zenoh_rep_buf[1] == 1));
            // add query payoad
            dds_rep_buf.extend_from_slice(&zenoh_rep_buf[4..]);

            if *LOG_PAYLOAD {
                tracing::debug!("{route_id}: routing reply for {request_id} from Zenoh to DDS - payload: {dds_rep_buf:02x?}");
            } else {
                tracing::trace!(
                    "{route_id}: routing reply for {request_id} from Zenoh to DDS - {} bytes",
                    dds_rep_buf.len()
                );
            }

            if let Some(Err(e)) = rep_writer.with(|writer| dds_write(writer, dds_rep_buf)) {
                tracing::warn!(
                    "{route_id}: routing reply for {request_id} from Zenoh to DDS failed: {e}"
                );
            }
        }
        Err(val) => {
            tracing::warn!("{route_id}: received error as reply for {request_id}: {val:?}");
        }
    }
}
