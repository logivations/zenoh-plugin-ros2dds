//
// Copyright (c) 2026 Logivations (fork-only regression test, RTDTK-1026)
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//

//! Regression test for the "W2" missing-reactivation wedge of RouteServiceCli.
//!
//! Scenario (the deterministic "native" trigger of tools/w2_repro.py, in a
//! single process):
//!
//! - a local ROS 2 service client (r2r) makes the bridge create a Service
//!   Client route for the service;
//! - a native zenoh Queryable serves the service AND keeps the route's
//!   Querier matched the whole time;
//! - a native liveliness token mimicking a REMOTE bridge's Service Server
//!   route announcement (`@/<id>/@ros2_lv/SS/<ke>/<typ>`) drives the route's
//!   remote_routes: declared -> add_remote_route(), dropped ->
//!   remove_remote_route() (the bridge ignores its own announcements, so the
//!   token must come from the test session);
//! - dropping the token deactivates the route's DDS entities while the
//!   Querier stays matched: no MatchingStatus edge can ever fire again;
//! - re-declaring the token must RE-ACTIVATE the route (the fix). Unfixed
//!   (picks base d0e8a2e), the route stays wedged forever: local_nodes and
//!   remote_routes populated, req_reader/rep_writer empty, real calls hang.
//!
//! Also pins the activation idempotency guard via ros_discovery_info: after
//! the reactivation (and the matched edge that may follow it), the bridge
//! must advertise the route through exactly the one new Request Reader gid
//! and the one new Reply Writer gid; the first activation's gids must be
//! gone. A double activation (missing guard) would leave the old gids behind
//! in ros_discovery_info, and total gid counts would grow.

pub mod common;

use std::time::{Duration, Instant};

use r2r::{self, QosProfile};
use serde_derive::{Deserialize, Serialize};
use zenoh::Wait;

// the ROS 2 service under test is "/<TEST_SERVICE>"
const TEST_SERVICE: &str = "test_w2_reactivation_srv";
// a fake remote-bridge id for the SS announcement (must differ from the
// bridge session's zid, which is random)
const FAKE_BRIDGE_ID: &str = "beeffacefeed2026";
// '/' escaped as in liveliness_mgt::escape_slashes
const SS_TOKEN_TYPE: &str = "example_interfaces§srv§AddTwoInts";

const STATE_CHANGE_TIMEOUT: Duration = Duration::from_secs(30);
const CALL_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_PERIOD: Duration = Duration::from_millis(200);

#[derive(Serialize, Deserialize, PartialEq, Clone)]
struct AddTwoIntsRequest {
    a: i64,
    b: i64,
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
struct AddTwoIntsReply {
    sum: i64,
}

fn ss_token_ke() -> String {
    format!("@/{FAKE_BRIDGE_ID}/@ros2_lv/SS/{TEST_SERVICE}/{SS_TOKEN_TYPE}")
}

/// Fetch the admin-space JSON of the bridge's Service Client route for
/// TEST_SERVICE (zid wildcarded: the bridge runs in-process on its own
/// session).
async fn cli_route_json(session: &zenoh::Session) -> Option<serde_json::Value> {
    let replies = session
        .get(format!("@/*/ros2/route/service/cli/{TEST_SERVICE}"))
        .await
        .ok()?;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            if let Ok(value) =
                serde_json::from_slice::<serde_json::Value>(&sample.payload().to_bytes())
            {
                return Some(value);
            }
        }
    }
    None
}

fn gid_field(route: &serde_json::Value, field: &str) -> Option<String> {
    let value = route.get(field)?.as_str()?;
    if value.is_empty() || value == "UNKOWN_GUID" || value == "UNKNOWN_GUID" {
        None
    } else {
        Some(value.to_string())
    }
}

/// Poll the admin space until the cli route reaches the wanted handle state:
/// `active = true` -> both req_reader and rep_writer gids present;
/// `active = false` -> route present with both handles empty (the W2 wedge
/// precondition after remove_remote_route()).
async fn wait_route_handles(
    session: &zenoh::Session,
    active: bool,
    what: &str,
) -> serde_json::Value {
    let deadline = Instant::now() + STATE_CHANGE_TIMEOUT;
    let mut last: Option<serde_json::Value> = None;
    while Instant::now() < deadline {
        if let Some(route) = cli_route_json(session).await {
            let req_reader = gid_field(&route, "req_reader");
            let rep_writer = gid_field(&route, "rep_writer");
            let is_active = req_reader.is_some() && rep_writer.is_some();
            let is_empty = req_reader.is_none() && rep_writer.is_none();
            if (active && is_active) || (!active && is_empty) {
                return route;
            }
            last = Some(route);
        }
        tokio::time::sleep(POLL_PERIOD).await;
    }
    panic!("timeout waiting for {what}; last admin state: {last:?}");
}

/// Read the bridge's ros_discovery_info advertisement with a raw CycloneDDS
/// reader (TRANSIENT_LOCAL, so the probe receives the current state when it
/// joins late) and return the (reader gids, writer gids) advertised for the
/// bridge node `bridge_participant_hint` belongs to. ROS_DISTRO >= iron wire
/// format (16-byte gids), which is what the jazzy test environment uses.
mod ros_discovery_probe {
    use std::{collections::HashMap, ffi::CString, mem::MaybeUninit};

    use cyclors::{
        qos::{
            Durability, DurabilityKind, History, HistoryKind, Qos, Reliability, ReliabilityKind,
            DDS_INFINITE_TIME,
        },
        *,
    };
    use serde_derive::Deserialize;

    #[derive(Deserialize)]
    #[allow(dead_code)]
    pub struct WireNodeEntitiesInfo {
        pub node_namespace: String,
        pub node_name: String,
        pub reader_gid_seq: Vec<[u8; 16]>,
        pub writer_gid_seq: Vec<[u8; 16]>,
    }

    #[derive(Deserialize)]
    #[allow(dead_code)]
    pub struct WireParticipantEntitiesInfo {
        pub gid: [u8; 16],
        pub node_entities_info_seq: Vec<WireNodeEntitiesInfo>,
    }

    pub struct Probe {
        participant: dds_entity_t,
        reader: dds_entity_t,
        // latest ParticipantEntitiesInfo per participant gid, accumulated
        // across reads (dds_takecdr consumes the samples)
        state: HashMap<Vec<u8>, WireParticipantEntitiesInfo>,
    }

    impl Probe {
        pub fn new() -> Probe {
            let domain: u32 = std::env::var("ROS_DOMAIN_ID")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            unsafe {
                let participant =
                    dds_create_participant(domain, std::ptr::null(), std::ptr::null());
                assert!(participant > 0, "failed to create probe participant");
                let cton = CString::new("ros_discovery_info").unwrap().into_raw();
                let ctyn = CString::new("rmw_dds_common::msg::dds_::ParticipantEntitiesInfo_")
                    .unwrap()
                    .into_raw();
                let topic = cdds_create_blob_topic(participant, cton, ctyn, true);
                assert!(topic > 0, "failed to create probe blob topic");
                let mut qos = Qos::default();
                qos.durability = Some(Durability {
                    kind: DurabilityKind::TRANSIENT_LOCAL,
                });
                qos.reliability = Some(Reliability {
                    kind: ReliabilityKind::RELIABLE,
                    max_blocking_time: DDS_INFINITE_TIME,
                });
                // KEEP_ALL: the topic is keyless (single instance), so a
                // KEEP_LAST reader would let the r2r participant's sample
                // evict the bridge's one before the probe takes it
                qos.history = Some(History {
                    kind: HistoryKind::KEEP_ALL,
                    depth: 0,
                });
                let qos_native = qos.to_qos_native();
                let reader = dds_create_reader(participant, topic, qos_native, std::ptr::null());
                Qos::delete_qos_native(qos_native);
                drop(CString::from_raw(cton));
                drop(CString::from_raw(ctyn));
                assert!(reader > 0, "failed to create probe reader");
                Probe {
                    participant,
                    reader,
                    state: HashMap::new(),
                }
            }
        }

        /// Take all pending ros_discovery_info samples, keeping the latest
        /// per participant (iron+ wire format), and merge them into the
        /// accumulated per-participant state.
        pub fn read(&mut self) -> &HashMap<Vec<u8>, WireParticipantEntitiesInfo> {
            let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            unsafe {
                let mut zp: *mut ddsi_serdata = std::ptr::null_mut();
                #[allow(clippy::uninit_assumed_init)]
                let mut si = MaybeUninit::<[dds_sample_info_t; 1]>::uninit();
                while dds_takecdr(
                    self.reader,
                    &mut zp,
                    1,
                    si.as_mut_ptr() as *mut dds_sample_info_t,
                    DDS_ANY_STATE,
                ) > 0
                {
                    let si_init = si.assume_init();
                    if si_init[0].valid_data {
                        // extract the serialized payload (same mechanics as
                        // the plugin's DDSRawSample)
                        let mut data = ddsrt_iovec_t {
                            iov_base: std::ptr::null_mut(),
                            iov_len: 0,
                        };
                        let size = ddsi_serdata_size(zp);
                        let sdref = ddsi_serdata_to_ser_ref(zp, 0, size as usize, &mut data);
                        let bytes = std::slice::from_raw_parts(
                            data.iov_base as *const u8,
                            data.iov_len as usize,
                        )
                        .to_vec();
                        ddsi_serdata_to_ser_unref(sdref, &data);
                        if bytes.len() > 20 {
                            // key: participant gid = first 16 payload bytes
                            // (after the 4-byte CDR header)
                            map.insert(bytes[4..20].to_vec(), bytes);
                        }
                    }
                    ddsi_serdata_unref(zp);
                }
            }
            for (gid, bytes) in map {
                if let Ok(info) = cdr::deserialize_from::<_, WireParticipantEntitiesInfo, _>(
                    &bytes[..],
                    cdr::size::Infinite,
                ) {
                    self.state.insert(gid, info);
                }
            }
            &self.state
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            unsafe {
                dds_delete(self.reader);
                dds_delete(self.participant);
            }
        }
    }
}

/// All gids (readers, writers) the BRIDGE node advertises on
/// ros_discovery_info, as hex strings (the r2r client node also publishes
/// on this topic and is filtered out; config default nodename).
fn all_advertised_gids(probe: &mut ros_discovery_probe::Probe) -> (Vec<String>, Vec<String>) {
    let mut readers = Vec::new();
    let mut writers = Vec::new();
    for participant in probe.read().values() {
        for node in &participant.node_entities_info_seq {
            if node.node_name == "zenoh_bridge_ros2dds" {
                readers.extend(node.reader_gid_seq.iter().map(hex::encode));
                writers.extend(node.writer_gid_seq.iter().map(hex::encode));
            }
        }
    }
    (readers, writers)
}

/// Poll ros_discovery_info until the bridge advertises `want_reader` and
/// `want_writer`; returns the advertised (readers, writers) at that point.
async fn wait_bridge_adverts(
    probe: &mut ros_discovery_probe::Probe,
    want_reader: &str,
    want_writer: &str,
    forbid: Option<(&str, &str)>,
    what: &str,
) -> (Vec<String>, Vec<String>) {
    let deadline = Instant::now() + STATE_CHANGE_TIMEOUT;
    let mut last: Option<(Vec<String>, Vec<String>)> = None;
    while Instant::now() < deadline {
        let (readers, writers) = all_advertised_gids(probe);
        let wanted =
            readers.iter().any(|g| g == want_reader) && writers.iter().any(|g| g == want_writer);
        let clean = match forbid {
            Some((old_reader, old_writer)) => {
                !readers.iter().any(|g| g == old_reader) && !writers.iter().any(|g| g == old_writer)
            }
            None => true,
        };
        if wanted && clean {
            return (readers, writers);
        }
        last = Some((readers, writers));
        tokio::time::sleep(POLL_PERIOD).await;
    }
    panic!("timeout waiting for {what}; last advertised gids: {last:?}");
}

async fn call_service(
    client: &r2r::Client<r2r::example_interfaces::srv::AddTwoInts::Service>,
    a: i64,
    b: i64,
    what: &str,
) {
    let deadline = Instant::now() + STATE_CHANGE_TIMEOUT;
    loop {
        let request = r2r::example_interfaces::srv::AddTwoInts::Request { a, b };
        let pending = client.request(&request).expect("request send failed");
        match tokio::time::timeout(CALL_TIMEOUT, pending).await {
            Ok(Ok(reply)) => {
                assert_eq!(reply.sum, a + b, "{what}: wrong sum");
                return;
            }
            Ok(Err(e)) => panic!("{what}: request error: {e}"),
            Err(_) if Instant::now() < deadline => {
                // the client may still be (re)matching the bridge's fresh
                // DDS endpoints - retry until the deadline
                continue;
            }
            Err(_) => panic!("{what}: service call kept timing out - route wedged?"),
        }
    }
}

#[test]
fn test_w2_service_cli_reactivation() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Catch panics INSIDE block_on: unwinding through the test fn would drop
    // the Runtime, whose Drop waits for the (infinite) r2r spin_blocking
    // task and hangs the test binary forever. The runtime is detached with
    // shutdown_background() before the verdict is raised.
    let outcome = rt.block_on(futures::FutureExt::catch_unwind(
        std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(Duration::from_secs(240), scenario()).await
        }),
    ));
    rt.shutdown_background();
    match outcome {
        Ok(Ok(())) => println!("Test passed"),
        Ok(Err(_elapsed)) => panic!("W2 scenario timed out after 240 s"),
        Err(panic) => {
            let msg = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "non-string panic payload".to_string());
            panic!("W2 scenario failed: {msg}");
        }
    }
}

async fn scenario() {
    {
        common::init_env();
        // in-process bridge
        tokio::spawn(common::create_bridge());

        // native zenoh side: Queryable serving the service the whole time
        // (this is what keeps the bridge's Querier matched across the
        // announcement retire/return - no MatchingStatus edge ever fires)
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let _queryable = session
            .declare_queryable(TEST_SERVICE)
            .callback(|query| {
                let request: AddTwoIntsRequest =
                    cdr::deserialize(&query.payload().unwrap().to_bytes()).unwrap();
                let response = AddTwoIntsReply {
                    sum: request.a + request.b,
                };
                let data = cdr::serialize::<_, _, cdr::CdrLe>(&response, cdr::Infinite).unwrap();
                query.reply(TEST_SERVICE, data).wait().unwrap();
            })
            .await
            .unwrap();

        // fake remote-bridge announcement of a Service Server route
        let token = session
            .liveliness()
            .declare_token(ss_token_ke().as_str())
            .await
            .unwrap();

        // local ROS 2 client of the service
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "w2_ros_client", "").unwrap();
        let client = node
            .create_client::<r2r::example_interfaces::srv::AddTwoInts::Service>(
                &format!("/{}", TEST_SERVICE),
                QosProfile::default(),
            )
            .unwrap();
        let _spin = tokio::task::spawn_blocking(move || loop {
            node.spin_once(std::time::Duration::from_millis(100));
        });

        // phase 1: the route activates (matching callback) and serves the
        // announced remote route + the local node
        let route = wait_route_handles(&session, true, "initial activation").await;
        let first_req_reader = gid_field(&route, "req_reader").unwrap();
        let first_rep_writer = gid_field(&route, "rep_writer").unwrap();
        call_service(&client, 11, 31, "call before the wedge").await;

        // The route must be serving both sides before the retire: the W2
        // wedge needs local_nodes non-empty, or RetiredServiceSrv would
        // simply remove the unused route. The ROS graph attribution of the
        // client node arrives via ros_discovery_info, so poll for it.
        wait_route_serving(&session).await;

        // snapshot the advertised gid sets while healthy
        let mut probe = ros_discovery_probe::Probe::new();
        let (healthy_readers, healthy_writers) = wait_bridge_adverts(
            &mut probe,
            &first_req_reader,
            &first_rep_writer,
            None,
            "healthy ros_discovery_info advertisement",
        )
        .await;

        // phase 2: the announcement retires while the Queryable keeps the
        // Querier matched -> remove_remote_route() deactivates the entities
        // and no matching edge can ever re-activate them (the W2 wedge
        // precondition)
        drop(token);
        wait_route_handles(&session, false, "deactivation on announcement retire").await;

        // phase 3: the announcement returns. add_remote_route() must
        // re-activate the DDS entities (the fix); unfixed, this poll times
        // out with the route still showing empty handles.
        let _token = session
            .liveliness()
            .declare_token(ss_token_ke().as_str())
            .await
            .unwrap();
        let route = wait_route_handles(&session, true, "reactivation on announcement return").await;
        let new_req_reader = gid_field(&route, "req_reader").unwrap();
        let new_rep_writer = gid_field(&route, "rep_writer").unwrap();
        assert_ne!(
            new_req_reader, first_req_reader,
            "a fresh Reader is expected"
        );
        assert_ne!(
            new_rep_writer, first_rep_writer,
            "a fresh Writer is expected"
        );

        // the real call works again, without any bridge restart
        call_service(&client, 17, 25, "call after reactivation").await;

        // idempotency guard pin: after the reactivation (and any matched
        // edge delivered after it), ros_discovery_info must advertise the
        // new Reader/Writer gids, must NOT advertise the first activation's
        // gids anymore, and the totals must not have grown (1 reader + 1
        // writer for this route, set semantics upstream).
        let (readers, writers) = wait_bridge_adverts(
            &mut probe,
            &new_req_reader,
            &new_rep_writer,
            Some((&first_req_reader, &first_rep_writer)),
            "reactivated ros_discovery_info advertisement without the stale gids",
        )
        .await;
        assert_eq!(
            (readers.len(), writers.len()),
            (healthy_readers.len(), healthy_writers.len()),
            "advertised endpoint totals changed across the wedge cycle (duplicate activation?): healthy ({healthy_readers:?}, {healthy_writers:?}) vs now ({readers:?}, {writers:?})"
        );
    }
}

/// Poll until the cli route serves both an announced remote route and the
/// local ROS client node (graph attribution arrives via ros_discovery_info).
async fn wait_route_serving(session: &zenoh::Session) {
    let deadline = Instant::now() + STATE_CHANGE_TIMEOUT;
    let mut last: Option<serde_json::Value> = None;
    while Instant::now() < deadline {
        if let Some(route) = cli_route_json(session).await {
            let serving_remote = route["remote_routes"]
                .as_array()
                .is_some_and(|remotes| !remotes.is_empty());
            let serving_local = route["local_nodes"]
                .as_array()
                .is_some_and(|nodes| !nodes.is_empty());
            if serving_remote && serving_local {
                return;
            }
            last = Some(route);
        }
        tokio::time::sleep(POLL_PERIOD).await;
    }
    panic!("timeout waiting for the route to serve remote_routes + local_nodes; last admin state: {last:?}");
}
