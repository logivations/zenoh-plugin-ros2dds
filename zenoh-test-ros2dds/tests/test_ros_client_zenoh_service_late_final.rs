//
// Copyright (c) 2024 ZettaScale Technology
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

pub mod common;

use std::time::{Duration, Instant};

use r2r::{self, QosProfile};
use serde_derive::{Deserialize, Serialize};
use zenoh::Wait;

// The test service
const TEST_SERVICE_LATE_FINAL: &str = "test_service_late_final";

// How long the Zenoh service keeps the query open after it has replied. Shorter than the
// bridge's default queries_timeout (5 s), so without the fix the reply reaches the ROS client
// only when the final arrives, after this delay.
const FINAL_DELAY: Duration = Duration::from_secs(3);

// The reply itself is immediate: the ROS client must get it well before FINAL_DELAY.
const MAX_REPLY_LATENCY: Duration = Duration::from_secs(1);

#[cfg(test)]
#[derive(Serialize, Deserialize, PartialEq, Clone)]
struct AddTwoIntsRequest {
    a: i64,
    b: i64,
}

#[cfg(test)]
#[derive(Serialize, Deserialize, PartialEq, Clone)]
struct AddTwoIntsReply {
    sum: i64,
}

// Incident (RTDTK-1026, br.cs 2026-10-07): after a peer dropped and rejoined, the camera server
// router stopped delivering the final for queries to two cameras' get_frame services. The
// cameras replied within 40 ms, but the Route Service Client held each reply until the query
// timed out, so every ROS call took the full queries_timeout and callers with a shorter timeout
// never got a frame. A ROS service has exactly one replier: its reply must not wait for the final.
#[test]
fn test_ros_client_zenoh_service_late_final() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let (sender, receiver) = std::sync::mpsc::channel();

    rt.block_on(async {
        common::init_env();
        // Create zenoh-bridge-ros2dds
        tokio::spawn(common::create_bridge());

        let a = 1;
        let b = 2;

        // Zenoh service that replies at once but sends the final only FINAL_DELAY later
        let session = zenoh::open(zenoh::Config::default()).await.unwrap();
        let _queryable = session
            .declare_queryable(TEST_SERVICE_LATE_FINAL)
            .callback(|query| {
                let request: AddTwoIntsRequest =
                    cdr::deserialize(&query.payload().unwrap().to_bytes()).unwrap();
                let response = AddTwoIntsReply {
                    sum: request.a + request.b,
                };
                let data = cdr::serialize::<_, _, cdr::CdrLe>(&response, cdr::Infinite).unwrap();
                query.reply(TEST_SERVICE_LATE_FINAL, data).wait().unwrap();
                // The final is sent when the query is dropped
                std::thread::spawn(move || {
                    std::thread::sleep(FINAL_DELAY);
                    drop(query);
                });
            })
            .await
            .unwrap();

        // ROS client
        let ctx = r2r::Context::create().unwrap();
        let mut node = r2r::Node::create(ctx, "ros_client_late_final", "").unwrap();
        let client = node
            .create_client::<r2r::example_interfaces::srv::AddTwoInts::Service>(
                &format!("/{}", TEST_SERVICE_LATE_FINAL),
                QosProfile::default(),
            )
            .unwrap();

        // Node spin
        let _handler = tokio::task::spawn_blocking(move || loop {
            node.spin_once(std::time::Duration::from_millis(100));
        });

        // Wait for the environment to be ready
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Send the request and time the response
        let my_req = r2r::example_interfaces::srv::AddTwoInts::Request { a, b };
        let start = Instant::now();
        let resp = client.request(&my_req).unwrap().await.unwrap();
        let latency = start.elapsed();

        // Tell the main test thread, we're completed
        sender.send((resp.sum, latency)).unwrap();
    });

    let test_result = receiver.recv_timeout(common::DEFAULT_TIMEOUT);
    // Stop the tokio runtime
    // Note that we should shutdown the runtime before doing any check that might panic the test.
    // Otherwise, the tasks inside the runtime will never be completed.
    rt.shutdown_background();
    match test_result {
        Ok((sum, latency)) => {
            assert_eq!(sum, 3);
            assert!(
                latency < MAX_REPLY_LATENCY,
                "the reply took {latency:?}: it was held until the late final ({FINAL_DELAY:?})"
            );
        }
        Err(_) => {
            panic!("Test failed due to timeout.....");
        }
    }
}
