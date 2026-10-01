# Full-binary lifecycle regressions

Run in a disposable ROS 2 Jazzy container with `rmw_cyclonedds_cpp`,
`example_interfaces`, `std_msgs`, `sensor_msgs`, `std_srvs` and `eclipse-zenoh==1.10.1`.
The existing Jazzy CI job installs these dependencies and retains process logs.
The Python driver starts the actual binary twice, with ROS service/client nodes
and native Zenoh peers. It uses loopback, DDS domains 181/182 and ports
17447/18000/18001; do not run concurrent copies in the same network namespace.

```sh
source /opt/ros/jazzy/setup.bash
python3 -m venv --system-site-packages /tmp/lifecycle-venv
/tmp/lifecycle-venv/bin/pip install eclipse-zenoh==1.10.1
cargo build --locked -p zenoh-bridge-ros2dds
/tmp/lifecycle-venv/bin/python tests/lifecycle/run.py --bridge target/debug/zenoh-bridge-ros2dds --output /tmp/lifecycle-default
/tmp/lifecycle-venv/bin/python tests/lifecycle/request_deadlines.py --bridge target/debug/zenoh-bridge-ros2dds --output /tmp/lifecycle-deadlines
/tmp/lifecycle-venv/bin/python tests/lifecycle/type_transition.py --bridge target/debug/zenoh-bridge-ros2dds --output /tmp/lifecycle-type-transition
cargo build --locked -p zenoh-bridge-ros2dds --features lifecycle-test-hooks
/tmp/lifecycle-venv/bin/python tests/lifecycle/run.py --bridge target/debug/zenoh-bridge-ros2dds --output /tmp/lifecycle-hooks --faults
/tmp/lifecycle-venv/bin/python tests/lifecycle/scheduler_retry.py --bridge target/debug/zenoh-bridge-ros2dds --output /tmp/lifecycle-retry
```

Use a fresh output directory for each run. Processes are stopped on success and
failure. Default builds must have no `LAB-ONLY lifecycle fault` marker; the hooks
build must contain it. Hooks are only for tests and must never be packaged.

Both modes require 24/24 real service replies, one independently observed DDS
request reader/reply writer per service and no duplicate detections after each
transition. They cover LE/BE request and response correlation, header-only
requests, same-name node replacement, remote ROS route return while a native
queryable keeps matching true (D3), and repeated client creation/removal with no
server (historical #382/#533).

Default mode exceeds DDS and Zenoh leases on each bridge, then requires recovery
without restarting either. Hooks mode fails each of the eight pair construction
boundaries and requires automatic retry without duplicates. It deliberately
invalidates an owned DDS reader to verify fatal cleanup quarantine, diagnostics
and recovery by restarting only the affected server bridge. The unaffected camera
must retain its session. This deliberate ownership violation does not establish
a natural production trigger for cleanup failure. The quarantine assertion is
first checked against live pairs, then requires empty pair GUIDs, independently
absent DDS endpoints and failed real calls before recovery. Unexpected SIGKILL
escalation fails the test; shutdown timings are retained in `shutdowns.jsonl`.

The deadline test receives a valid two-second native reply despite a one-second
receiving bridge timeout, checks explicit expiry and malformed deadline rejection,
then requires another real reply after expiry. After all known deadlines drain,
a live service route must show no reconciliation-sequence increments for three
idle seconds. It also retires a route with a pending request and requires caller
completion before the caller timeout. The type-transition test replaces an
Image writer with a String writer in the same participant and requires both
post-replacement String batches to arrive through a new correctly typed proxy.
It uses domain 196 and ports 17478/18178 in the same isolated namespace.

The focused retry test has one native queryable and one ROS client. It injects
one failed endpoint creation, keeps matching unchanged, and requires two correct
replies through the same recovered route without a bridge restart. The many-route
fault test alone cannot prove this: other routes' matching events might cause its
retry. As a negative control in a disposable source copy, replace only the
`maintenance.wait(maintenance_not_before)` select arm with a permanently pending
future; retain matching notifications and test-hook polling. The focused retry
test must fail waiting for service readiness. Do not ship that control. A build
with the old 250 ms polling interval must separately fail the deadline helper's
idle-sequence assertion; this distinguishes correctness from the idle CPU fix.

These bounded regressions complement the real-DDS unit tests and ordinary ROS
service/action/topic integration tests. They are not a multi-day soak, physical
Jetson qualification or proof that admin accounting alone detects orphan resources.

For a release-mode discovery CPU comparison (no DDS/network timing), run:

```sh
cargo test --locked --release -p zenoh-plugin-ros2dds --lib discovery_churn_benchmark -- --ignored --nocapture
```

It sends endpoints **after** their ROS graph, both into empty participants and
participants already containing up to 9,000 services, with up to 12,000 total
services in one or 512 nodes, and measures individual additions and disposals.
Run it on the same host/toolchain against the baseline;
wall-clock thresholds are intentionally not asserted in shared CI. Complete-binary
CPU measurements must also cover paced node teardown and measure settled idle
separately: including `destroy_node()` in an idle interval mislabels teardown CPU.

Also compare the dense-survivor and repeated-metadata workloads:

```sh
cargo test --locked --release -p zenoh-plugin-ros2dds --lib survivor_shape_benchmark -- --ignored --nocapture
```

The dense case removes the currently selected endpoint at every step and checks
the exact replacement and final withdrawal. The update case changes canonical
QoS metadata without changing identity and checks the final values. These cover
costs that a many-topic creation benchmark cannot reveal. Record both creation
and disposal costs and retain raw repeated results; no timing threshold runs in CI.
