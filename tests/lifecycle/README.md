# Full-binary lifecycle regressions

Run in a disposable ROS 2 Jazzy container with `rmw_cyclonedds_cpp`,
`example_interfaces`, `std_msgs`, `std_srvs` and `eclipse-zenoh==1.10.1`.
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
cargo build --locked -p zenoh-bridge-ros2dds --features lifecycle-test-hooks
/tmp/lifecycle-venv/bin/python tests/lifecycle/run.py --bridge target/debug/zenoh-bridge-ros2dds --output /tmp/lifecycle-hooks --faults
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
a natural production trigger for cleanup failure.

These bounded regressions complement the real-DDS unit tests and ordinary ROS
service/action/topic integration tests. They are not a multi-day soak, physical
Jetson qualification or proof that admin accounting alone detects orphan resources.
