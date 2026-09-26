# Lifecycle patch on 1.10.1

Base: `d8269b5bbca6bfadae61f830bfdbcf17a0a7b7cd`. Cargo.lock is unchanged.
This branch targets the standalone bridge. Dynamic plugin hot-unload is outside
its support contract.

The existing route manager is the only owner of route DDS resources. Matching
listeners hold weak observation cells, never endpoints. Each route incarnation
has its own cell, so a queued old callback cannot mutate its replacement.
The pinned Zenoh 1.10.1 implementation serializes matching callbacks and checks
current matching state before delivery. Callbacks perform no DDS/session work,
including during listener declaration/destruction (the historical #382/#533
lock cycle). The owner reconciles on notifications and a 250 ms tick; failed
activation retains intent and retries with 100 ms to 5 s backoff.

A service owns one request/reply pair. Construction rolls back on any failure;
ROS graph advertisement/withdrawal commits both endpoints together. Data callbacks
borrow revocable access, and retirement fences new writes before DDS deletion.
No access lock is held while Cyclone drains callbacks. The pinned CycloneDDS
implementation waits for callbacks before the listener argument is reclaimed.
Unprovable deletion quarantines the allocation and reports cleanup failure.

Local nodes are identified by participant GID and node name. Discovery derives
interfaces from the current ROS membership snapshot and current DDS endpoints;
a delayed disposal cannot remove a replacement participant or endpoint. Retention
and matching demand are distinct, including native Zenoh matches.

DDS writes cap reliability max_blocking_time at the DDS default 100 ms (a stricter
limit remains). Overloaded writes fail explicitly rather than preventing resource
retirement indefinitely. This does not change reliability matching. ROS graph
publication uses an unlocked snapshot and retries failure without losing concurrent
edits. Pending incoming queries expire even when no later message arrives; patched
callers communicate their timeout, while legacy/native callers use the receiving
bridge's configured timeout.

`@/<zid>/ros2/lifecycle` exposes build identity, reconciliation progress and owned
resource/cleanup/write-failure accounting. Admin data alone cannot detect every
orphan or establish application health. A timeout alone does not justify a restart.
Set BRIDGE_BUILD_ID to the exact commit when packaging. Test hooks are disabled by
default and must not be included in deployed binaries.

## Provenance

The participant identity and CString fixes preserve the isolated upstream #705
changes (`33e0078eb42aae13510836947467ecc46a5df9eb` and
`26a8533c67f9da6995e120f2003064754774e412`). The owner design supersedes the
background-listener workaround and incorporates the listener-lifetime intent of
#738 without resource mutation inside matching callbacks. Remaining changes are
local ownership, discovery convergence, bounded I/O/query lifetime and diagnostics.

## Verification

```
cargo fmt --all --check
cargo clippy --locked -p zenoh-plugin-ros2dds --all-targets -- -D warnings
ROS_DISTRO=jazzy cargo test --locked -p zenoh-plugin-ros2dds --lib
ROS_DISTRO=humble cargo test --locked -p zenoh-plugin-ros2dds --lib ros_discovery::tests::test_serde_prior_to_iron
BRIDGE_BUILD_ID=$(git rev-parse HEAD) cargo build --locked --release -p zenoh-bridge-ros2dds
```

The focused tests include real DDS failure rollback/callback drainage/backpressure,
retirement and stale observers, retry without another edge, graph publication
failure/concurrent edits, participant replacement, and discovery event permutations.
Full-binary ROS tests remain necessary: overlapping same-name restarts, remote route
return with matching held true, lease expiry/reconnect, real requests, duplicate
messages and endpoint/resource baselines after churn. A build is not a rollout gate.

Mixed 1.10.1/patched bridges retain wire compatibility, but the unpatched owner keeps
its defects. Rollout must preserve the existing ROS type/QoS contract. Changing the
type of an already retained route and dynamic plugin hot-unload are not covered.
Physical hardware, real workload and shadow-health validation precede fleet rollout.
