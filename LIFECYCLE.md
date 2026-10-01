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
Unprovable deletion quarantines the allocation and latches cleanup failure. This
protects a possibly live callback argument from being freed or reused; it is not
a transient activation failure. Query destruction (which can send a Zenoh final
response) runs outside DDS access guards, including failed writes, replacement
and callbacks rejected after retirement.

Local nodes are identified by participant GID and node name. Discovery derives
interfaces from the current ROS membership snapshot and current DDS endpoints.
ROS graph updates reconcile membership; individual DDS events update only the
affected interface, preserving the eclipse-zenoh upstream's incremental
discovery. Removal clears only that component and prefers a surviving same-type
endpoint from the same node's membership. A compatible survivor preserves the
route; a different-type survivor withdraws the old interface before rediscovery.
An interface entry is dropped once every component is gone. Same-type transfers
between nodes in one participant snapshot add the new owner before removing the
old owner, while type changes retire incompatible resources first. A disposal that no interface component references
leaves the derived state untouched, so it cannot rewrite a live interface.
A delayed disposal cannot remove a replacement participant or endpoint. Retention
and matching demand are distinct, including native Zenoh matches.

Known discovery limit: overlapping nodes with the same full name inside one DDS
participant can lose bridge routes while their local DDS services still answer.
The existing graph deserializer overwrites duplicate-name entries; the native RMW
graph also associates/removes nodes by name. Immediate same-context replacement
can overlap native cleanup. Node-instance identity/input handling remains unresolved
for this case; the passing distinct-participant restart tests do not qualify it.

DDS writes replace an infinite reliability max_blocking_time with the DDS default
100 ms; finite application values are preserved. Retirement can wait for that
configured finite timeout; preserving a timeout above one second is logged at
writer creation, and a retirement fence that waits longer than a second on an
in-flight operation logs the wait. Overloaded writes fail explicitly rather than preventing
resource retirement indefinitely. That attempted data delivery is lost on write failure;
this is an intentional overload behavior change, not a delivery guarantee. Counters
and route logs expose the failures. This does not change reliability matching. ROS graph
publication uses an unlocked snapshot and retries failure without losing concurrent
edits. Pending incoming queries with an advertised timeout expire even when no
later message arrives. Legacy/native callers expose no deadline in the pinned
Zenoh API: their queries remain until a reply or route retirement, as upstream
does. The receiving bridge's outgoing timeout cannot safely limit those calls.
Invalid explicit timeout metadata is rejected before forwarding to DDS.
Retirement fences new requests and releases pending queries outside locks.

Request correlation uses numeric client/sequence identity, independent of CDR byte
order. Request and reply headers use their own payload's byte order, including
native Zenoh big-endian messages and header-only empty requests. The existing
`rqh` attachment format is unchanged. This also fixes a pre-existing 1.10.1
interoperability defect; an unpatched receiving bridge still has that defect.

`@/<zid>/ros2/lifecycle` exposes build identity, reconciliation progress and owned
resource/cleanup/write-failure accounting. Admin data alone cannot detect every
orphan or establish application health. A timeout alone does not justify a restart.
Set BRIDGE_BUILD_ID to the exact commit when packaging. Test hooks are disabled by
default and must not be included in deployed binaries.

## Cleanup failure recovery

`recovery_required: true` / `cleanup_failures > 0` means that this process could
not prove safe DDS cleanup. It refuses new route endpoints until restarted; already
healthy routes may continue. The latch must not be reset in place. The standalone
bridge has one participant, so a per-participant latch would have the same scope.

Before restarting **that bridge**, preserve its build ID, ZID/process identity,
`lifecycle`, `route/**`, `node/**`, `dds/**`, recent logs, independent DDS inventory
and real request outcomes. Use the existing supervisor to restart only the faulty
owner. Verify a new process/session, expected endpoints and successful real requests
afterward. Do not restart the opposite bridge if verification fails; escalate.
Admin timeout by itself remains unknown, not evidence of this failure.

These patches expose the failure and its manual recovery contract; they do not
enable automatic recovery. Automation still requires diagnostic persistence,
persistent owner/site restart budgets, verification and shadow qualification.
Transient `activation failed:` messages belong to the owner's existing retry path;
external log-string watchdogs must not kill this process during that backoff.

## Provenance

The participant identity and CString fixes preserve the isolated upstream #705
changes (`33e0078eb42aae13510836947467ecc46a5df9eb` and
`26a8533c67f9da6995e120f2003064754774e412`). The owner design supersedes the
background-listener workaround and incorporates the listener-lifetime intent of
#738 without resource mutation inside matching callbacks. Remaining changes are
local ownership, discovery convergence, bounded I/O/query lifetime and diagnostics.
Action retirement also removes the action's correct admin key, avoiding stale
entries. The byte-order and out-of-guard Query destruction corrections are local.

## Verification

```
rustup component add rustfmt clippy
cargo fmt --all --check
cargo clippy --locked -p zenoh-plugin-ros2dds --all-targets -- -D warnings
ROS_DISTRO=jazzy cargo test --locked -p zenoh-plugin-ros2dds --lib
ROS_DISTRO=humble cargo test --locked -p zenoh-plugin-ros2dds --lib ros_discovery::tests::test_serde_prior_to_iron
BRIDGE_BUILD_ID=$(git rev-parse HEAD) cargo build --locked --release -p zenoh-bridge-ros2dds
```

The focused tests include real DDS failure rollback/callback drainage/backpressure,
retirement and stale observers, retry without another edge, graph publication
failure/concurrent edits, atomic pair publication/withdrawal, periodic-reader
retirement with an owned sample, participant replacement and discovery permutations.
[Full-binary lifecycle regressions](tests/lifecycle/README.md) run in the ROS Jazzy
CI job with default and fault-enabled builds. They check real requests, independent
DDS counts, native matching, same-name replacement, no-server listener teardown,
lease recovery, each construction failure and cleanup quarantine/recovery.
Physical workload and sustained soak checks remain required. A build is not a rollout gate.

Mixed 1.10.1/patched bridges retain wire compatibility, but the unpatched owner keeps
its defects. Last-endpoint replacement by a different type is covered by discovery
and complete-binary regressions. Concurrent incompatible interfaces still share
one name-keyed route; this does not add per-type routes, general QoS migration or
dynamic plugin hot-unload.
Physical hardware, real workload and shadow-health validation precede fleet rollout.
