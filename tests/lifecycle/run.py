"""Exercise the complete bridge with isolated DDS domains and real ROS/Zenoh peers.

Requires ROS 2, rmw_cyclonedds_cpp, example_interfaces and eclipse-zenoh==1.10.1.
Use a disposable container, as in CI. All listeners bind to loopback.
"""

import argparse
import json
import os
from pathlib import Path
import signal
import struct
import subprocess
import sys
import time
import urllib.request

import zenoh


def eventually(check, timeout=20):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        value = check()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {check.__name__}")


class Lab:
    def __init__(self, binary, output):
        self.binary = str(binary.resolve())
        self.output = output.resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.processes = {}
        self.logs = []
        self.forced_shutdowns = []
        self.environment = dict(
            os.environ,
            RMW_IMPLEMENTATION="rmw_cyclonedds_cpp",
            ROS_AUTOMATIC_DISCOVERY_RANGE="LOCALHOST",
        )
        xml = self.output / "cyclonedds.xml"
        xml.write_text(
            """<CycloneDDS><Domain><General><Interfaces>
<NetworkInterface address="127.0.0.1"/></Interfaces><AllowMulticast>false</AllowMulticast>
</General><Discovery><ParticipantIndex>auto</ParticipantIndex><MaxAutoParticipantIndex>32</MaxAutoParticipantIndex>
<Peers><Peer Address="127.0.0.1"/></Peers><LeaseDuration>3s</LeaseDuration></Discovery></Domain></CycloneDDS>"""
        )
        self.environment["CYCLONEDDS_URI"] = str(xml)
        for side, domain in [("server", 181), ("camera", 182)]:
            config = {
                "mode": "peer",
                "scouting": {"multicast": {"enabled": False}},
                "transport": {"link": {"tx": {"lease": 10000}}},
                "plugins": {
                    "ros2dds": {
                        "domain": domain,
                        "queries_timeout": {"default": 2},
                        "allow": {
                            kind: ["/test/.*"]
                            for kind in [
                                "service_clients",
                                "service_servers",
                                "publishers",
                                "subscribers",
                            ]
                        },
                    }
                },
            }
            if side == "server":
                config["listen"] = {"endpoints": ["tcp/127.0.0.1:17447"]}
            else:
                config["connect"] = {"endpoints": ["tcp/127.0.0.1:17447"]}
                config["listen"] = {"endpoints": []}
            (self.output / f"{side}.json5").write_text(json.dumps(config))

    def start(self, name, command, domain, extra=None):
        assert name not in self.processes
        log = (self.output / f"{name}.log").open("a")
        self.logs.append(log)
        self.processes[name] = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=log,
            env=dict(self.environment, ROS_DOMAIN_ID=str(domain), **(extra or {})),
        )

    def stop(self, name, sig=signal.SIGINT):
        process = self.processes.pop(name)
        started = time.monotonic()
        forced = False
        process.send_signal(sig)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            forced = True
            self.forced_shutdowns.append(name)
            process.kill()
            process.wait(timeout=5)
        with (self.output / "shutdowns.jsonl").open("a") as log:
            log.write(json.dumps(dict(
                name=name, signal=sig.name, forced=forced,
                elapsed_s=time.monotonic() - started, returncode=process.returncode,
            )) + "\n")

    def bridge(self, side):
        self.start(
            side,
            [
                self.binary,
                "-c",
                str(self.output / f"{side}.json5"),
                "--rest-http-port",
                "127.0.0.1:" + ("18000" if side == "server" else "18001"),
            ],
            181 if side == "server" else 182,
            {
                "ROS2DDS_LIFECYCLE_FAULT_FILE": str(self.output / f"{side}.fault"),
                "RUST_LOG": "zenoh_plugin_ros2dds=debug",
            },
        )

    def node(self, role):
        self.start(
            "ros-" + role,
            [sys.executable, str(Path(__file__).with_name("ros_node.py")), role],
            182 if role == "camera" else 181,
        )

    def admin(self, side, path="lifecycle"):
        port = 18000 if side == "server" else 18001
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/@/local/ros2/{path}", timeout=3
        ) as response:
            return [row["value"] for row in json.load(response)]

    def health(self, side):
        return self.admin(side)[0]

    def latest_probe(self):
        path = self.output / "ros-probe.log"
        if path.exists():
            for line in reversed(path.read_text().splitlines()):
                try:
                    return json.loads(line)
                except ValueError:
                    continue
        return None

    def healthy(self):
        after = time.time()
        consecutive = 0
        last_wall = None

        def ready():
            nonlocal consecutive, last_wall
            for name, process in self.processes.items():
                assert process.poll() is None, f"{name} exited; see {name}.log"
            row = self.latest_probe()
            if row and row["wall"] >= after and row["wall"] != last_wall:
                last_wall = row["wall"]
                healthy = (
                    time.time() - row["wall"] < 3
                    and row["ready"] == row["ok"] == 24
                    and row["bad"] == 0
                    and row["duplicates"] == 0
                    and row["detections"] > 0
                    and len(row["readers"]) == len(row["writers"]) == 24
                    and all(n == 1 for n in row["readers"] + row["writers"])
                )
                consecutive = consecutive + 1 if healthy else 0
            return consecutive >= 3

        eventually(ready)
        for side in ("server", "camera"):
            h = self.health(side)
            assert (
                h["owned_dds_endpoints"] == h["live_dds_endpoints"]
                and h["cleanup_failures"] == 0
            ), h

    def fault(self, kind, **fields):
        path = self.output / "server.fault"
        for suffix in (".ack", ".reached", ".release"):
            path.with_suffix(suffix).unlink(missing_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(dict(kind=kind, **fields)))
        temporary.replace(path)
        eventually(lambda: path.with_suffix(".ack").exists())
        assert json.loads(path.with_suffix(".ack").read_text())["ok"]

    def close(self):
        for name in list(self.processes):
            # Resume a paused process before deterministic cleanup.
            self.processes[name].send_signal(signal.SIGCONT)
            self.stop(name)
        for log in self.logs:
            log.close()
        if sys.exc_info()[0] is None:
            assert not self.forced_shutdowns, (
                "Graceful shutdown required SIGKILL; see shutdowns.jsonl",
                self.forced_shutdowns,
            )


def service_proxies_absent(routes):
    """A client route serializes its pair, not an `is_active` field."""
    expected = {f"/test/frame_{i}" for i in range(24)}
    frames = [route for route in routes if route["ros2_name"] in expected]
    assert {route["ros2_name"] for route in frames} == expected, routes
    return all(route["req_reader"] == route["rep_writer"] == "" for route in frames)


def wire_checks(lab, session):
    for little in (True, False):
        payload = bytes([0, int(little), 0, 0]) + struct.pack(
            "<qq" if little else ">qq", 1, 2
        )
        replies = list(session.get("test/frame_0", payload=payload, timeout=3))
        assert len(replies) == 1 and replies[0].ok, replies
        data = bytes(replies[0].ok.payload)
        assert struct.unpack("<q" if data[1] else ">q", data[4:])[0] == 3
        empty = list(
            session.get("test/empty", payload=bytes([0, int(little), 0, 0]), timeout=3)
        )
        assert len(empty) == 1 and empty[0].ok, empty
        assert bytes(empty[0].ok.payload)[:2] in (b"\0\0", b"\0\1")

    def reply(query):
        data = bytes(query.payload)
        a, b = struct.unpack("<qq" if data[1] else ">qq", data[4:])
        little = a == 1
        query.reply(
            "test/native",
            bytes([0, int(little), 0, 0])
            + struct.pack("<q" if little else ">q", a + b),
        )

    queryable = session.declare_queryable("test/native", reply)
    try:
        lab.node("native")
        assert (
            lab.processes["ros-native"].wait(timeout=20) == 0
        ), "ROS client failed to receive native reply"
        lab.processes.pop("ros-native")
    finally:
        queryable.undeclare()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--faults", action="store_true")
    args = parser.parse_args()
    lab = Lab(args.bridge, args.output)
    try:
        lab.bridge("server")
        lab.bridge("camera")
        lab.node("camera")
        lab.node("probe")
        lab.healthy()
        assert lab.health("server")["test_hooks"] == args.faults
        config = zenoh.Config()
        config.insert_json5("mode", '"client"')
        config.insert_json5("connect/endpoints", '["tcp/127.0.0.1:17447"]')
        config.insert_json5("scouting/multicast/enabled", "false")
        with zenoh.open(config) as session:
            wire_checks(lab, session)
            print(
                "PASS: LE/BE requests, header-only requests and native LE/BE replies",
                flush=True,
            )
            lab.node("missing")
            commands = lab.processes["ros-missing"].stdin
            for _ in range(10):
                commands.write(b"create\n")
                commands.flush()
                eventually(
                    lambda: lab.admin("server", "route/service/cli/test/missing")
                )
                commands.write(b"destroy\n")
                commands.flush()
                eventually(
                    lambda: not lab.admin("server", "route/service/cli/test/missing")
                )
            lab.stop("ros-missing")
            lab.healthy()
            print(
                "PASS: no-server client creation/removal without listener deadlock (#382)",
                flush=True,
            )
            lab.stop("ros-camera", signal.SIGKILL)
            lab.node("camera")
            time.sleep(4)  # Observe after the retired participant's DDS lease.
            lab.healthy()
            print("PASS: same-name replacement after delayed dispose", flush=True)

            native_queries = 0

            def keep_matching(_query):
                nonlocal native_queries
                native_queries += 1
                # Only the ROS service may satisfy the probe's response oracle.

            native = session.declare_queryable(
                "test/frame_0", keep_matching, complete=False
            )
            try:
                lab.healthy()
                assert native_queries > 0, "Native queryable never received a request"
                lab.stop("ros-camera")
                eventually(
                    lambda: not any(
                        r["is_active"]
                        for r in lab.admin("camera", "route/service/srv/**")
                    )
                )
                before_return = native_queries
                lab.node("camera")
                lab.healthy()
                assert native_queries > before_return
            finally:
                native.undeclare()
            lab.healthy()
            print(
                "PASS: remote route returns while native matching stays true",
                flush=True,
            )

            if args.faults:
                for stage in range(8):
                    lab.stop("ros-camera")
                    eventually(lambda: lab.health("server")["owned_dds_endpoints"] == 1)
                    before = sum(
                        r["lifecycle"]["activation_failures"]
                        for r in lab.admin("server", "route/service/cli/**")
                    )
                    lab.fault("fail_creation", after=stage)
                    lab.node("camera")
                    lab.healthy()
                    after = sum(
                        r["lifecycle"]["activation_failures"]
                        for r in lab.admin("server", "route/service/cli/**")
                    )
                    assert after > before, (stage, before, after)
                print("PASS: failure/retry at every pair creation boundary", flush=True)
                # Positive control: the assertion must reject live proxy pairs.
                assert not service_proxies_absent(
                    lab.admin("server", "route/service/cli/**")
                )
                lab.fault("invalidate_reader", service="/test/frame_0")
                lab.stop("ros-camera")
                eventually(lambda: lab.health("server")["cleanup_failures"] > 0)
                assert lab.health("server")["recovery_required"]
                camera_zid = lab.health("camera")["zid"]
                server_zid = lab.health("server")["zid"]
                lab.node("camera")
                eventually(
                    lambda: len(lab.admin("server", "route/service/cli/**")) >= 24
                )
                eventually(lambda: service_proxies_absent(
                    lab.admin("server", "route/service/cli/**")
                ))
                # Independently observe DDS and real calls after the failed
                # recreation, rather than treating an admin flag as health.
                after = time.time()
                eventually(lambda: (
                    (row := lab.latest_probe()) is not None
                    and row["wall"] >= after
                    and row["ready"] == row["ok"] == 0
                    and all(n == 0 for n in row["readers"] + row["writers"])
                ))
                assert all(
                    route["lifecycle"]["desired"]
                    and route["lifecycle"]["consecutive_failures"] > 0
                    for route in lab.admin("server", "route/service/cli/**")
                    if route["ros2_name"].startswith("/test/frame_")
                )
                lab.stop("server", signal.SIGKILL)
                lab.bridge("server")
                lab.healthy()
                assert lab.health("camera")["zid"] == camera_zid
                assert lab.health("server")["zid"] != server_zid
                print(
                    "PASS: cleanup quarantine and targeted bridge recovery", flush=True
                )
            else:
                for side in ("server", "camera"):
                    lab.processes[side].send_signal(signal.SIGSTOP)
                    try:
                        time.sleep(12)  # Exceed both configured leases.
                    finally:
                        lab.processes[side].send_signal(signal.SIGCONT)
                    lab.healthy()
                print(
                    "PASS: DDS/Zenoh lease expiry and recovery on both sides",
                    flush=True,
                )
    finally:
        lab.close()


if __name__ == "__main__":
    main()
