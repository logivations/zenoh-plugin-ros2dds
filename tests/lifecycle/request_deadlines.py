"""Check incoming deadlines with a real ROS service and native Zenoh caller.

Use the same disposable ROS/Zenoh environment as run.py, with a fresh output
directory. This starts one bridge; no receiving-side timeout can stand in for
the caller's deadline. Action services use this same incoming request path.
"""

import argparse
import json
from pathlib import Path
import signal
import struct
import sys
import time


def serve():
    import rclpy
    from example_interfaces.srv import AddTwoInts
    from rclpy.node import Node

    rclpy.init()
    node = Node("deadline_service")

    def reply(request, response):
        print(json.dumps({"event": "request", "id": request.b}), flush=True)
        time.sleep(request.a)
        response.sum = request.a + request.b
        print(json.dumps({"event": "reply", "id": request.b}), flush=True)
        return response

    node.create_service(AddTwoInts, "/test/deadline", reply)
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


def check_deadlines(lab):
    import zenoh
    from run import eventually

    config_path = lab.output / "server.json5"
    config = json.loads(config_path.read_text())
    config["plugins"]["ros2dds"]["queries_timeout"]["default"] = 1
    config_path.write_text(json.dumps(config))
    results = []

    def service_event(event, request_id):
        for line in (lab.output / "deadline-service.log").read_text().splitlines():
            try:
                if json.loads(line) == {"event": event, "id": request_id}:
                    return True
            except ValueError:
                pass
        return False

    def route():
        rows = lab.admin("server", "route/service/srv/test/deadline")
        return rows[0] if rows else None

    def active():
        try:
            current = route()
            return current and current["is_active"]
        except OSError:
            return False

    try:
        lab.bridge("server")
        lab.start("deadline-service", [sys.executable, __file__, "--serve"], 181)
        eventually(active)
        config = zenoh.Config.from_json5(json.dumps({
            "mode": "client", "connect": {"endpoints": ["tcp/127.0.0.1:17447"]},
            "scouting": {"multicast": {"enabled": False}},
        }))
        with zenoh.open(config) as session:
            last_explicit_deadline = time.monotonic()
            cases = [
                ("fast_unknown_deadline", None, 0, "reply"),
                ("slow_unknown_deadline", None, 2, "reply"),
                ("slow_explicit_deadline", "5000", 2, "reply"),
                ("explicit_expiry", "500", 2, "expire"),
                ("fast_after_expiry", None, 0, "reply"),
                ("malformed_deadline", "invalid", 0, "error"),
                ("overflow_deadline", "18446744073709551616", 0, "error"),
            ]
            for request_id, (name, timeout, delay, expected) in enumerate(cases, 1):
                selector = "test/deadline"
                if timeout is not None:
                    selector += "?__ros2dds_timeout_ms=" + timeout
                before = route()["expired_queries"]
                started = time.monotonic()
                if timeout is not None and expected != "error":
                    last_explicit_deadline = max(last_explicit_deadline, started + int(timeout) / 1000)
                replies = list(session.get(
                    selector, payload=b"\0\1\0\0" + struct.pack("<qq", delay, request_id), timeout=5,
                ))
                values, errors = [], []
                for reply in replies:
                    if reply.ok:
                        payload = bytes(reply.ok.payload)
                        values.append(struct.unpack("<q" if payload[1] else ">q", payload[4:])[0])
                    else:
                        errors.append(bytes(reply.err.payload).decode(errors="replace"))
                observed = {
                    "case": name, "elapsed_s": time.monotonic() - started,
                    "values": values, "errors": errors,
                }
                results.append(observed)
                (lab.output / "deadline-results.json").write_text(json.dumps(results, indent=2))
                print(json.dumps(observed), flush=True)
                if expected == "reply":
                    assert values == [delay + request_id] and not errors, observed
                elif expected == "expire":
                    assert not values and not errors, observed
                    assert route()["expired_queries"] == before + 1
                    assert route()["pending_queries"] == 0
                else:
                    assert not values and len(errors) == 1, observed
                    assert "Invalid __ros2dds_timeout_ms" in errors[0], observed
                    assert not service_event("request", request_id), "Invalid request reached DDS"
                if expected != "error":
                    eventually(lambda: service_event("reply", request_id))

            # Completed queries may leave one scheduled hint. Observe idle only
            # after every advertised deadline plus a full scan grace has elapsed.
            time.sleep(max(0, last_explicit_deadline + 1 - time.monotonic()))
            samples = []
            generation = route()["lifecycle"]["generation"]
            for _ in range(4):
                current, health = route(), lab.health("server")
                assert current["is_active"] and current["pending_queries"] == 0, current
                assert current["lifecycle"]["generation"] == generation
                assert health["owned_dds_endpoints"] == health["live_dds_endpoints"] == 2, health
                samples.append({"wall": time.time(), "health": health})
                if len(samples) < 4:
                    time.sleep(1)
            results.append({"case": "idle_live_route", "samples": samples})
            (lab.output / "deadline-results.json").write_text(json.dumps(results, indent=2))
            assert len({sample["health"]["reconciliation_sequence"] for sample in samples}) == 1, samples
            assert len({sample["health"]["zid"] for sample in samples}) == 1, samples
            print("PASS: live service route has zero maintenance scans during three idle seconds", flush=True)

            # A caller with no advertised deadline must remain pending past the
            # receiver's outgoing timeout, then finish when its route retires.
            caller_timeout = 15
            completion_bound = 2
            caller_started = time.monotonic()
            receiver = session.get(
                "test/deadline", payload=b"\0\1\0\0" + struct.pack("<qq", 30, 99), timeout=caller_timeout,
            )
            eventually(lambda: service_event("request", 99))
            time.sleep(1.5)
            assert route()["pending_queries"] == 1
            started = time.monotonic()
            lab.stop("deadline-service", signal.SIGKILL)
            eventually(lambda: route() is None)

            def retired_query_completed():
                try:
                    reply = receiver.try_recv()
                except zenoh.ZError:
                    # Zenoh 1.10.1's handler reports its closed reply channel
                    # with ZError; a pending empty channel returns None.
                    return True
                assert reply is None, "Retired route returned a reply"
                return False

            # This bound is below the caller's remaining deadline, so expiry
            # at the caller cannot masquerade as route-retirement cleanup.
            assert time.monotonic() - caller_started < caller_timeout - completion_bound, \
                "Insufficient caller budget to verify retirement"
            route_removed_at = time.monotonic()
            eventually(retired_query_completed, timeout=completion_bound)
            results.append({"case": "retirement_unknown_deadline", "elapsed_s": time.monotonic() - started,
                            "completion_after_route_removed_s": time.monotonic() - route_removed_at,
                            "pending_before_retirement": 1, "route_removed": True, "values": []})
            (lab.output / "deadline-results.json").write_text(json.dumps(results, indent=2))
            print("PASS: incoming deadline compatibility, explicit expiry/rejection and retirement", flush=True)
    finally:
        lab.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--serve", action="store_true")
    args = parser.parse_args()
    if args.serve:
        serve()
    else:
        from run import Lab
        assert args.bridge and args.output, "--bridge and --output are required"
        check_deadlines(Lab(args.bridge, args.output))
