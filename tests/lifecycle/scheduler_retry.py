"""Prove one failed activation retries without another matching change.

Run with a lifecycle-test-hooks binary in the disposable environment from run.py.
One native queryable remains declared while one ROS client waits for its failed
DDS proxy, makes two real requests, then stays alive for resource inspection.
"""

import argparse
import json
from pathlib import Path
import struct
import sys


def client():
    import rclpy
    from rclpy.node import Node
    from ros_node import native_replies
    from run import eventually

    rclpy.init()
    node = Node("scheduler_retry_client")
    try:
        native_replies(node)

        def one_pair():
            readers = node.get_subscriptions_info_by_topic("rq/test/nativeRequest", no_mangle=True)
            writers = node.get_publishers_info_by_topic("rr/test/nativeReply", no_mangle=True)
            return len(readers) == len(writers) == 1

        eventually(one_pair)
        print("NATIVE_DDS_PAIR: one request reader, one reply writer", flush=True)
        print("RETRY_REQUESTS_COMPLETE", flush=True)
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


def check_retry(lab):
    import zenoh
    from run import eventually

    requests = []

    def health():
        try:
            rows = lab.admin("server")
            return rows[0] if rows else None
        except OSError:
            return None

    def route():
        rows = lab.admin("server", "route/service/cli/test/native")
        return rows[0] if rows else None

    def reply(query):
        data = bytes(query.payload)
        a, b = struct.unpack("<qq" if data[1] else ">qq", data[4:])
        little = a == 1
        query.reply(
            "test/native", bytes([0, int(little), 0, 0])
            + struct.pack("<q" if little else ">q", a + b),
        )
        requests.append({"a": a, "b": b, "reply": a + b})

    try:
        lab.bridge("server")
        before = eventually(health)
        assert before["test_hooks"], "The retry regression requires test hooks"
        lab.fault("fail_creation", after=0)
        config = zenoh.Config.from_json5(json.dumps({
            "mode": "client", "connect": {"endpoints": ["tcp/127.0.0.1:17447"]},
            "scouting": {"multicast": {"enabled": False}},
        }))
        with zenoh.open(config) as session:
            queryable = session.declare_queryable("test/native", reply)
            try:
                lab.start("retry-client", [sys.executable, __file__, "--client"], 181)
                first = eventually(route)
                result_path = lab.output / "retry-results.json"
                result_path.write_text(json.dumps({"first_route": first, "completed": False}, indent=2))

                def completed():
                    assert lab.processes["retry-client"].poll() is None, \
                        "ROS client exited before recovery; see retry-client.log"
                    return "RETRY_REQUESTS_COMPLETE" in (lab.output / "retry-client.log").read_text()

                eventually(completed)
                current, after = route(), health()
                assert current["lifecycle"]["generation"] == first["lifecycle"]["generation"]
                assert current["lifecycle"]["desired"]
                assert current["lifecycle"]["activation_failures"] == 1, current
                assert current["lifecycle"]["consecutive_failures"] == 0, current
                assert current["req_reader"] and current["rep_writer"], current
                assert len(lab.admin("server", "route/service/cli/**")) == 1
                assert after["owned_dds_endpoints"] == after["live_dds_endpoints"] == 2, after
                assert after["owned_matching_listeners"] == 1, after
                assert after["cleanup_failures"] == 0 and after["pid"] == before["pid"], after
                assert after["zid"] == before["zid"], after
                assert sorted(requests, key=lambda request: request["a"]) == [
                    {"a": 1, "b": 2, "reply": 3}, {"a": 2, "b": 2, "reply": 4},
                ], requests
                result_path.write_text(json.dumps({
                    "first_route": first, "recovered_route": current,
                    "health": after, "requests": requests, "completed": True,
                }, indent=2))
                print("PASS: one activation failure, unchanged matching, two real replies, same route/process", flush=True)
            finally:
                queryable.undeclare()
    finally:
        lab.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--client", action="store_true")
    args = parser.parse_args()
    if args.client:
        client()
    else:
        from run import Lab
        assert args.bridge and args.output, "--bridge and --output are required"
        check_retry(Lab(args.bridge, args.output))
