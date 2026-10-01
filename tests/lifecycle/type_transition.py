"""Require a live topic route to recover when its last old-type writer disappears.

Run with the lifecycle lab's ROS/Zenoh Python environment. Only two child
processes are started; all network traffic is loopback on the selected domain.
Use a fresh output directory. Cleanup escalation is recorded in cleanup.json.
"""

import argparse
from contextlib import suppress
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request

IMAGE = "sensor_msgs/msg/Image"
STRING = "std_msgs/msg/String"
TOPIC = "/test_type_transition"


def producer(directory):
    import rclpy
    from rclpy.node import Node
    from sensor_msgs.msg import Image
    from std_msgs.msg import String

    rclpy.init()
    node = Node("type_transition_test")
    image_writer = node.create_publisher(Image, TOPIC, 10)
    string_writer = None
    try:
        for line in sys.stdin:
            command = json.loads(line)
            op = command["op"]
            if op in ("publish_image", "publish_string"):
                writer = image_writer if op == "publish_image" else string_writer
                until = time.monotonic() + 3
                while writer.get_subscription_count() == 0 and time.monotonic() < until:
                    time.sleep(0.02)
            if op == "publish_image":
                for _ in range(10):
                    image_writer.publish(Image())
                    time.sleep(0.02)
            elif op == "add_string":
                string_writer = node.create_publisher(String, TOPIC, 10)
            elif op == "drop_image":
                node.destroy_publisher(image_writer)
                image_writer = None
            elif op == "publish_string":
                for index in range(10):
                    string_writer.publish(String(data=f"{command['marker']}-{index}"))
                    time.sleep(0.02)
            elif op == "refresh_graph":
                node.create_publisher(String, "/test_type_refresh", 10)
            elif op != "inventory":
                raise AssertionError(op)
            value = {
                "id": command["id"],
                "op": op,
                "writers": [
                    {"type": e.topic_type, "gid": list(e.endpoint_gid), "node": e.node_name}
                    for e in node.get_publishers_info_by_topic(TOPIC)
                ],
                "readers": [
                    {"type": e.topic_type, "gid": list(e.endpoint_gid), "node": e.node_name}
                    for e in node.get_subscriptions_info_by_topic(TOPIC)
                ],
            }
            target = directory / f"ack-{command['id']}.json"
            temporary = target.with_suffix(".tmp")
            temporary.write_text(json.dumps(value, indent=2))
            temporary.replace(target)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


def run(bridge, output, domain=196, zenoh_port=17478, rest_port=18178):
    import zenoh

    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    assert not any(output.iterdir()), "Use a fresh output directory (stale acknowledgements are unsafe)"
    xml = output / "cyclonedds.xml"
    xml.write_text(
        """<CycloneDDS><Domain><General><Interfaces>
<NetworkInterface address="127.0.0.1"/></Interfaces><AllowMulticast>false</AllowMulticast>
</General><Discovery><ParticipantIndex>auto</ParticipantIndex><MaxAutoParticipantIndex>20</MaxAutoParticipantIndex>
<Peers><Peer Address="127.0.0.1"/></Peers><LeaseDuration>3s</LeaseDuration></Discovery></Domain></CycloneDDS>"""
    )
    endpoint = f"tcp/127.0.0.1:{zenoh_port}"
    config = output / "bridge.json5"
    config.write_text(json.dumps({
        "mode": "peer",
        "listen": {"endpoints": [endpoint]},
        "scouting": {"multicast": {"enabled": False}},
        "plugins": {"ros2dds": {"domain": domain, "allow": {"publishers": [TOPIC]}}},
    }))
    environment = dict(
        os.environ,
        ROS_DOMAIN_ID=str(domain),
        CYCLONEDDS_URI=str(xml),
        RMW_IMPLEMENTATION="rmw_cyclonedds_cpp",
        ROS_AUTOMATIC_DISCOVERY_RANGE="LOCALHOST",
        RUST_LOG="zenoh_plugin_ros2dds=debug",
    )
    processes, logs, received, results = {}, [], [], []
    session = subscriber = None
    command_id = 0

    def alive():
        for name, process in processes.items():
            assert process.poll() is None, f"{name} exited; see {output / (name + '.log')}"

    def wait_for(check, timeout=5):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            alive()
            if check():
                return True
            time.sleep(0.05)
        return False

    def admin():
        try:
            url = f"http://127.0.0.1:{rest_port}/@/local/ros2/route/topic/pub/**"
            with urllib.request.urlopen(url, timeout=2) as response:
                return [row["value"] for row in json.load(response)]
        except (OSError, ValueError):
            return []

    def proxy_ready(typ):
        routes = admin()
        return len(routes) == 1 and routes[0]["ros2_type"] == typ and routes[0]["dds_reader"]

    def command(op, **fields):
        nonlocal command_id
        command_id += 1
        process = processes["producer"]
        process.stdin.write(json.dumps({"id": command_id, "op": op, **fields}) + "\n")
        process.stdin.flush()
        path = output / f"ack-{command_id}.json"
        assert wait_for(path.exists), f"Producer command timed out: {op}"
        return json.loads(path.read_text())

    def record(phase, op=None):
        start = len(received)
        if op:
            command(op, marker=phase)
            wait_for(lambda: len(received) - start >= 10, timeout=3)
        payloads = received[start:]
        row = {
            "phase": phase,
            "received": len(payloads),
            "string_markers": sum(phase.encode() in data for data in payloads),
            "routes": admin(),
            "native_inventory": command("inventory"),
        }
        results.append(row)
        (output / "results.json").write_text(json.dumps(results, indent=2))
        return row

    try:
        commands = {
            "bridge": [str(bridge.resolve()), "-c", str(config), "--rest-http-port", f"127.0.0.1:{rest_port}"],
            "producer": [sys.executable, str(Path(__file__).resolve()), "--producer", "--output", str(output)],
        }
        for name, args in commands.items():
            log = (output / f"{name}.log").open("w")
            logs.append(log)
            processes[name] = subprocess.Popen(
                args, env=environment, stdin=subprocess.PIPE, text=True, stdout=log, stderr=log
            )
        assert wait_for(lambda: bool(admin()), timeout=15), "No initial topic route"
        session = zenoh.open(zenoh.Config.from_json5(json.dumps({
            "mode": "client",
            "connect": {"endpoints": [endpoint]},
            "scouting": {"multicast": {"enabled": False}},
        })))
        subscriber = session.declare_subscriber(
            TOPIC[1:], lambda sample: received.append(bytes(sample.payload))
        )
        assert wait_for(lambda: proxy_ready(IMAGE)), "Initial Image proxy did not activate"
        time.sleep(0.2)
        initial = record("initial", "publish_image")
        assert initial["received"] == 10, "Initial Image data path not established"

        command("add_string")
        assert wait_for(lambda: {e["type"] for e in command("inventory")["writers"]} == {IMAGE, STRING}), "Both writer types did not appear"
        overlap = record("overlap")
        writers = overlap["native_inventory"]["writers"]
        assert len(writers) == 2 and len({tuple(e["gid"][:12]) for e in writers}) == 1, "The two writers must share one DDS participant"
        command("drop_image")
        assert wait_for(lambda: [e["type"] for e in command("inventory")["writers"]] == [STRING]), "Old Image writer did not disappear"
        # Keep collecting traffic/evidence even if the old binary never changes
        # proxy type. Its final assertions must fail on the actual bad behavior.
        wait_for(lambda: proxy_ready(STRING))
        record("after_dispose", "publish_string")
        command("refresh_graph")
        wait_for(lambda: proxy_ready(STRING))
        time.sleep(0.2)
        record("after_fresh_graph", "publish_string")

        original_reader = initial["routes"][0]["dds_reader"]
        for row in results[2:]:
            assert row["received"] == row["string_markers"] == 10, f"String forwarding failed: {row}"
            assert len(row["routes"]) == 1, row
            route = row["routes"][0]
            assert route["ros2_type"] == STRING and route["dds_reader"] != original_reader, row
            assert route["remote_routes"] == [], "A remote bridge must not retain this route"
            inventory = row["native_inventory"]
            assert [e["type"] for e in inventory["writers"]] == [STRING], row
            assert [e["type"] for e in inventory["readers"]] == [STRING], row
        print("PASS: type handover recreates the proxy and forwards both String batches", flush=True)
    finally:
        with suppress(Exception):
            if subscriber is not None:
                subscriber.undeclare()
        with suppress(Exception):
            if session is not None:
                session.close()
        cleanup = []
        for name, process in reversed(list(processes.items())):
            forced = False
            if process.poll() is None:
                with suppress(ProcessLookupError):
                    process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                forced = True
                process.kill()
                process.wait(timeout=5)
            cleanup.append({"process": name, "forced_kill": forced, "returncode": process.returncode})
        (output / "cleanup.json").write_text(json.dumps(cleanup, indent=2))
        for log in logs:
            log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producer", action="store_true")
    parser.add_argument("--bridge", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--domain", type=int, default=196)
    parser.add_argument("--zenoh-port", type=int, default=17478)
    parser.add_argument("--rest-port", type=int, default=18178)
    args = parser.parse_args()
    if args.producer:
        producer(args.output)
    else:
        if args.bridge is None:
            parser.error("--bridge is required")
        run(args.bridge, args.output, args.domain, args.zenoh_port, args.rest_port)
