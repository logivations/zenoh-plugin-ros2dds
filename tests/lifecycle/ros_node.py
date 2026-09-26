"""ROS peers for the full-binary lifecycle regression. Run only in an isolated lab."""

import json
import os
import sys
import time

import rclpy
from example_interfaces.srv import AddTwoInts
from rclpy.node import Node
from std_msgs.msg import String
from std_srvs.srv import Empty

NAMES = [f"/test/frame_{i}" for i in range(24)]


def camera(node):
    def reply(req, response):
        response.sum = req.a + req.b
        return response

    services = [node.create_service(AddTwoInts, name, reply) for name in NAMES]
    services.append(node.create_service(Empty, "/test/empty", lambda req, res: res))
    publisher = node.create_publisher(String, "/test/detections", 10)
    sequence = 0

    def publish():
        nonlocal sequence
        sequence += 1
        publisher.publish(String(data=f"{os.getpid()}:{sequence}"))

    node.create_timer(0.1, publish)
    rclpy.spin(node)


def probe(node):
    clients = [node.create_client(AddTwoInts, name) for name in NAMES]
    received, pending = [], {}
    node.create_subscription(
        String, "/test/detections", lambda m: received.append(m.data), 100
    )

    def tick():
        ok = bad = 0
        for i, (future, start) in list(pending.items()):
            if future.done():
                valid = future.result() is not None and future.result().sum == 3
                ok += valid
                bad += not valid
                del pending[i]
            elif time.monotonic() - start > 3:
                clients[i].remove_pending_request(future)
                del pending[i]
                bad += 1
        for i, client in enumerate(clients):
            if i not in pending:
                pending[i] = (
                    client.call_async(AddTwoInts.Request(a=1, b=2)),
                    time.monotonic(),
                )
        writers = [
            len(node.get_publishers_info_by_topic("rr" + n + "Reply", no_mangle=True))
            for n in NAMES
        ]
        readers = [
            len(
                node.get_subscriptions_info_by_topic(
                    "rq" + n + "Request", no_mangle=True
                )
            )
            for n in NAMES
        ]
        print(
            json.dumps(
                dict(
                    wall=time.time(),
                    ready=sum(c.service_is_ready() for c in clients),
                    ok=ok,
                    bad=bad,
                    duplicates=len(received) - len(set(received)),
                    detections=len(received),
                    writers=writers,
                    readers=readers,
                )
            ),
            flush=True,
        )
        received.clear()

    node.create_timer(1, tick)
    rclpy.spin(node)


def native_replies(node):
    client = node.create_client(AddTwoInts, "/test/native")
    until = time.monotonic() + 15
    while not client.service_is_ready() and time.monotonic() < until:
        rclpy.spin_once(node, timeout_sec=0.05)
    assert (
        client.service_is_ready()
    ), "Native Zenoh queryable did not create a usable DDS proxy"
    for a in (1, 2):
        future = client.call_async(AddTwoInts.Request(a=a, b=2))
        rclpy.spin_until_future_complete(node, future, timeout_sec=5)
        assert (
            future.done() and future.result().sum == a + 2
        ), f"Reply with a={a} did not correlate"
    print("LE and BE native replies reached the ROS client", flush=True)


def missing_server(node):
    client = None
    for command in sys.stdin:
        if command.strip() == "create":
            assert client is None
            client = node.create_client(AddTwoInts, "/test/missing")
        elif command.strip() == "destroy":
            assert client is not None and not client.service_is_ready()
            node.destroy_client(client)
            client = None
        else:
            raise AssertionError(command)


if __name__ == "__main__":
    rclpy.init()
    node = Node("lifecycle_" + sys.argv[1])
    try:
        {
            "camera": camera,
            "probe": probe,
            "native": native_replies,
            "missing": missing_server,
        }[sys.argv[1]](node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()
