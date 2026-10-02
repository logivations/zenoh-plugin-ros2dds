#!/usr/bin/env python3
"""Minimal reproducer for the "W2" missing-reactivation wedge of
RouteServiceCli (server-side cli route keeps local_nodes and remote_routes
populated while req_reader/rep_writer stay deleted, so real service calls
hang until the bridge is restarted).

Two phases, both on a 2-container topology (server bridge + probe clients,
camera bridge + one ROS 2 node serving 24 AddTwoInts services):

  native  Deterministic: a native zenoh Queryable on the same key expression
          keeps the server's Querier matched while the camera node retires
          and returns. remove_remote_route() deactivates the route; no
          MatchingStatus edge ever fires again; add_remote_route() on the
          return must re-activate it (the fix) or the route wedges forever.

  churn   Probabilistic event-order inversion: same-name node churn
          overlapping a camera-bridge-connection flap. The server Querier's
          re-match edge (zenoh net thread) fires activate() before the
          routes-manager task has processed the stale liveliness retirement,
          whose remove_remote_route() then deletes the just-created entities;
          the following add_remote_route() never re-activates. No further
          matching edge arrives, so the wedge is permanent.

Wedge definition (as in the RTDTK-1026 stand): a cli route with
local_nodes != [] and remote_routes != [] whose req_reader/rep_writer stay
empty/UNKOWN, persisting over the persistence window, with the real call on
that service failing; recovery without any bridge restart counts as healthy.

Entirely local: docker only, isolated internal network, no production access.

Example:
  python3 tools/w2_repro.py --phase native --binary /eng/stand100/bin/picks2-<sha> \
      --attempts 3 --output /tmp/w2-native
  python3 tools/w2_repro.py --phase churn --binary ... --runs 10 --output /tmp/w2-churn
"""
import argparse
import datetime
import json
import pathlib
import random
import subprocess
import sys
import time

CAM_NODE = r'''
import os, rclpy
from rclpy.node import Node
from example_interfaces.srv import AddTwoInts
from std_msgs.msg import String
N = 24
class Cam(Node):
    def __init__(self):
        super().__init__('recognition_unit_1')  # same name on every restart
        self.srvs = [self.create_service(AddTwoInts, '/tracking_camera_%d/get_frame' % i, self.cb)
                     for i in range(1, N + 1)]
        self.pub = self.create_publisher(String, '/detections', 10)
        self.n = 0
        self.create_timer(0.1, self.tick)
    def cb(self, req, resp):
        resp.sum = req.a + req.b
        return resp
    def tick(self):
        self.n += 1
        m = String(); m.data = '%d:%d' % (os.getpid(), self.n)
        self.pub.publish(m)
rclpy.init(); rclpy.spin(Cam())
'''

PROBE = r'''
import json, time, rclpy
from rclpy.node import Node
from example_interfaces.srv import AddTwoInts
rclpy.init()
node = Node('w2_probe')
names = ['/tracking_camera_%d/get_frame' % i for i in range(1, 25)]
clients = [node.create_client(AddTwoInts, n) for n in names]
pending = {}
last = {i: None for i in range(24)}
def tick():
    now = time.time()
    for i, (fut, t0) in list(pending.items()):
        if fut.done():
            r = fut.result()
            last[i] = bool(r is not None and r.sum == 3)
            del pending[i]
        elif time.monotonic() - t0 > 3:
            clients[i].remove_pending_request(fut)
            last[i] = False
            del pending[i]
    for i, c in enumerate(clients):
        if i not in pending:
            pending[i] = (c.call_async(AddTwoInts.Request(a=1, b=2)), time.monotonic())
    ok = sum(1 for v in last.values() if v is True)
    bad = sum(1 for v in last.values() if v is False)
    print(json.dumps({'wall': now, 'ok': ok, 'bad': bad,
                      'ready': sum(c.service_is_ready() for c in clients),
                      'failing': [names[i] for i, v in last.items() if v is False]}),
          flush=True)
node.create_timer(1.0, tick)
rclpy.spin(node)
'''

NATIVE_QUERYABLE = r'''
import json, struct, sys, time, zenoh
# argv[1]: comma-separated key expressions to serve, argv[2]: connect endpoint
keys = sys.argv[1].split(',') if len(sys.argv) > 1 else ['tracking_camera_1/get_frame']
endpoint = sys.argv[2] if len(sys.argv) > 2 else 'tcp/srv:7447'
cfg = zenoh.Config()
cfg.insert_json5('mode', '"client"')
cfg.insert_json5('connect/endpoints', '["%s"]' % endpoint)
cfg.insert_json5('scouting/multicast/enabled', 'false')
s = zenoh.open(cfg)
def make_reply(ke):
    def reply(q):
        q.reply(ke, b'\x00\x01\x00\x00' + struct.pack('<q', 3))
    return reply
qs = [s.declare_queryable(ke, make_reply(ke)) for ke in keys]
print('NATIVE QUERYABLE READY: %s' % keys, flush=True)
while True:
    time.sleep(1)
'''

CYCLONEDDS_XML = ('<CycloneDDS><Domain><General><Interfaces>'
                  '<NetworkInterface address="127.0.0.1"/></Interfaces>'
                  '<AllowMulticast>false</AllowMulticast></General><Discovery>'
                  '<ParticipantIndex>auto</ParticipantIndex>'
                  '<MaxAutoParticipantIndex>30</MaxAutoParticipantIndex>'
                  '<Peers><Peer Address="127.0.0.1"/></Peers>'
                  '<LeaseDuration>3s</LeaseDuration></Discovery></Domain></CycloneDDS>')

SRV_CONF = json.dumps({
    "mode": "peer",
    "listen": {"endpoints": ["tcp/0.0.0.0:7447"]},
    "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}},
    "plugins": {"ros2dds": {
        "queries_timeout": {"default": 60.0},
        "allow": {"service_clients": ["/tracking_camera_.*/get_frame"],
                   "subscribers": ["/detections"]}}},
})

CAM_CONF = json.dumps({
    "mode": "client",
    "connect": {"endpoints": ["tcp/srv:7447"]},
    "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}},
    "plugins": {"ros2dds": {
        "queries_timeout": {"default": 60.0},
        "allow": {"service_servers": ["/tracking_camera_.*/get_frame"],
                   "publishers": ["/detections"]}}},
})


def sh(args, check=True, timeout=90):
    p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode:
        raise RuntimeError("%s -> %d: %s %s" % (args, p.returncode, p.stdout, p.stderr))
    return p


class Lab:
    def __init__(self, args):
        self.a = args
        self.prefix = args.prefix
        self.net = args.prefix + "net"
        self.out = pathlib.Path(args.output)
        self.out.mkdir(parents=True, exist_ok=True)
        self.cout = str(self.out)  # same path inside the containers (bind mount)
        self.events = (self.out / "events.jsonl").open("a")
        (self.out / "cam_node.py").write_text(CAM_NODE)
        (self.out / "probe.py").write_text(PROBE)
        (self.out / "native_queryable.py").write_text(NATIVE_QUERYABLE)
        (self.out / "cyclonedds.xml").write_text(CYCLONEDDS_XML)
        (self.out / "srv.json5").write_text(SRV_CONF)
        (self.out / "cam.json5").write_text(CAM_CONF)

    def event(self, kind, **data):
        row = dict(wall=time.time(), kind=kind, **data)
        self.events.write(json.dumps(row) + "\n")
        self.events.flush()
        print(json.dumps(row), flush=True)

    def exec(self, side, args, **kw):
        return sh(["docker", "exec", self.prefix + side, *args], **kw)

    def exec_bg(self, side, script):
        sh(["docker", "exec", "-d", self.prefix + side, "bash", "-c", script])

    def envline(self):
        return ("source %s >/dev/null 2>&1; "
                "export RMW_IMPLEMENTATION=rmw_cyclonedds_cpp "
                "CYCLONEDDS_URI=file://%s/cyclonedds.xml ROS_DOMAIN_ID=%d "
                "ROS_AUTOMATIC_DISCOVERY_RANGE=LOCALHOST ROS_LOG_DIR=/tmp/roslog; "
                % (self.a.ros_setup, self.cout, self.a.domain))

    def setup(self):
        for side in ("srv", "cam"):
            if sh(["docker", "inspect", self.prefix + side], check=False).returncode == 0:
                raise RuntimeError("container name already in use: " + self.prefix + side)
        sh(["docker", "network", "create", "--internal", self.net])
        for side in ("srv", "cam"):
            sh(["docker", "run", "-d", "--name", self.prefix + side,
                "--network", self.net, "--network-alias", side,
                "--cpus", "2", "--memory", "2g", "--pids-limit", "256",
                "--mount", "type=bind,src=%s,dst=%s" % (self.out, self.cout),
                "--mount", "type=bind,src=%s,dst=%s"
                % (pathlib.Path(self.a.binary).parent, "/w2bin"),
                "--mount", "type=bind,src=%s,dst=/zenoh_py" % self.a.zenoh_py,
                "--entrypoint", "sleep", self.a.image, "infinity"])
        self.start_bridge("srv")
        self.start_bridge("cam")
        self.start_node()
        self.exec_bg("srv", self.envline() +
                     "exec python3 %s/probe.py >> %s/probe.log 2>&1" % (self.cout, self.cout))
        self.wait_healthy(90)

    def binary_in_container(self):
        return "/w2bin/" + pathlib.Path(self.a.binary).name

    def start_bridge(self, side):
        rust_log = "zenoh_plugin_ros2dds=debug" if self.a.debug_logs else "zenoh_plugin_ros2dds=info"
        self.exec_bg(side, self.envline() +
                     "export RUST_LOG=%s; echo $$ > /tmp/bridge.pid; exec %s -c %s/%s.json5 "
                     "--rest-http-port 127.0.0.1:8000 >> %s/%s-bridge.log 2>&1"
                     % (rust_log, self.binary_in_container(), self.cout, side,
                        self.cout, side))
        self.event("bridge_started", side=side)

    def kill_bridge(self, side):
        self.exec(side, ["pkill", "-9", "-f", self.binary_in_container()], check=False)
        self.event("bridge_killed", side=side)

    def start_node(self):
        self.exec_bg("cam", self.envline() +
                     "exec python3 %s/cam_node.py >> %s/node.log 2>&1" % (self.cout, self.cout))
        self.event("node_started")

    def kill_node(self, graceful=False):
        self.exec("cam", ["pkill", "-2" if graceful else "-9", "-f", "cam_node.py"],
                  check=False)
        self.event("node_killed", graceful=graceful)

    def start_native(self, side="cam", keys="tracking_camera_1/get_frame",
                     endpoint="tcp/srv:7447"):
        self.exec_bg(side, "export PYTHONPATH=/zenoh_py; exec python3 "
                     "%s/native_queryable.py '%s' '%s' >> %s/native-%s.log 2>&1"
                     % (self.cout, keys, endpoint, self.cout, side))
        self.event("native_queryable_started", side=side, keys=keys)

    def kill_native(self, side="cam"):
        self.exec(side, ["pkill", "-2", "-f", "native_queryable.py"], check=False)
        self.event("native_queryable_killed", side=side)

    def pause(self, side):
        sh(["docker", "pause", self.prefix + side])
        self.event("container_paused", side=side)

    def unpause(self, side):
        sh(["docker", "unpause", self.prefix + side], check=False)
        self.event("container_unpaused", side=side)

    def admin(self, side, selector):
        p = self.exec(side, ["curl", "-sf", "--max-time", "8",
                             "http://127.0.0.1:8000/" + selector], check=False, timeout=20)
        try:
            return json.loads(p.stdout) if p.returncode == 0 and p.stdout.strip() else []
        except ValueError:
            return []

    def probe_row(self):
        path = self.out / "probe.log"
        if not path.exists():
            return None
        for line in reversed(path.read_text(errors="replace").splitlines()):
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if "ok" in row:
                return row
        return None

    def cli_routes(self):
        return self.admin("srv", "@/local/ros2/route/service/cli/**")

    def wedged_routes(self):
        """cli routes with demand on both sides but no DDS entities."""
        out = []
        for row in self.cli_routes():
            v = row.get("value", {})
            if (v.get("local_nodes") and v.get("remote_routes")
                    and not v.get("req_reader") and not v.get("rep_writer")):
                out.append(v.get("ros2_name"))
        return sorted(out)

    def wait_healthy(self, timeout, need=24):
        t0 = time.time()
        good = 0
        while time.time() - t0 < timeout:
            row = self.probe_row()
            if row and row["ready"] == need and row["ok"] == need and not self.wedged_routes():
                good += 1
                if good >= 2:
                    self.event("healthy", t_s=round(time.time() - t0, 1))
                    return True
            time.sleep(1.0)
        return False

    def close(self):
        for side in ("srv", "cam"):
            sh(["docker", "rm", "-f", self.prefix + side], check=False)
        sh(["docker", "network", "rm", self.net], check=False)
        self.event("cleanup_complete")


def check_wedge(lab, settle_s, persist_s):
    """Returns (wedged_names, recovered: bool). A wedge must persist for
    persist_s and show failing real calls; recovery = no wedged routes and
    24/24 real calls OK."""
    deadline = time.time() + settle_s
    while time.time() < deadline:
        w = lab.wedged_routes()
        row = lab.probe_row()
        if not w and row and row["ok"] == 24:
            return [], True
        time.sleep(2.0)
    first = set(lab.wedged_routes())
    if not first:
        # neither clean recovery nor an admin-visible wedge: report as wedged
        # on the probe evidence alone (calls failing without a bridge restart)
        row = lab.probe_row()
        lab.event("no_admin_wedge_but_unhealthy", probe=row)
        return ["<probe-unhealthy>"], False
    time.sleep(persist_s)
    second = set(lab.wedged_routes())
    persisting = sorted(first & second)
    row = lab.probe_row()
    failing = set(row.get("failing", [])) if row else set()
    lab.event("wedge_check", first=sorted(first), persisting=persisting,
              probe_failing=sorted(failing))
    return persisting, False


def phase_native(lab, args):
    results = []
    for attempt in range(1, args.attempts + 1):
        lab.event("native_attempt", n=attempt)
        if not lab.wait_healthy(90):
            raise AssertionError("not healthy before attempt %d" % attempt)
        lab.start_native()
        time.sleep(3)
        lab.kill_node(graceful=True)
        time.sleep(13)   # server processes RetiredServiceSrv -> deactivate
        mid = lab.cli_routes()
        (lab.out / ("native-%d-mid.json" % attempt)).write_text(json.dumps(mid, indent=1))
        lab.start_node()
        wedged, recovered = check_wedge(lab, settle_s=45, persist_s=args.persist_s)
        (lab.out / ("native-%d-end.json" % attempt)).write_text(
            json.dumps(lab.cli_routes(), indent=1))
        results.append({"attempt": attempt, "wedged": wedged, "recovered": recovered})
        lab.event("native_result", **results[-1])
        lab.kill_native()
        # Reset to a clean state before the next attempt by restarting the
        # SERVER bridge. (This restart is only harness setup between attempts;
        # the fix's acceptance is that the wedge recovers WITHIN an attempt
        # without any restart - see the `recovered` field above.)
        if attempt < args.attempts:
            lab.kill_bridge("srv")
            time.sleep(3)
            lab.start_bridge("srv")
            lab.wait_healthy(90)
        time.sleep(3)
    return results


def phase_churn(lab, args):
    """Minimal deterministic reduction of the stand's mixed-churn W2 trigger.

    The server-side cli route deactivates whenever its last remote route is
    removed (camera liveliness retired), and only ever re-activates on a
    zenoh matching *transition*. The reduction suppresses that transition the
    same way the real fleet does: a steady consumer (here, native zenoh
    Queryables hosted on the SERVER container's session, plus the server's own
    probe clients) keeps the Querier matched across the camera churn, so no
    matching edge is produced when the camera's remote route retires and
    returns. Overlapping a same-name node churn with a camera-bridge
    connection flap (docker pause/unpause - the bridge process stays alive,
    only its link to the server drops past the lease) drives the retire/return
    of the camera's service-server liveliness. On unpatched picks the route
    stays deactivated (req_reader/rep_writer empty) for the process lifetime;
    the fix re-activates it on add_remote_route / add_local_node.
    """
    rng = random.Random(args.seed)
    keys = ",".join("tracking_camera_%d/get_frame" % i
                    for i in range(1, args.held_keys + 1))
    # steady consumer on the SERVER session: survives the camera flap and
    # holds the Querier matched (no matching transition during churn).
    lab.start_native(side="srv", keys=keys, endpoint="tcp/127.0.0.1:7447")
    time.sleep(3)
    results = []
    for run in range(1, args.runs + 1):
        lab.event("churn_run", n=run)
        if not lab.wait_healthy(120):
            raise AssertionError("not healthy before run %d" % run)
        for cyc in range(args.cycles):
            # same-name node churn overlapping a camera connection flap
            lab.kill_node()
            time.sleep(rng.uniform(0, 0.2))
            lab.pause("cam")                      # connection flap (bridge alive)
            time.sleep(rng.uniform(4.0, 5.5))     # > lease (3s): server retires camera
            lab.unpause("cam")
            time.sleep(rng.uniform(0.0, 0.4))
            lab.start_node()                      # same-name node returns
            time.sleep(rng.uniform(3.0, 5.0))
            lab.event("churn_cycle_done", run=run, cycle=cyc)
        wedged, recovered = check_wedge(lab, settle_s=args.settle_s,
                                        persist_s=args.persist_s)
        (lab.out / ("churn-%d-end.json" % run)).write_text(
            json.dumps(lab.cli_routes(), indent=1))
        results.append({"run": run, "wedged": wedged, "recovered": recovered})
        lab.event("churn_result", **results[-1])
        if wedged:
            # reset via server-bridge restart (harness setup only; the fix
            # recovers without any restart, see `recovered`).
            lab.kill_native(side="srv")
            lab.kill_bridge("srv")
            time.sleep(3)
            lab.start_bridge("srv")
            lab.start_native(side="srv", keys=keys, endpoint="tcp/127.0.0.1:7447")
            time.sleep(3)
    lab.kill_native(side="srv")
    return results


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--phase", required=True, choices=["native", "churn"])
    ap.add_argument("--binary", required=True,
                    help="zenoh-bridge-ros2dds binary (host path; its dir is mounted)")
    ap.add_argument("--output", required=True)
    ap.add_argument("--image", default="quay.io/logivations/ml_all:latest")
    ap.add_argument("--prefix", default="picks2-w2-")
    ap.add_argument("--domain", type=int, default=147)
    ap.add_argument("--zenoh-py", default="/data/RTDTK-1026-engineering/detector/lab/zenoh_py",
                    help="dir with the python zenoh package (native queryable)")
    ap.add_argument("--ros-setup", default="/code/ros2_ws/install/setup.bash")
    ap.add_argument("--attempts", type=int, default=3, help="native attempts")
    ap.add_argument("--runs", type=int, default=10, help="churn runs")
    ap.add_argument("--cycles", type=int, default=4, help="churn cycles per run")
    ap.add_argument("--held-keys", type=int, default=3,
                    help="churn: number of service keys held matched by a "
                         "server-side native queryable")
    ap.add_argument("--settle-s", type=float, default=35)
    ap.add_argument("--persist-s", type=float, default=30)
    ap.add_argument("--seed", type=int, default=1026)
    ap.add_argument("--debug-logs", action="store_true")
    ap.add_argument("--keep", action="store_true", help="keep containers on exit")
    args = ap.parse_args()

    lab = Lab(args)
    verdict = {"phase": args.phase, "binary": pathlib.Path(args.binary).name,
               "started": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    try:
        lab.setup()
        results = (phase_native if args.phase == "native" else phase_churn)(lab, args)
        verdict["results"] = results
        verdict["wedged_count"] = sum(1 for r in results if r["wedged"])
        verdict["total"] = len(results)
    except BaseException as e:
        verdict["error"] = repr(e)
        raise
    finally:
        for side in ("srv", "cam"):
            log = lab.out / ("%s-bridge.log" % side)
            verdict["%s_bridge_panicked" % side] = (
                log.exists() and "panicked" in log.read_text(errors="replace"))
        (lab.out / "VERDICT.json").write_text(json.dumps(verdict, indent=1))
        print(json.dumps(verdict), flush=True)
        if not args.keep:
            lab.close()


if __name__ == "__main__":
    main()
