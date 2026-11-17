#!/usr/bin/env python3
"""Measures AreNodesSafeToTakeDown latency on a Kubernetes universe at a given tablet count, idle
and under load.

One run: top the universe up to --tablets user tablets per tserver with empty split tables, then
three phases: idle, saturate (ysql_bench, simple-update) and write (large-value YCQL batch writes,
to push follower lag). A sampler pod calls `yb-admin are_nodes_safe_to_take_down` for one tserver
at a time throughout, with YBA's default follower lag bound, and a scraper records the master and
tserver metrics that explain the latency. See README.md.

usage:
  sttd_bench.py run --universe-name my-universe --tablets 500 --out results/t500
  sttd_bench.py cleanup --universe-name my-universe
"""
import argparse
import csv
import json
import math
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

TOOLS_POD = "sttd-tools"
BENCH_POD = "sttd-ysqlbench"
WRITER_POD = "sttd-writer"
POD_LABEL = "sttd-bench"
FILLER_PREFIX = "sttd_filler_"
TABLETS_PER_FILLER = 50
# YBA passes yb.checks.follower_lag.max_threshold (1 minute) and uses a 30s client timeout, which
# makes the master answer by 28s at the latest.
LAG_BOUND_MS = 60000
CLIENT_TIMEOUT_MS = 30000

MASTER_METRICS = ["handler_latency_yb_master_MasterAdmin_AreNodesSafeToTakeDown"]
TSERVER_METRICS = [
    "handler_latency_yb_tserver_TabletServerService_CheckTserverTabletHealth",
    "rpc_incoming_queue_time",
    "rpcs_queue_overflow",
    "follower_lag_ms",
]

SAMPLER_SH = r"""#!/bin/bash
# usage: sttd-sampler.sh END_EPOCH MASTER_ADDRS LAG_MS TIMEOUT_MS UUID...
# One line per call: kind,start_ms,target,wall_ms,rc,output
END=$1; M=$2; LAG=$3; TMO=$4; shift 4
YBADMIN=/home/yugabyte/bin/yb-admin
n=0
while [ "$(date +%s)" -lt "$END" ]; do
  for u in "$@"; do
    s=$(date +%s%3N)
    out=$(timeout 60 $YBADMIN --master_addresses "$M" --timeout_ms "$TMO" are_nodes_safe_to_take_down "$u" "$LAG" 2>&1); rc=$?
    e=$(date +%s%3N)
    echo "sttd,$s,$u,$((e-s)),$rc,$(printf '%s' "$out" | tr '\n,' '  ' | cut -c1-400)"
    n=$((n+1))
    # A cheap master-leader RPC through the same yb-admin path: its wall time is the process,
    # connection and leader-lookup overhead included in every sttd sample.
    if [ $((n % 5)) -eq 0 ]; then
      s=$(date +%s%3N)
      out=$(timeout 60 $YBADMIN --master_addresses "$M" --timeout_ms "$TMO" list_all_masters 2>&1); rc=$?
      e=$(date +%s%3N)
      echo "noop,$s,-,$((e-s)),$rc,"
    fi
    sleep 1
  done
done
"""


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def now_ms():
    return int(time.time() * 1000)


class Kube:
    def __init__(self, context, namespace):
        self.base = ["kubectl"] + (["--context", context] if context else []) + ["-n", namespace]
        self.namespace = namespace

    def run(self, *args, input=None, timeout=300, check=True):
        p = subprocess.run(
            self.base + list(args), input=input, capture_output=True, text=True, timeout=timeout
        )
        if check and p.returncode != 0:
            raise RuntimeError("kubectl %s failed: %s" % (" ".join(args[:3]), p.stderr.strip()[:800]))
        return p.stdout

    def json(self, *args):
        return json.loads(self.run(*args, "-o", "json"))

    def exec(self, pod, script, input=None, timeout=300, check=True):
        args = ["exec"] + (["-i"] if input is not None else []) + [pod, "--", "bash", "-c", script]
        return self.run(*args, input=input, timeout=timeout, check=check)

    def popen(self, pod, script):
        return subprocess.Popen(
            self.base + ["exec", pod, "--", "bash", "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )


class Yba:
    def __init__(self):
        self.url = os.environ["YBA_URL"].rstrip("/")
        self.token = os.environ["YBA_API_TOKEN"]
        self.customer = os.environ["YBA_CUSTOMER_UUID"]

    def call(self, method, path, body=None, content_type="application/json", timeout=30):
        data = None
        if body is not None:
            data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        req = urllib.request.Request(
            self.url + path.replace("{c}", self.customer), data=data, method=method
        )
        req.add_header("X-AUTH-YW-API-TOKEN", self.token)
        req.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read().decode()
                return r.status, (json.loads(raw) if raw.strip()[:1] in "{[" else raw)
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()[:500]

    def universe_by_name(self, name):
        _, us = self.call("GET", "/api/v1/customers/{c}/universes")
        for u in us:
            if u["name"] == name:
                return u
        raise SystemExit("universe %s not found" % name)


class Universe:
    """Pod names and addresses of a YBA-created Kubernetes universe."""

    def __init__(self, kube, name):
        pods = kube.json("get", "pods")["items"]
        rx = re.compile(r"^(yb%s-[a-z0-9]+)-yb-(master|tserver)-(\d+)$" % re.escape(name))
        self.masters, self.tservers, release = [], [], None
        self.image, self.pull_secrets, self.zone = None, [], None
        for p in pods:
            m = rx.match(p["metadata"]["name"])
            if not m:
                continue
            release = m.group(1)
            (self.masters if m.group(2) == "master" else self.tservers).append(p["metadata"]["name"])
            if m.group(2) == "tserver":
                self.image = p["spec"]["containers"][0]["image"]
                self.pull_secrets = p["spec"].get("imagePullSecrets", [])
                self.zone = p["spec"].get("nodeSelector", {}).get("topology.kubernetes.io/zone")
        if not release or not self.masters or not self.tservers:
            raise SystemExit("no pods found for universe %s" % name)
        self.masters.sort()
        self.tservers.sort()
        self.release = release
        ns = kube.namespace
        self.master_hosts = ["%s.%s-yb-masters.%s.svc.cluster.local" % (m, release, ns) for m in self.masters]
        self.tserver_hosts = ["%s.%s-yb-tservers.%s.svc.cluster.local" % (t, release, ns) for t in self.tservers]
        self.master_addrs = ",".join(h + ":7100" for h in self.master_hosts)
        self.tserver_service = "%s-yb-tservers.%s.svc.cluster.local" % (release, ns)


class Bench:
    def __init__(self, args):
        self.args = args
        self.kube = Kube(args.context, args.namespace)
        self.yba = Yba()
        self.u = Universe(self.kube, args.universe_name)
        self.universe_uuid = self.yba.universe_by_name(args.universe_name)["universeUUID"]
        self.out = args.out
        os.makedirs(self.out, exist_ok=True)
        self.phases = []  # (name, start_ms, end_ms)
        self.stop = threading.Event()
        self.procs = []
        self.saved_keys = {}

    # ---------------------------------------------------------------- pods

    def pod_manifest(self, name, image, cpu, mem, command=None, args=None, pull_secrets=True):
        container = {
            "name": "main",
            "image": image,
            "resources": {
                "requests": {"cpu": str(cpu), "memory": mem},
                "limits": {"cpu": str(cpu), "memory": mem},
            },
        }
        if command:
            container["command"] = command
        if args:
            container["args"] = args
        spec = {"restartPolicy": "Never", "containers": [container]}
        if pull_secrets and self.u.pull_secrets:
            spec["imagePullSecrets"] = self.u.pull_secrets
        zone = self.args.zone or self.u.zone
        if zone:
            spec["nodeSelector"] = {"topology.kubernetes.io/zone": zone}
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name, "labels": {"app": POD_LABEL}},
            "spec": spec,
        }

    def ensure_pod(self, manifest):
        name = manifest["metadata"]["name"]
        out = self.kube.run("get", "pod", name, "--ignore-not-found", "-o", "name")
        if not out.strip():
            self.kube.run("apply", "-f", "-", input=json.dumps(manifest))
        self.kube.run("wait", "--for=condition=Ready", "pod/" + name, "--timeout=900s", timeout=960)

    def ensure_tool_pods(self):
        sleeper = ["bash", "-c", "sleep infinity"]
        self.ensure_pod(self.pod_manifest(TOOLS_POD, self.u.image, 1, "2Gi", command=sleeper))
        self.ensure_pod(
            self.pod_manifest(BENCH_POD, self.u.image, self.args.bench_cpu, "6Gi", command=sleeper)
        )
        self.kube.exec(TOOLS_POD, "cat > /tmp/sttd-sampler.sh && chmod +x /tmp/sttd-sampler.sh", input=SAMPLER_SH)

    # ---------------------------------------------------------------- DB helpers

    def ysql(self, sql, timeout=900):
        return self.kube.exec(
            TOOLS_POD,
            "/home/yugabyte/bin/ysqlsh -h %s -U yugabyte -v ON_ERROR_STOP=1 -qtA -f -" % self.u.tserver_service,
            input=sql,
            timeout=timeout,
        )

    def curl_many(self, urls, timeout=120):
        """Fetches the URLs from the tools pod; returns {url: body}."""
        script = "; ".join('echo "### %s"; curl -s -m 20 "%s"' % (u, u) for u in urls)
        out = self.kube.exec(TOOLS_POD, script, timeout=timeout, check=False)
        result, cur, buf = {}, None, []
        for line in out.splitlines():
            if line.startswith("### "):
                if cur is not None:
                    result[cur] = "\n".join(buf)
                cur, buf = line[4:], []
            else:
                buf.append(line)
        if cur is not None:
            result[cur] = "\n".join(buf)
        return result

    def tablet_servers(self):
        """Alive tservers as reported by the master leader: {host: info}."""
        for h in self.u.master_hosts:
            body = self.curl_many(["http://%s:7000/api/v1/tablet-servers" % h]).popitem()[1]
            try:
                data = json.loads(body)
            except ValueError:
                continue
            servers = {}
            for cluster in data.values():
                for hostport, info in cluster.items():
                    if info.get("status") == "ALIVE":
                        servers[hostport.split(":")[0]] = info
            if servers:
                return servers
        raise RuntimeError("no master answered /api/v1/tablet-servers")

    def user_tablets(self):
        ts = self.tablet_servers()
        totals = [i["user_tablets_total"] for i in ts.values()]
        leaders = sum(i["user_tablets_leaders"] for i in ts.values())
        return max(totals), min(totals), leaders, ts

    def ensure_tablets(self, target):
        cur, _, _, _ = self.user_tablets()
        log("user tablets per tserver: %d, target %d" % (cur, target))
        if cur < target:
            existing = int(
                self.ysql(
                    "select count(*) from pg_class where relkind = 'r' and relname like '%s%%';\n"
                    % FILLER_PREFIX
                ).strip()
                or 0
            )
            need = math.ceil((target - cur) / TABLETS_PER_FILLER)
            log("creating %d filler tables of %d tablets" % (need, TABLETS_PER_FILLER))
            # Each CREATE waits for its tablets; with thousands of tablets per tserver, or a tserver
            # still bootstrapping after a restart, that wait can pass YSQL's create timeout.
            self.wait_settled()
            started = time.time()
            for i in range(existing, existing + need):
                stmt = ("CREATE TABLE IF NOT EXISTS %s%04d (k bigint, v text, PRIMARY KEY (k HASH))"
                        " SPLIT INTO %d TABLETS;\n" % (FILLER_PREFIX, i, TABLETS_PER_FILLER))
                for attempt in range(1, 5):
                    try:
                        self.ysql(stmt)
                        break
                    except RuntimeError as e:
                        if attempt == 4:
                            raise
                        log("  %s%04d attempt %d failed: %s" % (FILLER_PREFIX, i, attempt, str(e)[-200:]))
                        time.sleep(30)
                        self.wait_settled()
                if (i - existing + 1) % 5 == 0 or i == existing + need - 1:
                    log("  %d/%d filler tables created (%.0fs)" % (i - existing + 1, need, time.time() - started))
        return self.wait_settled()

    def wait_settled(self, timeout_s=1800):
        """Waits until every tserver has every user tablet and each tablet has a leader."""
        deadline = time.time() + timeout_s
        stable = 0
        while time.time() < deadline:
            hi, lo, leaders, ts = self.user_tablets()
            ok = hi == lo and leaders >= hi
            log("settle: tablets per tserver %d..%d, leaders %d (%s)" % (
                lo, hi, leaders, " ".join(str(i["user_tablets_leaders"]) for i in ts.values())))
            stable = stable + 1 if ok else 0
            if stable >= 3:
                return hi
            time.sleep(20)
        raise RuntimeError("tablets did not settle in %ds" % timeout_s)

    def pod_incarnations(self):
        """{pod: (start time, DB container restarts, its last termination time)} for the universe."""
        result = {}
        for p in self.kube.json("get", "pods")["items"]:
            name = p["metadata"]["name"]
            if name not in self.u.masters and name not in self.u.tservers:
                continue
            restarts, last_term = 0, None
            for c in p["status"].get("containerStatuses", []):
                if c["name"] in ("yb-master", "yb-tserver"):
                    restarts += c.get("restartCount", 0)
                    last_term = (c.get("lastState", {}).get("terminated") or {}).get("finishedAt")
            result[name] = (p["status"].get("startTime"), restarts, last_term)
        return result

    @staticmethod
    def disruptions(before, after):
        """[(iso time, what)] for pods recreated or DB containers restarted between two snapshots.
        A recreated pod's time is its new start time, which trails the eviction by a few seconds."""
        out = []
        for pod in sorted(set(before) | set(after)):
            b, a = before.get(pod), after.get(pod)
            if a is None:
                out.append((None, "%s gone" % pod))
            elif b is None or b[0] != a[0]:
                out.append((a[0], "%s recreated" % pod))
            elif a[1] > b[1]:
                out.append((a[2], "%s restarted x%d" % (pod, a[1] - b[1])))
        return out

    def tserver_uuids(self):
        ts = self.tablet_servers()
        return [ts[h]["permanent_uuid"] for h in self.u.tserver_hosts if h in ts]

    # ---------------------------------------------------------------- load

    def ensure_ysql_bench_schema(self):
        # YB's ysql_bench names its tables ysql_bench_*, not pgbench_*.
        exists = self.ysql("select to_regclass('ysql_bench_accounts') is not null;\n").strip()
        if exists == "t":
            return
        log("ysql_bench init, scale %d" % self.args.bench_scale)
        self.kube.exec(
            BENCH_POD,
            "/home/yugabyte/postgres/bin/ysql_bench -h %s -p 5433 -U yugabyte -i -s %d yugabyte"
            % (self.u.tserver_service, self.args.bench_scale),
            timeout=3600,
        )

    def writer_manifest(self, run_time_s, name=WRITER_POD):
        args = [
            "--workload", "CassandraBatchKeyValue",
            "--nodes", self.u.tserver_service + ":9042",
            "--num_threads_write", str(self.args.writer_threads),
            "--num_threads_read", "0",
            "--batch_size", str(self.args.writer_batch),
            "--value_size", str(self.args.writer_value_size),
            "--num_unique_keys", "1000000000000",
            "--num_writes", "-1",
            "--run_time", str(run_time_s),
        ]
        return self.pod_manifest(name, self.args.sample_apps_image, self.args.writer_cpu, "6Gi",
                                 args=args, pull_secrets=False)

    def ensure_writer_table(self):
        exists = self.kube.exec(
            TOOLS_POD,
            "/home/yugabyte/bin/ycqlsh %s 9042 -e \"select table_name from system_schema.tables"
            " where keyspace_name='ybdemo_keyspace'\"" % self.u.tserver_service,
            check=False,
        )
        if "cassandrakeyvalue" in exists.lower():
            return
        log("creating the writer table with a short writer run")
        self.kube.run("delete", "pod", WRITER_POD, "--ignore-not-found", "--wait=true")
        self.kube.run("apply", "-f", "-", input=json.dumps(self.writer_manifest(20)))
        deadline = time.time() + 600
        while time.time() < deadline:
            phase = self.kube.run("get", "pod", WRITER_POD, "-o", "jsonpath={.status.phase}", check=False)
            if phase in ("Succeeded", "Failed"):
                break
            time.sleep(10)
        self.kube.run("delete", "pod", WRITER_POD, "--ignore-not-found", "--wait=true")

    def start_saturate(self, duration_s):
        cmd = (
            "exec /home/yugabyte/postgres/bin/ysql_bench -h %s -p 5433 -U yugabyte -N -M prepared"
            " -c %d -j %d -T %d -P 10 yugabyte"
            % (self.u.tserver_service, self.args.bench_clients, self.args.bench_threads, duration_s)
        )
        return self.stream(self.kube.popen(BENCH_POD, cmd), os.path.join(self.out, "ysql_bench.log"))

    def stop_saturate(self, proc):
        self.kube.exec(BENCH_POD, "pkill -f ysql_bench || true", check=False)
        proc.wait(timeout=120)

    def start_write(self, duration_s):
        self.kube.run("delete", "pod", WRITER_POD, "--ignore-not-found", "--wait=true")
        self.kube.run("apply", "-f", "-", input=json.dumps(self.writer_manifest(duration_s + 60)))
        self.kube.run("wait", "--for=condition=Ready", "pod/" + WRITER_POD, "--timeout=600s", timeout=660)
        return None

    def stop_write(self, _):
        logs = self.kube.run("logs", WRITER_POD, check=False, timeout=120)
        with open(os.path.join(self.out, "writer.log"), "w") as f:
            f.write(logs)
        self.kube.run("delete", "pod", WRITER_POD, "--ignore-not-found", "--wait=false")

    # ---------------------------------------------------------------- sampling

    def stream(self, proc, path, on_line=None):
        self.procs.append(proc)

        def pump():
            with open(path, "a") as f:
                for line in proc.stdout:
                    f.write(line)
                    f.flush()
                    if on_line:
                        on_line(line)

        threading.Thread(target=pump, daemon=True).start()
        return proc

    def start_sampler(self, run_s):
        uuids = self.tserver_uuids()
        log("sampler targets: %s" % ", ".join(uuids))
        end = int(time.time()) + run_s
        cmd = "/tmp/sttd-sampler.sh %d '%s' %d %d %s" % (
            end, self.u.master_addrs, LAG_BOUND_MS, CLIENT_TIMEOUT_MS, " ".join(uuids))
        host_by_uuid = {u: h.split(".")[0] for h, u in zip(self.u.tserver_hosts, uuids)}
        with open(os.path.join(self.out, "targets.json"), "w") as f:
            json.dump(host_by_uuid, f)
        return self.stream(self.kube.popen(TOOLS_POD, cmd), os.path.join(self.out, "samples.raw"))

    def scrape_once(self):
        row = {"t_ms": now_ms()}
        urls = ["http://%s:7000/prometheus-metrics?reset_histograms=false&metrics=%s"
                % (h, ",".join(MASTER_METRICS)) for h in self.u.master_hosts]
        urls += ["http://%s:9000/prometheus-metrics?reset_histograms=false&metrics=%s"
                 % (h, ",".join(TSERVER_METRICS)) for h in self.u.tserver_hosts]
        bodies = self.curl_many(urls)
        sums = {}
        max_lag = 0
        for url, body in bodies.items():
            for line in body.splitlines():
                if not line or line.startswith("#"):
                    continue
                m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9.eE+-]+)", line)
                if not m:
                    continue
                name, value = m.group(1), float(m.group(3))
                if name == "follower_lag_ms":
                    max_lag = max(max_lag, value)
                elif name.endswith("_sum") or name.endswith("_count") or name == "rpcs_queue_overflow":
                    sums[name] = sums.get(name, 0) + value
        row["master_sttd_sum_us"] = sums.get(MASTER_METRICS[0] + "_sum", 0)
        row["master_sttd_count"] = sums.get(MASTER_METRICS[0] + "_count", 0)
        row["ts_health_sum_us"] = sums.get(TSERVER_METRICS[0] + "_sum", 0)
        row["ts_health_count"] = sums.get(TSERVER_METRICS[0] + "_count", 0)
        row["ts_queue_sum_us"] = sums.get("rpc_incoming_queue_time_sum", 0)
        row["ts_queue_count"] = sums.get("rpc_incoming_queue_time_count", 0)
        row["ts_queue_overflow"] = sums.get("rpcs_queue_overflow", 0)
        row["max_follower_lag_ms"] = max_lag
        # kubectl top pod takes a single name, so list the namespace and filter.
        top = self.kube.run("top", "pod", "--no-headers", check=False)
        cpu = {}
        for line in top.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1].endswith("m"):
                cpu[parts[0]] = int(parts[1][:-1])
        row["tserver_cpu_m_max"] = max((cpu.get(t, 0) for t in self.u.tservers), default=0)
        row["tserver_cpu_m_avg"] = sum(cpu.get(t, 0) for t in self.u.tservers) / len(self.u.tservers)
        row["master_cpu_m_max"] = max((cpu.get(m, 0) for m in self.u.masters), default=0)
        status, ch = self.yba.call(
            "GET", "/api/v2/customers/{c}/universes/%s/cluster-health" % self.universe_uuid)
        if status == 200 and isinstance(ch, dict):
            sttd = ch.get("nodes_safe_to_take_down") or {}
            row["yba_status"] = ch.get("status")
            row["yba_reasons"] = " ".join(r.get("code", "") for r in ch.get("reasons") or [])
            row["yba_sweep_checked_at"] = sttd.get("checked_at")
            row["yba_sweep_duration_ms"] = sttd.get("duration_ms")
            row["yba_sweep_nodes"] = " ".join(
                "%s=%s/%s" % (n.get("node_name"), n.get("status"), n.get("duration_ms"))
                for n in sttd.get("nodes") or [])
        else:
            row["yba_status"] = "HTTP %s" % status
        return row

    def scraper(self, interval_s=15):
        path = os.path.join(self.out, "metrics.csv")
        fields = ["t_ms", "master_sttd_sum_us", "master_sttd_count", "ts_health_sum_us",
                  "ts_health_count", "ts_queue_sum_us", "ts_queue_count", "ts_queue_overflow",
                  "max_follower_lag_ms", "tserver_cpu_m_max", "tserver_cpu_m_avg",
                  "master_cpu_m_max", "yba_status", "yba_reasons", "yba_sweep_checked_at",
                  "yba_sweep_duration_ms", "yba_sweep_nodes"]
        with open(path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            if f.tell() == 0:
                w.writeheader()
            while not self.stop.is_set():
                try:
                    w.writerow(self.scrape_once())
                    f.flush()
                except Exception as e:  # keep scraping through transient kubectl/YBA errors
                    log("scrape failed: %s" % e)
                self.stop.wait(interval_s)

    # ---------------------------------------------------------------- YBA keys

    def set_universe_key(self, key, value):
        path = "/api/v1/customers/{c}/runtime_config/%s/key/%s" % (self.universe_uuid, key)
        # A key GET returns the effective (possibly inherited) value, so restoring it would pin
        # the default at universe scope. Only an entry set at this scope is put back.
        _, scoped = self.yba.call("GET", "/api/v1/customers/{c}/runtime_config/%s" % self.universe_uuid)
        own = {e["key"]: e["value"] for e in (scoped or {}).get("configEntries", [])
               if not e.get("inherited")} if isinstance(scoped, dict) else {}
        self.saved_keys[key] = own.get(key)
        status, body = self.yba.call("PUT", path, value, content_type="text/plain")
        if status != 200:
            raise RuntimeError("set %s: HTTP %s %s" % (key, status, body))

    def restore_keys(self):
        for key, prev in self.saved_keys.items():
            path = "/api/v1/customers/{c}/runtime_config/%s/key/%s" % (self.universe_uuid, key)
            if prev is None:
                self.yba.call("DELETE", path)
            else:
                self.yba.call("PUT", path, prev, content_type="text/plain")
        self.saved_keys = {}

    # ---------------------------------------------------------------- run

    def run(self):
        a = self.args
        self.ensure_tool_pods()
        self.ensure_ysql_bench_schema()
        self.ensure_writer_table()
        actual = self.ensure_tablets(a.tablets)
        hi, lo, leaders, ts = self.user_tablets()
        meta = {
            "label": a.label or os.path.basename(os.path.normpath(self.out)),
            "target_tablets": a.tablets,
            "user_tablets_per_tserver": hi,
            "leaders_per_tserver": {h.split(".")[0]: i["user_tablets_leaders"] for h, i in ts.items()},
            "system_tablets_per_tserver": max(i["system_tablets_total"] for i in ts.values()),
            "universe": a.universe_name,
            "tserver_resources": "see YBA universe",
            "lag_bound_ms": LAG_BOUND_MS,
            "client_timeout_ms": CLIENT_TIMEOUT_MS,
            "args": vars(a),
        }
        log("measuring at %d user tablets per tserver" % actual)

        # The YBA checker sweeps every minute during the run, so its own timings are recorded too.
        self.set_universe_key("yb.health.cluster_health.evaluation_interval", "60s")
        self.set_universe_key("yb.health.cluster_health.nodes_safe_to_take_down.interval", "60s")

        plan = [("idle", a.idle, None, None),
                ("saturate", a.saturate, self.start_saturate, self.stop_saturate),
                ("write", a.write, self.start_write, self.stop_write)]
        plan = [p for p in plan if p[0] in a.phases.split(",")]
        total = sum(p[1] + a.cooldown for p in plan) + 600
        scraper = threading.Thread(target=self.scraper, daemon=True)
        try:
            scraper.start()
            sampler = self.start_sampler(total)
            for name, duration, start, stop in plan:
                log("phase %s: %ds" % (name, duration))
                handle = start(duration) if start else None
                before = self.pod_incarnations()
                t0 = now_ms()
                time.sleep(duration)
                t1 = now_ms()
                if stop:
                    stop(handle)
                hits = self.disruptions(before, self.pod_incarnations())
                phase = {"name": name, "start_ms": t0, "end_ms": t1,
                         "disruptions": ["%s %s" % (t or "?", what) for t, what in hits]}
                if hits:
                    log("phase %s disrupted: %s" % (name, "; ".join(phase["disruptions"])))
                    # A liveness kill follows minutes of failed probes and an eviction precedes the
                    # new pod's start, so the clean window ends a minute before the first event.
                    times = [iso_ms(t) - 60000 for t, _ in hits if t]
                    phase["clean_end_ms"] = max(t0, min(times)) if times else t0
                self.phases.append(phase)
                self.write_meta(meta)
                log("cooldown %ds" % a.cooldown)
                time.sleep(a.cooldown)
        finally:
            self.stop.set()
            self.kube.exec(TOOLS_POD, "pkill -f sttd-sampler.sh || true", check=False)
            self.kube.exec(BENCH_POD, "pkill -f ysql_bench || true", check=False)
            self.kube.run("delete", "pod", WRITER_POD, "--ignore-not-found", "--wait=false", check=False)
            for p in self.procs:
                try:
                    p.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    p.kill()
            self.restore_keys()
            self.write_meta(meta)
        summary = summarize(self.out)
        log("summary written: %s" % os.path.join(self.out, "summary.json"))
        print(json.dumps(summary["phases"], indent=1))

    def write_meta(self, meta):
        meta = dict(meta, phases=self.phases)
        with open(os.path.join(self.out, "meta.json"), "w") as f:
            json.dump(meta, f, indent=1)

    def cleanup(self):
        self.kube.run("delete", "pod", "-l", "app=" + POD_LABEL, "--ignore-not-found", "--wait=false")
        log("deleted %s pods; filler tables are left in place" % POD_LABEL)


# -------------------------------------------------------------------- summary


def iso_ms(ts):
    """Kubernetes RFC 3339 UTC time ("2026-11-14T00:12:03Z") to epoch ms."""
    import calendar
    return calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")) * 1000


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def read_samples(out):
    rows = []
    path = os.path.join(out, "samples.raw")
    if not os.path.exists(path):
        return rows
    with open(path) as f:
        for line in f:
            parts = line.rstrip("\n").split(",", 5)
            if len(parts) < 5 or parts[0] not in ("sttd", "noop"):
                continue
            rows.append({"kind": parts[0], "t_ms": int(parts[1]), "target": parts[2],
                         "wall_ms": int(parts[3]), "rc": int(parts[4]),
                         "out": parts[5] if len(parts) > 5 else ""})
    return rows


def read_metrics(out):
    path = os.path.join(out, "metrics.csv")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def classify(sample):
    if sample["rc"] == 0:
        return "safe"
    out = sample["out"].lower()
    if "unsafe" in out or "enough replicas" in out:
        return "unsafe"
    return "error"


def delta_mean(metrics, start_ms, end_ms, sum_key, count_key):
    win = [m for m in metrics if start_ms <= int(m["t_ms"]) <= end_ms]
    if len(win) < 2:
        return None, 0
    d_sum = float(win[-1][sum_key]) - float(win[0][sum_key])
    d_cnt = float(win[-1][count_key]) - float(win[0][count_key])
    if d_cnt <= 0:
        return None, 0
    return d_sum / d_cnt / 1000.0, int(d_cnt)


def summarize(out, warmup_ms=30000):
    meta = json.load(open(os.path.join(out, "meta.json")))
    samples = read_samples(out)
    metrics = read_metrics(out)
    phases = {}
    for ph in meta["phases"]:
        # Latency stats cover the phase up to its first disruption (pod restart or eviction); the
        # rest of the phase is reported separately, since those calls measure an outage.
        start, end = ph["start_ms"] + warmup_ms, ph.get("clean_end_ms", ph["end_ms"])
        tail = [s for s in samples if s["kind"] == "sttd" and end < s["t_ms"] <= ph["end_ms"]]
        win = [s for s in samples if start <= s["t_ms"] <= end]
        sttd = [s for s in win if s["kind"] == "sttd"]
        noop = [s["wall_ms"] for s in win if s["kind"] == "noop" and s["rc"] == 0]
        walls = [s["wall_ms"] for s in sttd]
        classes = [classify(s) for s in sttd]
        mwin = [m for m in metrics if start <= int(m["t_ms"]) <= end]
        master_mean, master_n = delta_mean(metrics, start, end, "master_sttd_sum_us", "master_sttd_count")
        ts_mean, _ = delta_mean(metrics, start, end, "ts_health_sum_us", "ts_health_count")
        q_mean, _ = delta_mean(metrics, start, end, "ts_queue_sum_us", "ts_queue_count")
        yba = {}
        for m in mwin:
            if m.get("yba_sweep_checked_at") and m.get("yba_sweep_duration_ms"):
                yba[m["yba_sweep_checked_at"]] = int(m["yba_sweep_duration_ms"])
        phases[ph["name"]] = {
            "disruptions": ph.get("disruptions", []),
            "clean_seconds": max(0, (end - start) // 1000),
            "disrupted_tail": {
                "samples": len(tail),
                "unsafe": sum(1 for s in tail if classify(s) == "unsafe"),
                "error": sum(1 for s in tail if classify(s) == "error"),
                "max_wall_ms": max((s["wall_ms"] for s in tail), default=None),
            },
            "samples": len(sttd),
            "safe": classes.count("safe"),
            "unsafe": classes.count("unsafe"),
            "error": classes.count("error"),
            "wall_ms": {
                "p50": pct(walls, 50), "p90": pct(walls, 90), "p99": pct(walls, 99),
                "max": max(walls) if walls else None,
                "mean": statistics.mean(walls) if walls else None,
            },
            "yb_admin_overhead_ms_p50": pct(noop, 50),
            "master_handler_mean_ms": master_mean,
            "master_handler_calls": master_n,
            "tserver_health_rpc_mean_ms": ts_mean,
            "tserver_rpc_queue_mean_ms": q_mean,
            "max_follower_lag_ms": max((float(m["max_follower_lag_ms"]) for m in mwin), default=None),
            "tserver_cpu_m_avg": statistics.mean(float(m["tserver_cpu_m_avg"]) for m in mwin) if mwin else None,
            "tserver_cpu_m_max": max((float(m["tserver_cpu_m_max"]) for m in mwin), default=None),
            "master_cpu_m_max": max((float(m["master_cpu_m_max"]) for m in mwin), default=None),
            "yba_statuses": sorted({m.get("yba_status") for m in mwin if m.get("yba_status")}),
            "yba_sweep_ms": sorted(yba.values()),
            "unsafe_examples": [s["out"][:300] for s in sttd if classify(s) != "safe"][:3],
        }
    phases_tp = throughput(out, meta)
    for name, tp in phases_tp.items():
        if name in phases:
            phases[name]["load_throughput"] = tp
    summary = {"meta": meta, "phases": phases}
    with open(os.path.join(out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=1)
    return summary


def throughput(out, meta):
    result = {}
    path = os.path.join(out, "ysql_bench.log")
    if os.path.exists(path):
        tps = [float(m.group(1)) for m in re.finditer(r"progress: [0-9.]+ s, ([0-9.]+) tps", open(path).read())]
        if tps:
            result["saturate"] = "ysql_bench %.0f tps (median of %d 10s windows)" % (statistics.median(tps), len(tps))
    path = os.path.join(out, "writer.log")
    if os.path.exists(path):
        ops = [float(m.group(1)) for m in re.finditer(r"Write: ([0-9.]+) ops/sec", open(path).read())]
        if ops:
            vs = meta["args"]["writer_value_size"]
            med = statistics.median(ops)
            result["write"] = "%.0f rows/s x %d B = %.0f MB/s (median)" % (med, vs, med * vs / 1e6)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["run", "cleanup", "summarize"])
    p.add_argument("--universe-name", required=True)
    p.add_argument("--context", default=os.environ.get("KUBE_CONTEXT"),
                   help="kubectl context (default: KUBE_CONTEXT, else the current context)")
    p.add_argument("--namespace", default=os.environ.get("KUBE_NAMESPACE"),
                   required=not os.environ.get("KUBE_NAMESPACE"),
                   help="the universe's namespace (default: KUBE_NAMESPACE)")
    p.add_argument("--zone", help="nodeSelector zone for the bench pods (default: the tservers' zone)")
    p.add_argument("--tablets", type=int, default=500, help="target user tablets per tserver")
    p.add_argument("--label")
    p.add_argument("--out", default="results/run")
    p.add_argument("--phases", default="idle,saturate,write", help="phases to run, in this order")
    p.add_argument("--idle", type=int, default=180)
    p.add_argument("--saturate", type=int, default=300)
    p.add_argument("--write", type=int, default=300)
    p.add_argument("--cooldown", type=int, default=60)
    p.add_argument("--bench-cpu", type=int, default=6)
    p.add_argument("--bench-clients", type=int, default=128)
    p.add_argument("--bench-threads", type=int, default=12)
    p.add_argument("--bench-scale", type=int, default=20)
    p.add_argument("--writer-cpu", type=int, default=6)
    p.add_argument("--writer-threads", type=int, default=64)
    p.add_argument("--writer-batch", type=int, default=32)
    p.add_argument("--writer-value-size", type=int, default=16384)
    p.add_argument("--sample-apps-image", default="yugabytedb/yb-sample-apps:latest")
    a = p.parse_args()
    if a.command == "summarize":
        print(json.dumps(summarize(a.out)["phases"], indent=1))
        return
    b = Bench(a)
    if a.command == "cleanup":
        b.cleanup()
    else:
        b.run()


if __name__ == "__main__":
    main()
