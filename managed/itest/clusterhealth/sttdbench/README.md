# AreNodesSafeToTakeDown latency bench

Measures how long `AreNodesSafeToTakeDown` takes for one tserver of a YBA-managed Kubernetes
universe at a given tablet count, idle and under load. The cost model it probes, from
`src/yb/master/tablet_health_manager.cc`:

- The master leader walks every tablet of every running table, then sends one
  `CheckTserverTabletHealth` RPC to each other tserver that holds a replica of those tablets.
- That RPC waits in the tserver's normal RPC queue.
- The master waits until every tablet has a majority of replicas, outside the checked node, whose
  follower lag is under the bound. Otherwise it stops at the client deadline minus 2s (28s with
  YBA's 30s timeout).

## Run

```sh
export YBA_URL=... YBA_API_TOKEN=... YBA_CUSTOMER_UUID=...   # KUBE_NAMESPACE required, KUBE_CONTEXT optional
./sttd_bench.py run --universe-name my-universe --tablets 500  --out results/t500
./sttd_bench.py run --universe-name my-universe --tablets 2000 --out results/t2000
./sttd_bench.py run --universe-name my-universe --tablets 5000 --out results/t5000
python3 -m venv venv && venv/bin/pip install matplotlib
venv/bin/python chart.py results/charts results/t500 results/t2000 results/t5000
./sttd_bench.py cleanup --universe-name my-universe
```

A run:

1. Creates bench pods with the universe's DB image: `sttd-tools` for `yb-admin`, `ysqlsh`,
   `ycqlsh` and `curl`, and `sttd-ysqlbench`.
2. Initializes `ysql_bench` (once) and the writer's YCQL table.
3. Adds empty `sttd_filler_NNNN` tables (`SPLIT INTO 50 TABLETS`) until each tserver holds
   `--tablets` user tablets. It then waits until every tablet has a leader.
4. Runs three phases: `idle`, `saturate` (`ysql_bench -N`, 128 clients) and `write`
   (`CassandraBatchKeyValue`, 64 threads, 16 KiB values). Each phase is followed by a 60s cooldown.

Runs are cumulative: filler tables are never dropped.

Throughout the run:

- `sttd-tools` calls `yb-admin are_nodes_safe_to_take_down <tserver uuid> 60000`, rotating
  through the tservers with a 1s pause. Every fifth call it also times `list_all_masters`, to
  measure the overhead of `yb-admin` itself.
- Every 15s it records:
  - the master `AreNodesSafeToTakeDown` and tserver `CheckTserverTabletHealth` handler latency
    (sum/count);
  - tserver RPC queue time and overflows;
  - the max `follower_lag_ms`;
  - pod CPU;
  - the universe's `/cluster-health` result.

  YBA's own sweeps are set to every 60s for the run, then restored.

`--phases idle,saturate` runs a subset of the phases, for example to re-measure them.

A phase in which a DB pod restarts or is evicted is cut short for the latency stats. The cut is a
minute before the first event, because a liveness kill follows several failed probes. The calls
after the cut are counted separately as `disrupted_tail`. Calls during an outage measure the
outage, not the check.

On `pd-standard` disks (YBA's `yb-standard` class), the `write` phase can stall the tservers'
liveness disk probe, which has a 1s timeout. The tservers then restart, and with thousands of
tablets they can loop. Use an SSD-class storage class, or relax the probe on the tserver
StatefulSet.

Output in `--out`:

- `samples.raw`: one line per call;
- `metrics.csv`;
- `ysql_bench.log` and `writer.log`;
- `meta.json`;
- `summary.json`: per-phase percentiles. The first 30s of each phase are excluded.
