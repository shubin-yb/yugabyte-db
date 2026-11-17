# Cluster health exploratory itest

An end-to-end run of the universe cluster-health feature against a live YBA and a Kubernetes
universe. It covers the v2 `GET /api/v2/customers/{c}/universes/{u}/cluster-health` endpoint,
the `ybp_universe_cluster_health_status` gauge and the
`ybp_nodes_safe_to_take_down_call_duration_seconds` histogram. It is stdlib Go behind the `itest`
build tag and is not wired into sbt.

The YBA under test must run a build that has the endpoint and the
`yb.health.cluster_health.*` runtime keys. Without them, setup fails with 404
`No mutable key found`.

## Run

```sh
cd managed/itest/clusterhealth
export YBA_URL=http://<yba-host> YBA_API_TOKEN=... YBA_CUSTOMER_UUID=... YBA_UNIVERSE_UUID=...
export KUBE_CONTEXT=... KUBE_NAMESPACE=... TSERVER_SERVICE=... TSERVER_POD=...
go test -tags itest -v -count=1 -timeout 90m -run TestClusterHealthUnderLoad . -args -fault=follower-lag
```

`-timeout` must cover the worst case. The test computes it (44m with the defaults for `follower-lag`, 50m for `pod-kill`) and
refuses to start without that much headroom, because a `go test` timeout panic skips the
cleanups. Ctrl-C does run them.

## Flow

1. **Preflight.** Checks `app_version`, the universe (fails if it is paused), the tserver
   service, the pod, and the endpoint. Then it sets these universe-scope keys:
   - `yb.health.cluster_health.evaluation_interval=60s`
   - `yb.health.cluster_health.nodes_safe_to_take_down.interval=60s`
   - `yb.health.trigger_api.enabled=true`

   Cleanup restores each key to the universe-scope value it had before the run, or deletes it.
2. **Baseline.** Triggers a health check and waits for HEALTHY, then for gauge 1. UNKNOWN and an
   absent gauge are expected after a YBA restart.
3. **Load.** Runs one `yb-sample-apps` pod per workload against `TSERVER_SERVICE`. Sql\* workloads
   use port 5433 and Cassandra\* workloads use 9042. The phase lasts `-load-duration`, and any
   UNHEALTHY sample fails it.
4. **Fault.** Set with `-fault`:
   - `follower-lag` sets `yb.checks.follower_lag.max_threshold=1ms`. It expects UNHEALTHY with
     `NODES_NOT_SAFE_TO_TAKE_DOWN` within 4m, gauge 0, and a higher `result="unsafe"` count.
     It then restores the key and expects HEALTHY within 5m and gauge 1.
   - `pod-kill` deletes `TSERVER_POD`. A non-HEALTHY status within 3m is logged, not required.
     It then needs a 3m HEALTHY streak that starts within 15m, after the replacement pod is Ready.
   - `none` skips the fault.
5. **Report.** Stops the load and prints a timeline with one row per poll:
   - status, reason codes, health-check and sweep results and ages, and the sweep's `duration_ms`;
   - per-node statuses and the gauge;
   - the histogram `_count`, plus the new observations and their mean, for `cluster_health` and `task`.

   Then it summarizes status and gauge transitions, the distinct sweeps, per-node call durations,
   and histogram deltas. It fails if `_count{caller="cluster_health"}` did not grow.

The health check is forced every `-hc-trigger-interval`, because `yb.health.check_interval_ms` is
static.

## Environment and flags

| Env | Default |
|---|---|
| `YBA_URL` | `http://localhost:9000` |
| `YBA_API_TOKEN`, `YBA_CUSTOMER_UUID`, `YBA_UNIVERSE_UUID` | required (the test is skipped without them) |
| `YBA_INSECURE_TLS` | `false` |
| `KUBE_CONTEXT` | the current kubectl context |
| `KUBE_NAMESPACE` | required: the universe's namespace |
| `TSERVER_SERVICE` | required: the universe's tserver service, such as `yb<name>-<suffix>-yb-tservers` |
| `TSERVER_POD` | required for `-fault=pod-kill`: the tserver pod to delete |
| `DB_USER`, `DB_PASSWORD` | unset. The password is passed as a pod argument, so it ends up in the pod spec. |

| Flag (env) | Default |
|---|---|
| `-fault` (`FAULT`) | `follower-lag` |
| `-load-duration` (`LOAD_DURATION`) | `6m` |
| `-poll-interval` (`POLL_INTERVAL`) | `15s` |
| `-hc-trigger-interval` (`HC_TRIGGER_INTERVAL`) | `2m` |
| `-baseline-timeout` (`BASELINE_TIMEOUT`) | `10m` |
| `-workloads` (`WORKLOADS`, empty = no load) | `SqlInserts,CassandraKeyValue` |
| `-sample-apps-image` (`SAMPLE_APPS_IMAGE`) | `yugabytedb/yb-sample-apps:latest` |
| `-write-threads`, `-read-threads` | `2`, `2` |

Every kubectl call passes `-n`, and `--context` when `KUBE_CONTEXT` is set.

## Manual cleanup after `kill -9` or a timeout

The workload pods stop on their own once `--run_time` (the test budget) passes.

```sh
kubectl ${KUBE_CONTEXT:+--context "$KUBE_CONTEXT"} -n "$KUBE_NAMESPACE" delete pod -l app=cluster-health-itest
for k in yb.health.cluster_health.evaluation_interval \
         yb.health.cluster_health.nodes_safe_to_take_down.interval \
         yb.health.trigger_api.enabled yb.checks.follower_lag.max_threshold; do
  curl -s -X DELETE -H "X-AUTH-YW-API-TOKEN: $YBA_API_TOKEN" \
    "$YBA_URL/api/v1/customers/$YBA_CUSTOMER_UUID/runtime_config/$YBA_UNIVERSE_UUID/key/$k"
done
```

This deletes the keys rather than restoring them. Re-set any key that had a universe-scope value
before the run.
