//go:build itest

package clusterhealth

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"text/tabwriter"
	"time"
)

const (
	apiTokenHeader = "X-AUTH-YW-API-TOKEN"

	gaugeMetric = "ybp_universe_cluster_health_status"
	histMetric  = "ybp_nodes_safe_to_take_down_call_duration_seconds"
	callerCH    = "cluster_health"
	callerTask  = "task"

	keyEvalInterval = "yb.health.cluster_health.evaluation_interval"
	keySttdInterval = "yb.health.cluster_health.nodes_safe_to_take_down.interval"
	keyTriggerAPI   = "yb.health.trigger_api.enabled"
	keyFollowerLag  = "yb.checks.follower_lag.max_threshold"

	statusHealthy      = "HEALTHY"
	statusUnhealthy    = "UNHEALTHY"
	reasonNodesNotSafe = "NODES_NOT_SAFE_TO_TAKE_DOWN"

	// The checker ticks every minute and re-evaluates a universe 60s (+<=6s jitter) after the
	// previous evaluation, so the gauge can trail the API (which recomposes on every GET) by up
	// to two ticks.
	gaugeSettle = 3 * time.Minute

	podStartTimeout             = 5 * time.Minute
	followerLagUnhealthyTimeout = 4 * time.Minute
	followerLagRecoverTimeout   = 5 * time.Minute
	podKillNotHealthyWindow     = 3 * time.Minute
	podKillRecoverTimeout       = 15 * time.Minute
	// node_health.py flags process uptime < 8 min, but only once a health check runs after the
	// restart; a HEALTHY streak this long spans at least one forced health check.
	podKillStableFor = 3 * time.Minute

	workloadLabel = "cluster-health-itest"
)

var envErrs []string

var (
	flagLoadDuration    = flag.Duration("load-duration", envDuration("LOAD_DURATION", 6*time.Minute), "how long to run the workload before the fault phase")
	flagPollInterval    = flag.Duration("poll-interval", envDuration("POLL_INTERVAL", 15*time.Second), "API and metrics sampling interval")
	flagHCTrigger       = flag.Duration("hc-trigger-interval", envDuration("HC_TRIGGER_INTERVAL", 2*time.Minute), "how often to force a stored health check (yb.health.check_interval_ms is static)")
	flagBaselineTimeout = flag.Duration("baseline-timeout", envDuration("BASELINE_TIMEOUT", 10*time.Minute), "max wait for the first HEALTHY")
	flagFault           = flag.String("fault", envOr("FAULT", "follower-lag"), "fault after the load phase: follower-lag | pod-kill | none")
	flagWorkloads       = flag.String("workloads", envOr("WORKLOADS", "SqlInserts,CassandraKeyValue"), "comma-separated yb-sample-apps workloads (Sql* -> YSQL 5433, Cassandra* -> YCQL 9042); empty runs no load")
	flagImage           = flag.String("sample-apps-image", envOr("SAMPLE_APPS_IMAGE", "yugabytedb/yb-sample-apps:latest"), "yb-sample-apps image")
	flagWriteThreads    = flag.Int("write-threads", 2, "--num_threads_write per workload")
	flagReadThreads     = flag.Int("read-threads", 2, "--num_threads_read per workload")
)

func TestClusterHealthUnderLoad(t *testing.T) {
	cfg := loadConfig(t)

	budget := 2*time.Minute + *flagBaselineTimeout + gaugeSettle + podStartTimeout +
		*flagLoadDuration + faultBudget(cfg.fault) + 3*time.Minute
	// A go test -timeout panic skips t.Cleanup, which would leave the runtime config overrides
	// and workload pods behind, so refuse to start without enough headroom.
	if dl, ok := t.Deadline(); ok && time.Until(dl) < budget {
		t.Fatalf("worst-case run time is ~%v but the test deadline is in %v; pass -timeout %v",
			budget, time.Until(dl).Round(time.Second), (budget + 10*time.Minute).Round(time.Minute))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	t.Cleanup(stop)

	r := newRunner(t, ctx, cfg, budget)
	t.Cleanup(r.report)

	// 0) preflight and per-universe config.
	r.preflight()
	r.configureUniverse()

	// 1) baseline. UNKNOWN and an absent gauge are expected at first: the sweep cache and the
	// gauge are in-memory and empty after every YBA restart.
	r.triggerHealthCheck()
	s, ok := r.pollUntil("baseline", *flagBaselineTimeout, statusIs(statusHealthy), nil)
	if !ok {
		t.Fatalf("universe not HEALTHY within %v; last sample: %s", *flagBaselineTimeout, describeSample(s))
	}
	r.expectGauge("baseline", 1)

	// 2) clean load.
	r.startWorkloads()
	loadFrom := len(r.samples)
	r.pollUntil("load", *flagLoadDuration, nil, r.checkWorkloads)
	r.assertNoUnhealthy(r.samples[loadFrom:])

	// 3) fault.
	switch cfg.fault {
	case "follower-lag":
		r.faultFollowerLag()
	case "pod-kill":
		r.faultPodKill()
	}

	// 4) results.
	r.stopWorkloads()
	r.report()
	r.assertHistogramGrew()
}

// ---------------------------------------------------------------------------------------------
// Configuration

type config struct {
	ybaURL, token, customer, universe string
	insecureTLS                       bool
	kubeContext, namespace            string
	tserverService, tserverPod        string
	dbUser, dbPassword                string
	workloads                         []string
	fault                             string
}

func loadConfig(t *testing.T) config {
	t.Helper()
	if len(envErrs) > 0 {
		t.Fatalf("bad environment: %s", strings.Join(envErrs, "; "))
	}
	c := config{
		ybaURL:         strings.TrimRight(envOr("YBA_URL", "http://localhost:9000"), "/"),
		token:          os.Getenv("YBA_API_TOKEN"),
		customer:       os.Getenv("YBA_CUSTOMER_UUID"),
		universe:       os.Getenv("YBA_UNIVERSE_UUID"),
		insecureTLS:    os.Getenv("YBA_INSECURE_TLS") == "true",
		kubeContext:    os.Getenv("KUBE_CONTEXT"),
		namespace:      os.Getenv("KUBE_NAMESPACE"),
		tserverService: os.Getenv("TSERVER_SERVICE"),
		tserverPod:     os.Getenv("TSERVER_POD"),
		dbUser:         os.Getenv("DB_USER"),
		dbPassword:     os.Getenv("DB_PASSWORD"),
		workloads:      splitList(*flagWorkloads),
		fault:          *flagFault,
	}
	var missing []string
	for _, kv := range [][2]string{
		{"YBA_API_TOKEN", c.token}, {"YBA_CUSTOMER_UUID", c.customer}, {"YBA_UNIVERSE_UUID", c.universe},
		{"KUBE_NAMESPACE", c.namespace}, {"TSERVER_SERVICE", c.tserverService},
	} {
		if kv[1] == "" {
			missing = append(missing, kv[0])
		}
	}
	if len(missing) > 0 {
		t.Skipf("set %s to run this test against a live YBA", strings.Join(missing, ", "))
	}
	switch c.fault {
	case "follower-lag", "none":
	case "pod-kill":
		if c.tserverPod == "" {
			t.Fatalf("-fault=pod-kill needs TSERVER_POD, the tserver pod to delete")
		}
	default:
		t.Fatalf("-fault=%q: want follower-lag, pod-kill or none", c.fault)
	}
	for _, w := range c.workloads {
		if workloadPort(w) == 0 {
			t.Fatalf("workload %q: only Sql* (YSQL) and Cassandra* (YCQL) workloads are supported", w)
		}
	}
	return c
}

func faultBudget(fault string) time.Duration {
	switch fault {
	case "follower-lag":
		return followerLagUnhealthyTimeout + followerLagRecoverTimeout + 2*gaugeSettle
	case "pod-kill":
		return podKillRecoverTimeout + podKillStableFor + gaugeSettle
	}
	return 0
}

func workloadPort(w string) int {
	switch {
	case strings.HasPrefix(w, "Sql"):
		return 5433
	case strings.HasPrefix(w, "Cassandra"):
		return 9042
	}
	return 0
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		envErrs = append(envErrs, fmt.Sprintf("%s=%q: %v", key, v, err))
		return def
	}
	return d
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ---------------------------------------------------------------------------------------------
// Runner: owns the timeline, the YBA client and the kubectl calls.

type event struct {
	at  time.Time
	msg string
}

type sample struct {
	at      time.Time
	phase   string
	health  *clusterHealth
	apiErr  error
	metrics *metricsSnapshot
	metErr  error
}

type runner struct {
	t      *testing.T
	ctx    context.Context
	cfg    config
	api    *ybaClient
	start  time.Time
	budget time.Duration
	runID  string

	samples     []*sample
	events      []event
	lastTrigger time.Time
	reported    bool

	// Universe-scope runtime config the test changed: key -> value before the test, nil when the
	// key was not set at universe scope (restore = DELETE).
	overrides  map[string]string
	saved      map[string]*string
	savedOrder []string

	workloadPods     []string
	workloadsStopped bool
	workloadWarned   map[string]bool
}

func newRunner(t *testing.T, ctx context.Context, cfg config, budget time.Duration) *runner {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.insecureTLS {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &runner{
		t:              t,
		ctx:            ctx,
		cfg:            cfg,
		api:            &ybaClient{base: cfg.ybaURL, token: cfg.token, http: &http.Client{Transport: tr, Timeout: 60 * time.Second}},
		start:          time.Now(),
		budget:         budget,
		runID:          time.Now().Format("150405"),
		saved:          map[string]*string{},
		workloadWarned: map[string]bool{},
	}
}

func (r *runner) event(format string, args ...any) {
	e := event{at: time.Now(), msg: fmt.Sprintf(format, args...)}
	r.events = append(r.events, e)
	r.t.Logf("[%s] %s", fmtElapsed(e.at.Sub(r.start)), e.msg)
}

func (r *runner) custPath(format string, args ...any) string {
	return fmt.Sprintf("/api/v1/customers/%s", r.cfg.customer) + fmt.Sprintf(format, args...)
}

func (r *runner) preflight() {
	t := r.t
	ctx, cancel := context.WithTimeout(r.ctx, time.Minute)
	defer cancel()

	var ver map[string]any
	if err := r.api.getJSON(ctx, "/api/v1/app_version", &ver); err != nil {
		t.Fatalf("YBA at %s not reachable: %v", r.cfg.ybaURL, err)
	}
	t.Logf("YBA %s version %v", r.cfg.ybaURL, ver["version"])

	var u universeResp
	if err := r.api.getJSON(ctx, r.custPath("/universes/%s", r.cfg.universe), &u); err != nil {
		t.Fatalf("get universe: %v", err)
	}
	t.Logf("universe %s: %s", u.Name, u.summary())
	if u.Details.UniversePaused {
		t.Fatalf("universe %s is paused", u.Name)
	}
	if u.Details.UpdateInProgress {
		t.Logf("warning: universe %s has a task in progress (%s); status stays UNKNOWN until it ends",
			u.Name, u.Details.UpdatingTask)
	}

	if len(r.cfg.workloads) > 0 || r.cfg.fault == "pod-kill" {
		if _, err := exec.LookPath("kubectl"); err != nil {
			t.Fatalf("kubectl not on PATH: %v", err)
		}
		if _, err := r.kubectl(30*time.Second, "get", "service", r.cfg.tserverService, "-o", "name"); err != nil {
			t.Fatalf("tserver service: %v", err)
		}
		if r.cfg.fault == "pod-kill" {
			if _, err := r.kubectl(30*time.Second, "get", "pod", r.cfg.tserverPod, "-o", "name"); err != nil {
				t.Fatalf("tserver pod for -fault=pod-kill: %v", err)
			}
		}
	}

	h, err := r.fetchHealth()
	if err != nil {
		t.Fatalf("cluster-health API: %v (is a YBA build with the endpoint deployed?)", err)
	}
	t.Logf("initial cluster health: %s", describeHealth(h))
}

func (r *runner) configureUniverse() {
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	var scoped struct {
		ConfigEntries []struct {
			Inherited bool   `json:"inherited"`
			Key       string `json:"key"`
			Value     string `json:"value"`
		} `json:"configEntries"`
	}
	if err := r.api.getJSON(ctx, r.custPath("/runtime_config/%s", r.cfg.universe), &scoped); err != nil {
		r.t.Fatalf("read universe-scope runtime config: %v", err)
	}
	r.overrides = map[string]string{}
	for _, e := range scoped.ConfigEntries {
		if !e.Inherited {
			r.overrides[e.Key] = e.Value
		}
	}
	r.t.Cleanup(r.restoreAllKeys)

	// Durations go through HOCON parsing (ConfDataType.DurationType), so "60s"/"1ms" are valid.
	r.setKey(keyEvalInterval, "60s")
	r.setKey(keySttdInterval, "60s")
	r.setKey(keyTriggerAPI, "true")
}

func (r *runner) keyPath(key string) string {
	return r.custPath("/runtime_config/%s/key/%s", r.cfg.universe, key)
}

func (r *runner) setKey(key, value string) {
	if _, tracked := r.saved[key]; !tracked {
		if prev, ok := r.overrides[key]; ok {
			r.saved[key] = &prev
		} else {
			r.saved[key] = nil
		}
		r.savedOrder = append(r.savedOrder, key)
	}
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	// RuntimeConfController.setKey rejects anything but exactly "text/plain".
	if _, err := r.api.do(ctx, http.MethodPut, r.keyPath(key), "text/plain", []byte(value)); err != nil {
		r.t.Fatalf("set %s=%s on the universe: %v", key, value, err)
	}
	r.event("set %s=%s (universe scope)", key, value)
}

func (r *runner) restoreKey(ctx context.Context, key string) error {
	prev, tracked := r.saved[key]
	if !tracked {
		return nil
	}
	var err error
	if prev != nil {
		_, err = r.api.do(ctx, http.MethodPut, r.keyPath(key), "text/plain", []byte(*prev))
	} else {
		_, err = r.api.do(ctx, http.MethodDelete, r.keyPath(key), "", nil)
		// 404 means the entry is already gone (RuntimeConfigEntry.getOrBadRequest answers
		// NOT_FOUND despite its name) or the YBA build lacks the key, which then was never set.
		var he *httpError
		if errors.As(err, &he) && he.code == http.StatusNotFound {
			err = nil
		}
	}
	if err != nil {
		return err
	}
	delete(r.saved, key)
	if prev != nil {
		r.event("restored %s=%s (universe scope)", key, *prev)
	} else {
		r.event("deleted universe-scope %s", key)
	}
	return nil
}

func (r *runner) restoreAllKeys() {
	for i := len(r.savedOrder) - 1; i >= 0; i-- {
		key := r.savedOrder[i]
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := r.restoreKey(ctx, key); err != nil {
			r.t.Errorf("restore universe-scope %s failed, fix by hand (README): %v", key, err)
		}
		cancel()
	}
}

func (r *runner) triggerHealthCheck() {
	r.lastTrigger = time.Now()
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	if _, err := r.api.do(ctx, http.MethodGet, r.custPath("/universes/%s/trigger_health_check", r.cfg.universe), "", nil); err != nil {
		r.event("health check trigger failed: %v", err)
		return
	}
	r.event("hc triggered")
}

func (r *runner) fetchHealth() (*clusterHealth, error) {
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	var h clusterHealth
	path := fmt.Sprintf("/api/v2/customers/%s/universes/%s/cluster-health", r.cfg.customer, r.cfg.universe)
	if err := r.api.getJSON(ctx, path, &h); err != nil {
		return nil, err
	}
	return &h, nil
}

func (r *runner) fetchMetrics() (*metricsSnapshot, error) {
	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()
	// MetricsController extends the unauthenticated Controller; the token header is harmless.
	b, err := r.api.do(ctx, http.MethodGet, "/api/v1/prometheus_metrics", "", nil)
	if err != nil {
		return nil, err
	}
	return parseMetrics(b, r.cfg.universe), nil
}

func (r *runner) poll(phase string) *sample {
	s := &sample{at: time.Now(), phase: phase}
	s.health, s.apiErr = r.fetchHealth()
	s.metrics, s.metErr = r.fetchMetrics()
	r.samples = append(r.samples, s)
	if s.metErr != nil {
		r.t.Logf("metrics scrape failed: %v", s.metErr)
	} else if s.metrics.gaugeSeries > 1 {
		r.t.Logf("warning: %d %s series for universe %s", s.metrics.gaugeSeries, gaugeMetric, r.cfg.universe)
	}
	r.t.Logf("%s", strings.Join(r.rowFields(len(r.samples)-1), " | "))
	return s
}

// pollUntil samples every poll interval until done(s) holds or timeout passes, forcing a stored
// health check every hc-trigger-interval. each runs on every sample before done. With a nil done
// it samples for the whole timeout. Returns the last sample and whether done held.
func (r *runner) pollUntil(phase string, timeout time.Duration, done func(*sample) bool, each func(*sample)) (*sample, bool) {
	deadline := time.Now().Add(timeout)
	for {
		if time.Since(r.lastTrigger) >= *flagHCTrigger {
			r.triggerHealthCheck()
		}
		s := r.poll(phase)
		if each != nil {
			each(s)
		}
		if done != nil && done(s) {
			return s, true
		}
		if !time.Now().Before(deadline) {
			return s, false
		}
		select {
		case <-r.ctx.Done():
			r.t.Fatalf("interrupted: %v", r.ctx.Err())
		case <-time.After(min(*flagPollInterval, time.Until(deadline)+time.Second)):
		}
	}
}

func statusIs(want string) func(*sample) bool {
	return func(s *sample) bool { return s.health != nil && s.health.Status == want }
}

func gaugeIs(want float64) func(*sample) bool {
	return func(s *sample) bool { return s.metrics != nil && s.metrics.gauge != nil && *s.metrics.gauge == want }
}

func hasReason(h *clusterHealth, code string) bool {
	for _, rs := range h.Reasons {
		if rs.Code == code {
			return true
		}
	}
	return false
}

func (r *runner) expectGauge(phase string, want float64) {
	if s, ok := r.pollUntil(phase, gaugeSettle, gaugeIs(want), r.checkWorkloads); !ok {
		r.t.Errorf("[%s] %s{universe_uuid=%s} is %s, want %g within %v of the API status change",
			phase, gaugeMetric, r.cfg.universe, gaugeStr(s), want, gaugeSettle)
	}
}

func (r *runner) assertNoUnhealthy(samples []*sample) {
	var bad []string
	for _, s := range samples {
		if s.health != nil && s.health.Status == statusUnhealthy {
			bad = append(bad, fmt.Sprintf("%s %s", fmtElapsed(s.at.Sub(r.start)), describeHealth(s.health)))
		}
	}
	if len(bad) > 0 {
		r.t.Errorf("%d of %d samples were UNHEALTHY under clean load:\n%s", len(bad), len(samples), strings.Join(bad, "\n"))
	}
}

func (r *runner) firstMetrics() *metricsSnapshot {
	for _, s := range r.samples {
		if s.metrics != nil {
			return s.metrics
		}
	}
	return nil
}

func (r *runner) lastMetrics() *metricsSnapshot {
	for i := len(r.samples) - 1; i >= 0; i-- {
		if r.samples[i].metrics != nil {
			return r.samples[i].metrics
		}
	}
	return nil
}

func (r *runner) assertHistogramGrew() {
	first, last := r.firstMetrics(), r.lastMetrics()
	if first == nil || first == last {
		r.t.Errorf("fewer than two successful metrics scrapes; cannot check %s_count", histMetric)
		return
	}
	n0, _ := first.total(callerCH, "")
	n1, _ := last.total(callerCH, "")
	switch {
	case n1 < n0:
		r.t.Errorf("%s_count{caller=%q} went from %v to %v: counter reset, did YBA restart during the run?", histMetric, callerCH, n0, n1)
	case n1 == n0:
		r.t.Errorf("%s_count{caller=%q} stayed at %v for the whole run", histMetric, callerCH, n0)
	}
}

// ---------------------------------------------------------------------------------------------
// Faults

func (r *runner) faultFollowerLag() {
	t := r.t
	before := r.lastMetrics()
	// Followers always trail the leader by more than 1ms, so every node with a leader replica
	// reports UNSAFE without disturbing the universe.
	r.setKey(keyFollowerLag, "1ms")
	s, ok := r.pollUntil("fault", followerLagUnhealthyTimeout, func(s *sample) bool {
		return statusIs(statusUnhealthy)(s) && hasReason(s.health, reasonNodesNotSafe)
	}, r.checkWorkloads)
	if !ok {
		t.Errorf("follower-lag fault: no UNHEALTHY/%s within %v; last sample: %s", reasonNodesNotSafe, followerLagUnhealthyTimeout, describeSample(s))
	} else {
		r.event("UNHEALTHY/%s observed", reasonNodesNotSafe)
		if after := r.lastMetrics(); before != nil && after != nil {
			b, _ := before.total(callerCH, "unsafe")
			a, _ := after.total(callerCH, "unsafe")
			if a <= b {
				t.Errorf("%s_count{caller=%q,result=\"unsafe\"} did not increase (%v -> %v) during the follower-lag fault", histMetric, callerCH, b, a)
			}
		}
		r.expectGauge("fault", 0)
	}

	ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	err := r.restoreKey(ctx, keyFollowerLag)
	cancel()
	if err != nil {
		t.Fatalf("clear follower-lag fault: %v", err)
	}
	s, ok = r.pollUntil("recover", followerLagRecoverTimeout, statusIs(statusHealthy), r.checkWorkloads)
	if !ok {
		t.Errorf("not HEALTHY within %v of clearing the follower-lag fault; last sample: %s", followerLagRecoverTimeout, describeSample(s))
		return
	}
	r.expectGauge("recover", 1)
}

func (r *runner) faultPodKill() {
	t := r.t
	pod := r.cfg.tserverPod
	oldUID, _ := r.podState(pod)
	if oldUID == "" {
		t.Fatalf("pod %s not found", pod)
	}
	if _, err := r.kubectl(time.Minute, "delete", "pod", pod, "--wait=false"); err != nil {
		t.Fatalf("delete %s: %v", pod, err)
	}
	deletedAt := time.Now()
	r.event("fault: deleted pod %s", pod)

	replaced := false
	monitor := func(s *sample) {
		r.checkWorkloads(s)
		if replaced {
			return
		}
		if uid, ready := r.podState(pod); uid != "" && uid != oldUID && ready == "True" {
			replaced = true
			r.event("pod %s replaced and Ready %v after the delete", pod, time.Since(deletedAt).Round(time.Second))
		}
	}

	if _, saw := r.pollUntil("fault", podKillNotHealthyWindow, func(s *sample) bool {
		return s.health != nil && s.health.Status != statusHealthy
	}, monitor); saw {
		r.event("non-HEALTHY observed after the pod delete")
	} else {
		t.Logf("soft: no non-HEALTHY status within %v of deleting %s", podKillNotHealthyWindow, pod)
	}

	var streak time.Time
	stable := func(s *sample) bool {
		if !replaced || !statusIs(statusHealthy)(s) {
			streak = time.Time{}
			return false
		}
		if streak.IsZero() {
			streak = s.at
		}
		return s.at.Sub(streak) >= podKillStableFor
	}
	s, ok := r.pollUntil("recover", podKillRecoverTimeout+podKillStableFor-time.Since(deletedAt), stable, monitor)
	if !ok || streak.Sub(deletedAt) > podKillRecoverTimeout {
		t.Errorf("no HEALTHY streak of %v starting within %v of deleting %s; last sample: %s",
			podKillStableFor, podKillRecoverTimeout, pod, describeSample(s))
		return
	}
	r.event("HEALTHY again %v after the pod delete", streak.Sub(deletedAt).Round(time.Second))
	r.expectGauge("recover", 1)
}

// ---------------------------------------------------------------------------------------------
// Kubernetes

func (r *runner) kubectl(timeout time.Duration, args ...string) (string, error) {
	return runKubectl(r.ctx, r.cfg, timeout, args...)
}

func runKubectl(parent context.Context, cfg config, timeout time.Duration, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	full := []string{"-n", cfg.namespace}
	if cfg.kubeContext != "" {
		full = append(full, "--context", cfg.kubeContext)
	}
	full = append(full, args...)
	out, err := exec.CommandContext(ctx, "kubectl", full...).CombinedOutput()
	s := strings.TrimSpace(string(out))
	if err != nil {
		// Only the verb and object: workload args can carry a DB password.
		return s, fmt.Errorf("kubectl %s: %w: %s", strings.Join(args[:min(2, len(args))], " "), err, snippet(s, 500))
	}
	return s, nil
}

func (r *runner) podState(pod string) (uid, ready string) {
	out, err := r.kubectl(30*time.Second, "get", "pod", pod, "--ignore-not-found",
		"-o", `jsonpath={.metadata.uid}{" "}{.status.conditions[?(@.type=="Ready")].status}`)
	if err != nil {
		return "", ""
	}
	f := strings.Fields(out)
	if len(f) > 0 {
		uid = f[0]
	}
	if len(f) > 1 {
		ready = f[1]
	}
	return uid, ready
}

func (r *runner) startWorkloads() {
	t := r.t
	if len(r.cfg.workloads) == 0 {
		r.event("no workloads configured; the load phase runs without load")
		return
	}
	// --run_time bounds the pods if the test dies without running its cleanups.
	runTime := strconv.Itoa(int(r.budget.Seconds()))
	// Registered before any `kubectl run`: one that fails or is interrupted may still have
	// created its pod.
	selector := "app=" + workloadLabel + ",run-id=" + r.runID
	t.Cleanup(func() {
		if _, err := runKubectl(context.Background(), r.cfg, time.Minute, "delete", "pod", "-l", selector, "--ignore-not-found", "--wait=false"); err != nil {
			t.Errorf("delete workload pods %s: %v", selector, err)
		}
	})
	for _, w := range r.cfg.workloads {
		name := fmt.Sprintf("ch-itest-%s-%s", strings.ToLower(w), r.runID)
		appArgs := []string{
			"--workload", w,
			"--nodes", fmt.Sprintf("%s:%d", r.cfg.tserverService, workloadPort(w)),
			"--num_threads_write", strconv.Itoa(*flagWriteThreads),
			"--num_threads_read", strconv.Itoa(*flagReadThreads),
			"--num_writes", "1000000000", "--num_reads", "1000000000", "--num_unique_keys", "1000000000",
			"--run_time", runTime,
		}
		if r.cfg.dbUser != "" {
			appArgs = append(appArgs, "--username", r.cfg.dbUser)
		}
		logged := strings.Join(appArgs, " ")
		if r.cfg.dbPassword != "" {
			appArgs = append(appArgs, "--password", r.cfg.dbPassword)
			logged += " --password ***"
		}
		args := append([]string{"run", name, "--image=" + *flagImage, "--restart=Never",
			"--labels=app=" + workloadLabel + ",run-id=" + r.runID, "--"}, appArgs...)
		if _, err := r.kubectl(time.Minute, args...); err != nil {
			t.Fatalf("start workload %s: %v", w, err)
		}
		r.workloadPods = append(r.workloadPods, name)
		r.event("started %s: %s", name, logged)
	}
	if _, err := r.kubectl(podStartTimeout+30*time.Second, "wait", "--for=condition=Ready", "pod",
		"-l", "run-id="+r.runID, "--timeout="+podStartTimeout.String()); err != nil {
		out, _ := r.kubectl(30*time.Second, "get", "pods", "-l", "run-id="+r.runID, "-o", "wide")
		t.Fatalf("workload pods not Ready within %v: %v\n%s", podStartTimeout, err, out)
	}
	r.event("workload pods Ready")
}

func (r *runner) checkWorkloads(*sample) {
	if len(r.workloadPods) == 0 || r.workloadsStopped {
		return
	}
	out, err := r.kubectl(30*time.Second, "get", "pods", "-l", "run-id="+r.runID,
		"-o", `jsonpath={range .items[*]}{.metadata.name}{"="}{.status.phase}{" "}{end}`)
	if err != nil {
		r.event("workload status check failed: %v", err)
		return
	}
	for _, kv := range strings.Fields(out) {
		name, phase, _ := strings.Cut(kv, "=")
		if phase != "Running" && !r.workloadWarned[name] {
			r.workloadWarned[name] = true
			r.event("workload pod %s is %s: samples after this are not under full load", name, phase)
		}
	}
}

func (r *runner) stopWorkloads() {
	if len(r.workloadPods) == 0 || r.workloadsStopped {
		return
	}
	for _, p := range r.workloadPods {
		if out, err := r.kubectl(time.Minute, "logs", p, "--tail=8"); err == nil {
			r.t.Logf("workload %s, last log lines:\n%s", p, out)
		}
	}
	if _, err := r.kubectl(time.Minute, "delete", "pod", "-l", "run-id="+r.runID, "--ignore-not-found", "--wait=false"); err != nil {
		r.t.Errorf("stop workloads: %v", err)
	}
	r.workloadsStopped = true
	r.event("workloads stopped")
}

// ---------------------------------------------------------------------------------------------
// Reporting

var tableHeader = []string{"elapsed", "phase", "status", "reasons", "hc", "hc_age", "sttd", "sttd_age",
	"sttd_ms", "nodes", "gauge", "hist cluster_health", "hist task", "events"}

// rowFields renders one timeline row without the events column. Histogram cells show the
// cumulative _count, then "+new@mean" for observations since the previous scrape.
func (r *runner) rowFields(i int) []string {
	s := r.samples[i]
	var prev *metricsSnapshot
	for j := i - 1; j >= 0; j-- {
		if r.samples[j].metrics != nil {
			prev = r.samples[j].metrics
			break
		}
	}
	f := []string{fmtElapsed(s.at.Sub(r.start)), s.phase}
	if h := s.health; h == nil {
		f = append(f, "ERR", snippet(errString(s.apiErr), 80), "-", "-", "-", "-", "-", "-")
	} else {
		hc, hcAge := "-", "-"
		if h.HealthCheck != nil {
			hc, hcAge = h.HealthCheck.Result, fmtAge(s.at, h.HealthCheck.CheckTime)
		}
		st, stAge, stMs := "-", "-", "-"
		if h.NodesSafeToTakeDown != nil {
			st, stAge = h.NodesSafeToTakeDown.Result, fmtAge(s.at, h.NodesSafeToTakeDown.CheckedAt)
			stMs = strconv.FormatInt(h.NodesSafeToTakeDown.DurationMs, 10)
		}
		f = append(f, h.Status, reasonCodes(h), hc, hcAge, st, stAge, stMs, nodesSummary(h.NodesSafeToTakeDown))
	}
	return append(f, gaugeStr(s), histCell(s.metrics, prev, callerCH), histCell(s.metrics, prev, callerTask))
}

func (r *runner) report() {
	if r.reported || len(r.samples) == 0 {
		return
	}
	r.reported = true
	var b bytes.Buffer

	b.WriteString("timeline:\n")
	tw := tabwriter.NewWriter(&b, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(tableHeader, "\t"))
	ei := 0
	for i, s := range r.samples {
		var evs []string
		for ; ei < len(r.events) && !r.events[ei].at.After(s.at); ei++ {
			evs = append(evs, r.events[ei].msg)
		}
		fmt.Fprintln(tw, strings.Join(append(r.rowFields(i), strings.Join(evs, "; ")), "\t"))
	}
	tw.Flush()
	for ; ei < len(r.events); ei++ {
		fmt.Fprintf(&b, "%s  %s\n", fmtElapsed(r.events[ei].at.Sub(r.start)), r.events[ei].msg)
	}

	b.WriteString("\nstatus transitions:\n")
	prevStatus, prevGauge := "", ""
	for _, s := range r.samples {
		st, rc := "ERR", "-"
		if s.health != nil {
			st, rc = s.health.Status, reasonCodes(s.health)
		}
		if st != prevStatus {
			fmt.Fprintf(&b, "  %s %-9s %s -> %s [%s]\n", fmtElapsed(s.at.Sub(r.start)), s.phase, orStart(prevStatus), st, rc)
			prevStatus = st
		}
	}
	b.WriteString("\ngauge transitions:\n")
	for _, s := range r.samples {
		if g := gaugeStr(s); g != prevGauge {
			fmt.Fprintf(&b, "  %s %-9s %s -> %s\n", fmtElapsed(s.at.Sub(r.start)), s.phase, orStart(prevGauge), g)
			prevGauge = g
		}
	}

	b.WriteString("\nsamples per phase and status:\n")
	var phases []string
	counts := map[string]map[string]int{}
	for _, s := range r.samples {
		if counts[s.phase] == nil {
			counts[s.phase] = map[string]int{}
			phases = append(phases, s.phase)
		}
		st := "ERR"
		if s.health != nil {
			st = s.health.Status
		}
		counts[s.phase][st]++
	}
	for _, p := range phases {
		fmt.Fprintf(&b, "  %-9s %s\n", p, fmtCounts(counts[p]))
	}

	r.reportSweeps(&b)
	r.reportHistogram(&b)
	r.t.Log(b.String())
}

// reportSweeps lists each distinct sweep the API exposed and per-node call durations by status.
// A node's own status says nothing about that node: a down node's own check passes.
func (r *runner) reportSweeps(b *bytes.Buffer) {
	b.WriteString("\nsafe-to-take-down sweeps seen by the API:\n")
	seen := map[int64]bool{}
	type agg struct {
		n        int
		sum, max int64
	}
	byStatus := map[string]*agg{}
	for _, s := range r.samples {
		if s.health == nil || s.health.NodesSafeToTakeDown == nil || s.health.NodesSafeToTakeDown.CheckedAt.IsZero() {
			continue
		}
		st := s.health.NodesSafeToTakeDown
		key := st.CheckedAt.UnixNano()
		if seen[key] {
			continue
		}
		seen[key] = true
		var nodes []string
		for _, n := range st.Nodes {
			nodes = append(nodes, fmt.Sprintf("%s=%s(%dms)", n.NodeName, n.Status, n.DurationMs))
			if n.Status == "" || n.Status == "NOT_CHECKED" || n.Status == "NOT_ALIVE" || n.Status == "UNRESOLVED" {
				continue
			}
			a := byStatus[n.Status]
			if a == nil {
				a = &agg{}
				byStatus[n.Status] = a
			}
			a.n++
			a.sum += n.DurationMs
			a.max = max(a.max, n.DurationMs)
		}
		reasonCode := st.ReasonCode
		if reasonCode == "" {
			reasonCode = "-"
		}
		fmt.Fprintf(b, "  checked %s %s reason=%s total=%dms lag_bound=%dms: %s\n",
			fmtElapsed(st.CheckedAt.Sub(r.start)), st.Result, reasonCode, st.DurationMs, st.FollowerLagBoundMs, strings.Join(nodes, " "))
	}
	b.WriteString("\nper-node areNodesSafeToTakeDown durations from the API (distinct sweeps):\n")
	var statuses []string
	for k := range byStatus {
		statuses = append(statuses, k)
	}
	sort.Strings(statuses)
	for _, k := range statuses {
		a := byStatus[k]
		fmt.Fprintf(b, "  %-11s n=%d mean=%dms max=%dms\n", k, a.n, a.sum/int64(a.n), a.max)
	}
}

func (r *runner) reportHistogram(b *bytes.Buffer) {
	first, last := r.firstMetrics(), r.lastMetrics()
	b.WriteString("\n" + histMetric + " over the run (first -> last scrape):\n")
	if first == nil || last == nil {
		b.WriteString("  no metrics scraped\n")
		return
	}
	keys := map[histKey]bool{}
	for k := range last.count {
		keys[k] = true
	}
	for k := range first.count {
		keys[k] = true
	}
	var sorted []histKey
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool {
		a, c := sorted[i], sorted[j]
		return a.caller+a.result+a.target < c.caller+c.result+c.target
	})
	for _, k := range sorted {
		dn := last.count[k] - first.count[k]
		ds := last.sum[k] - first.sum[k]
		line := fmt.Sprintf("  caller=%s result=%s target=%s: +%v", k.caller, k.result, k.target, dn)
		switch {
		case dn < 0:
			line += " (counter reset: YBA restarted?)"
		case dn > 0:
			line += " mean=" + fmtSecs(ds/dn)
		}
		b.WriteString(line + "\n")
	}
	for _, caller := range []string{callerCH, callerTask} {
		if bound := maxBound(first, last, caller); bound != "" {
			fmt.Fprintf(b, "  caller=%s: slowest call %s (bucket bound)\n", caller, bound)
		}
	}
}

// maxBound bounds the slowest call between the two scrapes by the smallest bucket that holds
// every new observation.
func maxBound(first, last *metricsSnapshot, caller string) string {
	n1, _ := last.total(caller, "")
	n0, _ := first.total(caller, "")
	dn := n1 - n0
	if dn <= 0 {
		return ""
	}
	var les []float64
	for le := range last.buckets[caller] {
		les = append(les, le)
	}
	sort.Float64s(les)
	for i, le := range les {
		if last.buckets[caller][le]-first.buckets[caller][le] >= dn {
			if math.IsInf(le, 1) && i > 0 {
				return fmt.Sprintf("> %gs", les[i-1])
			}
			return fmt.Sprintf("<= %gs", le)
		}
	}
	return ""
}

func describeSample(s *sample) string {
	if s == nil {
		return "none"
	}
	out := "gauge=" + gaugeStr(s) + " "
	if s.health == nil {
		return out + "API error: " + errString(s.apiErr)
	}
	return out + describeHealth(s.health)
}

func describeHealth(h *clusterHealth) string {
	var b strings.Builder
	fmt.Fprintf(&b, "status=%s", h.Status)
	for _, rs := range h.Reasons {
		fmt.Fprintf(&b, "\n  reason %s: %s", rs.Code, snippet(rs.Message, 300))
	}
	if hc := h.HealthCheck; hc != nil {
		fmt.Fprintf(&b, "\n  health_check %s check_time=%s max_age=%dms has_error=%t has_warning=%t",
			hc.Result, fmtTime(hc.CheckTime), hc.MaxAgeMs, hc.HasError, hc.HasWarning)
		for i, fc := range hc.FailedChecks {
			if i == 5 {
				fmt.Fprintf(&b, "\n    ... %d more failed checks", len(hc.FailedChecks)-i)
				break
			}
			fmt.Fprintf(&b, "\n    failed %s/%s %q: %s", fc.NodeName, fc.Process, fc.CheckName, snippet(strings.Join(fc.Details, " / "), 200))
		}
	}
	if st := h.NodesSafeToTakeDown; st != nil {
		fmt.Fprintf(&b, "\n  nodes_safe_to_take_down %s reason=%s checked_at=%s duration=%dms lag_bound=%dms nodes=%s",
			st.Result, st.ReasonCode, fmtTime(st.CheckedAt), st.DurationMs, st.FollowerLagBoundMs, nodesSummary(st))
		for _, n := range st.Nodes {
			if n.Status != "SAFE" {
				fmt.Fprintf(&b, "\n    %s %s (%dms): %s", n.NodeName, n.Status, n.DurationMs, snippet(n.Message, 300))
			}
		}
	}
	return b.String()
}

func reasonCodes(h *clusterHealth) string {
	var codes []string
	for _, rs := range h.Reasons {
		codes = append(codes, rs.Code)
	}
	if len(codes) == 0 {
		return "-"
	}
	return strings.Join(codes, ",")
}

func nodesSummary(st *sttdInput) string {
	if st == nil || len(st.Nodes) == 0 {
		return "-"
	}
	counts := map[string]int{}
	for _, n := range st.Nodes {
		counts[n.Status]++
	}
	return fmtCounts(counts)
}

func fmtCounts(counts map[string]int) string {
	var keys []string
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, counts[k]))
	}
	return strings.Join(parts, ",")
}

func gaugeStr(s *sample) string {
	switch {
	case s == nil || s.metrics == nil:
		return "ERR"
	case s.metrics.gauge == nil:
		return "absent"
	}
	return strconv.FormatFloat(*s.metrics.gauge, 'g', -1, 64)
}

func histCell(cur, prev *metricsSnapshot, caller string) string {
	if cur == nil {
		return "ERR"
	}
	n, sum := cur.total(caller, "")
	cell := strconv.FormatFloat(n, 'f', -1, 64)
	if prev != nil {
		pn, ps := prev.total(caller, "")
		if dn := n - pn; dn > 0 {
			cell += fmt.Sprintf(" +%v@%s", dn, fmtSecs((sum-ps)/dn))
		}
	}
	return cell
}

func orStart(s string) string {
	if s == "" {
		return "(start)"
	}
	return s
}

func fmtElapsed(d time.Duration) string {
	sign := "+"
	if d < 0 {
		sign, d = "-", -d
	}
	d = d.Round(time.Second)
	return fmt.Sprintf("%s%02d:%02d", sign, int(d/time.Minute), int(d%time.Minute/time.Second))
}

func fmtAge(at time.Time, t flexTime) string {
	if t.IsZero() {
		return "-"
	}
	return at.Sub(t.Time).Round(time.Second).String()
}

func fmtTime(t flexTime) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func fmtSecs(sec float64) string {
	return time.Duration(sec * float64(time.Second)).Round(time.Millisecond).String()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func snippet(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// ---------------------------------------------------------------------------------------------
// YBA HTTP client

type ybaClient struct {
	base, token string
	http        *http.Client
}

type httpError struct {
	method, path string
	code         int
	body         string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.method, e.path, e.code, e.body)
}

func (c *ybaClient) do(ctx context.Context, method, path, contentType string, body []byte) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set(apiTokenHeader, c.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%s %s: read body: %w", method, path, err)
	}
	if resp.StatusCode/100 != 2 {
		return b, &httpError{method: method, path: path, code: resp.StatusCode, body: snippet(string(b), 300)}
	}
	return b, nil
}

func (c *ybaClient) getJSON(ctx context.Context, path string, out any) error {
	b, err := c.do(ctx, http.MethodGet, path, "", nil)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("GET %s: decode: %w (body: %s)", path, err, snippet(string(b), 200))
	}
	return nil
}

// ---------------------------------------------------------------------------------------------
// Wire types. Decoding is lenient: unknown fields are ignored, missing ones stay zero.

type clusterHealth struct {
	UniverseUUID        string     `json:"universe_uuid"`
	Status              string     `json:"status"`
	EvaluatedAt         flexTime   `json:"evaluated_at"`
	Reasons             []reason   `json:"reasons"`
	HealthCheck         *hcInput   `json:"health_check"`
	NodesSafeToTakeDown *sttdInput `json:"nodes_safe_to_take_down"`
}

type reason struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type hcInput struct {
	Result       string        `json:"result"`
	CheckTime    flexTime      `json:"check_time"`
	MaxAgeMs     int64         `json:"max_age_ms"`
	HasError     bool          `json:"has_error"`
	HasWarning   bool          `json:"has_warning"`
	FailedChecks []failedCheck `json:"failed_checks"`
}

type failedCheck struct {
	NodeName    string   `json:"node_name"`
	NodeAddress string   `json:"node_address"`
	Process     string   `json:"process"`
	CheckName   string   `json:"check_name"`
	Details     []string `json:"details"`
}

type sttdInput struct {
	Result             string       `json:"result"`
	ReasonCode         string       `json:"reason_code"`
	CheckedAt          flexTime     `json:"checked_at"`
	DurationMs         int64        `json:"duration_ms"`
	FollowerLagBoundMs int64        `json:"follower_lag_bound_ms"`
	Nodes              []nodeResult `json:"nodes"`
}

type nodeResult struct {
	NodeName   string `json:"node_name"`
	NodeUUID   string `json:"node_uuid"`
	IsMaster   bool   `json:"is_master"`
	IsTserver  bool   `json:"is_tserver"`
	Address    string `json:"address"`
	Status     string `json:"status"`
	Message    string `json:"message"`
	DurationMs int64  `json:"duration_ms"`
}

// flexTime accepts an ISO-8601 string, epoch milliseconds, Jackson's decimal epoch seconds for
// java.time types, or null.
type flexTime struct{ time.Time }

func (f *flexTime) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" || s == `""` {
		return nil
	}
	if strings.HasPrefix(s, `"`) {
		var str string
		if err := json.Unmarshal(b, &str); err != nil {
			return err
		}
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999Z0700"} {
			if t, err := time.Parse(layout, str); err == nil {
				f.Time = t
				return nil
			}
		}
		return fmt.Errorf("unrecognized time %q", str)
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("unrecognized time %s", s)
	}
	if v > 1e11 {
		f.Time = time.UnixMilli(int64(v))
	} else {
		f.Time = time.Unix(0, int64(v*1e9))
	}
	return nil
}

type universeResp struct {
	Name    string `json:"name"`
	Details struct {
		UpdateInProgress bool   `json:"updateInProgress"`
		UniversePaused   bool   `json:"universePaused"`
		UpdatingTask     string `json:"updatingTask"`
		Clusters         []struct {
			ClusterType string `json:"clusterType"`
			UserIntent  struct {
				ReplicationFactor int    `json:"replicationFactor"`
				YbSoftwareVersion string `json:"ybSoftwareVersion"`
				ProviderType      string `json:"providerType"`
			} `json:"userIntent"`
		} `json:"clusters"`
		Nodes []universeNode `json:"nodeDetailsSet"`
	} `json:"universeDetails"`
}

type universeNode struct {
	NodeName  string `json:"nodeName"`
	State     string `json:"state"`
	IsMaster  bool   `json:"isMaster"`
	IsTserver bool   `json:"isTserver"`
}

func (u *universeResp) summary() string {
	var parts []string
	for _, c := range u.Details.Clusters {
		parts = append(parts, fmt.Sprintf("%s %s RF%d %s", c.ClusterType, c.UserIntent.ProviderType,
			c.UserIntent.ReplicationFactor, c.UserIntent.YbSoftwareVersion))
	}
	nodes := append([]universeNode(nil), u.Details.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeName < nodes[j].NodeName })
	var ns []string
	for _, n := range nodes {
		role := ""
		if n.IsMaster {
			role += "M"
		}
		if n.IsTserver {
			role += "T"
		}
		ns = append(ns, fmt.Sprintf("%s=%s/%s", n.NodeName, n.State, role))
	}
	return fmt.Sprintf("%s; paused=%t updateInProgress=%t; nodes: %s",
		strings.Join(parts, ", "), u.Details.UniversePaused, u.Details.UpdateInProgress, strings.Join(ns, " "))
}

// ---------------------------------------------------------------------------------------------
// Prometheus text exposition

type histKey struct{ caller, result, target string }

type metricsSnapshot struct {
	gauge       *float64 // nil when the universe has no series
	gaugeSeries int
	count       map[histKey]float64
	sum         map[histKey]float64
	buckets     map[string]map[float64]float64 // caller -> le -> cumulative count over result/target
}

// total sums _count and _sum over the series matching caller and result ("" matches any).
func (m *metricsSnapshot) total(caller, result string) (n, sum float64) {
	for k, c := range m.count {
		if (caller == "" || k.caller == caller) && (result == "" || k.result == result) {
			n += c
			sum += m.sum[k]
		}
	}
	return n, sum
}

func parseMetrics(body []byte, universe string) *metricsSnapshot {
	m := &metricsSnapshot{
		count:   map[histKey]float64{},
		sum:     map[histKey]float64{},
		buckets: map[string]map[float64]float64{},
	}
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, gaugeMetric) && !strings.HasPrefix(line, histMetric) {
			continue
		}
		name, labels, v, ok := parsePromLine(line)
		if !ok {
			continue
		}
		k := histKey{caller: labels["caller"], result: labels["result"], target: labels["target"]}
		switch name {
		case gaugeMetric:
			if labels["universe_uuid"] == universe {
				m.gaugeSeries++
				if m.gauge == nil {
					m.gauge = &v
				}
			}
		case histMetric + "_count":
			m.count[k] += v
		case histMetric + "_sum":
			m.sum[k] += v
		case histMetric + "_bucket":
			le, err := strconv.ParseFloat(labels["le"], 64)
			if err != nil {
				continue
			}
			if m.buckets[k.caller] == nil {
				m.buckets[k.caller] = map[float64]float64{}
			}
			m.buckets[k.caller][le] += v
		}
	}
	return m
}

// parsePromLine parses `name{l="v",...} value [timestamp]`, unescaping \\, \" and \n in values.
func parsePromLine(line string) (name string, labels map[string]string, value float64, ok bool) {
	i := strings.IndexAny(line, "{ \t")
	if i <= 0 {
		return "", nil, 0, false
	}
	name, rest := line[:i], line[i:]
	labels = map[string]string{}
	if rest[0] == '{' {
		j := 1
		for {
			for j < len(rest) && (rest[j] == ',' || rest[j] == ' ') {
				j++
			}
			if j >= len(rest) {
				return "", nil, 0, false
			}
			if rest[j] == '}' {
				j++
				break
			}
			eq := strings.IndexByte(rest[j:], '=')
			if eq < 0 {
				return "", nil, 0, false
			}
			key := strings.TrimSpace(rest[j : j+eq])
			j += eq + 1
			if j >= len(rest) || rest[j] != '"' {
				return "", nil, 0, false
			}
			j++
			var sb strings.Builder
			for j < len(rest) && rest[j] != '"' {
				if rest[j] == '\\' && j+1 < len(rest) {
					j++
					if rest[j] == 'n' {
						sb.WriteByte('\n')
					} else {
						sb.WriteByte(rest[j])
					}
				} else {
					sb.WriteByte(rest[j])
				}
				j++
			}
			if j >= len(rest) {
				return "", nil, 0, false
			}
			j++
			labels[key] = sb.String()
		}
		rest = rest[j:]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", nil, 0, false
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return "", nil, 0, false
	}
	return name, labels, v, true
}
