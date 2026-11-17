// Copyright (c) YugabyteDB, Inc.

package com.yugabyte.yw.commissioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.yugabyte.yw.commissioner.ClusterHealth.FailedCheck;
import com.yugabyte.yw.commissioner.ClusterHealth.HealthCheckSnapshot;
import com.yugabyte.yw.commissioner.ClusterHealth.InputResult;
import com.yugabyte.yw.commissioner.ClusterHealth.Reason;
import com.yugabyte.yw.commissioner.ClusterHealth.Status;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepOutcome;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepResult;
import com.yugabyte.yw.commissioner.ClusterHealthRules.Facts;
import com.yugabyte.yw.commissioner.ClusterHealthRules.Settings;
import com.yugabyte.yw.models.HealthCheck;
import com.yugabyte.yw.models.helpers.NodeDetails;
import com.yugabyte.yw.models.helpers.NodeDetails.NodeState;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class ClusterHealthRulesTest {

  private static final Instant NOW = Instant.parse("2026-11-13T12:00:00Z");
  private static final UUID UNIVERSE = UUID.randomUUID();
  private static final Duration MAX_AGE = Duration.ofMinutes(15);
  private static final Duration STTD_INTERVAL = Duration.ofMinutes(15);
  private static final Duration EVAL_INTERVAL = Duration.ofMinutes(5);

  /** Defaults to a created, idle RF3 universe on a supported version with every node Live. */
  private static class FactsBuilder {
    boolean follower;
    boolean hasDetails = true;
    boolean created = true;
    boolean paused;
    boolean busy;
    String updatingTask;
    int rf = 3;
    boolean apiSupported = true;
    List<String> notLive = List.of();
    Instant blockedAt;

    Facts build() {
      return new Facts(
          UNIVERSE,
          follower,
          hasDetails,
          created,
          paused,
          busy,
          updatingTask,
          rf,
          apiSupported,
          notLive,
          blockedAt);
    }
  }

  private static Settings settings() {
    return settings(true, true);
  }

  private static Settings settings(boolean enabled, boolean sttdEnabled) {
    return new Settings(enabled, EVAL_INTERVAL, sttdEnabled, STTD_INTERVAL, MAX_AGE);
  }

  private static HealthCheckSnapshot passedHc() {
    return hc(Duration.ofMinutes(1), false, false);
  }

  private static HealthCheckSnapshot failedHc() {
    return hc(Duration.ofMinutes(1), true, false);
  }

  private static HealthCheckSnapshot hc(Duration age, boolean hasError, boolean hasWarning) {
    List<FailedCheck> failed =
        hasError
            ? List.of(new FailedCheck("n1", "10.0.0.1", "tserver", "Fatal log files", List.of("x")))
            : List.of();
    return new HealthCheckSnapshot(NOW.minus(age), hasError, hasWarning, failed);
  }

  private static SweepResult sweep(SweepOutcome outcome) {
    return sweep(outcome, Duration.ofMinutes(1));
  }

  private static SweepResult sweep(SweepOutcome outcome, Duration age) {
    Instant finished = NOW.minus(age);
    return new SweepResult(
        finished.minusMillis(50), finished, 50, outcome, "sweep says " + outcome, 60000, List.of());
  }

  private static ClusterHealth compose(
      FactsBuilder f, HealthCheckSnapshot hc, SweepResult s, Settings cfg) {
    return ClusterHealthRules.compose(f.build(), hc, s, cfg, NOW);
  }

  private static ClusterHealth compose(HealthCheckSnapshot hc, SweepResult s) {
    return compose(new FactsBuilder(), hc, s, settings());
  }

  /** {@code metric} null means no series. */
  private static void assertHealth(
      ClusterHealth h, Status status, Double metric, String... reasonCodes) {
    assertEquals(status, h.status());
    assertEquals(
        List.of(reasonCodes), h.reasons().stream().map(Reason::code).collect(Collectors.toList()));
    OptionalDouble value = ClusterHealthRules.metricValue(h);
    if (metric == null) {
      assertTrue("expected no series but got " + value, value.isEmpty());
    } else {
      assertEquals(metric, value.getAsDouble(), 0);
    }
    assertEquals(UNIVERSE, h.universeUuid());
    assertEquals(NOW, h.evaluatedAt());
  }

  private static NodeDetails node(String name, NodeState state) {
    NodeDetails node = new NodeDetails();
    node.nodeName = name;
    node.state = state;
    return node;
  }

  // Gates.

  @Test
  public void testFollowerGateWinsOverEverything() {
    FactsBuilder f = new FactsBuilder();
    f.follower = true;
    f.paused = true;
    ClusterHealth h = compose(f, failedHc(), sweep(SweepOutcome.UNSAFE), settings(false, true));
    assertHealth(h, Status.UNKNOWN, null, Reason.YBA_STANDBY);
    assertEquals(InputResult.UNKNOWN, h.healthCheck().result());
    assertEquals(InputResult.UNKNOWN, h.safeToTakeDown().result());
  }

  @Test
  public void testDisabledGate() {
    ClusterHealth h =
        compose(new FactsBuilder(), failedHc(), sweep(SweepOutcome.SAFE), settings(false, true));
    assertHealth(h, Status.UNKNOWN, null, Reason.DISABLED);
  }

  @Test
  public void testNotCreatedGate() {
    FactsBuilder noDetails = new FactsBuilder();
    noDetails.hasDetails = false;
    assertHealth(
        compose(noDetails, passedHc(), null, settings()), Status.UNKNOWN, null, Reason.NOT_CREATED);
    FactsBuilder failedCreate = new FactsBuilder();
    failedCreate.created = false;
    assertHealth(
        compose(failedCreate, failedHc(), null, settings()),
        Status.UNKNOWN,
        null,
        Reason.NOT_CREATED);
  }

  @Test
  public void testPausedGate() {
    FactsBuilder f = new FactsBuilder();
    f.paused = true;
    assertHealth(
        compose(f, failedHc(), null, settings()), Status.UNKNOWN, null, Reason.UNIVERSE_PAUSED);
  }

  @Test
  public void testBusyGate() {
    FactsBuilder f = new FactsBuilder();
    f.busy = true;
    f.updatingTask = "SoftwareUpgrade";
    ClusterHealth h = compose(f, failedHc(), sweep(SweepOutcome.UNSAFE), settings());
    assertHealth(h, Status.UNKNOWN, null, Reason.TASK_IN_PROGRESS);
    assertTrue(h.reasons().get(0).message().contains("SoftwareUpgrade"));
  }

  // Health check input.

  @Test
  public void testHealthCheckMissingIsWarmUp() {
    ClusterHealth h = compose(null, sweep(SweepOutcome.SAFE));
    assertHealth(h, Status.UNKNOWN, null, Reason.HEALTH_CHECK_MISSING);
    assertEquals(InputResult.UNKNOWN, h.healthCheck().result());
    assertEquals(InputResult.PASSED, h.safeToTakeDown().result());
  }

  @Test
  public void testHealthCheckStaleness() {
    ClusterHealth stale =
        compose(hc(MAX_AGE.plusSeconds(1), false, false), sweep(SweepOutcome.SAFE));
    assertHealth(stale, Status.UNKNOWN, 0.0, Reason.HEALTH_CHECK_STALE);

    ClusterHealth fresh =
        compose(hc(MAX_AGE.minusSeconds(1), false, false), sweep(SweepOutcome.SAFE));
    assertHealth(fresh, Status.HEALTHY, 1.0);
    assertEquals(InputResult.PASSED, fresh.healthCheck().result());
    assertEquals(MAX_AGE.toMillis(), fresh.healthCheck().maxAgeMs());
  }

  @Test
  public void testHealthCheckFromBeforeABlockIsPendingForMaxAge() {
    // A 30-minute task ended a minute ago; HealthChecker stored nothing while it ran.
    FactsBuilder f = new FactsBuilder();
    f.blockedAt = NOW.minus(Duration.ofMinutes(1));
    HealthCheckSnapshot old = hc(Duration.ofMinutes(31), false, false);
    ClusterHealth pending = compose(f, old, sweep(SweepOutcome.SAFE), settings());
    assertHealth(pending, Status.UNKNOWN, null, Reason.HEALTH_CHECK_PENDING);
    assertEquals(InputResult.UNKNOWN, pending.healthCheck().result());
    assertSame(old, pending.healthCheck().snapshot());

    // Pending errors from before the task are not a verdict, but a sweep failure still is.
    assertHealth(
        compose(f, hc(Duration.ofMinutes(31), true, false), sweep(SweepOutcome.SAFE), settings()),
        Status.UNKNOWN,
        null,
        Reason.HEALTH_CHECK_PENDING);
    assertHealth(
        compose(f, old, sweep(SweepOutcome.UNSAFE), settings()),
        Status.UNHEALTHY,
        0.0,
        Reason.HEALTH_CHECK_PENDING,
        Reason.NODES_NOT_SAFE_TO_TAKE_DOWN);

    // HealthChecker had a full max age since the block to store a result.
    f.blockedAt = NOW.minus(MAX_AGE).minusSeconds(1);
    assertHealth(
        compose(f, old, sweep(SweepOutcome.SAFE), settings()),
        Status.UNKNOWN,
        0.0,
        Reason.HEALTH_CHECK_STALE);

    // A result stored after the block counts as usual.
    f.blockedAt = NOW.minus(Duration.ofMinutes(5));
    assertHealth(compose(f, passedHc(), sweep(SweepOutcome.SAFE), settings()), Status.HEALTHY, 1.0);
  }

  @Test
  public void testCustomerStoreIntervalStretchesMaxAge() {
    Duration maxAge =
        ClusterHealthRules.effectiveHealthCheckMaxAge(
            Duration.ofMinutes(15),
            Duration.ofMinutes(5).toMillis(),
            Duration.ofHours(1).toMillis());
    assertEquals(Duration.ofHours(3), maxAge);
    // The configured value wins when it is larger.
    assertEquals(
        Duration.ofHours(1),
        ClusterHealthRules.effectiveHealthCheckMaxAge(Duration.ofHours(1), 300000, 300000));

    Settings cfg = new Settings(true, EVAL_INTERVAL, true, STTD_INTERVAL, maxAge);
    ClusterHealth h =
        compose(
            new FactsBuilder(),
            hc(Duration.ofHours(2), false, false),
            sweep(SweepOutcome.SAFE),
            cfg);
    assertHealth(h, Status.HEALTHY, 1.0);
  }

  @Test
  public void testHealthCheckErrorsAreUnhealthy() {
    ClusterHealth h = compose(failedHc(), sweep(SweepOutcome.SAFE));
    assertHealth(h, Status.UNHEALTHY, 0.0, Reason.HEALTH_CHECK_ERRORS);
    assertEquals(InputResult.FAILED, h.healthCheck().result());
    assertEquals(1, h.healthCheck().snapshot().failedChecks().size());
    assertTrue(h.reasons().get(0).message().contains("n1: Fatal log files"));
  }

  @Test
  public void testWarningsAloneAreHealthy() {
    ClusterHealth h = compose(hc(Duration.ofMinutes(1), false, true), sweep(SweepOutcome.SAFE));
    assertHealth(h, Status.HEALTHY, 1.0);
    assertTrue(h.healthCheck().snapshot().hasWarning());
  }

  @Test
  public void testSnapshotFromDetails() {
    HealthCheck.Details details = new HealthCheck.Details();
    details.setHasError(null);
    details.setHasWarning(null);
    HealthCheckSnapshot snapshot = ClusterHealthRules.toSnapshot(NOW, details);
    assertFalse(snapshot.hasError());
    assertFalse(snapshot.hasWarning());
    assertTrue(snapshot.failedChecks().isEmpty());
    assertFalse(ClusterHealthRules.toSnapshot(NOW, null).hasError());

    List<HealthCheck.Details.NodeData> data = new ArrayList<>();
    data.add(new HealthCheck.Details.NodeData().setNodeName("ok").setHasError(false));
    data.add(new HealthCheck.Details.NodeData().setNodeName("boxed").setHasError(null));
    for (int i = 0; i < ClusterHealthRules.MAX_FAILED_CHECKS + 5; i++) {
      data.add(
          new HealthCheck.Details.NodeData()
              .setNodeName("n" + i)
              .setNode("10.0.0." + i)
              .setProcess("tserver")
              .setMessage("Disk utilization")
              .setHasError(true)
              .setDetails(
                  IntStream.range(0, 10).mapToObj(j -> "line " + j).collect(Collectors.toList())));
    }
    details.setHasError(true).setData(data);
    snapshot = ClusterHealthRules.toSnapshot(NOW, details);
    assertTrue(snapshot.hasError());
    assertSame(NOW, snapshot.checkTime());
    assertEquals(ClusterHealthRules.MAX_FAILED_CHECKS, snapshot.failedChecks().size());
    FailedCheck first = snapshot.failedChecks().get(0);
    assertEquals("n0", first.nodeName());
    assertEquals("10.0.0.0", first.nodeAddress());
    assertEquals("tserver", first.process());
    assertEquals("Disk utilization", first.checkName());
    assertEquals(ClusterHealthRules.MAX_FAILED_CHECK_DETAILS, first.details().size());
  }

  // Nodes safe to take down input.

  @Test
  public void testNotLiveNodeFailsEvenWithFreshSafeSweep() {
    FactsBuilder f = new FactsBuilder();
    f.notLive = List.of("n2=Stopped");
    ClusterHealth h = compose(f, passedHc(), sweep(SweepOutcome.SAFE), settings());
    assertHealth(h, Status.UNHEALTHY, 0.0, Reason.NODE_NOT_LIVE);
    assertEquals(InputResult.FAILED, h.safeToTakeDown().result());
    assertTrue(h.safeToTakeDown().reason().message().contains("n2=Stopped"));
  }

  @Test
  public void testNotLiveNodesIgnoreRemovedStates() {
    List<NodeDetails> nodes = new ArrayList<>();
    nodes.add(node("n1", NodeState.Live));
    nodes.add(node("n3", NodeState.Stopped));
    nodes.add(node("n2", NodeState.Unreachable));
    for (NodeState state : ClusterHealthRules.REMOVED_STATES) {
      nodes.add(node("removed-" + state, state));
    }
    assertEquals(List.of("n2=Unreachable", "n3=Stopped"), ClusterHealthRules.notLiveNodes(nodes));
    assertEquals(List.of(), ClusterHealthRules.notLiveNodes(null));
    assertEquals(List.of(), ClusterHealthRules.notLiveNodes(Collections.emptyList()));
  }

  @Test
  public void testRf1FallsBackToHealthCheck() {
    FactsBuilder f = new FactsBuilder();
    f.rf = 1;
    ClusterHealth h = compose(f, passedHc(), null, settings());
    assertHealth(h, Status.HEALTHY, 1.0, Reason.RF1);
    assertEquals(InputResult.NOT_APPLICABLE, h.safeToTakeDown().result());

    // A new RF1 universe without a health check yet is warm-up, not 0.
    assertHealth(
        compose(f, null, null, settings()),
        Status.UNKNOWN,
        null,
        Reason.HEALTH_CHECK_MISSING,
        Reason.RF1);
  }

  @Test
  public void testNotApplicableWithFailedHealthCheckIsUnhealthy() {
    FactsBuilder unsupported = new FactsBuilder();
    unsupported.apiSupported = false;
    assertHealth(
        compose(unsupported, failedHc(), null, settings()),
        Status.UNHEALTHY,
        0.0,
        Reason.HEALTH_CHECK_ERRORS,
        Reason.DB_VERSION_UNSUPPORTED);
    assertHealth(
        compose(unsupported, passedHc(), null, settings()),
        Status.HEALTHY,
        1.0,
        Reason.DB_VERSION_UNSUPPORTED);

    ClusterHealth unsupportedOutcome = compose(failedHc(), sweep(SweepOutcome.UNSUPPORTED));
    assertHealth(
        unsupportedOutcome,
        Status.UNHEALTHY,
        0.0,
        Reason.HEALTH_CHECK_ERRORS,
        Reason.DB_VERSION_UNSUPPORTED);
    assertEquals(InputResult.NOT_APPLICABLE, unsupportedOutcome.safeToTakeDown().result());

    assertHealth(
        compose(new FactsBuilder(), failedHc(), null, settings(true, false)),
        Status.UNHEALTHY,
        0.0,
        Reason.HEALTH_CHECK_ERRORS,
        Reason.SAFE_TO_TAKE_DOWN_DISABLED);
  }

  @Test
  public void testNoSweepYet() {
    ClusterHealth warmUp = compose(passedHc(), null);
    assertHealth(warmUp, Status.UNKNOWN, null, Reason.NOT_YET_EVALUATED);
    assertNull(warmUp.safeToTakeDown().lastSweep());

    assertHealth(
        compose(failedHc(), null),
        Status.UNHEALTHY,
        0.0,
        Reason.HEALTH_CHECK_ERRORS,
        Reason.NOT_YET_EVALUATED);

    // Stale data is not warm-up.
    assertHealth(
        compose(hc(MAX_AGE.plusMinutes(1), false, false), null),
        Status.UNKNOWN,
        0.0,
        Reason.HEALTH_CHECK_STALE,
        Reason.NOT_YET_EVALUATED);
  }

  @Test
  public void testStaleSweep() {
    ClusterHealth h =
        compose(passedHc(), sweep(SweepOutcome.SAFE, STTD_INTERVAL.multipliedBy(3).plusSeconds(1)));
    assertHealth(h, Status.UNKNOWN, 0.0, Reason.SAFE_TO_TAKE_DOWN_STALE);
    assertHealth(
        compose(passedHc(), sweep(SweepOutcome.SAFE, STTD_INTERVAL.multipliedBy(3))),
        Status.HEALTHY,
        1.0);

    // Sweeps run only during evaluations, so a longer evaluation interval stretches the limit.
    Duration evalInterval = Duration.ofHours(1);
    Settings slowEval = new Settings(true, evalInterval, true, Duration.ofMinutes(1), MAX_AGE);
    assertHealth(
        compose(
            new FactsBuilder(),
            passedHc(),
            sweep(SweepOutcome.SAFE, evalInterval.multipliedBy(3)),
            slowEval),
        Status.HEALTHY,
        1.0);
    assertHealth(
        compose(
            new FactsBuilder(),
            passedHc(),
            sweep(SweepOutcome.SAFE, evalInterval.multipliedBy(3).plusSeconds(1)),
            slowEval),
        Status.UNKNOWN,
        0.0,
        Reason.SAFE_TO_TAKE_DOWN_STALE);
  }

  @Test
  public void testFailedSweepOutcomes() {
    ClusterHealth notAlive = compose(passedHc(), sweep(SweepOutcome.NOT_ALIVE));
    assertHealth(notAlive, Status.UNHEALTHY, 0.0, Reason.NODE_NOT_ALIVE);
    assertEquals("sweep says NOT_ALIVE", notAlive.safeToTakeDown().reason().message());

    ClusterHealth unsafe = compose(passedHc(), sweep(SweepOutcome.UNSAFE));
    assertHealth(unsafe, Status.UNHEALTHY, 0.0, Reason.NODES_NOT_SAFE_TO_TAKE_DOWN);
    assertEquals(InputResult.FAILED, unsafe.safeToTakeDown().result());
    assertEquals("sweep says UNSAFE", unsafe.safeToTakeDown().reason().message());
  }

  @Test
  public void testUnknownSweepOutcomes() {
    assertHealth(
        compose(passedHc(), sweep(SweepOutcome.UNRESOLVED)),
        Status.UNKNOWN,
        0.0,
        Reason.NODE_NOT_REGISTERED);
    ClusterHealth error = compose(passedHc(), sweep(SweepOutcome.ERROR));
    assertHealth(error, Status.UNKNOWN, 0.0, Reason.CHECK_ERROR);
    assertEquals(InputResult.UNKNOWN, error.safeToTakeDown().result());
  }

  @Test
  public void testSafeSweepAndPassedHealthCheckIsHealthy() {
    SweepResult s = sweep(SweepOutcome.SAFE);
    ClusterHealth h = compose(passedHc(), s);
    assertHealth(h, Status.HEALTHY, 1.0);
    assertEquals(InputResult.PASSED, h.healthCheck().result());
    assertEquals(InputResult.PASSED, h.safeToTakeDown().result());
    assertNull(h.safeToTakeDown().reason());
    assertSame(s, h.safeToTakeDown().lastSweep());
  }

  @Test
  public void testKnownFailureOutranksMissingData() {
    ClusterHealth h = compose(null, sweep(SweepOutcome.UNSAFE));
    assertHealth(
        h, Status.UNHEALTHY, 0.0, Reason.HEALTH_CHECK_MISSING, Reason.NODES_NOT_SAFE_TO_TAKE_DOWN);
  }

  @Test
  public void testSafeToTakeDownApplicability() {
    assertTrue(
        ClusterHealthRules.isSafeToTakeDownApplicable(new FactsBuilder().build(), settings()));
    FactsBuilder rf1 = new FactsBuilder();
    rf1.rf = 1;
    assertFalse(ClusterHealthRules.isSafeToTakeDownApplicable(rf1.build(), settings()));
    FactsBuilder unsupported = new FactsBuilder();
    unsupported.apiSupported = false;
    assertFalse(ClusterHealthRules.isSafeToTakeDownApplicable(unsupported.build(), settings()));
    assertFalse(
        ClusterHealthRules.isSafeToTakeDownApplicable(
            new FactsBuilder().build(), settings(true, false)));
  }
}
