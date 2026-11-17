// Copyright (c) YugabyteDB, Inc.

package com.yugabyte.yw.commissioner;

import com.yugabyte.yw.commissioner.ClusterHealth.FailedCheck;
import com.yugabyte.yw.commissioner.ClusterHealth.HealthCheckInput;
import com.yugabyte.yw.commissioner.ClusterHealth.HealthCheckSnapshot;
import com.yugabyte.yw.commissioner.ClusterHealth.InputResult;
import com.yugabyte.yw.commissioner.ClusterHealth.Reason;
import com.yugabyte.yw.commissioner.ClusterHealth.SafeToTakeDownInput;
import com.yugabyte.yw.commissioner.ClusterHealth.Status;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepOutcome;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepResult;
import com.yugabyte.yw.models.HealthCheck;
import com.yugabyte.yw.models.helpers.NodeDetails;
import com.yugabyte.yw.models.helpers.NodeDetails.NodeState;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * The cluster health policy. Everything here is a pure function of its arguments, so the checker's
 * gauge and the API read the same verdict.
 */
public final class ClusterHealthRules {

  static final int MAX_FAILED_CHECKS = 50;
  static final int MAX_FAILED_CHECK_DETAILS = 5;
  private static final int MAX_FAILED_CHECKS_IN_MESSAGE = 5;

  /** A sweep older than this many intervals no longer counts. */
  static final int SWEEP_STALE_INTERVALS = 3;

  /** A stored health check counts for this many health check periods. */
  static final int HEALTH_CHECK_STALE_PERIODS = 3;

  /** Node states that mean the node is on its way out of the universe, not down. */
  static final Set<NodeState> REMOVED_STATES =
      EnumSet.of(
          NodeState.Removed,
          NodeState.Decommissioned,
          NodeState.BeingDecommissioned,
          NodeState.Terminating,
          NodeState.Terminated);

  private static final Set<String> GATE_CODES =
      Set.of(
          Reason.YBA_STANDBY,
          Reason.DISABLED,
          Reason.NOT_CREATED,
          Reason.UNIVERSE_PAUSED,
          Reason.TASK_IN_PROGRESS);

  // Data that is missing only because nothing has run yet: a new universe, a YBA restart, or the
  // end of a task or pause, during which neither checker runs.
  private static final Set<String> WARM_UP_CODES =
      Set.of(Reason.HEALTH_CHECK_MISSING, Reason.HEALTH_CHECK_PENDING, Reason.NOT_YET_EVALUATED);

  // Reasons of a NOT_APPLICABLE input. They explain a verdict, they never cause one.
  private static final Set<String> NOT_APPLICABLE_CODES =
      Set.of(Reason.RF1, Reason.DB_VERSION_UNSUPPORTED, Reason.SAFE_TO_TAKE_DOWN_DISABLED);

  /**
   * What compose needs to know about the universe.
   *
   * @param notLiveNodes primary-cluster nodes that are neither Live nor being removed, as
   *     "name=state". Taken before any process filter: stopping a node through YBA clears its
   *     isMaster and isTserver flags.
   * @param healthCheckBlockedAt the last time the checker saw the universe in a state in which
   *     HealthChecker stores nothing (busy, paused, not created) or first evaluated it after YBA
   *     started; null if never. A health check stored before it is pending, not stale, for the
   *     maximum age after it, so the end of every long task does not publish 0.
   */
  public record Facts(
      UUID universeUuid,
      boolean isFollower,
      boolean hasDetails,
      boolean creationSucceeded,
      boolean paused,
      boolean busyByTask,
      String updatingTask,
      int primaryRf,
      boolean apiSupported,
      List<String> notLiveNodes,
      @Nullable Instant healthCheckBlockedAt) {}

  /**
   * @param evaluationInterval a sweep runs only during an evaluation, so its staleness is measured
   *     in whichever of this and sttdInterval is longer
   * @param healthCheckMaxAge the effective maximum age, see {@link #effectiveHealthCheckMaxAge}
   */
  public record Settings(
      boolean enabled,
      Duration evaluationInterval,
      boolean sttdEnabled,
      Duration sttdInterval,
      Duration healthCheckMaxAge) {}

  private ClusterHealthRules() {}

  public static ClusterHealth compose(
      Facts f,
      @Nullable HealthCheckSnapshot hc,
      @Nullable SweepResult s,
      Settings cfg,
      Instant now) {
    long maxAgeMs = cfg.healthCheckMaxAge().toMillis();
    Reason gate = gate(f, cfg);
    if (gate != null) {
      return new ClusterHealth(
          f.universeUuid(),
          Status.UNKNOWN,
          now,
          List.of(gate),
          new HealthCheckInput(InputResult.UNKNOWN, List.of(), hc, maxAgeMs),
          new SafeToTakeDownInput(InputResult.UNKNOWN, null, s));
    }
    HealthCheckInput hcInput = healthCheckInput(hc, f.healthCheckBlockedAt(), cfg, now);
    SafeToTakeDownInput sttdInput = safeToTakeDownInput(f, s, cfg, now);

    Status status;
    if (hcInput.result() == InputResult.FAILED || sttdInput.result() == InputResult.FAILED) {
      // A known failure outranks missing data.
      status = Status.UNHEALTHY;
    } else if (hcInput.result() == InputResult.PASSED
        && (sttdInput.result() == InputResult.PASSED
            || sttdInput.result() == InputResult.NOT_APPLICABLE)) {
      status = Status.HEALTHY;
    } else {
      status = Status.UNKNOWN;
    }
    List<Reason> reasons = new ArrayList<>(hcInput.reasons());
    if (sttdInput.reason() != null) {
      reasons.add(sttdInput.reason());
    }
    return new ClusterHealth(
        f.universeUuid(), status, now, List.copyOf(reasons), hcInput, sttdInput);
  }

  /**
   * The gauge value: 1 for HEALTHY, 0 for UNHEALTHY or for an UNKNOWN that is not just a gate or
   * warm-up. Empty means the series must not exist, so that tasks, pauses and restarts don't page.
   */
  public static OptionalDouble metricValue(ClusterHealth h) {
    switch (h.status()) {
      case HEALTHY:
        return OptionalDouble.of(1);
      case UNHEALTHY:
        return OptionalDouble.of(0);
      default:
        boolean gateOrWarmUp =
            h.reasons().stream()
                .map(Reason::code)
                .filter(code -> !NOT_APPLICABLE_CODES.contains(code))
                .allMatch(code -> GATE_CODES.contains(code) || WARM_UP_CODES.contains(code));
        return gateOrWarmUp ? OptionalDouble.empty() : OptionalDouble.of(0);
    }
  }

  /** The reason the universe is not evaluated at all, or null. */
  @Nullable
  public static Reason gate(Facts f, Settings cfg) {
    if (f.isFollower()) {
      return new Reason(
          Reason.YBA_STANDBY,
          "This YBA is an HA standby; cluster health is evaluated on the active YBA");
    }
    if (!cfg.enabled()) {
      return new Reason(Reason.DISABLED, "Cluster health is disabled for this universe");
    }
    if (!f.hasDetails() || !f.creationSucceeded()) {
      return new Reason(Reason.NOT_CREATED, "The universe has not been created successfully");
    }
    if (f.paused()) {
      return new Reason(Reason.UNIVERSE_PAUSED, "The universe is paused");
    }
    if (f.busyByTask()) {
      return new Reason(Reason.TASK_IN_PROGRESS, "Task in progress: " + f.updatingTask());
    }
    return null;
  }

  /** Whether the sweep can mean anything for this universe, before looking at node states. */
  public static boolean isSafeToTakeDownApplicable(Facts f, Settings cfg) {
    return f.primaryRf() > 1 && f.apiSupported() && cfg.sttdEnabled();
  }

  /**
   * The health check results are persisted once per store interval (a customer's alerting config
   * can stretch it past the check interval), so the maximum age never goes below a few of those.
   */
  public static Duration effectiveHealthCheckMaxAge(
      Duration configured, long checkIntervalMs, long effectiveStoreIntervalMs) {
    Duration floor =
        Duration.ofMillis(
            HEALTH_CHECK_STALE_PERIODS * Math.max(checkIntervalMs, effectiveStoreIntervalMs));
    return configured.compareTo(floor) >= 0 ? configured : floor;
  }

  public static List<String> notLiveNodes(@Nullable Collection<NodeDetails> primaryNodes) {
    if (primaryNodes == null) {
      return List.of();
    }
    return primaryNodes.stream()
        .filter(n -> n.state != NodeState.Live && !REMOVED_STATES.contains(n.state))
        .sorted(Comparator.comparing(n -> n.nodeName, Comparator.nullsLast(String::compareTo)))
        .map(n -> n.nodeName + "=" + n.state)
        .collect(Collectors.toUnmodifiableList());
  }

  public static HealthCheckSnapshot toSnapshot(
      Instant checkTime, @Nullable HealthCheck.Details details) {
    if (details == null) {
      return new HealthCheckSnapshot(checkTime, false, false, List.of());
    }
    List<FailedCheck> failedChecks =
        details.getData() == null
            ? List.of()
            : details.getData().stream()
                .filter(Objects::nonNull)
                .filter(nd -> Boolean.TRUE.equals(nd.getHasError()))
                .limit(MAX_FAILED_CHECKS)
                .map(
                    nd ->
                        new FailedCheck(
                            nd.getNodeName(),
                            nd.getNode(),
                            nd.getProcess(),
                            nd.getMessage(),
                            nd.getDetails() == null
                                ? List.of()
                                : nd.getDetails().stream()
                                    .limit(MAX_FAILED_CHECK_DETAILS)
                                    .toList()))
                .toList();
    return new HealthCheckSnapshot(
        checkTime,
        Boolean.TRUE.equals(details.getHasError()),
        Boolean.TRUE.equals(details.getHasWarning()),
        failedChecks);
  }

  private static HealthCheckInput healthCheckInput(
      @Nullable HealthCheckSnapshot hc, @Nullable Instant blockedAt, Settings cfg, Instant now) {
    long maxAgeMs = cfg.healthCheckMaxAge().toMillis();
    if (hc == null) {
      return new HealthCheckInput(
          InputResult.UNKNOWN,
          List.of(
              new Reason(
                  Reason.HEALTH_CHECK_MISSING,
                  "No health check result is stored for the universe")),
          null,
          maxAgeMs);
    }
    if (now.isAfter(hc.checkTime().plus(cfg.healthCheckMaxAge()))) {
      if (blockedAt != null && !now.isAfter(blockedAt.plus(cfg.healthCheckMaxAge()))) {
        return new HealthCheckInput(
            InputResult.UNKNOWN,
            List.of(
                new Reason(
                    Reason.HEALTH_CHECK_PENDING,
                    "The latest health check was stored at "
                        + hc.checkTime()
                        + ", before a task, a pause or a YBA restart during which none was stored;"
                        + " waiting for the next one")),
            hc,
            maxAgeMs);
      }
      return new HealthCheckInput(
          InputResult.UNKNOWN,
          List.of(
              new Reason(
                  Reason.HEALTH_CHECK_STALE,
                  "The latest health check was stored at "
                      + hc.checkTime()
                      + ", more than "
                      + cfg.healthCheckMaxAge()
                      + " ago")),
          hc,
          maxAgeMs);
    }
    if (hc.hasError()) {
      String failed =
          hc.failedChecks().stream()
              .limit(MAX_FAILED_CHECKS_IN_MESSAGE)
              .map(fc -> fc.nodeName() + ": " + fc.checkName())
              .collect(Collectors.joining("; "));
      return new HealthCheckInput(
          InputResult.FAILED,
          List.of(
              new Reason(
                  Reason.HEALTH_CHECK_ERRORS,
                  "The latest health check reported errors"
                      + (failed.isEmpty() ? "" : ": " + failed))),
          hc,
          maxAgeMs);
    }
    // Warnings are reported in the snapshot but never change the verdict.
    return new HealthCheckInput(InputResult.PASSED, List.of(), hc, maxAgeMs);
  }

  private static SafeToTakeDownInput safeToTakeDownInput(
      Facts f, @Nullable SweepResult s, Settings cfg, Instant now) {
    // Checked from the universe's node states first: no RPC is needed, and the health check skips
    // nodes that are not active.
    if (!f.notLiveNodes().isEmpty()) {
      return failed(
          Reason.NODE_NOT_LIVE,
          "Primary-cluster nodes are not Live: " + String.join(",", f.notLiveNodes()),
          s);
    }
    if (f.primaryRf() <= 1) {
      // The check always fails at RF1: no tablet has another replica.
      return notApplicable(Reason.RF1, "The primary cluster has replication factor 1", s);
    }
    if (!f.apiSupported()) {
      return notApplicable(
          Reason.DB_VERSION_UNSUPPORTED,
          "The universe's DB version does not support AreNodesSafeToTakeDown",
          s);
    }
    if (!cfg.sttdEnabled()) {
      return notApplicable(
          Reason.SAFE_TO_TAKE_DOWN_DISABLED, "The nodes safe to take down check is disabled", s);
    }
    if (s == null) {
      return new SafeToTakeDownInput(
          InputResult.UNKNOWN,
          new Reason(
              Reason.NOT_YET_EVALUATED,
              "The nodes have not been checked since YBA started or the universe was last busy,"
                  + " paused, not created or disabled"),
          null);
    }
    Duration interval =
        cfg.sttdInterval().compareTo(cfg.evaluationInterval()) >= 0
            ? cfg.sttdInterval()
            : cfg.evaluationInterval();
    Duration maxAge = interval.multipliedBy(SWEEP_STALE_INTERVALS);
    if (now.isAfter(s.finishedAt().plus(maxAge))) {
      return new SafeToTakeDownInput(
          InputResult.UNKNOWN,
          new Reason(
              Reason.SAFE_TO_TAKE_DOWN_STALE,
              "The nodes were last checked at "
                  + s.finishedAt()
                  + ", more than "
                  + maxAge
                  + " ago"),
          s);
    }
    SweepOutcome outcome = s.outcome();
    switch (outcome) {
      case SAFE:
        return new SafeToTakeDownInput(InputResult.PASSED, null, s);
      case NOT_ALIVE:
        return failed(Reason.NODE_NOT_ALIVE, s.message(), s);
      case UNSAFE:
        return failed(Reason.NODES_NOT_SAFE_TO_TAKE_DOWN, s.message(), s);
      case UNSUPPORTED:
        return notApplicable(Reason.DB_VERSION_UNSUPPORTED, s.message(), s);
      case UNRESOLVED:
        return new SafeToTakeDownInput(
            InputResult.UNKNOWN, new Reason(Reason.NODE_NOT_REGISTERED, s.message()), s);
      case ERROR:
      default:
        return new SafeToTakeDownInput(
            InputResult.UNKNOWN, new Reason(Reason.CHECK_ERROR, s.message()), s);
    }
  }

  private static SafeToTakeDownInput failed(String code, String message, SweepResult s) {
    return new SafeToTakeDownInput(InputResult.FAILED, new Reason(code, message), s);
  }

  private static SafeToTakeDownInput notApplicable(String code, String message, SweepResult s) {
    return new SafeToTakeDownInput(InputResult.NOT_APPLICABLE, new Reason(code, message), s);
  }
}
