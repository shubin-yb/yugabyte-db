// Copyright (c) YugabyteDB, Inc.

package com.yugabyte.yw.commissioner;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * A universe's cluster health: the latest stored health check combined with the last
 * AreNodesSafeToTakeDown sweep of its primary-cluster nodes. Composed by {@link
 * ClusterHealthRules#compose}.
 */
public record ClusterHealth(
    UUID universeUuid,
    Status status,
    Instant evaluatedAt,
    List<Reason> reasons,
    HealthCheckInput healthCheck,
    SafeToTakeDownInput safeToTakeDown) {

  public enum Status {
    HEALTHY,
    UNHEALTHY,
    UNKNOWN
  }

  public enum InputResult {
    PASSED,
    FAILED,
    NOT_APPLICABLE,
    UNKNOWN
  }

  public enum NodeStatus {
    SAFE,
    UNSAFE,
    NOT_ALIVE,
    UNRESOLVED,
    UNSUPPORTED,
    ERROR,
    NOT_CHECKED
  }

  public enum SweepOutcome {
    SAFE,
    UNSAFE,
    NOT_ALIVE,
    UNRESOLVED,
    UNSUPPORTED,
    ERROR
  }

  /** Codes are strings, not an enum, so that the API can add codes without breaking clients. */
  public record Reason(String code, String message) {
    // Gates: the universe is not evaluated at all.
    public static final String YBA_STANDBY = "YBA_STANDBY";
    public static final String DISABLED = "DISABLED";
    public static final String NOT_CREATED = "NOT_CREATED";
    public static final String UNIVERSE_PAUSED = "UNIVERSE_PAUSED";
    public static final String TASK_IN_PROGRESS = "TASK_IN_PROGRESS";
    // Health check input.
    public static final String HEALTH_CHECK_MISSING = "HEALTH_CHECK_MISSING";
    public static final String HEALTH_CHECK_STALE = "HEALTH_CHECK_STALE";
    public static final String HEALTH_CHECK_PENDING = "HEALTH_CHECK_PENDING";
    public static final String HEALTH_CHECK_ERRORS = "HEALTH_CHECK_ERRORS";
    // Nodes safe to take down input.
    public static final String NODE_NOT_LIVE = "NODE_NOT_LIVE";
    public static final String RF1 = "RF1";
    public static final String DB_VERSION_UNSUPPORTED = "DB_VERSION_UNSUPPORTED";
    public static final String SAFE_TO_TAKE_DOWN_DISABLED = "SAFE_TO_TAKE_DOWN_DISABLED";
    public static final String NOT_YET_EVALUATED = "NOT_YET_EVALUATED";
    public static final String SAFE_TO_TAKE_DOWN_STALE = "SAFE_TO_TAKE_DOWN_STALE";
    public static final String NODE_NOT_ALIVE = "NODE_NOT_ALIVE";
    public static final String NODES_NOT_SAFE_TO_TAKE_DOWN = "NODES_NOT_SAFE_TO_TAKE_DOWN";
    public static final String NODE_NOT_REGISTERED = "NODE_NOT_REGISTERED";
    public static final String CHECK_ERROR = "CHECK_ERROR";
  }

  /**
   * The latest stored health check. {@code checkTime} is when the row was persisted, not when the
   * run started.
   */
  public record HealthCheckSnapshot(
      Instant checkTime, boolean hasError, boolean hasWarning, List<FailedCheck> failedChecks) {}

  /** A health check entry with an error. {@code checkName} is the entry's message. */
  public record FailedCheck(
      String nodeName,
      String nodeAddress,
      String process,
      String checkName,
      List<String> details) {}

  /**
   * One primary-cluster NodeDetails in a sweep, keyed by name and uuid: on Kubernetes the address
   * is the pod FQDN, not an IP. A node's own result does not say whether that node is healthy: the
   * check for a down node passes while its peers' checks fail.
   */
  public record NodeResult(
      String nodeName,
      UUID nodeUuid,
      boolean isMaster,
      boolean isTserver,
      String address,
      NodeStatus status,
      String message,
      long durationMs) {}

  public record SweepResult(
      Instant startedAt,
      Instant finishedAt,
      long durationMs,
      SweepOutcome outcome,
      String message,
      long followerLagBoundMs,
      List<NodeResult> nodes) {}

  public record HealthCheckInput(
      InputResult result, List<Reason> reasons, HealthCheckSnapshot snapshot, long maxAgeMs) {}

  public record SafeToTakeDownInput(InputResult result, Reason reason, SweepResult lastSweep) {}
}
