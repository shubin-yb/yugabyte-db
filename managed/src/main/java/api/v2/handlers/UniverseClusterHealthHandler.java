// Copyright (c) YugabyteDB, Inc.

package api.v2.handlers;

import api.v2.models.ClusterHealthCheckFailure;
import api.v2.models.ClusterHealthCheckInput;
import api.v2.models.ClusterHealthInputResult;
import api.v2.models.NodeSafeToTakeDownResult;
import api.v2.models.NodeSafeToTakeDownStatus;
import api.v2.models.NodesSafeToTakeDownInput;
import api.v2.models.UniverseClusterHealth;
import api.v2.models.UniverseClusterHealthReason;
import api.v2.models.UniverseClusterHealthStatus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yugabyte.yw.commissioner.ClusterHealth;
import com.yugabyte.yw.commissioner.ClusterHealth.FailedCheck;
import com.yugabyte.yw.commissioner.ClusterHealth.HealthCheckInput;
import com.yugabyte.yw.commissioner.ClusterHealth.HealthCheckSnapshot;
import com.yugabyte.yw.commissioner.ClusterHealth.NodeResult;
import com.yugabyte.yw.commissioner.ClusterHealth.Reason;
import com.yugabyte.yw.commissioner.ClusterHealth.SafeToTakeDownInput;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepResult;
import com.yugabyte.yw.commissioner.UniverseClusterHealthChecker;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.Universe;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Singleton
public class UniverseClusterHealthHandler {

  private final UniverseClusterHealthChecker checker;

  @Inject
  public UniverseClusterHealthHandler(UniverseClusterHealthChecker checker) {
    this.checker = checker;
  }

  /**
   * Composes the health from what the checker stored. Never calls the universe, so a client can
   * poll this as often as it likes.
   */
  public UniverseClusterHealth getClusterHealth(UUID cUUID, UUID uniUUID) {
    Customer customer = Customer.getOrNotFound(cUUID);
    Universe universe = Universe.getOrNotFound(uniUUID, cUUID);
    return toV2(checker.getClusterHealth(customer, universe, Instant.now()));
  }

  // The API enum values are the internal enums' value names, so they map by name;
  // UniverseApiControllerClusterHealthTest fails if an internal value has no API value.

  private static UniverseClusterHealth toV2(ClusterHealth health) {
    return new UniverseClusterHealth()
        .universeUuid(health.universeUuid())
        .status(UniverseClusterHealthStatus.fromValue(health.status().name()))
        .evaluatedAt(toOffsetDateTime(health.evaluatedAt()))
        .reasons(health.reasons().stream().map(UniverseClusterHealthHandler::toV2).toList())
        .healthCheck(toV2(health.healthCheck()))
        .nodesSafeToTakeDown(toV2(health.safeToTakeDown()));
  }

  private static UniverseClusterHealthReason toV2(Reason reason) {
    return new UniverseClusterHealthReason().code(reason.code()).message(reason.message());
  }

  private static ClusterHealthCheckInput toV2(HealthCheckInput input) {
    ClusterHealthCheckInput out =
        new ClusterHealthCheckInput()
            .result(ClusterHealthInputResult.fromValue(input.result().name()))
            .maxAgeMs(input.maxAgeMs());
    HealthCheckSnapshot snapshot = input.snapshot();
    if (snapshot != null) {
      out.checkTime(toOffsetDateTime(snapshot.checkTime()))
          .hasError(snapshot.hasError())
          .hasWarning(snapshot.hasWarning())
          .failedChecks(
              snapshot.failedChecks().stream().map(UniverseClusterHealthHandler::toV2).toList());
    }
    return out;
  }

  private static ClusterHealthCheckFailure toV2(FailedCheck check) {
    return new ClusterHealthCheckFailure()
        .nodeName(check.nodeName())
        .nodeAddress(check.nodeAddress())
        .process(check.process())
        .checkName(check.checkName())
        .details(check.details());
  }

  private static NodesSafeToTakeDownInput toV2(SafeToTakeDownInput input) {
    NodesSafeToTakeDownInput out =
        new NodesSafeToTakeDownInput()
            .result(ClusterHealthInputResult.fromValue(input.result().name()))
            .reasonCode(input.reason() == null ? null : input.reason().code());
    SweepResult sweep = input.lastSweep();
    if (sweep != null) {
      out.checkedAt(toOffsetDateTime(sweep.finishedAt()))
          .durationMs(sweep.durationMs())
          .followerLagBoundMs(sweep.followerLagBoundMs())
          .nodes(sweep.nodes().stream().map(UniverseClusterHealthHandler::toV2).toList());
    }
    return out;
  }

  private static NodeSafeToTakeDownResult toV2(NodeResult node) {
    return new NodeSafeToTakeDownResult()
        .nodeName(node.nodeName())
        .nodeUuid(node.nodeUuid())
        .isMaster(node.isMaster())
        .isTserver(node.isTserver())
        .address(node.address())
        .status(NodeSafeToTakeDownStatus.fromValue(node.status().name()))
        .message(node.message())
        .durationMs(node.durationMs());
  }

  private static OffsetDateTime toOffsetDateTime(Instant instant) {
    return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
  }
}
