// Copyright (c) YugabyteDB, Inc.

package com.yugabyte.yw.api.v2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static play.test.Helpers.contentAsString;

import com.fasterxml.jackson.databind.JsonNode;
import com.yugabyte.yba.v2.client.ApiClient;
import com.yugabyte.yba.v2.client.ApiException;
import com.yugabyte.yba.v2.client.Configuration;
import com.yugabyte.yba.v2.client.api.UniverseApi;
import com.yugabyte.yba.v2.client.models.ClusterHealthCheckFailure;
import com.yugabyte.yba.v2.client.models.ClusterHealthCheckInput;
import com.yugabyte.yba.v2.client.models.ClusterHealthInputResult;
import com.yugabyte.yba.v2.client.models.NodeSafeToTakeDownResult;
import com.yugabyte.yba.v2.client.models.NodeSafeToTakeDownStatus;
import com.yugabyte.yba.v2.client.models.NodesSafeToTakeDownInput;
import com.yugabyte.yba.v2.client.models.UniverseClusterHealth;
import com.yugabyte.yba.v2.client.models.UniverseClusterHealthReason;
import com.yugabyte.yba.v2.client.models.UniverseClusterHealthStatus;
import com.yugabyte.yw.commissioner.ClusterHealth;
import com.yugabyte.yw.commissioner.ClusterHealth.NodeResult;
import com.yugabyte.yw.commissioner.ClusterHealth.NodeStatus;
import com.yugabyte.yw.commissioner.ClusterHealth.Reason;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepOutcome;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepResult;
import com.yugabyte.yw.commissioner.UniverseClusterHealthChecker;
import com.yugabyte.yw.common.FakeDBApplication;
import com.yugabyte.yw.common.ModelFactory;
import com.yugabyte.yw.common.config.UniverseConfKeys;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.Cluster;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.HealthCheck;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.Users;
import com.yugabyte.yw.models.helpers.CloudSpecificInfo;
import com.yugabyte.yw.models.helpers.NodeDetails;
import com.yugabyte.yw.models.helpers.NodeDetails.NodeState;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import play.libs.Json;
import play.mvc.Result;

/**
 * The GET reads what the background checker stored: these tests seed the health check table and the
 * checker's sweep cache, and check that no request reaches the universe.
 */
public class UniverseApiControllerClusterHealthTest extends FakeDBApplication {

  // ModelFactory's default version predates AreNodesSafeToTakeDown.
  private static final String SUPPORTED_VERSION = "2024.2.0.0-b1";
  private static final long LAG_MS = 60000;

  private Customer customer;
  private Universe universe;
  private String authToken;
  private UniverseApi universeApi;
  private UniverseClusterHealthChecker checker;

  @Before
  public void setUp() {
    customer = ModelFactory.testCustomer();
    Users user = ModelFactory.testUser(customer);
    authToken = user.createAuthToken();
    universe = newUniverse(customer, "cluster-health");
    // The JIT singleton the handler uses: its sweep cache is what the GET reads.
    checker = app.injector().instanceOf(UniverseClusterHealthChecker.class);

    ApiClient v2Client = Configuration.getDefaultApiClient();
    String basePath = String.format("http://localhost:%d/api/v2", port);
    v2Client = v2Client.setBasePath(basePath).addDefaultHeader("X-AUTH-TOKEN", authToken);
    Configuration.setDefaultApiClient(v2Client);
    universeApi = new UniverseApi();
  }

  private static Universe newUniverse(Customer customer, String name) {
    Universe created = ModelFactory.createUniverse(name, customer.getId());
    return Universe.saveDetails(
        created.getUniverseUUID(),
        u -> {
          UniverseDefinitionTaskParams details = u.getUniverseDetails();
          Cluster primary = details.getPrimaryCluster();
          primary.userIntent.ybSoftwareVersion = SUPPORTED_VERSION;
          primary.userIntent.replicationFactor = 3;
          details.nodeDetailsSet = new HashSet<>();
          for (int i = 1; i <= 3; i++) {
            NodeDetails node = new NodeDetails();
            node.nodeName = name + "-n" + i;
            node.nodeUuid = UUID.randomUUID();
            node.isMaster = true;
            node.isTserver = true;
            node.state = NodeState.Live;
            node.placementUuid = primary.uuid;
            node.cloudInfo = new CloudSpecificInfo();
            node.cloudInfo.private_ip = "10.0.0." + i;
            details.nodeDetailsSet.add(node);
          }
          u.setUniverseDetails(details);
        });
  }

  private List<NodeDetails> nodes() {
    return universe.getUniverseDetails().nodeDetailsSet.stream()
        .sorted(Comparator.comparing(n -> n.nodeName))
        .collect(Collectors.toList());
  }

  private static NodeResult nodeResult(NodeDetails node, NodeStatus status, String message) {
    return new NodeResult(
        node.nodeName,
        node.nodeUuid,
        node.isMaster,
        node.isTserver,
        node.cloudInfo.private_ip,
        status,
        message,
        42);
  }

  private void putSweep(SweepOutcome outcome, String message, List<NodeResult> nodes) {
    Instant finishedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    checker.putSweep(
        universe.getUniverseUUID(),
        new SweepResult(
            finishedAt.minusMillis(150), finishedAt, 150, outcome, message, LAG_MS, nodes));
  }

  private void putSafeSweep() {
    putSweep(
        SweepOutcome.SAFE,
        null,
        nodes().stream().map(n -> nodeResult(n, NodeStatus.SAFE, null)).toList());
  }

  private void storeHealthCheck(HealthCheck.Details details) {
    HealthCheck.addAndPrune(universe.getUniverseUUID(), customer.getId(), details);
  }

  private UniverseClusterHealth get() throws ApiException {
    return universeApi.getUniverseClusterHealth(customer.getUuid(), universe.getUniverseUUID());
  }

  private void assertNoRpc() throws Exception {
    verify(mockService, never()).getUniverseClient(any());
    verify(mockYBClient, never()).areNodesSafeToTakeDown(any(), any(), anyLong());
    verify(mockYBClient, never()).listTabletServers();
  }

  private static List<String> codes(List<UniverseClusterHealthReason> reasons) {
    return reasons.stream().map(UniverseClusterHealthReason::getCode).toList();
  }

  @Test
  public void testHealthyWithPassingHealthCheckAndSafeSweep() throws Exception {
    storeHealthCheck(new HealthCheck.Details().setHasError(false).setHasWarning(true));
    putSafeSweep();

    UniverseClusterHealth health = get();

    assertEquals(universe.getUniverseUUID(), health.getUniverseUuid());
    assertEquals(UniverseClusterHealthStatus.ClusterHealthHealthy, health.getStatus());
    assertNotNull(health.getEvaluatedAt());
    assertTrue(health.getReasons().isEmpty());

    ClusterHealthCheckInput hc = health.getHealthCheck();
    assertEquals(ClusterHealthInputResult.ClusterHealthInputPassed, hc.getResult());
    assertNotNull(hc.getCheckTime());
    assertFalse(hc.getHasError());
    // Warnings are reported but don't change the status.
    assertTrue(hc.getHasWarning());
    assertTrue(hc.getMaxAgeMs() > 0);
    assertTrue(hc.getFailedChecks().isEmpty());

    NodesSafeToTakeDownInput sttd = health.getNodesSafeToTakeDown();
    assertEquals(ClusterHealthInputResult.ClusterHealthInputPassed, sttd.getResult());
    assertNull(sttd.getReasonCode());
    assertEquals(
        checker.getSweep(universe.getUniverseUUID()).finishedAt(), sttd.getCheckedAt().toInstant());
    assertEquals(Long.valueOf(150), sttd.getDurationMs());
    assertEquals(Long.valueOf(LAG_MS), sttd.getFollowerLagBoundMs());
    assertEquals(3, sttd.getNodes().size());
    NodeDetails first = nodes().get(0);
    NodeSafeToTakeDownResult node = sttd.getNodes().get(0);
    assertEquals(first.nodeName, node.getNodeName());
    assertEquals(first.nodeUuid, node.getNodeUuid());
    assertTrue(node.getIsMaster());
    assertTrue(node.getIsTserver());
    assertEquals(first.cloudInfo.private_ip, node.getAddress());
    assertEquals(NodeSafeToTakeDownStatus.NodeSafeToTakeDownSafe, node.getStatus());
    assertNull(node.getMessage());
    assertEquals(Long.valueOf(42), node.getDurationMs());

    assertNoRpc();
  }

  @Test
  public void testHealthCheckErrorsMakeItUnhealthy() throws Exception {
    storeHealthCheck(
        new HealthCheck.Details()
            .setHasError(true)
            .setData(
                List.of(
                    new HealthCheck.Details.NodeData()
                        .setNodeName("cluster-health-n2")
                        .setNode("10.0.0.2")
                        .setProcess("yb-tserver")
                        .setMessage("Fatal log files")
                        .setHasError(true)
                        .setDetails(List.of("F1113 core dumped", "F1113 again")),
                    new HealthCheck.Details.NodeData()
                        .setNodeName("cluster-health-n1")
                        .setMessage("Disk utilization")
                        .setHasError(false))));
    putSafeSweep();

    UniverseClusterHealth health = get();

    assertEquals(UniverseClusterHealthStatus.ClusterHealthUnhealthy, health.getStatus());
    assertEquals(List.of(Reason.HEALTH_CHECK_ERRORS), codes(health.getReasons()));
    ClusterHealthCheckInput hc = health.getHealthCheck();
    assertEquals(ClusterHealthInputResult.ClusterHealthInputFailed, hc.getResult());
    assertTrue(hc.getHasError());
    assertEquals(1, hc.getFailedChecks().size());
    ClusterHealthCheckFailure failure = hc.getFailedChecks().get(0);
    assertEquals("cluster-health-n2", failure.getNodeName());
    assertEquals("10.0.0.2", failure.getNodeAddress());
    assertEquals("yb-tserver", failure.getProcess());
    assertEquals("Fatal log files", failure.getCheckName());
    assertEquals(List.of("F1113 core dumped", "F1113 again"), failure.getDetails());
    assertEquals(
        ClusterHealthInputResult.ClusterHealthInputPassed,
        health.getNodesSafeToTakeDown().getResult());
    assertNoRpc();
  }

  @Test
  public void testUnsafeNodeMakesItUnhealthy() throws Exception {
    storeHealthCheck(new HealthCheck.Details().setHasError(false));
    List<NodeDetails> nodes = nodes();
    putSweep(
        SweepOutcome.UNSAFE,
        nodes.get(1).nodeName + ": tablet t1 would lose its majority",
        List.of(
            nodeResult(nodes.get(0), NodeStatus.SAFE, null),
            nodeResult(nodes.get(1), NodeStatus.UNSAFE, "tablet t1 would lose its majority"),
            nodeResult(nodes.get(2), NodeStatus.NOT_CHECKED, null)));

    UniverseClusterHealth health = get();

    assertEquals(UniverseClusterHealthStatus.ClusterHealthUnhealthy, health.getStatus());
    assertEquals(List.of(Reason.NODES_NOT_SAFE_TO_TAKE_DOWN), codes(health.getReasons()));
    assertTrue(health.getReasons().get(0).getMessage().contains("would lose its majority"));
    NodesSafeToTakeDownInput sttd = health.getNodesSafeToTakeDown();
    assertEquals(ClusterHealthInputResult.ClusterHealthInputFailed, sttd.getResult());
    assertEquals(Reason.NODES_NOT_SAFE_TO_TAKE_DOWN, sttd.getReasonCode());
    assertEquals(
        List.of(
            NodeSafeToTakeDownStatus.NodeSafeToTakeDownSafe,
            NodeSafeToTakeDownStatus.NodeSafeToTakeDownUnsafe,
            NodeSafeToTakeDownStatus.NodeSafeToTakeDownNotChecked),
        sttd.getNodes().stream().map(NodeSafeToTakeDownResult::getStatus).toList());
    assertEquals("tablet t1 would lose its majority", sttd.getNodes().get(1).getMessage());
    assertNoRpc();
  }

  @Test
  public void testUnknownBeforeTheFirstSweep() throws Exception {
    storeHealthCheck(new HealthCheck.Details().setHasError(false));

    UniverseClusterHealth health = get();

    assertEquals(UniverseClusterHealthStatus.ClusterHealthUnknown, health.getStatus());
    assertEquals(List.of(Reason.NOT_YET_EVALUATED), codes(health.getReasons()));
    NodesSafeToTakeDownInput sttd = health.getNodesSafeToTakeDown();
    assertEquals(ClusterHealthInputResult.ClusterHealthInputUnknown, sttd.getResult());
    assertEquals(Reason.NOT_YET_EVALUATED, sttd.getReasonCode());
    assertNull(sttd.getCheckedAt());
    assertNull(sttd.getDurationMs());
    assertNull(sttd.getNodes());
    assertEquals(
        ClusterHealthInputResult.ClusterHealthInputPassed, health.getHealthCheck().getResult());
    assertNoRpc();
  }

  @Test
  public void testUnknownWithoutAHealthCheck() throws Exception {
    putSafeSweep();

    UniverseClusterHealth health = get();

    assertEquals(UniverseClusterHealthStatus.ClusterHealthUnknown, health.getStatus());
    assertEquals(List.of(Reason.HEALTH_CHECK_MISSING), codes(health.getReasons()));
    ClusterHealthCheckInput hc = health.getHealthCheck();
    assertEquals(ClusterHealthInputResult.ClusterHealthInputUnknown, hc.getResult());
    assertNull(hc.getCheckTime());
    assertNull(hc.getHasError());
    assertNoRpc();
  }

  @Test
  public void testPausedUniverseIsUnknownWithItsStoredInputs() throws Exception {
    storeHealthCheck(new HealthCheck.Details().setHasError(false));
    putSafeSweep();
    universe =
        Universe.saveDetails(
            universe.getUniverseUUID(),
            u -> {
              UniverseDefinitionTaskParams details = u.getUniverseDetails();
              details.universePaused = true;
              u.setUniverseDetails(details);
            });

    UniverseClusterHealth health = get();

    assertEquals(UniverseClusterHealthStatus.ClusterHealthUnknown, health.getStatus());
    assertEquals(List.of(Reason.UNIVERSE_PAUSED), codes(health.getReasons()));
    assertEquals(
        ClusterHealthInputResult.ClusterHealthInputUnknown, health.getHealthCheck().getResult());
    assertNotNull(health.getHealthCheck().getCheckTime());
    NodesSafeToTakeDownInput sttd = health.getNodesSafeToTakeDown();
    assertEquals(ClusterHealthInputResult.ClusterHealthInputUnknown, sttd.getResult());
    assertNull(sttd.getReasonCode());
    assertEquals(3, sttd.getNodes().size());
    assertNoRpc();
  }

  @Test
  public void testDisabledForTheUniverse() throws Exception {
    storeHealthCheck(new HealthCheck.Details().setHasError(false));
    putSafeSweep();
    mutableConfigFactory
        .forUniverse(universe)
        .setValue(UniverseConfKeys.clusterHealthEnabled.getKey(), "false");

    UniverseClusterHealth health = get();

    assertEquals(UniverseClusterHealthStatus.ClusterHealthUnknown, health.getStatus());
    assertEquals(List.of(Reason.DISABLED), codes(health.getReasons()));
    assertNoRpc();
  }

  @Test
  public void testUnknownUniverseIsNotFound() throws Exception {
    ApiException e =
        assertThrows(
            ApiException.class,
            () -> universeApi.getUniverseClusterHealth(customer.getUuid(), UUID.randomUUID()));
    assertEquals(404, e.getCode());
    assertNoRpc();
  }

  @Test
  public void testOtherCustomersUniverseIsNotFound() throws Exception {
    Customer other = ModelFactory.testCustomer("tc2", "Other Customer");
    Universe othersUniverse = newUniverse(other, "other-universe");

    ApiException e =
        assertThrows(
            ApiException.class,
            () ->
                universeApi.getUniverseClusterHealth(
                    customer.getUuid(), othersUniverse.getUniverseUUID()));
    assertEquals(404, e.getCode());
    assertNoRpc();
  }

  @Test
  public void testUnknownCustomerIsRejectedBeforeTheHandler() {
    // The auth filter rejects a customer that is not the caller's before the handler's 404.
    ApiException e =
        assertThrows(
            ApiException.class,
            () ->
                universeApi.getUniverseClusterHealth(
                    UUID.randomUUID(), universe.getUniverseUUID()));
    assertEquals(401, e.getCode());
  }

  @Test
  public void testWireFormat() throws Exception {
    storeHealthCheck(new HealthCheck.Details().setHasError(false));
    putSafeSweep();

    String path =
        String.format(
            "/api/v2/customers/%s/universes/%s/cluster-health",
            customer.getUuid(), universe.getUniverseUUID());
    Result result = doRequestWithAuthToken("GET", path, authToken);
    assertEquals(contentAsString(result), 200, result.status());
    JsonNode body = Json.parse(contentAsString(result));

    assertEquals(universe.getUniverseUUID().toString(), body.get("universe_uuid").asText());
    assertEquals("HEALTHY", body.get("status").asText());
    assertTrue(body.has("evaluated_at"));
    assertTrue(body.get("reasons").isArray());
    JsonNode hc = body.get("health_check");
    assertEquals("PASSED", hc.get("result").asText());
    assertTrue(hc.has("check_time"));
    assertTrue(hc.has("max_age_ms"));
    assertFalse(hc.get("has_error").asBoolean());
    JsonNode sttd = body.get("nodes_safe_to_take_down");
    assertEquals("PASSED", sttd.get("result").asText());
    assertTrue(sttd.has("checked_at"));
    assertEquals(150, sttd.get("duration_ms").asLong());
    assertEquals(LAG_MS, sttd.get("follower_lag_bound_ms").asLong());
    JsonNode node = sttd.get("nodes").get(0);
    assertEquals(nodes().get(0).nodeName, node.get("node_name").asText());
    assertEquals("SAFE", node.get("status").asText());
    assertTrue(node.get("is_master").asBoolean());
    assertTrue(node.get("is_tserver").asBoolean());
    assertEquals(42, node.get("duration_ms").asLong());
  }

  /** The handler maps enums by value name, so every internal value must exist in the API. */
  @Test
  public void testEveryInternalEnumValueHasAnApiValue() {
    List<String> missing = new ArrayList<>();
    for (ClusterHealth.Status v : ClusterHealth.Status.values()) {
      checkValue(missing, v.name(), api.v2.models.UniverseClusterHealthStatus::fromValue);
      checkValue(missing, v.name(), UniverseClusterHealthStatus::fromValue);
    }
    for (ClusterHealth.InputResult v : ClusterHealth.InputResult.values()) {
      checkValue(missing, v.name(), api.v2.models.ClusterHealthInputResult::fromValue);
      checkValue(missing, v.name(), ClusterHealthInputResult::fromValue);
    }
    for (NodeStatus v : NodeStatus.values()) {
      checkValue(missing, v.name(), api.v2.models.NodeSafeToTakeDownStatus::fromValue);
      checkValue(missing, v.name(), NodeSafeToTakeDownStatus::fromValue);
    }
    assertTrue("No API value for " + missing, missing.isEmpty());
  }

  private static void checkValue(
      List<String> missing, String value, Function<String, ?> fromValue) {
    try {
      assertNotNull(fromValue.apply(value));
    } catch (IllegalArgumentException e) {
      missing.add(value);
    }
  }
}
