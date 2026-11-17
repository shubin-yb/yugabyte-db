// Copyright (c) YugabyteDB, Inc.

package com.yugabyte.yw.commissioner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import com.typesafe.config.Config;
import com.yugabyte.yw.commissioner.ClusterHealth.NodeResult;
import com.yugabyte.yw.commissioner.ClusterHealth.NodeStatus;
import com.yugabyte.yw.commissioner.ClusterHealth.Reason;
import com.yugabyte.yw.commissioner.ClusterHealth.Status;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepOutcome;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepResult;
import com.yugabyte.yw.common.FakeDBApplication;
import com.yugabyte.yw.common.ModelFactory;
import com.yugabyte.yw.common.NodesSafeToTakeDownUtil;
import com.yugabyte.yw.common.NodesSafeToTakeDownUtil.Caller;
import com.yugabyte.yw.common.NodesSafeToTakeDownUtil.Result;
import com.yugabyte.yw.common.PlatformExecutorFactory;
import com.yugabyte.yw.common.PlatformScheduler;
import com.yugabyte.yw.common.config.RuntimeConfGetter;
import com.yugabyte.yw.common.config.UniverseConfKeys;
import com.yugabyte.yw.common.metrics.MetricService;
import com.yugabyte.yw.common.metrics.MetricStorage;
import com.yugabyte.yw.common.services.YBClientService;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.Cluster;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.ClusterType;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.HealthCheck;
import com.yugabyte.yw.models.HighAvailabilityConfig;
import com.yugabyte.yw.models.Metric;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.helpers.CloudSpecificInfo;
import com.yugabyte.yw.models.helpers.KnownAlertLabels;
import com.yugabyte.yw.models.helpers.NodeDetails;
import com.yugabyte.yw.models.helpers.NodeDetails.NodeState;
import com.yugabyte.yw.models.helpers.PlatformMetrics;
import com.yugabyte.yw.models.helpers.TaskType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.yb.client.AreNodesSafeToTakeDownResponse;
import org.yb.client.ListLiveTabletServersResponse;
import org.yb.client.ListMastersResponse;
import org.yb.client.ListTabletServersResponse;
import org.yb.client.MasterErrorException;
import org.yb.client.YBClientApi;
import org.yb.util.ServerInfo;
import org.yb.util.TabletServerInfo;

public class UniverseClusterHealthCheckerTest extends FakeDBApplication {

  private static final String SUPPORTED_VERSION = "2024.2.0.0-b1";
  private static final long LAG_MS = 60000;

  private Customer customer;
  private Universe universe;
  private YBClientService ybClientService;
  private YBClientApi client;
  private MetricService metrics;
  private MutableClock clock;
  private UniverseClusterHealthChecker checker;

  private static final class MutableClock extends Clock {
    private Instant now;

    MutableClock(Instant now) {
      this.now = now;
    }

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }

  @Before
  public void setUp() throws Exception {
    app.injector().instanceOf(MetricStorage.class).clear();
    customer = ModelFactory.testCustomer();
    clock = new MutableClock(Instant.now());
    ybClientService = mock(YBClientService.class);
    client = mock(YBClientApi.class);
    when(ybClientService.getUniverseClient(any())).thenReturn(client);
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong()))
        .thenReturn(new AreNodesSafeToTakeDownResponse(null));
    metrics = spy(metricService);
    checker =
        new UniverseClusterHealthChecker(
            app.injector().instanceOf(RuntimeConfGetter.class),
            app.injector().instanceOf(Config.class),
            mock(PlatformScheduler.class),
            mock(PlatformExecutorFactory.class),
            ybClientService,
            metrics,
            MoreExecutors.newDirectExecutorService(),
            clock);
    universe = newUniverse("u1", 3, vmNodes(3));
    mockRegisteredServers(universe);
  }

  // Universe and server helpers.

  private static NodeDetails node(String name, String ip, boolean master, boolean tserver) {
    NodeDetails node = new NodeDetails();
    node.nodeName = name;
    node.nodeUuid = UUID.randomUUID();
    node.isMaster = master;
    node.isTserver = tserver;
    node.state = NodeState.Live;
    node.cloudInfo = new CloudSpecificInfo();
    node.cloudInfo.private_ip = ip;
    return node;
  }

  private static List<NodeDetails> vmNodes(int count) {
    List<NodeDetails> nodes = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      nodes.add(node("n" + i, "10.0.0." + i, true, true));
    }
    return nodes;
  }

  private Universe newUniverse(String name, int rf, List<NodeDetails> nodes) {
    Universe created = ModelFactory.createUniverse(name, customer.getId());
    return Universe.saveDetails(
        created.getUniverseUUID(),
        u -> {
          UniverseDefinitionTaskParams details = u.getUniverseDetails();
          Cluster primary = details.getPrimaryCluster();
          primary.userIntent.ybSoftwareVersion = SUPPORTED_VERSION;
          primary.userIntent.replicationFactor = rf;
          details.nodeDetailsSet = new HashSet<>();
          for (NodeDetails node : nodes) {
            if (node.placementUuid == null) {
              node.placementUuid = primary.uuid;
            }
            details.nodeDetailsSet.add(node);
          }
          u.setUniverseDetails(details);
        });
  }

  private static NodeDetails nodeByName(UniverseDefinitionTaskParams details, String name) {
    return details.nodeDetailsSet.stream()
        .filter(n -> name.equals(n.nodeName))
        .findFirst()
        .orElseThrow();
  }

  private Universe update(Universe u, Consumer<UniverseDefinitionTaskParams> change) {
    return Universe.saveDetails(
        u.getUniverseUUID(),
        univ -> {
          UniverseDefinitionTaskParams details = univ.getUniverseDetails();
          change.accept(details);
          univ.setUniverseDetails(details);
        });
  }

  private static ServerInfo master(String uuid, String host, String state) {
    return new ServerInfo(uuid, host, 7100, false, state);
  }

  private static ServerInfo tserver(String uuid, String host) {
    return new ServerInfo(uuid, host, 9100, false, "");
  }

  /** Registers every node of the universe with the master leader, all alive. */
  private void mockRegisteredServers(Universe u) throws Exception {
    List<ServerInfo> masters = new ArrayList<>();
    List<ServerInfo> tservers = new ArrayList<>();
    for (NodeDetails node : u.getUniverseDetails().nodeDetailsSet) {
      if (node.isMaster) {
        masters.add(master("m-" + node.nodeName, node.cloudInfo.private_ip, "ALIVE"));
      }
      if (node.isTserver) {
        tservers.add(tserver("ts-" + node.nodeName, node.cloudInfo.private_ip));
      }
    }
    mockServers(
        tservers, tservers.stream().map(ServerInfo::getUuid).collect(Collectors.toList()), masters);
  }

  private void mockServers(
      List<ServerInfo> tservers, List<String> liveTserverUuids, List<ServerInfo> masters)
      throws Exception {
    ListTabletServersResponse tsResp = mock(ListTabletServersResponse.class);
    when(tsResp.getTabletServersList()).thenReturn(tservers);
    when(client.listTabletServers()).thenReturn(tsResp);
    List<TabletServerInfo> live = new ArrayList<>();
    for (String uuid : liveTserverUuids) {
      TabletServerInfo info = new TabletServerInfo();
      info.setPermanentUuid(uuid);
      live.add(info);
    }
    ListLiveTabletServersResponse liveResp = mock(ListLiveTabletServersResponse.class);
    when(liveResp.getTabletServers()).thenReturn(live);
    when(client.listLiveTabletServers()).thenReturn(liveResp);
    ListMastersResponse mastersResp = mock(ListMastersResponse.class);
    when(mastersResp.getMasters()).thenReturn(masters);
    when(client.listMasters()).thenReturn(mastersResp);
  }

  private void storeHealthCheck(Universe u, boolean hasError) {
    HealthCheck.addAndPrune(
        u.getUniverseUUID(), customer.getId(), new HealthCheck.Details().setHasError(hasError));
  }

  private List<Metric> gauge(Universe u) {
    return metricService.list(UniverseClusterHealthChecker.gaugeFilter(u.getUniverseUUID()));
  }

  private void assertGauge(Universe u, double value) {
    List<Metric> gauge = gauge(u);
    assertEquals(1, gauge.size());
    assertEquals(value, gauge.get(0).getValue(), 0);
  }

  private void assertNoGauge(Universe u) {
    assertTrue(gauge(u).isEmpty());
  }

  private void seedGaugeAndSweep(Universe u) {
    metricService.save(
        MetricService.buildMetricTemplate(
                PlatformMetrics.UNIVERSE_CLUSTER_HEALTH_STATUS, customer, u)
            .setValue(1.0));
    checker.putSweep(u.getUniverseUUID(), safeSweep(clock.instant()));
  }

  private static SweepResult safeSweep(Instant finishedAt) {
    return new SweepResult(
        finishedAt, finishedAt, 0, SweepOutcome.SAFE, null, LAG_MS, List.<NodeResult>of());
  }

  private static MasterErrorException masterError(String message) {
    MasterErrorException e = mock(MasterErrorException.class);
    when(e.getMessage()).thenReturn(message);
    return e;
  }

  private static long histogramCount(String target, Result result) {
    return NodesSafeToTakeDownUtil.histogram()
        .labelValues(Caller.CLUSTER_HEALTH.label, target, result.label())
        .getCount();
  }

  private static List<NodeStatus> statuses(SweepResult sweep) {
    return sweep.nodes().stream().map(NodeResult::status).collect(Collectors.toList());
  }

  // Sweep.

  @Test
  public void testSweepChecksEveryVmNodeOnce() throws Exception {
    long before = histogramCount("master_tserver", Result.SAFE);

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.SAFE, sweep.outcome());
    assertEquals(List.of(NodeStatus.SAFE, NodeStatus.SAFE, NodeStatus.SAFE), statuses(sweep));
    assertEquals(
        List.of("n1", "n2", "n3"),
        sweep.nodes().stream().map(NodeResult::nodeName).collect(Collectors.toList()));
    assertEquals("10.0.0.1", sweep.nodes().get(0).address());
    assertEquals(LAG_MS, sweep.followerLagBoundMs());
    for (int i = 1; i <= 3; i++) {
      String ip = "10.0.0." + i;
      verify(client, times(1)).areNodesSafeToTakeDown(Set.of(ip), Set.of(ip), LAG_MS);
    }
    verify(client, times(3)).areNodesSafeToTakeDown(any(), any(), anyLong());
    verify(client, times(1)).listTabletServers();
    verify(client, times(1)).listLiveTabletServers();
    verify(client, times(1)).listMasters();
    verify(client, times(1)).close();
    assertEquals(before + 3, histogramCount("master_tserver", Result.SAFE));
  }

  @Test
  public void testSweepChecksEachKubernetesPodAlone() throws Exception {
    List<NodeDetails> pods = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      pods.add(
          node(
              "yb-master-" + i,
              "yb-master-" + i + ".yb-masters.ns.svc.cluster.local",
              true,
              false));
      pods.add(
          node(
              "yb-tserver-" + i,
              "yb-tserver-" + i + ".yb-tservers.ns.svc.cluster.local",
              false,
              true));
    }
    Universe k8s = newUniverse("k8s", 3, pods);
    mockRegisteredServers(k8s);

    SweepResult sweep = checker.sweep(customer, k8s);

    assertEquals(SweepOutcome.SAFE, sweep.outcome());
    assertEquals(6, sweep.nodes().size());
    for (int i = 0; i < 3; i++) {
      verify(client)
          .areNodesSafeToTakeDown(
              Set.of("yb-master-" + i + ".yb-masters.ns.svc.cluster.local"), Set.of(), LAG_MS);
      verify(client)
          .areNodesSafeToTakeDown(
              Set.of(), Set.of("yb-tserver-" + i + ".yb-tservers.ns.svc.cluster.local"), LAG_MS);
    }
    verify(client, times(6)).areNodesSafeToTakeDown(any(), any(), anyLong());
  }

  @Test
  public void testUnregisteredNodeIsUnresolvedWithoutAnyCall() throws Exception {
    mockServers(
        List.of(tserver("ts-n1", "10.0.0.1"), tserver("ts-n2", "10.0.0.2")),
        List.of("ts-n1", "ts-n2"),
        List.of(
            master("m-n1", "10.0.0.1", "ALIVE"),
            master("m-n2", "10.0.0.2", "ALIVE"),
            master("m-n3", "10.0.0.3", "ALIVE")));

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.UNRESOLVED, sweep.outcome());
    assertEquals(
        List.of(NodeStatus.NOT_CHECKED, NodeStatus.NOT_CHECKED, NodeStatus.UNRESOLVED),
        statuses(sweep));
    assertTrue(sweep.message().contains("n3"));
    verify(client, never()).areNodesSafeToTakeDown(any(), any(), anyLong());
  }

  @Test
  public void testTserverNotLiveIsNotAliveWithoutAnyCall() throws Exception {
    List<ServerInfo> tservers =
        List.of(
            tserver("ts-n1", "10.0.0.1"),
            tserver("ts-n2", "10.0.0.2"),
            tserver("ts-n3", "10.0.0.3"));
    List<ServerInfo> masters =
        List.of(
            master("m-n1", "10.0.0.1", "ALIVE"),
            master("m-n2", "10.0.0.2", "ALIVE"),
            master("m-n3", "10.0.0.3", "ALIVE"));
    mockServers(tservers, List.of("ts-n1", "ts-n3"), masters);

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.NOT_ALIVE, sweep.outcome());
    assertEquals(NodeStatus.NOT_ALIVE, sweep.nodes().get(1).status());
    verify(client, never()).areNodesSafeToTakeDown(any(), any(), anyLong());
  }

  @Test
  public void testMasterNotAliveIsNotAliveWithoutAnyCall() throws Exception {
    mockServers(
        List.of(
            tserver("ts-n1", "10.0.0.1"),
            tserver("ts-n2", "10.0.0.2"),
            tserver("ts-n3", "10.0.0.3")),
        List.of("ts-n1", "ts-n2", "ts-n3"),
        List.of(
            master("m-n1", "10.0.0.1", "NETWORK_ERROR"),
            master("m-n2", "10.0.0.2", "ALIVE"),
            master("m-n3", "10.0.0.3", "ALIVE")));

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.NOT_ALIVE, sweep.outcome());
    assertEquals(NodeStatus.NOT_ALIVE, sweep.nodes().get(0).status());
    assertTrue(sweep.nodes().get(0).message().contains("NETWORK_ERROR"));
    verify(client, never()).areNodesSafeToTakeDown(any(), any(), anyLong());
  }

  @Test
  public void testRecreatedPodWithOneLiveUuidIsAlive() throws Exception {
    mockServers(
        List.of(
            tserver("ts-n1-old", "10.0.0.1"),
            tserver("ts-n1", "10.0.0.1"),
            tserver("ts-n2", "10.0.0.2"),
            tserver("ts-n3", "10.0.0.3")),
        List.of("ts-n1", "ts-n2", "ts-n3"),
        List.of(
            master("m-n1", "10.0.0.1", "ALIVE"),
            master("m-n2", "10.0.0.2", "ALIVE"),
            master("m-n3", "10.0.0.3", "ALIVE")));

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.SAFE, sweep.outcome());
    verify(client, times(3)).areNodesSafeToTakeDown(any(), any(), anyLong());
  }

  @Test
  public void testLegacyDualNicNodeIsCheckedAtItsRegisteredAddress() throws Exception {
    universe =
        update(universe, d -> nodeByName(d, "n1").cloudInfo.secondary_private_ip = "192.168.0.1");
    mockServers(
        List.of(
            tserver("ts-n1", "192.168.0.1"),
            tserver("ts-n2", "10.0.0.2"),
            tserver("ts-n3", "10.0.0.3")),
        List.of("ts-n1", "ts-n2", "ts-n3"),
        List.of(
            master("m-n1", "192.168.0.1", "ALIVE"),
            master("m-n2", "10.0.0.2", "ALIVE"),
            master("m-n3", "10.0.0.3", "ALIVE")));

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.SAFE, sweep.outcome());
    assertEquals("192.168.0.1", sweep.nodes().get(0).address());
    verify(client).areNodesSafeToTakeDown(Set.of("192.168.0.1"), Set.of("192.168.0.1"), LAG_MS);
  }

  @Test
  public void testSweepStopsAtFirstUnsafeNode() throws Exception {
    long before = histogramCount("master_tserver", Result.UNSAFE);
    MasterErrorException unsafe = masterError("2 tablet(s) would not have enough replicas");
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong()))
        .thenReturn(new AreNodesSafeToTakeDownResponse(null))
        .thenThrow(unsafe)
        .thenReturn(new AreNodesSafeToTakeDownResponse(null));

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.UNSAFE, sweep.outcome());
    assertEquals(
        List.of(NodeStatus.SAFE, NodeStatus.UNSAFE, NodeStatus.NOT_CHECKED), statuses(sweep));
    assertTrue(sweep.message().contains("n2"));
    assertTrue(sweep.message().contains("2 tablet(s) would not have enough replicas"));
    verify(client, times(2)).areNodesSafeToTakeDown(any(), any(), anyLong());
    assertEquals(before + 1, histogramCount("master_tserver", Result.UNSAFE));
  }

  @Test
  public void testTimeoutIsErrorAfterOneCall() throws Exception {
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong()))
        .thenThrow(new TimeoutException("deadline exceeded"));

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.ERROR, sweep.outcome());
    assertEquals(
        List.of(NodeStatus.ERROR, NodeStatus.NOT_CHECKED, NodeStatus.NOT_CHECKED), statuses(sweep));
    verify(client, times(1)).areNodesSafeToTakeDown(any(), any(), anyLong());
  }

  @Test
  public void testListErrorsAreErrors() throws Exception {
    when(client.listMasters()).thenThrow(new RuntimeException("no leader"));

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.ERROR, sweep.outcome());
    assertTrue(sweep.message().contains("no leader"));
    assertEquals(3, sweep.nodes().size());
    verify(client, never()).areNodesSafeToTakeDown(any(), any(), anyLong());
    verify(client).close();

    ListTabletServersResponse failed = mock(ListTabletServersResponse.class);
    when(failed.hasError()).thenReturn(true);
    when(client.listTabletServers()).thenReturn(failed);
    assertEquals(SweepOutcome.ERROR, checker.sweep(customer, universe).outcome());
  }

  @Test
  public void testSweepIsDiscardedWhenATaskStarts() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong()))
        .thenAnswer(
            invocation -> {
              if (calls.incrementAndGet() == 1) {
                update(
                    universe,
                    d -> {
                      d.updateInProgress = true;
                      d.updatingTask = TaskType.SoftwareUpgrade;
                    });
              }
              return new AreNodesSafeToTakeDownResponse(null);
            });

    assertNull(checker.sweep(customer, universe));
    assertEquals(1, calls.get());
  }

  @Test
  public void testTaskStartingDuringEvaluationCachesNothing() throws Exception {
    storeHealthCheck(universe, false);
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong()))
        .thenAnswer(
            invocation -> {
              update(
                  universe,
                  d -> {
                    d.updateInProgress = true;
                    d.updatingTask = TaskType.SoftwareUpgrade;
                  });
              return new AreNodesSafeToTakeDownResponse(null);
            });

    checker.evaluate(universe.getUniverseUUID());

    verify(client, times(1)).areNodesSafeToTakeDown(any(), any(), anyLong());
    assertNull(checker.getSweep(universe.getUniverseUUID()));
    assertNoGauge(universe);
    assertEquals(
        clock.instant().plus(UniverseClusterHealthChecker.TICK_INTERVAL),
        checker.nextEvalAt.get(universe.getUniverseUUID()));
  }

  @Test
  public void testNullClientIsError() {
    when(ybClientService.getUniverseClient(any())).thenReturn(null);

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.ERROR, sweep.outcome());
    assertNotNull(sweep.message());
  }

  @Test
  public void testReadReplicaNodesAreNeverChecked() throws Exception {
    NodeDetails readReplica = node("rr1", "10.0.0.9", false, true);
    universe =
        update(
            universe,
            d -> {
              Cluster rr = new Cluster(ClusterType.ASYNC, d.getPrimaryCluster().userIntent.clone());
              d.clusters.add(rr);
              readReplica.placementUuid = rr.uuid;
              d.nodeDetailsSet.add(readReplica);
            });
    mockRegisteredServers(universe);

    SweepResult sweep = checker.sweep(customer, universe);

    assertEquals(SweepOutcome.SAFE, sweep.outcome());
    assertEquals(3, sweep.nodes().size());
    verify(client, times(3)).areNodesSafeToTakeDown(any(), any(), anyLong());
    verify(client, never())
        .areNodesSafeToTakeDown(any(), argThat(ips -> ips.contains("10.0.0.9")), anyLong());
  }

  // Evaluation and the gauge.

  @Test
  public void testHealthyGauge() throws Exception {
    storeHealthCheck(universe, false);

    checker.evaluate(universe.getUniverseUUID());

    assertGauge(universe, 1);
    Metric gauge = gauge(universe).get(0);
    assertEquals(
        universe.getUniverseUUID().toString(), gauge.getLabelValue(KnownAlertLabels.UNIVERSE_UUID));
    assertEquals(universe.getName(), gauge.getLabelValue(KnownAlertLabels.UNIVERSE_NAME));
    assertEquals(
        customer.getUuid().toString(), gauge.getLabelValue(KnownAlertLabels.CUSTOMER_UUID));
    assertEquals(universe.getUniverseUUID(), gauge.getSourceUuid());
    // Three default evaluation intervals of 5 minutes.
    long expiresInSec =
        Duration.between(Instant.now(), gauge.getExpireTime().toInstant()).getSeconds();
    assertTrue("expires in " + expiresInSec, expiresInSec > 14 * 60 && expiresInSec <= 15 * 60);
    assertNotNull(checker.nextEvalAt.get(universe.getUniverseUUID()));
  }

  @Test
  public void testUnsafeSweepGivesZero() throws Exception {
    storeHealthCheck(universe, false);
    MasterErrorException unsafe = masterError("not enough replicas");
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong())).thenThrow(unsafe);

    checker.evaluate(universe.getUniverseUUID());

    assertGauge(universe, 0);
    ClusterHealth health = checker.getClusterHealth(customer, universe, clock.instant());
    assertEquals(Status.UNHEALTHY, health.status());
    assertEquals(Reason.NODES_NOT_SAFE_TO_TAKE_DOWN, health.reasons().get(0).code());
  }

  @Test
  public void testPausedUniverseMakesNoRpcAndHasNoGauge() {
    storeHealthCheck(universe, false);
    seedGaugeAndSweep(universe);
    universe = update(universe, d -> d.universePaused = true);

    checker.evaluate(universe.getUniverseUUID());

    verifyNoInteractions(ybClientService);
    assertNoGauge(universe);
    assertNull(checker.getSweep(universe.getUniverseUUID()));
  }

  @Test
  public void testNotCreatedUniverseMakesNoRpcAndHasNoGauge() {
    storeHealthCheck(universe, false);
    seedGaugeAndSweep(universe);
    universe = update(universe, d -> d.creationSucceeded = false);

    checker.evaluate(universe.getUniverseUUID());

    verifyNoInteractions(ybClientService);
    assertNoGauge(universe);
  }

  @Test
  public void testDisabledUniverseMakesNoRpcAndHasNoGauge() {
    storeHealthCheck(universe, false);
    seedGaugeAndSweep(universe);
    mutableConfigFactory
        .forUniverse(universe)
        .setValue(UniverseConfKeys.clusterHealthEnabled.getKey(), "false");

    checker.evaluate(universe.getUniverseUUID());

    verifyNoInteractions(ybClientService);
    assertNoGauge(universe);
    assertEquals(
        Reason.DISABLED,
        checker.getClusterHealth(customer, universe, clock.instant()).reasons().get(0).code());
  }

  @Test
  public void testBusyUniverseDropsItsSweepAndComesBackNextTick() {
    storeHealthCheck(universe, false);
    seedGaugeAndSweep(universe);
    universe =
        update(
            universe,
            d -> {
              d.updateInProgress = true;
              d.updatingTask = TaskType.SoftwareUpgrade;
            });

    checker.evaluate(universe.getUniverseUUID());

    verifyNoInteractions(ybClientService);
    assertNoGauge(universe);
    assertNull(checker.getSweep(universe.getUniverseUUID()));
    assertEquals(
        clock.instant().plus(UniverseClusterHealthChecker.TICK_INTERVAL),
        checker.nextEvalAt.get(universe.getUniverseUUID()));
  }

  @Test
  public void testEndOfALongTaskPublishesNoZero() {
    storeHealthCheck(universe, false);
    universe =
        update(
            universe,
            d -> {
              d.updateInProgress = true;
              d.updatingTask = TaskType.SoftwareUpgrade;
            });
    checker.evaluate(universe.getUniverseUUID());
    clock.advance(Duration.ofMinutes(29));
    checker.evaluate(universe.getUniverseUUID());
    universe =
        update(
            universe,
            d -> {
              d.updateInProgress = false;
              d.updatingTask = null;
            });
    clock.advance(Duration.ofMinutes(1));

    // The stored health check predates the task by 30 minutes.
    checker.evaluate(universe.getUniverseUUID());

    assertNoGauge(universe);
    ClusterHealth health = checker.getClusterHealth(customer, universe, clock.instant());
    assertEquals(Status.UNKNOWN, health.status());
    assertEquals(
        List.of(Reason.HEALTH_CHECK_PENDING),
        health.reasons().stream().map(Reason::code).collect(Collectors.toList()));
    assertEquals(SweepOutcome.SAFE, checker.getSweep(universe.getUniverseUUID()).outcome());

    // Still nothing stored a max age after the task: the health check is stale.
    clock.advance(Duration.ofMinutes(15));
    checker.evaluate(universe.getUniverseUUID());
    assertGauge(universe, 0);
    assertEquals(
        Reason.HEALTH_CHECK_STALE,
        checker.getClusterHealth(customer, universe, clock.instant()).reasons().get(0).code());
  }

  @Test
  public void testFirstEvaluationAfterStartToleratesAnOldHealthCheck() {
    storeHealthCheck(universe, false);
    clock.advance(Duration.ofMinutes(20));

    checker.evaluate(universe.getUniverseUUID());

    assertNoGauge(universe);
    assertEquals(
        Reason.HEALTH_CHECK_PENDING,
        checker.getClusterHealth(customer, universe, clock.instant()).reasons().get(0).code());

    clock.advance(Duration.ofMinutes(16));
    checker.evaluate(universe.getUniverseUUID());
    assertGauge(universe, 0);
  }

  @Test
  public void testBackupIsNotATask() throws Exception {
    storeHealthCheck(universe, false);
    universe =
        update(
            universe,
            d -> {
              d.updateInProgress = true;
              d.updatingTask = TaskType.CreateBackup;
            });

    checker.evaluate(universe.getUniverseUUID());

    verify(client, times(3)).areNodesSafeToTakeDown(any(), any(), anyLong());
    assertGauge(universe, 1);
  }

  @Test
  public void testNotApplicableUniversesMakeNoRpc() {
    Universe rf1 = newUniverse("rf1", 1, vmNodes(1));
    Universe oldVersion = newUniverse("old", 3, vmNodes(3));
    oldVersion =
        update(oldVersion, d -> d.getPrimaryCluster().userIntent.ybSoftwareVersion = "2.18.0.0-b1");
    Universe sttdOff = newUniverse("sttd-off", 3, vmNodes(3));
    mutableConfigFactory
        .forUniverse(sttdOff)
        .setValue(UniverseConfKeys.clusterHealthSafeToTakeDownEnabled.getKey(), "false");

    for (Universe u : List.of(rf1, oldVersion, sttdOff)) {
      checker.evaluate(u.getUniverseUUID());
      // No health check stored yet: warm-up, no series.
      assertNoGauge(u);
    }
    verifyNoInteractions(ybClientService);

    // The health check alone decides once it is stored.
    storeHealthCheck(rf1, false);
    checker.evaluate(rf1.getUniverseUUID());
    assertGauge(rf1, 1);
    verifyNoInteractions(ybClientService);
  }

  @Test
  public void testStoppedNodeIsUnhealthyWithoutRpc() {
    storeHealthCheck(universe, false);
    // As StopNodeInUniverse leaves it.
    universe =
        update(
            universe,
            d -> {
              NodeDetails node = nodeByName(d, "n2");
              node.state = NodeState.Stopped;
              node.isMaster = false;
              node.isTserver = false;
            });

    checker.evaluate(universe.getUniverseUUID());

    verifyNoInteractions(ybClientService);
    assertGauge(universe, 0);
    ClusterHealth health = checker.getClusterHealth(customer, universe, clock.instant());
    assertEquals(Status.UNHEALTHY, health.status());
    assertEquals(Reason.NODE_NOT_LIVE, health.reasons().get(0).code());
  }

  @Test
  public void testSweepIsCachedForItsInterval() throws Exception {
    storeHealthCheck(universe, false);

    checker.evaluate(universe.getUniverseUUID());
    checker.evaluate(universe.getUniverseUUID());

    verify(client, times(1)).listTabletServers();
    verify(client, times(3)).areNodesSafeToTakeDown(any(), any(), anyLong());
    verify(metrics, times(2)).cleanAndSave(any(), any());
    assertGauge(universe, 1);

    clock.advance(Duration.ofMinutes(15));
    checker.evaluate(universe.getUniverseUUID());

    verify(client, times(2)).listTabletServers();
    verify(client, times(6)).areNodesSafeToTakeDown(any(), any(), anyLong());
  }

  @Test
  public void testErrorIsRetriedAtTheNextEvaluation() throws Exception {
    storeHealthCheck(universe, false);
    when(client.listTabletServers()).thenThrow(new RuntimeException("no leader"));

    checker.evaluate(universe.getUniverseUUID());
    assertEquals(SweepOutcome.ERROR, checker.getSweep(universe.getUniverseUUID()).outcome());
    assertGauge(universe, 0);

    checker.evaluate(universe.getUniverseUUID());
    verify(client, times(2)).listTabletServers();
  }

  @Test
  public void testGaugeIsWrittenForAnInactiveSource() {
    storeHealthCheck(universe, false);
    // A resume runs markSourceActive and then falls through to markSourceInactive.
    metricService.markSourceActive(customer.getUuid(), universe.getUniverseUUID());
    metricService.markSourceInactive(customer.getUuid(), universe.getUniverseUUID());

    checker.evaluate(universe.getUniverseUUID());

    assertGauge(universe, 1);
  }

  @Test
  public void testLateWriteAfterRemovalIsDropped() {
    storeHealthCheck(universe, false);
    metricService.markSourceRemoved(customer.getUuid(), universe.getUniverseUUID());

    checker.evaluate(universe.getUniverseUUID());

    assertNoGauge(universe);
  }

  @Test
  public void testStandbyMakesNoRpc() {
    storeHealthCheck(universe, false);
    try (MockedStatic<HighAvailabilityConfig> ha = mockStatic(HighAvailabilityConfig.class)) {
      ha.when(HighAvailabilityConfig::isFollower).thenReturn(true);

      checker.evaluate(universe.getUniverseUUID());

      assertEquals(
          Reason.YBA_STANDBY,
          checker.getClusterHealth(customer, universe, clock.instant()).reasons().get(0).code());
    }
    verifyNoInteractions(ybClientService);
    assertNull(checker.nextEvalAt.get(universe.getUniverseUUID()));
    assertNoGauge(universe);
  }

  @Test
  public void testGetClusterHealthMakesNoRpc() {
    storeHealthCheck(universe, false);
    checker.putSweep(universe.getUniverseUUID(), safeSweep(clock.instant()));

    ClusterHealth health = checker.getClusterHealth(customer, universe, clock.instant());

    assertEquals(Status.HEALTHY, health.status());
    assertNotNull(health.healthCheck().snapshot().checkTime());
    verifyNoInteractions(ybClientService);
  }

  // processAll.

  @Test
  public void testProcessAllSkipsRunningAndNotDueUniverses() throws Exception {
    Universe running = newUniverse("running", 3, vmNodes(3));
    Universe notDue = newUniverse("not-due", 3, vmNodes(3));
    checker.inFlight.put(running.getUniverseUUID(), new CompletableFuture<>());
    Instant later = clock.instant().plus(Duration.ofMinutes(5));
    checker.nextEvalAt.put(notDue.getUniverseUUID(), later);

    checker.processAll();

    assertNull(checker.nextEvalAt.get(running.getUniverseUUID()));
    assertEquals(later, checker.nextEvalAt.get(notDue.getUniverseUUID()));
    assertNotNull(checker.nextEvalAt.get(universe.getUniverseUUID()));
    verify(ybClientService, times(1)).getUniverseClient(any());
    verify(ybClientService, never())
        .getUniverseClient(argThat(u -> !u.getUniverseUUID().equals(universe.getUniverseUUID())));
  }

  @Test
  public void testProcessAllEvaluatesWithinHalfATickOfTheDueTime() {
    Instant almostDue = clock.instant().plus(Duration.ofSeconds(10));
    checker.nextEvalAt.put(universe.getUniverseUUID(), almostDue);

    checker.processAll();

    assertTrue(checker.nextEvalAt.get(universe.getUniverseUUID()).isAfter(almostDue));
  }

  @Test
  public void testProcessAllForgetsDeletedUniverses() {
    UUID deleted = UUID.randomUUID();
    checker.inFlight.put(deleted, new CompletableFuture<>());
    checker.nextEvalAt.put(deleted, clock.instant());
    checker.putSweep(deleted, safeSweep(clock.instant()));

    checker.processAll();

    assertFalse(checker.inFlight.containsKey(deleted));
    assertFalse(checker.nextEvalAt.containsKey(deleted));
    assertNull(checker.getSweep(deleted));
  }

  @Test
  public void testProcessAllDoesNothingBeforeStart() {
    UniverseClusterHealthChecker notStarted =
        new UniverseClusterHealthChecker(
            app.injector().instanceOf(RuntimeConfGetter.class),
            app.injector().instanceOf(Config.class),
            mock(PlatformScheduler.class),
            mock(PlatformExecutorFactory.class),
            ybClientService,
            metrics);

    notStarted.processAll();

    assertTrue(notStarted.nextEvalAt.isEmpty());
    verify(ybClientService, never()).getUniverseClient(any());
    verify(metrics, never()).cleanAndSave(any(), any());
  }
}
