// Copyright (c) YugabyteDB, Inc.

package com.yugabyte.yw.commissioner;

import static com.yugabyte.yw.common.metrics.MetricService.buildMetricTemplate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import com.yugabyte.yw.commissioner.ClusterHealth.HealthCheckSnapshot;
import com.yugabyte.yw.commissioner.ClusterHealth.NodeResult;
import com.yugabyte.yw.commissioner.ClusterHealth.NodeStatus;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepOutcome;
import com.yugabyte.yw.commissioner.ClusterHealth.SweepResult;
import com.yugabyte.yw.commissioner.ClusterHealthRules.Facts;
import com.yugabyte.yw.commissioner.ClusterHealthRules.Settings;
import com.yugabyte.yw.commissioner.tasks.subtasks.CheckNodesAreSafeToTakeDown;
import com.yugabyte.yw.common.NodesSafeToTakeDownUtil;
import com.yugabyte.yw.common.NodesSafeToTakeDownUtil.Caller;
import com.yugabyte.yw.common.NodesSafeToTakeDownUtil.Outcome;
import com.yugabyte.yw.common.PlatformExecutorFactory;
import com.yugabyte.yw.common.PlatformScheduler;
import com.yugabyte.yw.common.Util;
import com.yugabyte.yw.common.config.CustomerConfKeys;
import com.yugabyte.yw.common.config.GlobalConfKeys;
import com.yugabyte.yw.common.config.RuntimeConfGetter;
import com.yugabyte.yw.common.config.UniverseConfKeys;
import com.yugabyte.yw.common.metrics.MetricService;
import com.yugabyte.yw.common.services.YBClientService;
import com.yugabyte.yw.forms.AlertingData;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams;
import com.yugabyte.yw.forms.UniverseDefinitionTaskParams.Cluster;
import com.yugabyte.yw.models.Customer;
import com.yugabyte.yw.models.HealthCheck;
import com.yugabyte.yw.models.HighAvailabilityConfig;
import com.yugabyte.yw.models.Universe;
import com.yugabyte.yw.models.configs.CustomerConfig;
import com.yugabyte.yw.models.filters.MetricFilter;
import com.yugabyte.yw.models.helpers.NodeDetails;
import com.yugabyte.yw.models.helpers.PlatformMetrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.yb.client.ListLiveTabletServersResponse;
import org.yb.client.ListMastersResponse;
import org.yb.client.ListTabletServersResponse;
import org.yb.client.YBClientApi;
import org.yb.util.ServerInfo;
import org.yb.util.TabletServerInfo;
import play.libs.Json;

/**
 * Evaluates each universe's {@link ClusterHealth} in the background and publishes it as {@link
 * PlatformMetrics#UNIVERSE_CLUSTER_HEALTH_STATUS}. The heavy AreNodesSafeToTakeDown sweep runs on
 * its own pool, rarely, one node at a time, and stops at the first node that is not safe: every
 * call holds a master leader RPC thread, and a call with an unresponsive peer takes ~28s and leaves
 * retrying tasks on the master.
 *
 * <p>Sweep results live in memory only: after a restart a universe is UNKNOWN until its first
 * sweep.
 */
@Slf4j
@Singleton
public class UniverseClusterHealthChecker {

  static final Duration TICK_INTERVAL = Duration.ofMinutes(1);
  static final Duration INITIAL_DELAY = Duration.ofMinutes(2);
  static final Duration MAX_JITTER = Duration.ofSeconds(30);

  // Evaluations and sweeps start on a tick, after the time recorded for the previous one. Without
  // this slack an interval equal to the tick would be honoured only every second tick.
  static final Duration DUE_SLACK = TICK_INTERVAL.dividedBy(2);

  private static final String MASTER_ALIVE = "ALIVE";

  // Outcomes that say nothing about the tablets. NOT_ALIVE and UNRESOLVED made no
  // AreNodesSafeToTakeDown call and ERROR stopped at its first failed one, so retrying them at
  // every evaluation adds at most one call per evaluation.
  private static final Set<SweepOutcome> RETRY_EVERY_EVALUATION =
      EnumSet.of(SweepOutcome.ERROR, SweepOutcome.UNRESOLVED, SweepOutcome.NOT_ALIVE);

  private final RuntimeConfGetter confGetter;
  private final Config config;
  private final PlatformScheduler platformScheduler;
  private final PlatformExecutorFactory executorFactory;
  private final YBClientService ybClientService;
  private final MetricService metricService;
  private final Clock clock;

  // Created in start(), so that injecting this class (the API does) starts no threads.
  private volatile ExecutorService executor;

  @VisibleForTesting final Map<UUID, Future<?>> inFlight = new ConcurrentHashMap<>();
  @VisibleForTesting final Map<UUID, Instant> nextEvalAt = new ConcurrentHashMap<>();
  private final Map<UUID, SweepResult> sweeps = new ConcurrentHashMap<>();
  // See Facts.healthCheckBlockedAt.
  private final Map<UUID, Instant> healthCheckBlockedAt = new ConcurrentHashMap<>();

  @Inject
  public UniverseClusterHealthChecker(
      RuntimeConfGetter confGetter,
      Config config,
      PlatformScheduler platformScheduler,
      PlatformExecutorFactory executorFactory,
      YBClientService ybClientService,
      MetricService metricService) {
    this(
        confGetter,
        config,
        platformScheduler,
        executorFactory,
        ybClientService,
        metricService,
        null,
        Clock.systemUTC());
  }

  @VisibleForTesting
  UniverseClusterHealthChecker(
      RuntimeConfGetter confGetter,
      Config config,
      PlatformScheduler platformScheduler,
      PlatformExecutorFactory executorFactory,
      YBClientService ybClientService,
      MetricService metricService,
      ExecutorService executor,
      Clock clock) {
    this.confGetter = confGetter;
    this.config = config;
    this.platformScheduler = platformScheduler;
    this.executorFactory = executorFactory;
    this.ybClientService = ybClientService;
    this.metricService = metricService;
    this.executor = executor;
    this.clock = clock;
  }

  public synchronized void start() {
    if (executor == null) {
      executor =
          executorFactory.createFixedExecutor(
              "Cluster-Health-Pool",
              confGetter.getGlobalConf(GlobalConfKeys.clusterHealthParallelism),
              new ThreadFactoryBuilder().setNameFormat("Cluster-Health-Pool-%d").build());
    }
    // schedule(), not scheduleAlwaysOn(): nothing is evaluated on an HA standby.
    platformScheduler.schedule(
        getClass().getSimpleName(), INITIAL_DELAY, TICK_INTERVAL, this::processAll);
  }

  /**
   * The universe's cluster health as of {@code now}, from the latest stored health check and the
   * cached sweep. Makes no RPC and writes nothing.
   */
  public ClusterHealth getClusterHealth(Customer customer, Universe universe, Instant now) {
    return ClusterHealthRules.compose(
        buildFacts(
            universe,
            HighAvailabilityConfig.isFollower(),
            healthCheckBlockedAt.get(universe.getUniverseUUID())),
        readHealthCheck(universe.getUniverseUUID()),
        sweeps.get(universe.getUniverseUUID()),
        buildSettings(customer, universe),
        now);
  }

  @Nullable
  public SweepResult getSweep(UUID universeUuid) {
    return sweeps.get(universeUuid);
  }

  @VisibleForTesting
  public void putSweep(UUID universeUuid, SweepResult sweep) {
    sweeps.put(universeUuid, sweep);
  }

  /** Submits every due universe. Makes no RPC, so it never holds up the scheduler. */
  @VisibleForTesting
  void processAll() {
    ExecutorService pool = executor;
    if (pool == null) {
      return;
    }
    try {
      Set<UUID> universeUuids =
          Universe.getSummaries().stream()
              .map(Universe.Summary::universeUUID)
              .collect(Collectors.toSet());
      inFlight.keySet().retainAll(universeUuids);
      nextEvalAt.keySet().retainAll(universeUuids);
      sweeps.keySet().retainAll(universeUuids);
      healthCheckBlockedAt.keySet().retainAll(universeUuids);
      Instant now = clock.instant();
      for (UUID universeUuid : universeUuids) {
        Future<?> running = inFlight.get(universeUuid);
        if (running != null && !running.isDone()) {
          continue;
        }
        Instant next = nextEvalAt.get(universeUuid);
        if (next != null && now.isBefore(next.minus(DUE_SLACK))) {
          continue;
        }
        try {
          inFlight.put(
              universeUuid,
              pool.submit(
                  () -> {
                    try {
                      evaluate(universeUuid);
                    } finally {
                      inFlight.remove(universeUuid);
                    }
                  }));
        } catch (RuntimeException e) {
          log.error("Failed to submit cluster health evaluation for universe {}", universeUuid, e);
        }
      }
    } catch (Exception e) {
      log.error("Failed to process cluster health evaluations", e);
    }
  }

  @VisibleForTesting
  void evaluate(UUID universeUuid) {
    try {
      // The tick skips a standby, but this instance may have been demoted since.
      if (HighAvailabilityConfig.isFollower()) {
        return;
      }
      Universe universe = Universe.maybeGet(universeUuid).orElse(null);
      if (universe == null) {
        return;
      }
      Customer customer = Customer.get(universe.getCustomerId());
      if (customer == null) {
        return;
      }
      Instant now = clock.instant();
      Settings settings = buildSettings(customer, universe);
      Duration evaluationInterval = settings.evaluationInterval();
      Instant next = now.plus(evaluationInterval).plus(jitter(evaluationInterval));
      if (nextEvalAt.put(universeUuid, next) == null) {
        // This instance has not evaluated the universe since it started, and its HealthChecker
        // may not have stored a result yet either.
        healthCheckBlockedAt.put(universeUuid, now);
      }

      Facts facts = buildFacts(universe, false, healthCheckBlockedAt.get(universeUuid));
      if (ClusterHealthRules.gate(facts, settings) != null) {
        dropState(universeUuid, facts, now);
        return;
      }
      if (facts.notLiveNodes().isEmpty()
          && ClusterHealthRules.isSafeToTakeDownApplicable(facts, settings)
          && isSweepDue(sweeps.get(universeUuid), settings.sttdInterval(), now)) {
        SweepResult sweep = sweep(customer, universe);
        if (sweep == null) {
          sweeps.remove(universeUuid);
        } else {
          sweeps.put(universeUuid, sweep);
        }
        // A sweep can take minutes: compose from the universe as it is now.
        universe = Universe.maybeGet(universeUuid).orElse(null);
        if (universe == null) {
          return;
        }
        now = clock.instant();
        facts = buildFacts(universe, false, healthCheckBlockedAt.get(universeUuid));
        if (ClusterHealthRules.gate(facts, settings) != null) {
          dropState(universeUuid, facts, now);
          return;
        }
      }
      ClusterHealth health =
          ClusterHealthRules.compose(
              facts, readHealthCheck(universeUuid), sweeps.get(universeUuid), settings, now);
      publish(customer, universe, health, evaluationInterval);
    } catch (Exception e) {
      // Nothing is published: the gauge expires on its own if this keeps failing.
      log.error("Cluster health evaluation failed for universe {}", universeUuid, e);
    }
  }

  /**
   * Checks every primary-cluster node, one at a time, and stops at the first one that is not safe.
   * Returns null when a task started on the universe during the sweep, as that result says nothing
   * about the universe outside the task.
   */
  @VisibleForTesting
  @Nullable
  SweepResult sweep(Customer customer, Universe universe) {
    UUID universeUuid = universe.getUniverseUUID();
    Instant startedAt = clock.instant();
    long startNanos = System.nanoTime();
    boolean cloudEnabled = confGetter.getConfForScope(customer, CustomerConfKeys.cloudEnabled);
    long lagMs =
        confGetter.getConfForScope(universe, UniverseConfKeys.followerLagMaxThreshold).toMillis();
    List<NodeDetails> targets = sweepTargets(universe);
    NodeResult[] results = new NodeResult[targets.size()];
    SweepOutcome outcome;
    String message;
    YBClientApi client = null;
    try {
      client = ybClientService.getUniverseClient(universe);
      if (client == null) {
        outcome = SweepOutcome.ERROR;
        message = "No client: the universe has no master addresses";
      } else {
        String[] outcomeMessage = new String[1];
        outcome =
            checkNodes(client, universe, targets, cloudEnabled, lagMs, results, outcomeMessage);
        if (outcome == null) {
          log.info(
              "Cluster health sweep of universe {} discarded: a task started on it", universeUuid);
          return null;
        }
        message = outcomeMessage[0];
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      outcome = SweepOutcome.ERROR;
      message = "Failed to check the nodes: " + e;
    } finally {
      closeQuietly(client, universeUuid);
    }
    List<NodeResult> nodes = new ArrayList<>(targets.size());
    for (int i = 0; i < targets.size(); i++) {
      nodes.add(
          results[i] != null
              ? results[i]
              : nodeResult(targets.get(i), null, NodeStatus.NOT_CHECKED, null, 0));
    }
    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    log.info(
        "Cluster health sweep of universe {}: {} in {} ms{}; nodes: {}",
        universeUuid,
        outcome,
        durationMs,
        message == null ? "" : " (" + message + ")",
        nodes.stream()
            .map(n -> n.nodeName() + "=" + n.status() + "/" + n.durationMs() + "ms")
            .collect(Collectors.joining(", ")));
    return new SweepResult(
        startedAt, clock.instant(), durationMs, outcome, message, lagMs, List.copyOf(nodes));
  }

  /**
   * Fills {@code results} and returns the sweep outcome, or null when a task started on the
   * universe. {@code message[0]} receives the outcome's message.
   */
  @Nullable
  private SweepOutcome checkNodes(
      YBClientApi client,
      Universe universe,
      List<NodeDetails> targets,
      boolean cloudEnabled,
      long lagMs,
      NodeResult[] results,
      String[] message)
      throws Exception {
    if (targets.isEmpty()) {
      message[0] = "No primary-cluster node runs a master or a tserver";
      return SweepOutcome.UNRESOLVED;
    }
    // YBClient.areNodesSafeToTakeDown silently drops addresses that match no registered server,
    // and the master answers OK for an empty set. So every node is matched to a registered, live
    // server here first, and the call is given the host exactly as the master reports it.
    ListTabletServersResponse tservers = client.listTabletServers();
    if (tservers.hasError()) {
      message[0] = "ListTabletServers failed: " + tservers.errorMessage();
      return SweepOutcome.ERROR;
    }
    ListLiveTabletServersResponse liveTservers = client.listLiveTabletServers();
    if (liveTservers.hasError()) {
      message[0] = "ListLiveTabletServers failed: " + liveTservers.errorMessage();
      return SweepOutcome.ERROR;
    }
    ListMastersResponse masters = client.listMasters();
    if (masters.hasError()) {
      message[0] = "ListMasters failed";
      return SweepOutcome.ERROR;
    }
    // Descriptors of dead tservers are listed too, and a re-created pod can have two uuids.
    Map<String, Set<String>> tserverUuidsByHost = new HashMap<>();
    for (ServerInfo ts : tservers.getTabletServersList()) {
      tserverUuidsByHost.computeIfAbsent(ts.getHost(), h -> new HashSet<>()).add(ts.getUuid());
    }
    Set<String> liveTserverUuids =
        liveTservers.getTabletServers().stream()
            .map(TabletServerInfo::getPermanentUuid)
            .collect(Collectors.toSet());
    Map<String, List<ServerInfo>> mastersByHost =
        masters.getMasters().stream()
            .filter(m -> m.getHost() != null)
            .collect(Collectors.groupingBy(ServerInfo::getHost));

    String[] masterHosts = new String[targets.size()];
    String[] tserverHosts = new String[targets.size()];
    List<String> notAlive = new ArrayList<>();
    List<String> unresolved = new ArrayList<>();
    for (int i = 0; i < targets.size(); i++) {
      NodeDetails node = targets.get(i);
      Set<String> candidates = candidateHosts(universe, node, cloudEnabled);
      String masterHost = node.isMaster ? firstMatch(candidates, mastersByHost.keySet()) : null;
      String tserverHost =
          node.isTserver ? firstMatch(candidates, tserverUuidsByHost.keySet()) : null;
      masterHosts[i] = masterHost;
      tserverHosts[i] = tserverHost;
      String address =
          Stream.of(masterHost, tserverHost)
              .filter(StringUtils::isNotEmpty)
              .findFirst()
              .orElse(candidates.stream().findFirst().orElse(null));
      List<String> problems = new ArrayList<>();
      if (node.isMaster && masterHost == null) {
        problems.add("no registered master at " + candidates);
      }
      if (node.isTserver && tserverHost == null) {
        problems.add("no registered tserver at " + candidates);
      }
      if (!problems.isEmpty()) {
        unresolved.add(node.nodeName);
        results[i] =
            nodeResult(node, address, NodeStatus.UNRESOLVED, String.join("; ", problems), 0);
        continue;
      }
      if (masterHost != null
          && mastersByHost.get(masterHost).stream()
              .noneMatch(m -> MASTER_ALIVE.equals(m.getState()))) {
        problems.add(
            "master is not alive: "
                + mastersByHost.get(masterHost).stream()
                    .map(ServerInfo::getState)
                    .collect(Collectors.joining(",")));
      }
      if (tserverHost != null
          && tserverUuidsByHost.get(tserverHost).stream().noneMatch(liveTserverUuids::contains)) {
        problems.add("tserver is not live");
      }
      if (!problems.isEmpty()) {
        notAlive.add(node.nodeName);
        results[i] =
            nodeResult(node, address, NodeStatus.NOT_ALIVE, String.join("; ", problems), 0);
      }
    }
    if (!notAlive.isEmpty()) {
      message[0] = "Not alive according to the master leader: " + String.join(", ", notAlive);
      return SweepOutcome.NOT_ALIVE;
    }
    if (!unresolved.isEmpty()) {
      message[0] =
          "Not registered with the master leader at their addresses: "
              + String.join(", ", unresolved);
      return SweepOutcome.UNRESOLVED;
    }

    for (int i = 0; i < targets.size(); i++) {
      NodeDetails node = targets.get(i);
      if (Universe.isUniverseBusyByTask(universe.getUniverseUUID()).orElse(true)) {
        return null;
      }
      Outcome o =
          NodesSafeToTakeDownUtil.check(
              client,
              masterHosts[i] != null ? Set.of(masterHosts[i]) : Set.of(),
              tserverHosts[i] != null ? Set.of(tserverHosts[i]) : Set.of(),
              lagMs,
              Caller.CLUSTER_HEALTH);
      String address = masterHosts[i] != null ? masterHosts[i] : tserverHosts[i];
      String detail =
          o.result() == NodesSafeToTakeDownUtil.Result.ERROR && o.error() != null
              ? o.error().toString()
              : o.message();
      switch (o.result()) {
        case SAFE:
          results[i] = nodeResult(node, address, NodeStatus.SAFE, null, o.durationMs());
          continue;
        case UNSAFE:
          results[i] = nodeResult(node, address, NodeStatus.UNSAFE, detail, o.durationMs());
          message[0] = node.nodeName + ": " + detail;
          return SweepOutcome.UNSAFE;
        case UNSUPPORTED:
          results[i] = nodeResult(node, address, NodeStatus.UNSUPPORTED, detail, o.durationMs());
          message[0] = "The DB does not support AreNodesSafeToTakeDown: " + detail;
          return SweepOutcome.UNSUPPORTED;
        case ERROR:
        default:
          results[i] = nodeResult(node, address, NodeStatus.ERROR, detail, o.durationMs());
          message[0] = node.nodeName + ": " + detail;
          return SweepOutcome.ERROR;
      }
    }
    return SweepOutcome.SAFE;
  }

  /** Primary-cluster nodes with a process, by name. Read replicas are skipped, as tasks do. */
  private static List<NodeDetails> sweepTargets(Universe universe) {
    Collection<NodeDetails> nodes =
        universe.getNodesInCluster(universe.getUniverseDetails().getPrimaryCluster().uuid);
    if (nodes == null) {
      return List.of();
    }
    return nodes.stream()
        .filter(n -> n.isMaster || n.isTserver)
        .sorted(Comparator.comparing(n -> n.nodeName, Comparator.nullsLast(String::compareTo)))
        .toList();
  }

  /**
   * The address YBA uses for the node, then its private and secondary IPs: a legacy dual-NIC
   * tserver registers with its secondary IP while YBA addresses it by its private one.
   */
  private static Set<String> candidateHosts(
      Universe universe, NodeDetails node, boolean cloudEnabled) {
    Set<String> candidates = new LinkedHashSet<>();
    if (node.cloudInfo == null) {
      return candidates;
    }
    String ipToUse = Util.getIpToUse(universe, node, cloudEnabled);
    if (StringUtils.isNotBlank(ipToUse)) {
      candidates.add(ipToUse);
    }
    Set<String> nodeIps = Util.getNodeIps(node);
    Stream.of(node.cloudInfo.private_ip, node.cloudInfo.secondary_private_ip)
        .filter(nodeIps::contains)
        .forEach(candidates::add);
    return candidates;
  }

  @Nullable
  private static String firstMatch(Set<String> candidates, Set<String> hosts) {
    return candidates.stream().filter(hosts::contains).findFirst().orElse(null);
  }

  private static NodeResult nodeResult(
      NodeDetails node, String address, NodeStatus status, String message, long durationMs) {
    return new NodeResult(
        node.nodeName,
        node.nodeUuid,
        node.isMaster,
        node.isTserver,
        address != null ? address : (node.cloudInfo != null ? node.cloudInfo.private_ip : null),
        status,
        message,
        durationMs);
  }

  private static boolean isSweepDue(@Nullable SweepResult last, Duration interval, Instant now) {
    return last == null
        || RETRY_EVERY_EVALUATION.contains(last.outcome())
        || !now.isBefore(last.finishedAt().plus(interval).minus(DUE_SLACK));
  }

  /** Forgets the universe's sweep and removes its gauge: a gate makes both meaningless. */
  private void dropState(UUID universeUuid, Facts facts, Instant now) {
    sweeps.remove(universeUuid);
    metricService.delete(gaugeFilter(universeUuid));
    if (!facts.hasDetails() || !facts.creationSucceeded() || facts.paused() || facts.busyByTask()) {
      // HealthChecker stores no result in these states either.
      healthCheckBlockedAt.put(universeUuid, now);
    }
    if (facts.busyByTask()) {
      // Come back at the next tick, so that the status returns soon after the task ends.
      nextEvalAt.put(universeUuid, now.plus(TICK_INTERVAL));
    }
  }

  private void publish(
      Customer customer, Universe universe, ClusterHealth health, Duration evaluationInterval) {
    MetricFilter filter = gaugeFilter(universe.getUniverseUUID());
    OptionalDouble value = ClusterHealthRules.metricValue(health);
    if (value.isEmpty()) {
      metricService.delete(filter);
      return;
    }
    // Expires on its own when the checker stops rewriting it (stalled, demoted, disabled).
    long expirySec = 3 * Math.max(evaluationInterval.toSeconds(), TICK_INTERVAL.toSeconds());
    metricService.cleanAndSave(
        List.of(
            buildMetricTemplate(
                    PlatformMetrics.UNIVERSE_CLUSTER_HEALTH_STATUS, customer, universe, expirySec)
                .setValue(value.getAsDouble())),
        filter);
  }

  @VisibleForTesting
  static MetricFilter gaugeFilter(UUID universeUuid) {
    return MetricFilter.builder()
        .metricName(PlatformMetrics.UNIVERSE_CLUSTER_HEALTH_STATUS)
        .sourceUuid(universeUuid)
        .build();
  }

  @VisibleForTesting
  static Facts buildFacts(
      Universe universe, boolean isFollower, @Nullable Instant healthCheckBlockedAt) {
    UUID universeUuid = universe.getUniverseUUID();
    UniverseDefinitionTaskParams details = universe.getUniverseDetails();
    Cluster primary = details == null ? null : details.getPrimaryCluster();
    if (primary == null || primary.userIntent == null) {
      return new Facts(
          universeUuid,
          isFollower,
          false,
          false,
          false,
          false,
          null,
          0,
          false,
          List.of(),
          healthCheckBlockedAt);
    }
    String version = primary.userIntent.ybSoftwareVersion;
    return new Facts(
        universeUuid,
        isFollower,
        true,
        details.creationSucceeded,
        details.universePaused,
        details.isUniverseBusyByTask(),
        details.updatingTask == null ? null : details.updatingTask.name(),
        primary.userIntent.replicationFactor,
        version != null && CheckNodesAreSafeToTakeDown.isApiSupported(version),
        ClusterHealthRules.notLiveNodes(details.getNodesInCluster(primary.uuid)),
        healthCheckBlockedAt);
  }

  private Settings buildSettings(Customer customer, Universe universe) {
    Duration maxAge =
        ClusterHealthRules.effectiveHealthCheckMaxAge(
            confGetter.getConfForScope(universe, UniverseConfKeys.clusterHealthHealthCheckMaxAge),
            config.getLong("yb.health.check_interval_ms"),
            effectiveStoreIntervalMs(customer));
    return new Settings(
        confGetter.getConfForScope(universe, UniverseConfKeys.clusterHealthEnabled),
        confGetter.getConfForScope(universe, UniverseConfKeys.clusterHealthEvaluationInterval),
        confGetter.getConfForScope(universe, UniverseConfKeys.clusterHealthSafeToTakeDownEnabled),
        confGetter.getConfForScope(universe, UniverseConfKeys.clusterHealthSafeToTakeDownInterval),
        maxAge);
  }

  /** How often HealthChecker persists a result for the customer, as HealthChecker decides it. */
  private long effectiveStoreIntervalMs(Customer customer) {
    long storeIntervalMs = config.getLong("yb.health.store_interval_ms");
    CustomerConfig alertConfig = CustomerConfig.getAlertConfig(customer.getUuid());
    if (alertConfig != null && alertConfig.getData() != null) {
      AlertingData alertingData = Json.fromJson(alertConfig.getData(), AlertingData.class);
      if (alertingData.checkIntervalMs > 0) {
        storeIntervalMs = alertingData.checkIntervalMs;
      }
    }
    return storeIntervalMs;
  }

  @Nullable
  private static HealthCheckSnapshot readHealthCheck(UUID universeUuid) {
    HealthCheck latest = HealthCheck.getLatest(universeUuid);
    if (latest == null || latest.getIdKey() == null || latest.getIdKey().checkTime == null) {
      return null;
    }
    return ClusterHealthRules.toSnapshot(
        latest.getIdKey().checkTime.toInstant(), latest.getDetailsJson());
  }

  private static Duration jitter(Duration interval) {
    long boundMs = Math.min(MAX_JITTER.toMillis(), interval.toMillis() / 10);
    return boundMs <= 0
        ? Duration.ZERO
        : Duration.ofMillis(ThreadLocalRandom.current().nextLong(boundMs + 1));
  }

  private static void closeQuietly(@Nullable YBClientApi client, UUID universeUuid) {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (Exception e) {
      log.warn("Failed to close the client of universe {}", universeUuid, e);
    }
  }
}
