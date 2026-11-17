// Copyright (c) YugabyteDB, Inc.

package com.yugabyte.yw.common;

import com.google.common.annotations.VisibleForTesting;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.yb.client.AreNodesSafeToTakeDownResponse;
import org.yb.client.MasterErrorException;
import org.yb.client.YBClientApi;

/**
 * The single place that calls {@link YBClientApi#areNodesSafeToTakeDown}: it classifies the result
 * the way the CheckNodesAreSafeToTakeDown subtask always has, and times every call into {@link
 * #CALL_DURATION_METRIC}.
 */
@Slf4j
public final class NodesSafeToTakeDownUtil {

  public static final String CALL_DURATION_METRIC =
      "ybp_nodes_safe_to_take_down_call_duration_seconds";

  public enum Caller {
    TASK("task"),
    CLUSTER_HEALTH("cluster_health");

    public final String label;

    Caller(String label) {
      this.label = label;
    }
  }

  public enum Result {
    SAFE,
    UNSAFE,
    UNSUPPORTED,
    ERROR;

    public String label() {
      return name().toLowerCase();
    }
  }

  /**
   * One call's result. {@code message} is the master's reason for UNSAFE and the exception message
   * for UNSUPPORTED and ERROR; {@code error} is set only for ERROR.
   */
  public record Outcome(Result result, String message, Exception error, long durationMs) {}

  private static final Histogram HISTOGRAM = buildHistogram();

  private NodesSafeToTakeDownUtil() {}

  private static Histogram buildHistogram() {
    // An unresponsive peer makes the master wait until the client deadline (~28s with the default
    // 30s socket read timeout), so the buckets go well past the 10s default.
    Histogram histogram =
        Histogram.builder()
            .name(CALL_DURATION_METRIC)
            .help(
                "Duration of one YBClientApi.areNodesSafeToTakeDown call (ListTabletServers +"
                    + " ListMasters + AreNodesSafeToTakeDown RPC)")
            .labelNames("caller", "target", "result")
            .classicOnly()
            .classicUpperBounds(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120)
            .build();
    // A failure here must not escape the static initializer: the resulting
    // ExceptionInInitializerError would fail every task that checks nodes before taking them down.
    try {
      PrometheusRegistry.defaultRegistry.register(histogram);
    } catch (RuntimeException e) {
      log.error("Failed to register {}; its values will not be exported", CALL_DURATION_METRIC, e);
    }
    return histogram;
  }

  @VisibleForTesting
  public static Histogram histogram() {
    return HISTOGRAM;
  }

  /** Runs one areNodesSafeToTakeDown call. Never throws. */
  public static Outcome check(
      YBClientApi client,
      Set<String> masterIps,
      Set<String> tserverIps,
      long followerLagBoundMs,
      Caller caller) {
    long startNanos = System.nanoTime();
    long durationNanos = 0;
    Result result = Result.ERROR;
    String message = null;
    Exception error = null;
    try {
      AreNodesSafeToTakeDownResponse resp =
          client.areNodesSafeToTakeDown(masterIps, tserverIps, followerLagBoundMs);
      if (resp.isSucessful()) {
        result = Result.SAFE;
      } else {
        result = Result.UNSAFE;
        message = resp.getErrorMessage();
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      message = e.getMessage();
      // The message test comes before the type test, as it always has in the subtask: a DB without
      // the RPC counts as unsupported whatever exception carries it. The master reports "unsafe"
      // as an error PB (IllegalState wrapped as INTERNAL_ERROR), which the client throws as a
      // MasterErrorException rather than returning an unsuccessful response.
      if (message != null && message.contains("invalid method name")) {
        result = Result.UNSUPPORTED;
      } else if (e instanceof MasterErrorException) {
        result = Result.UNSAFE;
      } else {
        result = Result.ERROR;
        error = e;
      }
    } finally {
      durationNanos = System.nanoTime() - startNanos;
      observe(caller, masterIps, tserverIps, result, durationNanos);
    }
    return new Outcome(result, message, error, TimeUnit.NANOSECONDS.toMillis(durationNanos));
  }

  private static void observe(
      Caller caller,
      Collection<String> masterIps,
      Collection<String> tserverIps,
      Result result,
      long durationNanos) {
    try {
      HISTOGRAM
          .labelValues(
              caller == null ? null : caller.label, target(masterIps, tserverIps), result.label())
          .observe(durationNanos / 1e9);
    } catch (RuntimeException e) {
      log.warn("Failed to record {}", CALL_DURATION_METRIC, e);
    }
  }

  @VisibleForTesting
  static String target(Collection<String> masterIps, Collection<String> tserverIps) {
    boolean masters = CollectionUtils.isNotEmpty(masterIps);
    boolean tservers = CollectionUtils.isNotEmpty(tserverIps);
    if (masters && tservers) {
      return "master_tserver";
    }
    if (masters) {
      return "master";
    }
    return tservers ? "tserver" : "none";
  }
}
