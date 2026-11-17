// Copyright (c) YugabyteDB, Inc.

package com.yugabyte.yw.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yugabyte.yw.common.NodesSafeToTakeDownUtil.Caller;
import com.yugabyte.yw.common.NodesSafeToTakeDownUtil.Outcome;
import com.yugabyte.yw.common.NodesSafeToTakeDownUtil.Result;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yb.WireProtocol.AppStatusPB;
import org.yb.client.AreNodesSafeToTakeDownResponse;
import org.yb.client.MasterErrorException;
import org.yb.client.YBClientApi;
import org.yb.master.MasterTypes.MasterErrorPB;

public class NodesSafeToTakeDownUtilTest {

  private static final Set<String> MASTERS = Set.of("10.0.0.1");
  private static final Set<String> TSERVERS = Set.of("10.0.0.2");

  private YBClientApi client;

  @Before
  public void setUp() {
    client = mock(YBClientApi.class);
  }

  @After
  public void tearDown() {
    // Don't leak an interrupt into the next test.
    Thread.interrupted();
  }

  // The histogram is static and shared across tests, so assertions use deltas.
  private static long count(Caller caller, String target, Result result) {
    return NodesSafeToTakeDownUtil.histogram()
        .labelValues(caller.label, target, result.label())
        .getCount();
  }

  private Outcome checkWithBoth() {
    return NodesSafeToTakeDownUtil.check(client, MASTERS, TSERVERS, 60000, Caller.CLUSTER_HEALTH);
  }

  // Created before any when(...) on the client: stubbing a mock inside another stubbing fails.
  private static MasterErrorException masterError(String message) {
    MasterErrorException e = mock(MasterErrorException.class);
    when(e.getMessage()).thenReturn(message);
    return e;
  }

  @Test
  public void testSuccessfulResponseIsSafe() throws Exception {
    when(client.areNodesSafeToTakeDown(MASTERS, TSERVERS, 60000))
        .thenReturn(new AreNodesSafeToTakeDownResponse(null));
    long before = count(Caller.CLUSTER_HEALTH, "master_tserver", Result.SAFE);

    Outcome outcome = checkWithBoth();

    assertEquals(Result.SAFE, outcome.result());
    assertNull(outcome.message());
    assertNull(outcome.error());
    assertTrue(outcome.durationMs() >= 0);
    assertEquals(before + 1, count(Caller.CLUSTER_HEALTH, "master_tserver", Result.SAFE));
  }

  @Test
  public void testErrorResponseIsUnsafe() throws Exception {
    MasterErrorPB error =
        MasterErrorPB.newBuilder()
            .setCode(MasterErrorPB.Code.INTERNAL_ERROR)
            .setStatus(
                AppStatusPB.newBuilder()
                    .setCode(AppStatusPB.ErrorCode.ILLEGAL_STATE)
                    .setMessage("2 tablet(s) would not have enough replicas"))
            .build();
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong()))
        .thenReturn(new AreNodesSafeToTakeDownResponse(error));

    Outcome outcome = checkWithBoth();

    assertEquals(Result.UNSAFE, outcome.result());
    assertEquals("2 tablet(s) would not have enough replicas", outcome.message());
    assertNull(outcome.error());
  }

  @Test
  public void testMasterErrorExceptionIsUnsafe() throws Exception {
    MasterErrorException unsafe = masterError("Taking down the requested node(s) is unsafe");
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong())).thenThrow(unsafe);
    long before = count(Caller.CLUSTER_HEALTH, "master_tserver", Result.UNSAFE);

    Outcome outcome = checkWithBoth();

    assertEquals(Result.UNSAFE, outcome.result());
    assertEquals("Taking down the requested node(s) is unsafe", outcome.message());
    assertNull(outcome.error());
    assertEquals(before + 1, count(Caller.CLUSTER_HEALTH, "master_tserver", Result.UNSAFE));
  }

  @Test
  public void testInvalidMethodNameIsUnsupportedEvenForMasterError() throws Exception {
    MasterErrorException unsupported = masterError("invalid method name: AreNodesSafeToTakeDown");
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong())).thenThrow(unsupported);
    long before = count(Caller.TASK, "master_tserver", Result.UNSUPPORTED);

    Outcome outcome = NodesSafeToTakeDownUtil.check(client, MASTERS, TSERVERS, 1, Caller.TASK);

    assertEquals(Result.UNSUPPORTED, outcome.result());
    assertNull(outcome.error());
    assertEquals(before + 1, count(Caller.TASK, "master_tserver", Result.UNSUPPORTED));
  }

  @Test
  public void testNullMessageIsErrorWithoutNpe() throws Exception {
    RuntimeException thrown = new RuntimeException((String) null);
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong())).thenThrow(thrown);
    long before = count(Caller.CLUSTER_HEALTH, "master_tserver", Result.ERROR);

    Outcome outcome = checkWithBoth();

    assertEquals(Result.ERROR, outcome.result());
    assertSame(thrown, outcome.error());
    assertEquals(before + 1, count(Caller.CLUSTER_HEALTH, "master_tserver", Result.ERROR));
  }

  @Test
  public void testTimeoutIsError() throws Exception {
    TimeoutException thrown = new TimeoutException("deadline exceeded");
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong())).thenThrow(thrown);

    Outcome outcome = checkWithBoth();

    assertEquals(Result.ERROR, outcome.result());
    assertSame(thrown, outcome.error());
    assertEquals("deadline exceeded", outcome.message());
  }

  @Test
  public void testInterruptIsErrorAndKeepsInterruptFlag() throws Exception {
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong()))
        .thenThrow(new InterruptedException());

    Outcome outcome = checkWithBoth();

    assertEquals(Result.ERROR, outcome.result());
    assertTrue(Thread.interrupted());
  }

  @Test
  public void testNullResponseIsError() throws Exception {
    Outcome outcome = checkWithBoth();

    assertEquals(Result.ERROR, outcome.result());
    assertTrue(outcome.error() instanceof NullPointerException);
  }

  @Test
  public void testTargetLabel() throws Exception {
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong()))
        .thenReturn(new AreNodesSafeToTakeDownResponse(null));
    long master = count(Caller.TASK, "master", Result.SAFE);
    long tserver = count(Caller.TASK, "tserver", Result.SAFE);
    long both = count(Caller.TASK, "master_tserver", Result.SAFE);
    long none = count(Caller.TASK, "none", Result.SAFE);

    NodesSafeToTakeDownUtil.check(client, MASTERS, Set.of(), 1, Caller.TASK);
    NodesSafeToTakeDownUtil.check(client, Set.of(), TSERVERS, 1, Caller.TASK);
    NodesSafeToTakeDownUtil.check(client, MASTERS, TSERVERS, 1, Caller.TASK);
    NodesSafeToTakeDownUtil.check(client, Set.of(), Set.of(), 1, Caller.TASK);
    NodesSafeToTakeDownUtil.check(client, null, null, 1, Caller.TASK);

    assertEquals(master + 1, count(Caller.TASK, "master", Result.SAFE));
    assertEquals(tserver + 1, count(Caller.TASK, "tserver", Result.SAFE));
    assertEquals(both + 1, count(Caller.TASK, "master_tserver", Result.SAFE));
    assertEquals(none + 2, count(Caller.TASK, "none", Result.SAFE));
  }

  @Test
  public void testNeverThrowsWhenTheLabelIsInvalid() throws Exception {
    when(client.areNodesSafeToTakeDown(any(), any(), anyLong()))
        .thenReturn(new AreNodesSafeToTakeDownResponse(null));

    // A null caller makes labelValues throw; the call result must still come back.
    Outcome outcome = NodesSafeToTakeDownUtil.check(client, MASTERS, TSERVERS, 1, null);

    assertEquals(Result.SAFE, outcome.result());
  }
}
