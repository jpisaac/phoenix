/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.jdbc;

import static org.apache.hadoop.test.GenericTestUtils.waitFor;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.exceptions.verification.WantedButNotInvoked;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class ParallelPhoenixConnectionTest {

    ParallelPhoenixContext context;

    ParallelPhoenixConnection parallelPhoenixConnection;
    PhoenixConnection connection1 = Mockito.mock(PhoenixConnection.class);
    PhoenixConnection connection2 = Mockito.mock(PhoenixConnection.class);

    @Before
    public void init() {
        context = new ParallelPhoenixContext(new Properties(), Mockito.mock(HighAvailabilityGroup.class),
            HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(), null);
        parallelPhoenixConnection = new ParallelPhoenixConnection(context,CompletableFuture.completedFuture(connection1),CompletableFuture.completedFuture(connection2));
    }

    @Test
    public void getWarningsBothWarnTest() throws Exception {
        SQLWarning warning1 = new SQLWarning("warning1");
        SQLWarning warning2 = new SQLWarning("warning2");
        Mockito.when(connection1.getWarnings()).thenReturn(warning1);
        Mockito.when(connection2.getWarnings()).thenReturn(warning2);

        SQLWarning result = parallelPhoenixConnection.getWarnings();
        assertEquals(warning1,result.getNextWarning());
        assertEquals(warning2,result.getNextWarning().getNextWarning());
    }

    @Test
    public void getWarnings1WarnTest() throws Exception {
        SQLWarning warning2 = new SQLWarning("warning2");
        Mockito.when(connection1.getWarnings()).thenReturn(null);
        Mockito.when(connection2.getWarnings()).thenReturn(warning2);

        SQLWarning result = parallelPhoenixConnection.getWarnings();
        assertEquals(warning2,result);
    }

    @Test
    public void getWarnings0WarnTest() throws Exception {
        Mockito.when(connection1.getWarnings()).thenReturn(null);
        Mockito.when(connection2.getWarnings()).thenReturn(null);

        SQLWarning result = parallelPhoenixConnection.getWarnings();
        assertEquals(null,result);
    }

    @Test
    public void isWrapperForPhoenixConnectionFalseTest() throws SQLException {
        boolean result = parallelPhoenixConnection.isWrapperFor(PhoenixConnection.class);
        assertEquals(false,result);
    }

    @Test
    public void isWrapperForPhoenixMonitoredConnectionTrueTest() throws SQLException {
        boolean result = parallelPhoenixConnection.isWrapperFor(PhoenixMonitoredConnection.class);
        assertEquals(true,result);
    }

    @Test
    public void unwrapPhoenixConnectionFailsTest() {
        try {
            parallelPhoenixConnection.unwrap(PhoenixConnection.class);
        } catch (SQLException e) {
            assertEquals(e.getErrorCode(), SQLExceptionCode.CLASS_NOT_UNWRAPPABLE.getErrorCode());
        }
    }

    @Test
    public void unwrapPhoenixMonitoredConnectionTest() throws SQLException {
        PhoenixMonitoredConnection result = parallelPhoenixConnection.unwrap(PhoenixMonitoredConnection.class);
        assertEquals(parallelPhoenixConnection,result);
    }

    @Test
    public void testCloseConnection1Error() throws SQLException {
        Mockito.doThrow(new SQLException()).when(connection1).close();
        parallelPhoenixConnection.close();
        Mockito.verify(connection2).close();
    }

    @Test
    public void testCloseConnection2Error() throws SQLException {
        Mockito.doThrow(new SQLException()).when(connection2).close();
        parallelPhoenixConnection.close();
        Mockito.verify(connection1).close();
    }

    @Test
    public void testCloseBothConnectionError() throws SQLException {
        Mockito.doThrow(new SQLException()).when(connection1).close();
        Mockito.doThrow(new SQLException()).when(connection2).close();
        try {
            parallelPhoenixConnection.close();
            fail("Close should throw exception when both underlying close throw exceptions");
        } catch (SQLException e) {
        }
        Mockito.verify(connection1).close();
        Mockito.verify(connection2).close();
    }

    @Test
    public void testConnection1CloseDelay() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);
        Supplier<Void> delaySupplier = getDelaySupplier(cdl);
        context.chainOnConn1(delaySupplier);
        // Chain on conn1 is lagging, we should return after closing conn2
        parallelPhoenixConnection.close();
        Mockito.verify(connection2).close();
        cdl.countDown();
        // Connection1 should eventually close
        waitForConnectionClose(connection1);
    }

    @Test
    public void testConnection2CloseDelay() throws Exception {
        CountDownLatch cdl = new CountDownLatch(1);
        Supplier<Void> delaySupplier = getDelaySupplier(cdl);
        context.chainOnConn2(delaySupplier);
        // Chain on conn2 is lagging, we should return after closing conn1
        parallelPhoenixConnection.close();
        Mockito.verify(connection1).close();
        cdl.countDown();
        // Connection2 should eventually close
        waitForConnectionClose(connection2);
    }

    @Test
    public void testConnectionCloseTimedout() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ParallelPhoenixUtil.PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB,
            "1000");
        ParallelPhoenixContext context =
                new ParallelPhoenixContext(properties, Mockito.mock(HighAvailabilityGroup.class),
                    HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(), null);
        parallelPhoenixConnection =
                new ParallelPhoenixConnection(context,
                        CompletableFuture.completedFuture(connection1),
                        CompletableFuture.completedFuture(connection2));
        CountDownLatch cdl1 = new CountDownLatch(1);
        CountDownLatch cdl2 = new CountDownLatch(1);
        Supplier<Void> delaySupplier1 = getDelaySupplier(cdl1);
        Supplier<Void> delaySupplier2 = getDelaySupplier(cdl2);
        context.chainOnConn1(delaySupplier1);
        context.chainOnConn2(delaySupplier2);
        long prevTimeoutCounter = GlobalClientMetrics.GLOBAL_HA_PARALLEL_TASK_TIMEOUT_COUNTER.getMetric()
                .getValue();
        try {
            parallelPhoenixConnection.close();
            fail("Should've timed out");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.OPERATION_TIMED_OUT.getErrorCode(),
                e.getErrorCode());
            assertTrue(GlobalClientMetrics.GLOBAL_HA_PARALLEL_TASK_TIMEOUT_COUNTER.getMetric()
                    .getValue() > prevTimeoutCounter);
        }
        cdl1.countDown();
        cdl2.countDown();
        waitForConnectionClose(connection1);
        waitForConnectionClose(connection2);
    }

    private void waitForConnectionClose(PhoenixConnection connection) throws Exception {
        waitFor(() -> {
            try {
                Mockito.verify(connection).close();
            } catch (SQLException | WantedButNotInvoked e) {
                return false;
            }
            return true;
        }, 1000, 30000);
    }

    private Supplier<Void> getDelaySupplier(CountDownLatch cdl) {
        return (() -> {
            try {
                cdl.await();
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
            return null;
        });
    }
}