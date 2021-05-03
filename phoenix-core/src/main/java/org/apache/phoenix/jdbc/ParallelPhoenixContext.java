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

import static org.apache.phoenix.jdbc.ParallelPhoenixUtil.PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.phoenix.query.QueryServices.AUTO_COMMIT_ATTRIB;

//This is POC code that relies on java 8, this can be implemented ourselves in a real version
public class ParallelPhoenixContext {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelPhoenixContext.class);

    public static String PARALLEL_PHOENIX_METRICS = "parallel_phoenix_metrics";

    private final ExecutorService executorForConn1;
    private final ExecutorService executorForConn2;

    //May need 2 properties in the future...
    // Depends on if we have phoenix.querytimeout and phoenix.secound.querytimeout
    private final Properties properties;

    private final HighAvailabilityGroup haGroup;
    private final long operationTimeoutMs;

    private CompletableFuture chainOnConn1 = CompletableFuture.completedFuture(new Object());
    private CompletableFuture chainOnConn2 = CompletableFuture.completedFuture(new Object());

    private volatile boolean isClosed = false;

    private ParallelPhoenixMetrics parallelPhoenixMetrics;

    public ParallelPhoenixMetrics getParallelPhoenixMetrics() {
        return parallelPhoenixMetrics;
    }

    /**
     * @param properties
     * @param haGroup
     * @param executors Executors to use for operations on connections. We use first executor in the
     *            list for connection1 and second for connection2
     * @param executorCapacities Ordered list of executorCapacities corresponding to executors. Null is interpreted as
     *            executors having capacity
     */
    ParallelPhoenixContext(Properties properties, HighAvailabilityGroup haGroup, List<ExecutorService> executors, List<Boolean> executorCapacities) {
        Preconditions.checkNotNull(executors);
        Preconditions.checkArgument(executors.size() >= 2, "Expected 2 executors, one for each connection");
        this.properties = properties;
        this.haGroup = haGroup;
        this.executorForConn1 = executors.get(0);
        this.executorForConn2 = executors.get(1);
        this.parallelPhoenixMetrics = new ParallelPhoenixMetrics();
        this.operationTimeoutMs = getOperationTimeoutMs(properties);
        initContext(executorCapacities);
    }

    /**
     * Initializes chainOnConn1 and chainOnConn2 according to capacity available in the threadpools.
     * If there is no capacity available we initialize the chain for that connection exceptionally
     * so any further operations on the chain also complete exceptionally
     * @param executorCapacities
     */
    private void initContext(List<Boolean> executorCapacities) {
        if (executorCapacities == null) {
            return;
        }
        Preconditions.checkArgument(executorCapacities.size() >= 2,
            "Expected 2 executorCapacities values for each threadpool");
        if (!executorCapacities.get(0)) {
            chainOnConn1 = new CompletableFuture<>();
            chainOnConn1.completeExceptionally(
                new SQLException("No capacity available for connection1 for cluster "
                        + this.haGroup.getGroupInfo().getUrl1()));
            LOG.debug("No capacity available for connection1 for cluster {}",
                this.haGroup.getGroupInfo().getUrl1());
        }
        if (!executorCapacities.get(1)) {
            chainOnConn2 = new CompletableFuture<>();
            chainOnConn2.completeExceptionally(
                new SQLException("No capacity available for connection2 for cluster "
                        + this.haGroup.getGroupInfo().getUrl2()));
            LOG.debug("No capacity available for connection2 for cluster {}",
                this.haGroup.getGroupInfo().getUrl2());
        }
    }

    public Properties getProperties() {
        return properties;
    }

    public HighAvailabilityGroup getHaGroup() {
        return haGroup;
    }

    public boolean isAutoCommit() {
        return Boolean.valueOf((String)properties.getOrDefault(AUTO_COMMIT_ATTRIB,"false"));
    }

    /**
     * Chains an operation on the connection from the last chained operation. This is to ensure that
     * we operate on the underlying phoenix connection (and related objects) using a single thread
     * at any given time. Operations are supposed to be expressed in the form of Supplier. All async
     * operations on the underlying connection should be chained using this method
     * @param Supplier <T>
     * @return CompletableFuture<T>
     */
    public <T> CompletableFuture<T> chainOnConn1(Supplier<T> s) {
        CompletableFuture<T> chainedFuture =
                this.chainOnConn1.thenApplyAsync((f) -> s.get(), executorForConn1);
        this.chainOnConn1 = chainedFuture;
        return chainedFuture;
    }

    public <T> void setConenction1Tail(CompletableFuture<T> future) {
        this.chainOnConn1 = future;
    }

    public <T> CompletableFuture<T> chainOnConn2(Supplier<T> s) {
        CompletableFuture<T> chainedFuture =
                this.chainOnConn2.thenApplyAsync((f) -> s.get(), executorForConn2);
        this.chainOnConn2 = chainedFuture;
        return chainedFuture;
    }

    public <T> void setConenction2Tail(CompletableFuture<T> future) {
        this.chainOnConn2 = future;
    }

    public void close() {
        isClosed = true;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void checkOpen() throws SQLException {
        if (isClosed) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CONNECTION_CLOSED)
                    .build()
                    .buildException();
        }
    }

    public Map<MetricType, Long> getContextMetrics() {
        return this.parallelPhoenixMetrics.getAllMetrics().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,t -> t.getValue().getValue()));
    }

    public void resetMetrics() {
        //We don't use ParallelPhoenixMetrics::reset() here as that will race with any remaining operations
        //Instead we generate new metrics, any updates won't be reflected in future reads
        parallelPhoenixMetrics = new ParallelPhoenixMetrics();
    }

    /**
     * Decorates metrics from PhoenixConnections table metrics with a virtual table of
     * PARALLEL_PHOENIX_METRICS name as well as all the context's metrics.
     * @param initialMetrics Table Specific Metrics class to populate
     */
    public void decorateMetrics(Map<String, Map<MetricType, Long>> initialMetrics) {
        //decorate
        initialMetrics.put(PARALLEL_PHOENIX_METRICS,getContextMetrics());
    }

    public long getOperationTimeout() {
        return this.operationTimeoutMs;
    }

    CompletableFuture<?> getChainOnConn1() {
        return this.chainOnConn1;
    }

    CompletableFuture<?> getChainOnConn2() {
        return this.chainOnConn2;
    }

    private long getOperationTimeoutMs(Properties properties) {
        long operationTimeoutMs;
        if (properties.getProperty(PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB) != null) {
            operationTimeoutMs = Long.parseLong(
                properties.getProperty(PHOENIX_HA_PARALLEL_OPERATION_TIMEOUT_ATTRIB));
        } else {
            operationTimeoutMs =
                    Long.parseLong(properties.getProperty(QueryServices.THREAD_TIMEOUT_MS_ATTRIB,
                        Long.toString(QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS)));
        }
        Preconditions.checkArgument(operationTimeoutMs >= 0);
        return operationTimeoutMs;
    }
}
