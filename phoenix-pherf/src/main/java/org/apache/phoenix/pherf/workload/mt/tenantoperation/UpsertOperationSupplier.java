/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.pherf.workload.mt.tenantoperation;

import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.thirdparty.com.google.common.base.Charsets;
import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.Upsert;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.OperationStats;
import org.apache.phoenix.pherf.workload.mt.UpsertOperation;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.thirdparty.com.google.common.hash.BloomFilter;
import org.apache.phoenix.thirdparty.com.google.common.hash.Funnel;
import org.apache.phoenix.thirdparty.com.google.common.hash.PrimitiveSink;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A supplier of {@link Function} that takes {@link UpsertOperation} as an input
 */
class UpsertOperationSupplier extends BaseOperationSupplier {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpsertOperationSupplier.class);
    private Map<String, List<Column>> columnsForUpsertOps = Maps.newConcurrentMap();
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public UpsertOperationSupplier(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario) {
        super(phoenixUtil, model, scenario);
    }

    private BloomFilter createTenantsLoadedFilter(Scenario scenario) {
        Funnel<String> opGroupLoadedFunnel = new Funnel<String>() {
            @Override
            public void funnel(String opGroupId, PrimitiveSink into) {
                into.putString(opGroupId, Charsets.UTF_8);
            }
        };

        // This holds the info whether the upsert operation column was initialized or not.
        return BloomFilter.create(opGroupLoadedFunnel, scenario.getUpserts().size(), 0.01);
    }

    @Override
    public Function<TenantOperationInfo, OperationStats> get() {
        return new Function<TenantOperationInfo, OperationStats>() {

            @Override
            public OperationStats apply(final TenantOperationInfo input) {

                final int batchSize = loadProfile.getBatchSize();
                final boolean useBatchApi = batchSize != 0;
                final int rowCount = useBatchApi ? batchSize : 1;

                final UpsertOperation operation = (UpsertOperation) input.getOperation();
                final Upsert upsert = operation.getUpsert();
                final String tenantGroup = input.getTenantGroupId();
                final String opGroup = input.getOperationGroupId();
                final String tenantId = upsert.isUseGlobalConnection() ? null : input.getTenantId();
                final String tableName = input.getTableName();
                final String scenarioName = input.getScenarioName();
                final List<Column> columns = upsert.getColumn();

                // If the scenario has not defined the columns,
                // then check if it has already been lazy loaded.
                if (columns.isEmpty()) {
                    if (columnsForUpsertOps.get(opGroup) != null && !columnsForUpsertOps.get(opGroup).isEmpty()) {
                        columns.addAll(columnsForUpsertOps.get(opGroup));
                    }
                }

                final String opName = String.format("%s:%s:%s:%s:%s",
                        scenarioName, tableName, opGroup, tenantGroup, input.getTenantId());
                long rowsCreated = 0;
                long startTime = 0, duration, totalDuration;
                int status = 0;
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                try (Connection connection = phoenixUtil.getConnection(tenantId)) {
                    // If list of columns has not been not provided or lazy loaded
                    // then use the metadata call to get the column list.
                    if (columns.isEmpty()) {
                        rwLock.writeLock().lock();
                        try {
                            if (columns.isEmpty()) {
                                List<Column> allCols = phoenixUtil.getColumnsFromPhoenix(scenario.getSchemaName(),
                                        scenario.getTableNameWithoutSchemaName(),
                                        connection);
                                columns.addAll(allCols);
                                columnsForUpsertOps.put(opGroup, allCols);
                            }
                        } finally {
                            rwLock.writeLock().unlock();
                        }
                    }

                    String sql = phoenixUtil.buildSql(columns, tableName);
                    startTime = EnvironmentEdgeManager.currentTimeMillis();
                    PreparedStatement stmt = null;
                    try {
                        stmt = connection.prepareStatement(sql);
                        for (long i = rowCount; i > 0; i--) {
                            LOGGER.debug("Operation " + opName + " executing ");
                            stmt = phoenixUtil.buildStatement(rulesApplier, scenario, columns, stmt, simpleDateFormat);
                            if (useBatchApi) {
                                stmt.addBatch();
                            } else {
                                rowsCreated += stmt.executeUpdate();
                            }
                        }
                    } catch (SQLException e) {
                        throw e;
                    } finally {
                        // Need to keep the statement open to send the remaining batch of updates
                        if (!useBatchApi && stmt != null) {
                            stmt.close();
                        }
                        if (connection != null) {
                            if (useBatchApi && stmt != null) {
                                int[] results = stmt.executeBatch();
                                for (int x = 0; x < results.length; x++) {
                                    int result = results[x];
                                    if (result < 1) {
                                        final String msg =
                                                "Failed to write update in batch (update count="
                                                        + result + ")";
                                        throw new RuntimeException(msg);
                                    }
                                    rowsCreated += result;
                                }
                                // Close the statement after our last batch execution.
                                stmt.close();
                            }

                            try {
                                connection.commit();
                                duration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                                LOGGER.info("Writer ( " + Thread.currentThread().getName()
                                        + ") committed Final Batch. Duration (" + duration + ") Ms");
                                connection.close();
                            } catch (SQLException e) {
                                // Swallow since we are closing anyway
                                LOGGER.error("Error when closing/committing", e);
                            }
                        }
                    }
                } catch (SQLException sqle) {
                    LOGGER.error("Operation " + opName + " failed with exception ", sqle);
                    status = -1;
                } catch (Exception e) {
                    LOGGER.error("Operation " + opName + " failed with exception ", e);
                    status = -1;
                }

                totalDuration = EnvironmentEdgeManager.currentTimeMillis() - startTime;
                return new OperationStats(input, startTime, status, rowsCreated, totalDuration);
            }
        };
    }
}
