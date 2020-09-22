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

package org.apache.phoenix.pherf.workload.continuous.tenantoperation;

import com.clearspring.analytics.util.Lists;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.WorkHandler;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.result.Result;
import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;
import org.apache.phoenix.pherf.result.impl.CSVFileResultHandler;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.continuous.OperationStats;
import org.apache.phoenix.pherf.workload.continuous.tenantoperation.TenantOperationWorkload.TenantOperationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * TODO Documentation
 */

public class TenantOperationWorkHandler implements WorkHandler<TenantOperationEvent>,
        LifecycleAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantOperationWorkHandler.class);
    private static volatile CSVFileResultHandler resultFileHandler;
    private final PhoenixUtil pUtil;
    private final List<OperationStats> handlerStats;
    private final String handlerId;
    private final DataModel dataModel;
    private final Scenario scenario;
    private final TenantOperationFactory tenantOperationFactory;

    static {
        resultFileHandler = new CSVFileResultHandler();
        resultFileHandler.setResultFileDetails(ResultFileDetails.CSV);
        resultFileHandler.setResultFileName("STATS_" + System.currentTimeMillis());
    }

    public TenantOperationWorkHandler(PhoenixUtil pUtil, DataModel dataModel, Scenario scenario, String handlerId) {
        this.handlerId = handlerId;
        this.pUtil = pUtil;
        this.dataModel = dataModel;
        this.scenario = scenario;
        this.tenantOperationFactory = new TenantOperationFactory(pUtil, dataModel, scenario);
        this.handlerStats = Lists.newArrayList();
    }

    @Override public void onEvent(TenantOperationEvent event)
            throws Exception {

        TenantOperationInfo input = event.getTenantOperationInfo();
        TenantOperationImpl op = tenantOperationFactory.getOperation(input);
        OperationStats stats = op.getMethod().apply(input);
        handlerStats.add(stats);
        LOGGER.info(pUtil.getGSON().toJson(stats));
    }

    @Override public void onStart() {
        LOGGER.info(String.format("TenantOperationWorkHandler started for %s:%s",
                scenario.getName(), scenario.getTableName()));
    }

    @Override public void onShutdown() {
        LOGGER.info(String.format("TenantOperationWorkHandler stopped for %s:%s",
                scenario.getName(), scenario.getTableName()));
        flushStats();
    }

    private void flushStats() {
        try {
            ResultFileDetails resultFileDetails = resultFileHandler.getResultFileDetails();
            for (OperationStats stats : handlerStats) {
                List<ResultValue> statsRow = stats.getCsvRepresentation(handlerId);
                Result result =
                        new Result(resultFileDetails,
                                resultFileDetails.getHeader().toString(),
                                statsRow);
                resultFileHandler.write(result);
            }
            handlerStats.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
