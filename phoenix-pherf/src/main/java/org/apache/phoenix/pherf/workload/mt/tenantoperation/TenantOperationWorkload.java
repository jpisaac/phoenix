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

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.result.Result;
import org.apache.phoenix.pherf.result.ResultUtil;
import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;
import org.apache.phoenix.pherf.result.impl.JsonResultHandler;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.Workload;
import org.apache.phoenix.pherf.workload.mt.EventGenerator;
import org.apache.phoenix.pherf.workload.mt.MultiTenantWorkload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * This class creates workload for tenant based load profiles.
 * It uses @see {@link TenantOperationFactory} in conjunction with
 * @see {@link WeightedRandomEventGenerator} to generate the load events.
 * It then publishes these events onto a RingBuffer based queue.
 * The @see {@link TenantOperationWorkHandler} drains the events from the queue and executes them.
 * Reference for RingBuffer based queue http://lmax-exchange.github.io/disruptor/
 */

public class TenantOperationWorkload implements Workload {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantOperationWorkload.class);
    private final TenantOperationEventGeneratorFactory evtGeneratorFactory
            = new TenantOperationEventGeneratorFactory();
    private final EventGenerator<TenantOperationInfo> generator;


    public TenantOperationWorkload(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            Properties properties) {
        this.generator =  evtGeneratorFactory.newEventGenerator(phoenixUtil,
                model, scenario, properties);
    }

    public TenantOperationWorkload(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            List<PherfWorkHandler> workHandlers, Properties properties) throws Exception {
        this.generator =  evtGeneratorFactory.newEventGenerator(phoenixUtil,
                model, scenario, workHandlers, properties);
    }

    @Override public Callable<Void> execute() throws Exception {
        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                generator.start();
                return null;
            }
        };
    }

    @Override public void complete() {
        try {
            generator.stop();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }
}
