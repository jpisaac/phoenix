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

import com.google.common.collect.Lists;
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
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.Workload;
import org.apache.phoenix.pherf.workload.continuous.ContinuousWorkload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * TODO Documentation
 * Using RingBuffer based queue http://lmax-exchange.github.io/disruptor/
 */

public class TenantOperationWorkload implements ContinuousWorkload, Workload {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantOperationWorkload.class);
    private static final int NUM_HANDLER_PER_MODEL = 4;

    private static class ContinuousWorkloadExceptionHandler implements ExceptionHandler {
        @Override public void handleEventException(Throwable ex, long sequence, Object event) {
            LOGGER.error("Sequence=" + sequence + ", event=" + event, ex);
            throw new RuntimeException(ex);
        }

        @Override public void handleOnStartException(Throwable ex) {
            LOGGER.error("On Start", ex);
            throw new RuntimeException(ex);
        }

        @Override public void handleOnShutdownException(Throwable ex) {
            LOGGER.error("On Shutdown", ex);
            throw new RuntimeException(ex);
        }
    }


    public static class TenantOperationEvent {
        TenantOperationInfo tenantOperationInfo;

        public TenantOperationInfo getTenantOperationInfo() {
            return tenantOperationInfo;
        }

        public void setTenantOperationInfo(TenantOperationInfo tenantOperationInfo) {
            this.tenantOperationInfo = tenantOperationInfo;
        }

        public static final EventFactory<TenantOperationEvent> EVENT_FACTORY = new EventFactory<TenantOperationEvent>() {
            public TenantOperationEvent newInstance() {
                return new TenantOperationEvent();
            }
        };
    }


    private final TenantOperationEventGenerator generator;
    private Disruptor<TenantOperationEvent> disruptor;
    private final DataModel dataModel;
    private final Scenario scenario;
    private final PhoenixUtil pUtil;
    private final List<WorkHandler> handlers;
    private final ExceptionHandler exceptionHandler;

    public TenantOperationWorkload(PhoenixUtil phoenixUtil, Properties properties,
            DataModel dataModel, Scenario scenario) throws Exception {
        this.dataModel = dataModel;
        this.scenario = scenario;
        this.pUtil = phoenixUtil;
        this.generator = new TenantOperationEventGenerator(phoenixUtil, dataModel, scenario);
        this.handlers = Lists.newArrayListWithCapacity(NUM_HANDLER_PER_MODEL);
        for (int i = 0; i < NUM_HANDLER_PER_MODEL; i++) {
            String handlerId = String.format("%s.%d", InetAddress.getLocalHost().getHostName(), i+1);
            handlers.add(new TenantOperationWorkHandler(
                    pUtil,
                    dataModel,
                    scenario,
                    handlerId));
        }
        this.exceptionHandler = new ContinuousWorkloadExceptionHandler();

    }

    @Override public void start() {

        String currentThreadName = Thread.currentThread().getName();
        disruptor = new Disruptor<TenantOperationEvent>(TenantOperationEvent.EVENT_FACTORY, 8192,
                Threads.getNamedThreadFactory(currentThreadName + "." + scenario.getName() ),
                ProducerType.SINGLE, new BlockingWaitStrategy());

        this.disruptor.setDefaultExceptionHandler(this.exceptionHandler);
        this.disruptor.handleEventsWithWorkerPool(this.handlers.toArray(new WorkHandler[] {}));
        RingBuffer<TenantOperationEvent> ringBuffer = this.disruptor.start();
        long numOperations = scenario.getLoadProfile().getNumOperations();
        while (numOperations > 0) {
            TenantOperationInfo sample = generator.next();
            --numOperations;
            // Publishers claim events in sequence
            long sequence = ringBuffer.next();
            TenantOperationEvent event = ringBuffer.get(sequence);
            event.setTenantOperationInfo(sample);
            // make the event available to EventProcessors
            ringBuffer.publish(sequence);
            LOGGER.info(String.format("published : %s:%s:%d",
                    scenario.getName(), scenario.getTableName(), numOperations));

        }
    }

    @Override public void stop() {
        this.disruptor.shutdown();
    }

    @Override public Callable<Void> execute() throws Exception {
        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                start();
                return null;
            }
        };
    }

    @Override public void complete() {
        stop();
    }
}
