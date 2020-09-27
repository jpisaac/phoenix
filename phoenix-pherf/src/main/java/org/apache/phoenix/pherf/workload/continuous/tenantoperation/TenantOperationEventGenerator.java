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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.OperationGroup;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.continuous.Operation;
import org.apache.phoenix.pherf.workload.continuous.EventGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * TODO Documentation
 */

public class TenantOperationEventGenerator
        implements EventGenerator<TenantOperationInfo> {

    private static class WeightedRandomSampler {
        private static final Random RANDOM = new Random();
        private final List<Operation> operationList;
        private final LoadProfile loadProfile;
        private final String modelName;
        private final String scenarioName;
        private final String tableName;
        private final EnumeratedDistribution<String> distribution;

        private final Map<String, TenantGroup> tenantGroupMap = Maps.newHashMap();
        private final Map<String, Pair<OperationGroup, Integer>> operationGroupSizes = Maps.newHashMap();
        private final Map<String, Operation> operationMap = Maps.newHashMap();

        public WeightedRandomSampler(List<Operation> operationList, DataModel model, Scenario scenario) {
            this.operationList = operationList;
            this.modelName = model.getName();
            this.scenarioName = scenario.getName();
            this.tableName = scenario.getTableName();
            this.loadProfile = scenario.getLoadProfile();

            for (Operation op : operationList) {
                for (OperationGroup og : loadProfile.getOpDistribution()) {
                    if (op.getId().compareTo(og.getId()) == 0) {
                        operationMap.put(op.getId(), op);
                    }
                }
            }
            Preconditions.checkArgument(!operationMap.isEmpty(),
                    "Operation list and load profile operation do not match");

            double totalTenantGroupWeight = 0.0f;
            double totalOperationGroupWeight = 0.0f;
            // Sum the weights to find the total weight,
            // so that individual group sizes can be calculated and also can be used
            // in the total probability distribution.
            for (TenantGroup tg : loadProfile.getTenantDistribution()) {
                totalTenantGroupWeight += tg.getWeight();
            }
            for (OperationGroup og : loadProfile.getOpDistribution()) {
                totalOperationGroupWeight += og.getWeight();
            }

            // Track the individual tenant group sizes,
            // so that given a generated sample we can get a random tenant for a group.
            for (TenantGroup tg : loadProfile.getTenantDistribution()) {
                double tgWeight = tg.getWeight()/totalTenantGroupWeight;
                tenantGroupMap.put(tg.getId(), tg);
            }

            for (OperationGroup og : loadProfile.getOpDistribution()) {
                double opWeight = og.getWeight()/totalOperationGroupWeight;
                double numOperationsForGroup = opWeight * loadProfile.getNumOperations();
                operationGroupSizes.put(og.getId(),
                        new Pair<OperationGroup, Integer>(og, (int)Math.round(numOperationsForGroup)));
            }

            // Initialize the sample probability distribution
            List<Pair<String, Double>> pmf = Lists.newArrayList();
            double totalWeight = totalTenantGroupWeight * totalOperationGroupWeight;
            for (TenantGroup tg : loadProfile.getTenantDistribution()) {
                for (String opId : operationMap.keySet()) {
                    String sampleName = String.format("%s:%s", tg.getId(), opId);
                    int opWeight = operationGroupSizes.get(opId).getFirst().getWeight();
                    double probability = (tg.getWeight() * opWeight)/totalWeight;
                    pmf.add(new Pair(sampleName, probability));
                }
            }
            this.distribution = new EnumeratedDistribution(pmf);
        }

        public TenantOperationInfo nextSample() {
            String sampleIndex = this.distribution.sample();
            String[] parts = sampleIndex.split(":");
            String tenantGroupId = parts[0];
            String opId = parts[1];

            Operation op = operationMap.get(opId);
            int numTenants = tenantGroupMap.get(tenantGroupId).getNumTenants();
            String tenantIdPrefix = Strings.padStart(tenantGroupId, loadProfile.getGroupIdLength(), '0');
            String formattedTenantId = String.format(loadProfile.getTenantIdFormat(),
                    tenantIdPrefix.substring(0, loadProfile.getGroupIdLength()), RANDOM.nextInt(numTenants));
            String paddedTenantId = Strings.padStart(formattedTenantId, loadProfile.getTenantIdLength(), '0');
            String tenantId = paddedTenantId.substring(0, loadProfile.getTenantIdLength());

            TenantOperationInfo sample = new TenantOperationInfo(modelName, scenarioName, tableName,
                    tenantGroupId, opId, tenantId, op);
            return sample;
        }
    }


    private static final Logger LOGGER = LoggerFactory.getLogger(
            TenantOperationEventGenerator.class);
    private final DataModel dataModel;
    private final Scenario scenario;
    private final TenantOperationFactory tenantOperationFactory;
    private final WeightedRandomSampler sampler;


    public TenantOperationEventGenerator(PhoenixUtil phoenixUtil, DataModel dataModel, Scenario scenario)
            throws Exception {
        this(phoenixUtil,
                PherfConstants.create().getProperties(PherfConstants.PHERF_PROPERTIES, true),
                dataModel, scenario);
    }

    public TenantOperationEventGenerator(PhoenixUtil phoenixUtil, Properties properties,
            DataModel dataModel, Scenario scenario) throws Exception {

        this.dataModel = dataModel;
        this.scenario = scenario;
        this.tenantOperationFactory = new TenantOperationFactory(phoenixUtil, dataModel, scenario);
        this.sampler = new WeightedRandomSampler(tenantOperationFactory.getOperationsForScenario(), dataModel, scenario);
    }

    @Override public TenantOperationInfo next() {
        return this.sampler.nextSample();
    }

}
