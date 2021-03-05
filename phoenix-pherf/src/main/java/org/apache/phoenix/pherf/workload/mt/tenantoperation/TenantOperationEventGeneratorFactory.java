package org.apache.phoenix.pherf.workload.mt.tenantoperation;

import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.EventGenerator;
import org.apache.phoenix.pherf.workload.mt.Operation;

import java.util.List;
import java.util.Properties;

public class TenantOperationEventGeneratorFactory implements EventGeneratorFactory <TenantOperationInfo> {
    public enum GeneratorType {
        WEIGHTED_RANDOM, UNIFORM
    }
    @Override public EventGenerator<TenantOperationInfo> newEventGenerator(PhoenixUtil phoenixUtil,
            DataModel model, Scenario scenario,
            Properties properties) {
        GeneratorType type = GeneratorType.valueOf(scenario.getGeneratorName());
        switch (type) {
        case WEIGHTED_RANDOM:
            return new WeightedRandomEventGenerator(phoenixUtil, model, scenario, properties);
        case UNIFORM:
            return new UniformDistributionEventGenerator(phoenixUtil, model, scenario, properties);
        default:
            throw new IllegalArgumentException("Unknown generator type");
        }
    }

    @Override public EventGenerator<TenantOperationInfo> newEventGenerator(PhoenixUtil phoenixUtil,
            DataModel model, Scenario scenario,
            List<PherfWorkHandler> workHandlers, Properties properties) {
        GeneratorType type = GeneratorType.valueOf(scenario.getGeneratorName());
        switch (type) {
        case WEIGHTED_RANDOM:
            return new WeightedRandomEventGenerator(phoenixUtil, model, scenario, workHandlers, properties);
        case UNIFORM:
            return new UniformDistributionEventGenerator(phoenixUtil, model, scenario, workHandlers, properties);
        default:
            throw new IllegalArgumentException("Unknown generator type");
        }

    }

}
