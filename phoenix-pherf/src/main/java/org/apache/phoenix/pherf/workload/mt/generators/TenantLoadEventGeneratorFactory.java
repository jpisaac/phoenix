package org.apache.phoenix.pherf.workload.mt.generators;

import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.handlers.PherfWorkHandler;

import java.util.List;
import java.util.Properties;

/**
 * A factory class for creating various supported load generators {@link LoadEventGenerator}
 */
public class TenantLoadEventGeneratorFactory implements
        LoadEventGeneratorFactory<TenantOperationInfo> {
    public enum GeneratorType {
        WEIGHTED, UNIFORM, SEQUENTIAL
    }
    @Override public LoadEventGenerator<TenantOperationInfo> newLoadEventGenerator(PhoenixUtil phoenixUtil,
            DataModel model, Scenario scenario,
            Properties properties) {
        GeneratorType type = GeneratorType.valueOf(scenario.getGeneratorName());
        switch (type) {
        case WEIGHTED:
            return new WeightedRandomLoadEventGenerator(phoenixUtil, model, scenario, properties);
        case UNIFORM:
            return new UniformDistributionLoadEventGenerator(phoenixUtil, model, scenario, properties);
        case SEQUENTIAL:
            return new SequentialLoadEventGenerator(phoenixUtil, model, scenario, properties);
        default:
            throw new IllegalArgumentException("Unknown generator type");
        }
    }

    @Override public LoadEventGenerator<TenantOperationInfo> newLoadEventGenerator(PhoenixUtil phoenixUtil,
            DataModel model, Scenario scenario,
            List<PherfWorkHandler> workHandlers, Properties properties) {
        GeneratorType type = GeneratorType.valueOf(scenario.getGeneratorName());
        switch (type) {
        case WEIGHTED:
            return new WeightedRandomLoadEventGenerator(phoenixUtil, model, scenario, workHandlers, properties);
        case UNIFORM:
            return new UniformDistributionLoadEventGenerator(phoenixUtil, model, scenario, workHandlers, properties);
        case SEQUENTIAL:
            return new SequentialLoadEventGenerator(phoenixUtil, model, scenario, workHandlers, properties);
        default:
            throw new IllegalArgumentException("Unknown generator type");
        }

    }

}
