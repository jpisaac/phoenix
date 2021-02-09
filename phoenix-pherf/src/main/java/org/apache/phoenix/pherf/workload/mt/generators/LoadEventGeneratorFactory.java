package org.apache.phoenix.pherf.workload.mt.generators;

import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.handlers.PherfWorkHandler;

import java.util.List;
import java.util.Properties;

/**
 * An interface that factory implementers need to implement
 * for creating various supported load generators {@link LoadEventGenerator}
 * @param <T> load event object
 */
public interface LoadEventGeneratorFactory<T> {
    LoadEventGenerator<T> newLoadEventGenerator(PhoenixUtil phoenixUtil,
            DataModel model, Scenario scenario,
            Properties properties) ;

    LoadEventGenerator<T> newLoadEventGenerator(PhoenixUtil phoenixUtil,
            DataModel model, Scenario scenario,
            List<PherfWorkHandler> workHandlers, Properties properties) ;

}
