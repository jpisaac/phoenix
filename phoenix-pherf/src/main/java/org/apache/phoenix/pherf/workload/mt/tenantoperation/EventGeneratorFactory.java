package org.apache.phoenix.pherf.workload.mt.tenantoperation;

import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.EventGenerator;
import org.apache.phoenix.pherf.workload.mt.Operation;

import java.util.List;
import java.util.Properties;

public interface EventGeneratorFactory<T> {
    EventGenerator<T> newEventGenerator(PhoenixUtil phoenixUtil,
            DataModel model, Scenario scenario,
            Properties properties) ;

    EventGenerator<T> newEventGenerator(PhoenixUtil phoenixUtil,
            DataModel model, Scenario scenario,
            List<PherfWorkHandler> workHandlers, Properties properties) ;

}
