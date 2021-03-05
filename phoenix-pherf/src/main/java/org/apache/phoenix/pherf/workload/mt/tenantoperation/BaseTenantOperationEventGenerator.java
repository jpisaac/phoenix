package org.apache.phoenix.pherf.workload.mt.tenantoperation;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.result.Result;
import org.apache.phoenix.pherf.result.ResultUtil;
import org.apache.phoenix.pherf.result.ResultValue;
import org.apache.phoenix.pherf.result.file.ResultFileDetails;
import org.apache.phoenix.pherf.result.impl.JsonResultHandler;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.EventGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public abstract class BaseTenantOperationEventGenerator implements EventGenerator<TenantOperationInfo> {
    protected static final int DEFAULT_NUM_HANDLER_PER_SCENARIO = 4;
    protected static final int DEFAULT_BUFFER_SIZE = 8192;

    protected Disruptor<TenantOperationEvent> disruptor;
    protected List<PherfWorkHandler> handlers;
    protected final Properties properties;
    protected final TenantOperationFactory operationFactory;
    protected final ExceptionHandler exceptionHandler;


    private static class WorkloadExceptionHandler implements ExceptionHandler {
        private static final Logger LOGGER = LoggerFactory.getLogger(WorkloadExceptionHandler.class);

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

    public BaseTenantOperationEventGenerator(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            List<PherfWorkHandler> workers, Properties properties) {
        this(phoenixUtil, model, scenario, workers, new WorkloadExceptionHandler(), properties);
    }

    public BaseTenantOperationEventGenerator(PhoenixUtil phoenixUtil, DataModel model,
            Scenario scenario, Properties properties) {

        operationFactory = new TenantOperationFactory(phoenixUtil, model, scenario);
        if (scenario.getPhoenixProperties() != null) {
            properties.putAll(scenario.getPhoenixProperties());
        }
        this.properties = properties;
        this.exceptionHandler = new WorkloadExceptionHandler();
    }

    public BaseTenantOperationEventGenerator(PhoenixUtil phoenixUtil, DataModel model,
            Scenario scenario,
            List<PherfWorkHandler> workers,
            ExceptionHandler exceptionHandler,
            Properties properties) {

        operationFactory = new TenantOperationFactory(phoenixUtil, model, scenario);
        if (scenario.getPhoenixProperties() != null) {
            properties.putAll(scenario.getPhoenixProperties());
        }
        this.properties = properties;
        this.handlers = workers;
        this.exceptionHandler = exceptionHandler;
    }


    @Override public PhoenixUtil getPhoenixUtil() { return operationFactory.getPhoenixUtil(); }

    @Override public Scenario getScenario() {
        return operationFactory.getScenario();
    }

    @Override public DataModel getModel() {
        return operationFactory.getModel();
    }

    @Override public Properties getProperties() {
        return this.properties;
    }

    protected void write(List<ResultValue> finalResults, RulesApplier rulesApplier) throws Exception {
        new ResultUtil().ensureBaseResultDirExists();
        JsonResultHandler resultWriter = null;
        try {
            resultWriter = new JsonResultHandler();
            resultWriter.setResultFileDetails(ResultFileDetails.JSON);
            resultWriter.setResultFileName(getScenario().getName().toUpperCase());
            resultWriter.write(new Result(ResultFileDetails.JSON, null, finalResults));
        } finally {
            if (resultWriter != null) {
                resultWriter.flush();
                resultWriter.close();
            }
        }
    }

    abstract public void start() throws Exception;

    @Override public void stop() throws Exception {
        // Wait for the handlers to finish the jobs
        if (disruptor != null) {
            disruptor.shutdown();
        }

        /*
        // TODO need to handle asynchronous result publishing
        if ((handlers != null)  && (outputResults)) {
            // Collect all the results from the individual handlers
            List<ResultValue> finalResult = new ArrayList<>();
            Scenario scenario = getScenario();
            for (PherfWorkHandler  pherfWorkHandler : handlers) {
                finalResult.addAll(pherfWorkHandler.getResults());
            }
            write(finalResult, null);
        }

        */
    }

    abstract protected TenantOperationInfo next();
}
