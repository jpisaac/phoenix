package org.apache.phoenix.pherf.workload.mt.tenantoperation;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.TenantGroup;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.mt.EventGenerator;
import org.apache.phoenix.pherf.workload.mt.Operation;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class UniformDistributionEventGenerator extends BaseTenantOperationEventGenerator {

    private static class UniformDistributionSampler {
        private final Random RANDOM = new Random();
        private final LoadProfile loadProfile;
        private final String modelName;
        private final String scenarioName;
        private final String tableName;
        private final UniformIntegerDistribution distribution;

        private final TenantGroup tenantGroup;
        private final List<Operation> operationList;

        public UniformDistributionSampler(List<Operation> operationList, DataModel model,
                Scenario scenario) {
            this.modelName = model.getName();
            this.scenarioName = scenario.getName();
            this.tableName = scenario.getTableName();
            this.loadProfile = scenario.getLoadProfile();
            this.operationList = operationList;

            // Track the individual tenant group with single tenant or global connection,
            // so that given a generated sample we can use the supplied tenant.
            // NOTE : Not sure if there is a case for multiple tenants in a uniform distribution.
            // For now keeping it simple.
            Preconditions.checkArgument(loadProfile.getTenantDistribution() != null,
                    "Tenant distribution cannot be null");
            Preconditions.checkArgument(!loadProfile.getTenantDistribution().isEmpty(),
                    "Tenant group cannot be empty");
            Preconditions.checkArgument(loadProfile.getTenantDistribution().size() == 1,
                    "Tenant group cannot be more than 1");
            tenantGroup = loadProfile.getTenantDistribution().get(0);

            this.distribution = new UniformIntegerDistribution(0, operationList.size() - 1);
        }

        public TenantOperationInfo nextSample() {
            int sampleIndex = this.distribution.sample();
            Operation op = operationList.get(sampleIndex);

            String tenantGroupId = tenantGroup.getId();
            String tenantIdPrefix = Strings
                    .padStart(tenantGroupId, loadProfile.getGroupIdLength(), 'x');
            String formattedTenantId = String.format(loadProfile.getTenantIdFormat(),
                    tenantIdPrefix.substring(0, loadProfile.getGroupIdLength()), 1);
            String paddedTenantId = Strings.padStart(formattedTenantId, loadProfile.getTenantIdLength(), 'x');
            String tenantId = paddedTenantId.substring(0, loadProfile.getTenantIdLength());

            TenantOperationInfo sample = new TenantOperationInfo(modelName, scenarioName, tableName,
                    tenantGroupId, op.getId(), tenantId, op);
            return sample;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(UniformDistributionEventGenerator.class);
    private final UniformDistributionSampler sampler;


    public UniformDistributionEventGenerator(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            Properties properties) {
        super(phoenixUtil, model, scenario, properties);
        this.sampler = new UniformDistributionSampler(operationFactory.getOperations(), model, scenario);
    }

    public UniformDistributionEventGenerator(PhoenixUtil phoenixUtil, DataModel model, Scenario scenario,
            List<PherfWorkHandler> workHandlers, Properties properties) {
        super(phoenixUtil, model, scenario, workHandlers, properties);
        this.sampler = new UniformDistributionSampler(operationFactory.getOperations(), model, scenario);
    }

    @Override public void start() throws Exception {
        Scenario scenario = operationFactory.getScenario();
        if (handlers == null) {
            handlers = Lists.newArrayListWithCapacity(DEFAULT_NUM_HANDLER_PER_SCENARIO);
            for (int i = 0; i < handlers.size(); i++) {
                String handlerId = String.format("%s.%d", InetAddress.getLocalHost().getHostName(), i+1);
                handlers.add(new TenantOperationWorkHandler(
                        operationFactory,
                        handlerId));
            }
        }

        String currentThreadName = Thread.currentThread().getName();
        disruptor = new Disruptor<TenantOperationEvent>(TenantOperationEvent.EVENT_FACTORY, DEFAULT_BUFFER_SIZE,
                Threads.getNamedThreadFactory(currentThreadName + "." + scenario.getName() ),
                ProducerType.SINGLE, new BlockingWaitStrategy());

        this.disruptor.setDefaultExceptionHandler(this.exceptionHandler);
        this.disruptor.handleEventsWithWorkerPool(this.handlers.toArray(new WorkHandler[] {}));
        RingBuffer<TenantOperationEvent> ringBuffer = this.disruptor.start();
        long numOperations = scenario.getLoadProfile().getNumOperations();
        while (numOperations > 0) {
            TenantOperationInfo sample = next();
            --numOperations;
            // Publishers claim events in sequence
            long sequence = ringBuffer.next();
            TenantOperationEvent event = ringBuffer.get(sequence);
            event.setTenantOperationInfo(sample);
            // make the event available to EventProcessors
            ringBuffer.publish(sequence);
            LOGGER.debug(String.format("published : %s:%s:%d",
                    scenario.getName(), scenario.getTableName(), numOperations));
        }
    }

    @Override public TenantOperationInfo next() {
        return this.sampler.nextSample();
    }
}
