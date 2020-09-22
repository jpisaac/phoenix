package org.apache.phoenix.pherf.workload;

import org.apache.phoenix.pherf.XMLConfigParserTest;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.LoadProfile;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.workload.continuous.tenantoperation.TenantOperationEventGenerator;
import org.apache.phoenix.pherf.workload.continuous.tenantoperation.TenantOperationInfo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.UnmarshalException;
import javax.xml.stream.XMLStreamException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TenantOperationEventGeneratorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantOperationEventGeneratorTest.class);

    public DataModel readTestDataModel(String resourceName) throws Exception {
        URL scenarioUrl = XMLConfigParserTest.class.getResource(resourceName);
        assertNotNull(scenarioUrl);
        Path p = Paths.get(scenarioUrl.toURI());
        try {
            return XMLConfigParser.readDataModel(p);
        } catch (UnmarshalException e) {
            // If we don't parse the DTD, the variable 'name' won't be defined in the XML
            LOGGER.warn("Caught expected exception", e);
        }
        return null;
    }

    @Test
    public void testSimpleEventGen() {
        try {
            DataModel model = readTestDataModel("/scenario/test_evt_gen1.xml");
            LoadProfile loadProfile = model.getScenarios().get(0).getLoadProfile();
            assertTrue("tenant group size is not as expected: ",
                    loadProfile.getTenantDistribution().size() == 3);
            assertTrue("operation group size is not as expected: ",
                    loadProfile.getOpDistribution().size() == 5);

            TenantOperationEventGenerator evtGen = new TenantOperationEventGenerator(
                    PhoenixUtil.create(), model, model.getScenarios().get(0));

            int numOperation = 10;
            loadProfile.setNumOperations(numOperation);
            while (numOperation-- > 0) {
                TenantOperationInfo info = evtGen.next();
                LOGGER.info(info.getTenantId());
            }
            evtGen.next();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    }
