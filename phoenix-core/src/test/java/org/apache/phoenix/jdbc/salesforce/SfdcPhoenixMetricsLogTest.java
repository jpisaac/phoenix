/*
 * Copyright, 1999-2019, SALESFORCE.com
 * All Rights Reserved
 * Company Confidential
 */
package org.apache.phoenix.jdbc.salesforce;

import com.google.common.collect.ImmutableMap;
import org.apache.phoenix.monitoring.MetricType;
import org.junit.Before;
import org.junit.Test;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Test does not work in intellij until unreleased 2019.3
 * see https://youtrack.jetbrains.com/issue/IDEA-122783
 * run with mvn test -Dtest=SfdcPhoenixMetricsLogTest
 */
public class SfdcPhoenixMetricsLogTest {

    private static final String TABLE = "TABLE";
    private static final String SQL = "SELECT * FROM TABLE WHERE A=?";
    private SfdcPhoenixMetricsLog phoenixMetricsLog;
    private TestLogger logger;
    private Map<MetricType,Long> metricsMap = ImmutableMap.of(MetricType.RESULT_SET_TIME_MS, 12L);
    private Map<String,Map<MetricType,Long>> tableMetricsMap = ImmutableMap.of(TABLE, metricsMap);

    @Before
    public void init(){
        phoenixMetricsLog = new SfdcPhoenixMetricsLog();
        logger = TestLoggerFactory.getTestLogger(SfdcPhoenixMetricsLog.class);
        logger.clear();
    }

    @Test
    public void logOverAllReadRequestMetricsTest() {
        phoenixMetricsLog.logOverAllReadRequestMetrics(metricsMap,SQL);
        assertTrue(logger.getLoggingEvents().get(0).getMessage().contains(SQL));
        assertTrue(logger.getLoggingEvents().get(0).getMessage().contains(
                MetricType.RESULT_SET_TIME_MS.shortName()+"=12"));
    }

    @Test
    public void logReadMetricInfoForMutationsSinceLastResetTest() {
        phoenixMetricsLog.logReadMetricInfoForMutationsSinceLastReset(tableMetricsMap);
        assertTrue(logger.getLoggingEvents().get(0).getMessage().contains(
                MetricType.RESULT_SET_TIME_MS.shortName()+"=12"));

    }

    @Test
    public void logRequestReadMetricsTest() {
        phoenixMetricsLog.logRequestReadMetrics(tableMetricsMap,SQL);
        assertTrue(logger.getLoggingEvents().get(0).getMessage().contains(SQL));
        assertTrue(logger.getLoggingEvents().get(0).getMessage().contains(
                MetricType.RESULT_SET_TIME_MS.shortName()+"=12"));
    }

    @Test
    public void logWriteMetricsfoForMutationsSinceLastResetTest() {
        phoenixMetricsLog.logWriteMetricsfoForMutationsSinceLastReset(tableMetricsMap);
        assertTrue(logger.getLoggingEvents().get(0).getMessage().contains(
                MetricType.RESULT_SET_TIME_MS.shortName()+"=12"));
    }
}