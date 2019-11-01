/*
 * Copyright, 1999-2019, SALESFORCE.com
 * All Rights Reserved
 * Company Confidential
 */
package org.apache.phoenix.jdbc.salesforce;

import org.apache.phoenix.jdbc.PhoenixMetricsLog;
import org.apache.phoenix.monitoring.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * For GDPR we first determine if this is a "parameterized" request before logging
 * We do this extremely naively to not affect perf,
 * should likely open a new jira to provide better logging interfaces with respect to GDPR
 * requirements and our team agreements like only using PreparedStatements for PII
 */
public class SfdcPhoenixMetricsLog implements PhoenixMetricsLog {

    private static final Logger LOGGER = LoggerFactory.getLogger(SfdcPhoenixMetricsLog.class);
    public static final String METRICS_FOR_TABLE = "TABLE";
    public static final String READ_ALL = "ReadAll";
    public static final String READ_TABLES = "ReadTables";
    public static final String LOGGED_SQL = "SQL";
    public static final String WRITE_METRICS = "WriteMetrics";
    public static final String READ_METRICS = "ReadMetrics";

    public SfdcPhoenixMetricsLog() {
    }

    private void buildStringForTableMetricsMaps(StringBuilder stringBuilder,
            Map<String, Map<MetricType, Long>> map){
        for(Map.Entry<String, Map<MetricType, Long>> entry : map.entrySet()){
            String table = entry.getKey();
            Map<MetricType, Long> metricsMap = entry.getValue();
            stringBuilder.append(String.format(METRICS_FOR_TABLE + "=%s[", table));
            buildStringForMetricsMap(stringBuilder, metricsMap);
            stringBuilder.append("]");
        }
    }

    private void buildStringForMetricsMap(StringBuilder builder, Map<MetricType, Long> map) {
        for(Map.Entry entry : map.entrySet()){
            MetricType type = (MetricType) entry.getKey();
            Long value = (Long) entry.getValue();
            builder.append(String.format("%s=%d,", type.shortName(), value));
        }
    }

    @Override
    public void logOverAllReadRequestMetrics(Map<MetricType, Long> map, String sql) {
        if(map.isEmpty()) {
            return;
        }
        StringBuilder logStringBuilder = new StringBuilder(READ_ALL + ":");
        logStringBuilder.append(String.format(LOGGED_SQL+ "=%s,", sql));
        buildStringForMetricsMap(logStringBuilder, map);
        LOGGER.info(logStringBuilder.toString());
    }

    @Override
    public void logRequestReadMetrics(Map<String, Map<MetricType, Long>> map, String sql) {
        if(map.isEmpty()) {
            return;
        }
        StringBuilder logStringBuilder = new StringBuilder(READ_TABLES + ":");
        logStringBuilder.append(String.format(LOGGED_SQL+ "=%s,", sql));
        buildStringForTableMetricsMaps(logStringBuilder, map);
        LOGGER.info(logStringBuilder.toString());
    }

    @Override
    public void logWriteMetricsfoForMutationsSinceLastReset(
            Map<String, Map<MetricType, Long>> map) {
        if(map.isEmpty()) {
            return;
        }
        StringBuilder logStringBuilder = new StringBuilder(WRITE_METRICS + ":");
        buildStringForTableMetricsMaps(logStringBuilder, map);
        LOGGER.info(logStringBuilder.toString());
    }

    @Override
    public void logReadMetricInfoForMutationsSinceLastReset(
            Map<String, Map<MetricType, Long>> map) {
        if(map.isEmpty()) {
            return;
        }
        StringBuilder logStringBuilder = new StringBuilder(READ_METRICS + ":");
        buildStringForTableMetricsMaps(logStringBuilder, map);
        LOGGER.info(logStringBuilder.toString());
    }
}