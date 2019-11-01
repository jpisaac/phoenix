/*
 * Copyright, 1999-2019, SALESFORCE.com
 * All Rights Reserved
 * Company Confidential
 */
package org.apache.phoenix.end2end.salesforce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.jdbc.LoggingPhoenixResultSet;
import org.apache.phoenix.jdbc.salesforce.SfdcLoggingPhoenixConnection;
import org.apache.phoenix.jdbc.salesforce.SfdcPhoenixMetricsLog;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.apache.phoenix.jdbc.salesforce.SfdcLoggingPhoenixConnection.SQL_NOT_LOGGED;
import static org.apache.phoenix.jdbc.salesforce.SfdcPhoenixMetricsLog.LOGGED_SQL;
import static org.apache.phoenix.jdbc.salesforce.SfdcPhoenixMetricsLog.METRICS_FOR_TABLE;
import static org.apache.phoenix.jdbc.salesforce.SfdcPhoenixMetricsLog.READ_ALL;
import static org.apache.phoenix.jdbc.salesforce.SfdcPhoenixMetricsLog.READ_TABLES;
import static org.apache.phoenix.jdbc.salesforce.SfdcPhoenixMetricsLog.WRITE_METRICS;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_SIZE;
import static org.apache.phoenix.monitoring.MetricType.NUM_PARALLEL_SCANS;
import static org.junit.Assert.assertTrue;

/**
 * Logging tests will not work in intellij until unreleased 2019.3
 * see https://youtrack.jetbrains.com/issue/IDEA-122783
 * run with mvn test -Dtest=SfdcLoggingPhoenixDriverIT
 */
public class SfdcLoggingPhoenixDriverIT extends BaseUniqueNamesOwnClusterIT {

    private TestLogger logger;
    private static final Properties props = new Properties();

    @BeforeClass
    public static void setUp() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        final HBaseTestingUtility hbaseTestUtil = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        conf.set(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        hbaseTestUtil.startMiniCluster();
        final String zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
    }

    @Before
    public void clearLogger() {
        logger = TestLoggerFactory.getTestLogger(SfdcPhoenixMetricsLog.class);
        logger.clear();
    }

    @Test
    public void testLoggingPhoenixConnection() throws Exception {
        try (Connection conn = DriverManager.getConnection(url)) {
            assertTrue(conn instanceof SfdcLoggingPhoenixConnection);
        }
    }

    @Test
    public void testWriteMetricsAreLogged() throws Exception {
        final String baseTableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(url, props)) {
            createTableAndUpsertOneRecord(conn, baseTableName);
        }
        String logLine = logger.getLoggingEvents().get(0).getMessage();
        assertTrue(logLine.contains(WRITE_METRICS));
        assertTrue(logLine.contains(METRICS_FOR_TABLE));
        assertTrue(logLine.contains(baseTableName));
        // We upserted just 1 record so mutation batch size should be 1
        assertTrue(logLine.contains(MUTATION_BATCH_SIZE.shortName() + "=1"));
    }

    @Test
    public void testReadMetricsAndSQLAreLoggedResultSetClosed() throws Exception {
        readMetricsLoggingHelper(true);
    }

    @Test
    public void testReadMetricsAreLoggedButSQLIsNotResultSetNotClosed() throws Exception {
        readMetricsLoggingHelper(false);
    }

    private void createTableAndUpsertOneRecord(Connection conn, String baseTableName) throws
            SQLException {
        // Create base table
        String baseTableDdl = "CREATE TABLE " + baseTableName + " (" +
                "PK CHAR(1) NOT NULL PRIMARY KEY," +
                "V1 CHAR(1), V2 CHAR(1))";
        conn.createStatement().execute(baseTableDdl);
        String upsertSql = "UPSERT INTO " + baseTableName + " VALUES (" +
                "'A', 'B', 'C')";
        conn.createStatement().execute(upsertSql);
        conn.commit();
    }

    private void readMetricsLoggingHelper(boolean closeResultSet) throws SQLException {
        final String baseTableName = generateUniqueName();
        String query = "SELECT * FROM " + baseTableName;
        try (Connection conn = DriverManager.getConnection(url, props)) {
            createTableAndUpsertOneRecord(conn, baseTableName);
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs instanceof LoggingPhoenixResultSet);
            while(rs.next()) {
                // do nothing
            }
            if (closeResultSet) {
                rs.close();
            }
        }
        assertReadLogLines(baseTableName, closeResultSet ? query : SQL_NOT_LOGGED);
    }

    private void assertReadLogLines(String baseTableName, String loggedQuery) {
        // We log 1 line for overall read metrics and 1 for read metrics specific to the queried table
        String readLogLines = logger.getLoggingEvents().get(1).getMessage() +
                logger.getLoggingEvents().get(2).getMessage();
        assertTrue(readLogLines.contains(READ_ALL));
        assertTrue(readLogLines.contains(READ_TABLES));
        assertTrue(readLogLines.contains(METRICS_FOR_TABLE));
        assertTrue(readLogLines.contains(baseTableName));
        assertTrue(readLogLines.contains(LOGGED_SQL + "=" + loggedQuery));
        // We queried just 1 record so number of scans executed in parallel should be 1
        assertTrue(readLogLines.contains(NUM_PARALLEL_SCANS.shortName() + "=1"));
    }

}
