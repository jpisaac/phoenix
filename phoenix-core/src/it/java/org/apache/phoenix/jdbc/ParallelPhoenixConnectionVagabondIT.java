/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.jdbc;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.ParallelPhoenixResultSetFactory.ParallelPhoenixResultSetType;
import org.apache.phoenix.query.BaseTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityPolicy.PARALLEL;
import static org.apache.phoenix.jdbc.HighAvailabilityTestingUtility.HBaseTestingUtilityPair;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Use case Tests failover basics for {@link ParallelPhoenixConnection}.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ParallelPhoenixConnectionVagabondIT {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelPhoenixConnectionIT.class);
    private static final HBaseTestingUtilityPair CLUSTERS = new HBaseTestingUtilityPair();
    private static final Properties GLOBAL_PROPERTIES = new Properties();
    private static final String tableName = generateUniqueName();
    private static final String GROUP_CONDITION = "ORGANIZATION_ID=? and CLIENT_TYPE=? and GROUP_ID=?";
    private static final String KEY_CONDITION = GROUP_CONDITION + " and MY_KEY=?";
    private static final String KEY_CONDITION_FOR_BATCH_GET = GROUP_CONDITION + " and MY_KEY IN ";
    private static final String KEY_CONDITION_FOR_BATCH_DELETE = GROUP_CONDITION + " and MY_KEY IN ";
    private static final String UPSERT_SQL = "UPSERT INTO " + tableName + "(ORGANIZATION_ID, CLIENT_TYPE, GROUP_ID, MY_KEY, MY_VALUE, SIZE, NEXT_CHUNK, POD, CREATED_DATE, EXPIRY_DATE) values (?,?,?,?,?,?,?,?,?,?)";
    private static final String SELECT_KEY_SQL = "SELECT EXPIRY_DATE, NEXT_CHUNK, MY_VALUE, CREATED_DATE FROM " + tableName + " WHERE " + KEY_CONDITION;
    private static final String SELECT_KEY_BATCH_SQL = "SELECT EXPIRY_DATE, NEXT_CHUNK, MY_VALUE, CREATED_DATE, MY_KEY FROM " + tableName + " WHERE " + KEY_CONDITION_FOR_BATCH_GET;
    private static final String SELECT_EXISTS_KEY_SQL = "SELECT EXPIRY_DATE, CREATED_DATE FROM " + tableName + " WHERE " + KEY_CONDITION;
    private static final String SELECT_GROUP_SQL = "SELECT MY_KEY FROM " + tableName + " WHERE " + GROUP_CONDITION;
    private static final String DELETE_KEY_SQL = "DELETE FROM " + tableName + " WHERE " + KEY_CONDITION + " AND NEXT_CHUNK = FALSE";
    private static final String DELETE_KEY_BATCH_SQL = "DELETE FROM " + tableName + " WHERE " + KEY_CONDITION_FOR_BATCH_DELETE;
    private static final String DELETE_GROUP_SQL = "DELETE FROM " + tableName + " WHERE " + GROUP_CONDITION;
    private static final String ORG_ID = "org000000000000001";
    private static final String GROUP_ID = "groupId";
    private static final Instant NOW = Instant.now();
    private static List<Connection> CONNECTIONS = null;
    @Rule
    public TestName testName = new TestName();
    /**
     * Client properties to create a connection per test.
     */
    private Properties clientProperties;
    /**
     * JDBC connection string for this test HA group.
     */
    private String jdbcUrl;
    /**
     * HA group for this test.
     */
    private HighAvailabilityGroup haGroup;

    @Parameters(name="ParallelPhoenixConnectionVagabondIT_resultSetType={0}") // name is used by failsafe as file name in reports
    public static Collection<String> data() {
        return Arrays.asList(ParallelPhoenixResultSetType.PARALLEL_PHOENIX_RESULT_SET.getName(),
            ParallelPhoenixResultSetType.PARALLEL_PHOENIX_NULL_COMPARING_RESULT_SET.getName());
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        CLUSTERS.start();
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        GLOBAL_PROPERTIES.setProperty(PHOENIX_HA_GROUP_ATTR, PARALLEL.name());

        String ddl = String.format("CREATE TABLE IF NOT EXISTS %s (  \n" +
                "  ORGANIZATION_ID CHAR(18) NOT NULL,  \n" +
                "  CLIENT_TYPE VARCHAR NOT NULL,  \n" +
                "  GROUP_ID VARCHAR NOT NULL,  \n" +
                "  MY_KEY VARCHAR NOT NULL,  \n" +
                "  MY_VALUE VARBINARY,  \n" +
                "  SIZE INTEGER,\n" +
                "  NEXT_CHUNK BOOLEAN,\n" +
                "  POD VARCHAR,  \n" +
                "  CREATED_DATE DATE,\n" +
                "  EXPIRY_DATE DATE,\n" +
                "  CONSTRAINT PK_DATA PRIMARY KEY   \n" +
                "  (  \n" +
                "    ORGANIZATION_ID,  \n" +
                "    CLIENT_TYPE,  \n" +
                "    GROUP_ID,  \n" +
                "    MY_KEY  \n" +
                "  )  \n" +
                ") IMMUTABLE_ROWS=true, VERSIONS=1, DISABLE_TABLE_SOR=true, REPLICATION_SCOPE=1, TTL=864000", tableName);

        CONNECTIONS = Lists.newArrayList(CLUSTERS.getCluster0Connection(), CLUSTERS.getCluster1Connection());

        for (Connection conn : CONNECTIONS) {
            try (Statement statement = conn.createStatement()) {
                statement.execute(ddl);
            }
            conn.commit();
        }

        CLUSTERS.checkReplicationComplete();

        //preload some data
        try (Connection connection = CLUSTERS.getCluster0Connection()) {
            loadData(connection, ORG_ID, GROUP_ID, 100, 20);
        }
        CLUSTERS.checkReplicationComplete();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        for (Connection conn : CONNECTIONS) {
            conn.close();
        }

        DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        CLUSTERS.close();
    }

    private static void loadData(Connection connection, String orgId, String groupId, int rows, int batchSize) throws SQLException {
        Integer counter = 0;
        //See W-8064529 for reuse of the preparedstatement
        for (int i = 0; i < rows; i++) {
            try (PreparedStatement ps = connection.prepareStatement(UPSERT_SQL)) {
                ps.setString(1, orgId);
                ps.setString(2, "CLIENT_TYPE");
                ps.setString(3, groupId);
                ps.setString(4, String.valueOf(counter++));
                ps.setBytes(5, new byte[]{counter.byteValue()});
                ps.setInt(6, 1);
                ps.setBoolean(7, false);
                ps.setString(8, "pod");
                ps.setTimestamp(9, Timestamp.from(NOW));
                ps.setTimestamp(10, Timestamp.from(NOW.plusSeconds(3600)));
                int result = ps.executeUpdate();
                if (result != 1) {
                    throw new RuntimeException("Phoenix error: upsert count is not one. It is " + result);
                }
            }
            if (!connection.getAutoCommit() && counter % batchSize == 0) {
                connection.commit();
            }
        }
        if (!connection.getAutoCommit()) {
            connection.commit(); //send any remaining rows
        }
    }

    public ParallelPhoenixConnectionVagabondIT(String resultSetType) {
        GLOBAL_PROPERTIES.setProperty(ParallelPhoenixResultSetFactory.PHOENIX_PARALLEL_RESULTSET_TYPE, resultSetType);
    }

    @Before
    public void setup() throws Exception {
        String haGroupName = testName.getMethodName();
        clientProperties = new Properties(GLOBAL_PROPERTIES);
        clientProperties.setProperty(PHOENIX_HA_GROUP_ATTR, haGroupName);

        // Make first cluster ACTIVE
        CLUSTERS.initClusterRole(haGroupName, PARALLEL);

        jdbcUrl = String.format("jdbc:phoenix:[%s|%s]",
                CLUSTERS.getUrl1(), CLUSTERS.getUrl2());
        haGroup = HighAvailabilityTestingUtility.getHighAvailibilityGroup(jdbcUrl, clientProperties);
        LOG.info("Initialized haGroup {} with URL {}", haGroup.getGroupInfo().getName(), jdbcUrl);
    }

    @Test
    public void testBatchWrite() throws SQLException {
        int rowsToWrite = 100;
        String orgId = StringUtils.rightPad(BaseTest.generateUniqueName(), 15).substring(0, 15);
        String groupId = testName.getMethodName();
        try (Connection connection = getParallelConnection()) {
            connection.setAutoCommit(false);
            loadData(connection, orgId, groupId, rowsToWrite, 20);
        }

        CLUSTERS.checkReplicationComplete();

        //ensure values on both clusters
        String query = String.format("SELECT COUNT(*) FROM %s WHERE GROUP_ID = '%s' AND ORGANIZATION_ID = '%s'", tableName, groupId, orgId);
        for (Connection conn : CONNECTIONS) {
            try (Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(query)) {
                assertTrue(rs.next());
                assertEquals(rowsToWrite, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testSinglePut() throws SQLException {
        //put - no autocommit single key
        String orgId = StringUtils.rightPad(BaseTest.generateUniqueName(), 15).substring(0, 15);
        String groupId = testName.getMethodName();
        try (Connection connection = getParallelConnection()) {
            try (PreparedStatement ps = connection.prepareStatement(UPSERT_SQL)) {
                ps.setString(1, orgId);
                ps.setString(2, "CLIENT_TYPE");
                ps.setString(3, groupId);
                ps.setString(4, "123");
                ps.setBytes(5, orgId.getBytes());
                ps.setInt(6, 1);
                ps.setBoolean(7, false);
                ps.setString(8, "pod");
                Instant now = Instant.now();
                ps.setTimestamp(9, Timestamp.from(now));
                ps.setTimestamp(10, Timestamp.from(now.plusSeconds(3600)));
                int result = ps.executeUpdate();
                assertEquals(1,result);
            }

        }
        CLUSTERS.checkReplicationComplete();

        for (Connection conn : CONNECTIONS) {
            try (Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(String.format("SELECT * FROM %s WHERE GROUP_ID = '%s' AND ORGANIZATION_ID = '%s'", tableName, groupId, orgId))) {
                assertTrue(rs.next());
                assertEquals("123", rs.getString(4));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testSingleDelete() throws SQLException {
        String orgId = StringUtils.rightPad(BaseTest.generateUniqueName(), 15).substring(0, 15);
        String groupId = testName.getMethodName();
        loadData(CLUSTERS.getCluster0Connection(), orgId, groupId, 10, 10);
        CLUSTERS.checkReplicationComplete();

        for (Connection conn : CONNECTIONS) {
            try (Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s WHERE ORGANIZATION_ID='%s'", tableName, orgId))) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1));
                assertFalse(rs.next());
            }
        }

        //delete
        try (Connection connection = getParallelConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE_KEY_SQL)) {
            statement.setString(1, orgId);
            statement.setString(2, "CLIENT_TYPE");
            statement.setString(3, groupId);
            statement.setString(4, String.valueOf(1));
            int result = statement.executeUpdate();
            assertEquals(1, result);
        }
        CLUSTERS.checkReplicationComplete();

        for (Connection conn : CONNECTIONS) {
            try (Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s WHERE ORGANIZATION_ID='%s'", tableName, orgId))) {
                assertTrue(rs.next());
                assertEquals(9, rs.getInt(1)); // deleted from 1 org
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testBatchDelete() throws SQLException {
        String orgId = StringUtils.rightPad(BaseTest.generateUniqueName(), 15).substring(0, 15);
        String groupId = testName.getMethodName();
        loadData(CLUSTERS.getCluster0Connection(), orgId, groupId, 10, 10);
        CLUSTERS.checkReplicationComplete();

        //delete batch
        try (Connection connection = getParallelConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE_KEY_BATCH_SQL + "(?,?,?,?,?)")) {
            statement.setString(1, orgId);
            statement.setString(2, "CLIENT_TYPE");
            statement.setString(3, groupId);

            statement.setString(4, String.valueOf(1));
            statement.setString(5, String.valueOf(2));
            statement.setString(6, String.valueOf(3));
            statement.setString(7, String.valueOf(4));
            statement.setString(8, String.valueOf(5));

            statement.executeUpdate();
        }
        CLUSTERS.checkReplicationComplete();

        for (Connection conn : CONNECTIONS) {
            try (Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s WHERE ORGANIZATION_ID='%s'", tableName, orgId))) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testGroupDelete() throws SQLException {
        String orgId = StringUtils.rightPad(BaseTest.generateUniqueName(), 15).substring(0, 15);
        String groupId1 = testName.getMethodName();
        loadData(CLUSTERS.getCluster0Connection(), orgId, groupId1, 10, 10);
        String groupId2 = testName.getMethodName() + "2";
        loadData(CLUSTERS.getCluster0Connection(), orgId, groupId2, 10, 10);
        CLUSTERS.checkReplicationComplete();

        //delete group
        try (Connection connection = getParallelConnection();
             PreparedStatement statement = connection.prepareStatement(DELETE_GROUP_SQL)) {
            statement.setString(1, orgId);
            statement.setString(2, "CLIENT_TYPE");
            statement.setString(3, groupId2);
            int result = statement.executeUpdate();
        }
        CLUSTERS.checkReplicationComplete();

        for (Connection conn : CONNECTIONS) {
            try (Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(String.format("SELECT COUNT(*) FROM %s WHERE ORGANIZATION_ID='%s'", tableName, orgId))) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    //getKey
    @Test
    public void testGetKey() throws SQLException {
        try (Connection conn = getParallelConnection();
             PreparedStatement statement = conn.prepareStatement(SELECT_KEY_SQL)) {

            statement.setString(1, ORG_ID);
            statement.setString(2, "CLIENT_TYPE");
            statement.setString(3, GROUP_ID);
            statement.setString(4, "3");

            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            //counter gets incremented prior to putting value so 3+1=4
            assertArrayEquals(new byte[]{Integer.valueOf(4).byteValue()}, rs.getBytes(3));
            assertFalse(rs.next());
        }
    }

    //getKeyExists
    @Test
    public void testKeyExists() throws SQLException {
        try (Connection conn = getParallelConnection();
             PreparedStatement statement = conn.prepareStatement(SELECT_EXISTS_KEY_SQL)) {

            statement.setString(1, ORG_ID);
            statement.setString(2, "CLIENT_TYPE");
            statement.setString(3, GROUP_ID);
            statement.setString(4, "3");

            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Date expiryDate = rs.getDate("EXPIRY_DATE");
            assertEquals(Date.from(NOW.plusSeconds(3600)), expiryDate);
            assertFalse(rs.next());
        }
    }

    //getKeyBatch
    @Test
    public void testGetKeysBatch() throws SQLException {
        try (Connection conn = getParallelConnection();
             PreparedStatement statement = conn.prepareStatement(SELECT_KEY_BATCH_SQL + "(?,?,?)")) {

            statement.setString(1, ORG_ID);
            statement.setString(2, "CLIENT_TYPE");
            statement.setString(3, GROUP_ID);
            statement.setString(4, "3");
            statement.setString(5, "6");
            statement.setString(6, "71");

            ResultSet rs = statement.executeQuery();
            //counter gets incremented prior to putting value so 3+1=4,7,72
            assertTrue(rs.next());
            assertArrayEquals(new byte[]{Integer.valueOf(4).byteValue()}, rs.getBytes(3));
            assertTrue(rs.next());
            assertArrayEquals(new byte[]{Integer.valueOf(7).byteValue()}, rs.getBytes(3));
            assertTrue(rs.next());
            assertArrayEquals(new byte[]{Integer.valueOf(72).byteValue()}, rs.getBytes(3));
            assertFalse(rs.next());
        }
    }

    //getAllKeys
    @Test
    public void testGetAllKeys() throws SQLException {

        try (Connection conn = getParallelConnection();
             PreparedStatement statement = conn.prepareStatement(SELECT_GROUP_SQL)) {
            statement.setString(1, ORG_ID);
            statement.setString(2, "CLIENT_TYPE");
            statement.setString(3, GROUP_ID);


            ResultSet rs = statement.executeQuery();
            List<String> keys = Lists.newArrayListWithCapacity(100);
            for (int i = 0; i < 100; i++) {
                keys.add(String.valueOf(i));
            }
            keys.sort(String::compareTo);
            for (String key : keys) {
                assertTrue(rs.next());
                assertEquals(key, rs.getString(1));
            }
            assertFalse(rs.next());
        }
    }

    /**
     * Returns a Parallel Phoenix Connection
     *
     * @return Parallel Phoenix Connection
     * @throws SQLException
     */
    private Connection getParallelConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(jdbcUrl, clientProperties);
        connection.setAutoCommit(true);
        return connection;
    }

}
