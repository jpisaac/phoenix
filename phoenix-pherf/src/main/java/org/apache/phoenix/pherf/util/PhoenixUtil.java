/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.util;

import com.google.gson.Gson;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.mapreduce.index.automation.PhoenixMRJobSubmitter;
import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataTypeMapping;
import org.apache.phoenix.pherf.configuration.Ddl;
import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.configuration.QuerySet;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.result.DataLoadTimeSummary;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;

public class PhoenixUtil {
    public static final String ASYNC_KEYWORD = "ASYNC";
    public static final Gson GSON = new Gson();
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixUtil.class);
    private static String zookeeper;
    private static int rowCountOverride = 0;
    private boolean testEnabled;
    private static PhoenixUtil instance;
    private static boolean useThinDriver;
    private static String queryServerUrl;
    private static final int ONE_MIN_IN_MS = 60000;
    private static String CurrentSCN = null;

    private PhoenixUtil() {
        this(false);
    }

    private PhoenixUtil(final boolean testEnabled) {
        this.testEnabled = testEnabled;
    }

    public static PhoenixUtil create() {
        return create(false);
    }

    public static PhoenixUtil create(final boolean testEnabled) {
        instance = instance != null ? instance : new PhoenixUtil(testEnabled);
        return instance;
    }

    public static void useThinDriver(String queryServerUrl) {
        PhoenixUtil.useThinDriver = true;
        PhoenixUtil.queryServerUrl = Objects.requireNonNull(queryServerUrl);
    }

    public static String getQueryServerUrl() {
        return PhoenixUtil.queryServerUrl;
    }

    public static boolean isThinDriver() {
        return PhoenixUtil.useThinDriver;
    }

    public static Gson getGSON() {
        return GSON;
    }

    public Connection getConnection() throws Exception {
        return getConnection(null);
    }

    public Connection getConnection(String tenantId) throws Exception {
        return getConnection(tenantId, testEnabled, null);
    }
    
    public Connection getConnection(String tenantId, Map<String, String> phoenixProperty) throws Exception {
        return getConnection(tenantId, testEnabled, phoenixProperty);
    }

    public Connection getConnection(String tenantId, boolean testEnabled, Map<String, String> phoenixProperty) throws Exception {
        if (useThinDriver) {
            if (null == queryServerUrl) {
                throw new IllegalArgumentException("QueryServer URL must be set before" +
                      " initializing connection");
            }
            Properties props = new Properties();
            if (null != tenantId) {
                props.setProperty("TenantId", tenantId);
                LOGGER.debug("\nSetting tenantId to " + tenantId);
            }
            String url = "jdbc:phoenix:thin:url=" + queryServerUrl + ";serialization=PROTOBUF";
            return DriverManager.getConnection(url, props);
        } else {
            if (null == zookeeper) {
                throw new IllegalArgumentException(
                        "Zookeeper must be set before initializing connection!");
            }
            Properties props = new Properties();
            if (null != tenantId) {
                props.setProperty("TenantId", tenantId);
                LOGGER.debug("\nSetting tenantId to " + tenantId);
            }
            
            if (phoenixProperty != null) {
            	for (Map.Entry<String, String> phxProperty: phoenixProperty.entrySet()) {
            		props.setProperty(phxProperty.getKey(), phxProperty.getValue());
					System.out.println("Setting connection property "
							+ phxProperty.getKey() + " to "
							+ phxProperty.getValue());
            	}
            }
            
            String url = "jdbc:phoenix:" + zookeeper + (testEnabled ? ";test=true" : "");
            return DriverManager.getConnection(url, props);
        }
    }

    public boolean executeStatement(String sql, Scenario scenario) throws Exception {
        Connection connection = null;
        boolean result = false;
        try {
            connection = getConnection(scenario.getTenantId());
            result = executeStatement(sql, connection);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return result;
    }

    /**
     * Execute statement
     *
     * @param sql
     * @param connection
     * @return
     * @throws SQLException
     */
    public boolean executeStatementThrowException(String sql, Connection connection)
            throws SQLException {
        boolean result = false;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            result = preparedStatement.execute();
            connection.commit();
        } finally {
            if(preparedStatement != null) {
                preparedStatement.close();
            }
        }
        return result;
    }

    public boolean executeStatement(String sql, Connection connection) throws SQLException{
        boolean result = false;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            result = preparedStatement.execute();
            connection.commit();
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @SuppressWarnings("unused")
    public boolean executeStatement(PreparedStatement preparedStatement, Connection connection) {
        boolean result = false;
        try {
            result = preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Delete existing tables with schema name set as {@link PherfConstants#PHERF_SCHEMA_NAME} with regex comparison
     *
     * @param regexMatch
     * @throws SQLException
     * @throws Exception
     */
    public void deleteTables(String regexMatch) throws Exception {
        regexMatch = regexMatch.toUpperCase().replace("ALL", ".*");
        Connection conn = getConnection();
        try {
            ResultSet resultSet = getTableMetaData(PherfConstants.PHERF_SCHEMA_NAME, null, conn);
            while (resultSet.next()) {
                String tableName = resultSet.getString(TABLE_SCHEM) == null ? resultSet
                        .getString(TABLE_NAME) : resultSet
                        .getString(TABLE_SCHEM)
                        + "."
                        + resultSet.getString(TABLE_NAME);
                if (tableName.matches(regexMatch)) {
                    LOGGER.info("\nDropping " + tableName);
                    try {
                        executeStatementThrowException("DROP TABLE "
                                + tableName + " CASCADE", conn);
                    } catch (org.apache.phoenix.schema.TableNotFoundException tnf) {
                        LOGGER.error("Table might be already be deleted via cascade. Schema: "
                                + tnf.getSchemaName()
                                + " Table: "
                                + tnf.getTableName());
                    }
                }
            }
        } finally {
            conn.close();
        }
    }

    public void dropChildView(RegionCoprocessorEnvironment taskRegionEnvironment, int depth) {
        TaskRegionObserver.SelfHealingTask task =
                new TaskRegionObserver.SelfHealingTask(
                        taskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
        for (int i = 0; i < depth; i++) {
            task.run();
        }
    }

    public ResultSet getTableMetaData(String schemaName, String tableName, Connection connection)
            throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet resultSet = dbmd.getTables(null, schemaName, tableName, null);
        return resultSet;
    }

    public ResultSet getColumnsMetaData(String schemaName, String tableName, Connection connection)
            throws SQLException {
        DatabaseMetaData dbmd = connection.getMetaData();
        ResultSet resultSet = dbmd.getColumns(null, schemaName.toUpperCase(), tableName.toUpperCase(), null);
        return resultSet;
    }

    public synchronized List<Column> getColumnsFromPhoenix(String schemaName, String tableName,
            Connection connection) throws SQLException {
        List<Column> columnList = new ArrayList<>();
        ResultSet resultSet = null;
        try {
            resultSet = getColumnsMetaData(schemaName, tableName, connection);
            while (resultSet.next()) {
                Column column = new Column();
                column.setName(resultSet.getString("COLUMN_NAME"));
                String typeName = resultSet.getString("TYPE_NAME").replace(" ", "_");
                if (tableName.toLowerCase().contains("_dc") && "JSON".equals(typeName.toUpperCase())) {
                    typeName = "JSONDC";
                } else if (tableName.toLowerCase().contains("_bson") && column.getName().toUpperCase().contains("JSONCOL")) {
                    typeName = "BSON";
                } else if (tableName.toLowerCase().contains("_binary") &&
                        !typeName.toUpperCase().equals("INTEGER") && !typeName.toUpperCase().equals("VARCHAR")) {
                    typeName = "JSONB";
                }
                column.setType(DataTypeMapping.valueOf(typeName));
                column.setLength(resultSet.getInt("COLUMN_SIZE"));
                columnList.add(column);
                LOGGER.info(String.format("getColumnsMetaData for column name : %s", column.getName() + " resultset: " + resultSet.getString("TYPE_NAME")
                + " " + column.getType() + " " + typeName));
            }
        } finally {
            if (null != resultSet) {
                resultSet.close();
            }
        }

        return Collections.unmodifiableList(columnList);
    }

    /**
     * Execute all querySet DDLs first based on tenantId if specified. This is executed
     * first since we don't want to run DDLs in parallel to executing queries.
     *
     * @param querySet
     * @throws Exception
     */
    public void executeQuerySetDdls(QuerySet querySet) throws Exception {
        for (Query query : querySet.getQuery()) {
            if (null != query.getDdl()) {
                Connection conn = null;
                try {
                    LOGGER.info("\nExecuting DDL:" + query.getDdl() + " on tenantId:" + query
                            .getTenantId());
                    executeStatement(query.getDdl(),
                            conn = getConnection(query.getTenantId()));
                } finally {
                    if (null != conn) {
                        conn.close();
                    }
                }
            }
        }
    }
    
    /**
     * Executes any ddl defined at the scenario level. This is executed before we commence
     * the data load.
     * 
     * @throws Exception
     */
    public void executeScenarioDdl(List<Ddl> ddls, String tenantId, DataLoadTimeSummary dataLoadTimeSummary) throws Exception {
        if (null != ddls) {
            Connection conn = null;
            try {
            	for (Ddl ddl : ddls) {
                    LOGGER.info("\nExecuting DDL:" + ddl + " on tenantId:" +tenantId);
	                long startTime = EnvironmentEdgeManager.currentTimeMillis();
	                executeStatement(ddl.toString(), conn = getConnection(tenantId));
	                if (ddl.getStatement().toUpperCase().contains(ASYNC_KEYWORD)) {
	                	waitForAsyncIndexToFinish(ddl.getTableName());
	                }
	                dataLoadTimeSummary.add(ddl.getTableName(), 0,
                        (int)(EnvironmentEdgeManager.currentTimeMillis() - startTime));
            	}
            } finally {
                if (null != conn) {
                    conn.close();
                }
            }
        }
    }

    /**
     * Waits for ASYNC index to build
     * @param tableName
     * @throws InterruptedException
     */
    public void waitForAsyncIndexToFinish(String tableName) throws InterruptedException {
    	//Wait for up to 15 mins for ASYNC index build to start
    	boolean jobStarted = false;
    	for (int i=0; i<15; i++) {
    		if (isYarnJobInProgress(tableName)) {
    			jobStarted = true;
    			break;
    		}
    		Thread.sleep(ONE_MIN_IN_MS);
    	}
    	if (jobStarted == false) {
    		throw new IllegalStateException("ASYNC index build did not start within 15 mins");
    	}

    	// Wait till ASYNC index job finishes to get approximate job E2E time
    	for (;;) {
    		if (!isYarnJobInProgress(tableName))
    			break;
    		Thread.sleep(ONE_MIN_IN_MS);
    	}
    }
    
    /**
     * Checks if a YARN job with the specific table name is in progress
     * @param tableName
     * @return
     */
    boolean isYarnJobInProgress(String tableName) {
		try {
            LOGGER.info("Fetching YARN apps...");
			Set<String> response = new PhoenixMRJobSubmitter().getSubmittedYarnApps();
			for (String str : response) {
                LOGGER.info("Runnng YARN app: " + str);
				if (str.toUpperCase().contains(tableName.toUpperCase())) {
					return true;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return false;
    }

	public static String getZookeeper() {
        return zookeeper;
    }

    public static void setZookeeper(String zookeeper) {
        LOGGER.info("Setting zookeeper: " + zookeeper);
        useThickDriver(zookeeper);
    }

    public static void useThickDriver(String zookeeper) {
        PhoenixUtil.useThinDriver = false;
        PhoenixUtil.zookeeper = Objects.requireNonNull(zookeeper);
    }

    public static int getRowCountOverride() {
        return rowCountOverride;
    }

    public static void setRowCountOverride(int rowCountOverride) {
        PhoenixUtil.rowCountOverride = rowCountOverride;
    }

    /**
     * Update Phoenix table stats
     *
     * @param tableName
     * @throws Exception
     */
    public void updatePhoenixStats(String tableName, Scenario scenario) throws Exception {
        LOGGER.info("Updating stats for " + tableName);
        executeStatement("UPDATE STATISTICS " + tableName, scenario);
    }

    public String getExplainPlan(Query query) throws SQLException {
    	return getExplainPlan(query, null, null);
    }
    
    /**
     * Get explain plan for a query
     *
     * @param query
     * @param ruleApplier 
     * @param scenario 
     * @return
     * @throws SQLException
     */
    public String getExplainPlan(Query query, Scenario scenario, RulesApplier ruleApplier) throws SQLException {
        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement statement = null;
        StringBuilder buf = new StringBuilder();
        try {
            conn = getConnection(query.getTenantId());
            String explainQuery;
            if (scenario != null && ruleApplier != null) {
            	explainQuery = query.getDynamicStatement(ruleApplier, scenario);
            }
            else {
            	explainQuery = query.getStatement();
            }
            
            statement = conn.prepareStatement("EXPLAIN " + explainQuery);
            rs = statement.executeQuery();
            while (rs.next()) {
                buf.append(rs.getString(1).trim().replace(",", "-"));
            }
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) rs.close();
            if (statement != null) statement.close();
            if (conn != null) conn.close();
        }
        return buf.toString();
    }

    public PreparedStatement buildStatement(RulesApplier rulesApplier, Scenario scenario, List<Column> columns,
            PreparedStatement statement, SimpleDateFormat simpleDateFormat) throws Exception {

        int count = 1;
        for (Column column : columns) {
            DataValue dataValue = rulesApplier.getDataForRule(scenario, column);
            switch (column.getType()) {
            case VARCHAR:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.VARCHAR);
                } else {
                    statement.setString(count, dataValue.getValue());
                }
                break;
            case JSON:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.VARCHAR);
                } else {
                    statement.setString(count, dataValue.getValue());
                }
                break;
            case JSONDC:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.VARCHAR);
                } else {
                    statement.setString(count, dataValue.getValue());
                }
                break;
            case BSON:
                    if (dataValue.getValue().equals("")) {
                        statement.setNull(count, Types.VARCHAR);
                    } else {
                        statement.setString(count, dataValue.getValue());
                    }
                    break;
            case JSONB:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.VARCHAR);
                } else {
                    statement.setBytes(count, dataValue.getValue().getBytes());
                }
                break;
            case CHAR:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.CHAR);
                } else {
                    statement.setString(count, dataValue.getValue());
                }
                break;
            case DECIMAL:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.DECIMAL);
                } else {
                    statement.setBigDecimal(count, new BigDecimal(dataValue.getValue()));
                }
                break;
            case INTEGER:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.INTEGER);
                } else {
                    statement.setInt(count, Integer.parseInt(dataValue.getValue()));
                }
                break;
            case UNSIGNED_LONG:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.OTHER);
                } else {
                    statement.setLong(count, Long.parseLong(dataValue.getValue()));
                }
                break;
            case BIGINT:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.BIGINT);
                } else {
                    statement.setLong(count, Long.parseLong(dataValue.getValue()));
                }
                break;
            case TINYINT:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.TINYINT);
                } else {
                    statement.setLong(count, Integer.parseInt(dataValue.getValue()));
                }
                break;
            case DATE:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.DATE);
                } else {
                    Date
                            date =
                            new java.sql.Date(simpleDateFormat.parse(dataValue.getValue()).getTime());
                    statement.setDate(count, date);
                }
                break;
            case VARCHAR_ARRAY:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.ARRAY);
                } else {
                    Array
                            arr =
                            statement.getConnection().createArrayOf("VARCHAR", dataValue.getValue().split(","));
                    statement.setArray(count, arr);
                }
                break;
            case VARBINARY:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.VARBINARY);
                } else {
                    statement.setBytes(count, dataValue.getValue().getBytes());
                }
                break;
            case TIMESTAMP:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.TIMESTAMP);
                } else {
                    java.sql.Timestamp
                            ts =
                            new java.sql.Timestamp(simpleDateFormat.parse(dataValue.getValue()).getTime());
                    statement.setTimestamp(count, ts);
                }
                break;
            default:
                break;
            }
            count++;
        }
        return statement;
    }

    private String JsonDoc1="{\n" +
            "   \"testCnt\": \"SomeCnt1\",                    \n" +
            "   \"test\": \"test1\",\n" +
            "   \"batchNo\": 1,\n" +
            "   \"infoTop\":[\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e407a8dbd65781450\",\n" +
            "                       \"index\": 0,\n" +
            "                       \"guid\": \"4f5a46f2-7271-492a-8347-a8223516715f\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$3,746.11\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 20,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Castaneda Golden\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"AUSTEX\",\n" +
            "                       \"email\": \"castanedagolden@austex.com\",\n" +
            "                       \"phone\": \"+1 (979) 486-3061\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Urbana\",\n" +
            "                       \"state\": \"Delaware\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"322 Hancock Street, Nicut, Georgia, 5007\",\n" +
            "                       \"about\": \"Esse anim minim nostrud aliquip. Quis anim ex dolore magna exercitation deserunt minim ad do est non. Magna fugiat eiusmod incididunt cupidatat. Anim occaecat nulla cillum culpa sunt amet.\\r\\n\",\n" +
            "                       \"registered\": \"2015-11-06T01:32:28 +08:00\",\n" +
            "                       \"latitude\": 83.51654,\n" +
            "                       \"longitude\": -93.749216,\n" +
            "                       \"tags\": [\n" +
            "                       \"incididunt\",\n" +
            "                       \"nostrud\",\n" +
            "                       \"incididunt\",\n" +
            "                       \"Lorem\",\n" +
            "                       \"mollit\",\n" +
            "                       \"tempor\",\n" +
            "                       \"incididunt\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Cortez Bowman\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Larsen Wolf\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Colon Rivers\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Castaneda Golden! You have 10 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982ef091f4785f15251f\",\n" +
            "                       \"index\": 1,\n" +
            "                       \"guid\": \"bcfc487d-de23-4721-86bd-809d37a007c2\",\n" +
            "                       \"isActive\": false,\n" +
            "                       \"balance\": \"$1,539.97\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 31,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Jackson Dillard\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"QUONATA\",\n" +
            "                       \"email\": \"jacksondillard@quonata.com\",\n" +
            "                       \"phone\": \"+1 (950) 552-3553\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Cetronia\",\n" +
            "                       \"state\": \"Massachusetts\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"848 Hampton Avenue, Shasta, Marshall Islands, 6596\",\n" +
            "                       \"about\": \"Mollit nisi cillum sunt aliquip. Est ex nisi deserunt aliqua anim nisi dolor. Ullamco est consectetur deserunt do voluptate excepteur esse reprehenderit laboris officia. Deserunt sint velit mollit aliquip amet ad in tempor excepteur magna proident Lorem reprehenderit consequat.\\r\\n\",\n" +
            "                       \"registered\": \"2018-05-13T10:54:03 +07:00\",\n" +
            "                       \"latitude\": -68.213281,\n" +
            "                       \"longitude\": -147.388909,\n" +
            "                       \"tags\": [\n" +
            "                       \"adipisicing\",\n" +
            "                       \"Lorem\",\n" +
            "                       \"sit\",\n" +
            "                       \"voluptate\",\n" +
            "                       \"cupidatat\",\n" +
            "                       \"deserunt\",\n" +
            "                       \"consectetur\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Casandra Best\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Lauri Santiago\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Maricela Foster\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Jackson Dillard! You have 4 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"strawberry\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982eecb0f6158d7415b7\",\n" +
            "                       \"index\": 2,\n" +
            "                       \"guid\": \"09b31b54-6341-4a7e-8e58-bec0f766d5f4\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$1,357.52\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 20,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Battle Washington\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ONTALITY\",\n" +
            "                       \"email\": \"battlewashington@ontality.com\",\n" +
            "                       \"phone\": \"+1 (934) 429-3950\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Windsor\",\n" +
            "                       \"state\": \"Virginia\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"299 Campus Place, Innsbrook, Nevada, 4795\",\n" +
            "                       \"about\": \"Consequat voluptate nisi duis nostrud anim cupidatat officia dolore non velit Lorem. Pariatur sit consectetur do reprehenderit irure Lorem consectetur ad nostrud. Dolore tempor est fugiat officia ad nostrud. Cupidatat quis aute consectetur Lorem. Irure qui tempor deserunt nisi quis quis culpa veniam cillum est. Aute consequat pariatur ut minim sunt.\\r\\n\",\n" +
            "                       \"registered\": \"2018-12-07T03:42:53 +08:00\",\n" +
            "                       \"latitude\": -6.967753,\n" +
            "                       \"longitude\": 64.796997,\n" +
            "                       \"tags\": [\n" +
            "                       \"in\",\n" +
            "                       \"do\",\n" +
            "                       \"labore\",\n" +
            "                       \"laboris\",\n" +
            "                       \"dolore\",\n" +
            "                       \"est\",\n" +
            "                       \"nisi\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Faye Decker\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Judy Skinner\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Angie Faulkner\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Battle Washington! You have 2 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e1298ef388f75cda0\",\n" +
            "                       \"index\": 3,\n" +
            "                       \"guid\": \"deebe756-c9cd-43f5-9dd6-bc8d2edeab01\",\n" +
            "                       \"isActive\": false,\n" +
            "                       \"balance\": \"$3,684.61\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 27,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Watkins Aguirre\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"WAAB\",\n" +
            "                       \"email\": \"watkinsaguirre@waab.com\",\n" +
            "                       \"phone\": \"+1 (861) 526-2440\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Healy\",\n" +
            "                       \"state\": \"Nebraska\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"245 Bouck Court, Malo, Minnesota, 8990\",\n" +
            "                       \"about\": \"Elit fugiat aliquip occaecat nostrud deserunt eu in ut et officia pariatur ipsum non. Dolor exercitation irure cupidatat velit eiusmod voluptate esse enim. Minim aliquip do ut esse irure commodo duis aliquip deserunt ea enim incididunt. Consequat Lorem id duis occaecat proident mollit ad officia fugiat. Nostrud irure deserunt commodo consectetur cillum. Quis qui eiusmod ullamco exercitation amet do occaecat sint laboris ut laboris amet. Elit consequat fugiat cupidatat enim occaecat ullamco.\\r\\n\",\n" +
            "                       \"registered\": \"2021-05-27T03:15:12 +07:00\",\n" +
            "                       \"latitude\": 86.552038,\n" +
            "                       \"longitude\": 175.688809,\n" +
            "                       \"tags\": [\n" +
            "                       \"nostrud\",\n" +
            "                       \"et\",\n" +
            "                       \"ullamco\",\n" +
            "                       \"aliqua\",\n" +
            "                       \"minim\",\n" +
            "                       \"tempor\",\n" +
            "                       \"proident\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Dionne Lindsey\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Bonner Logan\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Neal Case\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Watkins Aguirre! You have 5 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"strawberry\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e3cb0317d825dfbb5\",\n" +
            "                       \"index\": 4,\n" +
            "                       \"guid\": \"ac778765-da9a-4923-915b-1b967e1bee96\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$2,787.54\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 34,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Barbra Fry\",\n" +
            "                       \"gender\": \"female\",\n" +
            "                       \"company\": \"SPACEWAX\",\n" +
            "                       \"email\": \"barbrafry@spacewax.com\",\n" +
            "                       \"phone\": \"+1 (895) 538-2479\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Movico\",\n" +
            "                       \"state\": \"Pennsylvania\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"812 Losee Terrace, Elbert, South Dakota, 9870\",\n" +
            "                       \"about\": \"Ea Lorem nisi aliqua incididunt deserunt sint. Cillum do magna sint quis enim velit cupidatat deserunt pariatur esse labore. Laborum velit nostrud in occaecat amet commodo enim ex commodo. Culpa do est sit reprehenderit nulla duis ex irure reprehenderit velit aliquip. Irure et eiusmod ad minim laborum ut fugiat dolore in anim mollit aliquip aliqua sunt. Commodo Lorem anim magna eiusmod.\\r\\n\",\n" +
            "                       \"registered\": \"2020-05-05T05:27:59 +07:00\",\n" +
            "                       \"latitude\": -55.592888,\n" +
            "                       \"longitude\": 68.056625,\n" +
            "                       \"tags\": [\n" +
            "                       \"magna\",\n" +
            "                       \"sint\",\n" +
            "                       \"minim\",\n" +
            "                       \"dolore\",\n" +
            "                       \"ad\",\n" +
            "                       \"exercitation\",\n" +
            "                       \"laborum\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Mccullough Roman\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Lang Morales\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Luann Carrillo\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Barbra Fry! You have 6 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e44e4e11611e5f62a\",\n" +
            "                       \"index\": 5,\n" +
            "                       \"guid\": \"d02e17de-fed9-4839-8d75-e8d05fe68c94\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$1,023.39\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 38,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Byers Grant\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ZAGGLES\",\n" +
            "                       \"email\": \"byersgrant@zaggles.com\",\n" +
            "                       \"phone\": \"+1 (992) 570-3190\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Chamberino\",\n" +
            "                       \"state\": \"North Dakota\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"826 Cumberland Street, Shaft, Washington, 424\",\n" +
            "                       \"about\": \"Deserunt tempor sint culpa in ex occaecat quis exercitation voluptate mollit occaecat officia. Aute aliquip officia id cupidatat non consectetur nulla mollit laborum ex mollit culpa exercitation. Aute nisi ullamco adipisicing sit proident proident duis. Exercitation ex id id enim cupidatat pariatur amet reprehenderit fugiat ea.\\r\\n\",\n" +
            "                       \"registered\": \"2017-10-12T04:55:42 +07:00\",\n" +
            "                       \"latitude\": -26.03892,\n" +
            "                       \"longitude\": -35.959528,\n" +
            "                       \"tags\": [\n" +
            "                       \"et\",\n" +
            "                       \"adipisicing\",\n" +
            "                       \"excepteur\",\n" +
            "                       \"do\",\n" +
            "                       \"ad\",\n" +
            "                       \"exercitation\",\n" +
            "                       \"commodo\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Louise Clarke\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Pratt Velazquez\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Violet Reyes\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Byers Grant! You have 8 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982ef6ed0ffe65e0f414\",\n" +
            "                       \"index\": 6,\n" +
            "                       \"guid\": \"37f92715-a4d1-476e-98d9-b4901426c5ea\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$2,191.12\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 33,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Rasmussen Todd\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ROUGHIES\",\n" +
            "                       \"email\": \"rasmussentodd@roughies.com\",\n" +
            "                       \"phone\": \"+1 (893) 420-3792\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Floriston\",\n" +
            "                       \"state\": \"Indiana\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"295 McClancy Place, Berlin, Federated States Of Micronesia, 303\",\n" +
            "                       \"about\": \"Est cillum fugiat reprehenderit minim minim esse qui. Eiusmod quis pariatur adipisicing sunt ipsum duis dolor veniam. Aliqua ex cupidatat officia exercitation sint duis exercitation ut. Cillum magna laboris id Lorem mollit consequat ex anim voluptate Lorem enim et velit nulla. Non consectetur incididunt id et ad tempor amet elit tempor aliquip velit incididunt esse adipisicing. Culpa pariatur est occaecat voluptate. Voluptate pariatur pariatur esse cillum proident eiusmod duis proident minim magna sit voluptate exercitation est.\\r\\n\",\n" +
            "                       \"registered\": \"2015-10-10T12:39:42 +07:00\",\n" +
            "                       \"latitude\": -20.559815,\n" +
            "                       \"longitude\": 28.453852,\n" +
            "                       \"tags\": [\n" +
            "                       \"reprehenderit\",\n" +
            "                       \"velit\",\n" +
            "                       \"non\",\n" +
            "                       \"non\",\n" +
            "                       \"veniam\",\n" +
            "                       \"laborum\",\n" +
            "                       \"duis\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Stark Carney\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Price Roberts\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Lillian Henry\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Rasmussen Todd! You have 3 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       }\n" +
            "   ]\n" +
            "}";

    private String JsonDoc2="{\n" +
            "   \"testCnt\": \"SomeCnt2\",                    \n" +
            "   \"test\": \"test2\",\n" +
            "   \"batchNo\": 2,\n" +
            "   \"infoTop\":[\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e407a8dbd65781450\",\n" +
            "                       \"index\": 0,\n" +
            "                       \"guid\": \"4f5a46f2-7271-492a-8347-a8223516715f\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$3,746.11\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 20,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Castaneda Golden\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"AUSTEX\",\n" +
            "                       \"email\": \"castanedagolden@austex.com\",\n" +
            "                       \"phone\": \"+1 (979) 486-3061\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Urbana\",\n" +
            "                       \"state\": \"Delaware\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"322 Hancock Street, Nicut, Georgia, 5007\",\n" +
            "                       \"about\": \"Esse anim minim nostrud aliquip. Quis anim ex dolore magna exercitation deserunt minim ad do est non. Magna fugiat eiusmod incididunt cupidatat. Anim occaecat nulla cillum culpa sunt amet.\\r\\n\",\n" +
            "                       \"registered\": \"2015-11-06T01:32:28 +08:00\",\n" +
            "                       \"latitude\": 83.51654,\n" +
            "                       \"longitude\": -93.749216,\n" +
            "                       \"tags\": [\n" +
            "                       \"incididunt\",\n" +
            "                       \"nostrud\",\n" +
            "                       \"incididunt\",\n" +
            "                       \"Lorem\",\n" +
            "                       \"mollit\",\n" +
            "                       \"tempor\",\n" +
            "                       \"incididunt\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Cortez Bowman\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Larsen Wolf\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Colon Rivers\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Castaneda Golden! You have 10 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982ef091f4785f15251f\",\n" +
            "                       \"index\": 1,\n" +
            "                       \"guid\": \"bcfc487d-de23-4721-86bd-809d37a007c2\",\n" +
            "                       \"isActive\": false,\n" +
            "                       \"balance\": \"$1,539.97\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 31,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Jackson Dillard\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"QUONATA\",\n" +
            "                       \"email\": \"jacksondillard@quonata.com\",\n" +
            "                       \"phone\": \"+1 (950) 552-3553\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Cetronia\",\n" +
            "                       \"state\": \"Massachusetts\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"848 Hampton Avenue, Shasta, Marshall Islands, 6596\",\n" +
            "                       \"about\": \"Mollit nisi cillum sunt aliquip. Est ex nisi deserunt aliqua anim nisi dolor. Ullamco est consectetur deserunt do voluptate excepteur esse reprehenderit laboris officia. Deserunt sint velit mollit aliquip amet ad in tempor excepteur magna proident Lorem reprehenderit consequat.\\r\\n\",\n" +
            "                       \"registered\": \"2018-05-13T10:54:03 +07:00\",\n" +
            "                       \"latitude\": -68.213281,\n" +
            "                       \"longitude\": -147.388909,\n" +
            "                       \"tags\": [\n" +
            "                       \"adipisicing\",\n" +
            "                       \"Lorem\",\n" +
            "                       \"sit\",\n" +
            "                       \"voluptate\",\n" +
            "                       \"cupidatat\",\n" +
            "                       \"deserunt\",\n" +
            "                       \"consectetur\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Casandra Best\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Lauri Santiago\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Maricela Foster\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Jackson Dillard! You have 4 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"strawberry\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982eecb0f6158d7415b7\",\n" +
            "                       \"index\": 2,\n" +
            "                       \"guid\": \"09b31b54-6341-4a7e-8e58-bec0f766d5f4\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$1,357.52\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 20,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Battle Washington\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ONTALITY\",\n" +
            "                       \"email\": \"battlewashington@ontality.com\",\n" +
            "                       \"phone\": \"+1 (934) 429-3950\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Windsor\",\n" +
            "                       \"state\": \"Virginia\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"299 Campus Place, Innsbrook, Nevada, 4795\",\n" +
            "                       \"about\": \"Consequat voluptate nisi duis nostrud anim cupidatat officia dolore non velit Lorem. Pariatur sit consectetur do reprehenderit irure Lorem consectetur ad nostrud. Dolore tempor est fugiat officia ad nostrud. Cupidatat quis aute consectetur Lorem. Irure qui tempor deserunt nisi quis quis culpa veniam cillum est. Aute consequat pariatur ut minim sunt.\\r\\n\",\n" +
            "                       \"registered\": \"2018-12-07T03:42:53 +08:00\",\n" +
            "                       \"latitude\": -6.967753,\n" +
            "                       \"longitude\": 64.796997,\n" +
            "                       \"tags\": [\n" +
            "                       \"in\",\n" +
            "                       \"do\",\n" +
            "                       \"labore\",\n" +
            "                       \"laboris\",\n" +
            "                       \"dolore\",\n" +
            "                       \"est\",\n" +
            "                       \"nisi\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Faye Decker\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Judy Skinner\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Angie Faulkner\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Battle Washington! You have 2 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e1298ef388f75cda0\",\n" +
            "                       \"index\": 3,\n" +
            "                       \"guid\": \"deebe756-c9cd-43f5-9dd6-bc8d2edeab01\",\n" +
            "                       \"isActive\": false,\n" +
            "                       \"balance\": \"$3,684.61\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 27,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Watkins Aguirre\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"WAAB\",\n" +
            "                       \"email\": \"watkinsaguirre@waab.com\",\n" +
            "                       \"phone\": \"+1 (861) 526-2440\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Healy\",\n" +
            "                       \"state\": \"Nebraska\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"245 Bouck Court, Malo, Minnesota, 8990\",\n" +
            "                       \"about\": \"Elit fugiat aliquip occaecat nostrud deserunt eu in ut et officia pariatur ipsum non. Dolor exercitation irure cupidatat velit eiusmod voluptate esse enim. Minim aliquip do ut esse irure commodo duis aliquip deserunt ea enim incididunt. Consequat Lorem id duis occaecat proident mollit ad officia fugiat. Nostrud irure deserunt commodo consectetur cillum. Quis qui eiusmod ullamco exercitation amet do occaecat sint laboris ut laboris amet. Elit consequat fugiat cupidatat enim occaecat ullamco.\\r\\n\",\n" +
            "                       \"registered\": \"2021-05-27T03:15:12 +07:00\",\n" +
            "                       \"latitude\": 86.552038,\n" +
            "                       \"longitude\": 175.688809,\n" +
            "                       \"tags\": [\n" +
            "                       \"nostrud\",\n" +
            "                       \"et\",\n" +
            "                       \"ullamco\",\n" +
            "                       \"aliqua\",\n" +
            "                       \"minim\",\n" +
            "                       \"tempor\",\n" +
            "                       \"proident\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Dionne Lindsey\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Bonner Logan\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Neal Case\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Watkins Aguirre! You have 5 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"strawberry\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e3cb0317d825dfbb5\",\n" +
            "                       \"index\": 4,\n" +
            "                       \"guid\": \"ac778765-da9a-4923-915b-1b967e1bee96\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$2,787.54\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 34,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Barbra Fry\",\n" +
            "                       \"gender\": \"female\",\n" +
            "                       \"company\": \"SPACEWAX\",\n" +
            "                       \"email\": \"barbrafry@spacewax.com\",\n" +
            "                       \"phone\": \"+1 (895) 538-2479\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Movico\",\n" +
            "                       \"state\": \"Pennsylvania\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"812 Losee Terrace, Elbert, South Dakota, 9870\",\n" +
            "                       \"about\": \"Ea Lorem nisi aliqua incididunt deserunt sint. Cillum do magna sint quis enim velit cupidatat deserunt pariatur esse labore. Laborum velit nostrud in occaecat amet commodo enim ex commodo. Culpa do est sit reprehenderit nulla duis ex irure reprehenderit velit aliquip. Irure et eiusmod ad minim laborum ut fugiat dolore in anim mollit aliquip aliqua sunt. Commodo Lorem anim magna eiusmod.\\r\\n\",\n" +
            "                       \"registered\": \"2020-05-05T05:27:59 +07:00\",\n" +
            "                       \"latitude\": -55.592888,\n" +
            "                       \"longitude\": 68.056625,\n" +
            "                       \"tags\": [\n" +
            "                       \"magna\",\n" +
            "                       \"sint\",\n" +
            "                       \"minim\",\n" +
            "                       \"dolore\",\n" +
            "                       \"ad\",\n" +
            "                       \"exercitation\",\n" +
            "                       \"laborum\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Mccullough Roman\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Lang Morales\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Luann Carrillo\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Barbra Fry! You have 6 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982e44e4e11611e5f62a\",\n" +
            "                       \"index\": 5,\n" +
            "                       \"guid\": \"d02e17de-fed9-4839-8d75-e8d05fe68c94\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$1,023.39\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 38,\n" +
            "                       \"eyeColor\": \"green\",\n" +
            "                       \"name\": \"Byers Grant\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ZAGGLES\",\n" +
            "                       \"email\": \"byersgrant@zaggles.com\",\n" +
            "                       \"phone\": \"+1 (992) 570-3190\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Chamberino\",\n" +
            "                       \"state\": \"North Dakota\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"826 Cumberland Street, Shaft, Washington, 424\",\n" +
            "                       \"about\": \"Deserunt tempor sint culpa in ex occaecat quis exercitation voluptate mollit occaecat officia. Aute aliquip officia id cupidatat non consectetur nulla mollit laborum ex mollit culpa exercitation. Aute nisi ullamco adipisicing sit proident proident duis. Exercitation ex id id enim cupidatat pariatur amet reprehenderit fugiat ea.\\r\\n\",\n" +
            "                       \"registered\": \"2017-10-12T04:55:42 +07:00\",\n" +
            "                       \"latitude\": -26.03892,\n" +
            "                       \"longitude\": -35.959528,\n" +
            "                       \"tags\": [\n" +
            "                       \"et\",\n" +
            "                       \"adipisicing\",\n" +
            "                       \"excepteur\",\n" +
            "                       \"do\",\n" +
            "                       \"ad\",\n" +
            "                       \"exercitation\",\n" +
            "                       \"commodo\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Louise Clarke\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Pratt Velazquez\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Violet Reyes\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Byers Grant! You have 8 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"_id\": \"618d982ef6ed0ffe65e0f414\",\n" +
            "                       \"index\": 6,\n" +
            "                       \"guid\": \"37f92715-a4d1-476e-98d9-b4901426c5ea\",\n" +
            "                       \"isActive\": true,\n" +
            "                       \"balance\": \"$2,191.12\",\n" +
            "                       \"picture\": \"http://placehold.it/32x32\",\n" +
            "                       \"age\": 33,\n" +
            "                       \"eyeColor\": \"brown\",\n" +
            "                       \"name\": \"Rasmussen Todd\",\n" +
            "                       \"gender\": \"male\",\n" +
            "                       \"company\": \"ROUGHIES\",\n" +
            "                       \"email\": \"rasmussentodd@roughies.com\",\n" +
            "                       \"phone\": \"+1 (893) 420-3792\",\n" +
            "                       \"info\": {\n" +
            "                       \"address\": {\n" +
            "                       \"street\": \"function\",\n" +
            "                       \"town\": \"Floriston\",\n" +
            "                       \"state\": \"Indiana\"\n" +
            "                       }\n" +
            "                       },\n" +
            "                       \"address\": \"295 McClancy Place, Berlin, Federated States Of Micronesia, 303\",\n" +
            "                       \"about\": \"Est cillum fugiat reprehenderit minim minim esse qui. Eiusmod quis pariatur adipisicing sunt ipsum duis dolor veniam. Aliqua ex cupidatat officia exercitation sint duis exercitation ut. Cillum magna laboris id Lorem mollit consequat ex anim voluptate Lorem enim et velit nulla. Non consectetur incididunt id et ad tempor amet elit tempor aliquip velit incididunt esse adipisicing. Culpa pariatur est occaecat voluptate. Voluptate pariatur pariatur esse cillum proident eiusmod duis proident minim magna sit voluptate exercitation est.\\r\\n\",\n" +
            "                       \"registered\": \"2015-10-10T12:39:42 +07:00\",\n" +
            "                       \"latitude\": -20.559815,\n" +
            "                       \"longitude\": 28.453852,\n" +
            "                       \"tags\": [\n" +
            "                       \"reprehenderit\",\n" +
            "                       \"velit\",\n" +
            "                       \"non\",\n" +
            "                       \"non\",\n" +
            "                       \"veniam\",\n" +
            "                       \"laborum\",\n" +
            "                       \"duis\"\n" +
            "                       ],\n" +
            "                       \"friends\": [\n" +
            "                       {\n" +
            "                       \"id\": 0,\n" +
            "                       \"name\": \"Stark Carney\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 1,\n" +
            "                       \"name\": \"Price Roberts\"\n" +
            "                       },\n" +
            "                       {\n" +
            "                       \"id\": 2,\n" +
            "                       \"name\": \"Lillian Henry\"\n" +
            "                       }\n" +
            "                       ],\n" +
            "                       \"greeting\": \"Hello, Rasmussen Todd! You have 3 unread messages.\",\n" +
            "                       \"favoriteFruit\": \"banana\"\n" +
            "                       }\n" +
            "   ]\n" +
            "}";

    private static boolean aBoolean = false;
    public PreparedStatement buildStatement2(RulesApplier rulesApplier, String tableName, Scenario scenario, List<Column> columns,
                                            PreparedStatement statement, SimpleDateFormat simpleDateFormat) throws Exception {

        statement.setInt(1, Integer.parseInt(rulesApplier.getDataForRule(scenario, columns.get(0)).getValue()));
        statement.setString(2, rulesApplier.getDataForRule(scenario, columns.get(1)).getValue());
        if (tableName.toUpperCase().contains("_DC")) {
            statement.setString(3, rulesApplier.getDataForRule(scenario, columns.get(2)).getValue());
        } else if  (tableName.toUpperCase().contains("_BINARY")) {
            statement.setBytes(3, rulesApplier.getDataForRule(scenario, columns.get(2)).getValue().getBytes());
        } else if  (tableName.toUpperCase().contains("_BSON")) {
            Column col = new Column();
            col.setName(columns.get(2).getName());
            col.setLength(columns.get(2).getLength());
            col.setType(DataTypeMapping.BSON);
            //LOGGER.info("GOKCEN data for rule for BSON:" + rulesApplier.getDataForRule(scenario, col).getValue());
            statement.setString(3, rulesApplier.getDataForRule(scenario, col).getValue());
//            if (aBoolean) {
//                statement.setString(3, JsonDoc2);
//            } else {
//                statement.setString(3, JsonDoc1);
//            }
//            aBoolean = !aBoolean;
        } else {
            statement.setString(3, rulesApplier.getDataForRule(scenario, columns.get(2)).getValue());
        }

        return statement;
    }

    public String buildSql(final List<Column> columns, final String tableName) {
        StringBuilder builder = new StringBuilder();
        builder.append("upsert into ");
        builder.append(tableName);
        builder.append(" (");
        int count = 1;
        for (Column column : columns) {
            builder.append(column.getName());
            if (count < columns.size()) {
                builder.append(",");
            } else {
                builder.append(")");
            }
            count++;
        }
        builder.append(" VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i < columns.size() - 1) {
                builder.append("?,");
            } else {
                builder.append("?)");
            }
        }
        return builder.toString();
    }

    public org.apache.hadoop.hbase.util.Pair<Long, Long> getResults(
            Query query,
            ResultSet rs,
            String queryIteration,
            boolean isSelectCountStatement,
            Long queryStartTime) throws Exception {

        Long resultRowCount = 0L;
        while (rs.next()) {
            if (isSelectCountStatement) {
                resultRowCount = rs.getLong(1);
            } else {
                resultRowCount++;
            }
            long queryElapsedTime = EnvironmentEdgeManager.currentTimeMillis() - queryStartTime;
            if (queryElapsedTime >= query.getTimeoutDuration()) {
                LOGGER.error("Query " + queryIteration + " exceeded timeout of "
                        +  query.getTimeoutDuration() + " ms at " + queryElapsedTime + " ms.");
                return new org.apache.hadoop.hbase.util.Pair(resultRowCount, queryElapsedTime);
            }
        }
        return new org.apache.hadoop.hbase.util.Pair(resultRowCount, EnvironmentEdgeManager.currentTimeMillis() - queryStartTime);
    }

}
