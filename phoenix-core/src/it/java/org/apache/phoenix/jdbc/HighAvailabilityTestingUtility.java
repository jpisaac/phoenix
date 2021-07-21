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

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.GenericTestUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.PhoenixHAAdminTool.PhoenixHAAdminHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.test.GenericTestUtils.waitFor;
import static org.apache.phoenix.hbase.index.write.AbstractParallelWriterIndexCommitter.NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY;
import static org.apache.phoenix.jdbc.ClusterRoleRecordGeneratorTool.PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_KEY;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_ZK_RETRY_BASE_SLEEP_MS_KEY;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_ZK_RETRY_MAX_KEY;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_ZK_RETRY_MAX_SLEEP_MS_KEY;
import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_ZK_SESSION_TIMEOUT_MS_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Utility class for testing HBase failover.
 */
public class HighAvailabilityTestingUtility {
    private static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityTestingUtility.class);

    /**
     * Utility class for creating and maintaining HBase DR cluster pair.
     *
     * TODO: @Bharath check if we can use utility from upstream HBase for a pair of mini clusters.
     */
    public static class HBaseTestingUtilityPair implements Closeable {
        private final HBaseTestingUtility hbaseCluster1 = new HBaseTestingUtility();
        private final HBaseTestingUtility hbaseCluster2 = new HBaseTestingUtility();
        /** The host:port:/hbase format of the JDBC string for HBase cluster 1. */
        private String url1;
        /** The host:port:/hbase format of the JDBC string for HBase cluster 2. */
        private String url2;
        private PhoenixHAAdminHelper haAdmin1;
        private PhoenixHAAdminHelper haAdmin2;
        private Admin admin1;
        private Admin admin2;

        public HBaseTestingUtilityPair() {
            Configuration conf1 = hbaseCluster1.getConfiguration();
            Configuration conf2 = hbaseCluster2.getConfiguration();
            setUpDefaultHBaseConfig(conf1);
            setUpDefaultHBaseConfig(conf2);
        }

        /**
         * Instantiates and starts the two mini HBase DR cluster. Enable peering if configured.
         *
         * @throws Exception if fails to start either cluster
         */
        public void start() throws Exception {
            hbaseCluster1.startMiniCluster();
            hbaseCluster2.startMiniCluster();

            url1 = String.format("localhost:%d:/hbase", hbaseCluster1.getZkCluster().getClientPort());
            url2 = String.format("localhost:%d:/hbase", hbaseCluster2.getZkCluster().getClientPort());

            haAdmin1 = new PhoenixHAAdminHelper(getUrl1(), hbaseCluster1.getConfiguration(), PhoenixHAAdminTool.HighAvailibilityCuratorProvider.INSTANCE);
            haAdmin2 = new PhoenixHAAdminHelper(getUrl2(), hbaseCluster2.getConfiguration(), PhoenixHAAdminTool.HighAvailibilityCuratorProvider.INSTANCE);

            admin1 = hbaseCluster1.getHBaseAdmin();
            admin2 = hbaseCluster2.getHBaseAdmin();

            // Enable replication between the two HBase clusters.
            ReplicationPeerConfig replicationPeerConfig1 = new ReplicationPeerConfig();
            replicationPeerConfig1.setClusterKey(hbaseCluster2.getClusterKey());
            ReplicationPeerConfig replicationPeerConfig2 = new ReplicationPeerConfig();
            replicationPeerConfig2.setClusterKey(hbaseCluster1.getClusterKey());

            ReplicationAdmin replicationAdmin1 = new ReplicationAdmin(
                    hbaseCluster1.getConfiguration());
            replicationAdmin1.addPeer(PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT, replicationPeerConfig1);
            replicationAdmin1.close();

            ReplicationAdmin replicationAdmin2 = new ReplicationAdmin(
                    hbaseCluster2.getConfiguration());
            replicationAdmin2.addPeer(PHOENIX_HA_GROUP_STORE_PEER_ID_DEFAULT, replicationPeerConfig2);
            replicationAdmin2.close();

            LOG.info("MiniHBase DR cluster pair is ready for testing.  Cluster Urls [{},{}]",
                    getUrl1(), getUrl2());
            logClustersStates();
        }

        /** initialize two ZK clusters for cluster role znode. */
        public void initClusterRole(String haGroupName, HighAvailabilityPolicy policy)
                throws Exception {
            ClusterRoleRecord record = new ClusterRoleRecord(
                    haGroupName, policy,
                    getUrl1(), ClusterRole.ACTIVE,
                    getUrl2(), ClusterRole.STANDBY,
                    1);
            haAdmin1.createOrUpdateDataOnZookeeper(record);
            haAdmin2.createOrUpdateDataOnZookeeper(record);
        }

        /**
         * Set cluster roles for an HA group and wait the cluster role transition to happen.
         *
         * @param haGroup the HA group name
         * @param role1 cluster role for the first cluster in the group
         * @param role2 cluster role for the second cluster in the group
         */
        public void transitClusterRole(HighAvailabilityGroup haGroup, ClusterRole role1,
                ClusterRole role2) throws Exception {
            final ClusterRoleRecord newRoleRecord = new ClusterRoleRecord(
                    haGroup.getGroupInfo().getName(), haGroup.getRoleRecord().getPolicy(),
                    getUrl1(), role1,
                    getUrl2(), role2,
                    haGroup.getRoleRecord().getVersion() + 1); // always use a newer version
            LOG.info("Transiting cluster role for HA group {} V{}->V{}, existing: {}, new: {}",
                    haGroup.getGroupInfo().getName(), haGroup.getRoleRecord().getVersion(),
                    newRoleRecord.getVersion(), haGroup.getRoleRecord(), newRoleRecord);
            boolean successAtLeastOnce = false;
            try {
                haAdmin1.createOrUpdateDataOnZookeeper(newRoleRecord);
                successAtLeastOnce = true;
            } catch (IOException e) {
                LOG.warn("Fail to update new record on {} because {}", getUrl1(), e.getMessage());
            }
            try {
                haAdmin2.createOrUpdateDataOnZookeeper(newRoleRecord);
                successAtLeastOnce = true;
            } catch (IOException e) {
                LOG.warn("Fail to update new record on {} because {}", getUrl2(), e.getMessage());
            }

            if (!successAtLeastOnce) {
                throw new IOException("Failed to update the new role record on either cluster");
            }
            // wait for the cluster roles are populated into client side from ZK nodes.
            waitFor(() -> newRoleRecord.equals(haGroup.getRoleRecord()), 1000, 120_000);

            LOG.info("Now the HA group {} should have detected and updated V{} cluster role record",
                    haGroup, newRoleRecord.getVersion());
        }

        /**
         * Log the state of both clusters
         */
        public void logClustersStates() {
            String cluster1Status, cluster2Status;
            try {
                cluster1Status = admin1.getClusterStatus().toString();
            } catch (IOException e) {
                cluster1Status = "Unable to get cluster status.";
            }
            try {
                cluster2Status = admin2.getClusterStatus().toString();
            } catch (IOException e){
                cluster2Status = "Unable to get cluster status.";
            }
            LOG.info("Cluster Status [\n{},\n{}\n]", cluster1Status, cluster2Status);
        }

        /**
         * @return testing utility for cluster 1
         */
        public HBaseTestingUtility getHBaseCluster1() {
            return hbaseCluster1;
        }

        /**
         * @return testing utility for cluster 2
         */
        public HBaseTestingUtility getHBaseCluster2() {
            return hbaseCluster2;
        }

        /**
         * Returns a Phoenix Connection to a cluster
         * @param clusterIndex 0 based
         * @return a Phoenix Connection to the indexed cluster
         */
        public Connection getClusterConnection(int clusterIndex) throws SQLException {
            String clusterUrl = clusterIndex == 0 ? getUrl1() : getUrl2();
            Properties props = new Properties();
            String url = String.format("jdbc:phoenix:%s", clusterUrl);
            return DriverManager.getConnection(url, props);
        }

        /**
         * Returns a Phoenix Connection to cluster 0
         * @return a Phoenix Connection to the cluster
         */
        public Connection getCluster0Connection() throws SQLException {
            return getClusterConnection(0);
        }

        /**
         * Returns a Phoenix Connection to cluster 1
         * @return a Phoenix Connection to the cluster
         */
        public Connection getCluster1Connection() throws SQLException {
            return getClusterConnection(1);
        }

        //TODO: Replace with a real check for replication complete
        /**
         * Checks/waits with timeout if replication is done
         * @return true if replication is done else false
         */
        public boolean checkReplicationComplete() {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        /** A Testable interface that can throw checked exception. */
        @FunctionalInterface
        public interface Testable {
            void test() throws Exception;
        }

        /**
         * Shutdown mini HBase cluster, do the test, and restart the HBase cluster.
         *
         * Please note the ZK cluster and DFS cluster are untouched.
         *
         * @param cluster the HBase cluster facility whose HBase cluster to restart
         * @param testable testing logic that is runnable
         * @throws Exception if fails to stop, test or restart HBase cluster
         */
        public static void doTestWhenOneHBaseDown(HBaseTestingUtility cluster, Testable testable)
                throws Exception {
            final int zkClientPort = cluster.getZkCluster().getClientPort();
            try {
                LOG.info("Shutting down HBase cluster using ZK localhost:{}", zkClientPort);
                cluster.shutdownMiniHBaseCluster();
                LOG.info("Start testing when HBase is down using ZK localhost:{}", zkClientPort);
                testable.test();
                LOG.info("Test succeeded when HBase is down using ZK localhost:{}", zkClientPort);
            } finally {
                LOG.info("Finished testing when HBase is down using ZK localhost:{}", zkClientPort);
                cluster.startMiniHBaseCluster(1, 1);
                LOG.info("Restarted HBase cluster using ZK localhost:{}", zkClientPort);
            }
        }

        /**
         * Shutdown mini ZK and HBase cluster, do the test, and restart both HBase and ZK cluster.
         *
         * @param cluster the HBase cluster facility that has mini ZK, DFS and HBase cluster
         * @param testable testing logic that is runnable
         * @throws Exception if fails to stop, test or restart
         */
        public static void doTestWhenOneZKDown(HBaseTestingUtility cluster, Testable testable)
                throws Exception {
            final int zkClientPort = cluster.getZkCluster().getClientPort();
            try {
                LOG.info("Shutting down HBase cluster using ZK localhost:{}", zkClientPort);
                cluster.shutdownMiniHBaseCluster();
                LOG.info("Shutting down ZK cluster at localhost:{}", zkClientPort);
                cluster.shutdownMiniZKCluster();
                LOG.info("Start testing when ZK & HBase is down at localhost:{}", zkClientPort);
                testable.test();
                LOG.info("Test succeeded when ZK & HBase is down at localhost:{}", zkClientPort);
            } finally {
                LOG.info("Finished testing when ZK & HBase is down at localhost:{}", zkClientPort);
                cluster.startMiniZKCluster(1, zkClientPort);
                LOG.info("Restarted ZK cluster at localhost:{}", zkClientPort);
                cluster.startMiniHBaseCluster(1, 1);
                LOG.info("Restarted HBase cluster using ZK localhost:{}", zkClientPort);
            }
        }

        /**
         * @return the JDBC connection URL for this pair of HBase cluster in the HA format
         */
        public String getJdbcUrl() {
            return String.format("jdbc:phoenix:[%s|%s]", url1, url2);
        }

        public String getUrl1() {
            return url1;
        }

        public String getUrl2() {
            return url2;
        }

        /**
         * @return a ZK client by curator framework for the cluster 1.
         */
        public CuratorFramework createCurator1() throws IOException {
            Properties properties = new Properties();
            getHBaseCluster1().getConfiguration()
                    .iterator()
                    .forEachRemaining(k -> properties.setProperty(k.getKey(), k.getValue()));
            return HighAvailabilityGroup.getCurator(getUrl1(), properties);
        }

        /**
         * @return a ZK client by curator framework for the cluster 2.
         */
        public CuratorFramework createCurator2() throws IOException {
            Properties properties = new Properties();
            getHBaseCluster2().getConfiguration()
                    .iterator()
                    .forEachRemaining(k -> properties.setProperty(k.getKey(), k.getValue()));
            return HighAvailabilityGroup.getCurator(getUrl2(), properties);
        }

        /**
         * Create table on two clusters and enable replication.
         *
         * @param tableName the table name
         * @throws SQLException if error happens
         */
        public void createTableOnClusterPair(String tableName) throws SQLException {
            createTableOnClusterPair(tableName, true);
        }

        /**
         * Create table on two clusters.
         *
         * If the replication scope is true then enable replication for this table.
         *
         * @param tableName the table name
         * @param replicationScope the table replication scope true=1 and false=0
         * @throws SQLException if error happens
         */
        public void createTableOnClusterPair(String tableName, boolean replicationScope)
                throws SQLException {
            for (String url : Arrays.asList(getUrl1(), getUrl2())) {
                String jdbcUrl = String.format("jdbc:phoenix:%s", url);
                try (Connection conn = DriverManager.getConnection(jdbcUrl, new Properties())) {
                    conn.createStatement().execute(String.format(
                            "CREATE TABLE IF NOT EXISTS %s (\n"
                                    + "id INTEGER PRIMARY KEY,\n"
                                    + "v INTEGER\n"
                                    + ") REPLICATION_SCOPE=%d",
                            tableName, replicationScope ? 1 : 0));
                    conn.createStatement().execute(
                            String.format("CREATE LOCAL INDEX IF NOT EXISTS IDX_%s ON %s(v)",
                                    tableName, tableName));
                    conn.commit();
                    /*
                    This is being called to clear the metadata coprocessor global cache singleton
                    which otherwise short circuits the table creation on the 2nd call
                    As the 2 region servers share a jvm
                     */
                    ((PhoenixConnection) conn).getQueryServices().clearCache();
                }
            }
            LOG.info("Created table {} on cluster pair {}", tableName, this);
        }

        /**
         * Create multi-tenant table and view with randomly generated table name.
         *
         * @param tableName the table name
         * @throws SQLException if error happens
         */
        public void createTenantSpecificTable(String tableName) throws SQLException {
            for (String url : Arrays.asList(getUrl1(), getUrl2())) {
                String jdbcUrl = String.format("jdbc:phoenix:%s", url);
                try (Connection conn = DriverManager.getConnection(jdbcUrl, new Properties())) {
                    conn.createStatement().execute(String.format(
                            "CREATE TABLE IF NOT EXISTS %s (\n"
                                    + "tenant_id VARCHAR NOT NULL,\n"
                                    + "id INTEGER NOT NULL,\n"
                                    + "v INTEGER\n"
                                    + "CONSTRAINT pk PRIMARY KEY (tenant_id, id)"
                                    + ") REPLICATION_SCOPE=1, MULTI_TENANT=true",
                            tableName));
                    conn.commit();
                    /*
                    This is being called to clear the metadata coprocessor global cache singleton
                    which otherwise short circuits the table creation on the 2nd call
                    As the 2 region servers share a jvm
                     */
                    ((PhoenixConnection) conn).getQueryServices().clearCache();
                }
            }
            LOG.info("Created multi-tenant table {} on cluster pair {}", tableName, this);
        }

        /**
         * Shuts down the two hbase clusters.
         */
        @Override
        public void close() throws IOException {
            haAdmin1.close();
            haAdmin2.close();
            admin1.close();
            admin2.close();
            try {
                hbaseCluster1.shutdownMiniCluster();
                hbaseCluster2.shutdownMiniCluster();
            } catch (Exception e) {
                LOG.error("Got exception to close HBaseTestingUtilityPair", e);
                throw new IOException(e);
            }
            LOG.info("Cluster pair {} is closed successfully.", this);
        }

        @Override
        public String toString() {
            return "HBaseTestingUtilityPair{" + getUrl1() + ", " + getUrl2() + "}";
        }

        /** Sets up the default HBase configuration for Phoenix HA testing. */
        private static void setUpDefaultHBaseConfig(Configuration conf) {
            // Set Phoenix HA timeout for ZK client to be a smaller number
            conf.setInt(PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_KEY, 1000);
            conf.setInt(PHOENIX_HA_ZK_SESSION_TIMEOUT_MS_KEY, 1000);
            conf.setInt(PHOENIX_HA_ZK_RETRY_BASE_SLEEP_MS_KEY, 100);
            conf.setInt(PHOENIX_HA_ZK_RETRY_MAX_KEY, 2);
            conf.setInt(PHOENIX_HA_ZK_RETRY_MAX_SLEEP_MS_KEY, 1000);

            // Set Phoenix related settings, eg. for index management
            conf.set(IndexManagementUtil.WAL_EDIT_CODEC_CLASS_KEY,
                    IndexManagementUtil.INDEX_WAL_EDIT_CODEC_CLASS_NAME);
            // set the server rpc scheduler factory, used to configure the cluster
            conf.set(RSRpcServices.REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
                    PhoenixRpcSchedulerFactory.class.getName());
            conf.setInt(NUM_CONCURRENT_INDEX_WRITER_THREADS_CONF_KEY, 1);

            // Make HBase cluster not fail too fast for debbuging
            conf.setLong(HConstants.ZK_SESSION_TIMEOUT, 1_000_000);
            conf.setLong(HConstants.ZOOKEEPER_TICK_TIME, 6_000);
            conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
            // Make HBase run faster by skipping sanity checks
            conf.setBoolean("hbase.unsafe.stream.capability.enforcefalse", false);
            conf.setBoolean("hbase.procedure.store.wal.use.hsync", false);
            conf.setBoolean("hbase.table.sanity.checks", false);
            /*
             * The default configuration of mini cluster ends up spawning a lot of threads
             * that are not really needed by phoenix for test purposes. Limiting these threads
             * helps us in running several mini clusters at the same time without hitting
             * the threads limit imposed by the OS.
             */
            conf.setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 5);
            conf.setInt("hbase.regionserver.metahandler.count", 2);
            conf.setInt(HConstants.MASTER_HANDLER_COUNT, 2);
            conf.setInt("dfs.namenode.handler.count", 2);
            conf.setInt("dfs.namenode.service.handler.count", 2);
            conf.setInt("dfs.datanode.handler.count", 2);
            conf.setInt("ipc.server.read.threadpool.size", 2);
            conf.setInt("ipc.server.handler.threadpool.size", 2);
            conf.setInt("hbase.regionserver.hlog.syncer.count", 2);
            conf.setInt("hbase.hfile.compaction.discharger.interval", 5000);
            conf.setInt("hbase.hlog.asyncer.number", 2);
            conf.setInt("hbase.assignment.zkevent.workers", 5);
            conf.setInt("hbase.assignment.threads.max", 5);
            conf.setInt("hbase.catalogjanitor.interval", 5000);

            // Hadoop cluster settings to avoid failing tests
            conf.setInt(DFS_REPLICATION_KEY, 1); // we only need one replica for testing
        }
    }

    /**
     * This tests that basic operation using a connection works.
     *
     * @param conn the JDBC connection to test; could be a tenant specific connection
     * @param tableName the table name to test
     * @param haGroupName the HA Group name for this test
     * @throws SQLException if it fails to test
     */
    public static void doTestBasicOperationsWithConnection(Connection conn, String tableName, String haGroupName)
            throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            assertNotNull(conn.getClientInfo());
            assertEquals(haGroupName, conn.getClientInfo(PHOENIX_HA_GROUP_ATTR));
            doTestBasicOperationsWithStatement(conn, stmt, tableName);
        }
    }

    /**
     * This tests that basic operation using a Statement works.
     *
     * @param conn the JDBC connection from which the statement was created
     * @param stmt the JDBC statement to test
     * @param tableName the table name to test
     * @throws SQLException if it fails to test
     */
    public static void doTestBasicOperationsWithStatement(Connection conn, Statement stmt,
            String tableName) throws SQLException {
        int id = RandomUtils.nextInt();
        stmt.executeUpdate(String.format("UPSERT INTO %s VALUES(%d, 1984)", tableName, id));
        conn.commit();

        try (ResultSet rs = conn.createStatement().executeQuery(
                String.format("SELECT v FROM %s WHERE id = %d", tableName, id))) {
            assertTrue(rs.next());
            assertEquals(1984, rs.getInt(1));
        }
    }

    /**
     * Wrapper for HighAvailibilityGroup::get with some retries built in for timing issues on establishing connection
     * This helper method will wait up to 3 minutes to retrieve the HA Group
     */
    public static HighAvailabilityGroup getHighAvailibilityGroup(String jdbcUrl, Properties clientProperties) throws TimeoutException, InterruptedException {
        AtomicReference<HighAvailabilityGroup> haGroupRef = new AtomicReference<>();
        GenericTestUtils.waitFor(() -> {
            try {
                Optional<HighAvailabilityGroup> haGroup = HighAvailabilityGroup.get(jdbcUrl, clientProperties);
                if (!haGroup.isPresent()) {
                    return false;
                }
                haGroupRef.set(haGroup.get());
                return true;
            } catch (SQLException throwables) {
                return false;
            }
        },1_000,180_000);
        return haGroupRef.get();
    }

    public static List<ExecutorService> getListOfSingleThreadExecutorServices() {
        return ImmutableList.of(Executors.newFixedThreadPool(1), Executors.newFixedThreadPool(1));
    }
}
