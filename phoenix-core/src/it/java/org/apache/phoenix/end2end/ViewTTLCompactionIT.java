package org.apache.phoenix.end2end;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.protobuf.RpcCallback;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.coprocessor.generated.CompactionProtos;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

public class ViewTTLCompactionIT extends BaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ViewTTLCompactionIT.class);

    String tenantId = "00D0t001T000001";
    String multiTenantTableName = "TEST_ENTITY.BASE_HBPO_WITH_TTL";
    String multiTenantGlobalView = "TEST_ENTITY.GLOBAL_V000001_WITH_TTL";
    String multiTenantViewName = "TEST_ENTITY.ECZ";

    /*
    @Test public void testViewCompaction() throws Exception {
        String tenantId = generateUniqueName();
        Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        String schema = generateUniqueName();

        //String multiTenantTableName = schema + "." + generateUniqueName();
        String globalViewName = schema + "." + generateUniqueName();

        String view1 = generateUniqueName();
        String view2 = generateUniqueName();
        String customObjectView1 = schema + "." + view1;
        String customObjectView2 = schema + "." + view2;

        String tenantView = generateUniqueName();
        String tenantViewOnGlobalView = schema + "." + tenantView;
        String tenantViewIndex = tenantView + "_INDEX";
        String tenantIndex = schema + "." + tenantViewIndex;


        String
                multiTenantTableDDL =
                "CREATE TABLE IF NOT EXISTS " + multiTenantTableName
                        + "(TENANT_ID CHAR(15) NOT NULL, KP CHAR(3) NOT NULL, ID VARCHAR, NUM BIGINT "
                        + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, KP)) MULTI_TENANT=true";

        String
                globalViewDDL =
                "CREATE VIEW IF NOT EXISTS " + globalViewName + "(G1 BIGINT, G2 BIGINT) "
                        + "AS SELECT * FROM " + multiTenantTableName + " WHERE KP = '001' PHOENIX_TTL=30";

        String viewDDL = "CREATE VIEW IF NOT EXISTS %s (V1 BIGINT, V2 BIGINT) AS SELECT * FROM %s PHOENIX_TTL=30";
        String
                viewIndexDDL =
                "CREATE INDEX IF NOT EXISTS " + tenantViewIndex + " ON " + tenantViewOnGlobalView
                        + "(NUM DESC) INCLUDE (ID)";

        String
                customObjectViewDDL =
                "CREATE VIEW IF NOT EXISTS %s (V1 BIGINT, V2 BIGINT) " + "AS SELECT * FROM "
                        + multiTenantTableName + " WHERE KP = '%s' PHOENIX_TTL=30";

        String selectFromViewSQL = "SELECT * FROM %s";

        List<String> dmls = Arrays.asList(new String[] {
                String.format(viewDDL, tenantViewOnGlobalView, globalViewName),
                String.format(customObjectViewDDL, customObjectView1, view1),
                String.format(customObjectViewDDL, customObjectView2, view2),
                viewIndexDDL
        });

        // Create the various tables and views
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Base table.
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(multiTenantTableDDL);
            }
            // Global view.
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(globalViewDDL);
            }
            // Tenant views and indexes.
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
                for (String dml : dmls) {
                    try (Statement stmt = tenantConn.createStatement()) {
                        stmt.execute(dml);
                    }
                }
            }
        }


        Scan clientScan = getScan(multiTenantViewName);
        ClientProtos.Scan srcProtoScan = ProtobufUtil.toScan(clientScan);
        byte[] srcBytes = srcProtoScan.toByteArray();
        ClientProtos.Scan destProtoScan = ClientProtos.Scan.parseFrom(srcBytes);
        Scan serverScan = ProtobufUtil.toScan(destProtoScan);

        // Compact the various tables and views
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Table parentTable = conn.unwrap(PhoenixConnection.class).getQueryServices().
                    getTable(Bytes.toBytes(multiTenantTableName));
            //compactView(parentTable, multiTenantViewName, srcProtoScan);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

    }

     */



    void compactView(Table parentTable, final String multiTenantViewName, final ClientProtos.Scan srcProtoScan) throws Throwable {

        final Map<byte[], CompactionProtos.PhoenixTTLExpiredCompactionResponse>
                results =
                parentTable.coprocessorService(CompactionProtos.CompactionService.class, null, null,
                        new Batch.Call<CompactionProtos.CompactionService, CompactionProtos.PhoenixTTLExpiredCompactionResponse>() {
                            @Override
                            public CompactionProtos.PhoenixTTLExpiredCompactionResponse call(
                                    CompactionProtos.CompactionService instance)
                                    throws IOException {
                                ServerRpcController controller = new ServerRpcController();
                                BlockingRpcCallback<CompactionProtos.PhoenixTTLExpiredCompactionResponse>
                                        rpcCallback =
                                        new BlockingRpcCallback<CompactionProtos.PhoenixTTLExpiredCompactionResponse>();
                                CompactionProtos.PhoenixTTLExpiredCompactionRequest.Builder
                                        builder =
                                        CompactionProtos.PhoenixTTLExpiredCompactionRequest
                                                .newBuilder();
                                builder.setPhoenixTTL(180000);
                                builder.setSerializedScanFilter(srcProtoScan.toByteString());
                                instance.compactPhoenixTTLExpiredRows(controller, builder.build(), (RpcCallback<CompactionProtos.PhoenixTTLExpiredCompactionResponse>) rpcCallback);
                                if (controller.getFailedOn() != null) {
                                    throw controller.getFailedOn();
                                }
                                return rpcCallback.get();
                            }
                        });
        if (results.isEmpty()) {
            throw new IOException("Didn't get expected result size");
        }
        CompactionProtos.PhoenixTTLExpiredCompactionResponse
                tmpResponse =
                results.values().iterator().next();
        return;
    }

    @Test public void testCompaction() throws Exception {
        boolean global = false;
        Scan clientScan = getScan(multiTenantViewName, !global);
        //Scan clientScan = getScan(multiTenantGlobalView, global);
        ClientProtos.Scan srcProtoScan = ProtobufUtil.toScan(clientScan);
        byte[] srcBytes = srcProtoScan.toByteArray();
        ClientProtos.Scan destProtoScan = ClientProtos.Scan.parseFrom(srcBytes);
        Scan serverScan = ProtobufUtil.toScan(destProtoScan);

        // Compact the various tables and views
        String connectUrl = global ? "jdbc:phoenix:localhost;" :
                "jdbc:phoenix:localhost;" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;

        try (Connection conn = DriverManager.getConnection(connectUrl)) {
            Table parentTable = conn.unwrap(PhoenixConnection.class).getQueryServices().
                    getTable(Bytes.toBytes(multiTenantTableName));
            compactView(parentTable, multiTenantViewName, srcProtoScan);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    @Test public void testScans() throws Exception {

        boolean global = true;
        Scan scan = getScan(multiTenantGlobalView, global);
        //Scan scan = getScan(multiTenantViewName, !global);
        ClientProtos.Scan srcProtoScan = ProtobufUtil.toScan(scan);
        byte[] srcBytes = srcProtoScan.toByteArray();
        ClientProtos.Scan destProtoScan = ClientProtos.Scan.parseFrom(srcBytes);
        Scan serverScan = ProtobufUtil.toScan(destProtoScan);

        LOG.info(String.format("1.startRow : %s", Bytes.toStringBinary(scan.getStartRow())));
        LOG.info(String.format("1.stopRow : %s", Bytes.toString(scan.getStopRow())));
        LOG.info(String.format("1.filter : %s", scan.getFilter() != null ? scan.getFilter().toString() : "NO_FILTER"));

        LOG.info(String.format("2.startRow : %s", Bytes.toStringBinary(serverScan.getStartRow())));
        LOG.info(String.format("2.stopRow : %s", Bytes.toString(serverScan.getStopRow())));
        LOG.info(String.format("2.filter : %s", serverScan.getFilter() != null ? serverScan.getFilter().toString() : "NO_FILTER"));

    }

    private Scan getScan(String viewName, boolean global)
            throws SQLException {

        Properties props = new Properties();
        props.setProperty("phoenix.ttl.client_side.masking.enabled", "false");
        String connectUrl = global ? "jdbc:phoenix:localhost;" :
                 "jdbc:phoenix:localhost;" + PhoenixRuntime.TENANT_ID_ATTRIB + '=' + tenantId;

        try (Connection deleteConnection = DriverManager.getConnection(connectUrl, props);
                final Statement statement = deleteConnection.createStatement()) {
            deleteConnection.setAutoCommit(true);

            final String deleteIfExpiredStatement = String.format("select * from  %s", viewName);

            /*
            final String
                    deleteIfExpiredStatement =
                    String.format("select * from %s where %s", viewName, String.format(
                            "((TO_NUMBER(CURRENT_TIME()) - TO_NUMBER(phoenix_row_timestamp())) > %d)",
                            600000));


             */


            Preconditions.checkNotNull(deleteIfExpiredStatement);

            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(deleteIfExpiredStatement);
            final Scan scan = queryPlan.getContext().getScan();

            PTable
                    table =
                    PhoenixRuntime.getTable(deleteConnection,
                            tenantId, viewName);

            byte[] emptyColumnFamilyName = SchemaUtil.getEmptyColumnFamily(table);
            byte[]
                    emptyColumnName =
                    table.getEncodingScheme()
                            == PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS ?
                            QueryConstants.EMPTY_COLUMN_BYTES :
                            table.getEncodingScheme()
                                    .encode(QueryConstants.ENCODED_EMPTY_COLUMN_NAME);

            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_FAMILY_NAME,
                    emptyColumnFamilyName);
            scan.setAttribute(BaseScannerRegionObserver.EMPTY_COLUMN_QUALIFIER_NAME,
                    emptyColumnName);
            scan.setAttribute(BaseScannerRegionObserver.DELETE_PHOENIX_TTL_EXPIRED,
                    PDataType.TRUE_BYTES);
            scan.setAttribute(BaseScannerRegionObserver.MASK_PHOENIX_TTL_EXPIRED,
                    PDataType.FALSE_BYTES);
            scan.setAttribute(BaseScannerRegionObserver.PHOENIX_TTL,
                    Bytes.toBytes(Long.valueOf(table.getPhoenixTTL())));
            scan.setAttribute("__TenantId", Bytes.toBytes(tenantId));
            scan.setAttribute("__ViewName", Bytes.toBytes(viewName));

            return scan;
//            PhoenixResultSet
//                    rs =
//                    pstmt.newResultSet(queryPlan.iterator(), queryPlan.getProjector(),
//                            queryPlan.getContext());
        }
    }

}
