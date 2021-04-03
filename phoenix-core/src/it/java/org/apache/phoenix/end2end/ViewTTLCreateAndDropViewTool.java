package org.apache.phoenix.end2end;

import org.apache.phoenix.thirdparty.com.google.common.base.Throwables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Option;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.Options;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.phoenix.thirdparty.org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.BasicDataWriter;
import org.apache.phoenix.query.PhoenixTestBuilder.DataSupplier;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.schema.NewerTableAlreadyExistsException;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.OtherOptions;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TenantViewIndexOptions;
import static org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TenantViewOptions;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;

public class ViewTTLCreateAndDropViewTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(ViewTTLCreateAndDropViewTool.class);
    public static final int MAX_ROWS = 1000000;

    public enum ObjectType {
        CUSTOM_OBJECT, HBPO, MT_GLOBAL_VIEW
    }

    static List<String> TABLE_ADDT_COLUMNS = Arrays.asList(
            "CREATED_BY",
            "CREATED_DATE",
            "SYSTEM_MODSTAMP"
    );

    static private List<String> TENANT_VIEW_PK_COLS = Lists.newArrayList(
            "ONE_ZID",
            "ONE_VIEW_CREATED_DATE",
            "ONE_VIEW_CREATED_BY"
    );

    static private List<String> TENANT_VIEW_SPECIFIC_COLS = Lists.newArrayList(
            "ONE_ID",
            "ONE_TYPE",
            "ONE_PRIMARY_TITLE",
            "ONE_ORIGINAL_TITLE",
            "ONE_IS_ADULT",
            "ONE_START_YEAR",
            "ONE_END_YEAR",
            "ONE_RUNNING_MINUTES"
    );

    private List<String> TENANT_VIEW_SPECIFIC_COLS_TYPES = Lists.newArrayList(
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "BOOLEAN",
            "UNSIGNED_INT",
            "UNSIGNED_INT",
            "UNSIGNED_LONG"
    );

    static List<String> TENANT_VIEW_COLUMNS = Arrays.asList(
            "ONE_COL_0",
            "ONE_COL_1",
            "ONE_COL_2",
            "ONE_COL_3",
            "ONE_COL_4",
            "ONE_COL_5",
            "ONE_COL_6",
            "ONE_COL_7",
            "ONE_COL_8",
            "ONE_COL_9"
    );


    private static final Option READ_DATA_OPTION = new Option("r", "read", false,
            "If specified, read data");
    private static final Option DROP_VIEWS_OPTION = new Option("d", "drop", false,
            "If specified, drops views and links");
    private static final Option CREATE_VIEWS_OPTION = new Option("c", "create", false,
            "If specified, creates views and links");
    private static final Option RECREATE_VIEWS_OPTION = new Option("re", "recreate", false,
            "If specified, recreates views and links, i.e creates, drops , creates again with new model");
    private static final Option CREATE_MULTI_TENANT_OPTION = new Option("mt", "multi-tenant", false,
            "If specified, recreates views and links for multiple tenants");
    private static final Option CREATE_MULTI_VIEWS_OPTION = new Option("mv", "multi-view", false,
            "If specified, recreates multiple views and links for single");
    private static final Option CONNECT_URL_OPTION = new Option("u", "url", true,
            "If specified, uses the url to connect to phoenix");

    private static final Option OBJECT_TYPE = new Option("t", "type", true,
            "If specified, uses the type to create CUSTOM_OBJECT, HBPO or MT_GLOBAL_VIEW");

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(READ_DATA_OPTION);
        options.addOption(CONNECT_URL_OPTION);
        options.addOption(OBJECT_TYPE);
        options.addOption(DROP_VIEWS_OPTION);
        options.addOption(CREATE_VIEWS_OPTION);
        options.addOption(RECREATE_VIEWS_OPTION);
        options.addOption(CREATE_MULTI_TENANT_OPTION);
        options.addOption(CREATE_MULTI_VIEWS_OPTION);
        return options;
    }

    private boolean createViews = false;
    private boolean recreateViews = false;
    private boolean dropViews = false;
    private boolean readData = false;
    private boolean multipleTenantViews = false;
    private boolean multipleViewsPerTenant = false;
    private String connectUrl = "jdbc:phoenix:localhost";
    private ObjectType objectType = ObjectType.CUSTOM_OBJECT;

    public SchemaBuilder getTableSchemaBuilder(ObjectType type) {

        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setTableProps("COLUMN_ENCODED_BYTES=0, MULTI_TENANT=true, DEFAULT_COLUMN_FAMILY='Z' SPLIT ON ('00D0t000T000001Z01aa',"
                + "'00D0t000T000001Z01bb',"
                + "'00D0t000T000001Z01cc',"
                + "'00D0t000T000001Z01dd',"
                + "'00D0t000T000001Z01ee',"
                + "'00D0t000T000001Z01ff',"
                + "'00D0t000T000001Z01gg',"
                + "'00D0t000T000001Z01hh',"
                + "'00D0t000T000001Z01ii',"
                + "'00D0t000T000001Z01jj')");
        tableOptions.getTableColumns().addAll(TABLE_ADDT_COLUMNS);
        tableOptions.getTableColumnTypes().addAll(Lists.<String>newArrayList(
                "VARCHAR",
                "TIMESTAMP",
                "TIMESTAMP"
        ));

        OtherOptions otherOptions = OtherOptions.withDefaults();
        otherOptions.getTableCFs().addAll(Lists.<String>newArrayList(null, null, null));


        final SchemaBuilder schemaBuilder = new SchemaBuilder(getConnectUrl());
        if (type.equals(ObjectType.HBPO)) {
            SchemaBuilder.GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
            globalViewOptions.setTableProps(String.format("PHOENIX_TTL=%d", 300000));
            //otherOptions.getGlobalViewCFs().addAll(Lists.<String>newArrayList(null, null, null));

            SchemaBuilder.GlobalViewIndexOptions
                    globalViewIndexOptions = SchemaBuilder.GlobalViewIndexOptions.withDefaults();
            schemaBuilder
                    .withTableOptions(tableOptions)
                    .withGlobalViewOptions(globalViewOptions)
                    .withGlobalViewIndexOptions(globalViewIndexOptions)
                    .withOtherOptions(otherOptions);
        } else {
            schemaBuilder
                    .withTableOptions(tableOptions)
                    .withOtherOptions(otherOptions);

        }

        return schemaBuilder;
    }


    public SchemaBuilder getTenantViewBuilder(SchemaBuilder builder, String viewColPrefix) {

        List<String> CUR_TENANT_VIEW_PK_COLS = TENANT_VIEW_PK_COLS;
        List<String> CUR_TENANT_VIEW_COLUMNS = TENANT_VIEW_COLUMNS;
        List<String> CUR_TENANT_VIEW_SPECIFIC_COLS = TENANT_VIEW_SPECIFIC_COLS;


        TenantViewOptions tenantViewOptions = new TenantViewOptions();
        tenantViewOptions.setTableProps("PHOENIX_TTL=120000");
        tenantViewOptions.getTenantViewPKColumns().addAll(CUR_TENANT_VIEW_PK_COLS);
        tenantViewOptions.getTenantViewPKColumnTypes().addAll(
                Lists.<String>newArrayList("VARCHAR(10)", "DATE", "CHAR(15)"));
        //tenantViewOptions.setTenantViewPKColumnSort(Lists.newArrayList("DESC", "DESC", "DESC"));
        tenantViewOptions.getTenantViewColumns().addAll(CUR_TENANT_VIEW_COLUMNS);
        tenantViewOptions.getTenantViewColumns().addAll(CUR_TENANT_VIEW_SPECIFIC_COLS);

        OtherOptions otherOptions = builder.getOtherOptions();
        otherOptions.setTenantViewCFs(new ArrayList<String>());
        for (String col : CUR_TENANT_VIEW_COLUMNS) {
            tenantViewOptions.getTenantViewColumnTypes().add("VARCHAR");
            otherOptions.getTenantViewCFs().add(null);
        }

        for (int i = 0; i < CUR_TENANT_VIEW_SPECIFIC_COLS.size(); i++) {
            tenantViewOptions.getTenantViewColumnTypes().add(TENANT_VIEW_SPECIFIC_COLS_TYPES.get(i));
            otherOptions.getTenantViewCFs().add(null);
        }

        TenantViewIndexOptions tenantViewIndexOptions = TenantViewIndexOptions.withDefaults();
        tenantViewIndexOptions.setTenantViewIndexColumns(Lists.newArrayList(CUR_TENANT_VIEW_COLUMNS.get(9)));
        tenantViewIndexOptions.setTenantViewIncludeColumns(Lists.newArrayList(CUR_TENANT_VIEW_COLUMNS.get(7)));
        tenantViewIndexOptions.setLocal(false);

        builder
                .withTenantViewOptions(tenantViewOptions)
                .withTenantViewIndexOptions(tenantViewIndexOptions)
                .withOtherOptions(otherOptions);


        return builder;
    }

    private String getConnectUrl() {
        return connectUrl;
    }

    /*
    public void testRecreateCustomTenantView() throws Exception {
        SchemaBuilder tableBuilder = getTableSchemaBuilder();
        tableBuilder.build();
        TimeUnit.MILLISECONDS.sleep(1000);


        while (true) {
            SchemaBuilder view1Builder = getTenantViewBuilder(tableBuilder, "");
            TimeUnit.MILLISECONDS.sleep(1000);
            view1Builder.build();

            addMovieTestData(view1Builder, 1);

            TimeUnit.MILLISECONDS.sleep(1000);
            cleanupTenantView(view1Builder);
            SchemaBuilder view2Builder = getTenantViewBuilder(tableBuilder, "");

            TimeUnit.MILLISECONDS.sleep(1000);
            view2Builder.build();
            addMovieTestData(view2Builder, 2);

            TimeUnit.MILLISECONDS.sleep(1000);
            cleanupTenantView(view2Builder);

        }
    }
    */

    public void testCreateCustomTenantView() throws Exception {
        SchemaBuilder tableBuilder = getTableSchemaBuilder(ObjectType.CUSTOM_OBJECT);
        tableBuilder.build();
        SchemaBuilder viewBuilder = getTenantViewBuilder(tableBuilder, "");
        viewBuilder.buildNewView();
        addCustomObjectsTestData(viewBuilder, "");
    }

    public void testCreateHBPOView() throws Exception {
        SchemaBuilder tableBuilder = getTableSchemaBuilder(ObjectType.HBPO);
        SchemaBuilder viewBuilder = getTenantViewBuilder(tableBuilder, "");
        viewBuilder.build();
        addHBPOTestData(viewBuilder, "");
    }

    public void testCreateMultipleViews() throws Exception {
        SchemaBuilder tableBuilder = getTableSchemaBuilder(ObjectType.CUSTOM_OBJECT);
        tableBuilder.build();
        for (int i=0;i<2;i++) {
            SchemaBuilder viewBuilder = getTenantViewBuilder(tableBuilder, "");
            viewBuilder.buildNewView();
            addCustomObjectsTestData(viewBuilder, "");
        }
    }

    public void testCreateMultipleTenants() throws Exception {
        SchemaBuilder tableBuilder = getTableSchemaBuilder(ObjectType.CUSTOM_OBJECT);
        tableBuilder.build();
        for (int i=0;i<2;i++) {
            SchemaBuilder viewBuilder = getTenantViewBuilder(tableBuilder, "");
            viewBuilder.buildWithNewTenant();
            addCustomObjectsTestData(viewBuilder, "");
        }
    }

    public void testDropCustomTenantView() throws Exception {
        SchemaBuilder tableBuilder = getTableSchemaBuilder(ObjectType.CUSTOM_OBJECT);
        tableBuilder.build();
        //cleanupTenantView(builder);
        //cleanupTables(builder);
    }


    public void testReadData() throws Exception {
        SchemaBuilder tableBuilder = getTableSchemaBuilder(ObjectType.CUSTOM_OBJECT);
        tableBuilder.build();
        SchemaBuilder viewBuilder = getTenantViewBuilder(tableBuilder, "");
        viewBuilder.buildNewView();

        String
                tenantConnectUrl = getConnectUrl() + ';' + TENANT_ID_ATTRIB + '=' +  tableBuilder.getDataOptions().getTenantId();

        try (Connection connection = DriverManager.getConnection(tenantConnectUrl)) {
            //String sql = "SELECT ONE_COL_1 from TEST_ENTITY.Z01 WHERE (ONE_COL_1 like 'a%') AND (((now() - phoenix_row_timestamp()) * 86400 * 1000) < 3000000000000)";
            //String sql = "SELECT ONE_ZID, DUMMY from TEST_ENTITY.Z01(DUMMY DATE) WHERE (ONE_COL_2 like 'b%') AND (((NOW() - DUMMY) * 86400 * 1000) < 30000)";
            //String sql = "SELECT ONE_ZID, DUMMY from TEST_ENTITY.Z01(DUMMY DATE) WHERE (ONE_COL_2 like 'b%') AND (ONE_COL_1 like 'a%') ";
            //String sql = "SELECT ONE_ZID from TEST_ENTITY.Z01 WHERE (SQRT(ONE_COL_1) = 2) AND (ONE_COL_2 like 'b%')";
            //String sql = "SELECT SUBSTR(ONE_COL_2, 2), ROW_TIMESTAMP_STRING_3() from TEST_ENTITY.Z01 WHERE (ONE_COL_1 like '%')";
            //String sql = "select COL2, COL4, phoenix_row_timestamp() from TEST_ENTITY.Z01";
            String sql = "select COL2, COL4 from TEST_ENTITY.Z01";
            java.sql.Statement stmt = connection.createStatement();
            stmt.execute(sql);
            ResultSet rs = stmt.getResultSet();
            while (rs.next()) {
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
                //System.out.println(rs.getString(3));
            }
        }
    }


    private void cleanupTenantView(SchemaBuilder builder) throws Exception {
       String
               tenantConnectUrl = getConnectUrl() + ';' + TENANT_ID_ATTRIB + '=' +  builder.getDataOptions().getTenantId();
       try (Connection tenantConn = DriverManager.getConnection(tenantConnectUrl)) {

            DatabaseMetaData dbmd = tenantConn.getMetaData();
            // Drop VIEWs first, as we don't allow a TABLE with views to be dropped
            // Tables are sorted by TENANT_ID
            try (ResultSet rs = dbmd.getTables(null, null, null, new String[] { PTableType.VIEW.toString()})) {
                cleanupEntities(tenantConn, rs);
            }
        }
    }

    private void cleanupTables(SchemaBuilder builder) throws Exception {

        try (Connection globalConnection = DriverManager.getConnection(getConnectUrl())) {

            DatabaseMetaData dbmd = globalConnection.getMetaData();
            // Drop VIEWs first, as we don't allow a TABLE with views to be dropped
            // Tables are sorted by TENANT_ID
            try (ResultSet rs = dbmd.getTables(null, null, null, new String[] { PTableType.TABLE.toString()})) {
                cleanupEntities(globalConnection, rs);
            }
        }
    }

    public void cleanupEntities(Connection connection, ResultSet rs) throws Exception {
        connection.setAutoCommit(true);
        while (rs.next()) {
            String schemaName = rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM);
            if (schemaName.equals(PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME)) {
                continue;
            }

            String fullTableName = SchemaUtil.getEscapedTableName(
                    rs.getString(PhoenixDatabaseMetaData.TABLE_SCHEM),
                    rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            String deleteSQL = "DELETE FROM " + fullTableName + "";
            String
                    ddl = "DROP " + rs.getString(PhoenixDatabaseMetaData.TABLE_TYPE) + " " + fullTableName + "";
            String tenantId = rs.getString(1);
            try {
                connection.createStatement().execute(deleteSQL);
                connection.createStatement().executeUpdate(ddl);
            } catch (NewerTableAlreadyExistsException ex) {
                LOG.info("Newer table " + fullTableName + " or its delete marker exists. Ignore current deletion");
            } catch (TableNotFoundException ex) {
                LOG.info("Table " + fullTableName + " is already deleted.");
            }
        }

    }


    public void addCustomObjectsTestData(SchemaBuilder builder, String viewColPrefix) throws Exception {

        List<String> CUR_ADDT_COLUMNS = TABLE_ADDT_COLUMNS;
        List<String> CUR_TENANT_VIEW_PK_COLS = TENANT_VIEW_PK_COLS;
        List<String> CUR_TENANT_VIEW_SPECIFIC_COLS = TENANT_VIEW_SPECIFIC_COLS;
        List<String> CUR_TENANT_VIEW_COLUMNS = TENANT_VIEW_COLUMNS;


        TitleBasicsParser parser = new TitleBasicsParser();
        final List<TitleBasics> titles = parser.parseFile("/Users/jisaac/workspace/checkins/view-ttl/phoenix/phoenix-core/src/test/resources/test_data.tsv");
        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {
//            Timestamp now = new Timestamp(1538074748814L) ;//new Timestamp(System.currentTimeMillis());
//            String createdBy = String.format("xxxxx");
            String[] splits = new String[] {"aa","bb","cc","dd","ee","ff","gg","hh","ii","jj"};

            @Override
            public List<Object> getValues(int rowIndex) {
                int split_index = rowIndex%10;
                String splitPrefix = splits[split_index];
                Random rnd = new Random();
                String id = String.format("00A0y000%07d", rowIndex);
                String col1 = String.format("a%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col2 = String.format("b%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col3 = String.format("c%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col4 = String.format("d%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col5 = String.format("e%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col6 = String.format("f%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col7 = String.format("g%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format("h%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format("i%07d", rowIndex + rnd.nextInt(MAX_ROWS));

                String createdBy = String.format("i%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                Timestamp now = new Timestamp(System.currentTimeMillis());

                TitleBasics title = titles.get(rowIndex % titles.size());
                return Lists.newArrayList(new Object[] {
                        createdBy,                              // CREATED_BY
                        now,                                    // CREATED_DATE
                        now,                                    // SYSTEM_MODSTAMP
                        splitPrefix + "_" + title.getTitleId().substring(2), // ONE_ZID
                        now,                                    // ONE_VIEW_CREATED_DATE
                        createdBy,                              // ONE_VIEW_CREATED_BY
                        id + "_" + title.getTitleId(),          // ONE_ID
                        title.getTitleType(),                   // ONE_TYPE
                        title.getPrimaryTitle(),                // ONE_PRIMARY_TITLE
                        title.getOriginalTitle(),               // ONE_ORIGINAL_TITLE
                        title.isAdult(),                        // ONE_IS_ADULT
                        title.getStartYear(),                   // ONE_START_YEAR
                        title.getEndYear(),                     // ONE_END_YEAR
                        title.getRuntimeMinutes(),              // ONE_RUNNING_MINUTES
                        id,                                     // ONE_COL_0
                        col1,                                   // ONE_COL_1
                        col2,                                   // ONE_COL_2
                        col3,                                   // ONE_COL_3
                        col4,                                   // ONE_COL_4
                        col5,                                   // ONE_COL_5
                        col6,                                   // ONE_COL_6
                        col7,                                   // ONE_COL_7
                        col8,                                   // ONE_COL_8
                        col9                                    // ONE_COL_9
                });
            }
        };



        BasicDataWriter dataWriter = new BasicDataWriter();
        try (Connection connection = DriverManager
                .getConnection(builder.getUrl() + ';' + TENANT_ID_ATTRIB + '=' +  builder.getDataOptions().getTenantId())) {
            connection.setAutoCommit(true);
            dataWriter.setConnection(connection);
            dataWriter.setDataSupplier(dataSupplier);
            dataWriter.getUpsertColumns().addAll(CUR_ADDT_COLUMNS);
            dataWriter.getUpsertColumns().addAll(CUR_TENANT_VIEW_PK_COLS);
            dataWriter.getUpsertColumns().addAll(CUR_TENANT_VIEW_SPECIFIC_COLS);
            dataWriter.getUpsertColumns().addAll(CUR_TENANT_VIEW_COLUMNS.subList(0, 10));


            dataWriter.setTargetEntity(builder.getEntityTenantViewName());
            for (int i=0;i<1;i++) {
                for (int j=0;j<2;j++) {
                    dataWriter.upsertRow(j);
                    //TimeUnit.SECONDS.sleep(1);
                }
            }
        }
    }

    public void addHBPOTestData(SchemaBuilder builder, String viewColPrefix) throws Exception {

        List<String> CUR_TABLE_COLUMNS = PhoenixTestBuilder.DDLDefaults.TABLE_COLUMNS;
        List<String> CUR_ADDT_COLUMNS = TABLE_ADDT_COLUMNS;
        List<String> CUR_GLOBAL_VIEW_PK_COLS = PhoenixTestBuilder.DDLDefaults.GLOBAL_VIEW_PK_COLUMNS;
        List<String> CUR_GLOBAL_VIEW_COLS = PhoenixTestBuilder.DDLDefaults.GLOBAL_VIEW_COLUMNS;
        List<String> CUR_TENANT_VIEW_PK_COLS = TENANT_VIEW_PK_COLS;
        List<String> CUR_TENANT_VIEW_SPECIFIC_COLS = TENANT_VIEW_SPECIFIC_COLS;
        List<String> CUR_TENANT_VIEW_COLUMNS = TENANT_VIEW_COLUMNS;


        TitleBasicsParser parser = new TitleBasicsParser();
        final List<TitleBasics> titles = parser.parseFile("/Users/jisaac/workspace/checkins/view-ttl/phoenix/phoenix-core/src/test/resources/test_data.tsv");
        // Define the test data.
        DataSupplier dataSupplier = new DataSupplier() {
            //            Timestamp now = new Timestamp(1538074748814L) ;//new Timestamp(System.currentTimeMillis());
            //            String createdBy = String.format("xxxxx");
            String[] splits = new String[] {"aa","bb","cc","dd","ee","ff","gg","hh","ii","jj"};

            @Override
            public List<Object> getValues(int rowIndex) {
                int split_index = rowIndex%10;
                String splitPrefix = splits[split_index];
                Random rnd = new Random();
                String id = String.format("00A0y000%07d", rowIndex);
                String col1 = String.format("a%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col2 = String.format("b%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col3 = String.format("c%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col4 = String.format("d%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col5 = String.format("e%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col6 = String.format("f%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col7 = String.format("g%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col8 = String.format("h%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                String col9 = String.format("i%07d", rowIndex + rnd.nextInt(MAX_ROWS));

                String createdBy = String.format("i%07d", rowIndex + rnd.nextInt(MAX_ROWS));
                Timestamp now = new Timestamp(System.currentTimeMillis());

                TitleBasics title = titles.get(rowIndex % titles.size());
                return Lists.newArrayList(new Object[] {
                        createdBy,                              // CREATED_BY
                        now,                                    // CREATED_DATE
                        now,                                    // SYSTEM_MODSTAMP
                        col1,                                   // COL_1
                        col2,                                   // COL_2
                        col3,                                   // COL_3
                        id,                                     // ID
                        col4,                                   // COL_4
                        col5,                                   // COL_5
                        col6,                                   // COL_6
                        splitPrefix + "_" + title.getTitleId().substring(2), // ONE_ZID
                        now,                                    // ONE_VIEW_CREATED_DATE
                        createdBy,                              // ONE_VIEW_CREATED_BY
                        id + "_" + title.getTitleId(),          // ONE_ID
                        title.getTitleType(),                   // ONE_TYPE
                        title.getPrimaryTitle(),                // ONE_PRIMARY_TITLE
                        title.getOriginalTitle(),               // ONE_ORIGINAL_TITLE
                        title.isAdult(),                        // ONE_IS_ADULT
                        title.getStartYear(),                   // ONE_START_YEAR
                        title.getEndYear(),                     // ONE_END_YEAR
                        title.getRuntimeMinutes(),              // ONE_RUNNING_MINUTES
                        id,                                     // ONE_COL_0
                        col1,                                   // ONE_COL_1
                        col2,                                   // ONE_COL_2
                        col3,                                   // ONE_COL_3
                        col4,                                   // ONE_COL_4
                        col5,                                   // ONE_COL_5
                        col6,                                   // ONE_COL_6
                        col7,                                   // ONE_COL_7
                        col8,                                   // ONE_COL_8
                        col9                                    // ONE_COL_9
                });
            }
        };



        BasicDataWriter dataWriter = new BasicDataWriter();
        try (Connection connection = DriverManager
                .getConnection(builder.getUrl() + ';' + TENANT_ID_ATTRIB + '=' +  builder.getDataOptions().getTenantId())) {
            connection.setAutoCommit(true);
            dataWriter.setConnection(connection);
            dataWriter.setDataSupplier(dataSupplier);
            dataWriter.getUpsertColumns().addAll(CUR_ADDT_COLUMNS);
            dataWriter.getUpsertColumns().addAll(CUR_TABLE_COLUMNS);
            dataWriter.getUpsertColumns().addAll(CUR_GLOBAL_VIEW_PK_COLS);
            dataWriter.getUpsertColumns().addAll(CUR_GLOBAL_VIEW_COLS);
            dataWriter.getUpsertColumns().addAll(CUR_TENANT_VIEW_PK_COLS);
            dataWriter.getUpsertColumns().addAll(CUR_TENANT_VIEW_SPECIFIC_COLS);
            dataWriter.getUpsertColumns().addAll(CUR_TENANT_VIEW_COLUMNS.subList(0, 10));


            dataWriter.setTargetEntity(builder.getEntityTenantViewName());
            for (int i=0;i<1;i++) {
                for (int j=0;j<2;j++) {
                    dataWriter.upsertRow(j);
                    //TimeUnit.SECONDS.sleep(1);
                }
            }
        }
    }

    private void parseOptions(String[] args) throws Exception {

        final Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        try {
            CommandLine cmdLine = parser.parse(options, args);
            /*
            if (!cmdLine.hasOption(CREATE_VIEWS_OPTION.getOpt()) && !cmdLine.hasOption(DROP_VIEWS_OPTION.getOpt())) {
                throw new IllegalStateException("Specify either " + CREATE_VIEWS_OPTION.getOpt() + " or "
                        + DROP_VIEWS_OPTION.getOpt());
            }
            */

            if (cmdLine.hasOption(CONNECT_URL_OPTION.getOpt())) {
                connectUrl = cmdLine.getOptionValue(CONNECT_URL_OPTION.getOpt());
            }

            if (cmdLine.hasOption(OBJECT_TYPE.getOpt())) {
                String objectTypeStr = cmdLine.getOptionValue(OBJECT_TYPE.getOpt());
                objectType = ObjectType.valueOf(objectTypeStr);
            }

            if (cmdLine.hasOption(READ_DATA_OPTION.getOpt())) {
                readData = true;
            }
            if (cmdLine.hasOption(DROP_VIEWS_OPTION.getOpt())) {
                dropViews = true;
            }
            if (cmdLine.hasOption(READ_DATA_OPTION.getOpt())) {
                readData = true;
            }
            else if (cmdLine.hasOption(CREATE_VIEWS_OPTION.getOpt())) {
                createViews = true;
            }
            else if (cmdLine.hasOption(RECREATE_VIEWS_OPTION.getOpt())) {
                recreateViews = true;
            }
            else if (cmdLine.hasOption(CREATE_MULTI_TENANT_OPTION.getOpt())) {
                multipleTenantViews = true;
            }
            else if (cmdLine.hasOption(CREATE_MULTI_VIEWS_OPTION.getOpt())) {
                multipleViewsPerTenant = true;
            }

        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }


    }

    private void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);
    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }


    @Override
    public int run(String[] args) throws Exception {
        try {
            parseOptions(args);

            if (dropViews) {
                testDropCustomTenantView();
            }
            else if (readData) {
                testReadData();
            }
            else if (createViews) {
                switch (objectType) {
                case HBPO:
                    testCreateHBPOView();
                    break;
                case CUSTOM_OBJECT:
                    testCreateCustomTenantView();
                    break;
                case MT_GLOBAL_VIEW:
                default:

                }
            }
            else if (recreateViews) {
                //testRecreateCustomTenantView();
            }
            else if (multipleViewsPerTenant) {
                testCreateMultipleViews();
            }
            else if (multipleTenantViews) {
                testCreateMultipleTenants();
            }


        } catch (Exception e) {
            LOG.error(Throwables.getStackTraceAsString(e));

        } finally {
        }

        return 0;
    }


    /**
     *
     * Parses a tab delimited file containing movie info (title.basics).
     *
     */
    private static class TitleBasicsParser {

        public static void main(String[] args) throws Exception {
            TitleBasicsParser
                    parser = new TitleBasicsParser();
            parser.parseFile("/Users/jisaac/workspace/k8s/git/soma/bigdata/phoenix-tests/src/main/resources/test_data.tsv");
        }


        public List<TitleBasics> parseFile(String documentFilename) throws Exception {

            List<String> lines = null;
            List<TitleBasics> movieList = new ArrayList<TitleBasics>();

            try {

                Path path = Paths.get(documentFilename);
                AtomicInteger count = new AtomicInteger();
                //lines = Files.lines(path);
                String[] genres = new String[] {};
                for (String l : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                    String[] titleStrParts = l.split("\t");
                    if (titleStrParts.length < 9) {
                        throw new IllegalArgumentException("Movie details not formatted correctly");
                    }
                    int year = 1970;
                    try {

                        short startYear = titleStrParts[5].equals("\\N") ? 0 : Short.valueOf(titleStrParts[5]);
                        short endYear = titleStrParts[6].equals("\\N") ? 0 : Short.valueOf(titleStrParts[6]);
                        long runtimeMinutes = titleStrParts[7].equals("\\N") ? 0 : Integer.valueOf(titleStrParts[7]);
                        TitleBasics newTitle = new TitleBasics(
                                titleStrParts[0],
                                titleStrParts[1],
                                titleStrParts[2],
                                titleStrParts[3],
                                false,
                                startYear, endYear, runtimeMinutes, genres
                        );
                        //					if (movieList.size() == 5) {
                        //						return;
                        //					}
                        count.incrementAndGet();
                        movieList.add(newTitle);
                    }
                    catch(Exception e) {
                        // ignoring ill formatted entries for now.
                        e.printStackTrace();
                    }


                }
                System.out.println("done:" + count.get() + ":" + movieList.size());

                return movieList;
            } finally {
            }

        }

    }


    private static class TitleBasics implements Comparable<TitleBasics> {

        private final String titleId;
        private final String titleType;
        private final String primaryTitle;
        private final String originalTitle;
        private final boolean isAdult;
        private final short startYear;
        private final short endYear;
        private final long runtimeMinutes;
        private final String[] genres;


        public TitleBasics(String titleId, String titleType, String primaryTitle, String originalTitle, boolean isAdult,
                short startYear, short endYear, long runtimeMinutes, String[] genres) {
            super();
            this.titleId = titleId;
            this.titleType = titleType;
            this.primaryTitle = primaryTitle;
            this.originalTitle = originalTitle;
            this.isAdult = isAdult;
            this.startYear = startYear;
            this.endYear = endYear;
            this.runtimeMinutes = runtimeMinutes;
            this.genres = genres;
        }


        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((titleId == null) ? 0 : titleId.hashCode());
            return result;
        }


        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TitleBasics other = (TitleBasics) obj;
            if (titleId == null) {
                if (other.titleId != null)
                    return false;
            } else if (!titleId.equals(other.titleId))
                return false;
            return true;
        }

        public String getTitleId() {
            return titleId;
        }
        public String getTitleType() {
            return titleType;
        }
        public String getPrimaryTitle() {
            return primaryTitle;
        }
        public String getOriginalTitle() {
            return originalTitle;
        }
        public boolean isAdult() {
            return isAdult;
        }
        public short getStartYear() {
            return startYear;
        }
        public short getEndYear() {
            return endYear;
        }
        public long getRuntimeMinutes() {
            return runtimeMinutes;
        }
        public String[] getGenres() {
            return genres;
        }


        public int compareTo(TitleBasics o) {
            if (this == o)
                return 0;
            if (o == null)
                throw new NullPointerException();
            return this.titleId.compareTo(o.titleId);
        }

    }

    public static void main(final String[] args) {
        try {
            LOG.info("Starting Phoenix Canary Test tool...");
            ToolRunner.run(new ViewTTLCreateAndDropViewTool(), args);
        } catch (Exception e) {
            LOG.error("Error in running Phoenix Canary Test tool. " + e);
        }
        LOG.info("Exiting Phoenix Canary Test tool...");
    }

}
