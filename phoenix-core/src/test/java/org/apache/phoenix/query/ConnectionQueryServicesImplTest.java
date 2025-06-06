/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.query;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_FAMILY_NAME_BYTES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_SCHEMA_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_FOR_MUTEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.SystemExitRule;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ConnectionQueryServicesImplTest {
    private static final PhoenixIOException PHOENIX_IO_EXCEPTION =
            new PhoenixIOException(new Exception("Test exception"));
    private HTableDescriptor sysMutexTableDescCorrectTTL =
            new HTableDescriptor(TableName.valueOf(SYSTEM_MUTEX_NAME))
                    .addFamily(new HColumnDescriptor(SYSTEM_MUTEX_FAMILY_NAME_BYTES)
                            .setTimeToLive(TTL_FOR_MUTEX));

    @ClassRule
    public static final SystemExitRule SYSTEM_EXIT_RULE = new SystemExitRule();

    @Mock
    private ConnectionQueryServicesImpl mockCqs;

    @Mock
    private HBaseAdmin mockAdmin;

    @Mock
    private ReadOnlyProps readOnlyProps;

    @Mock
    private ClusterConnection mockConn;

    @Mock
    private HTableInterface mockTable;

    @Mock
    private Configuration mockConf;

    public static final HTableDescriptor SYS_TASK_TDB =
        new HTableDescriptor(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME));
    public static final HTableDescriptor SYS_TASK_TDB_SP =
        new HTableDescriptor(TableName.valueOf(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME))
            .setRegionSplitPolicyClassName("abc");

    @Before
    public void setup() throws IOException, NoSuchFieldException,
            IllegalAccessException, SQLException {
        MockitoAnnotations.initMocks(this);
        Field props = ConnectionQueryServicesImpl.class
            .getDeclaredField("props");
        props.setAccessible(true);
        props.set(mockCqs, readOnlyProps);
        props = ConnectionQueryServicesImpl.class.getDeclaredField("connection");
        props.setAccessible(true);
        props.set(mockCqs, mockConn);
        when(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin))
            .thenCallRealMethod();
        when(mockCqs.updateAndConfirmSplitPolicyForTask(SYS_TASK_TDB))
            .thenCallRealMethod();
        when(mockCqs.updateAndConfirmSplitPolicyForTask(SYS_TASK_TDB_SP))
            .thenCallRealMethod();
        when(mockCqs.getSysMutexTable()).thenCallRealMethod();
        when(mockCqs.getTable(Matchers.<byte[]>any())).thenCallRealMethod();
        when(mockCqs.getTableIfExists(Matchers.<byte[]>any())).thenCallRealMethod();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExceptionHandlingOnSystemNamespaceCreation() throws Exception {
        // Invoke the real methods for these two calls
        when(mockCqs.createSchema(any(List.class), anyString())).thenCallRealMethod();
        doCallRealMethod().when(mockCqs).ensureSystemTablesMigratedToSystemNamespace();
        // Do nothing for this method, just check that it was invoked later
        doNothing().when(mockCqs).createSysMutexTableIfNotExists(any(HBaseAdmin.class));

        // Spoof out this call so that ensureSystemTablesUpgrade() will return-fast.
        when(mockCqs.getSystemTableNamesInDefaultNamespace(any(HBaseAdmin.class)))
                .thenReturn(Collections.<TableName> emptyList());

        // Throw a special exception to check on later
        doThrow(PHOENIX_IO_EXCEPTION).when(mockCqs).ensureNamespaceCreated(anyString());

        // Make sure that ensureSystemTablesMigratedToSystemNamespace will try to migrate
        // the system tables.
        Map<String,String> props = new HashMap<>();
        props.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, "true");
        when(mockCqs.getProps()).thenReturn(new ReadOnlyProps(props));
        mockCqs.ensureSystemTablesMigratedToSystemNamespace();

        // Should be called after upgradeSystemTables()
        // Proves that execution proceeded
        verify(mockCqs).getSystemTableNamesInDefaultNamespace(any(HBaseAdmin.class));

        try {
            // Verifies that the exception is propagated back to the caller
            mockCqs.createSchema(Collections.<Mutation> emptyList(), "");
        } catch (PhoenixIOException e) {
            assertEquals(PHOENIX_IO_EXCEPTION, e);
        }
    }

    @Test
    public void testGetNextRegionStartKey() {
        HRegionInfo mockHRegionInfo = org.mockito.Mockito.mock(HRegionInfo.class);
        HRegionLocation mockRegionLocation = org.mockito.Mockito.mock(HRegionLocation.class);
        ConnectionQueryServicesImpl mockCqsi = org.mockito.Mockito.mock(ConnectionQueryServicesImpl.class,
                org.mockito.Mockito.CALLS_REAL_METHODS);
        byte[] corruptedStartAndEndKey = "0x3000".getBytes();
        byte[] corruptedDecreasingKey = "0x2999".getBytes();
        byte[] notCorruptedStartKey = "0x2999".getBytes();
        byte[] notCorruptedEndKey = "0x3000".getBytes();
        byte[] notCorruptedNewKey = "0x3001".getBytes();
        byte[] mockTableName = "dummyTable".getBytes();
        when(mockRegionLocation.getRegionInfo()).thenReturn(mockHRegionInfo);
        when(mockHRegionInfo.getRegionName()).thenReturn(mockTableName);

        // comparing the current regionInfo endKey is equal to the previous endKey
        // [0x3000, Ox3000) vs 0x3000
        GlobalClientMetrics.GLOBAL_HBASE_COUNTER_METADATA_INCONSISTENCY.getMetric().reset();
        when(mockHRegionInfo.getStartKey()).thenReturn(corruptedStartAndEndKey);
        when(mockHRegionInfo.getEndKey()).thenReturn(corruptedStartAndEndKey);
        testGetNextRegionStartKey(mockCqsi, mockRegionLocation, corruptedStartAndEndKey, true);

        // comparing the current regionInfo endKey is less than previous endKey
        // [0x3000,0x2999) vs 0x3000
        GlobalClientMetrics.GLOBAL_HBASE_COUNTER_METADATA_INCONSISTENCY.getMetric().reset();
        when(mockHRegionInfo.getStartKey()).thenReturn(corruptedStartAndEndKey);
        when(mockHRegionInfo.getEndKey()).thenReturn(corruptedDecreasingKey);
        testGetNextRegionStartKey(mockCqsi, mockRegionLocation, corruptedStartAndEndKey, true);

        // comparing the current regionInfo endKey is greater than the previous endKey
        // [0x3000,0x3000) vs 0x3001
        GlobalClientMetrics.GLOBAL_HBASE_COUNTER_METADATA_INCONSISTENCY.getMetric().reset();
        when(mockHRegionInfo.getStartKey()).thenReturn(notCorruptedStartKey);
        when(mockHRegionInfo.getEndKey()).thenReturn(notCorruptedNewKey);
        testGetNextRegionStartKey(mockCqsi, mockRegionLocation, notCorruptedEndKey, false);

        // test EMPTY_START_ROW
        GlobalClientMetrics.GLOBAL_HBASE_COUNTER_METADATA_INCONSISTENCY.getMetric().reset();
        when(mockHRegionInfo.getStartKey()).thenReturn(HConstants.EMPTY_START_ROW);
        when(mockHRegionInfo.getEndKey()).thenReturn(notCorruptedEndKey);
        testGetNextRegionStartKey(mockCqsi, mockRegionLocation, HConstants.EMPTY_START_ROW, false);
        
        //test EMPTY_END_ROW
        GlobalClientMetrics.GLOBAL_HBASE_COUNTER_METADATA_INCONSISTENCY.getMetric().reset();
        when(mockHRegionInfo.getStartKey()).thenReturn(notCorruptedStartKey);
        when(mockHRegionInfo.getEndKey()).thenReturn(HConstants.EMPTY_END_ROW);
        testGetNextRegionStartKey(mockCqsi, mockRegionLocation, notCorruptedStartKey, false);
    }

    private void testGetNextRegionStartKey(ConnectionQueryServicesImpl mockCqsi,
                                           HRegionLocation mockRegionLocation, byte[] key, boolean isCorrupted) {
        try {
            mockCqsi.getNextRegionStartKey(mockRegionLocation, key);
             if (isCorrupted) {
                 fail();
             }
        } catch (IOException e) {
            if (!isCorrupted) {
                fail();
            }
        }

        assertEquals(isCorrupted ? 1 : 0,
                GlobalClientMetrics.GLOBAL_HBASE_COUNTER_METADATA_INCONSISTENCY.getMetric().getValue());
    }
    @Test
    public void testSysMutexCheckReturnsFalseWhenTableAbsent() throws Exception {
        // Override the getTableDescriptor() call to throw instead
        doThrow(new TableNotFoundException())
                .when(mockAdmin)
                .getTableDescriptor(Bytes.toBytes(SYSTEM_MUTEX_NAME));
        doThrow(new TableNotFoundException())
                .when(mockAdmin)
                .getTableDescriptor(TableName.valueOf(SYSTEM_SCHEMA_NAME,
                        SYSTEM_MUTEX_TABLE_NAME));
        assertFalse(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin));
    }

    @Test
    public void testSysMutexCheckModifiesTTLWhenWrong() throws Exception {
        // Set the wrong TTL
        HTableDescriptor sysMutexTableDesc =
                new HTableDescriptor(TableName.valueOf(SYSTEM_MUTEX_NAME))
                        .addFamily(new HColumnDescriptor(SYSTEM_MUTEX_FAMILY_NAME_BYTES)
                                .setTimeToLive(HConstants.FOREVER));
        when(mockAdmin.getTableDescriptor(Bytes.toBytes(SYSTEM_MUTEX_NAME)))
                .thenReturn(sysMutexTableDesc);

        assertTrue(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin));
        verify(mockAdmin, Mockito.times(1)).modifyTable(
                sysMutexTableDescCorrectTTL.getTableName(), sysMutexTableDescCorrectTTL);
    }

    @Test
    public void testSysMutexCheckDoesNotModifyTableDescWhenTTLCorrect() throws Exception {
        when(mockAdmin.getTableDescriptor(Bytes.toBytes(SYSTEM_MUTEX_NAME)))
                .thenReturn(sysMutexTableDescCorrectTTL);

        assertTrue(mockCqs.checkIfSysMutexExistsAndModifyTTLIfRequired(mockAdmin));
        verify(mockAdmin, Mockito.times(0)).modifyTable(
                any(TableName.class), any(HTableDescriptor.class));
    }

    @Test
    public void testSysTaskSplitPolicy() throws Exception {
        assertTrue(mockCqs.updateAndConfirmSplitPolicyForTask(SYS_TASK_TDB));
        assertFalse(mockCqs.updateAndConfirmSplitPolicyForTask(SYS_TASK_TDB));
    }

    @Test
    public void testSysTaskSplitPolicyWithError() {
        try {
            mockCqs.updateAndConfirmSplitPolicyForTask(SYS_TASK_TDB_SP);
            fail("Split policy for SYSTEM.TASK cannot be updated");
        } catch (SQLException e) {
            assertEquals("ERROR 908 (43M19): REGION SPLIT POLICY is incorrect."
                + " Region split policy for table TASK is expected to be "
                + "among: [null, org.apache.phoenix.schema.SystemTaskSplitPolicy]"
                + " , actual split policy: abc tableName=SYSTEM.TASK",
                e.getMessage());
        }
    }

    @Test
    public void testGetSysMutexTableWithName() throws Exception {
        when(mockAdmin.tableExists(any(TableName.class))).thenReturn(true);
        when(mockCqs.getAdmin()).thenReturn(mockAdmin);
        when(mockCqs.getTable(Bytes.toBytes("SYSTEM.MUTEX"))).thenReturn(mockTable);
        assertSame(mockCqs.getSysMutexTable(), mockTable);
        verify(mockAdmin, Mockito.times(1)).tableExists(any(TableName.class));
        verify(mockCqs, Mockito.times(1)).getAdmin();
        verify(mockCqs, Mockito.times(2)).getTable(Matchers.<byte[]>any());
    }

    @Test
    public void testGetSysMutexTableWithNamespace() throws Exception {
        when(mockAdmin.tableExists(any(TableName.class))).thenReturn(false);
        when(mockCqs.getAdmin()).thenReturn(mockAdmin);
        when(mockCqs.getTable(Bytes.toBytes("SYSTEM:MUTEX"))).thenReturn(mockTable);
        assertSame(mockCqs.getSysMutexTable(), mockTable);
        verify(mockAdmin, Mockito.times(1)).tableExists(any(TableName.class));
        verify(mockCqs, Mockito.times(1)).getAdmin();
        verify(mockCqs, Mockito.times(2)).getTable(Matchers.<byte[]>any());
    }
}
