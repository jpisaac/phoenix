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
package org.apache.phoenix.hbase.index.write;

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.phoenix.hbase.index.StubAbortable;
import org.apache.phoenix.hbase.index.TableName;
import org.apache.phoenix.hbase.index.covered.IndexMetaData;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;

public class TestParalleIndexWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestParalleIndexWriter.class);
  @Rule
  public TableName test = new TableName();
  private final byte[] row = Bytes.toBytes("row");

  @Test
  public void testCorrectlyCleansUpResources() throws Exception{
    ExecutorService exec = Executors.newFixedThreadPool(1);
    RegionCoprocessorEnvironment e =Mockito.mock(RegionCoprocessorEnvironment.class);
    Configuration conf =new Configuration();
    Mockito.when(e.getConfiguration()).thenReturn(conf);
    Mockito.when(e.getSharedData()).thenReturn(new ConcurrentHashMap<String,Object>());
    FakeTableFactory factory = new FakeTableFactory(
        Collections.<ImmutableBytesPtr, HTableInterface> emptyMap());
    TrackingParallelWriterIndexCommitter writer = new TrackingParallelWriterIndexCommitter(VersionInfo.getVersion());
    Abortable mockAbort = Mockito.mock(Abortable.class);
    Stoppable mockStop = Mockito.mock(Stoppable.class);
    // create a simple writer
    writer.setup(factory, exec, mockAbort, mockStop,e);
    // stop the writer
    writer.stop(this.test.getTableNameString() + " finished");
    assertTrue("Factory didn't get shutdown after writer#stop!", factory.shutdown);
    assertTrue("ExectorService isn't terminated after writer#stop!", exec.isShutdown());
    Mockito.verifyZeroInteractions(mockAbort, mockStop);
  }

  @SuppressWarnings({ "unchecked", "deprecation" })
  @Test
  public void testSynchronouslyCompletesAllWrites() throws Exception {
    LOGGER.info("Starting " + test.getTableNameString());
    LOGGER.info("Current thread is interrupted: " + Thread.interrupted());
    Abortable abort = new StubAbortable();
    Stoppable stop = Mockito.mock(Stoppable.class);
    ExecutorService exec = Executors.newFixedThreadPool(1);
    Map<ImmutableBytesPtr, HTableInterface> tables =
        new LinkedHashMap<ImmutableBytesPtr, HTableInterface>();
    FakeTableFactory factory = new FakeTableFactory(tables);
    RegionCoprocessorEnvironment e =Mockito.mock(RegionCoprocessorEnvironment.class);
    Configuration conf =new Configuration();
    Mockito.when(e.getConfiguration()).thenReturn(conf);
    Mockito.when(e.getSharedData()).thenReturn(new ConcurrentHashMap<String,Object>());
    Region mockRegion = Mockito.mock(Region.class);
    Mockito.when(e.getRegion()).thenReturn(mockRegion);
    HTableDescriptor mockTableDesc = Mockito.mock(HTableDescriptor.class);
    Mockito.when(mockRegion.getTableDesc()).thenReturn(mockTableDesc);
    ImmutableBytesPtr tableName = new ImmutableBytesPtr(this.test.getTableName());
    Put m = new Put(row);
    m.addColumn(Bytes.toBytes("family"), Bytes.toBytes("qual"), null);
    Multimap<HTableInterfaceReference, Mutation> indexUpdates =
        ArrayListMultimap.<HTableInterfaceReference, Mutation> create();
    indexUpdates.put(new HTableInterfaceReference(tableName), m);

    HTableInterface table = Mockito.mock(HTableInterface.class);
    final boolean[] completed = new boolean[] { false };
    Mockito.when(table.batch(Mockito.anyList())).thenAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        // just keep track that it was called
        completed[0] = true;
        return null;
      }
    });
    Mockito.when(table.getTableName()).thenReturn(test.getTableName());
    // add the table to the set of tables, so its returned to the writer
    tables.put(tableName, table);

    // setup the writer and failure policy
    TrackingParallelWriterIndexCommitter writer = new TrackingParallelWriterIndexCommitter(VersionInfo.getVersion());
    writer.setup(factory, exec, abort, stop, e);
    writer.write(indexUpdates, true, ScanUtil.UNKNOWN_CLIENT_VERSION);
    assertTrue("Writer returned before the table batch completed! Likely a race condition tripped",
      completed[0]);
    writer.stop(this.test.getTableNameString() + " finished");
    assertTrue("Factory didn't get shutdown after writer#stop!", factory.shutdown);
    assertTrue("ExectorService isn't terminated after writer#stop!", exec.isShutdown());
  }
}
