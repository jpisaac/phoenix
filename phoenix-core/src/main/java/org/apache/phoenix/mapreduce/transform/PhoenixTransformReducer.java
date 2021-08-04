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
package org.apache.phoenix.mapreduce.transform;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.schema.transform.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reducer class that does only one task and that is to update the index state of the table.
 */
public class PhoenixTransformReducer extends
        Reducer<ImmutableBytesWritable, IntWritable, NullWritable, NullWritable> {
    private AtomicBoolean calledOnce = new AtomicBoolean(false);

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PhoenixTransformReducer.class);

    @Override
    protected void setup(Context context) throws IOException {
    }

    @Override
    protected void reduce(ImmutableBytesWritable arg0, Iterable<IntWritable> arg1,
                          Context context)
            throws IOException, InterruptedException {
        if (!calledOnce.compareAndSet(false, true)) {
            return;
        }

        try (final Connection
                     connection = ConnectionUtil.getInputConnection(context.getConfiguration())){
            // Complete full Transform and add a partial transform
            Transform.completeTransform(connection, context.getConfiguration());
        } catch (Exception e) {
            LOGGER.error(" Failed to complete transform", e);
            throw new RuntimeException(e.getMessage());
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{

    }
}