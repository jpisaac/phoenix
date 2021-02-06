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

package org.apache.phoenix.pherf.rules;

import com.google.common.base.Preconditions;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataSequence;
import org.apache.phoenix.pherf.configuration.DataTypeMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class SequentialListDataGenerator implements RuleBasedDataGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SequentialListDataGenerator.class);

    private final Column columnRule;
    private final AtomicLong counter;

    public SequentialListDataGenerator(Column columnRule) {
        Preconditions.checkArgument(columnRule.getDataSequence() == DataSequence.SEQUENTIAL);
        Preconditions.checkArgument(columnRule.getDataValues().size() > 0);
        this.columnRule = columnRule;
        counter = new AtomicLong(0);
        LOGGER.info(String.format("Adding rule for %s, %d", columnRule.getName(), columnRule.getDataValues().size()));
    }

    /**
     * Note that this method rolls over for attempts to get larger than maxValue
     * @return new DataValue
     */
    @Override
    public DataValue getDataValue() {
        long pos = counter.incrementAndGet();
        int index = (int) pos % columnRule.getDataValues().size();
        return columnRule.getDataValues().get(index);
    }

}
