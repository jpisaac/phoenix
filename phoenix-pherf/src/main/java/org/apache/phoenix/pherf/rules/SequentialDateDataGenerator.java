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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SequentialDateDataGenerator implements RuleBasedDataGenerator {
    private final static SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final Column columnRule;
    private final AtomicInteger counter;
    private final Calendar now = GregorianCalendar.getInstance();

    public SequentialDateDataGenerator(Column columnRule) {
        Preconditions.checkArgument(columnRule.getDataSequence() == DataSequence.SEQUENTIAL);
        Preconditions.checkArgument(isDateType(columnRule.getType()));
        this.columnRule = columnRule;
        counter = new AtomicInteger(0);
    }

    /**
     * Note that this method rolls over for attempts to get larger than maxValue
     * @return new DataValue
     */
    @Override
    public DataValue getDataValue() {
        now.add(Calendar.SECOND, counter.getAndIncrement());
        return new DataValue(columnRule.getType(), SIMPLE_DATE_FORMAT.format(now.getTime()));
    }

    // Probably could go into a util class in the future
    boolean isDateType(DataTypeMapping mapping) {
        switch (mapping) {
        case DATE:
        case TIMESTAMP:
            return true;
        default:
            return false;
        }
    }
}
