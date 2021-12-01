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

package org.apache.phoenix.schema.types;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;

import java.sql.SQLException;
import java.sql.Types;
import java.text.Format;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * A Phoenix data type to represent JSON. The json data type stores an exact copy of the input text,
 * which processing functions must reparse on each execution. Because the json type stores an exact
 * copy of the input text, it will preserve semantically-insignificant white space between tokens,
 * as well as the order of keys within JSON objects. Also, if a JSON object within the value
 * contains the same key more than once, all the key/value pairs are kept. It stores the data as
 * string in single column of HBase and it has same data size limit as Phoenix's Varchar.
 * <p>
 * JSON data types are for storing JSON (JavaScript Object Notation) data, as specified in RFC 7159.
 * Such data can also be stored as text, but the JSON data types have the advantage of enforcing
 * that each stored value is valid according to the JSON rules.
 */
// Deserialize/Serialize byte[]
public class PJsonB extends PVarbinary{

    public static final PJsonB INSTANCE = new PJsonB();

    private PJsonB() {
        super("JSONB", Types.VARBINARY, byte[].class, null, 50);
    }

    @Override
    public boolean canBePrimaryKey() {
        return false;
    }

    @Override
    public boolean isEqualitySupported() {
        return false;
    }

    @Override
    public int toBytes(Object object, byte[] bytes, int offset) {

        if (object == null) {
            return 0;
        }
        byte[] b = toBytes(object);
        System.arraycopy(b, 0, bytes, offset, b.length);
        return b.length;

    }

    @Override
    public byte[] toBytes(Object object, SortOrder sortOrder) {
        if (object == null) {
            return ByteUtil.EMPTY_BYTE_ARRAY;
        }

        try {
            if (object instanceof Map) {
                return PhoenixJson.toBytes((Map)object);
            } else {
                if (object instanceof String) {
                   // return toBytes(object);
                    //return PhoenixJson.toBytes(PhoenixJson.fromBytes(Bytes.toBytes((String) object)));
                    //return Bytes.toBytes((String) object);
                    return PhoenixJson.toBytes(PhoenixJson.fromBytes(Bytes.toBytes((String) object)));
                    //return PhoenixJson.convertToFlattenMapBytes(Bytes.toBytes((String) object));
                }
                return PhoenixJson.convertToFlattenMapBytes((byte[]) object);
            }
        } catch (SQLException throwables) {
            throw new IllegalDataException(throwables);
        }
    }

    @Override
    public byte[] toBytes(Object object)  {
        if (object == null) {
            return ByteUtil.EMPTY_BYTE_ARRAY;
        }

        try {
            if (object instanceof Map) {
                return PhoenixJson.toBytes((Map)object);
            } else {
                if (object instanceof String) {
                    object = Bytes.toBytes((String) object);
                }
                return PhoenixJson.convertToFlattenMapBytes((byte[]) object);
            }
        } catch (SQLException throwables) {
            throw new IllegalDataException(throwables);
        }
    }

    @Override
    public Object toObject(byte[] bytes, int offset, int length,
                           @SuppressWarnings("rawtypes") PDataType actualType, SortOrder sortOrder,
                           Integer maxLength, Integer scale) {

        Object object =
                PVarbinary.INSTANCE.toObject(bytes, offset, length, actualType, sortOrder, maxLength,
                        scale);
        /*
         * avoiding the type casting of object to String by calling toString() since String's
         * toString() returns itself.
         */
        //return object == null ? object : getPhoenixJson(object);
        return PhoenixJson.fromBytes((byte[]) object);
    }

    @Override
    public Object toObject(Object object, @SuppressWarnings("rawtypes") PDataType actualType) {
        if (object == null) {
            return null;
        }
        return object;
    }

    @Override
    public boolean isCoercibleTo(@SuppressWarnings("rawtypes") PDataType targetType) {
        return equalsAny(targetType, this, PVarbinary.INSTANCE);

    }

    @Override
    public boolean isFixedWidth() {
        return false;
    }

    @Override
    public int estimateByteSize(Object o) {
        PhoenixJson phoenixJson = (PhoenixJson) o;
        return phoenixJson.estimateByteSize();
    }

    @Override
    public Integer getByteSize() {
        return null;
    }


//    @Override
//    public Object toObject(String value) {
//        return getPhoenixJson(value);
//    }

    @Override
    public boolean isBytesComparableWith(@SuppressWarnings("rawtypes") PDataType otherType) {
        return otherType == PVarbinary.INSTANCE ;
    }

    @Override
    public String toStringLiteral(Object o, Format formatter) {
        if (o == null) {
            return StringUtil.EMPTY_STRING;
        }
        PhoenixJson phoenixJson = (PhoenixJson) o;
        return PVarchar.INSTANCE.toStringLiteral(phoenixJson.toString(), formatter);
    }
}
