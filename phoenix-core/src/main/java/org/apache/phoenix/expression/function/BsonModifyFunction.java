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
package org.apache.phoenix.expression.function;

import com.google.common.base.Preconditions;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import com.jayway.jsonpath.spi.mapper.JsonSmartMappingProvider;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.BsonValueParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.JsonValueBParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.ByteBufferBsonInput;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Built-in function for JSON_VALUE
 * JSON_VALUE(<column_with_json/json_string>, <path> [returning <type>])
 * Extracts a scalar JSON value—everything except object and array—and returns it as a native type.
 * The optional returning clause performs a typecast. Without a returning clause, JSON_VALUE returns a string.
 *
 */
@FunctionParseNode.BuiltInFunction(name = BsonModifyFunction.NAME, nodeClass = BsonValueParseNode.class,
        args = {
                @FunctionParseNode.Argument(allowedTypes = { PBson.class, PVarchar.class }),
                @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }) ,
                @FunctionParseNode.Argument(allowedTypes = { PVarchar.class })})
public class BsonModifyFunction extends ScalarFunction {

    public static final String NAME = "BSON_MODIFY";
    private static final String ARRAY_SUBSCRIPT_PATTERN = "(.*)\\[(\\d+)\\]";

    // This is called from ExpressionType newInstance
    public BsonModifyFunction() {

    }

    public BsonModifyFunction(List<Expression> children, String jsonPath, String newValue) {
        super(children);
        Preconditions.checkNotNull(jsonPath);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getColValExpr().evaluate(tuple, ptr)) {
            return false;
        }
        if (ptr == null || ptr.getLength() == 0) {
            return true;
        }

        // Column name or JSON string
        RawBsonDocument top = (RawBsonDocument) PBson.INSTANCE.toObject(ptr, getColValExpr().getSortOrder());

        if (!getJSONPathExpr().evaluate(tuple, ptr)) {
            return false;
        }

        if (ptr.getLength() == 0) {
            return true;
        }

        String jsonPathExprStr = (String) PVarchar.INSTANCE.toObject(ptr,
            getJSONPathExpr().getSortOrder());
        if (jsonPathExprStr == null) {
            return true;
        }

        if (!getNewValueExpr().evaluate(tuple, ptr)) {
            return false;
        }

        String newVal = (String)PVarchar.INSTANCE.toObject(ptr,
            getNewValueExpr().getSortOrder());

        Configuration conf = Configuration
            .builder()
            .jsonProvider(new BsonJsonProvider())
            .mappingProvider(new JsonSmartMappingProvider())
            .build();
        BsonValue newValue = JsonPath.using(conf).parse(newVal).json();
        BsonDocument root = fromRaw(top);
        JsonPath.using(conf).parse(root).set(jsonPathExprStr, newValue);
        RawBsonDocument updated =
            new RawBsonDocumentCodec().decode(new BsonDocumentReader(root), DecoderContext.builder().build());
        ByteBuffer buffer = updated.getByteBuffer().asNIO();
        ptr.set(buffer.array(), buffer.arrayOffset(), buffer.limit());
        return true;
    }

    private Expression getNewValueExpr() {
        return getChildren().get(2);
    }

    private Expression getColValExpr() {
        return getChildren().get(0);
    }

    private Expression getJSONPathExpr() {
        return getChildren().get(1);
    }

    private BsonDocument fromRaw(RawBsonDocument rawDocument) {
        // Transform to an in memory BsonDocument instance
        BsonBinaryReader bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(rawDocument.getByteBuffer()));
        try {
            return new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
        } finally {
            bsonReader.close();
        }
    }

    @Override
    public PDataType getDataType() {
        return PBson.INSTANCE;
    }
}
