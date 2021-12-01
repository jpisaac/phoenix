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
import com.google.gson.JsonElement;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.JsonValueDCParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJsonDC;
import org.apache.phoenix.schema.types.PVarchar;

import java.util.List;

/**
 * Built-in function for JSON_VALUE
 * JSON_VALUE(<column_with_json/json_string>, <path> [returning <type>])
 * Extracts a scalar JSON value—everything except object and array—and returns it as a native type.
 * The optional returning clause performs a typecast. Without a returning clause, JSON_VALUE returns a string.
 *
 */
@FunctionParseNode.BuiltInFunction(name = JsonModifyDCFunction.NAME, nodeClass = JsonValueDCParseNode.class,
        args = {
                @FunctionParseNode.Argument(allowedTypes = { PVarchar.class, PJsonDC.class}),
                @FunctionParseNode.Argument(allowedTypes = { PVarchar.class }) ,
                @FunctionParseNode.Argument(allowedTypes = { PVarchar.class })})
public class JsonModifyDCFunction extends ScalarFunction {

    public static final String NAME = "JSON_MODIFY_DC";
    private String jsonPath;
    private String newValue;
    private boolean topLevel;
    private String dynColumnName;
    private byte[] dynColumnValue;

    // This is called from ExpressionType newInstance
    public JsonModifyDCFunction() {

    }

    public JsonModifyDCFunction(List<Expression> children, String jsonPath, String newValue) {
        super(children);
        Preconditions.checkNotNull(jsonPath);
        this.jsonPath = jsonPath;
        this.newValue = newValue;
        this.topLevel = false;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getColValExpr().evaluate(tuple,  ptr)) {
            return false;
        }
        if (ptr == null || ptr.getLength() == 0) {
            return true;
        }

        // Column name or JSON string
        String colValue = (String)PVarchar.INSTANCE.toObject(ptr,
                getColValExpr().getSortOrder());
        if (colValue == null) {
            return true;
        }

        if (!getJSONPathExpr().evaluate(tuple, ptr)) {
            return false;
        }

        if (ptr.getLength() == 0) {
            return true;
        }

        String jsonPathExprStr = (String)PVarchar.INSTANCE.toObject(ptr,
                getJSONPathExpr().getSortOrder());
        if (jsonPathExprStr == null) {
            return true;
        }

        if (!getNewValueExpr().evaluate(tuple, ptr)) {
            return false;
        }

        String newVal = (String)PVarchar.INSTANCE.toObject(ptr,
                getNewValueExpr().getSortOrder());

            // TODO: support reading with type
            //String jsonFieldString = JsonPath.read(colValue, jsonPathExprStr);
        String jsonFieldString = setJsonStringForPath(colValue, jsonPathExprStr, newVal);
        ptr.set(PVarchar.INSTANCE.toBytes(jsonFieldString));

        return true;
    }

    public boolean isTopLevel() {
        return topLevel;
    }

    public String getColumnName(){
        return dynColumnName.toUpperCase();
    }

    public byte[] getDynColumnValue() {
        return dynColumnValue;
    }

    private String setJsonStringForPath(String strJson, String strJPath, String newValue) {
        Configuration conf = Configuration.builder().jsonProvider(new GsonJsonProvider()).mappingProvider(new GsonMappingProvider()).build();
        DocumentContext documentContext = JsonPath.using(conf).parse(strJson);
        JsonElement newJsonElem = JsonPath.using(conf).parse(newValue).json();

        JsonElement changedElem = JsonPath.using(conf).parse(strJPath).json();
        dynColumnName = strJPath.substring(strJPath.indexOf("$.") + "$.".length());
        if (newJsonElem.isJsonPrimitive() && !dynColumnName.contains(".")) {
            topLevel = true;
            dynColumnValue = Bytes.toBytes(newValue);
        }

        String result = documentContext.set(strJPath, newJsonElem).jsonString();

        if (result.startsWith("\"") && result.endsWith("\"")) {
            result = result.substring(1, result.length()-1);
        }
        return result;
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

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }
}
