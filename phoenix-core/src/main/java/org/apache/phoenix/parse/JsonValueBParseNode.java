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
package org.apache.phoenix.parse;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.function.JsonModifyBFunction;
import org.apache.phoenix.expression.function.JsonModifyDCFunction;
import org.apache.phoenix.expression.function.JsonModifyFunction;
import org.apache.phoenix.expression.function.JsonValueBFunction;
import org.apache.phoenix.expression.function.JsonValueDCFunction;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PJson;
import org.apache.phoenix.schema.types.PJsonB;
import org.apache.phoenix.schema.types.PVarchar;

import java.sql.SQLException;
import java.util.List;

public class JsonValueBParseNode extends FunctionParseNode {
    private String name;
    public JsonValueBParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
        super(name, children, info);
        this.name = name;
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context) throws SQLException {
        PDataType dataType = children.get(0).getDataType();
        String jsonPath = (String)((LiteralExpression)children.get(1)).getValue();
        if (!dataType.isCoercibleTo(PJsonB.INSTANCE)) {
            throw new SQLException(dataType + " type is unsupported for JSON_VALUE_B().");
        }
        if (name.equalsIgnoreCase(JsonModifyBFunction.NAME)) {
            return new JsonModifyBFunction(children, jsonPath,  (String)((LiteralExpression)children.get(2)).getValue());
        } else {
            return new JsonValueBFunction(children, jsonPath);
        }
    }
}
