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

import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.function.JsonValueDCFunction;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PVarchar;

import javax.naming.Name;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.phoenix.compile.FromCompiler.addDynamicColumns;


/**
 * 
 * Class that creates a new select statement by filtering out nodes.
 * Currently only supports filtering out boolean nodes (i.e. nodes
 * that may be ANDed and ORed together.
 *
 * TODO: generize this
 * 
 * @since 0.1
 */
public class SelectStatementRewriter extends ParseNodeRewriter {
    
    /**
     * Rewrite the select statement by filtering out expression nodes from the WHERE clause
     * @param statement the select statement from which to filter.
     * @param removeNodes expression nodes to filter out of WHERE clause.
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement removeFromWhereClause(SelectStatement statement, Set<ParseNode> removeNodes) throws SQLException {
        if (removeNodes.isEmpty()) {
            return statement;
        }
        ParseNode where = statement.getWhere();
        SelectStatementRewriter rewriter = new SelectStatementRewriter(removeNodes);
        where = where.accept(rewriter);
        // Return new SELECT statement with updated WHERE clause
        return NODE_FACTORY.select(statement, where);
    }

    public static SelectStatement rewriteForJson(StatementContext context, SelectStatement statement) throws SQLException {
        List<AliasedNode> selectNodes = statement.getSelect();
        List<AliasedNode> newSelectNodes = new ArrayList<>();
        ParseNode newWhereNode = statement.getWhere();

        if (selectNodes != null) {
            List<ColumnDef> newColumnDefs = new ArrayList<>();
            List<String> newColDefNames = new ArrayList<>();
            for (AliasedNode selectNode : selectNodes) {
                if (selectNode.getNode().getChildren().size() > 1 &&
                        selectNode.getNode().toString().contains(JsonValueDCFunction.NAME)) {
                    String cfName = selectNode.getNode().getChildren().get(0).toString();
                    String exprName = selectNode.getNode().getChildren().get(1).toString();
                    exprName=exprName.replace("'","");
                    String dynColName = exprName.substring(exprName.indexOf("$.") + "$.".length());
                    if (!dynColName.contains(".") && !dynColName.contains("*") && !dynColName.contains("[")) {
                        ColumnName newDynColName = ColumnName.newColumnName(cfName, dynColName);
                        ColumnDef dynColDef = new ParseNodeFactory().columnDef(newDynColName,
                                PVarchar.INSTANCE.getSqlTypeName(), false, null, null, false, org.apache.phoenix.schema.SortOrder.ASC,
                                null, false);

                        if (!newColDefNames.contains(newDynColName.toString())) {
                            newColumnDefs.add(dynColDef);
                            newColDefNames.add(newDynColName.toString());
                        }
                        ColumnParseNode newColumnParseNode = NODE_FACTORY.column(
                                TableName.create(null, cfName),
                                dynColName, dynColName);
                        AliasedNode newDynColNode = NODE_FACTORY.aliasedNode(null, newColumnParseNode);
                        newSelectNodes.add(newDynColNode);
                    } else {
                        newSelectNodes.add(selectNode);
                    }
                } else {
                    newSelectNodes.add(selectNode);
                }
            }

            ParseNode whereNode = statement.getWhere();

                if (whereNode!= null && whereNode.getChildren().size() > 1 &&
                        whereNode.getChildren().get(0).toString().contains(JsonValueDCFunction.NAME)) {
                    String cfName = whereNode.getChildren().get(0).getChildren().get(0).toString();
                    String exprName = whereNode.getChildren().get(0).getChildren().get(1).toString();
                    exprName = exprName.replace("'", "");
                    String dynColName = exprName.substring(exprName.indexOf("$.") + "$.".length());
                    if (!dynColName.contains(".") && !dynColName.contains("*") && !dynColName.contains("[")) {
                        ColumnName newDynColName = ColumnName.newColumnName(cfName, dynColName);
                        ColumnDef dynColDef = new ParseNodeFactory().columnDef(newDynColName,
                                PVarchar.INSTANCE.getSqlTypeName(), false, null, null, false, org.apache.phoenix.schema.SortOrder.ASC,
                                null, false);

                        if (!newColDefNames.contains(newDynColName.toString())) {
                            newColumnDefs.add(dynColDef);
                            newColDefNames.add(newDynColName.toString());
                        }
                        ColumnParseNode newColumnParseNode = NODE_FACTORY.column(
                                TableName.create(null, cfName),
                                dynColName, dynColName);
//                        ColumnParseNode newColumnParseNode = NODE_FACTORY.column(
//                                TableName.create(null, cfName),
//                                dynColName, "type");
                        newWhereNode = new EqualParseNode(newColumnParseNode, whereNode.getChildren().get(1));
                    }
                }

            if (newColumnDefs.size() > 0) {
                NamedTableNode tblNode = null;
                if (statement.getFrom() instanceof NamedTableNode) {
                    tblNode = (NamedTableNode) statement.getFrom();
                    List<ColumnDef> columnDefs = new ArrayList<>(tblNode.getDynamicColumns());
                    TableRef newTableDef = new TableRef(context.getCurrentTable());
                    PTable pTableWithDynamicColumns = addDynamicColumns(newColumnDefs, newTableDef.getTable());
                    newTableDef.setTable(pTableWithDynamicColumns);
                    for (ColumnDef colDef : newColumnDefs) {
                        columnDefs.add(colDef);
                    }

                    NamedTableNode namedTableNode = NamedTableNode.create(tblNode.getAlias(), tblNode.getName(), columnDefs);
                    SelectStatement selectStatement = SelectStatement.create(statement, namedTableNode, newSelectNodes);
                    context.setCurrentTable(newTableDef);
                    SelectStatement newSelectStmt = NODE_FACTORY.select(selectStatement, newWhereNode);
                    ColumnResolver newColResolver =FromCompiler.getResolverForQuery(newSelectStmt, context.getConnection());
                    context.setResolver(newColResolver);
                    return newSelectStmt;
                }
            }
        }
        return statement;
    }

    /**
     * Rewrite the select statement by filtering out expression nodes from the HAVING clause
     * and anding them with the WHERE clause.
     * @param statement the select statement from which to move the nodes.
     * @param moveNodes expression nodes to filter out of HAVING clause and add to WHERE clause.
     * @return new select statement
     * @throws SQLException 
     */
    public static SelectStatement moveFromHavingToWhereClause(SelectStatement statement, Set<ParseNode> moveNodes) throws SQLException {
        if (moveNodes.isEmpty()) {
            return statement;
        }
        ParseNode andNode = NODE_FACTORY.and(new ArrayList<ParseNode>(moveNodes));
        ParseNode having = statement.getHaving();
        SelectStatementRewriter rewriter = new SelectStatementRewriter(moveNodes);
        having = having.accept(rewriter);
        ParseNode where = statement.getWhere();
        if (where == null) {
            where = andNode;
        } else {
            where = NODE_FACTORY.and(Arrays.asList(where,andNode));
        }
        // Return new SELECT statement with updated WHERE and HAVING clauses
        return NODE_FACTORY.select(statement, where, having);
    }
    
    private static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    private final Set<ParseNode> removeNodes;
    
    private SelectStatementRewriter(Set<ParseNode> removeNodes) {
        this.removeNodes = removeNodes;
    }
    
    private static interface CompoundNodeFactory {
        ParseNode createNode(List<ParseNode> children);
    }
    
    private boolean enterCompoundNode(ParseNode node) {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    private ParseNode leaveCompoundNode(CompoundParseNode node, List<ParseNode> children, CompoundNodeFactory factory) {
        int newSize = children.size();
        int oldSize = node.getChildren().size();
        if (newSize == oldSize) {
            return node;
        } else if (newSize > 1) {
            return factory.createNode(children);
        } else if (newSize == 1) {
            // TODO: keep or collapse? Maybe be helpful as context of where a problem occurs if a node could not be consumed
            return(children.get(0));
        } else {
            return null;
        }
    }
    
    @Override
    public boolean visitEnter(AndParseNode node) throws SQLException {
        return enterCompoundNode(node);
    }

    @Override
    public ParseNode visitLeave(AndParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.and(children);
            }
        });
    }

    @Override
    public boolean visitEnter(OrParseNode node) throws SQLException {
        return enterCompoundNode(node);
    }

    @Override
    public ParseNode visitLeave(OrParseNode node, List<ParseNode> nodes) throws SQLException {
        return leaveCompoundNode(node, nodes, new CompoundNodeFactory() {
            @Override
            public ParseNode createNode(List<ParseNode> children) {
                return NODE_FACTORY.or(children);
            }
        });
    }
    
    @Override
    public boolean visitEnter(ComparisonParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }

    @Override
    public ParseNode visitLeave(ComparisonParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
    
    @Override
    public boolean visitEnter(LikeParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    @Override
    public ParseNode visitLeave(LikeParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
    
    @Override
    public boolean visitEnter(InListParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    @Override
    public ParseNode visitLeave(InListParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
    
    @Override
    public boolean visitEnter(InParseNode node) throws SQLException {
        if (removeNodes.contains(node)) {
            return false;
        }
        return true;
    }
    
    @Override
    public ParseNode visitLeave(InParseNode node, List<ParseNode> c) throws SQLException {
        return c.isEmpty() ? null : node;
    }
}
