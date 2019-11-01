/*
 * Copyright, 1999-2019, SALESFORCE.com
 * All Rights Reserved
 * Company Confidential
 */
package org.apache.phoenix.jdbc.salesforce;

import org.apache.phoenix.jdbc.LoggingPhoenixConnection;
import org.apache.phoenix.jdbc.LoggingPhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixMetricsLog;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SfdcLoggingPhoenixConnection extends LoggingPhoenixConnection {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SfdcLoggingPhoenixConnection.class);
    public static final String SQL_NOT_LOGGED = "SQL Not Logged";

    SfdcLoggingPhoenixConnection(Connection conn, PhoenixMetricsLog phoenixMetricsLog) {
        super(conn, phoenixMetricsLog);
    }

    @Override public void close() throws SQLException {
        //super.close will silently close all the underlying statements/result sets
        //as this will hit the delegate class which will then close without logging
        List<PhoenixStatement>
                statements =
                this.conn.unwrap(PhoenixConnection.class).getStatements();
        for (PhoenixStatement statement : statements) {
            if (statement != null && !statement.isClosed()) {
                String mutationString = statement.getUpdateOperation() == null ? "null" : statement.getUpdateOperation().toString();
                LOGGER.warn(String.format("Unclosed Statement: %s with mutation operation %s", statement.toString(), mutationString));
                // Cannot iterate over all result sets since we get a ConcurrentModificationException
                // when trying to close them below since we remove each resultset from the statement
                // when doing PhoenixResultSet#close.
                ResultSet rs = statement.getResultSet();
                if (rs != null && !rs.isClosed()) {
                    PhoenixResultSet phoenixResultSet = rs.unwrap(PhoenixResultSet.class);
                    LOGGER.warn(String.format("Unclosed ResultSet %s", phoenixResultSet.toString()));
                    //trigger the metrics, but we no longer have access to the sql
                    LoggingPhoenixResultSet tempResultSet =
                        new LoggingPhoenixResultSet(phoenixResultSet, this.getPhoenixMetricsLog(), SQL_NOT_LOGGED);
                    tempResultSet.close();
                }
            }
        }
        super.close();
    }

}