package org.apache.phoenix.jdbc;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;


public class ParallelPhoenixPreparedStatementTest {

    ParallelPhoenixContext context;
    CompletableFuture<PhoenixMonitoredPreparedStatement> future1;
    CompletableFuture<PhoenixMonitoredPreparedStatement> future2;
    PhoenixMonitoredPreparedStatement statement1;
    PhoenixMonitoredPreparedStatement statement2;


    ParallelPhoenixPreparedStatement phoenixPreparedStatement;

    @Before
    public void init() throws Exception {
		context = new ParallelPhoenixContext(new Properties(), Mockito.mock(HighAvailabilityGroup.class),
		    HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(), null);

        statement1 = Mockito.mock(PhoenixMonitoredPreparedStatement.class);
        statement2 = Mockito.mock(PhoenixMonitoredPreparedStatement.class);

        future1 = CompletableFuture.completedFuture(statement1);
        future2 = CompletableFuture.completedFuture(statement2);

        phoenixPreparedStatement = new ParallelPhoenixPreparedStatement(context,future1,future2);
    }

    @Test
    public void getStatement1() throws SQLException {
        future1 = Mockito.mock(CompletableFuture.class);
        future2 = Mockito.mock(CompletableFuture.class);
        phoenixPreparedStatement = new ParallelPhoenixPreparedStatement(context,future1,future2);
        assertEquals(future1, phoenixPreparedStatement.getStatement1());
    }

    @Test
    public void getStatement2() throws SQLException {
        future1 = Mockito.mock(CompletableFuture.class);
        future2 = Mockito.mock(CompletableFuture.class);
        phoenixPreparedStatement = new ParallelPhoenixPreparedStatement(context,future1,future2);
        assertEquals(future2, phoenixPreparedStatement.getStatement2());
    }

    @Test
    public void executeQuery() throws SQLException, ExecutionException, InterruptedException {
        ResultSet mockResultSet1 = Mockito.mock(ResultSet.class);
        ResultSet mockResultSet2 = Mockito.mock(ResultSet.class);

        Mockito.when(statement1.executeQuery()).thenReturn(mockResultSet1);
        Mockito.when(statement2.executeQuery()).thenReturn(mockResultSet2);

        ResultSet rs = phoenixPreparedStatement.executeQuery();

        //TODO: make this less dependant on sleep
        Thread.sleep(5000);

        Mockito.verify(statement1).executeQuery();
        Mockito.verify(statement2).executeQuery();
        ParallelPhoenixResultSet parallelRS = (ParallelPhoenixResultSet) rs;
        assertEquals(mockResultSet1,parallelRS.getResultSetFuture1().get());
        assertEquals(mockResultSet2,parallelRS.getResultSetFuture2().get());
    }

    @Test
    public void setInt() throws SQLException, ExecutionException, InterruptedException {
        phoenixPreparedStatement.setInt(1,2);

        //TODO: make this less dependant on sleep
        Thread.sleep(5000);

        Mockito.verify(statement1).setInt(1,2);
        Mockito.verify(statement2).setInt(1,2);
    }

    @Test
    public void execute() throws SQLException, ExecutionException, InterruptedException {
        phoenixPreparedStatement.execute();

        //TODO: make this less dependant on sleep
        Thread.sleep(5000);

        Mockito.verify(statement1).execute();
        Mockito.verify(statement2).execute();
    }
}