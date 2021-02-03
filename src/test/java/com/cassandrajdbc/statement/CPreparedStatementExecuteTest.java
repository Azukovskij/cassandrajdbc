/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.statement;

import static com.cassandrajdbc.test.util.ResultSetMatcher.hasResultCount;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.cassandrajdbc.connection.CassandraConnection;
import com.cassandrajdbc.test.util.MethodPointer;
import com.datastax.driver.core.Cluster;

@RunWith(Parameterized.class)
public class CPreparedStatementExecuteTest {
    
    private static final String KEYSPACE_NAME = "CPreparedStatementTests";
    private static final String TABLE_NAME = KEYSPACE_NAME + ".ExecuteTest";
    
    private static CassandraConnection connection;

    private final CPreparedStatement testObject;
    private final MethodPointer execute;
    private final Matcher<Object> expectedResult;
    
    @BeforeClass
    public static void connect() {
        connection = new CassandraConnection(Cluster.builder()
            .addContactPoint("localhost")
            .build().connect());
        connection.getSession().execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME
            + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        connection.getSession().execute("DROP TABLE IF EXISTS " + TABLE_NAME);
        connection.getSession().execute("CREATE TABLE " + TABLE_NAME 
            + "(ID VARCHAR, DATA VARCHAR, PRIMARY KEY (ID))");
        
    }
    
    @Parameterized.Parameters(name = "{2} q={1} ")
    public static List<Object[]> data() throws Exception {
        return Arrays.asList(new Object[][] {
            // executeQuery
            {createData(3), "SELECT * FROM " + TABLE_NAME, new MethodPointer("executeQuery"), hasResultCount(3)},
            {createData(5), null, new MethodPointer("executeQuery", "SELECT * FROM " + TABLE_NAME), hasResultCount(5)},
            // execute
            {nodata(), "SELECT * FROM " + TABLE_NAME, new MethodPointer("execute"), equalTo(false)},
            {createData(1), null, new MethodPointer("execute", "SELECT * FROM " + TABLE_NAME), equalTo(true)},
            {createData(1), null, new MethodPointer("execute", "SELECT * FROM " + TABLE_NAME, 1), equalTo(true)},
            {createData(1), null, new MethodPointer("execute", "SELECT * FROM " + TABLE_NAME, new int[] {1}), equalTo(true)},
            {createData(1), null, new MethodPointer("execute", "SELECT * FROM " + TABLE_NAME, new String[] {"ID"}), equalTo(true)},
            {nodata(), null, new MethodPointer("execute", "INSERT INTO " + TABLE_NAME + "(ID,DATA) VALUES('t','t')"), equalTo(false)},
            {createData(1), null, new MethodPointer("execute", "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'"), equalTo(false)},
            {nodata(), null, new MethodPointer("execute", "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "2 (ID VARCHAR, PRIMARY KEY (ID))"), equalTo(false)},
            // executeUpdate
            {createData(1), "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'", new MethodPointer("executeUpdate"), equalTo(0)},
            {createData(1), null, new MethodPointer("executeUpdate", "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'"), equalTo(0)},
            {createData(1), null, new MethodPointer("executeUpdate", "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'", 1), equalTo(0)},
            {createData(1), null, new MethodPointer("executeUpdate", "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'", new int[] {1}), equalTo(0)},
            {createData(1), null, new MethodPointer("executeUpdate", "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'", new String[] {"ID"}), equalTo(0)},
            // executeBatch
            {createToBatch(3), null, new MethodPointer("executeBatch"), equalTo(new int[]{-2, -2, -2})},
            {nodata().andThen(PreparedStatement::addBatch), "INSERT INTO " + TABLE_NAME + "(ID,DATA) VALUES('b','b')", new MethodPointer("executeBatch"), equalTo(new int[]{-2})},
            {createToBatch(4).andThen(Statement::clearBatch).andThen(createToBatch(1)), null, new MethodPointer("executeBatch"), equalTo(new int[]{-2})}
        });
    }
    
    public CPreparedStatementExecuteTest(Action initializer, String stmtSql, MethodPointer execute, Matcher<Object> expectedResult) throws SQLException {
        this.testObject = new CPreparedStatement(connection, stmtSql);
        this.execute = execute;
        this.expectedResult = expectedResult;
        initializer.run(this.testObject);
    }
    
    @Test
    public void shouldProduceExpectedResult() throws SQLException {
        assertThat(execute.invoke(testObject), expectedResult);
    }

    private static Action nodata() {
        return stmt -> connection.getSession().execute("TRUNCATE " + TABLE_NAME);
    }

    private static Action createData(int count) {
        return stmt -> {
            nodata().run(stmt);
            prepareBatch(count).forEach(connection.getSession()::execute);
        };
    }

    private static Action createToBatch(int count) {
        return stmt -> {
            nodata().run(stmt);
            prepareBatch(count).forEach(sql -> {
                try {
                    stmt.addBatch(sql);
                } catch (SQLException e) {
                    throw new AssertionError(e);
                }
            });
        };
    }
    
    public static Stream<String> prepareBatch(int count) {
        return IntStream.range(0, count)
            .mapToObj(i -> "INSERT INTO " + TABLE_NAME + "(ID,DATA) VALUES ('" + i + "','V" + i + "')");
    }

    @FunctionalInterface
    private static interface Action {
        
        void run(PreparedStatement stmt) throws SQLException;
        
        default Action andThen(Action after) {
            return (t) -> { run(t); after.run(t); };
        }
    }
    
}
