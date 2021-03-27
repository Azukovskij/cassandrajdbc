/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.statement;

import static com.cassandrajdbc.test.util.CassandraTestConnection.getConnection;
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

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;

import com.cassandrajdbc.test.util.MethodPointer;

@RunWith(Parameterized.class)
@TestExecutionListeners({ CassandraUnitTestExecutionListener.class })
@CassandraDataSet(keyspace = "CPreparedStatementTests", value = { "CPreparedStatementTests/Tables.cql" })
@EmbeddedCassandra
public class CPreparedStatementExecuteTest {
    
    private static final String KEYSPACE_NAME = "CPreparedStatementTests";
    private static final String TABLE_NAME = KEYSPACE_NAME + ".ExecuteTest";
    
    @ClassRule
    public static final SpringClassRule scr = new SpringClassRule();

    @Rule
    public final SpringMethodRule smr = new SpringMethodRule();

    private final Action initializer;
    private final String stmtSql;
    private final MethodPointer execute;
    private final Matcher<Object> expectedResult;

    
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
            {createData(1), "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'", new MethodPointer("executeUpdate"), equalTo(1)},
            {createData(1), null, new MethodPointer("executeUpdate", "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'"), equalTo(1)},
            {createData(1), null, new MethodPointer("executeUpdate", "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'", 1), equalTo(1)},
            {createData(1), null, new MethodPointer("executeUpdate", "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'", new int[] {1}), equalTo(1)},
            {createData(1), null, new MethodPointer("executeUpdate", "UPDATE " + TABLE_NAME + " SET DATA='3' WHERE ID='1'", new String[] {"ID"}), equalTo(1)},
            // executeBatch
            {createToBatch(3), null, new MethodPointer("executeBatch"), equalTo(new int[]{-2, -2, -2})},
            {nodata().andThen(PreparedStatement::addBatch), "INSERT INTO " + TABLE_NAME + "(ID,DATA) VALUES('b','b')", new MethodPointer("executeBatch"), equalTo(new int[]{-2})},
            {createToBatch(4).andThen(Statement::clearBatch).andThen(createToBatch(1)), null, new MethodPointer("executeBatch"), equalTo(new int[]{-2})}
        });
    }
    
    public CPreparedStatementExecuteTest(Action initializer, String stmtSql, MethodPointer execute, Matcher<Object> expectedResult) throws SQLException {
        this.initializer = initializer;
        this.stmtSql = stmtSql;
        this.execute = execute;
        this.expectedResult = expectedResult;
    }
    
    @Test
    public void shouldProduceExpectedResult() throws SQLException {
        CPreparedStatement  testObject = new CPreparedStatement(getConnection(), stmtSql);
        initializer.run(testObject);
        
        assertThat(execute.invoke(testObject), expectedResult);
    }

    private static Action nodata() {
        return stmt -> getConnection().getSession().execute("TRUNCATE " + TABLE_NAME);
    }

    private static Action createData(int count) {
        return stmt -> {
            nodata().run(stmt);
            prepareBatch(count).forEach(getConnection().getSession()::execute);
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
