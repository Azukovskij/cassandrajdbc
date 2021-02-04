/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.result;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cassandrajdbc.connection.CassandraConnection;
import com.cassandrajdbc.statement.CPreparedStatement;
import com.cassandrajdbc.test.util.ResultSetMatcher.CheckedFunction;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;

public class CResultSetIterationTest {
    
    private static final String KEYSPACE_NAME = "CPreparedStatementTests";
    private static final String TABLE_NAME = KEYSPACE_NAME + ".ResultSetTest";
    
    private static CassandraConnection connection;
    
    @BeforeClass
    public static void connect() {
        connection = new CassandraConnection(Cluster.builder()
            .addContactPoint("localhost")
            .build().connect(), null);
        connection.getSession().execute("DROP KEYSPACE IF EXISTS " + KEYSPACE_NAME);
        connection.getSession().execute("CREATE KEYSPACE " + KEYSPACE_NAME
            + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        connection.getSession().execute("CREATE TABLE IF NOT EXISTS "
            + TABLE_NAME + "(ID VARCHAR, DATA VARCHAR, PRIMARY KEY (ID))");
    }

    @Before
    public void init() {
        connection.getSession().execute("TRUNCATE " + TABLE_NAME);
    }

    @Test
    public void shouldIterateForward() throws SQLException {
        insertData(11);
        
        CPreparedStatement stmt = new CPreparedStatement(connection);
        ResultSet results = stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
        
        assertTrue(results.isBeforeFirst());
        assertFalse(results.isFirst());
        
        assertTrue(results.next());
        assertTrue(results.isFirst());
        
        String firstValue = results.getString("ID");
        List<String> allValues = readFully(results, rs -> rs.getString("ID"));
        allValues.add(firstValue);

        assertThat(allValues, hasItems("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
        assertThat(allValues, hasSize(11));
        
        assertTrue(results.isLast());
        assertThat(results.getString("ID"), notNullValue());
    }
    

    @Test
    public void shouldIterateBackwards() throws SQLException {
        CPreparedStatement stmt = new CPreparedStatement(connection);
        CResultSet results = new CResultSet(stmt, new CResultSetMetaData(KEYSPACE_NAME, "TESTITERATION", 
            new String[] {"ID", "DATA" }, new DataType[] { DataType.varchar(), DataType.varchar() }), 
                Arrays.asList(new CResultSet.Row(0, new Object[] {"1", "D1"}), 
                    new CResultSet.Row(1, new Object[] {"2", "D2"}), 
                    new CResultSet.Row(1, new Object[] {"3", "D3"})));
        
        readFully(results, rs -> rs.getString("ID"));
        
        assertTrue(results.previous());
        assertThat(results.getString("ID"), equalTo("3"));

        assertTrue(results.previous());
        assertThat(results.getString("ID"), equalTo("2"));

        assertTrue(results.previous());
        assertThat(results.getString("ID"), equalTo("1"));

        results.setFetchDirection(ResultSet.FETCH_REVERSE);
        
        assertTrue(results.previous());
        assertThat(results.getString("ID"), equalTo("1"));

        assertTrue(results.previous());
        assertThat(results.getString("ID"), equalTo("2"));

        assertTrue(results.next());
        assertThat(results.getString("ID"), equalTo("2"));

        assertTrue(results.next());
        assertThat(results.getString("ID"), equalTo("1"));
        
        results.setFetchDirection(ResultSet.FETCH_FORWARD);

        results.absolute(1);
        assertThat(results.getString("ID"), equalTo("1"));

        results.absolute(-1);
        assertThat(results.getString("ID"), equalTo("3"));

        results.relative(-2);
        assertThat(results.getString("ID"), equalTo("1"));
    }
    

    @Test
    public void shouldCountRows() throws SQLException {
        insertData(6);
        
        CPreparedStatement stmt = new CPreparedStatement(connection);
        ResultSet results = stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
        
        List<Integer> indexes = readFully(results, rs -> rs.getRow());
        assertThat(indexes, contains(1, 2, 3, 4, 5, 6));
    }
    
    @Test
    public void shouldIterateStatement() throws SQLException {
        insertData(5);
        
        CPreparedStatement stmt = new CPreparedStatement(connection);
        assertTrue(stmt.execute("SELECT * FROM " + TABLE_NAME));
        
        List<String> allValues = readFully(stmt.getResultSet(), rs -> rs.getString("ID"));
        assertThat(allValues, hasItems("0", "1", "2", "3", "4"));
    }
    
    private <T> List<T> readFully(ResultSet query, CheckedFunction<ResultSet, T> mapper) throws SQLException {
        List<T> values = new ArrayList<>();
        while (query.next()) {
            values.add(mapper.apply(query));
        }
        return values;
    }
    
    private void insertData(int count) {
        IntStream.range(0, count)
            .forEach(i -> connection.getSession()
                .execute("INSERT INTO " + TABLE_NAME + "(ID,DATA) VALUES ('" + i + "', 'D" + i + "')"));
        
    }
    
}
