/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.statement;

import static com.cassandrajdbc.test.util.BinaryMatcher.readerEqualTo;
import static com.cassandrajdbc.test.util.BinaryMatcher.streamEqualTo;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.cassandrajdbc.connection.CassandraConnection;
import com.cassandrajdbc.test.util.MethodPointer;
import com.cassandrajdbc.types.SerialNClob;
import com.datastax.driver.core.Cluster;

@RunWith(Parameterized.class)
public class CPreparedStatementParamsTest {
    
    private static final String KEYSPACE_NAME = "CPreparedStatementParamsTest";
    
    private static CassandraConnection connection;
    
    private final MethodPointer setter;
    private final MethodPointer getter;
    private final Matcher<Object> expectedValue;
    
    private final String dataType;
    private final String tableName;
    private final String rowId;

    private final CPreparedStatement testObject;
    
    @BeforeClass
    public static void connect() {
        connection = new CassandraConnection(Cluster.builder()
            .addContactPoint("localhost")
            .build().connect(), null);
        connection.getSession().execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME
            + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    }
    
    @Parameterized.Parameters(name = "{0} = {3}")
    public static List<Object[]> data() throws Exception {
        Calendar neg6Tz = Calendar.getInstance(TimeZone.getTimeZone("GMT-6"));
        Calendar pos1Tz = Calendar.getInstance(TimeZone.getTimeZone("GMT+1"));
        return Arrays.asList(new Object[][] {
            { "VARCHAR", new MethodPointer("setString", 1, "TEST"), new MethodPointer("getString", 2), equalTo("TEST") },
            { "TEXT", new MethodPointer("setString", 1, "TEST"), new MethodPointer("getString", 2), equalTo("TEST") },
            { "TEXT", new MethodPointer("setNString", 1, "TEST"), new MethodPointer("getNString", 2), equalTo("TEST") },
            { "TEXT", new MethodPointer("setNull", 1, Types.VARCHAR), new MethodPointer("getString", 2), nullValue() },
            { "BOOLEAN", new MethodPointer("setBoolean", 1, true), new MethodPointer("getBoolean", 2), equalTo(true) },
            { "INT", new MethodPointer("setInt", 1, 66), new MethodPointer("getInt", 2), equalTo(66) },
            { "BIGINT", new MethodPointer("setLong", 1, 66L), new MethodPointer("getLong", 2), equalTo(66L) },
            { "FLOAT", new MethodPointer("setFloat", 1, 66.0F), new MethodPointer("getFloat", 2), equalTo(66.0F) },
            { "DOUBLE", new MethodPointer("setDouble", 1, 66.0D), new MethodPointer("getDouble", 2), equalTo(66.0D) },
            { "SMALLINT", new MethodPointer("setShort", 1, (short)32), new MethodPointer("getShort", 2), equalTo((short)32) },
            { "TINYINT", new MethodPointer("setByte", 1, (byte)32), new MethodPointer("getByte", 2), equalTo((byte)32) },
            { "DECIMAL", new MethodPointer("setBigDecimal", 1, BigDecimal.valueOf(64)), new MethodPointer("getBigDecimal", 2), equalTo(BigDecimal.valueOf(64)) },
            { "VARCHAR", new MethodPointer("setURL", 1, new URL("http://bla.com")), new MethodPointer("getURL", 2), equalTo(new URL("http://bla.com")) },

            { "DATE", new MethodPointer("setDate", 1, new Date(98, 02, 03)), new MethodPointer("getDate", 2), equalTo(new Date(98, 02, 03)) },
            { "TIME", new MethodPointer("setTime", 1, new Time(20, 01, 02)), new MethodPointer("getTime", 2), equalTo(new Time(20, 01, 02)) },
            { "TIMESTAMP", new MethodPointer("setTimestamp", 1, new Timestamp(125)), new MethodPointer("getTimestamp", 2), equalTo(new Timestamp(125)) },
         
            { "DATE", new MethodPointer("setDate", 1, new Date(98, 02, 04), neg6Tz), new MethodPointer("getDate", 2, pos1Tz), equalTo(new Date(98, 02, 04)) },
            { "TIME", new MethodPointer("setTime", 1, new Time(20, 01, 02), neg6Tz), new MethodPointer("getTime", 2, pos1Tz), equalTo(new Time(03, 01, 02)) },
            { "TIMESTAMP", new MethodPointer("setTimestamp", 1, new Timestamp(125), neg6Tz), new MethodPointer("getTimestamp", 2, pos1Tz), equalTo(new Timestamp(125 + TimeUnit.HOURS.toMillis(7))) },
            
            { "BLOB", new MethodPointer("setBytes", 1, new byte[] {1,2,3}), new MethodPointer("getBytes", 2), equalTo(new byte[] {1,2,3}) },
            { "BLOB", new MethodPointer("setBinaryStream", 1, new ByteArrayInputStream(new byte[] {1,2,4})), new MethodPointer("getBinaryStream", 2), streamEqualTo(new byte[] {1,2,4}) },
            { "BLOB", new MethodPointer("setBlob", 1, new SerialBlob(new byte[] {1,2,5})), new MethodPointer("getBlob", 2), equalTo(new SerialBlob(new byte[] {1,2,5})) },

            { "TEXT", new MethodPointer("setClob", 1, new SerialClob(new char[] {'a','b','c'})), new MethodPointer("getClob", 2), equalTo(new SerialClob(new char[] {'a','b','c'})) },
            { "TEXT", new MethodPointer("setNClob", 1, new SerialNClob(new char[] {'a','b','d'})), new MethodPointer("getNClob", 2), equalTo(new SerialClob(new char[] {'a','b','d'})) },
            { "TEXT", new MethodPointer("setCharacterStream", 1, new StringReader("abe")), new MethodPointer("getCharacterStream", 2), readerEqualTo("abe".getBytes()) },
            { "TEXT", new MethodPointer("setNCharacterStream", 1, new StringReader("abd")), new MethodPointer("getNCharacterStream", 2), readerEqualTo("abd".getBytes()) },

            { "INT", new MethodPointer("setObject", 1, 66), new MethodPointer("getObject", 2), equalTo(66) },
            { "BIGINT", new MethodPointer("setObject", 1, 66L, JDBCType.BIGINT), new MethodPointer("getObject", 2), equalTo(66L) },
            { "FLOAT", new MethodPointer("setObject", 1, 66.0F, JDBCType.FLOAT, 1), new MethodPointer("getObject", 2), equalTo(66.0F) }
        });
    }
    
    public CPreparedStatementParamsTest(String dataType, MethodPointer setter, MethodPointer getter, Matcher<Object> expectedValue) {
        this.dataType = dataType;
        this.setter = setter;
        this.getter = getter;
        this.expectedValue = expectedValue;
        this.tableName = "CPreparedStatementParamsTest.ParamsTest" + dataType + getter.getMethodName().substring(3).toUpperCase();
        this.testObject = new CPreparedStatement(connection, null, 1);
        this.rowId = UUID.randomUUID().toString();
    }
    
    @Test
    public void shouldPrepareParamByIndex() throws SQLException {
        connection.getSession().execute("CREATE TABLE IF NOT EXISTS " + tableName + " (ID VARCHAR, DATA " + dataType + ", PRIMARY KEY (ID))");
        
        setter.invoke(testObject);
        
        assertThat(testObject.executeUpdate("INSERT INTO " + tableName + "(ID,DATA) VALUES ('" + rowId + "', ?)"), equalTo(0));
        
        ResultSet rs = testObject.executeQuery("SELECT * FROM " + tableName + " WHERE ID='" + rowId + "'");
        rs.next();
        assertThat(getter.invoke(rs), expectedValue);
    }
    
    @Test
    public void shouldPrepareParamByname() throws SQLException {
        connection.getSession().execute("CREATE TABLE IF NOT EXISTS " + tableName + " (ID VARCHAR, DATA " + dataType + ", PRIMARY KEY (ID))");
        
        setter.invoke(testObject);
        
        assertThat(testObject.executeUpdate("INSERT INTO " + tableName + "(ID,DATA) VALUES ('" + rowId + "', ?)"), equalTo(0));
        
        ResultSet rs = testObject.executeQuery("SELECT * FROM " + tableName + " WHERE ID='" + rowId + "'");
        rs.next();
        getter.getArgs()[0] = "DATA";
        assertThat(getter.invoke(rs), expectedValue);
    }
    
    @Test
    public void shouldDecsribeParamMetadata() throws SQLException {
        setter.invoke(testObject);
        
        ParameterMetaData meta = testObject.getParameterMetaData();
        assertThat(meta.getParameterCount(), equalTo(1));
        
    }
    

}
