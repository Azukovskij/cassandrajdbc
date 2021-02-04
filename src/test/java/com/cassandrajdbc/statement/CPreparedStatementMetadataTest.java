/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.statement;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cassandrajdbc.connection.CassandraConnection;
import com.datastax.driver.core.Cluster;

public class CPreparedStatementMetadataTest {
    
    private static final String KEYSPACE_NAME = "CPreparedStatementTests";
    private static final String TABLE_NAME = KEYSPACE_NAME + ".MetadataTest";
    
    private static CassandraConnection connection;
    
    @BeforeClass
    public static void connect() {
        connection = new CassandraConnection(Cluster.builder()
            .addContactPoint("localhost")
            .build().connect(), null);
        connection.getSession().execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE_NAME
            + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        connection.getSession().execute("DROP TABLE IF EXISTS " + TABLE_NAME);
        connection.getSession().execute("CREATE TABLE " + TABLE_NAME 
            + "(ID VARCHAR, DATA INT, PRIMARY KEY (ID))");
    }

    @Before
    public void init() {
        connection.getSession().execute("TRUNCATE " + TABLE_NAME);
    }
    
    @Test
    public void shoulProvideWildcardMetadata() throws SQLException {
        CPreparedStatement stmt = new CPreparedStatement(connection);
        ResultSetMetaData meta = stmt.executeQuery("SELECT * FROM " + TABLE_NAME).getMetaData();
        
        assertThat(meta.getColumnCount(), equalTo(2));
        assertThat(meta.getSchemaName(1), equalTo("cpreparedstatementtests"));
        assertThat(meta.getTableName(1), equalTo("metadatatest"));

        assertThat(meta.getColumnName(1), equalTo("id"));
        assertThat(meta.getColumnLabel(1), equalTo("id"));
        assertThat(meta.getColumnClassName(1), equalTo(String.class.getName()));
        assertThat(meta.getColumnTypeName(1), equalTo(JDBCType.VARCHAR.getName()));
        assertThat(meta.getColumnType(1), equalTo(JDBCType.VARCHAR.getVendorTypeNumber()));
        assertThat(meta.isNullable(1), equalTo(0));
        assertTrue(meta.isReadOnly(1));
        assertFalse(meta.isWritable(1));

        assertThat(meta.getColumnName(2), equalTo("data"));
        assertThat(meta.getColumnLabel(2), equalTo("data"));
        assertThat(meta.getColumnClassName(2), equalTo(Integer.class.getName()));
        assertThat(meta.getColumnTypeName(2), equalTo(JDBCType.INTEGER.getName()));
        assertThat(meta.getColumnType(2), equalTo(JDBCType.INTEGER.getVendorTypeNumber()));
        assertThat(meta.isNullable(2), equalTo(1));
        assertFalse(meta.isReadOnly(2));
        assertTrue(meta.isWritable(2));
    }
    
    @Test
    public void shouldProideColumnLabels() throws SQLException {
        CPreparedStatement stmt = new CPreparedStatement(connection);
        ResultSetMetaData meta = stmt.executeQuery("SELECT DATA as d, ID as i FROM " + TABLE_NAME).getMetaData();
       
        assertThat(meta.getColumnCount(), equalTo(2));
        assertThat(meta.getSchemaName(1), equalTo("cpreparedstatementtests"));
        assertThat(meta.getTableName(1), equalTo("metadatatest"));

        assertThat(meta.getColumnName(1), equalTo("data"));
        assertThat(meta.getColumnLabel(1), equalTo("d"));

        assertThat(meta.getColumnName(2), equalTo("id"));
        assertThat(meta.getColumnLabel(2), equalTo("i"));
    }
    
    @Test
    public void shouldPrebuildMetadata() throws SQLException {
        CPreparedStatement stmt = new CPreparedStatement(connection, "SELECT * FROM " + TABLE_NAME + " WHERE ID=?");
        ResultSetMetaData meta = stmt.getMetaData();
        
        assertThat(meta.getColumnCount(), equalTo(2));
        assertThat(meta.getSchemaName(1), equalTo("cpreparedstatementtests"));
        assertThat(meta.getTableName(1), equalTo("metadatatest"));

        assertThat(meta.getColumnName(1), equalTo("id"));
        assertThat(meta.getColumnName(2), equalTo("data"));
    }
    
    

}
