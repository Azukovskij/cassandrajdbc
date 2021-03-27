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

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.cassandrajdbc.test.util.CassandraTestConnection;


@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({ CassandraUnitTestExecutionListener.class })
@CassandraDataSet(keyspace = "CPreparedStatementTests", value = { "CPreparedStatementTests/Tables.cql" })
@EmbeddedCassandra
public class CPreparedStatementMetadataTest {
    
    private static final String KEYSPACE_NAME = "CPreparedStatementTests";
    private static final String TABLE_NAME = KEYSPACE_NAME + ".MetadataTest";
    
    @Test
    public void shoulProvideWildcardMetadata() throws SQLException {
        CPreparedStatement stmt = new CPreparedStatement(CassandraTestConnection.getConnection());
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
        CPreparedStatement stmt = new CPreparedStatement(CassandraTestConnection.getConnection());
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
        CPreparedStatement stmt = new CPreparedStatement(CassandraTestConnection.getConnection(), "SELECT * FROM " + TABLE_NAME + " WHERE ID=?");
        ResultSetMetaData meta = stmt.getMetaData();
        
        assertThat(meta.getColumnCount(), equalTo(2));
        assertThat(meta.getSchemaName(1), equalTo("cpreparedstatementtests"));
        assertThat(meta.getTableName(1), equalTo("metadatatest"));

        assertThat(meta.getColumnName(1), equalTo("id"));
        assertThat(meta.getColumnName(2), equalTo("data"));
    }
    
    

}
