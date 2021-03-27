/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.connection;

import static com.cassandrajdbc.test.util.CassandraTestConnection.getConnection;
import static com.cassandrajdbc.test.util.ResultSetMatcher.hasResultCount;
import static com.cassandrajdbc.test.util.ResultSetMatcher.hasResultItems;
import static com.cassandrajdbc.test.util.ResultSetMatcher.resultsEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.cassandrajdbc.CassandraURL;
import com.datastax.driver.core.exceptions.InvalidQueryException;

@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({ CassandraUnitTestExecutionListener.class })
@CassandraDataSet(keyspace = "CDatabaseMetaDataTest", value = { "CDatabaseMetaDataTest/Tables.cql" })
@EmbeddedCassandra
public class CDatabaseMetaDataTest {
    
    private static final String KEYSPACE_NAME = "CDatabaseMetaDataTest";
    private static final String TABLE_NAME = KEYSPACE_NAME + ".MetadataTest";
    private static final String FUNC_NAME = KEYSPACE_NAME + ".MetadataFunc";
    private static final String TYPE_NAME = KEYSPACE_NAME + ".MetadataType";
    

    @Test
    public void shouldDescribeSchemas() throws SQLException {
        CDatabaseMetaData meta = getMetadata();
        
        assertThat(meta.getCatalogs(), resultsEqualTo(Arrays.asList(""), rs -> rs.getString("TABLE_CAT")));
        assertThat(meta.getSchemas(), hasResultItems(Collections.singleton("cdatabasemetadatatest"), rs -> rs.getString("TABLE_SCHEM")));
        
        assertThat(meta.getSchemas(null, "CDatabaseMetaDa%"), hasResultItems(Collections.singleton("cdatabasemetadatatest"), rs -> rs.getString("TABLE_SCHEM")));
    }

    @Test
    public void shouldDescribeSchema() throws SQLException {
        CDatabaseMetaData meta = getMetadata();
        
        assertThat(meta.getTableTypes(), hasResultItems(Arrays.asList("TABLE", "SYSTEM TABLE"), rs -> rs.getString("TABLE_TYPE")));
        assertThat(meta.getTables(null, KEYSPACE_NAME, null, new String[]{"VIEW"}), hasResultCount(0));
        assertThat(meta.getTables(null, KEYSPACE_NAME, null, null), resultsEqualTo(Arrays.asList(TABLE_NAME.toLowerCase()), 
            rs -> rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME")));
        assertThat(meta.getTables(null, null, "MetadataTest", null), hasResultItems(Arrays.asList(TABLE_NAME.toLowerCase()), 
            rs -> rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME")));
     
    }

    @Test
    public void shouldDescribeTables() throws SQLException {
        CDatabaseMetaData meta = getMetadata();
        
        assertThat(meta.getTables(null, "CDatabaseMeta%", "Meta%", null), resultsEqualTo(Arrays.asList(TABLE_NAME.toLowerCase()), 
            rs -> rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME")));

        assertThat(meta.getColumns(null, KEYSPACE_NAME, "MetadataTest", null), hasResultItems(Arrays.asList("id", "data"), 
            rs -> rs.getString("COLUMN_NAME")));
        assertThat(meta.getColumns(null, KEYSPACE_NAME, "MetadataTest", "DA%"), resultsEqualTo(Arrays.asList("data"),
            rs -> rs.getString("COLUMN_NAME")));
        assertThat(meta.getPrimaryKeys(null, KEYSPACE_NAME, "MetadataTest"), resultsEqualTo(Arrays.asList("id"), 
            rs -> rs.getString("COLUMN_NAME")));

    }

    @Test
    public void shouldDescribeFunctions() throws SQLException {
        try {
            getConnection().getSession().execute("CREATE FUNCTION IF NOT EXISTS " + FUNC_NAME
                + "(data TEXT, num INT) RETURNS NULL ON NULL INPUT RETURNS TEXT "
                + "LANGUAGE java AS $$ return data.substring(0,num); $$");
        } catch (InvalidQueryException e) {
            return; // disabled in cassandra.yaml
        }
        
        CDatabaseMetaData meta = getMetadata();
        assertThat(meta.getFunctions(null, null, null), resultsEqualTo(Arrays.asList("metadatafunc"), rs -> rs.getString(3)));
        assertThat(meta.getFunctionColumns(null, null, "metadatafunc", null), 
            resultsEqualTo(Arrays.asList("data:VARCHAR", "num:INTEGER", "RESULT:VARCHAR"), 
                rs -> rs.getString("COLUMN_NAME") + ":" + rs.getString("TYPE_NAME")));
    }

    @Test
    public void shouldDescribeTypes() throws SQLException {
        getConnection().getSession().execute("CREATE TYPE IF NOT EXISTS " + TYPE_NAME 
            + " (time TIMESTAMP, data TEXT)");
        
        CDatabaseMetaData meta = getMetadata();
        assertThat(meta.getTypeInfo(), hasResultItems(Arrays.asList("TEXT", "VARCHAR", "UUID", 
            "INT", "BIGINT", "BIGINT", "VARINT", "TINYINT", "SMALLINT", "DATE", "TIMESTAMP", 
            "TIME", "FLOAT", "DECIMAL", "DOUBLE", "BOOLEAN", "BLOB"), rs -> rs.getString("TYPE_NAME")));
        assertThat(meta.getUDTs(null, null, null, null), 
            hasResultItems(Arrays.asList("metadatatype"), rs -> rs.getString("TYPE_NAME")));
        assertThat(meta.getAttributes(null, null, "metadatat%", null), 
            resultsEqualTo(Arrays.asList("TIMESTAMP:time", "VARCHAR:data"), 
                rs -> rs.getString("ATTR_TYPE_NAME") + ":" + rs.getString("ATTR_NAME")));
        
    }

    @Test
    public void shouldDEscribeClientInfoProps() throws SQLException {
        getConnection().getSession().execute("CREATE TYPE IF NOT EXISTS " + TYPE_NAME 
            + " (time TIMESTAMP, data TEXT)");
        
        CDatabaseMetaData meta = getMetadata();
        assertThat(meta.getClientInfoProperties(), hasResultItems(Arrays.asList("username"), 
            rs -> rs.getString("NAME")));
    }

    public CDatabaseMetaData getMetadata() {
        return new CDatabaseMetaData(CassandraURL.create("jdbc:cassandra://localhost:9042").get(), getConnection());
    }

}
