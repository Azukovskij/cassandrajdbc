/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.sql.ResultSet;
import java.util.List;
import java.util.UUID;

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.cassandrajdbc.test.util.CassandraTestConnection;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({ CassandraUnitTestExecutionListener.class })
@CassandraDataSet(keyspace = "InsertStatementTest", value = { "StatementTests/Tables.cql" })
@EmbeddedCassandra
public class InsertStatementTest {
    
    @Test
    public void shouldInsertCastUuidToString() throws Exception {
        Session session = EmbeddedCassandraServerHelper.getSession();
        
        UUID id = UUID.randomUUID();
        CassandraTestConnection.executeSql("insert into InsertStatementTest.Simple (id, \"value\") values ('" + id + "', 'value');");
        
        List<Row> rows = session.execute("select * from InsertStatementTest.Simple where id=" + id).all();
        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).getString("value"), equalTo("value"));
    }
    
    @Test
    public void shouldInsertPreparedStatement() throws Exception {
        Session session = EmbeddedCassandraServerHelper.getSession();
        
        UUID id = UUID.randomUUID();
        CassandraTestConnection.executeSql("insert into InsertStatementTest.Simple (id, \"value\")"
            + " values (?, ?);", id, "some-value");
        
        List<Row> rows = session.execute("select * from InsertStatementTest.Simple where id=" + id).all();
        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).getString("value"), equalTo("some-value"));
    }

    @Test
    public void shouldInsertFromSelect() throws Exception {
        UUID id = UUID.randomUUID();
        CassandraTestConnection.executeSql("insert into InsertStatementTest.Simple (id, \"value\")"
            + " values ('" + id + "', 'value');");
        CassandraTestConnection.executeSql("insert into InsertStatementTest.AllTypes (id, \"varchar\")"
            + " (select id, \"value\" as \"varchar\" from InsertStatementTest.Simple where id='" + id + "')");
    
    }

    @Test
    @Ignore
    public void shouldInsertReturning() throws Exception {
        UUID id = UUID.randomUUID();
        ResultSet rs = CassandraTestConnection.executeSqlQuery("insert into InsertStatementTest.Simple (id, \"value\")"
            + " values ('" + id + "', 'value') RETURNING *;");
    
    }
    
//    [ WITH [ RECURSIVE ] with_query [, ...] ]
//        INSERT INTO table_name [ AS alias ] [ ( column_name [, ...] ) ]
//            { DEFAULT VALUES }
//            [ ON CONFLICT [ conflict_target ] conflict_action ]
//            [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ]

}
