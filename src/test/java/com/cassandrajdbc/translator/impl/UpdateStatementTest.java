/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.util.List;
import java.util.UUID;

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.cassandrajdbc.test.util.CassandraTestConnection;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({ CassandraUnitTestExecutionListener.class })
@CassandraDataSet(keyspace = "UpdateStatementTest", value = { "StatementTests/Tables.cql", "StatementTests/Data.Simple.cql", "StatementTests/Data.Joined.cql"})
@EmbeddedCassandra
public class UpdateStatementTest {

    @Test
    public void shouldUpdateValues() throws Exception {
        Session session = EmbeddedCassandraServerHelper.getSession();
        
        UUID id = UUID.fromString("f63da0c8-0ca3-4e28-9383-32a54712531f");
        CassandraTestConnection.executeSql("update UpdateStatementTest.Simple set \"value\"='updated-3' where id=" + id);
        
        List<Row> rows = session.execute("select * from UpdateStatementTest.Simple where id=" + id).all();
        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).getString("value"), equalTo("updated-3"));
    }
    
    @Test
    public void shouldUpdatePreparedStatement() throws Exception {
        Session session = EmbeddedCassandraServerHelper.getSession();
        
        UUID id = UUID.fromString("6ca9741c-7c55-41cc-8419-65949f92d62b");
        CassandraTestConnection.executeSql("update UpdateStatementTest.Simple "
            + " set \"value\"=? where id=?;", "updated-6", id);
        
        List<Row> rows = session.execute("select * from UpdateStatementTest.Simple where id=" + id).all();
        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).getString("value"), equalTo("updated-6"));
    }

    @Test
    public void shouldUpdateFromSelect() throws Exception {
        Session session = EmbeddedCassandraServerHelper.getSession();
        
        CassandraTestConnection.executeSql("update UpdateStatementTest.Simple set \"value\"='updated-1'"
            + " where id in (select id from UpdateStatementTest.Simple where value='value-1')");
        
        List<Row> rows = session.execute("select * from UpdateStatementTest.Simple where id=317476b9-391c-4ae4-992f-1f398155c6e8").all();
        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).getString("value"), equalTo("updated-1"));
    }
    
//    [ WITH [ RECURSIVE ] with_query [, ...] ]
//        UPDATE [ ONLY ] table [ * ] [ [ AS ] alias ]
//            SET { column = { expression | DEFAULT } |
//                  ( column [, ...] ) = ( { expression | DEFAULT } [, ...] ) } [, ...]
//            [ FROM from_list ]
//            [ WHERE condition | WHERE CURRENT OF cursor_name ]
//            [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ]
    

}
