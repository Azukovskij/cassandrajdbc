/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

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
@CassandraDataSet(keyspace = "DeleteStatementTest", value = { "StatementTests/Tables.cql", "StatementTests/Data.Simple.cql", "StatementTests/Data.Joined.cql"})
@EmbeddedCassandra
public class DeleteStatementTest {

    @Test
    public void shouldDeleteValues() throws Exception {
        Session session = EmbeddedCassandraServerHelper.getSession();
        
        UUID id = UUID.fromString("f63da0c8-0ca3-4e28-9383-32a54712531f");
        CassandraTestConnection.executeSql("delete from DeleteStatementTest.Simple where id=" + id);
        
        assertThat(session.execute("select * from DeleteStatementTest.Simple where id=" + id).all(), empty());
    }
    
    @Test
    @Ignore
    public void shouldDeletePreparedStatement() throws Exception {
        Session session = EmbeddedCassandraServerHelper.getSession();
        
        CassandraTestConnection.executeSql("delete from DeleteStatementTest.Simple "
            + " where \"value\"=?", "value-6");
        
        assertThat(session.execute("select * from DeleteStatementTest.Simple where \"value\"='value-6'").all(), empty());
    }

    @Test
    public void shouldDeleteFromSelect() throws Exception {
        Session session = EmbeddedCassandraServerHelper.getSession();
        
        CassandraTestConnection.executeSql("delete from DeleteStatementTest.Simple"
            + " where id in (select id from DeleteStatementTest.Simple where \"value\"='value-1')");
        
        assertThat(session.execute("select * from DeleteStatementTest.Simple where \"value\"='value-1'").all(), empty());
    }
    
//    [ WITH [ RECURSIVE ] with_query [, ...] ]
//        DELETE FROM [ ONLY ] table_name [ * ] [ [ AS ] alias ]
//            [ USING from_item [, ...] ]
//            [ WHERE condition | WHERE CURRENT OF cursor_name ]
//            [ RETURNING * | output_expression [ [ AS ] output_name ] [, ...] ]
    

}
