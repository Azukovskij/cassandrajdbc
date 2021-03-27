/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import static com.cassandrajdbc.test.util.ResultSetMatcher.hasResultCount;
import static com.cassandrajdbc.test.util.ResultSetMatcher.hasResultItems;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.ResultSet;
import java.util.Arrays;

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.cassandrajdbc.test.util.CassandraTestConnection;

@RunWith(SpringJUnit4ClassRunner.class)
@TestExecutionListeners({ CassandraUnitTestExecutionListener.class })
@CassandraDataSet(keyspace = "StatementTests", value = { "StatementTests/Tables.cql", "StatementTests/Data.Simple.cql" })
@EmbeddedCassandra
public class SelectStatementTest {
    
    @Test
    public void shouldAllowFiltering() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple where value = 'value-5'");
        assertThat(rs, hasResultItems(Arrays.asList("value-5"), row -> row.getString(1)));;
    }
    
    @Test
    public void shouldAliasColumns() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value as aaa from StatementTests.Simple where value = 'value-4'");
        assertThat(rs, hasResultItems(Arrays.asList("value-4"), row -> row.getString("aaa")));;
    }
    
    @Test
    @Ignore
    public void shouldUnionResults() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("(select value from StatementTests.Simple where value = 'value-6')"
            + " UNION (select value from StatementTests.Simple where value = 'value-7')"
            + " UNION DISTINCT (select value from StatementTests.Simple where value = 'value-8')");
        assertThat(rs, hasResultItems(Arrays.asList("value-6", "value-7", "value-8"), row -> row.getString(1)));
    }
    
    @Test
    @Ignore
    public void shouldIntersectResults() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("(select value from StatementTests.Simple where value = 'value-8')"
            + " INTERSECT (select distinct value from StatementTests.Simple where value = 'value-8')");
        assertThat(rs, hasResultItems(Arrays.asList("value-8"), row -> row.getString(1)));
    }
    
    @Test
    @Ignore
    public void shouldExceptResults() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("(select * from StatementTests.Simple)"
            + " EXCEPT (select value from StatementTests.Simple where value = 'value-100')");
        assertThat(rs, hasResultItems(Arrays.asList("value-8"), row -> row.getString(1)));
    }
    
    @Test
    @Ignore
    public void shoulFilterWildcard() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple where value = 'value-1*'");
        assertThat(rs, hasResultItems(Arrays.asList("value-1", "value-10", "value-100"), row -> row.getString(1)));
    }
    
    @Test
    @Ignore
    public void shouldLimitResults() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple limit 3");
        assertThat(rs, hasResultCount(3));
    }

    @Test
    @Ignore
    public void shouldOffsetResults() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple limit 2 offset 3");
        assertThat(rs, hasResultCount(3));
    }
    
    @Test
    @Ignore
    public void shouldOrderByColumn() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple order by value NULLS LAST");
        assertThat(rs, hasResultItems(Arrays.asList("value-4"), row -> row.getString("aaa")));;
    }
    
    @Test
    @Ignore
    public void shouldListeralValue() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value, 'text-0' as text from StatementTests.Simple where value = 'value-6'");
        assertThat(rs, hasResultItems(Arrays.asList("text-0"), row -> row.getString("text")));
    }
    
    @Test
    @Ignore
    public void shouldSelectDistinctColumns() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select distinct value from StatementTests.Simple");
        assertThat(rs, hasResultCount(10));
    }
    
    @Test
    @Ignore
    public void shouldSelectDistinctOnColumns() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select distinct on (value) * from StatementTests.Simple");
        assertThat(rs, hasResultCount(10));
    }
    
    
    @Test
    @Ignore
    public void shoudParseWhereInClauses() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select * from StatementTests.Simple where value in ('value1', 'value-3')");
        assertThat(rs, hasResultItems(Arrays.asList("value-1", "value-3"), row -> row.getString(2)));
    }


//    [ WITH [ RECURSIVE ] with_query [, ...] ]
//        SELECT 
//            [ FROM from_item [, ...] ]
//            [ GROUP BY expression [, ...] ]
//            [ HAVING condition [, ...] ]
//
//        where from_item can be one of:
//
//            [ ONLY ] table_name [ * ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
//            ( select ) [ AS ] alias [ ( column_alias [, ...] ) ]
//            with_query_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
//            function_name ( [ argument [, ...] ] ) [ AS ] alias [ ( column_alias [, ...] | column_definition [, ...] ) ]
//            function_name ( [ argument [, ...] ] ) AS ( column_definition [, ...] )
//            from_item [ NATURAL ] join_type from_item [ ON join_condition | USING ( join_column [, ...] ) ]
//
//        and with_query is:
//
//            with_query_name [ ( column_name [, ...] ) ] AS ( select | insert | update | delete )
//
//        TABLE [ ONLY ] table_name [ * ]
    
    
}
