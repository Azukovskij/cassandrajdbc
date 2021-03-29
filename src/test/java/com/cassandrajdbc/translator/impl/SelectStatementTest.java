/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import static com.cassandrajdbc.test.util.ResultSetMatcher.hasResultCount;
import static com.cassandrajdbc.test.util.ResultSetMatcher.resultsEqualTo;
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
        assertThat(rs, resultsEqualTo(Arrays.asList("value-5"), row -> row.getString(1)));;
    }
    
    @Test
    public void shouldAliasColumns() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value as aaa from StatementTests.Simple where value = 'value-4'");
        assertThat(rs, resultsEqualTo(Arrays.asList("value-4"), row -> row.getString("aaa")));;
    }
    
    @Test
    public void shouldLimitResults() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple limit 3");
        assertThat(rs, resultsEqualTo(Arrays.asList("value-8", "value-0", "value-10"), row -> row.getString(1)));
    }

    @Test
    public void shouldOffsetResults() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple limit 2 offset 1");
        assertThat(rs, resultsEqualTo(Arrays.asList("value-0", "value-10"), row -> row.getString(1)));
    }
    
    @Test
    public void shouldUnionResults() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("(select value from StatementTests.Simple where value = 'value-6')"
            + " UNION (select value from StatementTests.Simple where value = 'value-7')"
            + " UNION (select value from StatementTests.Simple where value = 'value-8')");
        assertThat(rs, resultsEqualTo(Arrays.asList("value-6", "value-7", "value-8", "value-8"), row -> row.getString(1)));
    }
    
    @Test
    public void shouldIntersectResults() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("(select value from StatementTests.Simple where value = 'value-8')"
            + " INTERSECT (select value from StatementTests.Simple where value = 'value-8')");
        assertThat(rs, resultsEqualTo(Arrays.asList("value-8", "value-8"), row -> row.getString(1)));
    }
    

    @Test
    public void shouldExceptResults() throws Exception {
        ResultSet rs1 = CassandraTestConnection.executeSqlQuery("(select * from StatementTests.Simple)");
        ResultSet rs2 = CassandraTestConnection.executeSqlQuery("(select * from StatementTests.Simple)"
            + " EXCEPT (select * from StatementTests.Simple where value = 'value-100')");

        assertThat(rs1, hasResultCount(13));
        assertThat(rs2, hasResultCount(12));
    }
    
    @Test
    public void shouldAllowCount() throws Exception {
        ResultSet rs1 = CassandraTestConnection.executeSqlQuery("select count(*) from StatementTests.Simple where value = 'value-NA'");
        ResultSet rs2 = CassandraTestConnection.executeSqlQuery("select count(id) as BLA from StatementTests.Simple where value = 'value-6'");
        assertThat(rs1, resultsEqualTo(Arrays.asList(0L), row -> row.getLong(1)));
        assertThat(rs2, resultsEqualTo(Arrays.asList(1L), row -> row.getLong("BLA")));
    }
    
    @Test
    public void shoulFilterWildcard() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple where value like 'value-1%'");
        assertThat(rs, hasResultCount(3));
    }
    
    @Test
    public void shouldSelectDistinctColumns() throws Exception {
        ResultSet rs1 = CassandraTestConnection.executeSqlQuery("select distinct value from StatementTests.Simple");
        ResultSet rs2 = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple");
        assertThat(rs1, hasResultCount(12));
        assertThat(rs2, hasResultCount(13));
    }
    
    @Test
    public void shouldSelectDistinctOnColumns() throws Exception {
        ResultSet rs1 = CassandraTestConnection.executeSqlQuery("select distinct on (value) * from StatementTests.Simple");
        ResultSet rs2 = CassandraTestConnection.executeSqlQuery("select distinct on (value) value as aaa from StatementTests.Simple");
        assertThat(rs1, hasResultCount(12));
        assertThat(rs2, hasResultCount(12));
    }
    
    @Test
    public void shouldOrderByColumn() throws Exception {
        ResultSet rs1 = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple order by value NULLS LAST LIMIT 3");
        ResultSet rs2 = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple order by value DESC");
        assertThat(rs1, resultsEqualTo(Arrays.asList("value-0", "value-1", "value-10"), row -> row.getString(1)));
        assertThat(rs2, resultsEqualTo(Arrays.asList("value-9", "value-8", "value-8", "value-7", "value-6", "value-5", "value-4", 
            "value-3", "value-2", "value-100", "value-10", "value-1", "value-0"), row -> row.getString(1)));
    }
    
    @Test
    @Ignore
    public void shouldReturnListeralValue() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value, 'text-0' as text from StatementTests.Simple where value = 'value-6'");
        assertThat(rs, resultsEqualTo(Arrays.asList("text-0"), row -> row.getString("text")));
    }
    
    @Test
    @Ignore
    public void shoulFilterNotWildcard() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple where value not like 'value-1%'");
        assertThat(rs, hasResultCount(10));
    }
    
    
    @Test
    @Ignore
    public void shoudParseWhereInClauses() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select * from StatementTests.Simple where value in ('value1', 'value-3')");
        assertThat(rs, resultsEqualTo(Arrays.asList("value-1", "value-3"), row -> row.getString(2)));
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
