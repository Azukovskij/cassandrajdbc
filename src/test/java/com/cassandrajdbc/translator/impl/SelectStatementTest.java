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
@CassandraDataSet(keyspace = "StatementTests", value = { "StatementTests/Tables.cql", "StatementTests/Data.Simple.cql", "StatementTests/Data.Joined.cql"})
@EmbeddedCassandra
public class SelectStatementTest {
    
    @Test
    public void shouldSelectTableColumns() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select t.* from StatementTests.Simple as t where value = 'value-3'");
        assertThat(rs, resultsEqualTo(Arrays.asList("value-3"), row -> row.getString(2)));;
    }
    
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

        assertThat(rs1, hasResultCount(14));
        assertThat(rs2, hasResultCount(13));
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
        assertThat(rs, hasResultCount(4));
    }
    
    @Test
    public void shouldSelectDistinctColumns() throws Exception {
        ResultSet rs1 = CassandraTestConnection.executeSqlQuery("select distinct value from StatementTests.Simple");
        ResultSet rs2 = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple");
        assertThat(rs1, hasResultCount(12));
        assertThat(rs2, hasResultCount(14));
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
        assertThat(rs1, resultsEqualTo(Arrays.asList("value-0", "value-1", "value-1"), row -> row.getString(1)));
        assertThat(rs2, resultsEqualTo(Arrays.asList("value-9", "value-8", "value-8", "value-7", "value-6", "value-5", "value-4", 
            "value-3", "value-2", "value-100", "value-10", "value-1", "value-1", "value-0"), row -> row.getString(1)));
    }
    
    @Test
    public void shouldReturnListeralValue() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value, 'text-0' as text, id from StatementTests.Simple where value = 'value-6'");
        assertThat(rs, resultsEqualTo(Arrays.asList("value-6|text-0|f4ca6543-92b4-4c7b-983b-5baca67b6cd0"), 
            row -> row.getString(1) + "|" + row.getString("text") + "|" + row.getString(3)));
    }
    
    @Test
    public void shouldSubSelectRows() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple where id in "
            + "(select simple_fk from StatementTests.Joined)");
        assertThat(rs, resultsEqualTo(Arrays.asList("value-2", "value-1", "value-0"), row -> row.getString(1)));
    }
    
    @Test
    public void shouldInnerJoinRows() throws Exception {
        ResultSet rs1 = CassandraTestConnection.executeSqlQuery("select j.\"value\", s.\"value\" from StatementTests.Simple as s "
            + " INNER JOIN StatementTests.Joined as j ON (s.id = j.simple_fk) where s.\"value\"='value-1'");
        ResultSet rs2 = CassandraTestConnection.executeSqlQuery("select j.\"value\", s.\"value\" from StatementTests.Joined as s "
            + " INNER JOIN StatementTests.Simple as j ON (j.id = s.simple_fk) where j.\"value\"='value-1'");
        assertThat(rs1, resultsEqualTo(Arrays.asList("value-1|value-1.3", "value-1|value-1.2", "value-1|value-1.1"), 
            row -> row.getString(2)  + "|" + row.getString(1)));
        assertThat(rs2, resultsEqualTo(Arrays.asList("value-1.3|value-1", "value-1.2|value-1", "value-1.1|value-1"), 
            row -> row.getString(2)  + "|" + row.getString(1)));
    }
    
    @Test
    public void shouldLeftJoinRows() throws Exception {
        ResultSet rs1 = CassandraTestConnection.executeSqlQuery("select j.\"value\", s.\"value\" from StatementTests.Simple as s "
            + " LEFT JOIN StatementTests.Joined as j ON (s.id = j.simple_fk) where s.\"value\"='value-1'");
        ResultSet rs2 = CassandraTestConnection.executeSqlQuery("select j.\"value\", s.\"value\" from StatementTests.Joined as s "
            + " LEFT JOIN StatementTests.Simple as j ON (j.id = s.simple_fk) where j.\"value\" = 'value-1'"); 
        assertThat(rs1, resultsEqualTo(Arrays.asList("value-1|null", "value-1|value-1.3", "value-1|value-1.2", "value-1|value-1.1"), 
            row -> row.getString(2)  + "|" + row.getString(1)));
        assertThat(rs2, resultsEqualTo(Arrays.asList("value-1.3|value-1", "value-1.2|value-1", "value-1.1|value-1"), 
            row -> row.getString(2)  + "|" + row.getString(1)));
    }
    
    @Test
    @Ignore  // not implemented yet
    public void shouldRightJoinRows() throws Exception {
        ResultSet rs1 = CassandraTestConnection.executeSqlQuery("select s.\"value\" as Simple, j.\"value\" Joined, jj.\"value\" as Joined2 from StatementTests.Simple as s "
            + "RIGHT JOIN StatementTests.Joined as j ON (s.id = j.simple_fk) "
            + "RIGHT JOIN StatementTests.Joined2 as jj ON (j.simple_fk = jj.simple_fk)");
        ResultSet rs2 = CassandraTestConnection.executeSqlQuery("select s.\"value\" as Simple, j.\"value\" Joined, jj.\"value\" as Joined2 from StatementTests.Simple as s "
            + "RIGHT JOIN StatementTests.Joined2 as jj ON (s.id = jj.simple_fk) "
            + "RIGHT JOIN StatementTests.Joined as j ON (s.id = j.simple_fk)");
        assertThat(rs1, resultsEqualTo(Arrays.asList(
            "value-1|value-1.3|value-1.1", "value-1|value-1.2|value-1.1", "value-1|value-1.1|value-1.1",
            "value-1|value-1.3|value-1.2", "value-1|value-1.2|value-1.2", "value-1|value-1.1|value-1.2",
            "value-1|value-1.3|value-1.3", "value-1|value-1.2|value-1.3", "value-1|value-1.1|value-1.3",
            "null|null|value-0.0"), 
            row -> row.getString(1)  + "|" + row.getString(2) + "|" + row.getString(3)));
        assertThat(rs2, resultsEqualTo(Arrays.asList(
            "null|value-0.1|null", "null|value-0.2|null", "null|value-0.3|null",
            "value-1|value-1.1|value-1.3", "value-1|value-1.1|value-1.2", "value-1|value-1.1|value-1.1",
            "value-1|value-1.2|value-1.3", "value-1|value-1.2|value-1.2", "value-1|value-1.2|value-1.1",
            "value-1|value-1.3|value-1.3", "value-1|value-1.3|value-1.2", "value-1|value-1.3|value-1.1",
            "null|value-2.1|null", "null|value-2.2|null", "null|value-2.3|null", "null|value-00.1|null",
            "null|value-00.2|null", "null|value-00.3|null"), 
            row -> row.getString(1)  + "|" + row.getString(2) + "|" + row.getString(3)));
        
//        ResultSet rs2 = CassandraTestConnection.executeSqlQuery("select s.\"value\" as Simple, j1.\"value\" Joined, j2.\"value\" as Joined2 from StatementTests.Simple as s "
//            + "RIGHT JOIN StatementTests.Joined2 as j2 ON (j4.simple_fk = j2.simple_fk AND j3.simple_fk = j2.simple_fk) "
//            + "RIGHT JOIN StatementTests.Joined2 as j3 ON (j5.simple_fk = j3.simple_fk) "
//            + "RIGHT JOIN StatementTests.Joined2 as j4 ON (j3.simple_fk = j4.simple_fk) "
//            + "RIGHT JOIN StatementTests.Joined2 as j5 ON (s.id = j5.simple_fk) "
//            + "RIGHT JOIN StatementTests.Joined as j1 ON (j1.simple_fk = j2.simple_fk)");
//      select * from j1 
//      select * from j2 where fk=:j1.fk
//          select * from j4 where fk=:j2.fk
//              select * from j3 where (fk=:j4.fk AND fk=:j2.fk)
//                  select * from j5 where fk=:j3.fk
//                      select * from s where id=:j5.fk
//      OR
//      select * from j1 
//      select * from j2 where fk=:j1.fk
//          select * from j3 where fk=:j2.fk
//              select * from j4 where (fk=:j3.fk AND fk=:j2.fk)
//                  select * from j5 where fk=:j3.fk
//                      select * from s where id=:j5.fk
        
        
//      ResultSet rs2 = CassandraTestConnection.executeSqlQuery("select s.\"value\" as Simple, j1.\"value\" Joined, j2.\"value\" as Joined2 from StatementTests.Simple as s "
//          + "RIGHT JOIN StatementTests.Joined2 as j2 ON (j4.simple_fk = j2.simple_fk) "
//          + "RIGHT JOIN StatementTests.Joined2 as j6 ON (j1.simple_fk = j6.simple_fk) "
//          + "RIGHT JOIN StatementTests.Joined2 as j3 ON (j5.simple_fk = j3.simple_fk) "
//          + "RIGHT JOIN StatementTests.Joined2 as j4 ON (j3.simple_fk = j4.simple_fk) "
//          + "RIGHT JOIN StatementTests.Joined2 as j5 ON (s.id = j5.simple_fk) "
//          + "RIGHT JOIN StatementTests.Joined as j1 ON (j1.simple_fk = j2.simple_fk)");
        
//        select * from Joined j1 
//        select * from Joined2 j2 where fk=:j1.fk
//            select * from Joined3 j3 where fk=:j2.fk
//                select * from j4 where (fk=:j3.fk AND fk=:j2.fk)
//                    select * from j5 where fk=:j3.fk
//                        select * from j6 where fk=:j1.fk 
//                            select * from Simple s where id=:j5.fk
        
        
    }
    
    @Test
    @Ignore  // not implemented yet
    public void shouldFullJoinRows() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select s.*, j.\"value\" from StatementTests.Simple as s "
            + " FULL JOIN StatementTests.Joined as j ON (s.id = j.simple.id) where s.\"value\"='value-1'");
        assertThat(rs, resultsEqualTo(Arrays.asList("value-0", "value-1", "value-2"), row -> row.getString(1)));
    }

    @Test
    @Ignore  // not implemented yet
    public void shouldCombineJoins() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select s.\"value\" as Simple, j.\"value\" Joined, jj.\"value\" as Joined2 from StatementTests.Simple as s "
            + "INNER JOIN StatementTests.Joined as j ON (s.id = j.simple_fk) "
            + "LEFT JOIN StatementTests.Joined2 as jj ON (j.simple_fk = jj.simple_fk) "
            + "WHERE jj.\"value\" is null");
        assertThat(rs, resultsEqualTo(Arrays.asList(
                "value-0|value-0.1|null", "value-0|value-0.2|null", "value-0|value-0.3|null",
                "value-2|value-2.1|null", "value-2|value-2.2|null", "value-2|value-2.3|null"), 
            row -> row.getString(1)  + "|" + row.getString(2) + "|" + row.getString(3)));
    }
    
    @Test
    @Ignore  // not implemented yet
    public void shoulFilterNotWildcard() throws Exception {
        ResultSet rs = CassandraTestConnection.executeSqlQuery("select value from StatementTests.Simple where value not like 'value-1%'");
        assertThat(rs, hasResultCount(10));
    }
    
    
    @Test
    @Ignore // not implemented yet
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
