/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.cassandrajdbc.expressions.AssignmentParser;
import com.cassandrajdbc.expressions.WhereParser;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update.Assignments;

import net.sf.jsqlparser.expression.Expression;

public class Update implements CqlBuilder<net.sf.jsqlparser.statement.update.Update> {
    
    @Override
    public Class<net.sf.jsqlparser.statement.update.Update> getInputType() {
        return net.sf.jsqlparser.statement.update.Update.class;
    }
    
    @Override
    public CStatement translate(SqlStatement<net.sf.jsqlparser.statement.update.Update> stmt, ClusterConfiguration config) {
        return new SimpleCStatement(stmt, buildCql(stmt.getStatement(), config));
    }

    private RegularStatement buildCql(net.sf.jsqlparser.statement.update.Update stmt, ClusterConfiguration config) {
        if(stmt.getJoins() != null) {
            throw new UnsupportedOperationException("joins not supported " + stmt);
        }
        if(stmt.getSelect() != null) {
            throw new UnsupportedOperationException("subselect not supported" + stmt);
        }
        if(stmt.getReturningExpressionList() != null) {
            throw new UnsupportedOperationException("returning not supported" + stmt);
        }

        RegularStatement[] updates = Stream.of(stmt.getTable())
            .map(config::getTableMetadata)
            .map(table -> WhereParser.instance(table, config).apply(stmt.getWhere())
                .collect(() -> createAssignment(stmt, table, config), 
                    Assignments::where, (a,b) -> { throw new IllegalStateException("no parallel"); }))
            .toArray(RegularStatement[]::new);
        return updates.length == 1 ? updates[0] : QueryBuilder.unloggedBatch(updates);
    }

    private Assignments createAssignment(net.sf.jsqlparser.statement.update.Update stmt, TableMetadata table, ClusterConfiguration config) {
        BiFunction<String, Expression, Stream<Assignment>> assignmentParser = AssignmentParser.instance(table, config);
        return IntStream.range(0, stmt.getColumns().size()).boxed()
            .flatMap(i ->  assignmentParser.apply(stmt.getColumns().get(i).getColumnName(), getValue(stmt, i)))
            .collect(() -> QueryBuilder.update(table).with(),
                Assignments::and, (a,b) -> { throw new IllegalStateException("no parallel"); });
    }

    private Expression getValue(net.sf.jsqlparser.statement.update.Update stmt, int idx) {
        return stmt.getExpressions().get(idx);
    }

}
