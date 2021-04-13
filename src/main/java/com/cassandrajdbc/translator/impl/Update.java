/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
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
    public CStatement translate(SqlStatement<net.sf.jsqlparser.statement.update.Update> sql, ClusterConfiguration config) {
        net.sf.jsqlparser.statement.update.Update stmt = sql.getStatement();
        checkNullOrEmpty(stmt.getJoins());
        checkNullOrEmpty(stmt.getSelect());
        checkNullOrEmpty(stmt.getLimit());
        checkNullOrEmpty(stmt.getReturningExpressionList());
        
        List<WhereParser> where = stmt.getTables().stream()
            .map(table -> new WhereParser(stmt.getWhere(), table, config))
            .collect(Collectors.toList());
        
        if (where.stream().anyMatch(w -> !w.getSubselects().isEmpty())) {
            return (pstmt, params) -> {
                RegularStatement[] updates1 = where.stream()
                    .map(w -> Stream.concat(w.getClauses().stream(), w.getSubselects().stream()
                            .map(estmt -> estmt.extract(pstmt, params)))
                        .collect(() -> createAssignment(stmt, w.getTableMetadata(), config), 
                            Assignments::where, this::noparallel))
                    .toArray(RegularStatement[]::new);
                return new SimpleCStatement(sql, updates1.length == 1 ? updates1[0] : QueryBuilder.unloggedBatch(updates1))
                    .execute(pstmt, params);
            };
        }
        
        RegularStatement[] updates2 = where.stream()
            .map(w -> w.getClauses().stream()
                .collect(() -> createAssignment(stmt, w.getTableMetadata(), config), 
                    Assignments::where, this::noparallel))
            .toArray(RegularStatement[]::new);
        return new SimpleCStatement(sql, updates2.length == 1 ? updates2[0] : QueryBuilder.unloggedBatch(updates2));
    }

    private Assignments createAssignment(net.sf.jsqlparser.statement.update.Update stmt, TableMetadata table, ClusterConfiguration config) {
        BiFunction<String, Expression, Stream<Assignment>> assignmentParser = AssignmentParser.instance(table, config);
        return IntStream.range(0, stmt.getColumns().size()).boxed()
            .flatMap(i ->  assignmentParser.apply(stmt.getColumns().get(i).getColumnName(), getValue(stmt, i)))
            .collect(() -> QueryBuilder.update(table).with(),
                Assignments::and, this::noparallel);
    }

    private Expression getValue(net.sf.jsqlparser.statement.update.Update stmt, int idx) {
        return stmt.getExpressions().get(idx);
    }
    
    private static void checkNullOrEmpty(Object object) {
        if(object == null || "".equals(object)) {
            return;
        } 
        if(object instanceof Iterable && 
            !((Iterable<?>)object).iterator().hasNext()) {
            return;
        }
        unsupported();
    }
    
    private static <T> T unsupported() {
        throw new UnsupportedOperationException();
    }

    private <A,B> void noparallel(A a, B b) {
        throw new IllegalStateException("no parallel");
    }
    

}
