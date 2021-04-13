/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.cassandrajdbc.expressions.WhereParser;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.DelegateCStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.sf.jsqlparser.schema.Table;

public class Delete implements CqlBuilder<net.sf.jsqlparser.statement.delete.Delete>{

    @Override
    public Class<? extends net.sf.jsqlparser.statement.delete.Delete> getInputType() {
        return net.sf.jsqlparser.statement.delete.Delete.class;
    }

    @Override
    public CStatement translate(SqlStatement<net.sf.jsqlparser.statement.delete.Delete> sql, ClusterConfiguration config) {
        net.sf.jsqlparser.statement.delete.Delete stmt = sql.getStatement();
        checkNullOrEmpty(stmt.getJoins());
        checkNullOrEmpty(stmt.getLimit());
        checkNullOrEmpty(stmt.getOrderByElements());
        
        List<WhereParser> where = resolveTables(stmt)
            .map(table -> new WhereParser(stmt.getWhere(), table, config))
            .collect(Collectors.toList());
        
        if (where.stream().anyMatch(w -> !w.getSubselects().isEmpty())) {
            return new DelegateCStatement(sql, (pstmt, params) -> {
                RegularStatement[] deletes1 = where.stream()
                    .map(w -> Stream.concat(w.getClauses().stream(), w.getSubselects().stream()
                            .map(ss -> ss.extract(pstmt, params)))
                        .collect(() -> QueryBuilder.delete().all().from(w.getTableMetadata()), 
                                com.datastax.driver.core.querybuilder.Delete::where, this::noparallel) )
                    .toArray(RegularStatement[]::new);
                return new SimpleCStatement(sql, deletes1.length == 1 ? deletes1[0] : QueryBuilder.unloggedBatch(deletes1));
            });
        }
        
        RegularStatement[] deletes2 = where.stream()
            .map(w -> w.getClauses().stream()
                .collect(() -> QueryBuilder.delete().all().from(w.getTableMetadata()), 
                        com.datastax.driver.core.querybuilder.Delete::where, this::noparallel) )
            .toArray(RegularStatement[]::new);
        return new SimpleCStatement(sql, deletes2.length == 1 ? deletes2[0] : QueryBuilder.unloggedBatch(deletes2));
    }

    private Stream<Table> resolveTables(net.sf.jsqlparser.statement.delete.Delete stmt) {
        return Optional.ofNullable(stmt.getTable())
            .map(Stream::of)
            .orElseGet(() -> Optional.ofNullable(stmt.getTables())
                .map(List::stream)
                .orElseGet(Stream::empty));
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
