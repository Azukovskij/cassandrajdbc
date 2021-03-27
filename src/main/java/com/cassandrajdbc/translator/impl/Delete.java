/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.cassandrajdbc.expressions.ClauseParser;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
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
    public CStatement translate(SqlStatement<net.sf.jsqlparser.statement.delete.Delete> stmt, ClusterConfiguration config) {
        return new SimpleCStatement(stmt, buildCql(stmt.getStatement(), config));
    }
    
    private RegularStatement buildCql(net.sf.jsqlparser.statement.delete.Delete stmt, ClusterConfiguration config) {
        if(stmt.getJoins() != null) {
            throw new UnsupportedOperationException("joins not supported " + stmt);
        }
        RegularStatement[] deletes = resolveTables(stmt)
            .map(config::getTableMetadata)
            .map(table -> ClauseParser.instance(table, config).apply(stmt.getWhere())
                .collect(() -> QueryBuilder.delete().all().from(table), 
                        com.datastax.driver.core.querybuilder.Delete::where, 
                        (a,b) -> { throw new IllegalStateException("no parallel"); }) )
            .toArray(RegularStatement[]::new);
        return deletes.length == 1 ? deletes[0] : QueryBuilder.unloggedBatch(deletes);
    }

    private Stream<Table> resolveTables(net.sf.jsqlparser.statement.delete.Delete stmt) {
        return Optional.ofNullable(stmt.getTable())
            .map(Stream::of)
            .orElseGet(() -> Optional.ofNullable(stmt.getTables())
                .map(List::stream)
                .orElseGet(Stream::empty));
    }

}
