/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import com.cassandrajdbc.expressions.ClauseParser;
import com.cassandrajdbc.statement.StatementOptions;
import com.cassandrajdbc.statement.StatementOptions.Collation;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

public class Delete implements CqlBuilder<net.sf.jsqlparser.statement.delete.Delete>{

    @Override
    public Class<? extends net.sf.jsqlparser.statement.delete.Delete> getInputType() {
        return net.sf.jsqlparser.statement.delete.Delete.class;
    }

    @Override
    public RegularStatement buildCql(net.sf.jsqlparser.statement.delete.Delete stmt, StatementOptions config) {
        if(stmt.getJoins() != null) {
            throw new UnsupportedOperationException("joins not supported " + stmt);
        }
        Function<Expression, Stream<Clause>> clauseParser = ClauseParser.instance(config);
        RegularStatement[] deletes = resolveTables(stmt)
            .map(table -> clauseParser.apply(stmt.getWhere())
                .collect(() -> QueryBuilder.delete().all()
                    .from(table.getSchemaName(), escape(table.getName(), config)), 
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
    
    private String escape(String value, StatementOptions config) {
        return config.getCollation() == Collation.CASE_SENSITIVE ? "\"" + value + "\"" : value;
    }

}
