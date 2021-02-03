/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;
import java.util.stream.IntStream;

import com.cassandrajdbc.expressions.CqlValue;
import com.cassandrajdbc.statement.StatementOptions;
import com.cassandrajdbc.statement.StatementOptions.Collation;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.sf.jsqlparser.schema.Column;

public class Insert implements CqlBuilder<net.sf.jsqlparser.statement.insert.Insert> {

    @Override
    public Class<net.sf.jsqlparser.statement.insert.Insert> getInputType() {
        return net.sf.jsqlparser.statement.insert.Insert.class;
    }

    @Override
    public RegularStatement buildCql(net.sf.jsqlparser.statement.insert.Insert stmt, StatementOptions config) {
        if(stmt.getSelect() != null) {
            throw new UnsupportedOperationException("Insert from select is not supported, query: '" + stmt + "'");
        }
        RegularStatement[] inserts = CqlValue.toCqlValueList(stmt.getItemsList()).stream()
            .map(values -> IntStream.range(0, stmt.getColumns().size()).boxed()
                .collect(() -> QueryBuilder.insertInto(stmt.getTable().getSchemaName(), escape(stmt.getTable().getName(), config)), 
                    (insert, i) -> addValue(insert, stmt.getColumns().get(i), values[i], config), 
                    (a,b) -> { throw new IllegalStateException("no parallel"); }))
            .toArray(RegularStatement[]::new);
        return inserts.length == 1 ? inserts[0] : QueryBuilder.unloggedBatch(inserts);
    }
    
    private com.datastax.driver.core.querybuilder.Insert addValue(com.datastax.driver.core.querybuilder.Insert insert, Column column, Object value, StatementOptions config) {
        return insert.value(escape(column.getColumnName(), config), value);
    }
    
    private String escape(String value, StatementOptions config) {
        return config.getCollation() == Collation.CASE_SENSITIVE ? "\"" + value + "\"" : value;
    }

}
