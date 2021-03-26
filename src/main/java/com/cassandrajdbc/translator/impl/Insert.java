/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;
import java.util.UUID;
import java.util.stream.IntStream;

import com.cassandrajdbc.expressions.CqlValue;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class Insert implements CqlBuilder<net.sf.jsqlparser.statement.insert.Insert> {

    @Override
    public Class<net.sf.jsqlparser.statement.insert.Insert> getInputType() {
        return net.sf.jsqlparser.statement.insert.Insert.class;
    }

    @Override
    public RegularStatement buildCql(net.sf.jsqlparser.statement.insert.Insert stmt, ClusterConfiguration config) {
        if(stmt.getSelect() != null) {
            throw new UnsupportedOperationException("Insert from select is not supported, query: '" + stmt + "'");
        }
        TableMetadata table = config.getTableMetadata(stmt.getTable());
        RegularStatement[] inserts = CqlValue.toCqlValueList(stmt.getItemsList()).stream()
            .map(values -> IntStream.range(0, stmt.getColumns().size()).boxed()
                .collect(() -> QueryBuilder.insertInto(table), 
                    (insert, i) -> addValue(insert, config.getColumnMetadata(table, stmt.getColumns().get(i).getColumnName()), values[i]), 
                    (a,b) -> { throw new IllegalStateException("no parallel"); }))
            .toArray(RegularStatement[]::new);
        return inserts.length == 1 ? inserts[0] : QueryBuilder.unloggedBatch(inserts);
    }
    
    private com.datastax.driver.core.querybuilder.Insert addValue(com.datastax.driver.core.querybuilder.Insert insert, ColumnMetadata column, Object value) {
        return insert.value(column.getName(), normalize(column, value));
    }

    // FIXME move to ConverterRegistry
    private Object normalize(ColumnMetadata col, Object val) {
        if(col.getType() == DataType.uuid() && val instanceof String) {
            return UUID.fromString((String) val);
        }
        return val;
    }

}
