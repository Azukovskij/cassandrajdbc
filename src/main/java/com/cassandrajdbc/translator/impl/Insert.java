/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;
import java.util.stream.IntStream;

import com.cassandrajdbc.expressions.ItemListParser;
import com.cassandrajdbc.expressions.ValueParser;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.ChainingCStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.sf.jsqlparser.expression.Expression;

public class Insert implements CqlBuilder<net.sf.jsqlparser.statement.insert.Insert> {
    
    private Select selectTransaltor = new Select();
    
    @Override
    public Class<net.sf.jsqlparser.statement.insert.Insert> getInputType() {
        return net.sf.jsqlparser.statement.insert.Insert.class;
    }
    
    @Override
    public CStatement translate(SqlStatement<net.sf.jsqlparser.statement.insert.Insert> stmt, ClusterConfiguration config) {
        net.sf.jsqlparser.statement.insert.Insert sql = stmt.getStatement();
        if(sql.getSelect() != null) {
            return new ChainingCStatement(selectTransaltor.translate(new SqlStatement(sql.getSelect()), config), row -> {
                // FIXME
                
                throw new UnsupportedOperationException("Insert from select is not supported, query: '" + stmt + "'");
            });
        }
        return new SimpleCStatement(stmt, buildCql(stmt.getStatement(), config));
    }

    private RegularStatement buildCql(net.sf.jsqlparser.statement.insert.Insert stmt, ClusterConfiguration config) {
      
        TableMetadata table = config.getTableMetadata(stmt.getTable());
        
        RegularStatement[] inserts = ItemListParser.instance().apply(stmt.getItemsList())
            .map(exprList -> IntStream.range(0, stmt.getColumns().size()).boxed()
                .collect(() -> QueryBuilder.insertInto(table), 
                    (insert, i) -> addValue(insert, config.getColumnMetadata(table, stmt.getColumns().get(i).getColumnName()), exprList.get(i)), 
                    (a,b) -> { throw new IllegalStateException("no parallel"); }))
            .toArray(RegularStatement[]::new);
        return inserts.length == 1 ? inserts[0] : QueryBuilder.unloggedBatch(inserts);
    }
    
    private com.datastax.driver.core.querybuilder.Insert addValue(com.datastax.driver.core.querybuilder.Insert insert, ColumnMetadata column, Expression valueExpr) {
        return insert.value(column.getName(), ValueParser.instance(column).apply(valueExpr));
    }

}
