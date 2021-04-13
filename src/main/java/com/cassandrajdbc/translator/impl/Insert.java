/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.cassandrajdbc.expressions.ItemListParser;
import com.cassandrajdbc.expressions.ValueParser;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.CStatement.CRow;
import com.cassandrajdbc.translator.stmt.ChainingCStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;

public class Insert implements CqlBuilder<net.sf.jsqlparser.statement.insert.Insert> {
    
    private Select selectTransaltor = new Select();
    
    @Override
    public Class<net.sf.jsqlparser.statement.insert.Insert> getInputType() {
        return net.sf.jsqlparser.statement.insert.Insert.class;
    }
    
    @Override
    public CStatement translate(SqlStatement<net.sf.jsqlparser.statement.insert.Insert> stmt, ClusterConfiguration config) {
        net.sf.jsqlparser.statement.insert.Insert sql = stmt.getStatement();
        checkNullOrEmpty(sql.getModifierPriority());
        checkNullOrEmpty(sql.getDuplicateUpdateColumns());
        checkNullOrEmpty(sql.getDuplicateUpdateExpressionList());
        checkNullOrEmpty(sql.getReturningExpressionList());
        
        if(sql.getSelect() != null) {
            return new ChainingCStatement(selectTransaltor.translate(new SqlStatement(sql.getSelect()), config), 
                row -> new SimpleCStatement(stmt, buildCql(stmt.getStatement(), row, config)));
        }
        return new SimpleCStatement(stmt, buildCql(stmt.getStatement(), config));
    }

    
    private RegularStatement buildCql(net.sf.jsqlparser.statement.insert.Insert stmt, CRow row, ClusterConfiguration config) {
        TableMetadata table = config.getTableMetadata(stmt.getTable());
        Object[] columns = row.columnValues();
        return IntStream.range(0, columns.length).boxed()
            .collect(() -> QueryBuilder.insertInto(table), 
                (insert, i) -> addValue(insert, config.getColumnMetadata(table, stmt.getColumns().get(i).getColumnName()), columns[i]), this::noparallel);
    }

    private RegularStatement buildCql(net.sf.jsqlparser.statement.insert.Insert stmt, ClusterConfiguration config) {
        TableMetadata table = config.getTableMetadata(stmt.getTable());
        RegularStatement[] inserts = ItemListParser.instance(config).apply(stmt.getItemsList())
            .map(exprList -> IntStream.range(0, stmt.getColumns().size()).boxed()
                .collect(() -> QueryBuilder.insertInto(table), 
                    (insert, i) -> addValue(insert, config.getColumnMetadata(table, stmt.getColumns().get(i).getColumnName()), exprList.get(i)), this::noparallel))
            .toArray(RegularStatement[]::new);
        return inserts.length == 1 ? inserts[0] : QueryBuilder.unloggedBatch(inserts);
    }

    private <A,B> void noparallel(A a, B b) {
        throw new IllegalStateException("no parallel");
    }
    
    private com.datastax.driver.core.querybuilder.Insert addValue(com.datastax.driver.core.querybuilder.Insert insert, ColumnMetadata column, Expression valueExpr) {
        return addValue(insert, column, ValueParser.instance(column).apply(valueExpr));
    }

    private com.datastax.driver.core.querybuilder.Insert addValue(com.datastax.driver.core.querybuilder.Insert insert,
        ColumnMetadata column, Object apply) {
        return insert.value(column.getName(), apply);
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

}
