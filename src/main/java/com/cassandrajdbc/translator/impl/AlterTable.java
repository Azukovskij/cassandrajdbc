/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import java.util.Optional;
import java.util.stream.Stream;

import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.cassandrajdbc.types.ColumnTypes;
import com.cassandrajdbc.types.ColumnTypes.ColumnType;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.schemabuilder.Alter;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;

import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterExpression.ColumnDataType;
import net.sf.jsqlparser.statement.alter.AlterOperation;


public class AlterTable implements CqlBuilder<net.sf.jsqlparser.statement.alter.Alter>{

    @Override
    public Class<net.sf.jsqlparser.statement.alter.Alter> getInputType() {
        return net.sf.jsqlparser.statement.alter.Alter.class;
    }
    
    @Override
    public CStatement translate(SqlStatement<net.sf.jsqlparser.statement.alter.Alter> stmt, ClusterConfiguration config) {
        return new SimpleCStatement(stmt, buildCql(stmt.getStatement(), config));
    }

    private RegularStatement buildCql(net.sf.jsqlparser.statement.alter.Alter stmt, ClusterConfiguration config) {
        TableMetadata table = config.getTableMetadata(stmt.getTable());
        Alter alter = SchemaBuilder.alterTable(table.getKeyspace().getName(), getExternalName(table, config));
        return stmt.getAlterExpressions().stream()
            .flatMap(expression -> stream(expression)
                .map(column -> alterColumn(alter, table, expression.getOperation(), column, config)))
            .reduce((a,b) -> b)
            .orElseThrow(() -> new IllegalStateException("No columns in alter " + stmt));
    }

    private SchemaStatement alterColumn(Alter alter, TableMetadata table, AlterOperation operation, ColumnDataType column, ClusterConfiguration config) {
        switch (operation) {
            case ADD:
                return alter.addColumn(getExternalName(table, column.getColumnName(), config))
                    .type(ColumnTypes.forName(column.getColDataType().getDataType(), ColumnType::getCqlType));
            case DROP:
                return alter.dropColumn(getExternalName(table, column.getColumnName(), config));
            default:
                throw new IllegalStateException("Unknown alter operation " + operation);
        }
    }
    
    private Stream<ColumnDataType> stream(AlterExpression col) {
        return Optional.ofNullable(col.getColDataTypeList())
            .map(columns -> col.getColumnName() == null ? columns.stream() : columns.stream().limit(1))
            .orElseGet(() -> Stream.of(col.new ColumnDataType(col.getColumnName(), null)));
    }
    
    private String getExternalName(TableMetadata table, ClusterConfiguration config) {
        return escape(table.getName());
    }
    
    private String getExternalName(TableMetadata table, String columnName, ClusterConfiguration config) {
        return escape(config.getColumnMetadata(table, columnName).getName());
    }

    private String escape(String string) {
        return string.equals(string.toLowerCase())
            ? string
            : "\"" + string + "\"";
    }

}
