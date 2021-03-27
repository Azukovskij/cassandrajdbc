/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;


import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.cassandrajdbc.statement.StatementOptions.Collation;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.types.ColumnTypes;
import com.cassandrajdbc.types.ColumnTypes.ColumnType;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;


public class CreateTable implements CqlBuilder<net.sf.jsqlparser.statement.create.table.CreateTable> {
    
    private static final List<String> NOT_NULL_SPEC = Arrays.asList("NOT", "NULL");

    @Override
    public Class<net.sf.jsqlparser.statement.create.table.CreateTable> getInputType() {
        return net.sf.jsqlparser.statement.create.table.CreateTable.class;
    }

    @Override
    public RegularStatement buildCql(net.sf.jsqlparser.statement.create.table.CreateTable stmt, ClusterConfiguration config) {
        Set<String> primaryKeys = resolvePrimaryKeyNames(stmt);
        Create result = stmt.getColumnDefinitions().stream()
            .collect(() -> SchemaBuilder.createTable(stmt.getTable().getSchemaName(), escape(stmt.getTable().getName(), config)), 
                (create,col) -> addColumn(create, col, primaryKeys.contains(col.getColumnName()), config), 
                (a,b) -> { throw new IllegalStateException("no parallel"); });
        return stmt.isIfNotExists() ? result.ifNotExists() : result;
    }

    private Set<String> resolvePrimaryKeyNames(net.sf.jsqlparser.statement.create.table.CreateTable stmt) {
        return stmt.getIndexes().stream()
            .filter(idx -> idx.getType().equals("PRIMARY KEY"))
            .flatMap(idx -> idx.getColumnsNames().stream())
            .collect(Collectors.toSet());
    }

    private Create addColumn(Create cql, ColumnDefinition column, boolean primarykey, ClusterConfiguration config) {
        String columnName = escape(column.getColumnName(), config);
        DataType dataType = ColumnTypes.forName(column.getColDataType().getDataType(), ColumnType::getCqlType);
        if(!primarykey) {
            return cql.addColumn(columnName, dataType);
        }
        return isNotNull(column) ? cql.addPartitionKey(columnName, dataType) : cql.addClusteringColumn(columnName, dataType) ;
    }
    
    private boolean isNotNull(ColumnDefinition column) {
        return column.getColumnSpecStrings() != null && column.getColumnSpecStrings().containsAll(NOT_NULL_SPEC);
    }
    
    private String escape(String value, ClusterConfiguration config) {
        return config.getStatementOptions().getCollation() == Collation.CASE_SENSITIVE ? "\"" + value + "\"" : value;
    }
    
}
