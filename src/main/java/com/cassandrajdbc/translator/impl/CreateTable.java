/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;


import static java.util.function.Predicate.not;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.cassandrajdbc.statement.StatementOptions.Collation;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.cassandrajdbc.types.ColumnTypes;
import com.cassandrajdbc.types.ColumnTypes.ColumnType;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;


public class CreateTable implements CqlBuilder<net.sf.jsqlparser.statement.create.table.CreateTable> {
    
    private static final String CK_SPEC = "CK";
    private static final String PK_SPEC = "PK";
    
    private static final List<String> NOT_NULL_SPEC = Arrays.asList("NOT", "NULL");

    @Override
    public Class<net.sf.jsqlparser.statement.create.table.CreateTable> getInputType() {
        return net.sf.jsqlparser.statement.create.table.CreateTable.class;
    }
    
    @Override
    public CStatement translate(SqlStatement<net.sf.jsqlparser.statement.create.table.CreateTable> stmt, ClusterConfiguration config) {
        return new SimpleCStatement(stmt, buildCql(stmt.getStatement(), config));
    }

    private RegularStatement buildCql(net.sf.jsqlparser.statement.create.table.CreateTable stmt, ClusterConfiguration config) {
        initKeyColumnSpecs(stmt);
        Create result = stmt.getColumnDefinitions().stream()
            .collect(() -> SchemaBuilder.createTable(stmt.getTable().getSchemaName(), escape(stmt.getTable().getName(), config)), 
                (create,col) -> addColumn(create, col, config), 
                (a,b) -> { throw new IllegalStateException("no parallel"); });
        return stmt.isIfNotExists() ? result.ifNotExists() : result;
    }

    private void initKeyColumnSpecs(net.sf.jsqlparser.statement.create.table.CreateTable stmt) {
        Map<String, ColumnDefinition> columns = stmt.getColumnDefinitions().stream()
            .collect(Collectors.toMap(ColumnDefinition::getColumnName, Function.identity()));
        Map<ColumnDefinition, Boolean> primaryKeys = stmt.getIndexes().stream()
            .filter(idx -> idx.getType().equals("PRIMARY KEY"))
            .flatMap(idx -> idx.getColumnsNames().stream())
            .map(columns::get)
            .collect(Collectors.toMap(Function.identity(), this::isNotNull, (a,b) -> { throw new IllegalStateException("no parallel"); }, LinkedHashMap::new));
        
        // keys configured via column spec
        if(primaryKeys.keySet().stream().anyMatch(this::isPkColumn)) {
            primaryKeys.keySet().stream()
                .filter(not(this::isPkColumn))
                .forEach(col -> addColumnSpec(col, CK_SPEC));
            
        } else {
            // if no partition keys configured, make first column partitioning
            if(!primaryKeys.isEmpty() && !primaryKeys.values().contains(true)) {
                primaryKeys.entrySet().iterator().next().setValue(true);
            }
            
            // use non-null constraint to to mark partitioning keys
            primaryKeys.entrySet().forEach(e -> addColumnSpec(e.getKey(), e.getValue() ? PK_SPEC : CK_SPEC));
        }
    }

    private void addColumnSpec(ColumnDefinition col, String spec) {
        col.addColumnSpecs(spec);
    }

    private List<String> getColumnSpecs(ColumnDefinition col) {
        return Optional.ofNullable(col.getColumnSpecs())
            .orElseGet(Collections::emptyList);
    }

    private Create addColumn(Create cql, ColumnDefinition column, ClusterConfiguration config) {
        String columnName = escape(column.getColumnName(), config);
        DataType dataType = ColumnTypes.forName(column.getColDataType().getDataType(), ColumnType::getCqlType);
        if(isPkColumn(column)) {
            return cql.addPartitionKey(columnName, dataType);
        }
        if(isCkColumn(column)) {
            return cql.addClusteringColumn(columnName, dataType);
        }
        return cql.addColumn(columnName, dataType);
    }

    private boolean isPkColumn(ColumnDefinition column) {
        return getColumnSpecs(column).contains(PK_SPEC);
    }

    private boolean isCkColumn(ColumnDefinition column) {
        return getColumnSpecs(column).contains(CK_SPEC);
    }
    
    private boolean isNotNull(ColumnDefinition column) {
        return getColumnSpecs(column).containsAll(NOT_NULL_SPEC);
    }
    
    private String escape(String value, ClusterConfiguration config) {
        return config.getStatementOptions().getCollation() == Collation.CASE_SENSITIVE ? "\"" + value + "\"" : value;
    }
    
}
