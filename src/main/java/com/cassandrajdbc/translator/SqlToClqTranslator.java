/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cassandrajdbc.statement.StatementOptions;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.NativeCStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.cassandrajdbc.util.SPI;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

public class SqlToClqTranslator {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleCStatement.class);
    private static final Map<Class<?>, CqlBuilder<?>> builders = SPI.loadAll(CqlBuilder.class)
                    .collect(Collectors.toUnmodifiableMap(CqlBuilder::getInputType, Function.identity(), (a,b) -> a));
    
    public static CStatement translateToCQL(String sql, Metadata clusterMetadata) {
        try {
            return translateToCQL(SqlParser.parse(sql), clusterMetadata)
                .orElseGet(() -> new NativeCStatement(sql));
        } catch (Exception e) {
            logger.trace("SQL parse failed, using native statement", e);
            return new NativeCStatement(sql);
        }
    }
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Optional<CStatement> translateToCQL(SqlStatement<?> statement, Metadata clusterMetadata) {
        ClusterConfiguration clusterConfig = new ClusterConfiguration(clusterMetadata, new StatementOptions());
        return Optional.ofNullable(builders.get(statement.getStatement().getClass()))
            .map(builder -> ((CqlBuilder)builder).translate((SqlStatement)statement, clusterConfig));
    }
    
    public interface CqlBuilder<T extends Statement> {
        
        Class<? extends T> getInputType();
        
        CStatement translate(SqlStatement<T> stmt, ClusterConfiguration config);
        
    }
    
    public static class ClusterConfiguration {
        
        private final Metadata clusterMetadata;
        private final StatementOptions statementOptions;

        ClusterConfiguration(Metadata clusterMetadata, StatementOptions statementOptions) {
            this.clusterMetadata = clusterMetadata;
            this.statementOptions = statementOptions;
        }

        public Metadata getClusterMetadata() {
            return clusterMetadata;
        }
        
        public TableMetadata getTableMetadata(Table table) {
            var name = unquote(table.getName());
            return Optional.ofNullable(clusterMetadata.getKeyspace(table.getSchemaName()))
                .map(ks -> Optional.ofNullable(ks.getTable(name))
                    .orElseGet(() -> ks.getTable("\"" + name + "\"")))
                .orElseThrow(() -> new IllegalArgumentException("Table does not exist " + table.getFullyQualifiedName()));
        }
        
        public ColumnMetadata getColumnMetadata(TableMetadata table, String columnName) {
            var name = unquote(columnName);
            return Optional.of(table)
                .map(tableMetdata -> Optional.ofNullable(tableMetdata.getColumn(name))
                    .orElseGet(() -> tableMetdata.getColumn("\"" + name + "\"")))
                .orElseThrow(() -> new IllegalArgumentException("Column does not exist " + 
                    table.getKeyspace().getName() + "." + table.getName() + "." + columnName));
        }
        
        public StatementOptions getStatementOptions() {
            return statementOptions;
        }
        
        private String unquote(String name) {
            return name.startsWith("\"") && name.endsWith("\"") ? name.substring(1, name.length() - 1) : name; 
        }
        
    }

}
