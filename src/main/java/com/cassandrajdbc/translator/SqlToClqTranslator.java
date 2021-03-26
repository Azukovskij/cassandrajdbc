/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.cassandrajdbc.statement.StatementOptions;
import com.cassandrajdbc.util.SPI;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.TableMetadata;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

public class SqlToClqTranslator {
    
    private final static Map<Class<?>, CqlBuilder<?>> builders = SPI.loadAll(CqlBuilder.class)
                    .collect(Collectors.toUnmodifiableMap(CqlBuilder::getInputType, Function.identity(), (a,b) -> a));
    
    
    public static RegularStatement translateToCQL(Statement statement, Metadata clusterMetadata) {
        ClusterConfiguration clusterConfig = new ClusterConfiguration(clusterMetadata, new StatementOptions());
        return Optional.ofNullable(builders.get(statement.getClass()))
            .map(builder -> ((CqlBuilder<Statement>)builder).buildCql(statement, clusterConfig))
            .orElse(null);
    }
    
    public interface CqlBuilder<T extends Statement> {
        
        Class<? extends T> getInputType();
        
        RegularStatement buildCql(T stmt, ClusterConfiguration config);
        
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
            return Optional.ofNullable(clusterMetadata.getKeyspace(table.getSchemaName()))
                .map(ks -> Optional.ofNullable(ks.getTable(table.getName()))
                    .orElseGet(() -> ks.getTable("\"" + table.getName() + "\"")))
                .orElseThrow(() -> new IllegalArgumentException("Table does not exist " + table.getFullyQualifiedName()));
        }
        
        public ColumnMetadata getColumnMetadata(TableMetadata table, String columnName) {
            return Optional.of(table)
                .map(tableMetdata -> Optional.ofNullable(tableMetdata.getColumn(columnName))
                    .orElseGet(() -> tableMetdata.getColumn("\"" + columnName + "\"")))
                .orElseThrow(() -> new IllegalArgumentException("Column does not exist " + 
                    table.getKeyspace().getName() + "." + table.getName() + "." + "." + columnName));
        }
        
        public StatementOptions getStatementOptions() {
            return statementOptions;
        }
        
    }

}
