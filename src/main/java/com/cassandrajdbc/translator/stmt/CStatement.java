/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.stmt;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.commons.lang3.ArrayUtils;

import com.cassandrajdbc.statement.CPreparedStatement;

public interface CStatement {
    
    Iterable<CRow> execute(CPreparedStatement stmt, List<Object> params) throws SQLException;

    default String toNativeSQL() throws SQLException {
        throw new SQLException(getClass().getSimpleName() + " does support native SQL translation");
    }
    
    class CRow {
        
        private Function<Class<?>, Object>[] columns;
        private List<String> columnNames;

        @SuppressWarnings("unchecked")
        public CRow(List<String> columnNames, BiFunction<Integer, Class<?>, Object> delegate) {
            this(columnNames, IntStream.range(0, columnNames.size())
                .mapToObj(i -> (Function<Class<?>, Object>)t -> delegate.apply(i, t))
                .toArray(Function[]::new));
        }
        
        private CRow(List<String> columnNames, Function<Class<?>, Object>[] columns) {
            this.columnNames = columnNames;
            this.columns = columns;
        }
        
        @SuppressWarnings("unchecked")
        public <T> T getColumn(int idx, Class<T> type) {
            return (T) columns[idx].apply(type);
        }
        
        public <T> T getColumn(String name, Class<T> type) {
            return (T) columns[columnNames.indexOf(name)].apply(type);
        }
        
        public CRow add(String name, Function<Class<?>, Object> value) {
            List<String> columnNames = new ArrayList<>(this.columnNames);
            columnNames.add(name);
            return new CRow(columnNames, ArrayUtils.add(columns, value));
        }
        
        public CRow add(String name, Function<Class<?>, Object> value, int index) {
            List<String> columnNames = new ArrayList<>(this.columnNames);
            columnNames.add(index, name);
            return new CRow(columnNames, ArrayUtils.add(columns, index, value));
        }
        
        public CRow merge(CRow row) {          
            List<String> columnNames = new ArrayList<>(this.columnNames);
            columnNames.addAll(row.columnNames);
            return new CRow(columnNames, ArrayUtils.addAll(columns, row.columns));
        }
        
        @SuppressWarnings("unchecked")
        public CRow arrange(List<String> columnNames) {     
            Function<Class<?>, Object>[] array = columnNames.stream()
                .map(name -> this.columns[this.columnNames.indexOf(name)])
                .toArray(Function[]::new);
            return new CRow(columnNames, array);
        }
        
        public Object[] columnValues() {
            return Arrays.stream(columns)
                .map(f -> f.apply(Object.class))
                .toArray();
        }
        
        public List<String> columnNames() {
            return columnNames;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(columnValues()));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CRow)) {
                return false;
            }
            CRow other = (CRow) obj;
            return Arrays.equals(columnValues(), other.columnValues());
        }
        
    }
    
}
