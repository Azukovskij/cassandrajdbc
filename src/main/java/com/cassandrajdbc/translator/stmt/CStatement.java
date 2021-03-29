/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.stmt;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import com.cassandrajdbc.statement.CPreparedStatement;

public interface CStatement {
    
    Iterable<CRow> execute(CPreparedStatement stmt, List<Object> params) throws SQLException;

    default String toNativeSQL() throws SQLException {
        throw new SQLException(getClass().getSimpleName() + " does support native SQL translation");
    }
    
    class CRow {
        
        private final BiFunction<Integer, Class<?>, Object> delegate;
        private final int width;

        public CRow(BiFunction<Integer, Class<?>, Object> delegate, int width) {
            this.delegate = delegate;
            this.width = width;
        }
        
        @SuppressWarnings("unchecked")
        public <T> T getColumn(int idx, Class<T> type) {
            return (T) delegate.apply(idx, type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(width, Arrays.hashCode(columns()));
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
            return width == other.width && Arrays.equals(columns(), other.columns());
        }
        
        private Object[] columns() {
            return IntStream.range(0, width)
                .mapToObj(i -> getColumn(i, Object.class))
                .toArray();
        }
        
    }
    
}
