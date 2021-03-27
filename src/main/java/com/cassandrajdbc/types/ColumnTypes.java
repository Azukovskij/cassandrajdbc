/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;

import com.datastax.driver.core.DataType;

import net.sf.jsqlparser.statement.create.table.ColDataType;

// FIXME
public class ColumnTypes {
    
    public enum StandardTypes implements ColumnType {
        VARCHAR(JDBCType.VARCHAR, DataType.varchar(), String.class),
        CHAR(JDBCType.CHAR, DataType.varchar(), String.class),
        TEXT(JDBCType.VARCHAR, DataType.text(), String.class),
        CLOB(JDBCType.CLOB, DataType.varchar(), Clob.class),
        UUID(JDBCType.VARCHAR, DataType.uuid(), UUID.class),
        INT(JDBCType.INTEGER, DataType.cint(), Integer.class),
        INTEGER(JDBCType.INTEGER, DataType.cint(), Integer.class),
        NUMERIC(JDBCType.NUMERIC, DataType.cint(), Integer.class),
        BIGINT(JDBCType.BIGINT, DataType.bigint(), Long.class),
        VARINT(JDBCType.INTEGER, DataType.varint(), Integer.class),
        TINYINT(JDBCType.TINYINT, DataType.tinyint(), Byte.class),
        SMALLINT(JDBCType.SMALLINT, DataType.smallint(), Short.class),
        DATE(JDBCType.DATE, DataType.date(), Date.class),
        TIMESTAMP(JDBCType.TIMESTAMP, DataType.timestamp(), Timestamp.class),
        TIME(JDBCType.TIME, DataType.time(), Time.class),
        FLOAT(JDBCType.FLOAT, DataType.cfloat(), Float.class),
        DECIMAL(JDBCType.DECIMAL, DataType.decimal(), BigDecimal.class),
        DOUBLE(JDBCType.DOUBLE, DataType.cdouble(), Double.class),
        BOOLEAN(JDBCType.BOOLEAN, DataType.cboolean(), Boolean.class),
        BLOB(JDBCType.BLOB, DataType.blob(), byte[].class),
        BINARY(JDBCType.BINARY, DataType.blob(), byte[].class);
        
        private final SQLType sqlType;
        private final DataType cqlType;
        private final Class<?> javaType;
     
        private StandardTypes(SQLType sqlType, DataType cqlType, Class<?> javaType) {
            this.sqlType = sqlType;
            this.cqlType = cqlType;
            this.javaType = javaType;
        }

        @Override
        public SQLType getSqlType() {
            return sqlType;
        }

        @Override
        public DataType getCqlType() {
            return cqlType;
        }

        @Override
        public Class<?> getJavaType() {
            return javaType;
        }
    }
    
    public static ColumnType[] values() {
        return StandardTypes.values();
    }

    public static <T> T forName(String typeName, Function<ColumnType, T> mapper) {
        if(typeName == null) {
            return null;
        }
        return resolveType(t -> t.getSqlType().getName().equals(typeName))
            .or(() -> resolveType(t -> t.getCqlType().getName().name().equalsIgnoreCase(typeName)))
            .map(mapper)
            .orElseThrow(() -> new IllegalArgumentException("Unsupported data type " + typeName + ", please register TypeMapping for this type"));
    }

    public static <T> T fromCqlType(DataType type, Function<ColumnType, T> mapper) {
        if(type == null) {
            return null;
        }
        return resolveType(t -> t.getCqlType().equals(type))
            .map(mapper)
            .orElseThrow(() -> new IllegalArgumentException("Unsupported data type " + type + ", please register TypeMapping for this type"));
    }
    

    private static Optional<ColumnType> resolveType(Predicate<ColumnType> criteria) {
        return Arrays.stream(StandardTypes.values())
            .map(ColumnType.class::cast)
            .filter(criteria)
            .findFirst();
    }
    
    
    public interface ColumnType {

        SQLType getSqlType();
        
        DataType getCqlType();
        
        Class<?> getJavaType();
        
        default ColDataType getSqlDataType() {
            ColDataType dataType = new ColDataType();
            dataType.setDataType(getSqlType().getName());
            return dataType;
        }
        
    }

}

