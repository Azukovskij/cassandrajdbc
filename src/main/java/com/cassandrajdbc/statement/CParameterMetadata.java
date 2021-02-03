/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.statement;

import java.sql.JDBCType;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.List;

import com.cassandrajdbc.types.CqlType;
import com.cassandrajdbc.types.CqlType.TypeInfo;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;

class CParameterMetadata implements ParameterMetaData{
    
    private final ParamInfo[] params;
    
    CParameterMetadata(List<Object> params, CodecRegistry registry) {
        this.params = params.stream()
            .map(obj -> obj == null ? new ParamInfo() :
                new ParamInfo(registry.codecFor(obj).getCqlType()))
            .toArray(ParamInfo[]::new);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
          return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }


    @Override
    public int getParameterCount() throws SQLException {
        return params.length;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return params[param].sqlType.getVendorTypeNumber();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return params[param].sqlType.getName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return params[param].javaType.getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return parameterModeUnknown;
    }
    
    private static class ParamInfo {
        
        private final SQLType sqlType;
        private final Class<?> javaType;
        
        public ParamInfo() {
            this.sqlType = JDBCType.NULL;
            this.javaType = Object.class;
        }
        
        public ParamInfo(DataType dataType) {
            TypeInfo type = CqlType.resolve(dataType);
            this.sqlType = type.getSqlType();
            this.javaType = type.getJavaType();
        }
        
    }

}
