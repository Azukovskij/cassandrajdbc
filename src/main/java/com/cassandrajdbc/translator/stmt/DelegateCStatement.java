/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.stmt;

import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

import com.cassandrajdbc.statement.CPreparedStatement;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;

public class DelegateCStatement implements CStatement {
    
    private SqlStatement<?> sql;
    private BiFunction<CPreparedStatement, List<Object>, CStatement> delegate;
    
    public DelegateCStatement(SqlStatement<?> sql, BiFunction<CPreparedStatement, List<Object>, CStatement> delegate) {
        this.sql = sql;
        this.delegate = delegate;
    }

    @Override
    public Iterable<CRow> execute(CPreparedStatement stmt, List<Object> params) throws SQLException {
        return delegate.apply(stmt, params).execute(stmt, params);
    }

    @Override
    public String toNativeSQL() {
        return sql.getSql();
    }

}
