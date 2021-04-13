/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.stmt;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import com.cassandrajdbc.statement.CPreparedStatement;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.collect.Iterables;

public class NativeCStatement implements CStatement {

    private final String nativeSQL;

    public NativeCStatement(String nativeSQL) {
        this.nativeSQL = nativeSQL;
    }

    @Override
    public Iterable<CRow> execute(CPreparedStatement stmt, List<Object> params) throws SQLException {
        Session session = stmt.getConnection().getSession();
        com.datastax.driver.core.Statement prepared = params.isEmpty() 
                ? new SimpleStatement(nativeSQL)
                : session.prepare(nativeSQL)
                    .bind(params.toArray(Object[]::new));
        prepared.setFetchSize(stmt.getFetchSize());
        prepared.setReadTimeoutMillis(stmt.getQueryTimeout());
        return toCRow(session.execute(prepared));
    }

    private Iterable<CRow> toCRow(Iterable<Row> rows) {
        return Iterables.transform(rows, row -> 
            new CRow(getColumnNames(row), (i,t) -> Object.class.equals(t) ? row.getObject(i) : row.get(i, t)));
    }
    
    private List<String> getColumnNames(Row row) {
        return row.getColumnDefinitions().asList().stream()
            .map(Definition::getName)
            .collect(Collectors.toList());
    }
    
    @Override
    public String toNativeSQL() throws SQLException {
        return nativeSQL;
    }

}
