/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.stmt;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.cassandrajdbc.statement.CPreparedStatement;

public class ChainingCStatement implements CStatement {
    
    private final CStatement delegate;
    private final Function<CRow, CStatement> next;
    
    public ChainingCStatement(CStatement delegate, Function<CRow, CStatement> next) {
        this.delegate = delegate;
        this.next = next;
    }

    @Override
    public Iterable<CRow> execute(CPreparedStatement stmt, List<Object> params) throws SQLException {
        return stream(delegate.execute(stmt, params))
            .flatMap(row -> {
                try {
                    return stream(next.apply(row).execute(stmt, params));
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            })
            .collect(Collectors.toList());
    }
    
    private Stream<CRow> stream(Iterable<CRow> rows) {
        return StreamSupport.stream(rows.spliterator(), false);
    }
    

}
