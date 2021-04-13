/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.stmt;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

import com.cassandrajdbc.statement.CPreparedStatement;
import com.cassandrajdbc.translator.stmt.CStatement.CRow;

public class ExtractingCStatement<T> {
    
    private final CStatement delegate;
    private final Function<Iterable<CRow>, T> extractor;
    
    public ExtractingCStatement(CStatement delegate, Function<Iterable<CRow>, T> extractor) {
        this.delegate = delegate;
        this.extractor = extractor;
    }

    public T extract(CPreparedStatement stmt, List<Object> params) {
        try {
            return extractor.apply(delegate.execute(stmt, params));
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
    
}
