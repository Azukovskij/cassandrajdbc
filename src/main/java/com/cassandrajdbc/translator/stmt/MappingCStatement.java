/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.stmt;

import java.sql.SQLException;
import java.util.List;
import java.util.function.UnaryOperator;

import com.cassandrajdbc.statement.CPreparedStatement;

public class MappingCStatement implements CStatement {
    
    private final CStatement delegate;
    private final UnaryOperator<Iterable<CRow>> resultMapper;

    public MappingCStatement(CStatement delegate, UnaryOperator<Iterable<CRow>> resultMapper) {
        this.delegate = delegate;
        this.resultMapper = resultMapper;
    }

    @Override
    public Iterable<CRow> execute(CPreparedStatement stmt, List<Object> params) throws SQLException {
        return resultMapper.apply(delegate.execute(stmt, params));
    }

}
