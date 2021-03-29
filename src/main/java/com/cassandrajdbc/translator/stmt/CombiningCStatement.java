/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.stmt;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.function.BinaryOperator;

import com.cassandrajdbc.statement.CPreparedStatement;

public class CombiningCStatement implements CStatement {
    
    private final List<CStatement> children;
    private final BinaryOperator<Iterable<CRow>> combiner;
    
    public CombiningCStatement(List<CStatement> children,
        BinaryOperator<Iterable<CRow>> combiner) {
        this.children = children;
        this.combiner = combiner;
    }

    @Override
    public Iterable<CRow> execute(CPreparedStatement stmt, List<Object> params) throws SQLException {
        return children.stream()
            .map(child -> {
                try {
                    return child.execute(stmt, params);
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            })
            .reduce(combiner)
            .orElseGet(Collections::emptyList);
    }

}
