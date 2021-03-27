/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.stmt;

import java.sql.SQLException;
import java.util.List;

import com.cassandrajdbc.statement.CPreparedStatement;
import com.datastax.driver.core.Row;

public interface CStatement {
    
    Iterable<Row> execute(CPreparedStatement stmt, List<Object> params) throws SQLException;

    default String toNativeSQL() throws SQLException {
        throw new SQLException(getClass().getSimpleName() + " does support native SQL translation");
    }
    
}
