/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.stmt;

import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cassandrajdbc.statement.CPreparedStatement;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.QueryValidationException;

public class SimpleCStatement implements CStatement {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleCStatement.class);
    private SqlStatement<?> sql;
    private RegularStatement cql;
    
    
    public SimpleCStatement(SqlStatement<?> sql, RegularStatement cql) {
        this.sql = sql;
        this.cql = cql;
    }

    @Override
    public ResultSet execute(CPreparedStatement stmt, List<Object> params) throws SQLException {
        Session session = stmt.getConnection().getSession();
        try {
            return session.execute(params.isEmpty() 
                ? cql 
                : stmt.getConnection().getSession().prepare(cql).bind(params.toArray(Object[]::new)));
        } catch (QueryValidationException | UnsupportedOperationException | IllegalStateException e) {
            if(sql.getSql() == null) {
                throw e;
            }
            logger.trace("CQL failed, falling back to native sql", e);
            return session.execute(params.isEmpty() 
                ? new SimpleStatement(sql.getSql()) 
                : stmt.getConnection().getSession().prepare(sql.getSql()).bind(params.toArray(Object[]::new)));
        }
    }

    @Override
    public String toNativeSQL() {
        return cql.toString();
    }

}
