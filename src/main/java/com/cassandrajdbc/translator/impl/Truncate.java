/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import com.cassandrajdbc.statement.StatementOptions;
import com.cassandrajdbc.statement.StatementOptions.Collation;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class Truncate implements CqlBuilder<net.sf.jsqlparser.statement.truncate.Truncate>{

    @Override
    public Class<? extends net.sf.jsqlparser.statement.truncate.Truncate> getInputType() {
        return net.sf.jsqlparser.statement.truncate.Truncate.class;
    }

    @Override
    public RegularStatement buildCql(net.sf.jsqlparser.statement.truncate.Truncate stmt,
        StatementOptions config) {
        return QueryBuilder.truncate(stmt.getTable().getSchemaName(), escape(stmt.getTable().getName(), config));
    }

    private String escape(String value, StatementOptions config) {
        return config.getCollation() == Collation.CASE_SENSITIVE ? "\"" + value + "\"" : value;
    }
    
}
