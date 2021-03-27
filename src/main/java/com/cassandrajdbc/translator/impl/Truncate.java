/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class Truncate implements CqlBuilder<net.sf.jsqlparser.statement.truncate.Truncate>{

    @Override
    public Class<? extends net.sf.jsqlparser.statement.truncate.Truncate> getInputType() {
        return net.sf.jsqlparser.statement.truncate.Truncate.class;
    }
    
    @Override
    public CStatement translate(SqlStatement<net.sf.jsqlparser.statement.truncate.Truncate> stmt, ClusterConfiguration config) {
        return new SimpleCStatement(stmt, QueryBuilder.truncate(config.getTableMetadata(stmt.getStatement().getTable())));
    }
    
}
