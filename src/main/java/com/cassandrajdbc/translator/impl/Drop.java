/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import com.cassandrajdbc.statement.StatementOptions;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

import net.sf.jsqlparser.schema.Table;

public class Drop implements CqlBuilder<net.sf.jsqlparser.statement.drop.Drop> {

    @Override
    public Class<? extends net.sf.jsqlparser.statement.drop.Drop> getInputType() {
        return net.sf.jsqlparser.statement.drop.Drop.class;
    }

    @Override
    public RegularStatement buildCql(net.sf.jsqlparser.statement.drop.Drop stmt, ClusterConfiguration config) {
        Table item = stmt.getName();
        switch (stmt.getType()) {
            case "TABLE":
                com.datastax.driver.core.schemabuilder.Drop res = SchemaBuilder.dropTable(item.getSchemaName(), item.getName());
                return stmt.isIfExists() ? res.ifExists() : res;
            default:
                throw new UnsupportedOperationException("Dropping " + stmt.getType() + " not supported");
        }
    }

}
