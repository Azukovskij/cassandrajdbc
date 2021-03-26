/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.datastax.driver.core.RegularStatement;

public class Select implements CqlBuilder<net.sf.jsqlparser.statement.select.Select> {

    @Override
    public Class<? extends net.sf.jsqlparser.statement.select.Select> getInputType() {
        return net.sf.jsqlparser.statement.select.Select.class;
    }

    @Override
    public RegularStatement buildCql(net.sf.jsqlparser.statement.select.Select stmt, ClusterConfiguration config) {
        throw new UnsupportedOperationException("selects not supported yet");
    }
    
}
