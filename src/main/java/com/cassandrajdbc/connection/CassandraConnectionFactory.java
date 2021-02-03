/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.connection;

import java.util.Properties;

import com.cassandrajdbc.CassandraURL;
import com.cassandrajdbc.util.SPI;

public interface CassandraConnectionFactory {
    
    static CassandraConnectionFactory instance() {
        return SPI.load(CassandraConnectionFactory.class)
            .orElseGet(DefaultConnectionFactory::new);
    }
    
    CassandraConnection create(CassandraURL connectionUrl, Properties driverProperties);


}
