/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc;

import java.util.Properties;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SocketOptions;

public class CClientInfo {
    
    private enum StandardProperties implements Definition {
        USERNAME("username", null, "Username to use to login to Cassandra hosts"),
        PASSWORD("password", null, "Password corresponding to username property"),
        PORT("port", "9042", "Default port to use to connect to the Cassandra host"),
        CONNECT_TIMEOUT("connectTimeout", "5", "Connection timeout in milliseconds."),
        READ_TIMEOUT("readTimeout", "30", "Per-host read timeout in milliseconds. "
            + "If it is less than or equal to 0, read timeouts are disabled."),
        CONSISTENCY_LEVEL("consistencyLevel", "LOCAL_ONE", ""),
        COMPRESSION("compression", "LZ4", ""),
        FETCH_SIZE("fetchSize", "100", "Fetch size to be use for queries that don't explicitly have a fetch size."),
        ;

        private final String name;
        private final String defaultValue;
        private final String description;

        StandardProperties(String name, String defaultValue, String description) {
            this.name = name;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getDefaultValue() {
            return defaultValue;
        }

        @Override
        public String getDesciption() {
            return description;
        }
        
    }
    
    public CClientInfo(Properties properties) {
        /*
        
        PoolingOptions poolingOptions = new PoolingOptions();
        
        SocketOptions soptions = new SocketOptions();
        soptions.setReadTimeoutMillis(5);
        soptions.setConnectTimeoutMillis(5);
        
        QueryOptions qoptions = new QueryOptions();
        qoptions.setFetchSize(0);
        qoptions.setConsistencyLevel(ConsistencyLevel.ONE);
        
        
        Cluster.builder()
            .addContactPoint("")
            .withCredentials("", "")
            .withSocketOptions(soptions)
            .withPoolingOptions(poolingOptions)
            .withCompression(Compression.LZ4)
            .withQueryOptions(qoptions)
            ;
        
        */
    }
    
    public static Definition[] desribe() {
        return StandardProperties.values();
    }

    
    public static interface Definition {
        String getName();
        String getDefaultValue();
        String getDesciption();
    }
    
    
}
