/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.test.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.cassandrajdbc.CassandraURL;
import com.cassandrajdbc.connection.CassandraConnection;
import com.cassandrajdbc.types.codec.BlobCodec;
import com.cassandrajdbc.types.codec.ByteArrayCodec;
import com.cassandrajdbc.types.codec.ClobCodec;
import com.cassandrajdbc.types.codec.InputStreamCodec;
import com.cassandrajdbc.types.codec.SqlDateCodec;
import com.cassandrajdbc.types.codec.SqlTimeCodec;
import com.cassandrajdbc.types.codec.SqlTimestampCodec;
import com.cassandrajdbc.types.codec.StringReaderCodec;
import com.cassandrajdbc.types.codec.StringStreamCodec;
import com.cassandrajdbc.types.codec.TypeCastingCodec;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;

public class CassandraTestConnection {
    
    private static boolean configured;
    
    public static CassandraConnection getConnection() {
        configureCodecs();
        String connectionUri = "jdbc:cassandra://" 
                + EmbeddedCassandraServerHelper.getHost() 
                + ":" + EmbeddedCassandraServerHelper.getNativeTransportPort();
        return new CassandraConnection(EmbeddedCassandraServerHelper.getSession(), CassandraURL.create(connectionUri).get());
    }
    

    public static boolean executeSql(String sql, Object... params) throws SQLException {
        PreparedStatement stmt = getConnection().prepareStatement(sql);
        for(int i = 0; i < params.length; i++) {
            stmt.setObject(i + 1, params[i]);
        }
        return stmt.execute();
    }
    

    public static ResultSet executeSqlQuery(String sql, Object... params) throws SQLException {
        PreparedStatement stmt = getConnection().prepareStatement(sql);
        for(int i = 0; i < params.length; i++) {
            stmt.setObject(i + 1, params[i]);
        }
        return stmt.executeQuery(sql);
    }
    
    
    public static void configureCodecs() {
        if(configured) {
            return;
        }
        CodecRegistry registry = EmbeddedCassandraServerHelper.getCluster().getConfiguration().getCodecRegistry();
        registry.register(new TypeCodec[] { new SqlDateCodec(registry), new SqlTimeCodec(registry), new SqlTimestampCodec(registry),
                new ByteArrayCodec(registry), new InputStreamCodec(registry), new InputStreamCodec(registry), 
                new StringReaderCodec(registry), new BlobCodec(registry), new StringStreamCodec(registry), new ClobCodec(registry) });
        TypeCastingCodec.configure(registry);
        configured = true;
    }
    

}
