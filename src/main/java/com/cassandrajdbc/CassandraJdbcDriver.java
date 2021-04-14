/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cassandrajdbc.connection.CassandraConnection;
import com.cassandrajdbc.connection.CassandraConnectionFactory;
import com.cassandrajdbc.types.codec.BlobCodec;
import com.cassandrajdbc.types.codec.ByteArrayCodec;
import com.cassandrajdbc.types.codec.ClobCodec;
import com.cassandrajdbc.types.codec.InputStreamCodec;
import com.cassandrajdbc.types.codec.SqlDateCodec;
import com.cassandrajdbc.types.codec.SqlTimeCodec;
import com.cassandrajdbc.types.codec.SqlTimestampCodec;
import com.cassandrajdbc.types.codec.StringBlobCodec;
import com.cassandrajdbc.types.codec.StringMapCodec;
import com.cassandrajdbc.types.codec.StringReaderCodec;
import com.cassandrajdbc.types.codec.StringStreamCodec;
import com.cassandrajdbc.types.codec.TypeCastingCodec;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;


/**
 * 
 * @author azukovskij
 *
 */
public class CassandraJdbcDriver implements Driver {

    private static final String TYPE_CASTING_ENABLED = "cassandrajdbcdriver.string.casting.enabled";
    
    private static final Logger logger = LoggerFactory.getLogger(CassandraJdbcDriver.class);
    
    static {
        try {
            DriverManager.registerDriver(new CassandraJdbcDriver());
        } catch (SQLException ex){
            logger.error("Failed to register jdbc driver", ex);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        var cassandraUrl = CassandraURL.create(url)
            .orElseThrow(() -> new  IllegalArgumentException("Invalid cassandra url " + url + ""));
        var connection = CassandraConnectionFactory.instance()
            .create(cassandraUrl, info);
        configureCodecs(info, connection);
        return new CassandraConnection(connection.getSession(), cassandraUrl);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return CassandraURL.create(url).isPresent();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    private void configureCodecs(Properties info, CassandraConnection connection) {
        CodecRegistry registry = connection.getSession().getCluster().getConfiguration().getCodecRegistry();
        if("true".equalsIgnoreCase(info.getProperty(TYPE_CASTING_ENABLED, "true"))) {
            TypeCastingCodec.configure(registry);
        }
        registry.register(new TypeCodec[] { new SqlDateCodec(registry), new SqlTimeCodec(registry), new SqlTimestampCodec(registry),
            new ByteArrayCodec(registry), new InputStreamCodec(registry), new InputStreamCodec(registry), 
            new StringReaderCodec(registry), new BlobCodec(registry), new StringBlobCodec(registry), new StringMapCodec(registry), new StringStreamCodec(registry), new ClobCodec(registry) });
    }
  

}
