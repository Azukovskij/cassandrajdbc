/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.connection;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cassandrajdbc.CassandraURL;
import com.cassandrajdbc.expressions.CqlValue;
import com.cassandrajdbc.statement.CPreparedStatement;
import com.cassandrajdbc.translator.CqlToSqlParser;
import com.cassandrajdbc.translator.SqlToClqTranslator;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

public class CassandraConnection implements Connection {
    
    private final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);
    
    private final Session session;
    private final CassandraURL connectionUrl;

    private String defaultSchema;
    private Properties clientInfo;
    private String catalog;
    private boolean readOnly;

    public CassandraConnection(Session session, CassandraURL connectionUrl) {
        this.session = session;
        this.connectionUrl = connectionUrl;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new CDatabaseMetaData(connectionUrl, this);
    }
    
    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return toSql(sql);
    }

    public String toSql(String sql) {
        try {
            var translated = SqlToClqTranslator.translateToCQL(CqlToSqlParser.parse(sql));
            if(translated != null) {
                logger.debug("Translated query {}", translated);
                return translated.toString()
                    .replace("'" + CqlValue.PARAM + "'", "?");
            }
        } catch (Exception e) {
            logger.trace("SQL parse failed", e);
        }
        logger.debug("CQ statement passthrough {}", sql);
        return sql;
    }
    
    @Override
    public void close() throws SQLException {
        session.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return session.isClosed();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        return catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cassandra does not support transactions");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cassandra does not support transactions");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Cassandra does not support transactions");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Cassandra does not support transactions");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Cassandra does not support transactions");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !session.isClosed();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        clientInfo.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        clientInfo.clear();
        clientInfo.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return clientInfo;
    }

    @Override
    public void setSchema(String defaultSchema) throws SQLException {
        checkClosed();
        this.defaultSchema = defaultSchema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return defaultSchema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        executor.execute(session::close);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new CPreparedStatement(this);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
        throws SQLException {
        return createStatement();
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new CPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
        throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepare call not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepare call not supported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
        int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepare call not supported");
    }
    
    
    public Session getSession() {
        return session;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        checkClosed();
        if (iface.isAssignableFrom(getClass())) {
          return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();
        return iface.isAssignableFrom(getClass());
    }

    private String normalizeSql(String sql) {
        if(sql.startsWith("ALTER TABLE") && sql.contains("ADD ") &&
            !sql.contains("ADD COLUMN")) {
            return sql.replace("ADD ", "ADD COLUMN ");
        }
        return sql;
    }
    
    private void checkClosed() throws SQLException {
        if(session.isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

}
