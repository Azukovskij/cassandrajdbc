/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.statement;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.cassandrajdbc.connection.CassandraConnection;
import com.cassandrajdbc.result.CResultSet;
import com.cassandrajdbc.result.CResultSetMetaData;
import com.cassandrajdbc.types.codec.BlobCodec;
import com.cassandrajdbc.types.codec.ByteArrayCodec;
import com.cassandrajdbc.types.codec.ClobCodec;
import com.cassandrajdbc.types.codec.InputStreamCodec;
import com.cassandrajdbc.types.codec.SqlDateCodec;
import com.cassandrajdbc.types.codec.SqlTimeCodec;
import com.cassandrajdbc.types.codec.SqlTimestampCodec;
import com.cassandrajdbc.types.codec.StringReaderCodec;
import com.cassandrajdbc.types.codec.StringStreamCodec;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;


public class CPreparedStatement implements PreparedStatement {
    
    private final CassandraConnection connection;
    
    private final List<String> batchStatements = new ArrayList<>();
    private final List<List<Object>> batchParams = new ArrayList<>();
    
    private String preparedStatement;
    private List<Object> preparedParams = new ArrayList<>();
    
    private boolean closed;
    private boolean closeOnCompletion;
    private int fetchSize = 1000;
    private int queryTimeoutSec = 120000;
    private int limit;

    private ResultSet lastResultSet;
    
    public CPreparedStatement(CassandraConnection connection) {
        this(connection, null);
    }

    public CPreparedStatement(CassandraConnection connection, String sql) {
        this(connection, sql, connection.getSession().getCluster()
            .getConfiguration().getQueryOptions().getFetchSize());
    }

    @VisibleForTesting
    CPreparedStatement(CassandraConnection connection, String sql, int fetchSize) {
        this.connection = connection;
        this.preparedStatement = sql;
        this.fetchSize = fetchSize;
        
        CodecRegistry registry = connection.getSession().getCluster().getConfiguration().getCodecRegistry();
        registry.register(new SqlDateCodec(registry), new SqlTimeCodec(registry), new SqlTimestampCodec(registry),
            new ByteArrayCodec(registry), new InputStreamCodec(registry), new InputStreamCodec(registry), 
            new StringReaderCodec(registry), new BlobCodec(registry), new StringStreamCodec(registry), new ClobCodec(registry));
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

    @Override
    public CassandraConnection getConnection() throws SQLException {
        return connection;
    }

    /*
     * query operations
     */
    public com.datastax.driver.core.ResultSet executeInternal(String sql) {
        return sql == null ? null : connection.getSession().execute(prepareStatement(sql, preparedParams));
    }

    public Statement prepareStatement(String sql, List<Object> params) {
        Statement stmt = params.isEmpty() ?  new SimpleStatement(sql) : new BoundStatement(connection.getSession().prepare(sql))
            .bind(params.toArray(Object[]::new));
        stmt.setReadTimeoutMillis(queryTimeoutSec);
        stmt.setFetchSize(fetchSize);
        return stmt;
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(preparedStatement);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return executeQuery(sql).hasNext();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }
    
    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(preparedStatement);
    }
    
    @Override
    public CResultSet executeQuery(String sql) throws SQLException {
        CResultSet resultSet = null;
        try {
            if (sql == null) {
                throw new SQLException("Null statement.");
            }
//          Matcher matcherExplainPlan = PATTERN_EXPLAIN_PLAN.matcher(sql);
//          if (matcherExplainPlan.matches()) {
//              return explainPlan(matcherExplainPlan.group(1));
//          } 
            resultSet = sql.isBlank() ? new CResultSet(this) :
                new CResultSet(this, getMetadata(sql), executeInternal(sql));
        } catch (DriverException e) {
            throw new SQLException("Cassandra error", e);
        } finally {
            if(lastResultSet != null) {
                lastResultSet.close();
            }
            lastResultSet = resultSet;
            preparedParams.clear();
        }
        return resultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate(preparedStatement);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return executeQuery(sql).hasNext() ? 1 : 0;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }
    
    /*
     * batch operations
     */

    @Override
    public int[] executeBatch() throws SQLException {
        try {
            for(List<Statement> batch : Lists.partition(IntStream.range(0, batchStatements.size())
                    .mapToObj(i -> prepareStatement(batchStatements.get(i), batchParams.get(i)))
                    .collect(Collectors.toList()), 100)) {
                connection.getSession().execute(QueryBuilder.unloggedBatch(batch.toArray(RegularStatement[]::new)));
            }
            return batchStatements.stream().mapToInt(sql -> SUCCESS_NO_INFO).toArray();
        } catch (DriverException e) {
            throw new SQLException("Cassandra error", e);
        } finally {
            if(lastResultSet != null) {
                lastResultSet.close();
            }
            lastResultSet = null;
        }
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        this.batchStatements.add(sql);
        this.batchParams.add(new ArrayList<>(preparedParams));
    }

    @Override
    public void addBatch() throws SQLException {
        if(preparedStatement != null) {
            addBatch(preparedStatement);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        this.batchStatements.clear();
        this.batchParams.clear();
    }
    
    
    /*
     * metadata
     */
    
    @Override
    public int getMaxFieldSize() throws SQLException {
        return Integer.MAX_VALUE; // 2GB
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }
    
    /*
     * query options
     */

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("unmodifyable entity");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_UNKNOWN;
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        this.fetchSize = fetchSize;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return limit;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.limit = max;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeoutSec;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.queryTimeoutSec = seconds;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return closeOnCompletion;
    }
    
    
    /*
     * results
     */

    @Override
    public ResultSet getResultSet() throws SQLException {
        return lastResultSet;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return getMetadata(preparedStatement);
    }

    public CResultSetMetaData getMetadata(String sql) {
        return CResultSetMetaData.Parser.parse(sql, 
            connection.getSession().getCluster().getMetadata());
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
    
    /*
     * statement state 
     */

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }
    

    /*
     * parameter operations
     */

    @Override
    public void clearParameters() throws SQLException {
        preparedParams.clear();
    }
    
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new CParameterMetadata(preparedParams, connection.getSession().getCluster()
            .getConfiguration().getCodecRegistry());
    }
    
    public List<Object> readParams() throws SQLException {
        checkClosed();
        return Collections.unmodifiableList(preparedParams);
    }
    
    
    /*
     * parameter setters
     */
    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber());
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType.getVendorTypeNumber(), scaleOrLength);
    }       
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setParam(parameterIndex, x, JDBCType.valueOf(targetSqlType));
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setParam(parameterIndex, x, JDBCType.valueOf(targetSqlType));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setParam(parameterIndex, x, null);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setString(parameterIndex, x.toString());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParam(parameterIndex, null, JDBCType.valueOf(sqlType));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setParam(parameterIndex, null, JDBCType.valueOf(sqlType));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.TINYINT);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.BLOB);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.SMALLINT);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.INTEGER);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.BIGINT);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.FLOAT);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.DECIMAL);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.VARCHAR);
    }
    

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.DATE);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.TIME);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, new Time((x.getTime() - getOffset(cal, x.getTime()))));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.TIMESTAMP);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setParam(parameterIndex, new Timestamp(x.getTime() - getOffset(cal, x.getTime())), JDBCType.TIMESTAMP);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.BINARY);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setParam(parameterIndex, inputStream, JDBCType.BLOB);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.BLOB);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        setParam(parameterIndex, x, JDBCType.CLOB);
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex, reader);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setParam(parameterIndex, reader, JDBCType.CLOB);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        setParam(parameterIndex, value, JDBCType.NCLOB);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setNCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setParam(parameterIndex, reader, JDBCType.VARCHAR);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setParam(parameterIndex, value, JDBCType.NVARCHAR);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setParam(parameterIndex, toString(x, StandardCharsets.UTF_8), JDBCType.VARCHAR);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setAsciiStream(parameterIndex, x, (int)length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setParam(parameterIndex, toString(x, StandardCharsets.US_ASCII), JDBCType.VARCHAR);
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    private int getOffset(Calendar cal, long time) {
        return cal.getTimeZone().getOffset(time) - TimeZone.getDefault().getOffset(time);
    }

    private void setParam(int parameterIndex, Object value, SQLType type) {
        if(value instanceof Timestamp) {
            value = Timestamp.from(LocalDateTime.ofInstant(((Timestamp) value).toInstant(), ZoneOffset.UTC)
                .atZone(ZoneId.systemDefault()).toInstant());
        }
        IntStream.range(preparedParams.size(), parameterIndex)
            .forEach(i -> preparedParams.add(i, null));
        preparedParams.set(parameterIndex - 1, value);
    }
    
    
    private String toString(InputStream is, Charset charset) {
        try {
            return CharStreams.toString(new InputStreamReader(is, charset));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void checkClosed() throws SQLException {
        if(closed || connection.isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }
    
}
