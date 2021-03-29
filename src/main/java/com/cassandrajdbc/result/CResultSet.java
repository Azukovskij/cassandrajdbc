/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.result;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import com.cassandrajdbc.translator.stmt.CStatement.CRow;
import com.cassandrajdbc.types.SerialNClob;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.google.common.collect.Iterators;

public class CResultSet implements ResultSet {

    private final CResultSetMetaData metadata;
    private final Statement statement;

    private int direction = ResultSet.FETCH_FORWARD;
    
    private Object lastColumn;
    private Row currentRow;
    private Iterable<Row> rows;
    private Iterator<Row> iterator;
    
    public CResultSet(Statement statement) {
        this.statement = statement;
        this.metadata = new CResultSetMetaData(null, null, new String[0], new DataType[0]);
        this.rows = Collections.emptyList();
        this.iterator = iterator(rows);
    }

    public CResultSet(Statement statement, CResultSetMetaData metadata, Iterator<CRow> results) throws SQLException {
        this.statement = statement;
        this.metadata = metadata;
        this.rows = () -> new ResultSetIterator(results);
        this.iterator = iterator(this.rows);
    }
    
    public CResultSet(Statement statement, CResultSetMetaData metadata, Iterable<Row> rows) {
        this.statement = statement;
        this.metadata = metadata;
        this.rows = rows;
        this.iterator = iterator(rows);
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
          return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }
    
    @Override
    public void close() throws SQLException {
        rows = null;
        iterator = iterator(Collections.emptyList());
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }
    
    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return Objects.requireNonNull(statement, "statement is null");
    }
    
    @Override
    public int getType() throws SQLException {
        return getStatement().getResultSetType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return getStatement().getResultSetConcurrency();
    }
    
    @Override
    public int getHoldability() throws SQLException {
        return getStatement().getResultSetHoldability();
    }
    
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return metadata.findColumn(columnLabel);
    }
    
    @Override
    public void setFetchSize(int rows) throws SQLException {
        getStatement().setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return getStatement().getFetchSize();
    }

    
    
    /*
     * row iteration 
     */
    
    @Override
    public int getRow() throws SQLException {
        return currentRow == null ? 0 : currentRow.index;
    }

    @Override
    public boolean next() throws SQLException {
        if(direction == ResultSet.FETCH_REVERSE) {
            return previousElement();
        }
        return nextElement();
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }
    
    @Override
    public boolean previous() throws SQLException {
        if(direction == ResultSet.FETCH_REVERSE) {
            return nextElement();
        }
        return previousElement();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return lastColumn != null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentRow == null;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return currentRow != null && currentRow.index == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return currentRow != null && !hasNext();
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        iterator = iterator(rows);
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        last();
        next();
    }

    @Override
    public boolean first() throws SQLException {
        beforeFirst();
        return next();
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        currentRow = Iterators.getLast(iterator, null);
        return currentRow != null;
    }
    
    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        if(direction == ResultSet.FETCH_REVERSE) {
            return absoluteElem(-row);
        }
        return absoluteElem(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        if(direction == ResultSet.FETCH_REVERSE) {
            return relativeElem(-rows);
        }
        return relativeElem(rows);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.direction = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return direction;
    }
    
    public Iterator<Row> iterator() {
        return iterator;
    }

    private boolean nextElement() {
        if (hasNext()) {
            currentRow = iterator.next();
            return true;
        }
        return false;
    }

    private boolean previousElement() {
        if (iterator instanceof ListIterator && ((ListIterator<Row>) iterator).hasPrevious()) {
            currentRow = ((ListIterator<Row>) iterator).previous();
            return true;
        }
        return false;
    }

    private boolean absoluteElem(int row) throws SQLException {
        if(row > 0) {
            iterator = iterator(rows);
        } else {
            Iterators.getLast(iterator, null);
        }
        return relative(row);
    }

    private boolean relativeElem(int rows) throws SQLFeatureNotSupportedException {
        if(rows > 0) {
           currentRow = IntStream.range(0, rows)
               .mapToObj(i -> hasNext() ? iterator.next() : null)
               .reduce(null, (a,b) -> b);
           return currentRow != null;
        }
        if(!(iterator instanceof ListIterator)) {
            throw new SQLFeatureNotSupportedException("forward only iterator");
        }
        currentRow = IntStream.range(0, -rows)
            .mapToObj(i -> ((ListIterator<Row>)iterator).hasPrevious() ? ((ListIterator<Row>)iterator).previous() : null)
            .reduce(null, (a,b) -> b);
        return currentRow != null;
    }
    
    
    /*
     * database value changes 
     */

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
    
    
    /*
     * column getters
     */

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(columnIndex, Object.class);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }
    
    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if(currentRow == null) {
            return null;
        }
        Object object = currentRow.values.apply(columnIndex - 1, type);
        if(object != null && !type.isInstance(object)) {
            throw new SQLException("Column is not of expected type got " + object.getClass() + " expecting " + type);
        }
        if(object instanceof LocalDate && type.isAssignableFrom(Date.class)) {
            return (T) getDate(columnIndex);
        }
        if(object instanceof java.util.Date && !(object instanceof Timestamp) && type.isAssignableFrom(Timestamp.class)) {
            return (T) getTimestamp(columnIndex);
        }
        return (T) object;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }
    
    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getObject(columnIndex, BigDecimal.class);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getObject(columnIndex, BigDecimal.class);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getObject(columnIndex, Byte.class);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getObject(columnIndex, Short.class);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getObject(columnIndex, Integer.class);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getObject(columnIndex, Long.class);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getObject(columnIndex, Float.class);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getObject(columnIndex, Double.class);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }
    
    @Override
    public String getString(int columnIndex) throws SQLException {
        return getObject(columnIndex, String.class);
    }
    
    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getObject(columnIndex, Boolean.class);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        try {
            return new URL(getString(columnIndex));
        } catch (MalformedURLException e) {
            throw new SQLException("Column is not URL");
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return getObject(columnIndex, byte[].class);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return getObject(columnIndex, Blob.class);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel), Blob.class);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getObject(columnIndex, InputStream.class);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return getNClob(columnIndex);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return new SerialNClob(getObject(columnIndex, String.class).toCharArray());
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return getNCharacterStream(columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return new StringReader(getString(columnIndex));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }


    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getObject(columnIndex, Date.class);
    } 

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }   

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getObject(columnIndex, Time.class);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        Time time = getTime(columnIndex);
        return new Time(time.getTime() + getOffset(cal, time.getTime()));
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getObject(columnIndex, Timestamp.class);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        Timestamp ts = getTimestamp(columnIndex);
        return new Timestamp(ts.getTime() + getOffset(cal, ts.getTime()));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }
    


    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
    
    
    /*
     * column setters
     */

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        updateObject(columnIndex, null);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        updateObject(findColumn(columnLabel), null);
    }
    
    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }
    
    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateNString(int columnIndex, String x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateNString(String columnLabel, String x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        updateObject(columnIndex, x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        updateObject(columnIndex, x);
    }
    
    
    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
    public Iterable<Row> toRows() {
        return rows;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return rows != null;
    }


    private Iterator<Row> iterator(Iterable<Row> rows) {
        return rows instanceof List ? ((List<Row>)rows).listIterator() : rows.iterator();
    }
    
    private void checkClosed() throws SQLException {
        if(rows == null) {
            throw new SQLException("ResultSet is closed");
        }
    }
    
    private int getOffset(Calendar cal, long time) {
        return cal.getTimeZone().getOffset(time) - TimeZone.getDefault().getOffset(time);
    }
    
    /**
     * 
     * @author azukovskij
     *
     */
    public static class Row {
        
        private final int index;
        private final BiFunction<Integer, Class<?>, Object> values;
        
        public Row(int index, Object[] columns) {
            this.index = index + 1;
            this.values = (i,t) -> columns[i];
        }

        Row(int index, CRow row) {
            this.index = index + 1;
            this.values = (i,t) -> row.getColumn(i, t);
        }
        
        @SuppressWarnings("unchecked")
        public <T> T getColumn(int idx, Class<T> type) {
            Object res = values.apply(idx, type);
            if(res instanceof LocalDate && type.isAssignableFrom(Date.class)) {
                return (T) values.apply(idx, Date.class);
            }
            if(res instanceof java.util.Date && !(res instanceof Timestamp) && type.isAssignableFrom(Timestamp.class)) {
                return (T) values.apply(idx, Timestamp.class);
            }
            return (T) res;
        }

    }
    
    private static class ResultSetIterator implements Iterator<Row> {
        
        private final Iterator<CRow> delegate;
        private final AtomicInteger index = new AtomicInteger();

        public ResultSetIterator(Iterator<CRow> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public Row next() {
            return new Row(index.getAndIncrement(), delegate.next());
        }
        
    }

}
