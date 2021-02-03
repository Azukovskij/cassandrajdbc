/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.sql.Clob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialException;

import com.cassandrajdbc.types.SerialNClob;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;

public class ClobCodec extends CodecDelegate<Clob, String> {
    
    private final StringReaderCodec delegate;
    
    public ClobCodec(CodecRegistry registry) {
        super(registry, DataType.text(), Clob.class, String.class);
        this.delegate = new StringReaderCodec(registry);
    }

    @Override
    protected String write(Clob value) {
        try {
            return delegate.write(((SerialClob)value).getCharacterStream());
        } catch (SerialException e) {
            throw new IllegalArgumentException("Failed to write char stream");
        }
    }

    @Override
    protected Clob read(String value) {
        try {
            return new SerialNClob(value.toCharArray());
        } catch (SQLException e) {
            throw new IllegalArgumentException("Failed to read binary stream");
        }
    }
    
}
