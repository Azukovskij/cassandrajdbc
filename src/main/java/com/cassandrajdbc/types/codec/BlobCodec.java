/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialException;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;

public class BlobCodec extends CodecDelegate<Blob, ByteBuffer> {
    
    private final InputStreamCodec delegate;
    
    public BlobCodec(CodecRegistry registry) {
        super(registry, DataType.blob(), Blob.class, ByteBuffer.class);
        this.delegate = new InputStreamCodec(registry);
    }

    @Override
    protected ByteBuffer write(Blob value) {
        try {
            return delegate.write(((SerialBlob)value).getBinaryStream());
        } catch (SerialException e) {
            throw new IllegalArgumentException("Failed to write binary stream");
        }
    }

    @Override
    protected Blob read(ByteBuffer value) {
        try {
            return new SerialBlob(value.array());
        } catch (SQLException e) {
            throw new IllegalArgumentException("Failed to read binary stream");
        }
    }
    
}

