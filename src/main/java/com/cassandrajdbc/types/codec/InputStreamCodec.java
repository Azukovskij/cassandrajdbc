/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

public class InputStreamCodec extends CodecDelegate<InputStream, ByteBuffer> {
    
    public InputStreamCodec(CodecRegistry registry) {
        super(registry, DataType.blob(), InputStream.class, ByteBuffer.class);
    }

    @Override
    protected ByteBuffer write(InputStream value) {
        try {
            return ByteBuffer.wrap(value.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected InputStream read(ByteBuffer value) {
        return new ByteBufferBackedInputStream(value);
    }
    
}