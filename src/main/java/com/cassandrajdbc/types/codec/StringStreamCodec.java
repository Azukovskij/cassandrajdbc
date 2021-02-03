/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.io.CharStreams;

public class StringStreamCodec extends CodecDelegate<InputStream, String> {
    
    public StringStreamCodec(CodecRegistry registry) {
        super(registry, DataType.text(), InputStream.class, String.class);
    }

    @Override
    protected String write(InputStream value) {
        try {
            return CharStreams.toString(new InputStreamReader(value, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected InputStream read(String value) {
        return new ByteBufferBackedInputStream(ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
    }
    
}