/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;

public class StringBlobCodec extends CodecDelegate<String, ByteBuffer> {
    
    public StringBlobCodec(CodecRegistry registry) {
        super(registry, DataType.blob(), String.class, ByteBuffer.class);
    }

    @Override
    protected ByteBuffer write(String value) {
        return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected String read(ByteBuffer bytes) {
        return new String(bytes.array(), StandardCharsets.UTF_8);
    }
    
}