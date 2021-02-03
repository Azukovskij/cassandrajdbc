/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.nio.ByteBuffer;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;

public class ByteArrayCodec extends CodecDelegate<byte[], ByteBuffer> {
    
    public ByteArrayCodec(CodecRegistry registry) {
        super(registry, DataType.blob(), byte[].class, ByteBuffer.class);
    }

    @Override
    protected ByteBuffer write(byte[] value) {
        return ByteBuffer.wrap((byte[]) value);
    }

    @Override
    protected byte[] read(ByteBuffer value) {
        return value.array();
    }
    
}
