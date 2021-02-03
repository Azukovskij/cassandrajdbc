/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.nio.ByteBuffer;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

abstract class CodecDelegate<I,O> extends TypeCodec<I> {

    private TypeCodec<O> delegate;

    protected CodecDelegate(CodecRegistry registry, DataType cqlType, Class<I> managedClass, Class<O> storedClass) {
        super(cqlType, managedClass);
        this.delegate = registry.codecFor(cqlType, storedClass);
    }
    
    @Override
    public I parse(String value) throws InvalidTypeException {
        return read(delegate.parse(value));
    }

    @Override
    public String format(I value) throws InvalidTypeException {
        return delegate.format(write(value));
    }
        
    @Override
    public ByteBuffer serialize(I value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return delegate.serialize(write(value), protocolVersion);
    }

    @Override
    public I deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return read(delegate.deserialize(bytes, protocolVersion));
    }
    
    protected abstract O write(I value);
    
    protected abstract I read(O value);

}
