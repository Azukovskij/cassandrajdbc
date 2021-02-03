/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.UncheckedIOException;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.google.common.io.CharStreams;

public class StringReaderCodec extends CodecDelegate<Reader, String> {
    
    public StringReaderCodec(CodecRegistry registry) {
        super(registry, DataType.text(), Reader.class, String.class);
    }

    @Override
    protected String write(Reader value) {
        try {
            return CharStreams.toString(value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected Reader read(String value) {
        return new StringReader(value);
    }
    
}
