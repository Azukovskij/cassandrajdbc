/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.test.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import com.google.common.base.Function;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

/**
 * 
 * @author azukovskij
 *
 * @param <T>
 */
public class BinaryMatcher<T> extends BaseMatcher<T> {
    
    private final byte[] expected;
    private final Function<T, byte[]> mapper;
    
    public static Matcher<InputStream> streamEqualTo(byte[] expected) {
        return new BinaryMatcher<InputStream>(expected, is -> {
            try {
                return ByteStreams.toByteArray(is);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
    
    public static Matcher<Reader> readerEqualTo(byte[] expected) {
        return new BinaryMatcher<Reader>(expected, reader -> {
            try {
                return CharStreams.toString(reader).getBytes();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private BinaryMatcher(byte[] expected, Function<T, byte[]> mapper) {
        this.expected = expected;
        this.mapper = mapper;
    }

    @Override 
    public boolean matches(Object item) {
        byte[] actual = mapper.apply((T) item);
        return Arrays.equals(actual, this.expected);
    }

    @Override 
    public void describeTo(Description description) {
        description.appendText("InputStream containing ").appendValue(expected);
    }

}
