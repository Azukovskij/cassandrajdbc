/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.sql.Time;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;

public class SqlTimeCodec extends CodecDelegate<Time, Long> {
    
    public SqlTimeCodec(CodecRegistry registry) {
        super(registry, DataType.time(), Time.class, Long.class);
    }

    @Override
    protected Long write(Time value) {
        return (((Time) value).getTime() * 1000000) % TimeUnit.DAYS.toNanos(1);
    }

    @Override
    protected Time read(Long value) {
        return new Time((Long)value / 1000000);
    }
    
}
