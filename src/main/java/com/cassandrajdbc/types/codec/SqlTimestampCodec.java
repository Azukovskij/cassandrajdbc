/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;

public class SqlTimestampCodec extends CodecDelegate<Timestamp, Date> {
    
    public SqlTimestampCodec(CodecRegistry registry) {
        super(registry, DataType.timestamp(), Timestamp.class, Date.class);
    }

    @Override
    protected Date write(Timestamp value) {
        return Date.from(LocalDateTime.ofInstant(((Timestamp) value).toInstant(), ZoneOffset.UTC)
            .atZone(ZoneId.systemDefault()).toInstant());
    }

    @Override
    protected Timestamp read(Date value) {
        return Timestamp.from(LocalDateTime.ofInstant(value.toInstant(), ZoneOffset.systemDefault())
            .atZone(ZoneOffset.UTC).toInstant());
    }
    
}
