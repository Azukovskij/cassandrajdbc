/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.sql.Date;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;

public class SqlDateCodec extends CodecDelegate<Date, LocalDate> {
    
    public SqlDateCodec(CodecRegistry registry) {
        super(registry, DataType.date(), Date.class, LocalDate.class);
    }

    @Override
    protected LocalDate write(Date value) {
        return LocalDate.fromYearMonthDay(((Date) value).getYear() + 1900, ((Date) value).getMonth(), ((Date) value).getDate());
    }

    @Override
    protected Date read(LocalDate value) {
        return new Date(((LocalDate) value).getYear() - 1900, ((LocalDate) value).getMonth(), ((LocalDate) value).getDay());
    }

}
