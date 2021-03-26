/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.types.codec;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.UUID;
import java.util.function.Function;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;

public final class TypeCastingCodec<I,O> extends CodecDelegate<I,O> {

    private final Function<O, I> cast;
    private final Function<I, O> uncast;
    
    public static void configure(CodecRegistry registry) {
        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.uuid()), 
            String.class, UUID::toString, UUID::fromString));
        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.varchar()), 
            UUID.class, UUID::fromString, UUID::toString));
        
        
        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.bigint()), 
            Integer.class, Long::intValue, Integer::longValue));
        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.cint()), 
            Long.class, Integer::longValue, Long::intValue));

        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.decimal()), 
            Byte.class, BigDecimal::byteValue, BigDecimal::valueOf));
        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.decimal()), 
            Short.class, BigDecimal::shortValue, BigDecimal::valueOf));
        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.decimal()), 
            Integer.class, BigDecimal::intValue, BigDecimal::valueOf));
        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.decimal()), 
            Long.class, BigDecimal::longValue, BigDecimal::valueOf));
        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.decimal()), 
            Double.class, BigDecimal::doubleValue, BigDecimal::valueOf));
        
        
        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.date(), com.datastax.driver.core.LocalDate.class), 
            String.class, TypeCastingCodec::fromatDseDate, TypeCastingCodec::parseDseDate));
        registry.register(new TypeCastingCodec<com.datastax.driver.core.LocalDate, String>(registry.codecFor(DataType.varchar()), 
            com.datastax.driver.core.LocalDate.class, TypeCastingCodec::parseDseDate, TypeCastingCodec::fromatDseDate));

        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.timestamp(), Date.class), 
            String.class, TypeCastingCodec::formatJavaDate, TypeCastingCodec::parseJavaDate));
        registry.register(new TypeCastingCodec<>(registry.codecFor(DataType.varchar(), String.class), 
            Date.class, TypeCastingCodec::parseJavaDate, TypeCastingCodec::formatJavaDate));
    }

    private static com.datastax.driver.core.LocalDate parseDseDate(String string) {
        var ld = LocalDate.parse(string);
        return com.datastax.driver.core.LocalDate.fromYearMonthDay(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth());
    }

    private static String fromatDseDate(com.datastax.driver.core.LocalDate date) {
        return LocalDate.of(date.getYear(), date.getMonth(), date.getDay()).toString();
    }
    
    private static String formatJavaDate(Date date) {
        return ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault())
            .truncatedTo(ChronoUnit.MILLIS)
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    private static Date parseJavaDate(String string) {
        return Date.from(ZonedDateTime.parse(string).toInstant());
    }
    

    TypeCastingCodec(TypeCodec<O> delegate, Class<I> output,
        Function<O, I> cast, Function<I, O> uncast) {
        super(delegate, output);
        this.cast = cast;
        this.uncast = uncast;
    }

    @Override
    protected O write(I value) {
        return uncast.apply(value);
    }

    @Override
    protected I read(O value) {
        return cast.apply(value);
    }

}