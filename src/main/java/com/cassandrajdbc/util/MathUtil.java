/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.util;

import java.math.BigDecimal;
import java.util.function.BiFunction;

public class MathUtil {
    
    public static boolean isNumber(Object object) {
        return object == null || object instanceof Number;
    }
    
    public static <T extends Number> Number apply(T left, T right, BiFunction<BigDecimal,BigDecimal,BigDecimal> op) {
        if(left instanceof Integer) {
            return op.apply(number((Integer)left), number((Integer)right))
                .intValue();
        }
        if(left instanceof Long) {
            return op.apply(number((Long)left), number((Long)right))
                .intValue();
        }
        if(left instanceof Float) {
            return op.apply(number((Float)left), number((Float)right))
                .floatValue();
        }
        if(left instanceof Double) {
            return op.apply(number((Double)left), number((Double)right))
                .doubleValue();
        }
        if(left instanceof Byte) {
            return op.apply(number((Byte)left), number((Byte)right))
                .byteValue();
        }
        if(left instanceof Short) {
            return op.apply(number((Short)left), number((Short)right))
                .shortValue();
        }
        if(left instanceof BigDecimal) {
            return op.apply((BigDecimal)left, (BigDecimal)right);
        }
        return op.apply(number(left), number(right)).intValue();
    }

    private static BigDecimal number(Integer val) {
        return val == null ? BigDecimal.ZERO : new BigDecimal(val);
    }
    
    private static BigDecimal number(Long val) {
        return val == null ? BigDecimal.ZERO : new BigDecimal(val);
    }
    
    private static BigDecimal number(Float val) {
        return val == null ? BigDecimal.ZERO : new BigDecimal(val);
    }
    
    private static BigDecimal number(Double val) {
        return val == null ? BigDecimal.ZERO : new BigDecimal(val);
    }
    
    private static BigDecimal number(Byte val) {
        return val == null ? BigDecimal.ZERO : new BigDecimal(val);
    }
    
    private static BigDecimal number(Short val) {
        return val == null ? BigDecimal.ZERO : new BigDecimal(val);
    }
    
    private static BigDecimal number(Number val) {
        return val == null ? BigDecimal.ZERO : new BigDecimal(val.intValue());
    }
}
