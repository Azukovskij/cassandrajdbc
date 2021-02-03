/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class Primitives {
    
    private static final Map<Class<?>, Class<?>> MAPPINGS = ImmutableMap.<Class<?>, Class<?>>builder()
        .put(int.class, Integer.class)
        .put(double.class, Double.class)
        .put(long.class, Long.class)
        .put(float.class, Float.class)
        .put(short.class, Short.class)
        .put(boolean.class, Boolean.class)
        .put(char.class, Character.class)
        .put(byte.class, Byte.class)
        .build();
    
    public static Class<?> boxed(Class<?> type) {
        return type.isPrimitive() ? MAPPINGS.get(type) : type;
    }

}
