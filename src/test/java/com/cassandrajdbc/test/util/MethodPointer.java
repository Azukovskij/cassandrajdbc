/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.test.util;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.cassandrajdbc.Primitives;

public class MethodPointer {
    
    private final String methodName;
    private final Object[] args;

    public MethodPointer(String methodName, Object... args) {
        this.methodName = methodName;
        this.args = args;
    }
    
    public String getMethodName() {
        return methodName;
    }
    
    public Object[] getArgs() {
        return args;
    }
    
    @Override
    public String toString() {
        return methodName + "(" + Arrays.stream(args)
            .map(arg -> arg == null ? "null" : arg.getClass().getSimpleName())
            .collect(Collectors.joining(", ")) + ")";
    }
    
    
    public Object invoke(Object object) {
        Method[] methods = Arrays.stream(object.getClass().getMethods())
            .filter(m -> m.getName().equals(methodName) && args.length == m.getParameterCount() && IntStream.range(0, args.length)
            .allMatch(i -> args[i] == null || Primitives.boxed(m.getParameterTypes()[i]).isInstance(args[i])))
            .toArray(Method[]::new);
        return invoke(object, 0, methods);
    }

    private Object invoke(Object object, int idx, Method[] methods) throws AssertionError {
        try {
            if(idx >= methods.length) {
                throw new AssertionError("no method " + object.getClass().getSimpleName() + "#" + this);
            }
            Arrays.stream(args).forEach(this::reset);
            return methods[idx].invoke(object, args);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("failed to invoke method", e);
        } catch (IllegalArgumentException e) {
            return invoke(object, idx + 1, methods);
        }
    }
    
    private void reset(Object arg) {
        try {
            arg.getClass().getMethod("reset").invoke(arg); // streams/readers
        } catch (Exception e) {
        }
    }
    
}
