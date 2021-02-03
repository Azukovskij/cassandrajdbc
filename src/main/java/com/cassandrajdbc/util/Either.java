/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.util;

import java.util.Optional;

public class Either<A,B> {

    private final A one;
    private B other;
    
    private Either(A one, B other) {
        this.one = one;
        this.other = other;
    }
    
    public static <A, B> Either<A, B> ofOne(A one) {
        return new Either<>(one, null);
    }
    
    public static <A, B> Either<A, B> ofOther(B other) {
        return new Either<>(null, other);
    }
    
    public Optional<A> getOne() {
        return Optional.ofNullable(one);
    }
    
    public Optional<B> getOther() {
        return Optional.ofNullable(other);
    }
    
}