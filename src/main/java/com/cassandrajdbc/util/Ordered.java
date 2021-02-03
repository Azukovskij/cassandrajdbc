/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.util;

import java.util.Comparator;

public interface Ordered {
    
    static final int DEFAULT_ORDER = 0;
    
    static <T> Comparator<T> comparator() {
        return Comparator.comparing(v -> v instanceof Ordered ? ((Ordered)v).getOrder() : DEFAULT_ORDER);
    }
    
    int getOrder();

}
