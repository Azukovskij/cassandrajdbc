/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.util;

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Stream;

public final class SPI {
    
    private SPI() {}
    
    public static <T> Optional<T> load(Class<T> type) {
        return loadAll(type)
            .findFirst();
    }
    
    public static <T> Stream<T> loadAll(Class<T> type) {
        return ServiceLoader.load(type).stream()
            .map(Provider::get)
            .sorted(Ordered.comparator());
    }
    
}
