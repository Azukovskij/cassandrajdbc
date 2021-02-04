/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.cassandrajdbc.statement.StatementOptions;
import com.cassandrajdbc.util.SPI;
import com.datastax.driver.core.RegularStatement;

import net.sf.jsqlparser.statement.Statement;

public class SqlToClqTranslator {
    
    private final static Map<Class<?>, CqlBuilder<?>> builders = SPI.loadAll(CqlBuilder.class)
                    .collect(Collectors.toUnmodifiableMap(CqlBuilder::getInputType, Function.identity(), (a,b) -> a));
    
    private static final StatementOptions stmtConfig = new StatementOptions();
    
    
    public static RegularStatement translateToCQL(Statement statement) {
        return Optional.ofNullable(builders.get(statement.getClass()))
            .map(builder -> ((CqlBuilder<Statement>)builder).buildCql(statement, stmtConfig))
            .orElse(null);
    }
    
    public interface CqlBuilder<T extends Statement> {
        
        Class<? extends T> getInputType();
        
        RegularStatement buildCql(T stmt, StatementOptions config);
        
    }

}
