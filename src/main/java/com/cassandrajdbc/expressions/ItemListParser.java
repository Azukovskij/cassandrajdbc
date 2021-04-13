/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.expressions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.impl.Select.SelectVisitorImpl;
import com.cassandrajdbc.translator.stmt.CStatement;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ItemListParser implements ItemsListVisitor {

    private List<List<Expression>> values = Collections.emptyList();
    private ClusterConfiguration config;
    private CStatement subselect;

    public ItemListParser(ItemsList items, ClusterConfiguration config) {
        this.config = config;
        items.accept(this);
    }
    
    public static Function<ItemsList, Stream<List<Expression>>> instance(ClusterConfiguration config) {
        return expr -> {
            return new ItemListParser(expr, config).getValues().stream();
        };
    }

    @Override
    public void visit(SubSelect subSelect) {
        checkNullOrEmpty(subSelect.getAlias());
        checkNullOrEmpty(subSelect.getWithItemsList());
        this.subselect = SelectVisitorImpl.visit(null, subSelect.getSelectBody(), config);
    }

    @Override
    public void visit(ExpressionList expressionList) {
        values = Arrays.asList(expressionList.getExpressions());
    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        values = multiExprList.getExprList().stream()
            .map(ExpressionList::getExpressions)
            .collect(Collectors.toList());
    }

    public List<List<Expression>> getValues() {
        return values;
    }
    
    public CStatement getSubselect() {
        return subselect;
    }
    
    private static void checkNullOrEmpty(Object object) {
        if(object == null || "".equals(object)) {
            return;
        } 
        if(object instanceof Iterable && 
            !((Iterable<?>)object).iterator().hasNext()) {
            return;
        }
        unsupported();
    }
    
    private static <T> T unsupported() {
        throw new UnsupportedOperationException();
    }
    
}