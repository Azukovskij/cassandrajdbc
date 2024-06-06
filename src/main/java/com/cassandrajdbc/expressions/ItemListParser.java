/* Copyright © 2021 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.expressions;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ItemListParser implements ItemsListVisitor {

    private List<List<Expression>> values;
    
    public static Function<ItemsList, Stream<List<Expression>>> instance() {
        return expr -> {
            ItemListParser visitor = new ItemListParser();
            expr.accept(visitor);
            return visitor.getValues().stream();
        };
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new UnsupportedOperationException("Subselects are not supported");
    }

    @Override
    public void visit(ExpressionList expressionList) {
        values = Arrays.asList(expressionList.getExpressions());
    }

    @Override
    public void visit(NamedExpressionList namedExpressionList) {
        values = Arrays.asList(namedExpressionList.getExpressions());
    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        values = multiExprList.getExprList().stream()
            .map(ExpressionList::getExpressions)
            .collect(Collectors.toList());
    }

    List<List<Expression>> getValues() {
        return values;
    }
    
}