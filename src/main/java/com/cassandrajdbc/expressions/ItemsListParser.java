/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ItemsListParser extends ItemsListVisitorAdapter {

    private final List<Stream<Expression>> elements = new ArrayList<Stream<Expression>>();

    ItemsListParser() {}
    
    public static Function<ItemsList, Stream<Stream<Expression>>> instance() {
        return list -> {
            ItemsListParser visitor = new ItemsListParser();
            list.accept(visitor);
            return visitor.elements.stream();
        };
    }
    
    @Override
    public void visit(ExpressionList expressionList) {
        elements.add(expressionList.getExpressions().stream());
    }

    @Override
    public void visit(MultiExpressionList multiExprList) {
        multiExprList.getExprList().forEach(this::visit);
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new UnsupportedOperationException("sub selects are not supported");
    }

}
