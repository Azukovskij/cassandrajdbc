/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;

public class AssignmentParser extends ExpressionVisitorAdapter {

    private final Function<Expression, Stream<Object>> valueParser = ValueParser.instance();
    private final List<Assignment> assignments = new ArrayList<>();
    private final String columnName;
    
    public static BiFunction<String, Expression, Stream<Assignment>> instance() {
        return (columnName, expr) -> {
            AssignmentParser incrementVisitor = new AssignmentParser(columnName);
            ValueParser assignVisitor = new ValueParser();
            expr.accept(incrementVisitor);
            expr.accept(assignVisitor);
            return Stream.concat(
                    assignVisitor.getValues()
                        .map(val -> QueryBuilder.set(columnName, val)), 
                    incrementVisitor.assignments.stream()
                );
        };
    }

    AssignmentParser(String columnName) {
        this.columnName = columnName;
    }
    
    @Override
    public void visit(Addition expr) {
        assignments.add(QueryBuilder.incr(columnName(expr.getLeftExpression()), sum(expr.getRightExpression())));
    }
    
    @Override
    public void visit(Subtraction expr) {
        assignments.add(QueryBuilder.decr(columnName(expr.getLeftExpression()), sum(expr.getRightExpression())));
    }
    
    @Override
    public void visit(Division expr) {
        throw new UnsupportedOperationException("cassandra does not support column division");
    }
    
    @Override
    public void visit(Multiplication expr) {
        throw new UnsupportedOperationException("cassandra does not support column multiplication");
    }

    private int sum(Expression val) {
        return valueParser.apply(val)
            .mapToInt(Integer.class::cast)
            .sum();
    }
    
    private String columnName(Expression expr) {
        if(expr instanceof Column && ((Column) expr).getColumnName().equals(columnName)) {
            return columnName;
        }
        throw new UnsupportedOperationException("Column assignment not supported " + expr);
    }

}
