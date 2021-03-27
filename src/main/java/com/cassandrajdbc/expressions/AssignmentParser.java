/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.expressions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;

/**
 * update set (...)
 * 
 * @author azukovskij
 *
 */
public class AssignmentParser extends ExpressionVisitorAdapter {

    private final Function<Expression, Object> valueParser = ValueParser.instance();
    private final List<Assignment> assignments = new ArrayList<>();
    private final String columnName;
    
    
    public static BiFunction<String, Expression, Stream<Assignment>> instance(TableMetadata table, ClusterConfiguration config) {
        return (columnName, expr) -> {
            ColumnMetadata column = config.getColumnMetadata(table, columnName);
            
            
            AssignmentParser incrementVisitor = new AssignmentParser(columnName);
            expr.accept(incrementVisitor);
            
            return Stream.concat(
                    Stream.of(QueryBuilder.set(column.getName(), ValueParser.instance(column).apply(expr))), 
                    
                    
                    incrementVisitor.assignments.stream() // FIXME review
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
        return (int) valueParser.apply(val);
    }
    
    private String columnName(Expression expr) {
        if(expr instanceof Column && ((Column) expr).getColumnName().equals(columnName)) {
            return columnName;
        }
        throw new UnsupportedOperationException("Column assignment not supported " + expr);
    }


}
