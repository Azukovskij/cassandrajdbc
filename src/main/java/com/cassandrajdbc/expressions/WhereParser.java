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
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

/**
 * where (...) 
 * 
 * @author azukovskij
 *
 */
public class WhereParser extends ExpressionVisitorAdapter {

    private final List<Clause> clauses = new ArrayList<>();
    private final ClusterConfiguration config;
    private final TableMetadata tableMetadata;

    WhereParser(TableMetadata tableMetadata, ClusterConfiguration config) {
        this.tableMetadata = tableMetadata;
        this.config = config;
    }
    
    public static Function<Expression, Stream<Clause>> instance(TableMetadata table, ClusterConfiguration config) {
        return list -> {
            if(list == null) {
                return Stream.empty();
            }
            WhereParser visitor = new WhereParser(table, config);
            list.accept(visitor);
            return visitor.clauses.stream();
        };
    }

    @Override
    public void visit(InExpression expr) {
        ColumnMetadata column = resolveColumn(expr.getLeftExpression());
        clauses.add(QueryBuilder.in(column.getName(), ItemListParser.instance().apply(expr.getRightItemsList())
            .flatMap(List::stream)
            .map(ValueParser.instance(column))
            .toArray()));
    }
    
    @Override
    public void visit(OrExpression expr) {
        throw new UnsupportedOperationException("cassandra does not support OR clauses");
    }

    @Override
    public void visit(EqualsTo expr) {
        doVisit(expr, QueryBuilder::eq);
    }

    @Override
    public void visit(NotEqualsTo expr) {
        doVisit(expr, QueryBuilder::ne);
    }

    @Override
    public void visit(GreaterThan expr) {
        doVisit(expr, QueryBuilder::gt);
    }

    @Override
    public void visit(GreaterThanEquals expr) {
        doVisit(expr, QueryBuilder::gte);
    }

    @Override
    public void visit(MinorThan expr) {
        doVisit(expr, QueryBuilder::lt);
    }

    @Override
    public void visit(MinorThanEquals expr) {
        doVisit(expr, QueryBuilder::lte);
    }

    @Override
    public void visit(LikeExpression expr) {
        if(expr.isNot()) {
            throw new UnsupportedOperationException();
        }
        doVisit(expr, QueryBuilder::like);
    }

    private void doVisit(BinaryExpression expr, BiFunction<String, Object, Clause> matcher) {
        ColumnMetadata column = resolveColumn(expr.getLeftExpression());
        clauses.add(matcher.apply(column.getName(), 
            ValueParser.instance(column).apply(expr.getRightExpression())));
    }
    
    private ColumnMetadata resolveColumn(Expression expr) {
        if(expr instanceof Column) {
            String columnName = ((Column) expr).getColumnName();
            return config.getColumnMetadata(tableMetadata, columnName);
        }
        throw new UnsupportedOperationException("Column assignment not supported " + expr);
    }
    
}
