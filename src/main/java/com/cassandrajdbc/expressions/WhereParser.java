/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.expressions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.stmt.CStatement.CRow;
import com.cassandrajdbc.translator.stmt.ExtractingCStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.BindMarker;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.Iterables;

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
import net.sf.jsqlparser.schema.Table;

/**
 * where (...) 
 * 
 * @author azukovskij
 *
 */
public class WhereParser extends ExpressionVisitorAdapter {

    private final List<Clause> clauses = new ArrayList<>();
    private final List<ExtractingCStatement<Clause>> subselects = new ArrayList<>();
    private final List<Column> bindColumns = new ArrayList<>();
    private final Table table;
    private final ClusterConfiguration config;
    private final Predicate<CRow> predicate;

    public WhereParser(Expression expression, Table table, ClusterConfiguration config) {
        this.table = table;
        this.config = config;
        Optional.ofNullable(expression).ifPresent(e -> e.accept(this));
        this.predicate = new PredicateParser(expression).getPredicate();
    }
    
    @Override
    public void visit(InExpression expr) {
        ColumnMetadata column = column(expr.getLeftExpression())
            .map(this::getColumnMetadata)
            .orElseThrow(() -> new UnsupportedOperationException("Column assignment not supported " + expr));
        ItemListParser parser = new ItemListParser(expr.getRightItemsList(), config);
        Optional.ofNullable(parser.getSubselect())
            .map(subselect -> new ExtractingCStatement<Clause>(subselect, rows -> QueryBuilder.in(column.getName(), 
                Iterables.transform(rows, r -> r.getColumn(0, Object.class)))))  // FIXME convert types
            .ifPresentOrElse(subselects::add, () -> clauses.add(QueryBuilder.in(column.getName(), parser.getValues().stream()
                .flatMap(List::stream)
                .map(ValueParser.instance(column))
                .toArray())));
    }
    
    @Override
    public void visit(OrExpression expr) {
        throw new UnsupportedOperationException("cassandra does not support OR clauses"); // FIXME
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
    
    public TableMetadata getTableMetadata() {
        return config.getTableMetadata(table);
    }
    
    public List<Clause> getClauses() {
        return clauses;
    }
    
    public List<ExtractingCStatement<Clause>> getSubselects() {
        return subselects;
    }
    
    private void doVisit(BinaryExpression expr, BiFunction<String, Object, Clause> matcher) {
        Expression left = expr.getLeftExpression();
        Expression right = expr.getRightExpression();
        if(expr.isNot()) {
            throw new UnsupportedOperationException(); //TODO
        }
        column(left)
            .flatMap(leftCol -> column(right)
                .map(rightCol -> bindClause(leftCol, rightCol, matcher))
                .orElseGet(() -> matchValue(leftCol, right, matcher)))
            .or(() -> column(right)
                .flatMap(rightCol -> matchValue(rightCol, left, matcher)))
            .ifPresent(clauses::add);
    }
    
    private Optional<Clause> matchValue(Column left, Expression value,
            BiFunction<String, Object, Clause> matcher) {
        if(!matchesTable(this.table, left)) {
            return Optional.empty();
        }
        ColumnMetadata column = getColumnMetadata(left);
        return Optional.of(matcher.apply(getColunName(left), ValueParser.instance(column).apply(value)));
    }
    
    private Optional<Clause> bindClause(Column left, Column right,
            BiFunction<String, Object, Clause> matcher) {
        if(matchesTable(this.table, left)) {
            return Optional.of(matcher.apply(getColunName(left), bind(right)));
        }
        if(matchesTable(this.table, right)) {
            return Optional.of(matcher.apply(getColunName(right), bind(left)));
        }
        return Optional.empty();
    }

    private BindMarker bind(Column column) {
        bindColumns.add(column);
        return QueryBuilder.bindMarker();
    }

    private String getColunName(Column col) {
        return getColumnMetadata(col).getName();
    }
    
    private Optional<Column> column(Expression expr) {
        return Optional.ofNullable(expr)
            .filter(Column.class::isInstance)
            .map(Column.class::cast);
    }
    
    public List<Column> getBindColumns() {
        return bindColumns;
    }
    
    public Predicate<CRow> getPredicate() {
        return predicate;
    }
    
    private ColumnMetadata getColumnMetadata(Column col) {
        return config.getColumnMetadata(getTableMetadata(), col.getColumnName());
    }

    private boolean matchesTable(Table stmtTable, Column column) {
        Table table = column.getTable();
        if(table == null || stmtTable == null || table.getFullyQualifiedName().isEmpty()) {
            return true;
        }
        return Optional.ofNullable(stmtTable.getAlias())
            .map(as -> Objects.equals(as.getName(), table.getFullyQualifiedName()))
            .orElseGet(() -> Objects.equals(stmtTable.getFullyQualifiedName(), table.getFullyQualifiedName()));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private class PredicateParser extends ExpressionVisitorAdapter {
        
        private Predicate<CRow> predicate = r -> true;
        
        public PredicateParser(Expression expression) {
            Optional.ofNullable(expression).ifPresent(e -> e.accept(this));
        }
        
        @Override
        public void visit(EqualsTo expr) {
            doVisit(expr, Objects::equals);
        }

        @Override
        public void visit(NotEqualsTo expr) {
            doVisit(expr, (a,b) -> !Objects.equals(a, b));
        }

        @Override
        public void visit(GreaterThan expr) {
            Comparator comp = Comparator.naturalOrder();
            doVisit(expr, (a,b) -> comp.compare(a, b) > 0);
        }

        @Override
        public void visit(GreaterThanEquals expr) {
            Comparator comp = Comparator.naturalOrder();
            doVisit(expr, (a,b) -> comp.compare(a, b) >= 0);
        }

        @Override
        public void visit(MinorThan expr) {
            Comparator comp = Comparator.naturalOrder();
            doVisit(expr, (a,b) -> comp.compare(a, b) < 0);
        }

        @Override
        public void visit(MinorThanEquals expr) {
            Comparator comp = Comparator.naturalOrder();
            doVisit(expr, (a,b) -> comp.compare(a, b) <= 0);
        }

        @Override
        public void visit(LikeExpression expr) {
            doVisit(expr, this::like);
        }

        private boolean like(Object a, Object b) {
            return String.valueOf(a)
                .matches(String.valueOf(b)
                    .replace("_", ".")
                    .replace("%", ".*"));
        }
        
        
        private void doVisit(BinaryExpression expr, BiPredicate<Object, Object> matcher) {
            Expression left = expr.getLeftExpression();
            Expression right = expr.getRightExpression();
            predicate = column(left)
                .map(leftCol -> column(right)
                    .map(rightCol -> matchColumns(leftCol, rightCol, matcher))
                    .orElseGet(() -> matchValue(leftCol, right, matcher)))
                .or(() -> column(right)
                    .map(rightCol -> matchValue(rightCol, left, matcher)))
                .map(predicate::and)
                .orElse(predicate);
        }
        
        private Predicate<CRow> matchColumns(Column left, Column right, BiPredicate<Object, Object> matcher) {
            if(!matchesTable(WhereParser.this.table, left)) {
                return r -> true;
            }
            String leftName = left.getFullyQualifiedName();
            String rightName = right.getFullyQualifiedName();
            return row -> !row.columnNames().contains(leftName) || !row.columnNames().contains(rightName) 
                || matcher.test(row.getColumn(leftName, Object.class), row.getColumn(rightName, Object.class));
        }
        
        private Predicate<CRow> matchValue(Column left, Expression valueExpr, BiPredicate<Object, Object> matcher) {
            if(!matchesTable(WhereParser.this.table, left)) {
                return r -> true;
            }
            String columnName = left.getFullyQualifiedName();
            Object value = ValueParser.instance(getColumnMetadata(left)).apply(valueExpr);
            return row -> !row.columnNames().contains(columnName) 
                || matcher.test(row.getColumn(columnName, Object.class), value);
        }
        
        public Predicate<CRow> getPredicate() {
            return predicate;
        }
        
    }
    
}
