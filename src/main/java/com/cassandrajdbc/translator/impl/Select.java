/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cassandrajdbc.expressions.ValueParser;
import com.cassandrajdbc.expressions.WhereParser;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.CStatement.CRow;
import com.cassandrajdbc.translator.stmt.CombiningCStatement;
import com.cassandrajdbc.translator.stmt.MappingCStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.ExceptOp;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.IntersectOp;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.MinusOp;
import net.sf.jsqlparser.statement.select.OrderByElement.NullOrdering;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.UnionOp;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;

public class Select implements CqlBuilder<net.sf.jsqlparser.statement.select.Select> {
    
    private static final Logger logger = LoggerFactory.getLogger(Select.class);

    @Override
    public Class<? extends net.sf.jsqlparser.statement.select.Select> getInputType() {
        return net.sf.jsqlparser.statement.select.Select.class;
    }
    
    @Override
    public CStatement translate(SqlStatement<net.sf.jsqlparser.statement.select.Select> stmt, ClusterConfiguration config) {
        SelectVisitorImpl visitor = new SelectVisitorImpl(stmt, config);
        stmt.getStatement().getSelectBody().accept(visitor);
        return visitor.getResult();
    }
    
    private static class SelectVisitorImpl implements SelectVisitor {
        
        private final ClusterConfiguration config;
        private SqlStatement<?> sql;
        private CStatement result;
        
        public static CStatement visit(SqlStatement<?> sql, SelectBody body, ClusterConfiguration config) {
            SelectVisitorImpl visitor = new SelectVisitorImpl(sql, config);
            body.accept(visitor);
            return visitor.getResult();
        }

        public SelectVisitorImpl(SqlStatement<?> sql, ClusterConfiguration config) {
            this.sql = sql;
            this.config = config;
        }

        @Override
        public void visit(PlainSelect plainSelect) {
            result = parse(plainSelect);
        }

        private CStatement parse(PlainSelect plainSelect) {
            checkNullOrEmpty(plainSelect.getIntoTables());
            checkNullOrEmpty(plainSelect.getJoins());
            checkNullOrEmpty(plainSelect.getGroupByColumnReferences());
            checkNullOrEmpty(plainSelect.getHaving());
            checkNullOrEmpty(plainSelect.getFetch());
            checkNullOrEmpty(plainSelect.getSkip());
            checkNullOrEmpty(plainSelect.getFirst());
            checkNullOrEmpty(plainSelect.getTop());
            
            FromItemVisitorImpl fromVisitor = new FromItemVisitorImpl(sql, plainSelect, config);
            plainSelect.getFromItem().accept(fromVisitor);
            return fromVisitor.getResult();
        }

        @Override
        public void visit(SetOperationList setOpList) {
            checkNullOrEmpty(setOpList.getOffset());
            checkNullOrEmpty(setOpList.getLimit());
            
            Queue<SetOperation> operations = new ArrayDeque<>(setOpList.getOperations());
            List<CStatement> selects = setOpList.getSelects().stream()
                .map(select -> SelectVisitorImpl.visit(sql, select, config))
                .collect(Collectors.toList());
            result = new CombiningCStatement(selects, (a,b) -> {
                SetOperation op = operations.poll();
                if(op instanceof UnionOp) {
                    return ((UnionOp) op).isDistinct() ? unsupported() : Iterables.concat(a, b);
                }
                if(op instanceof IntersectOp) {
                    logger.info("Potentially slow INTERSECT in query {} performing full table scan on right side of intersection", sql);
                    return Iterables.filter(a, Sets.newHashSet(b)::contains);
                }
                if(op instanceof ExceptOp || op instanceof MinusOp) {
                    logger.info("Potentially slow EXCEPT in query {} performing full table scan on right side of intersection", sql);
                    var other = Sets.newHashSet(b);
                    return Iterables.filter(a, r -> !other.contains(r));
                }
                return unsupported();
            });
        }

        
        @Override
        public void visit(WithItem withItem) {
            unsupported();
        }
        
        public CStatement getResult() {
            return result;
        }

        
    }
    
    private static class FromItemVisitorImpl implements FromItemVisitor {
        
        private final PlainSelect select;
        private final ClusterConfiguration config;
        private SqlStatement<?> sql;
        private CStatement result;

        public FromItemVisitorImpl(SqlStatement<?> sql, PlainSelect select, ClusterConfiguration config) {
            this.sql = sql;
            this.select = select;
            this.config = config;
        }

        @Override
        public void visit(Table table) {
            TableMetadata tableMetadata = config.getTableMetadata(table);
            // selection
            Selection selection = QueryBuilder.select();
            List<String> columns = select.getSelectItems().stream()
                .flatMap(item -> selectItem(selection, item, tableMetadata))
                .collect(Collectors.toList());
        
            // from/where
            com.datastax.driver.core.querybuilder.Select from = selection.from(tableMetadata).allowFiltering();
            WhereParser.instance(tableMetadata, config).apply(select.getWhere()).forEach(from::where);
            result = new SimpleCStatement(sql, from);
            
            // distinct
            if(select.getDistinct() != null) {
                logger.info("Potentially slow DISTINCT in query {} fetching all results to perform distinct", sql);
                Set<Object> distinct = Sets.newConcurrentHashSet();
                int[] indexes = buildColumnIndexes(columns, Optional.ofNullable(select.getDistinct().getOnSelectItems()).stream()
                    .flatMap(List::stream).map(SelectionItem::new).collect(Collectors.toList()));
                result = new MappingCStatement(result, res -> Iterables.filter(res, row -> {
                    if(indexes.length == 0) {
                        return distinct.add(row);
                    }
                    return distinct.add(Arrays.stream(indexes).mapToObj(i -> row.getColumn(i, Object.class))
                        .collect(Collectors.toList()));
                }));
            }
            
            // order by
            if(select.getOrderByElements() != null) {
                Optional<Comparator<Object>> sortBy = select.getOrderByElements().stream()
                    .flatMap(e -> Optional.ofNullable(e.getExpression()).stream().map(SelectionItem::new)
                        .map(item -> columns.indexOf(item.identity().get()))
                        .map(i -> Comparator.comparing(r -> (Comparable)((CRow)r).getColumn(i, Object.class)))
                        .map(c -> e.isAsc() ? c : c.reversed())
                        .map(c -> e.getNullOrdering() == NullOrdering.NULLS_FIRST ? Comparator.nullsFirst(c) : Comparator.nullsLast(c)))
                        .reduce(Comparator::thenComparing);
                result = new MappingCStatement(result, res -> {
                    if(sortBy.isEmpty()) {
                        return res;
                    }
                    logger.info("Potentially slow ORDER BY in query {} fetching all results to perform distinct", sql);
                    return () -> StreamSupport.stream(res.spliterator(), false)
                        .sorted(sortBy.get())
                        .iterator();
                });
            }
            
            // offset + limit
            if(select.getOffset() != null) {
                result = new MappingCStatement(result, res -> Iterables.skip(res, (int)select.getOffset().getOffset()));
            }
            if(select.getLimit() != null) {
                result = new MappingCStatement(result, res -> Iterables.limit(res, ((Long)select.getLimit().getRowCount()).intValue()));
            }
        }

        private int[] buildColumnIndexes(List<String> columns, List<SelectionItem> distinctOn) {
            if(distinctOn.isEmpty() || distinctOn.stream().anyMatch(i -> i.identity().isEmpty())) {
                return new int[0];
            }
            return distinctOn.stream()
                .mapToInt(item -> columns.indexOf(item.identity().get()))
                .toArray();
        }
        
        
        private Stream<String> selectItem(Selection selection, SelectItem select, TableMetadata table) {
            SelectionItem visitor = new SelectionItem(select);

            // select *
            if(visitor.isAllColumns()) {
                selection.all();
                return table.getColumns().stream()
                    .map(ColumnMetadata::getName);
            }

            // select col,col2
            Optional.ofNullable(visitor.getColumn())
                .map(col -> selection.column(getColumnName(table, col)))
                // select 'aa'
                .or(() -> Optional.of(visitor.getValue())
                    .map(value -> {
                        return unsupported();
                    }))
                // select funct()
                .or(() -> Optional.ofNullable(visitor.getFunction())
                    .flatMap(f -> {
                        if(f.getName().equalsIgnoreCase("count")) {
                            if(f.isAllColumns()) {
                                selection.countAll();
                                return Optional.empty(); 
                            }
                            return f.getParameters().getExpressions().stream()
                                .map(p -> selection.count(getColumnName(table, SelectionItem.column(p))))
                                .findFirst();
                        }
                        return Optional.of(selection.fcall(f.getName(), f.getParameters().getExpressions().stream()
                            .map(SelectionItem::value)
                            .toArray()));
                    }))

                .ifPresentOrElse(alias -> Optional.ofNullable(visitor.getAlias()).ifPresent(alias::as), 
                    () -> checkNullOrEmpty(visitor.getAlias()));
            
            return visitor.identity().stream();
        }

        private String getColumnName(TableMetadata table, Column col) {
            return config.getColumnMetadata(table, col.getColumnName()).getName();
        }

        @Override
        public void visit(SubSelect subSelect) {
            unsupported();
        }

        @Override
        public void visit(SubJoin subjoin) {
            unsupported();
        }

        @Override
        public void visit(LateralSubSelect lateralSubSelect) {
            unsupported();
        }

        @Override
        public void visit(ValuesList valuesList) {
            unsupported();
        }

        @Override
        public void visit(TableFunction tableFunction) {
            unsupported();
        }
        
        public CStatement getResult() {
            return result;
        }
        
    }
    
    
    private static class SelectionItem extends ValueParser implements SelectItemVisitor {
        
        private boolean allColumns;
        private Column column;
        private net.sf.jsqlparser.expression.Function function;
        private String alias;
        
        public SelectionItem(SelectItem item) {
            item.accept(this);
        }
        
        public SelectionItem(Expression expression) {
            expression.accept(this);
        }
        
        static Column column(Expression expression) {
            return Optional.ofNullable(new SelectionItem(expression).column)
                .orElseGet(Select::unsupported);
        }

        static Object value(Expression expression) {
            return Optional.ofNullable(new SelectionItem(expression).getValue())
                .orElseGet(Select::unsupported);
        }

        
        @Override
        public void visit(Column column) {
            this.column = column;
        }
        
        @Override
        public void visit(net.sf.jsqlparser.expression.Function function) {
            this.function = function;
        }
        
        @Override
        public void visit(AllColumns allColumns) {
            this.allColumns = true;
        }

        @Override
        public void visit(SelectExpressionItem columns) {
            columns.getExpression().accept(this);
            this.alias = Optional.ofNullable(columns.getAlias())
                .map(Alias::getName).orElse(null);
        }

        @Override
        public void visit(AllTableColumns allTableColumns) {
            unsupported();
        }
        
        public boolean isAllColumns() {
            return allColumns;
        }
        
        public Column getColumn() {
            return column;
        }
        
        public String getAlias() {
            return alias;
        }
        
        public net.sf.jsqlparser.expression.Function getFunction() {
            return function;
        }
        
        public Optional<String> identity() {
            return Optional.ofNullable(column)
                    .map(Column::getFullyQualifiedName)
                .or(() -> Optional.ofNullable(function)
                    .map(f -> f.getName()));
        }

        @Override
        public int hashCode() {
            return Objects.hash(alias, allColumns, columnName(), functionName());
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SelectionItem)) {
                return false;
            }
            SelectionItem other = (SelectionItem) obj;
            return Objects.equals(alias, other.alias) && allColumns == other.allColumns
                && Objects.equals(columnName(), other.columnName()) && Objects.equals(functionName(), other.functionName());
        }
        


        private String columnName() {
            return column == null ? null : column.getFullyQualifiedName();
        }
        
        private String functionName() {
            return function == null ? null : function.toString();
        }


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
