/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import static java.util.function.Predicate.not;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cassandrajdbc.expressions.ValueParser;
import com.cassandrajdbc.expressions.WhereParser;
import com.cassandrajdbc.statement.CPreparedStatement;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.CStatement.CRow;
import com.cassandrajdbc.translator.stmt.CombiningCStatement;
import com.cassandrajdbc.translator.stmt.DelegateCStatement;
import com.cassandrajdbc.translator.stmt.MappingCStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.ExceptOp;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.IntersectOp;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.MinusOp;
import net.sf.jsqlparser.statement.select.Offset;
import net.sf.jsqlparser.statement.select.OrderByElement;
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
    
    public static class SelectVisitorImpl implements SelectVisitor {
        
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

        private CStatement parse(PlainSelect select) {
            checkNullOrEmpty(select.getIntoTables());
//            checkNullOrEmpty(select.getJoins());
            checkNullOrEmpty(select.getGroupByColumnReferences());
            checkNullOrEmpty(select.getHaving());
            
            checkNullOrEmpty(select.getFetch());
            checkNullOrEmpty(select.getSkip());
            checkNullOrEmpty(select.getFirst());
            checkNullOrEmpty(select.getTop());
            
            
            
            SelectionContext context = new SelectionContext(select, config);
            
            CStatement full = (stmt, params) -> () -> Optional.ofNullable(select.getJoins()).stream()
                .flatMap(List::stream)
                .filter(not(Join::isRight))
                .map(join -> leftJoin(join, select.getWhere(), context, config))
                .reduce(join(plainSelect(select, context), params, false), Select::flatMapReduce)
                .apply(stmt, new CRow(Collections.emptyList(), (a,b) -> null))
                .map(row -> row.arrange(context.getColumns()))
                .iterator();
                
            
            return literalColumns(context.getSelectItems(), context.getColumns())
                .andThen(distinct(select.getDistinct(), context.getColumns()))
                .andThen(orderBy(select.getOrderByElements(), context.getColumns()))
                .andThen(pagination(select.getOffset(), select.getLimit()))
                .apply(full);
        }
        


        private FromItemVisitorImpl plainSelect(PlainSelect select, SelectionContext context) {
            return new FromItemVisitorImpl(sql, select.getFromItem(), select.getWhere(), context, config);
        }

        // TODO: right join impl
        private FromItemVisitorImpl rightJoin(PlainSelect select, Join join, SelectionContext context) {
            Deque<FromItemVisitorImpl> executionPlan = new ArrayDeque<>();

            
            FromItemVisitorImpl res = new FromItemVisitorImpl(null, join.getRightItem(), null, context, config);
            executionPlan.addFirst(res);
            
            JoinContext<Join> first = new JoinContext<>(join.getRightItem(), join.getOnExpression(), join);
            JoinContext<PlainSelect> root = new JoinContext<>(select.getFromItem(), select.getWhere(), select);
            List<JoinContext<Join>> joins = select.getJoins().stream()
                .filter(j -> j != join)
                .map(j -> new JoinContext<>(j.getRightItem(), j.getOnExpression(), j))
                .collect(Collectors.toList());
            
            Object[] array = Stream.concat(Stream.of(first),orgnaizeJoins(first, root, joins, first.leftTables(), first.rightTables))
                .toArray();
            
            return res;
        }

        
        private Stream<JoinContext<?>> orgnaizeJoins(JoinContext<?> previous, JoinContext<PlainSelect> root, List<JoinContext<Join>> joins, Set<String> left, Set<String> right) {
            if(root != null && root.isNextFor(left, right)) {
                return Stream.concat(Stream.of(root), orgnaizeJoins(root, null, joins, 
                    Sets.union(left, root.leftTables()), Sets.union(left, root.rightTables)));
            }
            Iterator<JoinContext<Join>> iterator = joins.iterator();
            return Stream.generate(() -> iterator)
                .takeWhile(Iterator::hasNext)
                .flatMap(i -> {
                    JoinContext<Join> next = i.next();
                    if(!next.isNextFor(left, right)) {
                        return Stream.empty();
                    }
                    i.remove();
                    return Stream.of(next);
                })
                .collect(Collectors.toList()).stream()
                .flatMap(next -> Stream.concat(Stream.of(next), orgnaizeJoins(next, root, joins, 
                    Sets.union(left, next.leftTables()), Sets.union(left, next.rightTables))));
        }

        private static class JoinContext<T> extends ExpressionVisitorAdapter implements FromItemVisitor {

            private final Set<String> fromNames = new HashSet<>();
            private final Set<String> fromAliases = new HashSet<>();
            private final Set<String> rightTables = new HashSet<>();
            private T delegate;
            
            public JoinContext(FromItem from, Expression where, T delegate) {
                this.delegate = delegate;
                from.accept(this);
                Optional.ofNullable(where).ifPresent(w -> w.accept(this));
            }
            
            public Set<String> leftTables() {
                return Sets.union(fromAliases, fromNames);
            }
            
            @Override
            public void visit(Column column) {
                String tableName = column.getTable().getName();
                if(!fromNames.contains(tableName) && !fromAliases.contains(tableName)) {
                    Table table = Objects.requireNonNull(column.getTable());
                    visit(table.getFullyQualifiedName(), table.getAlias(), rightTables, rightTables);
                }
            }
            
            @Override
            public String toString() {
                return delegate.toString();
            }
            
            @Override
            public void visit(Table tableName) {
                visit(tableName.getFullyQualifiedName(), tableName.getAlias(), fromNames, fromAliases);
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

            @Override
            public void visit(SubSelect subSelect) {
                unsupported();
            }
            
            public boolean isNextFor(Set<String> left, Set<String> right) {
                return (!this.rightTables.isEmpty() && left.containsAll(rightTables)) ||
                    right.containsAll(this.fromNames) || right.containsAll(this.fromAliases);
            }

            private void visit(String fullName, Alias alias, Set<String> tables, Set<String> aliases) {
                tables.add(fullName);
                Optional.ofNullable(alias)
                    .ifPresent(as -> aliases.add(as.getName()));
            }
            
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
    
    private static class SelectionContext extends ExpressionVisitorAdapter implements FromItemVisitor {

        private final ClusterConfiguration config;
        
        private final Map<String, TableMetadata> tables = new HashMap<>();
        
        private final List<String> columns;
        
        private final List<String> joins;
        
        private final Map<TableMetadata, Selection> tableSelects;

        private final List<SelectionItem> selectItems;
        
        private final List<SelectionItem> joinItems;

        public SelectionContext(PlainSelect select, ClusterConfiguration config) {
            this.config = config;
            Optional.ofNullable(select.getFromItem()).ifPresent(f -> f.accept(this));
            Optional.ofNullable(select.getJoins()).stream()
                .flatMap(List::stream)
                .map(Join::getRightItem)
                .forEach(j -> j.accept(this));
            this.columns = new ArrayList<>();
            this.joins = new ArrayList<>(); 
            this.selectItems = select.getSelectItems().stream()
                .map(SelectionItem::new)
                .collect(Collectors.toList());
            this.joinItems = new ArrayList<>();
            this.tableSelects = selectItems.stream()
                .collect(Collectors.groupingBy(this::getTable, LinkedHashMap::new,
                    Collectors.collectingAndThen(Collectors.toList(), list -> list.stream()
                        .reduce(QueryBuilder.select(), (s,i) -> select(s, i, columns::add), (a,b) -> b))));
            Optional.ofNullable(select.getJoins())
                .ifPresent(joins -> joins.forEach(join -> join.getOnExpression().accept(this)));
        }

        private TableMetadata getTable(SelectionItem item) {
            if(item.getTable() == null || item.getTable().getFullyQualifiedName().isBlank()) {
                return Iterables.getFirst(tables.values(), null);
            }
            return tables.get(item.getTable().getFullyQualifiedName());
        }

        @Override
        public void visit(Table tableName) {
            TableMetadata table = config.getTableMetadata(tableName);
            tables.put(tableName.getFullyQualifiedName(), table);
            Optional.ofNullable(tableName.getAlias())
                .ifPresent(alias -> tables.put(alias.getName(), table));
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
        
        @Override
        public void visit(Column column) {
            if(columns.contains(column.getFullyQualifiedName())) {
                return; 
            }
            SelectionItem item = new SelectionItem(column);
            Selection selection = Optional.ofNullable(tables.get(item.getTable().getFullyQualifiedName()))
                .map(table ->  tableSelects.computeIfAbsent(table, k -> QueryBuilder.select()))
                .orElseThrow(() ->  new IllegalStateException("table not resolvable"));
            select(selection, item, joins::add);
            joinItems.add(item);
        }
        
        public List<String> getColumns() {
            return columns;
        }
        
        public List<String> getColumns(Table table) {
            return Stream.concat(selectItems.stream(), joinItems.stream())
               .filter(item -> matchesTable(table, item.getTable()))
               .flatMap(this::columns)
               .collect(Collectors.toList());
        }

        private boolean matchesTable(Table left, Table right) {
            if(left == null || right == null || 
                left.getFullyQualifiedName() == null || right.getFullyQualifiedName().isEmpty()) {
                return true;
            }
            return Objects.equals(Optional.ofNullable(left.getAlias())
                .map(Alias::getName)
                .orElseGet(left::getFullyQualifiedName), right.getFullyQualifiedName());
        }

        public List<String> getJoins() {
            return joins;
        }
        
        public Selection getTableSelect(TableMetadata table) {
            return tableSelects.get(table);
        }

        public TableMetadata getTableMetadata(Table table) {
            return tables.get(table.getFullyQualifiedName());
        }
        
        public List<SelectionItem> getSelectItems() {
            return selectItems;
        }
        
        private Stream<String> columns(SelectionItem select) {
            TableMetadata table = getTable(select);
            // select *
            if(select.isAllColumns()) {
                String prefix = Optional.ofNullable(select.getTable()).map(t -> t.getName() + ".").orElse("");
                return table.getColumns().stream().map(col -> prefix + col.getName());
            }
            // select funct()
            var f = select.getFunction();
            if(f != null && f.getName().equalsIgnoreCase("count") && f.isAllColumns()) {
                return Stream.of("count");
            }
            // select col,col2
            return select.identity().stream();
        }
        
        
        // todo selection item visitor subtype
        private Selection select(Selection selection, SelectionItem select, Consumer<String> columns) {
            columns(select).forEach(columns);
            
            TableMetadata table = getTable(select);
            // select *
            if(select.isAllColumns()) {
                selection.all();
                return selection;
            }

            // select col,col2
            if(select.getColumn() != null) {
                Optional.ofNullable(select.getAlias())
                    .ifPresent(selection.column(getColumnName(table, select.getColumn()))::as);
                return selection;
            }
            
            // select funct()
            if(select.getFunction() != null) {
                var f = select.getFunction();
                if(f.getName().equalsIgnoreCase("count")) {
                    if(f.isAllColumns()) {
                        selection.countAll();
                        return selection;
                    }
                    f.getParameters().getExpressions().stream().findFirst()
                        .map(exp -> selection.count(getColumnName(table, SelectionItem.column(exp))))
                        .ifPresent(sel -> Optional.ofNullable(select.getAlias()).ifPresent(sel::as));
                } else {
                    Optional.ofNullable(select.getAlias())
                        .ifPresent(selection.fcall(f.getName(), f.getParameters().getExpressions().stream()
                            .map(SelectionItem::value)
                            .toArray())::as); 
                }
            }
            return selection;
        }
        
        private String getColumnName(TableMetadata table, Column col) {
            return config.getColumnMetadata(table, col.getColumnName()).getName();
        }
        
    }
    
    private static class FromItemVisitorImpl implements FromItemVisitor {
        
        private final ClusterConfiguration config;
        private SqlStatement<?> sql;
        private CStatement result;
        private SelectionContext context;
        private Expression where;
        private final List<WhereParser> whereClauses = new ArrayList<>();
        private final List<String> columns = new ArrayList<>();

        public FromItemVisitorImpl(SqlStatement<?> sql, FromItem from, Expression where, SelectionContext context, ClusterConfiguration config) {
            this.sql = sql;
            this.where = where;
            this.config = config;
            this.context = context;
            from.accept(this);
        }
        
        public List<Object> getParams(CRow row) {
            return whereClauses.stream()
                .flatMap(where -> where.getBindColumns().stream())
                .filter(col -> row.columnNames().contains(col.getFullyQualifiedName())) // FIXME
                .map(col -> row.getColumn(col.getFullyQualifiedName(), Object.class))
                .collect(Collectors.toList());
        }

        @Override
        public void visit(Table table) {
            TableMetadata tableMetadata = context.getTableMetadata(table);
            
            // selection
            Selection selection = context.getTableSelect(tableMetadata);
            
            // from/where
            com.datastax.driver.core.querybuilder.Select from = selection.from(tableMetadata).allowFiltering();
            result = where(from, table, where);
        }

        private CStatement where(com.datastax.driver.core.querybuilder.Select from, Table table, Expression where) {
            WhereParser whereParser = new WhereParser(where, table, config);
            whereParser.getClauses().forEach(from::where);
            whereClauses.add(whereParser);
            List<String> columns = context.getColumns(table);
            this.columns.addAll(columns);
            if (whereParser.getSubselects().isEmpty()) {
                return new SimpleCStatement(sql, from, columns);
            }
            return new DelegateCStatement(sql, (pstmt, params) -> {
                whereParser.getSubselects().stream()
                    .forEach(es -> from.where(es.extract(pstmt, params)));
                return new SimpleCStatement(sql, from, columns);
            });
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
        
        public List<String> getColumns() {
            return columns;
        }
        
        public boolean matches(CRow row) {
            return whereClauses.stream()
                .allMatch(where -> where.getPredicate().test(row));
        }
    }
    
    
    private static class SelectionItem extends ValueParser implements SelectItemVisitor {
        
        private boolean allColumns;
        private Column column;
        private net.sf.jsqlparser.expression.Function function;
        private String alias;
        private Table table;
        
        public SelectionItem(SelectItem item) {
            super(null);
            item.accept(this);
        }
        
        public SelectionItem(Expression expression) {
            super(expression);
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
            this.table = column.getTable();
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
            this.table = allTableColumns.getTable();
            this.allColumns = true;
        }
        
        public Table getTable() {
            return table;
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
            return Optional.ofNullable(alias)
                .or(() -> Optional.ofNullable(column)
                    .map(Column::getFullyQualifiedName))
                .or(() -> Optional.ofNullable(function)
                    .map(f -> f.getName()));
        }
        

        public Stream<String> identities() {
            return Stream.of(Optional.ofNullable(alias), Optional.ofNullable(column)
                    .map(Column::getFullyQualifiedName), Optional.ofNullable(function)
                    .map(f -> f.getName()))
                .flatMap(opt -> opt.stream());
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
    

    private static UnaryOperator<CStatement> literalColumns(List<SelectionItem> selectItems, List<String> columns) {
        // literal columns
        return selectItems.stream()
            .filter(item -> !item.isAllColumns() && item.getColumn() == null && item.getFunction() == null)
            .map(item -> literal(item, columns))
            .reduce(Function::andThen)
            .map(rowMapper -> (UnaryOperator<CStatement>) previous ->  new MappingCStatement(previous, res -> Iterables.transform(res, rowMapper::apply)))
            .orElseGet(UnaryOperator::identity);
    }
    
    private static Function<CRow, CRow> literal(SelectionItem item, List<String> columns) {
        // select 'value'
        String name = item.identity().get();
        return row -> row.add(name, t -> item.getValue(), columns.indexOf(item.identity()
            .orElseGet(() -> unsupported())));
    }

    private static UnaryOperator<CStatement> distinct(Distinct distinct, List<String> columns) {
        // distinct
        if(distinct == null) {
            return UnaryOperator.identity();
        }
        logger.info("Potentially slow DISTINCT in query {} fetching all results to perform distinct", distinct);
        Set<Object> distinctValues = Sets.newConcurrentHashSet();
        int[] indexes = buildColumnIndexes(columns, Optional.ofNullable(distinct.getOnSelectItems()).stream()
            .flatMap(List::stream).map(SelectionItem::new).collect(Collectors.toList()));
        return previous -> new MappingCStatement(previous, res -> Iterables.filter(res, row -> {
            if(indexes.length == 0) {
                return distinctValues.add(row);
            }
            return distinctValues.add(Arrays.stream(indexes).mapToObj(i -> row.getColumn(i, Object.class))
                .collect(Collectors.toList()));
        }));
    }

    @SuppressWarnings("unchecked")
    private static UnaryOperator<CStatement> orderBy(List<OrderByElement> orderBy, List<String> columns) {
        // order by
        if(orderBy == null || orderBy.isEmpty()) {
            return UnaryOperator.identity();
        }
        Comparator<Object> sortBy = orderBy.stream()
            .flatMap(e -> Optional.ofNullable(e.getExpression()).stream().map(SelectionItem::new)
                .map(item -> columns.indexOf(item.identity().get()))
                .map(i -> Comparator.comparing(r -> (Comparable<Object>)((CRow)r).getColumn(i, Object.class)))
                .map(c -> e.isAsc() ? c : c.reversed())
                .map(c -> e.getNullOrdering() == NullOrdering.NULLS_FIRST ? Comparator.nullsFirst(c) : Comparator.nullsLast(c)))
                .reduce(Comparator::thenComparing).get();
        return result -> new MappingCStatement(result, res -> {
            logger.info("Potentially slow ORDER BY in query {} fetching all results to perform distinct", orderBy);
            return () -> stream(res)
                .sorted(sortBy)
                .iterator();
        });
    }

    private static Stream<CRow> stream(Iterable<CRow> res) {
        return StreamSupport.stream(res.spliterator(), false);
    }

    private static UnaryOperator<CStatement> pagination(Offset offset, Limit limit) {
        // offset + limit
        UnaryOperator<Iterable<CRow>> offsetFunc = Optional.ofNullable(offset)
            .map(off -> (UnaryOperator<Iterable<CRow>>) res -> Iterables.skip(res, (int)off.getOffset()))
            .orElseGet(UnaryOperator::identity);
        UnaryOperator<Iterable<CRow>> limitFunc = Optional.ofNullable(limit)
            .map(l -> (UnaryOperator<Iterable<CRow>>) res -> Iterables.limit(res, (int)l.getRowCount()))
            .orElseGet(UnaryOperator::identity);
        return result -> new MappingCStatement(result, rows -> limitFunc.apply(offsetFunc.apply(rows)));
    }

    private static <T, R> BiFunction<T, R, Stream<R>> flatMapReduce(
        BiFunction<T, R, Stream<R>> a, BiFunction<T, R, Stream<R>> b) {
        return (stmt, row) -> a.apply(stmt, row)
            .flatMap(next -> b.apply(stmt, next));
    }

    private static BiFunction<CPreparedStatement, CRow, Stream<CRow>> leftJoin(Join join, Expression where,
        SelectionContext context, ClusterConfiguration config) {
        FromItemVisitorImpl from = new FromItemVisitorImpl(null, join.getRightItem(), and(where, join.getOnExpression()), context, config);
        return join(from, Collections.emptyList(), join.isInner());
    }

    private static BiFunction<CPreparedStatement, CRow, Stream<CRow>> rightJoin(Join join, Expression where,
        SelectionContext context, ClusterConfiguration config) {
        FromItemVisitorImpl from = new FromItemVisitorImpl(null, join.getRightItem(), where, context, config); // FIXME on-expression..?
        return join(from, Collections.emptyList(), join.isInner());
    }

    private static BiFunction<CPreparedStatement, CRow, Stream<CRow>> join(FromItemVisitorImpl from, List<Object> params, boolean inner) {
        return (BiFunction<CPreparedStatement, CRow, Stream<CRow>>)(stmt, row) -> {
            try {
                List<Object> joinParams = Stream.concat(params.stream(), from.getParams(row).stream()).collect(Collectors.toList());
                if(joinParams.isEmpty()) {
//                    return Stream.of(row); // not applicable, FIXME error?
                }
                Iterator<CRow> joined = from.getResult().execute(stmt, joinParams).iterator();
                if(joined.hasNext() || inner) {
                    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(joined, 0), false)
                        .map(row::merge)
                        .filter(from::matches);
                }
                return Stream.of(row.merge(new CRow(from.getColumns(), (t,i) -> null)))
                    .filter(from::matches);
            } catch (SQLException e) {
                throw new IllegalArgumentException(e);
            }
        };
    }
    
    private static <T> Stream<T> reverse(List<T> list) {
        return IntStream.range(0, list.size())
            .mapToObj(i -> list.get(list.size() - i - 1));
    }

    private static Expression and(Expression one, Expression other) {
        return Stream.of(one, other)
            .filter(Objects::nonNull)
            .reduce(AndExpression::new)
            .orElse(null);
    }
    
    private static int[] buildColumnIndexes(List<String> columns, List<SelectionItem> distinctOn) {
        if(distinctOn.isEmpty() || distinctOn.stream().anyMatch(i -> i.identity().isEmpty())) {
            return new int[0];
        }
        return distinctOn.stream()
            .flatMapToInt(item -> item.identities()
                .mapToInt(columns::indexOf)
                .filter(i -> i != -1)
                .limit(1))
            .toArray();
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
        logger.error("this feaure is not yet supported by select translator");
        throw new UnsupportedOperationException();
    }
    
}
