/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.translator.impl;

import java.util.List;
import java.util.Optional;

import com.cassandrajdbc.expressions.ValueParser;
import com.cassandrajdbc.expressions.WhereParser;
import com.cassandrajdbc.translator.SqlParser.SqlStatement;
import com.cassandrajdbc.translator.SqlToClqTranslator.ClusterConfiguration;
import com.cassandrajdbc.translator.SqlToClqTranslator.CqlBuilder;
import com.cassandrajdbc.translator.stmt.CStatement;
import com.cassandrajdbc.translator.stmt.SimpleCStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.SelectionOrAlias;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;

public class Select implements CqlBuilder<net.sf.jsqlparser.statement.select.Select> {

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

        public SelectVisitorImpl(SqlStatement<?> sql, ClusterConfiguration config) {
            this.sql = sql;
            this.config = config;
        }

        @Override
        public void visit(PlainSelect plainSelect) {
            checkNullOrEmpty(plainSelect.getDistinct());
            checkNullOrEmpty(plainSelect.getIntoTables());
            checkNullOrEmpty(plainSelect.getJoins());
            checkNullOrEmpty(plainSelect.getGroupByColumnReferences());
            checkNullOrEmpty(plainSelect.getOrderByElements());
            checkNullOrEmpty(plainSelect.getHaving());
            checkNullOrEmpty(plainSelect.getLimit());
            checkNullOrEmpty(plainSelect.getOffset());
            checkNullOrEmpty(plainSelect.getFetch());
            checkNullOrEmpty(plainSelect.getSkip());
            checkNullOrEmpty(plainSelect.getFirst());
            checkNullOrEmpty(plainSelect.getTop());
            
            FromItemVisitorImpl fromVisitor = new FromItemVisitorImpl(sql, plainSelect.getSelectItems(), plainSelect.getWhere(), config);
            plainSelect.getFromItem().accept(fromVisitor);
            result = fromVisitor.getResult();
        }

        @Override
        public void visit(SetOperationList setOpList) {
            unsupported();
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
        
        private final List<SelectItem> select;
        private final Expression where;
        private final ClusterConfiguration config;
        private SqlStatement<?> sql;
        private CStatement result;

        public FromItemVisitorImpl(SqlStatement<?> sql, List<SelectItem> select, Expression where, ClusterConfiguration config) {
            this.sql = sql;
            this.select = select;
            this.where = where;
            this.config = config;
        }

        @Override
        public void visit(Table table) {
            TableMetadata tableMetadata = config.getTableMetadata(table);
            Selection selection = QueryBuilder.select();
            select.forEach(c -> c.accept(new SelectItemVisitorImpl(selection, tableMetadata, config)));
            com.datastax.driver.core.querybuilder.Select from = selection.from(tableMetadata).allowFiltering();
            WhereParser.instance(tableMetadata, config).apply(where).forEach(from::where);
            result = new SimpleCStatement(sql, from);
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
    
    private static class SelectItemVisitorImpl implements SelectItemVisitor {
        
        private final ClusterConfiguration config;
        private final TableMetadata table;
        
        private Selection selection;
        
        public SelectItemVisitorImpl(Selection selection, TableMetadata table, ClusterConfiguration config) {
            this.selection = selection;
            this.table = table;
            this.config = config;
        }

        @Override
        public void visit(AllColumns allColumns) {
            selection.all();
        }

        @Override
        public void visit(SelectExpressionItem selectExpressionItem) {
            ColumnVisitor visitor = new ColumnVisitor();
            selectExpressionItem.getExpression().accept(visitor);
            Column column = visitor.getColumn();
            checkNullOrEmpty(visitor.getValue());
            checkNullOrEmpty(column.getTable().getName());
            
            SelectionOrAlias as = selection.column(config.getColumnMetadata(table, column.getColumnName()).getName());
            Optional.ofNullable(selectExpressionItem.getAlias()).ifPresent(a -> as.as(a.getName()));
            
        }

        @Override
        public void visit(AllTableColumns allTableColumns) {
            unsupported();
        }
        
    }
    
    
    private static class ColumnVisitor extends ValueParser {

        private Column column;

        @Override
        public void visit(Column column) {
            this.column = column;
        }
        
        public Column getColumn() {
            return column;
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
    
    private static void unsupported() {
        throw new UnsupportedOperationException();
    }
    
    

}
