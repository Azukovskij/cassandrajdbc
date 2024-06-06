/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.result;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.cassandrajdbc.statement.StatementOptions.Collation;
import com.cassandrajdbc.translator.SqlParser;
import com.cassandrajdbc.types.ColumnTypes;
import com.cassandrajdbc.types.ColumnTypes.ColumnType;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesisFromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;
import net.sf.jsqlparser.statement.select.WithItem;

/**
 * 
 * @author azukovskij
 *
 */
public class CResultSetMetaData implements ResultSetMetaData {

    private Column[] columns;
    private Collation collation;
    private Map<String, Integer> columnIndexes;
    
    public CResultSetMetaData(String schemaName, String tableName, String[] columnNames, DataType[] columnTypes) {
        Table table = new Table(schemaName, tableName);
        this.columns = IntStream.range(0, columnNames.length)
            .mapToObj(i -> new Column(table, columnNames[i], columnNames[i], columnNullableUnknown, columnTypes[i]))
            .toArray(Column[]::new);
        this.columnIndexes = IntStream.range(0, columnNames.length).boxed()
            .collect(Collectors.toMap(i -> columnNames[i], i -> i + 1, (a,b) -> a));
    }
    

    CResultSetMetaData(Column[] columns, Metadata clusterMeta) {
        this.columns = columns;
        this.columnIndexes = IntStream.range(0, columns.length).boxed()
            .collect(Collectors.toMap(i -> columns[i].label, i -> i + 1, (a,b) -> a));
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
          return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
    
    public int findColumn(String columnLabel) {
        return Optional.ofNullable(columnIndexes.get(columnLabel))
            .or(() -> Optional.ofNullable(columnIndexes.get(columnLabel.toLowerCase())))
            .orElseThrow(() -> new NoSuchElementException("No column for label " + columnLabel));
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return collation == Collation.CASE_INSENSITIVE;
    }
    
    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columns[column - 1].nullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columns[column - 1].label;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columns[column - 1].name;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return columns[column - 1].table.schema;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return columns[column - 1].table.name;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return JDBCType.valueOf(columns[column - 1].type.getDataType()).getVendorTypeNumber();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return columns[column - 1].type.getDataType();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return columns[column - 1].nullable == columnNoNulls;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return !isReadOnly(column);
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return isWritable(column);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return columns[column - 1].valueType.getName();
    }
    
    private final static class Table {

        private String schema;
        private String name;
        
        Table(String schema, String name) {
            this.schema = schema;
            this.name = name;
        }
        
    }
    
    private static final class Column {
        
        private final Table table;
        private final String label;
        private final String name;
        private final int nullable;
        private final ColDataType type;
        private final Class<?> valueType;
        
        Column(Table table, String name, String label, int nullable, DataType type) {
            this.table = table;
            this.name = name;
            this.label = label;
            this.nullable = nullable;
            ColumnType col = ColumnTypes.fromCqlType(type, java.util.function.Function.identity());
            this.type = col == null ? null : col.getSqlDataType();
            this.valueType = col == null ? null : col.getJavaType();
        }
        
    }
    
    /**
     * Utility to create {@link CResultSetMetaData} instances from SQL string 
     * 
     * @author azukovskij
     *
     */
    public static final class Parser extends StatementVisitorAdapter implements SelectVisitor {
        
        private static final Pattern FALLBACK_PARSE_PATTERN = Pattern.compile("SELECT.*(COUNT|count)?.*FROM ([^\\.]*)\\.([^ ]+).*");

        private final List<Column> columns = new ArrayList<>();
        private final Metadata metadata;
        
        public static CResultSetMetaData parse(String sql, Metadata metadata) throws SQLException {
            if(sql == null) {
                return new CResultSetMetaData(new Column[0], metadata);
            }
            try {
                Parser parser = new Parser(metadata);
                SqlParser.parse(sql).getStatement().accept(parser);
                return new CResultSetMetaData(parser.columns.toArray(Column[]::new), metadata);
            } catch (JSQLParserException e) {
                return new CResultSetMetaData(parseColumnsAsRegExp(sql, metadata), metadata);
            } catch (IllegalStateException e) {
                throw new SQLException(e);
            }
        }

        private static Column[] parseColumnsAsRegExp(String sql, Metadata metadata) {
            var matcher = FALLBACK_PARSE_PATTERN.matcher(sql);
            if (!matcher.matches()) {
                return new Column[0];
            }
            return Optional.of(matcher.group(2))
                .filter(str -> !str.isBlank())
                .map(metadata::getKeyspace)
                .map(keyspace -> keyspace.getTable(matcher.group(3)))
                .map(table -> {
                    var tb = new Table(table.getKeyspace().getName(), table.getName());
                    if(matcher.group(1) != null) { // count
                        return new Column[] { new Column(tb, "count", "count", columnNoNulls, DataType.bigint())};
                    }
                    return table.getColumns().stream()
                        .map(cm -> new Column(tb, cm.getName(), cm.getName(), columnNullableUnknown, cm.getType()))
                        .toArray(Column[]::new);
                })
                .orElseGet(() -> new Column[0]);

        }

        private Parser(Metadata metadata) {
            this.metadata = metadata;
        }
        
        @Override
        public void visit(Select select) {
            select.getSelectBody().accept(this);
        }

        @Override
        public void visit(Statements stmts) {
            stmts.getStatements().forEach(stmt -> stmt.accept(this));
        }

        @Override
        public void visit(SetOperationList setOpList) {
            setOpList.getSelects().forEach(stmt -> stmt.accept(this));
        }

        @Override
        public void visit(PlainSelect plainSelect) {
            SelectVisitor parser = new SelectVisitor();
            Optional.ofNullable(plainSelect.getFromItem()).ifPresent(from -> from.accept(parser));
            plainSelect.getSelectItems().forEach(item -> item.accept(parser));
            this.columns.addAll(parser.columns);
        }

        @Override
        public void visit(WithItem withItem) {
            throw new UnsupportedOperationException();
        }
        
        
        /**
         * Selects items from 
         * 
         * @author azukovskij
         *
         */
        private final class SelectVisitor implements SelectItemVisitor, FromItemVisitor {
            
            private TableMetadata table;
            private final List<Column> columns = new ArrayList<>();

            @Override
            public void visit(AllColumns all) {
                if(table == null) {
                    throw new UnsupportedOperationException();
                }
                allColumns(this.table);
            }

            @Override
            public void visit(AllTableColumns all) {
                allColumns(metadata.getKeyspace(all.getTable().getSchemaName())
                    .getTable(all.getTable().getName()));
            }

            @Override
            public void visit(SelectExpressionItem item) {
                if(table == null) {
                    throw new UnsupportedOperationException();
                }
                Table table = new Table(this.table.getKeyspace().getName(), this.table.getName());
                Expression expression = item.getExpression();
                if(expression instanceof net.sf.jsqlparser.schema.Column) {
                     ColumnMetadata c = this.table.getColumn(((net.sf.jsqlparser.schema.Column) expression).getColumnName());
                     this.columns.add(new Column(table, c.getName(), getAlias(item, c.getName()), 
                        this.table.getPrimaryKey().contains(c) ? columnNoNulls : columnNullable, c.getType()));
                } else if(item.getExpression() instanceof Function && "COUNT".equals(((Function)item.getExpression()).getName())) {
                    this.columns.add(new Column(table, "count", "count", columnNoNulls, DataType.bigint()));
                } else {
                    String name = getAlias(item, null);
                    this.columns.add(new Column(table, name, name, columnNoNulls, null));
                }
            }

            @Override
            public void visit(net.sf.jsqlparser.schema.Table table) {
                this.table = Optional.ofNullable(metadata.getKeyspace(table.getSchemaName()))
                    .flatMap(ks -> Optional.ofNullable(ks.getTable(table.getName()))
                        .or(() -> Optional.ofNullable(ks.getTable("\"" + table.getName() + "\""))))
                    .orElseThrow(() -> new IllegalStateException("Table " + table.getSchemaName() 
                        + "." + table.getName() + " does not exists"));
            }
            
            @Override
            public void visit(ParenthesisFromItem aThis) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visit(ValuesList valuesList) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visit(SubSelect subSelect) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visit(SubJoin subjoin) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visit(LateralSubSelect lateralSubSelect) {
                throw new UnsupportedOperationException();
            }
            
            @Override
            public void visit(TableFunction tableFunction) {
                throw new UnsupportedOperationException();
            }

            public String getAlias(SelectExpressionItem item, String defaultValue) {
                return Optional.ofNullable(item.getAlias())
                    .map(Alias::getName)
                    .orElse(defaultValue);
            }

            private void allColumns(TableMetadata tableMeta) {
                Table table = new Table(tableMeta.getKeyspace().getName(), tableMeta.getName());
                tableMeta.getColumns().stream()
                    .map(c -> new Column(table, c.getName(), c.getName(), 
                        tableMeta.getPrimaryKey().contains(c) ? columnNoNulls : columnNullable, c.getType()))
                    .forEach(columns::add);
            }

            
        }
        
        
    }
    
 

}
