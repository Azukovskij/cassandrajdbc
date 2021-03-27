/* Copyright © 2020 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.cassandrajdbc.connection;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLType;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.cassandrajdbc.CClientInfo;
import com.cassandrajdbc.CassandraURL;
import com.cassandrajdbc.result.CResultSet;
import com.cassandrajdbc.result.CResultSetMetaData;
import com.cassandrajdbc.statement.CPreparedStatement;
import com.cassandrajdbc.types.ColumnTypes;
import com.cassandrajdbc.types.ColumnTypes.ColumnType;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableOptionsMetadata;

class CDatabaseMetaData implements DatabaseMetaData {

    private final CassandraURL connectionUrl;
    private final CassandraConnection connection;

    public CDatabaseMetaData(CassandraURL connectionUrl, CassandraConnection connection) {
        this.connectionUrl = connectionUrl;
        this.connection = connection;
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

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return Optional.ofNullable(connectionUrl)
            .map(CassandraURL::getRawUrl)
            .orElse(null);
    }

    @Override
    public String getUserName() throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "Cassandra";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "3.x";
    }

    @Override
    public String getDriverName() throws SQLException {
        return "Cassandra JDBC Driver";
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return true;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "KEYSPACE";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "PROCEDURE";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 48;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 48;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 48;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 1;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return true;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 3;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateXOpen;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }
    
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
        throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "tables", "PROCEDURE_CAT", 
            "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "SMALLINT:COLUMN_TYPE", "INTEGER:DATA_TYPE",
            "TYPE_NAME", "INTEGER:PRECISION", "INTEGER:LENGTH", "SMALLINT:SCALE", "SMALLINT:RADIX",
            "INTEGER:NULLABLE", "REMARKS", "INTEGER:SQL_DATA_TYPE", "INTEGER:SQL_DATETIME_SUB",
            "INTEGER:CHAR_OCTET_LENGTH", "INTEGER:CHAR_OCTET_LENGTH", "INTEGER:ORDINAL_POSITION",
            "IS_NULLABLE", "SPECIFIC_NAME");
        return toResultSet(metadata);
    }
    
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
        String columnNamePattern) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns", "PROCEDURE_CAT", 
            "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "SMALLINT:COLUMN_TYPE", "INTEGER:DATA_TYPE", 
            "TYPE_NAME", "INTEGER:PRECISION", "INTEGER:LENGTH", "SMALLINT:SCALE", "SMALLINT:RADIX",
            "SMALLINT:NULLABLE", "REMARKS", "COLUMN_DEF", "INTEGER:SQL_DATA_TYPE", "INTEGER:SQL_DATETIME_SUB",
            "INTEGER:CHAR_OCTET_LENGTH", "INTEGER:ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME");
        return toResultSet(metadata);
    }
    
    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
        throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns", "TABLE_CAT", 
            "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE");
        return toResultSet(metadata);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
        throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "tables", "TABLE_CAT", 
            "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE");
        return toResultSet(metadata);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns", "SMALLINT:SCOPE", "COLUMN_NAME", 
            "INTEGER:DATA_TYPE", "TYPE_NAME", "INTEGER:COLUMN_SIZE", "INTEGER:BUFFER_LENGTH", "SMALLINT:DECIMAL_DIGITS", "SMALLINT:PSEUDO_COLUMN");
        return toResultSet(metadata);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
        throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns", "SMALLINT:SCOPE", "COLUMN_NAME", 
            "INTEGER:DATA_TYPE", "TYPE_NAME", "INTEGER:COLUMN_SIZE", "INTEGER:BUFFER_LENGTH", "SMALLINT:DECIMAL_DIGITS", "SMALLINT:PSEUDO_COLUMN");
        return toResultSet(metadata);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns", "PKTABLE_CAT", "PKTABLE_SCHEM", 
            "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", 
            "SMALLINT:KEY_SEQ", "SMALLINT:UPDATE_RULE", "SMALLINT:DELETE_RULE", "FK_NAME", "PK_NAME", "SMALLINT:DEFERRABILITY");
        return toResultSet(metadata);
    }
    
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns", "PKTABLE_CAT", "PKTABLE_SCHEM", 
            "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", 
            "SMALLINT:KEY_SEQ", "SMALLINT:UPDATE_RULE", "SMALLINT:DELETE_RULE", "FK_NAME", "PK_NAME", "SMALLINT:DEFERRABILITY");
        return toResultSet(metadata);
    }
    
    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
        String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns", "PKTABLE_CAT", "PKTABLE_SCHEM", 
            "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", 
            "SMALLINT:KEY_SEQ", "SMALLINT:UPDATE_RULE", "SMALLINT:DELETE_RULE", "FK_NAME", "PK_NAME", "SMALLINT:DEFERRABILITY");
        return toResultSet(metadata);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
        throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "tables", "TABLE_CAT", "TABLE_SCHEM", 
            "TABLE_NAME", "BOOLEAN:NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "SMALLINT:TYPE", "SMALLINT:ORDINAL_POSITION", 
            "COLUMN_NAME", "ASC_OR_DESC", "BIGINT:CARDINALITY", "BIGINT:PAGES", "FILTER_CONDITION");
        return toResultSet(metadata);
    }
    
    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns", "TABLE_CAT", "TABLE_SCHEM", 
            "TABLE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME");
        return toResultSet(metadata);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "tables", "TABLE_CAT", "TABLE_SCHEM", 
            "TABLE_NAME", "SUPERTABLE_NAME");
        return toResultSet(metadata);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
        String columnNamePattern) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns", "TABLE_CAT", "TABLE_SCHEM", 
            "TABLE_NAME", "COLUMN_NAME", "INTEGER:DATA_TYPE", "INTEGER:COLUMN_SIZE", "INTEGER:DECIMAL_DIGITS", 
            "INTEGER:NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS", "INTEGER:CHAR_OCTET_LENGTH", "IS_NULLABLE");
        return toResultSet(metadata);
    }
    
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
        throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "tables", "TABLE_CAT", "TABLE_SCHEM", 
            "TABLE_NAME","TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEMA", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
        if(types != null && Arrays.stream(types)
            .noneMatch(type -> "SYSTEM TABLE".equals(type) || "TABLE".equals(type))) {
            return toResultSet(metadata);
        }
        return toResultSet(metadata, getDatabaseMetadata().getKeyspaces().stream()
            .filter(ks -> matches(ks.getName(), schemaPattern))
            .flatMap(ks -> ks.getTables().stream())
            .filter(tb -> matches(tb.getName(), tableNamePattern))
            .map(tb -> new Object[]{ 
                    "",  // "TABLE_CAT
                    tb.getKeyspace().getName(), // "TABLE_SCHEM"
                    tb.getName(), // "TABLE_NAME"
                    getTableType(tb.getName()), // "TABLE_TYPE"
                    tb.getOptions().getComment(), // "REMARKS"
                    "", // "TYPE_CAT"
                    "", // "TYPE_SCHEMA"
                    "", // "TYPE_NAME"
                    "", // "SELF_REFERENCING_COL_NAME"
                    ""  // "REF_GENERATION"
            })
            .sorted(orderByColumn(0) // TABLE_CAT
                .thenComparing(orderByColumn(1))) // TABLE_SCHEM
            .toArray(Object[][]::new));
    }


    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "keyspaces", "TABLE_SCHEM", "TABLE_CATALOG");
        List<KeyspaceMetadata> keyspaces = getDatabaseMetadata().getKeyspaces();
        return new CResultSet(new CPreparedStatement(connection), metadata, IntStream.range(0, keyspaces.size())
            .filter(i -> matches(keyspaces.get(i).getName(), schemaPattern))    
            .mapToObj(i -> new CResultSet.Row(i, new Object[]{ keyspaces.get(i).getName(), "" }))
                .collect(Collectors.toList()));
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "keyspaces", "TABLE_CAT");
        return new CResultSet(new CPreparedStatement(connection), metadata, 
            Collections.singletonList(new CResultSet.Row(0, new Object[]{""})));
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "types", "TABLE_TYPE");
        return new CResultSet(new CPreparedStatement(connection), metadata, Arrays.asList(
            new CResultSet.Row(0, new Object[]{ "TABLE" }),
            new CResultSet.Row(1, new Object[]{ "SYSTEM TABLE" })));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
        throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns",  "TABLE_CAT", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME",
                    "INTEGER:DATA_TYPE", "TYPE_NAME", "INTEGER:COLUMN_SIZE", "INTEGER:BUFFER_LENGTH", "INTEGER:DECIMAL_DIGITS", "INTEGER:NUM_PREC_RADIX",
                    "INTEGER:NULLABLE", "REMARKS", "COLUMN_DEF", "INTEGER:SQL_DATA_TYPE", "INTEGER:SQL_DATETIME_SUB", "INTEGER:CHAR_OCTET_LENGTH",
                    "INTEGER:ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
                    "SMALLINT:SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "OPTIONS");
        List<KeyspaceMetadata> keyspaces = getDatabaseMetadata().getKeyspaces();
        return toResultSet(metadata, keyspaces.stream()
            .filter(ks -> matches(ks.getName(), schemaPattern))
            .flatMap(ks -> ks.getTables().stream())
            .filter(tb -> matches(tb.getName(), tableNamePattern))
            .flatMap(tb -> tb.getColumns().stream()
                .filter(col -> matches(col.getName(), columnNamePattern))
                .map(col -> {
                    SQLType sqlType = ColumnTypes.fromCqlType(col.getType(), ColumnType::getSqlType);
                    return new Object[]{ 
                            "",  // "TABLE_CAT
                            tb.getKeyspace().getName(), // "TABLE_SCHEMA"
                            tb.getName(), // "TABLE_NAME"
                            col.getName(), // "COLUMN_NAME"
                            sqlType.getVendorTypeNumber(),  // DATA_TYPE
                            sqlType.getName(),  // TYPE_NAME
                            800, // "COLUMN_SIZE"
                            0, // "BUFFER_LENGTH"
                            0, // "DECIMAL_DIGITS"
                            10, // "NUM_PREC_RADIX"
                            (tb.getPrimaryKey().contains(col) ? 0 : 1), // "NULLABLE"
                            "", // "REMARKS"
                            "", // "COLUMN_DEF"
                            0, // "SQL_DATA_TYPE" 
                            0, // "SQL_DATETIME_SUB"
                            800, // "CHAR_OCTET_LENGTH"
                            tb.getColumns().indexOf(col) + 1, // "ORDINAL_POSITION"
                            "", // "IS_NULLABLE"
                            null, // "SCOPE_CATLOG"
                            null, // "SCOPE_SCHEMA"
                            null, // "SCOPE_TABLE"
                            null, // "SOURCE_DATA_TYPE"
                            "NO", // "IS_AUTOINCREMENT"
                            getTableOptionString(tb) // "OPTIONS"
                    };
                }))
            .sorted(orderByColumn(0) // TABLE_CAT
                .thenComparing(orderByColumn(1)) // TABLE_SCHEM
                .thenComparing(orderByColumn(2)) // TABLE_NAME
                .thenComparing(orderByColumn(16))) // ORDINAL_POSITION
            .toArray(Object[][]::new));
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "columns", "TABLE_CAT", 
            "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "SMALLINT:KEY_SEQ", "PK_NAME");
        Metadata dbMetadata = getDatabaseMetadata();
        return toResultSet(metadata, Stream.concat(Optional.ofNullable(dbMetadata.getKeyspace(schema)).stream(), 
                Optional.ofNullable(dbMetadata.getKeyspace("\"" + schema + "\"")).stream())
            .flatMap(ks -> Stream.concat(Optional.ofNullable(ks.getTable(table)).stream(), 
                Optional.ofNullable(ks.getTable("\"" + table + "\"")).stream()))
            .flatMap(tb -> IntStream.range(0, tb.getPrimaryKey().size())
                .mapToObj(i -> {
                    ColumnMetadata col = tb.getPrimaryKey().get(i);
                    String keyspace = tb.getKeyspace().getName();
                    return new Object[]{
                            "",  // "TABLE_CAT
                            keyspace, // "TABLE_SCHEMA"
                            tb.getName(), // "TABLE_NAME"
                            col.getName(), // "COLUMN_NAME"
                            (short)i, // "KEY_SEQ"
                            (tb.getPartitionKey().contains(col) ? "PK_" : "CK_") 
                                + keyspace.toUpperCase() + "_" + tb.getName().toUpperCase() // PK_NAME
                    };
                }))
            .toArray(Object[][]::new));
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
        throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "functions", "FUNCTION_CAT", 
            "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "SMALLINT:FUNCTION_TYPE");
        return toResultSet(metadata, getDatabaseMetadata().getKeyspaces().stream()
            .filter(ks -> matches(ks.getName(), schemaPattern))
            .flatMap(ks -> ks.getFunctions().stream())
            .filter(fn -> matches(fn.getSimpleName(), functionNamePattern))
            .map(fn -> new Object[]{
                    "", // "FUNCTION_CAT"
                    fn.getKeyspace().getName(), // "FUNCTION_SCHEM"
                    fn.getSimpleName(), // "FUNCTION_NAME"
                    "", // "REMARKS"
                    (short)functionResultUnknown, // "FUNCTION_TYPE"
                    fn.getSignature() // "SPECIFIC_NAME"
            })
            .sorted(orderByColumn(0)
                .thenComparing(orderByColumn(1))
                .thenComparing(orderByColumn(2))
                .thenComparing(orderByColumn(5)))
            .toArray(Object[][]::new));
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
        String columnNamePattern) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "functions", "FUNCTION_CAT", 
            "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "SMALLINT:COLUMN_TYPE", "INTEGER:DATA_TYPE",
            "TYPE_NAME", "INTEGER:PRECISION", "INTEGER:LENGTH", "SMALLINT:SCALE", "SMALLINT:RADIX",
            "SMALLINT:NULLABLE", "REMARKS", "INTEGER:CHAR_OCTET_LENGTH", "INTEGER:ORDINAL_POSITION", 
            "IS_NULLABLE", "SPECIFIC_NAME");
        return toResultSet(metadata, getDatabaseMetadata().getKeyspaces().stream()
            .filter(ks -> matches(ks.getName(), schemaPattern))
            .flatMap(ks -> ks.getFunctions().stream())
            .filter(fn -> matches(fn.getSimpleName(), schemaPattern))
            .flatMap(fn -> {
                AtomicInteger index = new AtomicInteger();
                return Stream.concat(fn.getArguments().entrySet().stream(), 
                        Stream.of(new SimpleEntry<String, DataType>("RESULT", fn.getReturnType())))
                    .filter(e -> matches(e.getKey(), columnNamePattern))
                    .map(e -> {
                        SQLType type = ColumnTypes.fromCqlType(e.getValue(), ColumnType::getSqlType);
                        return new Object[]{
                                "",     // "FUNCTION_CAT"
                                fn.getKeyspace().getName(), // "FUNCTION_SCHEM"
                                fn.getSimpleName(), // "FUNCTION_NAME"
                                e.getKey(), // "COLUMN_NAME"
                                "RESULT".equals(e.getKey()) ? functionColumnOut : functionColumnIn,    // "COLUMN_TYPE"
                                type.getVendorTypeNumber(), // "DATA_TYPE"
                                type.getName(), // "TYPE_NAME"
                                0,      // "PRECISION"
                                800,    // "LENGTH"
                                null,   // "SCALE"
                                (short)10, // "RADIX"
                                functionNullableUnknown, // "NULLABLE"
                                "",     // "REMARKS"
                                800,    // "CHAR_OCTET_LENGTH"
                                index.incrementAndGet(), // "ORDINAL_POSITION"
                                "",   // "IS_NULLABLE"
                                fn.getSignature() // "SPECIFIC_NAME"
                        };
                    });
            })
            .sorted(orderByColumn(0)
                .thenComparing(orderByColumn(1))
                .thenComparing(orderByColumn(2))
                .thenComparing(orderByColumn(16)))
            .toArray(Object[][]::new));
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "types", "TYPE_NAME", 
            "INTEGER:DATA_TYPE", "INTEGER:PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS",
            "SMALLINT:NULLABLE", "BOOLEAN:CASE_SENSITIVE", "SMALLINT:SEARCHABLE", "BOOLEAN:UNSIGNED_ATTRIBUTE", 
            "BOOLEAN:FIXED_PREC_SCALE", "BOOLEAN:AUTO_INCREMENT", "LOCAL_TYPE_NAME", "SMALLINT:MINIMUM_SCALE", 
            "SMALLINT:MAXIMUM_SCALE", "INTEGER:SQL_DATA_TYPE", "INTEGER:SQL_DATETIME_SUB", "INTEGER:NUM_PREC_RADIX");
        return toResultSet(metadata, Arrays.stream(ColumnTypes.values()).map(t -> {
                return new Object[] { 
                        t.getCqlType().getName().name(), // "TYPE_NAME"
                        t.getSqlType().getName(), // "DATA_TYPE"
                        0, // "PRECISION"
                        isTextType(t) ? "'" : null, // "LITERAL_PREFIX"
                        isTextType(t) ? "'" : null, // "LITERAL_SUFFIX"
                        null, // "CREATE_PARAMS"
                        typeNullableUnknown, // "NULLABLE"
                        true, // "CASE_SENSITIVE"
                        typePredNone, // "SEARCHABLE"
                        false, // "UNSIGNED_ATTRIBUTE"
                        false, // "FIXED_PREC_SCALE"
                        false, // "AUTO_INCREMENT"
                        null, // "LOCAL_TYPE_NAME"
                        0, // "MINIMUM_SCALE"
                        10, // "MAXIMUM_SCALE"
                        0, // "SQL_DATA_TYPE"
                        0, // "SQL_DATETIME_SUB"
                        10 // "NUM_PREC_RADIX"
                };
            }).toArray(Object[][]::new));
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
        throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "types", "TYPE_CAT", 
            "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "INTEGER:DATA_TYPE", "REMARKS",
            "SMALLINT:BASE_TYPE");
        if(types != null && Arrays.binarySearch(types, JDBCType.STRUCT.getVendorTypeNumber()) == -1) {
            return toResultSet(metadata);
        }
        return toResultSet(metadata, getDatabaseMetadata().getKeyspaces().stream()
            .filter(ks -> matches(ks.getName(), schemaPattern))
            .flatMap(ks -> ks.getUserTypes().stream())
            .filter(tp -> matches(tp.getTypeName(), typeNamePattern))
            .map(tp -> new Object[] {
                    "", // "TYPE_CAT"
                    tp.getKeyspace(), // "TYPE_SCHEM"
                    tp.getTypeName(), // "TYPE_NAME"
                    "java.sql.Struct", // "CLASS_NAME"
                    JDBCType.STRUCT.getVendorTypeNumber(), // "DATA_TYPE"
                    "", // "REMARKS"
                    null // "BASE_TYPE"
            })
            .sorted(orderByColumn(0)
                .thenComparing(orderByColumn(1))
                .thenComparing(orderByColumn(2)))
            .toArray(Object[][]::new));
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
        String attributeNamePattern) throws SQLException {
        CResultSetMetaData metadata = createMetadata("system_schema", "types", "TYPE_CAT", 
            "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "INTEGER:DATA_TYPE", "ATTR_TYPE_NAME",
            "INTEGER:ATTR_SIZE", "INTEGER:DECIMAL_DIGITS", "INTEGER:NUM_PREC_RADIX", "INTEGER:NULLABLE",
            "REMARKS", "ATTR_DEF", "INTEGER:SQL_DATA_TYPE", "INTEGER:SQL_DATETIME_SUB", "INTEGER:CHAR_OCTET_LENGTH",
            "INTEGER:ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SMALLINT:SOURCE_DATA_TYPE");
        return toResultSet(metadata, getDatabaseMetadata().getKeyspaces().stream()
            .filter(ks -> matches(ks.getName(), schemaPattern))
            .flatMap(ks -> ks.getUserTypes().stream())
            .filter(tp -> matches(tp.getTypeName(), typeNamePattern))
            .flatMap(tp -> {
                AtomicInteger index = new AtomicInteger();
                return tp.getFieldNames().stream()
                    .map(name -> {
                        SQLType type = ColumnTypes.fromCqlType(tp.getFieldType(name), ColumnType::getSqlType);
                        return new Object[]{
                             "", // "TYPE_CAT"
                             tp.getKeyspace(), // "TYPE_SCHEM"
                             tp.getTypeName(), // "TYPE_NAME"
                             name,             // "ATTR_NAME"
                             type.getVendorTypeNumber(), // DATA_TYPE
                             type.getName(),   // "ATTR_TYPE_NAME"
                             800,             // "ATTR_SIZE"
                             null,             // "DECIMAL_DIGITS"
                             10,               // "NUM_PREC_RADIX"
                             (int) attributeNullableUnknown, // NULLABLE
                             "",               // "REMARKS"
                             null,             // "ATTR_DEF"
                             type.getVendorTypeNumber(),  // "SQL_DATA_TYPE"
                             null,             // "SQL_DATETIME_SUB"
                             800, // "CHAR_OCTET_LENGTH"
                             index.incrementAndGet(), // ORDINAL_POSITION
                             "",               // "IS_NULLABLE"
                             null,             // "SCOPE_CATALOG"
                             null,             // "SCOPE_SCHEMA"
                             null,             // "SCOPE_TABLE"
                             null,             // "SOURCE_DATA_TYPE"
                        };
                    });
            })
            .sorted(orderByColumn(0)
                .thenComparing(orderByColumn(1))
                .thenComparing(orderByColumn(2)))
            .toArray(Object[][]::new));
    }
    
    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        CResultSetMetaData metadata = createMetadata("system", "local", "NAME", "INTEGER::MAX_LEN",
            "DEFAULT_VALUE", "DESCRIPTION");
        return toResultSet(metadata, Arrays.stream(CClientInfo.desribe())
            .map(def -> new Object[]{
                    def.getName(), // "NAME"
                    800, // "MAX_LEN"
                    def.getDefaultValue(),  // "DEFAULT_VALUE"
                    def.getDesciption()     // "DEFAULT_VALUE"
            })
            .toArray(Object[][]::new));
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    private Metadata getDatabaseMetadata() {
        return connection.getSession().getCluster().getMetadata();
    }

    private CResultSet toResultSet(CResultSetMetaData metadata) {
        return new CResultSet(new CPreparedStatement(connection), metadata, Collections.emptyList());
    }

    private CResultSet toResultSet(CResultSetMetaData metadata, Object[][] rows) {
        return new CResultSet(new CPreparedStatement(connection), metadata, IntStream.range(0, rows.length)
            .mapToObj(i -> new CResultSet.Row(i, rows[i]))
            .collect(Collectors.toList()));
    }

    private String getTableType(String tableName) {
        return tableName.startsWith("system_") || tableName.startsWith("dse_") ? "SYSTEM TABLE" : "TABLE";
    }
    
    private boolean matches(String value, String pattern) {
        return pattern == null || value.toLowerCase().matches(pattern.toLowerCase().replace("%", ".*"));
    }

    private Comparator<Object[]> orderByColumn(int idx) {
        return Comparator.comparing(vals -> String.valueOf(vals[idx]));
    }
    
    private CResultSetMetaData createMetadata(String schemaName, String tableName, String... columns) {
        String[] columnNames = Arrays.stream(columns)
                .map(s -> s.split(":"))
                .map(parts -> parts.length == 1 ? parts[0] : parts[1])
                .toArray(String[]::new);
        DataType[] dataTypes = Arrays.stream(columns)
                .map(s -> s.split(":"))
                .map(parts -> parts.length == 1 ? DataType.text() : ColumnTypes.forName(parts[0], ColumnType::getCqlType))
                .toArray(DataType[]::new);
        return new CResultSetMetaData("system_schema", "keyspaces", columnNames, dataTypes);
    }
    
    private boolean isTextType(ColumnType t) {
        return t.getCqlType() == DataType.text() || t.getCqlType() == DataType.varchar();
    }
        
    private String getTableOptionString(TableMetadata tableMetadata) {
        StringBuilder sb = new StringBuilder();
        TableOptionsMetadata options = tableMetadata.getOptions();
        if( options != null ){
            sb.append("bloom_filter_fp_chance = " ).append(options.getBloomFilterFalsePositiveChance() ).
                    append("\n AND caching = '").append( options.getCaching()).append("'").
                    append("\n  AND comment = '").append( options.getComment()).append("'").
                    append("\n  AND compaction = ").append( options.getCompaction()).
                    append("\n  AND compression = ").append( options.getCompression()).
                    append("\n  AND dclocal_read_repair_chance = " ).append(options.getLocalReadRepairChance() ).
                    append("\n  AND default_time_to_live = " ).append(options.getDefaultTimeToLive() ).
                    append("\n  AND gc_grace_seconds = " ).append(options.getGcGraceInSeconds() ).
                    append("\n  AND max_index_interval = " ).append(options.getMaxIndexInterval() ).
                    append("\n  AND memtable_flush_period_in_ms = " ).append(options.getMemtableFlushPeriodInMs() ).
                    append("\n  AND min_index_interval = " ).append(options.getMinIndexInterval() ).
                    append("\n  AND read_repair_chance = " ).append(options.getReadRepairChance() ).
                    append("\n  AND speculative_retry = '").append(options.getSpeculativeRetry() ).append("'");
        }
        return sb.toString();
    }

}
