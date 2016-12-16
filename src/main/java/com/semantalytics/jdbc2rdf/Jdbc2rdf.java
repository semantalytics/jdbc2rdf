package com.semantalytics.jdbc2rdf;




import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.semantalytics.jdbc2rdf.JDBC.*;

public class Jdbc2rdf {
    private final ModelBuilder modelBuilder = new ModelBuilder();
    private final DatabaseMetaData metaData;
    private final ValueFactory valueFactory = SimpleValueFactory.getInstance();

    public Jdbc2rdf(final String jdbcUrl) throws SQLException {

        metaData = DriverManager.getConnection(jdbcUrl).getMetaData();
    }

    public static void main(String... args) throws SQLException {

        if (args.length != 1) {

            System.out.println("usage: Jdbc2rdf <jdbc-url>");
            System.exit(1);
        }


        Jdbc2rdf jdbc2rdf = new Jdbc2rdf(args[0]);
        jdbc2rdf.parse(args[0]);
        jdbc2rdf.print();
    }


    public void print() {
        RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, System.out);

        try {
            writer.startRDF();
            for (Statement st : modelBuilder.build()) {
                writer.handleStatement(st);
            }
            writer.endRDF();
        } catch (RDFHandlerException e) {
            // oh no, do something!
        }
    }

    void parseTable(final Resource subject, final ResultSet tableMetadata) throws SQLException {
        modelBuilder.defaultGraph().add(subject, valueFactory.createIRI(""), tableMetadata.getString("TABLE_NAME"));
    }

    void parseTables(final ResultSet tablesMetadata) throws SQLException {
        while(tablesMetadata.next()) {
            parseTable(valueFactory.createBNode(),  tablesMetadata);
        }
    }

    public void parse(final String subject) throws SQLException {

        parseTables(metaData.getTables(null, null, "%", null));

        modelBuilder.defaultGraph().add(subject, ALL_PROCEEDURES_ARE_CALLABLE, metaData.allProceduresAreCallable());
        modelBuilder.defaultGraph().add(subject, ALL_TABLES_ARE_SELECTABLE, metaData.allTablesAreSelectable());
        modelBuilder.defaultGraph().add(subject, AUTO_COMMIT_FAILURE_CLOSES_ALL_RESULTS_SETS, metaData.autoCommitFailureClosesAllResultSets());
        modelBuilder.defaultGraph().add(subject, DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT, metaData.dataDefinitionCausesTransactionCommit());
        modelBuilder.defaultGraph().add(subject, DATA_DEFINITION_IGNORED_IN_TRANSACTIONS, metaData.dataDefinitionIgnoredInTransactions());
        modelBuilder.defaultGraph().add(subject, DOES_MAX_ROW_SIZE_INCLUDE_BLOBS, metaData.doesMaxRowSizeIncludeBlobs());
        modelBuilder.defaultGraph().add(subject, JDBC_MAJOR_VERSION, metaData.getJDBCMajorVersion());
        modelBuilder.defaultGraph().add(subject, JDBC_MINOR_VERSION, metaData.getJDBCMinorVersion());
        modelBuilder.defaultGraph().add(subject, MAX_BINARY_LITERAL_LENGTH, metaData.getMaxBinaryLiteralLength());
        modelBuilder.defaultGraph().add(subject, MAX_CATALOG_NAME_LENGTH, metaData.getMaxCatalogNameLength());
        modelBuilder.defaultGraph().add(subject, MAX_CHAR_LITERAL_LENGTH, metaData.getMaxCharLiteralLength());
        modelBuilder.defaultGraph().add(subject, MAX_COLUMN_NAME_LENGTH, metaData.getMaxColumnNameLength());
        modelBuilder.defaultGraph().add(subject, MAX_COLUMNS_IN_GROUP_BY, metaData.getMaxColumnsInGroupBy());
        modelBuilder.defaultGraph().add(subject, MAX_COLUMNS_IN_INDEX, metaData.getMaxColumnsInIndex());
        modelBuilder.defaultGraph().add(subject, MAX_COLUMNS_IN_ORDER_BY, metaData.getMaxColumnsInOrderBy());
        modelBuilder.defaultGraph().add(subject, MAX_COLUMNS_IN_SELECT, metaData.getMaxColumnsInSelect());
        modelBuilder.defaultGraph().add(subject, MAX_COLUMNS_IN_TABLE, metaData.getMaxColumnsInTable());
        modelBuilder.defaultGraph().add(subject, MAX_CONNECTIONS, metaData.getMaxConnections());
        modelBuilder.defaultGraph().add(subject, CATALOG_SEPARATOR, metaData.getCatalogSeparator());
        modelBuilder.defaultGraph().add(subject, CATALOG_TERM, metaData.getCatalogTerm());
        modelBuilder.defaultGraph().add(subject, DATABASE_MAJOR_VERSION, metaData.getDatabaseMajorVersion());
        modelBuilder.defaultGraph().add(subject, DATABASE_MINOR_VERSION, metaData.getDatabaseMinorVersion());
        modelBuilder.defaultGraph().add(subject, DATABASE_PRODUCT_NAME, metaData.getDatabaseProductName());
        modelBuilder.defaultGraph().add(subject, DATABASE_PRODUCT_VERSION, metaData.getDatabaseProductVersion());
        modelBuilder.defaultGraph().add(subject, DEFAULT_TRANSACTION_ISOLATION, metaData.getDefaultTransactionIsolation());
        modelBuilder.defaultGraph().add(subject, DRIVER_MAJOR_VERSION, metaData.getDriverMajorVersion());
        modelBuilder.defaultGraph().add(subject, DRIVER_MINOR_VERSION, metaData.getDriverMinorVersion());
        modelBuilder.defaultGraph().add(subject, DRIVER_NAME, metaData.getDriverName());
        modelBuilder.defaultGraph().add(subject, DRIVER_VERSION, metaData.getDriverMinorVersion());
        modelBuilder.defaultGraph().add(subject, EXTRA_NAME_CHARACTERS, metaData.getExtraNameCharacters());
        modelBuilder.defaultGraph().add(subject, IDENTIFIER_QUOTE_STRING, metaData.getIdentifierQuoteString());
        modelBuilder.defaultGraph().add(subject, MAX_CURSOR_NAME_LENGTH, metaData.getMaxCursorNameLength());
        modelBuilder.defaultGraph().add(subject, MAX_INDEX_LENGTH, metaData.getMaxIndexLength());
        modelBuilder.defaultGraph().add(subject, MAX_PROCEDURE_NAME_LENGTH, metaData.getMaxProcedureNameLength());
        modelBuilder.defaultGraph().add(subject, MAX_ROW_SIZE, metaData.getMaxRowSize());
        modelBuilder.defaultGraph().add(subject, MAX_SCHEMA_NAME_LENGTH, metaData.getMaxSchemaNameLength());
        modelBuilder.defaultGraph().add(subject, MAX_STATEMENTLENGTH, metaData.getMaxStatementLength());
        modelBuilder.defaultGraph().add(subject, MAX_STATEMENTS, metaData.getMaxStatements());
        modelBuilder.defaultGraph().add(subject, MAX_TABLE_NAME_LENGTH, metaData.getMaxTableNameLength());
        modelBuilder.defaultGraph().add(subject, MAX_TABLES_IN_SELECT, metaData.getMaxTablesInSelect());
        modelBuilder.defaultGraph().add(subject, MAX_USER_NAME_LENGTH, metaData.getMaxUserNameLength());
        modelBuilder.defaultGraph().add(subject, NUMERIC_FUNCTIONS, metaData.getNumericFunctions());
        modelBuilder.defaultGraph().add(subject, PROCEDURE_TERM, metaData.getProcedureTerm());
        modelBuilder.defaultGraph().add(subject, RESULT_SET_HOLDABILITY, metaData.getResultSetHoldability());
        modelBuilder.defaultGraph().add(subject, SCHEMA_TERM, metaData.getSchemaTerm());
        modelBuilder.defaultGraph().add(subject, SEARCH_STRING_ESCAPE, metaData.getSearchStringEscape());
        modelBuilder.defaultGraph().add(subject, SQL_KEYWORDS, metaData.getSQLKeywords());
        modelBuilder.defaultGraph().add(subject, SQL_STATE_TYPE, metaData.getSQLStateType());
        modelBuilder.defaultGraph().add(subject, STRING_FUNCTIONS, metaData.getStringFunctions());
        modelBuilder.defaultGraph().add(subject, SYSTEM_FUNCTIONS, metaData.getSystemFunctions());
        modelBuilder.defaultGraph().add(subject, TIME_DATE_FUNCTIONS, metaData.getTimeDateFunctions());
        modelBuilder.defaultGraph().add(subject, URL, metaData.getURL());
        modelBuilder.defaultGraph().add(subject, USER_NAME, metaData.getUserName());
        modelBuilder.defaultGraph().add(subject, CATALOG_AT_START, metaData.isCatalogAtStart());
        modelBuilder.defaultGraph().add(subject, READ_ONLY, metaData.isReadOnly());
        modelBuilder.defaultGraph().add(subject, LOCATORS_UPDATE_COPY, metaData.locatorsUpdateCopy());
        modelBuilder.defaultGraph().add(subject, NULL_PLUS_NON_NULL_IS_NULL, metaData.nullPlusNonNullIsNull());
        modelBuilder.defaultGraph().add(subject, NULLS_ARE_SORTED_AT_END, metaData.nullsAreSortedAtEnd());
        modelBuilder.defaultGraph().add(subject, NULLS_ARE_SORTED_AT_START, metaData.nullsAreSortedAtStart());
        modelBuilder.defaultGraph().add(subject, NULLS_ARE_SORTED_HIGH, metaData.nullsAreSortedHigh());
        modelBuilder.defaultGraph().add(subject, NULLS_ARE_SORTED_LOW, metaData.nullsAreSortedLow());
        modelBuilder.defaultGraph().add(subject, STORES_LOWER_CASE_IDENTIFIERS, metaData.storesLowerCaseIdentifiers());
        modelBuilder.defaultGraph().add(subject, STORES_LOWER_CASE_QUOTED_IDENTIFIERS, metaData.storesLowerCaseQuotedIdentifiers());
        modelBuilder.defaultGraph().add(subject, STORES_MIXED_CASE_IDENTIFIERS, metaData.storesMixedCaseIdentifiers());
        modelBuilder.defaultGraph().add(subject, STORES_MIXED_CASE_QUOTED_IDENTIFIERS, metaData.storesMixedCaseQuotedIdentifiers());
        modelBuilder.defaultGraph().add(subject, STORES_UPPER_CASE_IDENTIFIERS, metaData.storesUpperCaseIdentifiers());
        modelBuilder.defaultGraph().add(subject, STORES_UPPER_CASE_QUOTED_IDENTIFIERS, metaData.storesUpperCaseQuotedIdentifiers());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_ALTER_TABLE_WITH_ADD_COLUMN, metaData.supportsAlterTableWithAddColumn());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_ALTER_TABLE_WITH_DROP_COLUMN, metaData.supportsAlterTableWithDropColumn());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_ANSI92_ENTRY_LEVEL_SQL, metaData.supportsANSI92EntryLevelSQL());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_ANSI92_FULL_SQL, metaData.supportsANSI92FullSQL());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_ANSI92_INTERMEDIATE_SQL, metaData.supportsANSI92IntermediateSQL());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_BATCH_UPDATES, metaData.supportsBatchUpdates());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_CATALOGS_IN_DATA_MANIPULATION, metaData.supportsCatalogsInDataManipulation());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_CATALOGS_IN_INDEX_DEFINITIONS, metaData.supportsCatalogsInPrivilegeDefinitions());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_CATALOGS_IN_PRIVILEGE_DEFINITIONS, metaData.supportsCatalogsInPrivilegeDefinitions());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_CATALOGS_IN_PROCEDURE_CALLS, metaData.supportsCatalogsInProcedureCalls());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_CATALOGS_IN_TABLE_DEFINITIONS, metaData.supportsCatalogsInTableDefinitions());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_COLUMN_ALIASING, metaData.supportsColumnAliasing());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_CORE_SQL_GRAMMAR, metaData.supportsCoreSQLGrammar());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_CORRELATED_SUBQUERIES, metaData.supportsCorrelatedSubqueries());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_DATA_DEFINITION_AND_DATA_MANIPULATION_TRANSACTIONS, metaData.supportsDataDefinitionAndDataManipulationTransactions());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_DATA_MANIPULATION_TRANSACTIONS_ONLY, metaData.supportsDataManipulationTransactionsOnly());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES, metaData.supportsDifferentTableCorrelationNames());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_EXPRESSIONS_IN_ORDER_BY, metaData.supportsExpressionsInOrderBy());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_EXTENDED_SQL_GRAMMAR, metaData.supportsExtendedSQLGrammar());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_FULL_OUTER_JOINS, metaData.supportsFullOuterJoins());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_GET_GENERATED_KEYS, metaData.supportsGetGeneratedKeys());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_GROUP_BY, metaData.supportsGroupBy());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_GROUP_BY_BEYOND_SELECT, metaData.supportsGroupByBeyondSelect());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_GROUP_BY_UNRELATED, metaData.supportsGroupByUnrelated());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY, metaData.supportsIntegrityEnhancementFacility());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_LIKE_ESCAPE_CLAUSE, metaData.supportsLikeEscapeClause());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_LIMITED_OUTER_JOINS, metaData.supportsLimitedOuterJoins());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_MINIMUM_SQL_GRAMMAR, metaData.supportsMinimumSQLGrammar());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_MIXED_CASE_IDENTIFIERS, metaData.supportsMixedCaseIdentifiers());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS, metaData.supportsMixedCaseQuotedIdentifiers());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_MULTIPLE_OPEN_RESULTS, metaData.supportsMultipleOpenResults());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_MULTIPLE_RESULT_SETS, metaData.supportsMultipleResultSets());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_MULTIPLE_TRANSACTIONS, metaData.supportsMultipleTransactions());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_NAMED_PARAMETERS, metaData.supportsNamedParameters());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_NON_NULLABLE_COLUMNS, metaData.supportsNonNullableColumns());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_OPEN_CURSORS_ACROSS_COMMIT, metaData.supportsOpenCursorsAcrossCommit());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_OPEN_CURSORS_ACROSS_ROLLBACK, metaData.supportsOpenCursorsAcrossRollback());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_OPEN_STATEMENTS_ACROSS_COMMIT, metaData.supportsOpenStatementsAcrossCommit());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_OPEN_STATEMENTS_ACROSS_ROLLBACK, metaData.supportsOpenStatementsAcrossRollback());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_ORDER_BY_UNRELATED, metaData.supportsOrderByUnrelated());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_OUTER_JOINS, metaData.supportsOuterJoins());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_POSITIONED_DELETE, metaData.supportsPositionedDelete());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_POSITIONED_UPDATE, metaData.supportsPositionedUpdate());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SAVEPOINTS, metaData.supportsSavepoints());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SCHEMAS_IN_DATA_MANIPULATION, metaData.supportsSchemasInDataManipulation());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SCHEMAS_IN_INDEX_DEFINITIONS, metaData.supportsSchemasInIndexDefinitions());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SCHEMAS_IN_PRIVILEGE_DEFINITIONS, metaData.supportsSchemasInPrivilegeDefinitions());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SCHEMAS_IN_PROCEDURE_CALLS, metaData.supportsSchemasInProcedureCalls());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SCHEMAS_IN_TABLE_DEFINITIONS, metaData.supportsSchemasInTableDefinitions());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SELECT_FOR_UPDATE, metaData.supportsSelectForUpdate());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_STATEMENT_POOLING, metaData.supportsStatementPooling());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_STORED_FUNCTIONS_USING_CALL_SYNTAX, metaData.supportsStoredFunctionsUsingCallSyntax());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_STORED_PROCEDURES, metaData.supportsStoredProcedures());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SUBQUERIES_IN_COMPARISONS, metaData.supportsSubqueriesInComparisons());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SUBQUERIES_IN_EXISTS, metaData.supportsSubqueriesInExists());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SUBQUERIES_IN_INS, metaData.supportsSubqueriesInIns());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_SUBQUERIES_IN_QUANTIFIEDS, metaData.supportsSubqueriesInQuantifieds());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_TABLE_CORRELATION_NAMES, metaData.supportsTableCorrelationNames());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_TRANSACTIONS, metaData.supportsTransactions());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_UNION, metaData.supportsUnion());
        modelBuilder.defaultGraph().add(subject, SUPPORTS_UNION_ALL, metaData.supportsUnionAll());
        modelBuilder.defaultGraph().add(subject, USES_LOCAL_FILE_PER_TABLE, metaData.usesLocalFilePerTable());
        modelBuilder.defaultGraph().add(subject, USES_LOCAL_FILES, metaData.usesLocalFiles());
    }

}
