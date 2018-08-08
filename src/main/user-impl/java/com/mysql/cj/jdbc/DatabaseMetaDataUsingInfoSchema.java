/*
 * Copyright (c) 2005, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.result.ResultSetFactory;
import com.mysql.cj.result.Field;
import com.mysql.cj.util.LRUCache;
import com.mysql.cj.util.StringUtils;

/**
 * DatabaseMetaData implementation that uses INFORMATION_SCHEMA
 */
public class DatabaseMetaDataUsingInfoSchema extends DatabaseMetaData {

    private static Map<ServerVersion, String> keywordsCache = Collections.synchronizedMap(new LRUCache<>(10));

    protected enum FunctionConstant {
        // COLUMN_TYPE values
        FUNCTION_COLUMN_UNKNOWN, FUNCTION_COLUMN_IN, FUNCTION_COLUMN_INOUT, FUNCTION_COLUMN_OUT, FUNCTION_COLUMN_RETURN, FUNCTION_COLUMN_RESULT,
        // NULLABLE values
        FUNCTION_NO_NULLS, FUNCTION_NULLABLE, FUNCTION_NULLABLE_UNKNOWN;
    }

    protected DatabaseMetaDataUsingInfoSchema(JdbcConnection connToSet, String databaseToSet, ResultSetFactory resultSetFactory) throws SQLException {
        super(connToSet, databaseToSet, resultSetFactory);
    }

    protected ResultSet executeMetadataQuery(java.sql.PreparedStatement pStmt) throws SQLException {
        ResultSet rs = pStmt.executeQuery();
        ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).setOwningStatement(null);

        return rs;
    }

    @Override
    public java.sql.ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        StringBuilder sqlBuf = new StringBuilder("SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME,");
        sqlBuf.append(" COLUMN_NAME, NULL AS GRANTOR, GRANTEE, PRIVILEGE_TYPE AS PRIVILEGE, IS_GRANTABLE FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES WHERE");
        if (catalog != null) {
            sqlBuf.append(" TABLE_SCHEMA LIKE ? AND");
        }

        sqlBuf.append(" TABLE_NAME =?");
        if (columnNamePattern != null) {
            sqlBuf.append(" AND COLUMN_NAME LIKE ?");
        }
        sqlBuf.append(" ORDER BY COLUMN_NAME, PRIVILEGE_TYPE");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());
            int nextId = 1;
            pStmt.setString(nextId++, catalog);
            pStmt.setString(nextId++, table);
            if (columnNamePattern != null) {
                pStmt.setString(nextId, columnNamePattern);
            }

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition()
                    .setFields(new Field[] { new Field("", "TABLE_CAT", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 64),
                            new Field("", "TABLE_SCHEM", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 1),
                            new Field("", "TABLE_NAME", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 64),
                            new Field("", "COLUMN_NAME", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 64),
                            new Field("", "GRANTOR", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 77),
                            new Field("", "GRANTEE", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 77),
                            new Field("", "PRIVILEGE", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 64),
                            new Field("", "IS_GRANTABLE", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 3) });

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableName, String columnNamePattern) throws SQLException {
        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        StringBuilder sqlBuf = new StringBuilder("SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME,");

        appendJdbcTypeMappingQuery(sqlBuf, "DATA_TYPE", "COLUMN_TYPE");
        sqlBuf.append(" AS DATA_TYPE, ");

        sqlBuf.append("UPPER(CASE");
        sqlBuf.append(
                " WHEN LOCATE('UNSIGNED', UPPER(COLUMN_TYPE)) != 0 AND LOCATE('UNSIGNED', UPPER(DATA_TYPE)) = 0 AND LOCATE('SET', UPPER(DATA_TYPE)) <> 1 AND LOCATE('ENUM', UPPER(DATA_TYPE)) <> 1 THEN CONCAT(DATA_TYPE, ' UNSIGNED')");
        if (this.tinyInt1isBit) {
            sqlBuf.append(" WHEN UPPER(DATA_TYPE)='TINYINT' THEN CASE");
            if (this.transformedBitIsBoolean) {
                sqlBuf.append(" WHEN LOCATE('(1)', COLUMN_TYPE) != 0 THEN 'BOOLEAN'");
            } else {
                sqlBuf.append(" WHEN LOCATE('(1)', COLUMN_TYPE) != 0 THEN 'BIT'");
            }
            sqlBuf.append(" WHEN LOCATE('UNSIGNED', UPPER(COLUMN_TYPE)) != 0 AND LOCATE('UNSIGNED', UPPER(DATA_TYPE)) = 0 THEN 'TINYINT UNSIGNED'");
            sqlBuf.append(" ELSE DATA_TYPE END ");
        }

        // spatial data types
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='POINT' THEN 'GEOMETRY'");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='LINESTRING' THEN 'GEOMETRY'");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='POLYGON' THEN 'GEOMETRY'");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='MULTIPOINT' THEN 'GEOMETRY'");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='MULTILINESTRING' THEN 'GEOMETRY'");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='MULTIPOLYGON' THEN 'GEOMETRY'");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='GEOMETRYCOLLECTION' THEN 'GEOMETRY'");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='GEOMCOLLECTION' THEN 'GEOMETRY'");

        sqlBuf.append(" ELSE UPPER(DATA_TYPE) END) AS TYPE_NAME,");

        sqlBuf.append("UPPER(CASE");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='DATE' THEN 10"); // supported range is '1000-01-01' to '9999-12-31'
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='TIME' THEN 16"); // supported range is '-838:59:59.000000' to '838:59:59.000000'
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='DATETIME' THEN 26"); // supported range is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='TIMESTAMP' THEN 26"); // supported range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999' UTC
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='YEAR' THEN 4");
        if (this.tinyInt1isBit) {
            sqlBuf.append(" WHEN UPPER(DATA_TYPE)='TINYINT' AND LOCATE('(1)', COLUMN_TYPE) != 0 THEN 1");
        }
        // workaround for Bug#69042 (16712664), "MEDIUMINT PRECISION/TYPE INCORRECT IN INFORMATION_SCHEMA.COLUMNS", I_S bug returns NUMERIC_PRECISION=7 for MEDIUMINT UNSIGNED when it must be 8.
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='MEDIUMINT' AND LOCATE('UNSIGNED', UPPER(COLUMN_TYPE)) != 0 THEN 8");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='JSON' THEN 1073741824"); // JSON columns is limited to the value of the max_allowed_packet system variable (max value 1073741824)

        // spatial data types
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='GEOMETRY' THEN 65535");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='POINT' THEN 65535");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='LINESTRING' THEN 65535");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='POLYGON' THEN 65535");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='MULTIPOINT' THEN 65535");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='MULTILINESTRING' THEN 65535");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='MULTIPOLYGON' THEN 65535");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='GEOMETRYCOLLECTION' THEN 65535");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='GEOMCOLLECTION' THEN 65535");

        sqlBuf.append(" WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN NUMERIC_PRECISION");
        sqlBuf.append(" WHEN CHARACTER_MAXIMUM_LENGTH > ");
        sqlBuf.append(Integer.MAX_VALUE);
        sqlBuf.append(" THEN ");
        sqlBuf.append(Integer.MAX_VALUE);
        sqlBuf.append(" ELSE CHARACTER_MAXIMUM_LENGTH");
        sqlBuf.append(" END) AS COLUMN_SIZE,");

        sqlBuf.append(maxBufferSize);
        sqlBuf.append(" AS BUFFER_LENGTH,");

        sqlBuf.append("UPPER(CASE");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='DECIMAL' THEN NUMERIC_SCALE");
        sqlBuf.append(" WHEN UPPER(DATA_TYPE)='FLOAT' OR UPPER(DATA_TYPE)='DOUBLE' THEN");
        sqlBuf.append(" CASE WHEN NUMERIC_SCALE IS NULL THEN 0");
        sqlBuf.append(" ELSE NUMERIC_SCALE END");
        sqlBuf.append(" ELSE NULL END) AS DECIMAL_DIGITS,");

        sqlBuf.append("10 AS NUM_PREC_RADIX,");

        sqlBuf.append("UPPER(CASE");
        sqlBuf.append(" WHEN IS_NULLABLE='NO' THEN ");
        sqlBuf.append(columnNoNulls);
        sqlBuf.append(" ELSE CASE WHEN IS_NULLABLE='YES' THEN ");
        sqlBuf.append(columnNullable);
        sqlBuf.append(" ELSE ");
        sqlBuf.append(columnNullableUnknown);
        sqlBuf.append(" END END) AS NULLABLE,");

        sqlBuf.append("COLUMN_COMMENT AS REMARKS,");
        sqlBuf.append("COLUMN_DEFAULT AS COLUMN_DEF,");
        sqlBuf.append("0 AS SQL_DATA_TYPE,");
        sqlBuf.append("0 AS SQL_DATETIME_SUB,");

        sqlBuf.append("CASE WHEN CHARACTER_OCTET_LENGTH > ");
        sqlBuf.append(Integer.MAX_VALUE);
        sqlBuf.append(" THEN ");
        sqlBuf.append(Integer.MAX_VALUE);
        sqlBuf.append(" ELSE CHARACTER_OCTET_LENGTH END AS CHAR_OCTET_LENGTH,");

        sqlBuf.append("ORDINAL_POSITION, IS_NULLABLE, NULL AS SCOPE_CATALOG, NULL AS SCOPE_SCHEMA, NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE,");
        sqlBuf.append("IF (EXTRA LIKE '%auto_increment%','YES','NO') AS IS_AUTOINCREMENT, ");
        sqlBuf.append("IF (EXTRA LIKE '%GENERATED%','YES','NO') AS IS_GENERATEDCOLUMN ");

        sqlBuf.append("FROM INFORMATION_SCHEMA.COLUMNS");

        StringBuilder conditionBuf = new StringBuilder();

        if (catalog != null) {
            conditionBuf.append(
                    "information_schema".equalsIgnoreCase(catalog) || "performance_schema".equalsIgnoreCase(catalog) || !StringUtils.hasWildcards(catalog)
                            ? " TABLE_SCHEMA = ?" : " TABLE_SCHEMA LIKE ?");
        }
        if (tableName != null) {
            if (conditionBuf.length() > 0) {
                conditionBuf.append(" AND");
            }
            conditionBuf.append(StringUtils.hasWildcards(tableName) ? " TABLE_NAME LIKE ?" : " TABLE_NAME = ?");
        }
        if (columnNamePattern != null) {
            if (conditionBuf.length() > 0) {
                conditionBuf.append(" AND");
            }
            conditionBuf.append(StringUtils.hasWildcards(columnNamePattern) ? " COLUMN_NAME LIKE ?" : " COLUMN_NAME = ?");
        }

        if (conditionBuf.length() > 0) {
            sqlBuf.append(" WHERE");
        }
        sqlBuf.append(conditionBuf);
        sqlBuf.append(" ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());

            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            if (tableName != null) {
                pStmt.setString(nextId++, tableName);
            }
            if (columnNamePattern != null) {
                pStmt.setString(nextId, columnNamePattern);
            }

            ResultSet rs = executeMetadataQuery(pStmt);

            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createColumnsFields());
            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public java.sql.ResultSet getCrossReference(String primaryCatalog, String primarySchema, String primaryTable, String foreignCatalog, String foreignSchema,
            String foreignTable) throws SQLException {
        if (primaryTable == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        if (primaryCatalog == null && this.nullCatalogMeansCurrent) {
            primaryCatalog = this.database;
        }

        if (foreignCatalog == null && this.nullCatalogMeansCurrent) {
            foreignCatalog = this.database;
        }

        primaryCatalog = this.pedantic ? primaryCatalog : StringUtils.unQuoteIdentifier(primaryCatalog, this.quotedId);
        foreignCatalog = this.pedantic ? foreignCatalog : StringUtils.unQuoteIdentifier(foreignCatalog, this.quotedId);

        StringBuilder sqlBuf = new StringBuilder(
                "SELECT A.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT,NULL AS PKTABLE_SCHEM, A.REFERENCED_TABLE_NAME AS PKTABLE_NAME,");
        sqlBuf.append(" A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME, A.TABLE_SCHEMA AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM,");
        sqlBuf.append(" A.TABLE_NAME AS FKTABLE_NAME, A.COLUMN_NAME AS FKCOLUMN_NAME, A.ORDINAL_POSITION AS KEY_SEQ,");
        sqlBuf.append(generateUpdateRuleClause());
        sqlBuf.append(" AS UPDATE_RULE,");
        sqlBuf.append(generateDeleteRuleClause());
        sqlBuf.append(" AS DELETE_RULE, A.CONSTRAINT_NAME AS FK_NAME,");
        sqlBuf.append(" (SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = A.REFERENCED_TABLE_SCHEMA");
        sqlBuf.append(" AND TABLE_NAME = A.REFERENCED_TABLE_NAME AND CONSTRAINT_TYPE IN ('UNIQUE','PRIMARY KEY') LIMIT 1) AS PK_NAME, ");
        sqlBuf.append(importedKeyNotDeferrable);
        sqlBuf.append(" AS DEFERRABILITY FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A");
        sqlBuf.append(" JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B USING (TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME) ");
        sqlBuf.append(generateOptionalRefContraintsJoin());
        sqlBuf.append("WHERE B.CONSTRAINT_TYPE = 'FOREIGN KEY'");
        if (primaryCatalog != null) {
            sqlBuf.append(" AND A.REFERENCED_TABLE_SCHEMA LIKE ?");
        }
        sqlBuf.append(" AND A.REFERENCED_TABLE_NAME=?");
        if (foreignCatalog != null) {
            sqlBuf.append(" AND A.TABLE_SCHEMA LIKE ?");
        }
        sqlBuf.append(" AND A.TABLE_NAME=?");
        sqlBuf.append(" ORDER BY A.TABLE_SCHEMA, A.TABLE_NAME, A.ORDINAL_POSITION");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());
            int nextId = 1;
            if (primaryCatalog != null) {
                pStmt.setString(nextId++, primaryCatalog);
            }
            pStmt.setString(nextId++, primaryTable);
            if (foreignCatalog != null) {
                pStmt.setString(nextId++, foreignCatalog);
            }
            pStmt.setString(nextId, foreignTable);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createFkMetadataFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public java.sql.ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        // TODO: Can't determine actions using INFORMATION_SCHEMA yet...

        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        //CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION

        StringBuilder sqlBuf = new StringBuilder(
                "SELECT A.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, A.REFERENCED_TABLE_NAME AS PKTABLE_NAME,");
        sqlBuf.append(" A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME, A.TABLE_SCHEMA AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, A.TABLE_NAME AS FKTABLE_NAME,");
        sqlBuf.append(" A.COLUMN_NAME AS FKCOLUMN_NAME, A.ORDINAL_POSITION AS KEY_SEQ,");
        sqlBuf.append(generateUpdateRuleClause());
        sqlBuf.append(" AS UPDATE_RULE,");
        sqlBuf.append(generateDeleteRuleClause());
        sqlBuf.append(" AS DELETE_RULE, A.CONSTRAINT_NAME AS FK_NAME, (SELECT CONSTRAINT_NAME FROM");
        sqlBuf.append(" INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = A.REFERENCED_TABLE_SCHEMA AND");
        sqlBuf.append(" TABLE_NAME = A.REFERENCED_TABLE_NAME AND CONSTRAINT_TYPE IN ('UNIQUE','PRIMARY KEY') LIMIT 1) AS PK_NAME,");
        sqlBuf.append(importedKeyNotDeferrable);
        sqlBuf.append(" AS DEFERRABILITY FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A");
        sqlBuf.append(" JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B USING (TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME) ");
        sqlBuf.append(generateOptionalRefContraintsJoin());
        sqlBuf.append(" WHERE B.CONSTRAINT_TYPE = 'FOREIGN KEY'");
        if (catalog != null) {
            sqlBuf.append(" AND A.REFERENCED_TABLE_SCHEMA LIKE ?");
        }
        sqlBuf.append(" AND A.REFERENCED_TABLE_NAME=?");
        sqlBuf.append(" ORDER BY A.TABLE_SCHEMA, A.TABLE_NAME, A.ORDINAL_POSITION");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());
            int nextId = 1;

            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            pStmt.setString(nextId, table);

            ResultSet rs = executeMetadataQuery(pStmt);

            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createFkMetadataFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }

    }

    private String generateOptionalRefContraintsJoin() {
        return ("JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R ON (R.CONSTRAINT_NAME = B.CONSTRAINT_NAME "
                + "AND R.TABLE_NAME = B.TABLE_NAME AND R.CONSTRAINT_SCHEMA = B.TABLE_SCHEMA) ");
    }

    private String generateDeleteRuleClause() {
        return ("CASE WHEN R.DELETE_RULE='CASCADE' THEN " + String.valueOf(importedKeyCascade) + " WHEN R.DELETE_RULE='SET NULL' THEN "
                + String.valueOf(importedKeySetNull) + " WHEN R.DELETE_RULE='SET DEFAULT' THEN " + String.valueOf(importedKeySetDefault)
                + " WHEN R.DELETE_RULE='RESTRICT' THEN " + String.valueOf(importedKeyRestrict) + " WHEN R.DELETE_RULE='NO ACTION' THEN "
                + String.valueOf(importedKeyNoAction) + " ELSE " + String.valueOf(importedKeyNoAction) + " END ");
    }

    private String generateUpdateRuleClause() {
        return ("CASE WHEN R.UPDATE_RULE='CASCADE' THEN " + String.valueOf(importedKeyCascade) + " WHEN R.UPDATE_RULE='SET NULL' THEN "
                + String.valueOf(importedKeySetNull) + " WHEN R.UPDATE_RULE='SET DEFAULT' THEN " + String.valueOf(importedKeySetDefault)
                + " WHEN R.UPDATE_RULE='RESTRICT' THEN " + String.valueOf(importedKeyRestrict) + " WHEN R.UPDATE_RULE='NO ACTION' THEN "
                + String.valueOf(importedKeyNoAction) + " ELSE " + String.valueOf(importedKeyNoAction) + " END ");
    }

    @Override
    public java.sql.ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        StringBuilder sqlBuf = new StringBuilder("SELECT A.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM,");
        sqlBuf.append(" A.REFERENCED_TABLE_NAME AS PKTABLE_NAME, A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME, A.TABLE_SCHEMA AS FKTABLE_CAT,");
        sqlBuf.append(" NULL AS FKTABLE_SCHEM, A.TABLE_NAME AS FKTABLE_NAME, A.COLUMN_NAME AS FKCOLUMN_NAME, A.ORDINAL_POSITION AS KEY_SEQ,");
        sqlBuf.append(generateUpdateRuleClause());
        sqlBuf.append(" AS UPDATE_RULE,");
        sqlBuf.append(generateDeleteRuleClause());
        sqlBuf.append(" AS DELETE_RULE, A.CONSTRAINT_NAME AS FK_NAME, (SELECT CONSTRAINT_NAME FROM");
        sqlBuf.append(" INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = A.REFERENCED_TABLE_SCHEMA AND");
        sqlBuf.append(" TABLE_NAME = A.REFERENCED_TABLE_NAME AND CONSTRAINT_TYPE IN ('UNIQUE','PRIMARY KEY') LIMIT 1) AS PK_NAME,");
        sqlBuf.append(importedKeyNotDeferrable);
        sqlBuf.append(" AS DEFERRABILITY FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A");
        sqlBuf.append(" JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B USING (CONSTRAINT_NAME, TABLE_NAME) ");
        sqlBuf.append(generateOptionalRefContraintsJoin());
        sqlBuf.append("WHERE B.CONSTRAINT_TYPE = 'FOREIGN KEY'");
        if (catalog != null) {
            sqlBuf.append(" AND A.TABLE_SCHEMA LIKE ?");
        }
        sqlBuf.append(" AND A.TABLE_NAME=?");
        sqlBuf.append(" AND A.REFERENCED_TABLE_SCHEMA IS NOT NULL");
        sqlBuf.append(" ORDER BY A.REFERENCED_TABLE_SCHEMA, A.REFERENCED_TABLE_NAME, A.ORDINAL_POSITION");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());
            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            pStmt.setString(nextId, table);

            ResultSet rs = executeMetadataQuery(pStmt);

            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createFkMetadataFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        StringBuilder sqlBuf = new StringBuilder("SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, NON_UNIQUE,");
        sqlBuf.append("TABLE_SCHEMA AS INDEX_QUALIFIER, INDEX_NAME,");
        sqlBuf.append(tableIndexOther);
        sqlBuf.append(" AS TYPE, SEQ_IN_INDEX AS ORDINAL_POSITION, COLUMN_NAME,");
        sqlBuf.append("COLLATION AS ASC_OR_DESC, CARDINALITY, NULL AS PAGES, NULL AS FILTER_CONDITION FROM INFORMATION_SCHEMA.STATISTICS WHERE");
        if (catalog != null) {
            sqlBuf.append(" TABLE_SCHEMA LIKE ? AND");
        }
        sqlBuf.append(" TABLE_NAME LIKE ?");

        if (unique) {
            sqlBuf.append(" AND NON_UNIQUE=0 ");
        }
        sqlBuf.append("ORDER BY NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX");

        java.sql.PreparedStatement pStmt = null;

        try {

            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());
            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            pStmt.setString(nextId, table);

            ResultSet rs = executeMetadataQuery(pStmt);

            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createIndexInfoFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public java.sql.ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {

        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        StringBuilder sqlBuf = new StringBuilder("SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME,");
        sqlBuf.append(" COLUMN_NAME, SEQ_IN_INDEX AS KEY_SEQ, 'PRIMARY' AS PK_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE");
        if (catalog != null) {
            sqlBuf.append(" TABLE_SCHEMA LIKE ? AND");
        }
        sqlBuf.append(" TABLE_NAME LIKE ?");
        sqlBuf.append(" AND INDEX_NAME='PRIMARY' ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());
            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            pStmt.setString(nextId, table);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition()
                    .setFields(new Field[] { new Field("", "TABLE_CAT", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 255),
                            new Field("", "TABLE_SCHEM", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 0),
                            new Field("", "TABLE_NAME", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 255),
                            new Field("", "COLUMN_NAME", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 32),
                            new Field("", "KEY_SEQ", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.SMALLINT, 5),
                            new Field("", "PK_NAME", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 32) });

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {

        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        StringBuilder sqlBuf = new StringBuilder(
                "SELECT ROUTINE_SCHEMA AS PROCEDURE_CAT, NULL AS PROCEDURE_SCHEM, ROUTINE_NAME AS PROCEDURE_NAME, NULL AS RESERVED_1,");
        sqlBuf.append(" NULL AS RESERVED_2, NULL AS RESERVED_3, ROUTINE_COMMENT AS REMARKS, CASE WHEN ROUTINE_TYPE = 'PROCEDURE' THEN ");
        sqlBuf.append(procedureNoResult);
        sqlBuf.append(" WHEN ROUTINE_TYPE='FUNCTION' THEN ");
        sqlBuf.append(procedureReturnsResult);
        sqlBuf.append(" ELSE ");
        sqlBuf.append(procedureResultUnknown);
        sqlBuf.append(" END AS PROCEDURE_TYPE, ROUTINE_NAME AS SPECIFIC_NAME FROM INFORMATION_SCHEMA.ROUTINES");

        StringBuilder conditionBuf = new StringBuilder();
        if (!this.conn.getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue()) {
            conditionBuf.append(" ROUTINE_TYPE = 'PROCEDURE'");
        }
        if (catalog != null) {
            if (conditionBuf.length() > 0) {
                conditionBuf.append(" AND");
            }
            conditionBuf.append(" ROUTINE_SCHEMA LIKE ?");
        }
        if (procedureNamePattern != null) {
            if (conditionBuf.length() > 0) {
                conditionBuf.append(" AND");
            }
            conditionBuf.append(" ROUTINE_NAME LIKE ?");
        }

        if (conditionBuf.length() > 0) {
            sqlBuf.append(" WHERE");
            sqlBuf.append(conditionBuf);
        }
        sqlBuf.append(" ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());
            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            if (procedureNamePattern != null) {
                pStmt.setString(nextId, procedureNamePattern);
            }

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createFieldMetadataForGetProcedures());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {

        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        // Here's what we get from MySQL ...
        // SPECIFIC_CATALOG                             NULL 
        // SPECIFIC_SCHEMA                              db17 
        // SPECIFIC_NAME                                p 
        // ORDINAL_POSITION                             1 
        // PARAMETER_MODE                               OUT 
        // PARAMETER_NAME                               a 
        // DATA_TYPE                                    int 
        // CHARACTER_MAXIMUM_LENGTH                     NULL 
        // CHARACTER_OCTET_LENGTH                       NULL 
        // CHARACTER_SET_NAME                           NULL 
        // COLLATION_NAME                               NULL 
        // NUMERIC_PRECISION                            10 
        // NUMERIC_SCALE                                0 
        // DTD_IDENTIFIER                               int(11)
        StringBuilder sqlBuf = new StringBuilder("SELECT SPECIFIC_SCHEMA AS PROCEDURE_CAT, NULL AS `PROCEDURE_SCHEM`, ");
        sqlBuf.append(" SPECIFIC_NAME AS `PROCEDURE_NAME`,");
        sqlBuf.append(" IFNULL(PARAMETER_NAME, '') AS `COLUMN_NAME`,");
        sqlBuf.append(" CASE WHEN PARAMETER_MODE = 'IN' THEN ");
        sqlBuf.append(procedureColumnIn);
        sqlBuf.append(" WHEN PARAMETER_MODE = 'OUT' THEN ");
        sqlBuf.append(procedureColumnOut);
        sqlBuf.append(" WHEN PARAMETER_MODE = 'INOUT' THEN ");
        sqlBuf.append(procedureColumnInOut);
        sqlBuf.append(" WHEN ORDINAL_POSITION = 0 THEN ");
        sqlBuf.append(procedureColumnReturn);
        sqlBuf.append(" ELSE ");
        sqlBuf.append(procedureColumnUnknown);
        sqlBuf.append(" END AS `COLUMN_TYPE`, ");

        //DATA_TYPE
        appendJdbcTypeMappingQuery(sqlBuf, "DATA_TYPE", "DTD_IDENTIFIER");

        sqlBuf.append(" AS `DATA_TYPE`, ");

        // TYPE_NAME
        sqlBuf.append(" UPPER(CASE WHEN LOCATE('UNSIGNED', UPPER(DATA_TYPE)) != 0 AND LOCATE('UNSIGNED', UPPER(DATA_TYPE)) = 0");
        sqlBuf.append(" THEN CONCAT(DATA_TYPE, ' UNSIGNED') ELSE DATA_TYPE END) AS `TYPE_NAME`,");

        // PRECISION</B> int => precision
        sqlBuf.append(" NUMERIC_PRECISION AS `PRECISION`,");

        // LENGTH</B> int => length in bytes of data
        sqlBuf.append(" CASE WHEN LCASE(DATA_TYPE)='date' THEN 10 WHEN LCASE(DATA_TYPE)='time' THEN 8 WHEN LCASE(DATA_TYPE)='datetime' THEN 19");
        sqlBuf.append(" WHEN LCASE(DATA_TYPE)='timestamp' THEN 19 WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN NUMERIC_PRECISION");
        sqlBuf.append(" WHEN CHARACTER_MAXIMUM_LENGTH > ");
        sqlBuf.append(Integer.MAX_VALUE);
        sqlBuf.append(" THEN ");
        sqlBuf.append(Integer.MAX_VALUE);
        sqlBuf.append(" ELSE CHARACTER_MAXIMUM_LENGTH END AS LENGTH,");

        // SCALE</B> short => scale
        sqlBuf.append("NUMERIC_SCALE AS `SCALE`, ");
        // RADIX</B> short => radix
        sqlBuf.append("10 AS RADIX,");

        sqlBuf.append(procedureNullable);
        sqlBuf.append(" AS `NULLABLE`, NULL AS `REMARKS`, NULL AS `COLUMN_DEF`, NULL AS `SQL_DATA_TYPE`, NULL AS `SQL_DATETIME_SUB`,");
        sqlBuf.append(" CHARACTER_OCTET_LENGTH AS `CHAR_OCTET_LENGTH`, ORDINAL_POSITION, 'YES' AS `IS_NULLABLE`, SPECIFIC_NAME");
        sqlBuf.append(" FROM INFORMATION_SCHEMA.PARAMETERS");

        StringBuilder conditionBuf = new StringBuilder();
        if (!this.conn.getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue()) {
            conditionBuf.append(" ROUTINE_TYPE = 'PROCEDURE'");
        }
        if (catalog != null) {
            if (conditionBuf.length() > 0) {
                conditionBuf.append(" AND");
            }
            conditionBuf.append(" SPECIFIC_SCHEMA LIKE ?");
        }
        if (procedureNamePattern != null) {
            if (conditionBuf.length() > 0) {
                conditionBuf.append(" AND");
            }
            conditionBuf.append(" SPECIFIC_NAME LIKE ?");
        }
        if (columnNamePattern != null) {
            if (conditionBuf.length() > 0) {
                conditionBuf.append(" AND");
            }
            conditionBuf.append(" (PARAMETER_NAME LIKE ? OR PARAMETER_NAME IS NULL)");
        }

        if (conditionBuf.length() > 0) {
            sqlBuf.append(" WHERE");
            sqlBuf.append(conditionBuf);
        }
        sqlBuf.append(" ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ROUTINE_TYPE, ORDINAL_POSITION");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());

            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            if (procedureNamePattern != null) {
                pStmt.setString(nextId++, procedureNamePattern);
            }
            if (columnNamePattern != null) {
                pStmt.setString(nextId, columnNamePattern);
            }

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createProcedureColumnsFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        if (tableNamePattern != null) {
            List<String> parseList = StringUtils.splitDBdotName(tableNamePattern, catalog, this.quotedId,
                    this.session.getServerSession().isNoBackslashEscapesSet());
            //There *should* be 2 rows, if any.
            if (parseList.size() == 2) {
                tableNamePattern = parseList.get(1);
            }
        }

        java.sql.PreparedStatement pStmt = null;

        StringBuilder sqlBuf = new StringBuilder("SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, ");
        sqlBuf.append("CASE WHEN TABLE_TYPE='BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' ");
        sqlBuf.append("ELSE 'TABLE' END WHEN TABLE_TYPE='TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE, ");
        sqlBuf.append("TABLE_COMMENT AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, ");
        sqlBuf.append("NULL AS REF_GENERATION FROM INFORMATION_SCHEMA.TABLES");

        if (catalog != null || tableNamePattern != null) {
            sqlBuf.append(" WHERE");
        }

        if (catalog != null) {
            sqlBuf.append("information_schema".equalsIgnoreCase(catalog) || "performance_schema".equalsIgnoreCase(catalog) || !StringUtils.hasWildcards(catalog)
                    ? " TABLE_SCHEMA = ?" : " TABLE_SCHEMA LIKE ?");
        }

        if (tableNamePattern != null) {
            if (catalog != null) {
                sqlBuf.append(" AND");
            }
            sqlBuf.append(StringUtils.hasWildcards(tableNamePattern) ? " TABLE_NAME LIKE ?" : " TABLE_NAME = ?");
        }
        if (types != null && types.length > 0) {
            sqlBuf.append(" HAVING TABLE_TYPE IN (?,?,?,?,?)");
        }
        sqlBuf.append(" ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME");
        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());

            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog != null ? catalog : "%");
            }
            if (tableNamePattern != null) {
                pStmt.setString(nextId++, tableNamePattern);
            }

            if (types != null && types.length > 0) {
                for (int i = 0; i < 5; i++) {
                    pStmt.setNull(nextId + i, MysqlType.VARCHAR.getJdbcType());
                }
                for (int i = 0; i < types.length; i++) {
                    TableType tableType = TableType.getTableTypeEqualTo(types[i]);
                    if (tableType != TableType.UNKNOWN) {
                        pStmt.setString(nextId++, tableType.getName());
                    }
                }
            }

            ResultSet rs = executeMetadataQuery(pStmt);

            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).setColumnDefinition(createTablesFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {

        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        StringBuilder sqlBuf = new StringBuilder("SELECT NULL AS SCOPE, COLUMN_NAME, ");
        appendJdbcTypeMappingQuery(sqlBuf, "DATA_TYPE", "COLUMN_TYPE");
        sqlBuf.append(" AS DATA_TYPE, UPPER(COLUMN_TYPE) AS TYPE_NAME,");
        sqlBuf.append(" CASE WHEN LCASE(DATA_TYPE)='date' THEN 10 WHEN LCASE(DATA_TYPE)='time' THEN 8");
        sqlBuf.append(" WHEN LCASE(DATA_TYPE)='datetime' THEN 19 WHEN LCASE(DATA_TYPE)='timestamp' THEN 19");
        sqlBuf.append(" WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN NUMERIC_PRECISION WHEN CHARACTER_MAXIMUM_LENGTH > ");
        sqlBuf.append(Integer.MAX_VALUE);
        sqlBuf.append(" THEN ");
        sqlBuf.append(Integer.MAX_VALUE);
        sqlBuf.append(" ELSE CHARACTER_MAXIMUM_LENGTH END AS COLUMN_SIZE, ");
        sqlBuf.append(maxBufferSize);
        sqlBuf.append(" AS BUFFER_LENGTH,NUMERIC_SCALE AS DECIMAL_DIGITS, ");
        sqlBuf.append(Integer.toString(java.sql.DatabaseMetaData.versionColumnNotPseudo));
        sqlBuf.append(" AS PSEUDO_COLUMN FROM INFORMATION_SCHEMA.COLUMNS WHERE");
        if (catalog != null) {
            sqlBuf.append(" TABLE_SCHEMA LIKE ? AND");
        }
        sqlBuf.append(" TABLE_NAME LIKE ?");
        sqlBuf.append(" AND EXTRA LIKE '%on update CURRENT_TIMESTAMP%'");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());
            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            pStmt.setString(nextId, table);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition()
                    .setFields(new Field[] { new Field("", "SCOPE", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.SMALLINT, 5),
                            new Field("", "COLUMN_NAME", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 32),
                            new Field("", "DATA_TYPE", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.INT, 5),
                            new Field("", "TYPE_NAME", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 16),
                            new Field("", "COLUMN_SIZE", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.INT, 16),
                            new Field("", "BUFFER_LENGTH", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.INT, 16),
                            new Field("", "DECIMAL_DIGITS", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.SMALLINT, 16),
                            new Field("", "PSEUDO_COLUMN", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.SMALLINT, 5) });

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {

        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        // FUNCTION_CAT
        // FUNCTION_SCHEM
        // FUNCTION_NAME
        // COLUMN_NAME
        // COLUMN_TYPE
        StringBuilder sqlBuf = new StringBuilder("SELECT SPECIFIC_SCHEMA AS FUNCTION_CAT, NULL AS `FUNCTION_SCHEM`, SPECIFIC_NAME AS `FUNCTION_NAME`, ");
        sqlBuf.append("IFNULL(PARAMETER_NAME, '') AS `COLUMN_NAME`, CASE WHEN PARAMETER_MODE = 'IN' THEN ");
        sqlBuf.append(getFunctionConstant(FunctionConstant.FUNCTION_COLUMN_IN));
        sqlBuf.append(" WHEN PARAMETER_MODE = 'OUT' THEN ");
        sqlBuf.append(getFunctionConstant(FunctionConstant.FUNCTION_COLUMN_OUT));
        sqlBuf.append(" WHEN PARAMETER_MODE = 'INOUT' THEN ");
        sqlBuf.append(getFunctionConstant(FunctionConstant.FUNCTION_COLUMN_INOUT));
        sqlBuf.append(" WHEN ORDINAL_POSITION = 0 THEN ");
        sqlBuf.append(getFunctionConstant(FunctionConstant.FUNCTION_COLUMN_RETURN));
        sqlBuf.append(" ELSE ");
        sqlBuf.append(getFunctionConstant(FunctionConstant.FUNCTION_COLUMN_UNKNOWN));
        sqlBuf.append(" END AS `COLUMN_TYPE`, ");

        //DATA_TYPE
        appendJdbcTypeMappingQuery(sqlBuf, "DATA_TYPE", "DTD_IDENTIFIER");

        sqlBuf.append(" AS `DATA_TYPE`, ");

        // TYPE_NAME
        sqlBuf.append(
                "UPPER(CASE WHEN LOCATE('UNSIGNED', UPPER(DATA_TYPE)) != 0 AND LOCATE('UNSIGNED', UPPER(DATA_TYPE)) = 0 THEN CONCAT(DATA_TYPE, ' UNSIGNED') "
                        + "ELSE DATA_TYPE END) AS `TYPE_NAME`,");

        // PRECISION int => precision
        sqlBuf.append("NUMERIC_PRECISION AS `PRECISION`, ");
        // LENGTH int => length in bytes of data
        sqlBuf.append("CASE WHEN LCASE(DATA_TYPE)='date' THEN 10 WHEN LCASE(DATA_TYPE)='time' THEN 8 WHEN LCASE(DATA_TYPE)='datetime' THEN 19 WHEN "
                + "LCASE(DATA_TYPE)='timestamp' THEN 19 WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN NUMERIC_PRECISION WHEN CHARACTER_MAXIMUM_LENGTH > "
                + Integer.MAX_VALUE + " THEN " + Integer.MAX_VALUE + " ELSE CHARACTER_MAXIMUM_LENGTH END AS LENGTH, ");

        // SCALE short => scale
        sqlBuf.append("NUMERIC_SCALE AS `SCALE`, ");
        // RADIX short => radix
        sqlBuf.append("10 AS RADIX,");
        // NULLABLE
        // REMARKS
        // CHAR_OCTET_LENGTH *
        // ORDINAL_POSITION *
        // IS_NULLABLE *
        sqlBuf.append(getFunctionConstant(FunctionConstant.FUNCTION_NULLABLE));
        sqlBuf.append(" AS `NULLABLE`,  NULL AS `REMARKS`, CHARACTER_OCTET_LENGTH AS `CHAR_OCTET_LENGTH`,  ORDINAL_POSITION, 'YES' AS `IS_NULLABLE`,");
        // SPECIFIC_NAME *
        sqlBuf.append(" SPECIFIC_NAME FROM INFORMATION_SCHEMA.PARAMETERS WHERE");

        StringBuilder conditionBuf = new StringBuilder();
        if (catalog != null) {
            conditionBuf.append(" SPECIFIC_SCHEMA LIKE ?");
        }

        if (functionNamePattern != null) {
            if (conditionBuf.length() > 0) {
                conditionBuf.append(" AND");
            }
            conditionBuf.append(" SPECIFIC_NAME LIKE ?");
        }

        if (columnNamePattern != null) {
            if (conditionBuf.length() > 0) {
                conditionBuf.append(" AND");
            }
            conditionBuf.append(" (PARAMETER_NAME LIKE ? OR PARAMETER_NAME IS NULL)");
        }

        if (conditionBuf.length() > 0) {
            conditionBuf.append(" AND");
        }
        conditionBuf.append(" ROUTINE_TYPE='FUNCTION'");

        sqlBuf.append(conditionBuf);
        sqlBuf.append(" ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());
            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            if (functionNamePattern != null) {
                pStmt.setString(nextId++, functionNamePattern);
            }
            if (columnNamePattern != null) {
                pStmt.setString(nextId, columnNamePattern);
            }

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createFunctionColumnsFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Getter to DatabaseMetaData.function* constants.
     * 
     * @param constant
     *            the constant id from DatabaseMetaData fields to return.
     * 
     * @return one of the java.sql.DatabaseMetaData#function* fields.
     */
    protected int getFunctionConstant(FunctionConstant constant) {
        switch (constant) {
            case FUNCTION_COLUMN_IN:
                return functionColumnIn;
            case FUNCTION_COLUMN_INOUT:
                return functionColumnInOut;
            case FUNCTION_COLUMN_OUT:
                return functionColumnOut;
            case FUNCTION_COLUMN_RETURN:
                return functionReturn;
            case FUNCTION_COLUMN_RESULT:
                return functionColumnResult;
            case FUNCTION_COLUMN_UNKNOWN:
                return functionColumnUnknown;
            case FUNCTION_NO_NULLS:
                return functionNoNulls;
            case FUNCTION_NULLABLE:
                return functionNullable;
            case FUNCTION_NULLABLE_UNKNOWN:
                return functionNullableUnknown;
            default:
                return -1;
        }
    }

    @Override
    public java.sql.ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {

        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        catalog = this.pedantic ? catalog : StringUtils.unQuoteIdentifier(catalog, this.quotedId);

        StringBuilder sqlBuf = new StringBuilder(
                "SELECT ROUTINE_SCHEMA AS FUNCTION_CAT, NULL AS FUNCTION_SCHEM, ROUTINE_NAME AS FUNCTION_NAME, ROUTINE_COMMENT AS REMARKS, ");
        sqlBuf.append(getFunctionNoTableConstant());
        sqlBuf.append(" AS FUNCTION_TYPE, ROUTINE_NAME AS SPECIFIC_NAME FROM INFORMATION_SCHEMA.ROUTINES");
        sqlBuf.append(" WHERE ROUTINE_TYPE LIKE 'FUNCTION'");
        if (catalog != null) {
            sqlBuf.append(" AND ROUTINE_SCHEMA LIKE ?");
        }
        if (functionNamePattern != null) {
            sqlBuf.append(" AND ROUTINE_NAME LIKE ?");
        }

        sqlBuf.append(" ORDER BY FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME, SPECIFIC_NAME");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());
            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            if (functionNamePattern != null) {
                pStmt.setString(nextId, functionNamePattern);
            }

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition()
                    .setFields(new Field[] { new Field("", "FUNCTION_CAT", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 255),
                            new Field("", "FUNCTION_SCHEM", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 255),
                            new Field("", "FUNCTION_NAME", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 255),
                            new Field("", "REMARKS", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 255),
                            new Field("", "FUNCTION_TYPE", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.SMALLINT, 6),
                            new Field("", "SPECIFIC_NAME", this.getMetadataCollationIndex(), this.getMetadataEncoding(), MysqlType.CHAR, 255) });

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        if (!this.conn.getServerVersion().meetsMinimum(ServerVersion.parseVersion("8.0.11"))) {
            return super.getSQLKeywords();
        }

        String keywords = keywordsCache.get(this.conn.getServerVersion());
        if (keywords != null) {
            return keywords;
        }

        synchronized (keywordsCache) {
            // Double check, maybe another thread already added it.
            keywords = keywordsCache.get(this.conn.getServerVersion());
            if (keywords != null) {
                return keywords;
            }

            List<String> keywordsFromServer = new ArrayList<>();
            Statement stmt = this.conn.getMetadataSafeStatement();
            ResultSet rs = stmt.executeQuery("SELECT WORD FROM INFORMATION_SCHEMA.KEYWORDS WHERE RESERVED=1 ORDER BY WORD");
            while (rs.next()) {
                keywordsFromServer.add(rs.getString(1));
            }
            stmt.close();

            keywordsFromServer.removeAll(SQL2003_KEYWORDS);
            keywords = String.join(",", keywordsFromServer);

            keywordsCache.put(this.conn.getServerVersion(), keywords);
            return keywords;
        }
    }

    private final void appendJdbcTypeMappingQuery(StringBuilder buf, String mysqlTypeColumnName, String fullMysqlTypeColumnName) {

        buf.append("CASE ");
        for (MysqlType mysqlType : MysqlType.values()) {

            buf.append(" WHEN UPPER(");
            buf.append(mysqlTypeColumnName);
            buf.append(")='");
            buf.append(mysqlType.getName());
            buf.append("' THEN ");

            switch (mysqlType) {
                case TINYINT:
                case TINYINT_UNSIGNED:
                    if (this.tinyInt1isBit) {
                        buf.append("CASE");
                        if (this.transformedBitIsBoolean) {
                            buf.append(" WHEN LOCATE('(1)', ");
                            buf.append(fullMysqlTypeColumnName);
                            buf.append(") != 0 THEN 16");
                        } else {
                            buf.append(" WHEN LOCATE('(1)', ");
                            buf.append(fullMysqlTypeColumnName);
                            buf.append(") != 0 THEN -7");
                        }
                        buf.append(" ELSE -6 END ");
                    } else {
                        buf.append(mysqlType.getJdbcType());
                    }
                    break;

                default:
                    buf.append(mysqlType.getJdbcType());
            }
        }

        buf.append(" WHEN UPPER(DATA_TYPE)='POINT' THEN -2");
        buf.append(" WHEN UPPER(DATA_TYPE)='LINESTRING' THEN -2");
        buf.append(" WHEN UPPER(DATA_TYPE)='POLYGON' THEN -2");
        buf.append(" WHEN UPPER(DATA_TYPE)='MULTIPOINT' THEN -2");
        buf.append(" WHEN UPPER(DATA_TYPE)='MULTILINESTRING' THEN -2");
        buf.append(" WHEN UPPER(DATA_TYPE)='MULTIPOLYGON' THEN -2");
        buf.append(" WHEN UPPER(DATA_TYPE)='GEOMETRYCOLLECTION' THEN -2");
        buf.append(" WHEN UPPER(DATA_TYPE)='GEOMCOLLECTION' THEN -2");

        buf.append(" ELSE 1111");
        buf.append(" END ");

    }
}
