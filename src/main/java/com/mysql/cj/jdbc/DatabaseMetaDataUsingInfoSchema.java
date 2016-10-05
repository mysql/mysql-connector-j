/*
  Copyright (c) 2005, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.MysqlType;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.io.ResultSetFactory;

/**
 * DatabaseMetaData implementation that uses INFORMATION_SCHEMA
 */
public class DatabaseMetaDataUsingInfoSchema extends DatabaseMetaData {

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
        ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).setOwningStatement(null);

        return rs;
    }

    @Override
    public java.sql.ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        if (columnNamePattern == null) {
            if (this.nullNamePatternMatchesAll) {
                columnNamePattern = "%";
            } else {
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.9"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                catalog = this.database;
            }
        }

        String sql = "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME,"
                + "COLUMN_NAME, NULL AS GRANTOR, GRANTEE, PRIVILEGE_TYPE AS PRIVILEGE, IS_GRANTABLE FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES WHERE "
                + "TABLE_SCHEMA LIKE ? AND TABLE_NAME =? AND COLUMN_NAME LIKE ? ORDER BY COLUMN_NAME, PRIVILEGE_TYPE";

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sql);

            if (catalog != null) {
                pStmt.setString(1, catalog);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, table);
            pStmt.setString(3, columnNamePattern);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition()
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
        if (columnNamePattern == null) {
            if (this.nullNamePatternMatchesAll) {
                columnNamePattern = "%";
            } else {
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.9"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                catalog = this.database;
            }
        }

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

        sqlBuf.append("FROM INFORMATION_SCHEMA.COLUMNS WHERE ");

        final boolean operatingOnInformationSchema = "information_schema".equalsIgnoreCase(catalog);

        if (catalog != null) {
            if ((operatingOnInformationSchema)
                    || ((StringUtils.indexOfIgnoreCase(0, catalog, "%") == -1) && (StringUtils.indexOfIgnoreCase(0, catalog, "_") == -1))) {
                sqlBuf.append("TABLE_SCHEMA = ? AND ");
            } else {
                sqlBuf.append("TABLE_SCHEMA LIKE ? AND ");
            }

        } else {
            sqlBuf.append("TABLE_SCHEMA LIKE ? AND ");
        }

        if (tableName != null) {
            if ((StringUtils.indexOfIgnoreCase(0, tableName, "%") == -1) && (StringUtils.indexOfIgnoreCase(0, tableName, "_") == -1)) {
                sqlBuf.append("TABLE_NAME = ? AND ");
            } else {
                sqlBuf.append("TABLE_NAME LIKE ? AND ");
            }

        } else {
            sqlBuf.append("TABLE_NAME LIKE ? AND ");
        }

        if ((StringUtils.indexOfIgnoreCase(0, columnNamePattern, "%") == -1) && (StringUtils.indexOfIgnoreCase(0, columnNamePattern, "_") == -1)) {
            sqlBuf.append("COLUMN_NAME = ? ");
        } else {
            sqlBuf.append("COLUMN_NAME LIKE ? ");
        }
        sqlBuf.append("ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());

            if (catalog != null) {
                pStmt.setString(1, catalog);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, tableName);
            pStmt.setString(3, columnNamePattern);

            ResultSet rs = executeMetadataQuery(pStmt);

            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createColumnsFields());
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
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        if (primaryCatalog == null) {
            if (this.nullCatalogMeansCurrent) {
                primaryCatalog = this.database;
            }
        }

        if (foreignCatalog == null) {
            if (this.nullCatalogMeansCurrent) {
                foreignCatalog = this.database;
            }
        }

        String sql = "SELECT A.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT,NULL AS PKTABLE_SCHEM, A.REFERENCED_TABLE_NAME AS PKTABLE_NAME,"
                + "A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME, A.TABLE_SCHEMA AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, A.TABLE_NAME AS FKTABLE_NAME, "
                + "A.COLUMN_NAME AS FKCOLUMN_NAME, A.ORDINAL_POSITION AS KEY_SEQ," + generateUpdateRuleClause() + " AS UPDATE_RULE,"
                + generateDeleteRuleClause() + " AS DELETE_RULE," + "A.CONSTRAINT_NAME AS FK_NAME," + "(SELECT CONSTRAINT_NAME FROM"
                + " INFORMATION_SCHEMA.TABLE_CONSTRAINTS" + " WHERE TABLE_SCHEMA = A.REFERENCED_TABLE_SCHEMA AND" + " TABLE_NAME = A.REFERENCED_TABLE_NAME AND"
                + " CONSTRAINT_TYPE IN ('UNIQUE','PRIMARY KEY') LIMIT 1)" + " AS PK_NAME," + importedKeyNotDeferrable + " AS DEFERRABILITY " + "FROM "
                + "INFORMATION_SCHEMA.KEY_COLUMN_USAGE A JOIN " + "INFORMATION_SCHEMA.TABLE_CONSTRAINTS B "
                + "USING (TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME) " + generateOptionalRefContraintsJoin() + "WHERE " + "B.CONSTRAINT_TYPE = 'FOREIGN KEY' "
                + "AND A.REFERENCED_TABLE_SCHEMA LIKE ? AND A.REFERENCED_TABLE_NAME=? "
                + "AND A.TABLE_SCHEMA LIKE ? AND A.TABLE_NAME=? ORDER BY A.TABLE_SCHEMA, A.TABLE_NAME, A.ORDINAL_POSITION";

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sql);
            if (primaryCatalog != null) {
                pStmt.setString(1, primaryCatalog);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, primaryTable);

            if (foreignCatalog != null) {
                pStmt.setString(3, foreignCatalog);
            } else {
                pStmt.setString(3, "%");
            }

            pStmt.setString(4, foreignTable);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createFkMetadataFields());

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
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                catalog = this.database;
            }
        }

        //CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION

        String sql = "SELECT A.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, A.REFERENCED_TABLE_NAME AS PKTABLE_NAME, "
                + "A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME, A.TABLE_SCHEMA AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, A.TABLE_NAME AS FKTABLE_NAME,"
                + "A.COLUMN_NAME AS FKCOLUMN_NAME, A.ORDINAL_POSITION AS KEY_SEQ," + generateUpdateRuleClause() + " AS UPDATE_RULE,"
                + generateDeleteRuleClause() + " AS DELETE_RULE," + "A.CONSTRAINT_NAME AS FK_NAME," + "(SELECT CONSTRAINT_NAME FROM"
                + " INFORMATION_SCHEMA.TABLE_CONSTRAINTS" + " WHERE TABLE_SCHEMA = A.REFERENCED_TABLE_SCHEMA AND" + " TABLE_NAME = A.REFERENCED_TABLE_NAME AND"
                + " CONSTRAINT_TYPE IN ('UNIQUE','PRIMARY KEY') LIMIT 1)" + " AS PK_NAME," + importedKeyNotDeferrable + " AS DEFERRABILITY " + "FROM "
                + "INFORMATION_SCHEMA.KEY_COLUMN_USAGE A JOIN " + "INFORMATION_SCHEMA.TABLE_CONSTRAINTS B "
                + "USING (TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_NAME) " + generateOptionalRefContraintsJoin() + "WHERE " + "B.CONSTRAINT_TYPE = 'FOREIGN KEY' "
                + "AND A.REFERENCED_TABLE_SCHEMA LIKE ? AND A.REFERENCED_TABLE_NAME=? " + "ORDER BY A.TABLE_SCHEMA, A.TABLE_NAME, A.ORDINAL_POSITION";

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sql);

            if (catalog != null) {
                pStmt.setString(1, catalog);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, table);

            ResultSet rs = executeMetadataQuery(pStmt);

            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createFkMetadataFields());

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
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                catalog = this.database;
            }
        }

        String sql = "SELECT A.REFERENCED_TABLE_SCHEMA AS PKTABLE_CAT, NULL AS PKTABLE_SCHEM, A.REFERENCED_TABLE_NAME AS PKTABLE_NAME,"
                + "A.REFERENCED_COLUMN_NAME AS PKCOLUMN_NAME, A.TABLE_SCHEMA AS FKTABLE_CAT, NULL AS FKTABLE_SCHEM, A.TABLE_NAME AS FKTABLE_NAME, "
                + "A.COLUMN_NAME AS FKCOLUMN_NAME, A.ORDINAL_POSITION AS KEY_SEQ," + generateUpdateRuleClause() + " AS UPDATE_RULE,"
                + generateDeleteRuleClause() + " AS DELETE_RULE," + "A.CONSTRAINT_NAME AS FK_NAME," + "(SELECT CONSTRAINT_NAME FROM"
                + " INFORMATION_SCHEMA.TABLE_CONSTRAINTS" + " WHERE TABLE_SCHEMA = A.REFERENCED_TABLE_SCHEMA AND" + " TABLE_NAME = A.REFERENCED_TABLE_NAME AND"
                + " CONSTRAINT_TYPE IN ('UNIQUE','PRIMARY KEY') LIMIT 1)" + " AS PK_NAME," + importedKeyNotDeferrable + " AS DEFERRABILITY " + "FROM "
                + "INFORMATION_SCHEMA.KEY_COLUMN_USAGE A " + "JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B USING " + "(CONSTRAINT_NAME, TABLE_NAME) "
                + generateOptionalRefContraintsJoin() + "WHERE " + "B.CONSTRAINT_TYPE = 'FOREIGN KEY' " + "AND A.TABLE_SCHEMA LIKE ? " + "AND A.TABLE_NAME=? "
                + "AND A.REFERENCED_TABLE_SCHEMA IS NOT NULL " + "ORDER BY A.REFERENCED_TABLE_SCHEMA, A.REFERENCED_TABLE_NAME, A.ORDINAL_POSITION";

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sql);

            if (catalog != null) {
                pStmt.setString(1, catalog);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, table);

            ResultSet rs = executeMetadataQuery(pStmt);

            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createFkMetadataFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        StringBuilder sqlBuf = new StringBuilder("SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, NON_UNIQUE,");
        sqlBuf.append("TABLE_SCHEMA AS INDEX_QUALIFIER, INDEX_NAME," + tableIndexOther + " AS TYPE, SEQ_IN_INDEX AS ORDINAL_POSITION, COLUMN_NAME,");
        sqlBuf.append("COLLATION AS ASC_OR_DESC, CARDINALITY, NULL AS PAGES, NULL AS FILTER_CONDITION FROM INFORMATION_SCHEMA.STATISTICS WHERE ");
        sqlBuf.append("TABLE_SCHEMA LIKE ? AND TABLE_NAME LIKE ?");

        if (unique) {
            sqlBuf.append(" AND NON_UNIQUE=0 ");
        }

        sqlBuf.append("ORDER BY NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX");

        java.sql.PreparedStatement pStmt = null;

        try {
            if (catalog == null) {
                if (this.nullCatalogMeansCurrent) {
                    catalog = this.database;
                }
            }

            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());

            if (catalog != null) {
                pStmt.setString(1, catalog);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, table);

            ResultSet rs = executeMetadataQuery(pStmt);

            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createIndexInfoFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public java.sql.ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {

        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                catalog = this.database;
            }
        }

        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        String sql = "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, "
                + "COLUMN_NAME, SEQ_IN_INDEX AS KEY_SEQ, 'PRIMARY' AS PK_NAME FROM INFORMATION_SCHEMA.STATISTICS "
                + "WHERE TABLE_SCHEMA LIKE ? AND TABLE_NAME LIKE ? AND INDEX_NAME='PRIMARY' ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX";

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sql);

            if (catalog != null) {
                pStmt.setString(1, catalog);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, table);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition()
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

        if ((procedureNamePattern == null) || (procedureNamePattern.length() == 0)) {
            if (this.nullNamePatternMatchesAll) {
                procedureNamePattern = "%";
            } else {
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.11"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        String db = null;

        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                db = this.database;
            }
        } else {
            db = catalog;
        }

        String sql = "SELECT ROUTINE_SCHEMA AS PROCEDURE_CAT, NULL AS PROCEDURE_SCHEM, ROUTINE_NAME AS PROCEDURE_NAME, NULL AS RESERVED_1, "
                + "NULL AS RESERVED_2, NULL AS RESERVED_3, ROUTINE_COMMENT AS REMARKS, CASE WHEN ROUTINE_TYPE = 'PROCEDURE' THEN " + procedureNoResult
                + " WHEN ROUTINE_TYPE='FUNCTION' THEN " + procedureReturnsResult + " ELSE " + procedureResultUnknown
                + " END AS PROCEDURE_TYPE, ROUTINE_NAME AS SPECIFIC_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE " + getRoutineTypeConditionForGetProcedures()
                + "ROUTINE_SCHEMA LIKE ? AND ROUTINE_NAME LIKE ? ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE";

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sql);

            if (db != null) {
                pStmt.setString(1, db);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, procedureNamePattern);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createFieldMetadataForGetProcedures());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Returns a condition to be injected in the query that returns metadata for procedures only. Overrides
     * DatabaseMetaDataUsingInfoSchema#injectRoutineTypeConditionForGetProcedures. When not empty must end with "AND ".
     * 
     * @return String with the condition to be injected.
     */
    protected String getRoutineTypeConditionForGetProcedures() {
        return this.conn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).getValue() ? ""
                : "ROUTINE_TYPE = 'PROCEDURE' AND ";
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        if ((procedureNamePattern == null) || (procedureNamePattern.length() == 0)) {
            if (this.nullNamePatternMatchesAll) {
                procedureNamePattern = "%";
            } else {
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.11"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        String db = null;

        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                db = this.database;
            }
        } else {
            db = catalog;
        }

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

        StringBuilder sqlBuf = new StringBuilder("SELECT SPECIFIC_SCHEMA AS PROCEDURE_CAT, NULL AS `PROCEDURE_SCHEM`, "
                + "SPECIFIC_NAME AS `PROCEDURE_NAME`, IFNULL(PARAMETER_NAME, '') AS `COLUMN_NAME`, CASE WHEN PARAMETER_MODE = 'IN' THEN " + procedureColumnIn
                + " WHEN PARAMETER_MODE = 'OUT' THEN " + procedureColumnOut + " WHEN PARAMETER_MODE = 'INOUT' THEN " + procedureColumnInOut
                + " WHEN ORDINAL_POSITION = 0 THEN " + procedureColumnReturn + " ELSE " + procedureColumnUnknown + " END AS `COLUMN_TYPE`, ");

        //DATA_TYPE
        appendJdbcTypeMappingQuery(sqlBuf, "DATA_TYPE", "DTD_IDENTIFIER");

        sqlBuf.append(" AS `DATA_TYPE`, ");

        // TYPE_NAME
        sqlBuf.append(
                "UPPER(CASE WHEN LOCATE('UNSIGNED', UPPER(DATA_TYPE)) != 0 AND LOCATE('UNSIGNED', UPPER(DATA_TYPE)) = 0 THEN CONCAT(DATA_TYPE, ' UNSIGNED') "
                        + "ELSE DATA_TYPE END) AS `TYPE_NAME`,");

        // PRECISION</B> int => precision
        sqlBuf.append("NUMERIC_PRECISION AS `PRECISION`, ");
        // LENGTH</B> int => length in bytes of data
        sqlBuf.append("CASE WHEN LCASE(DATA_TYPE)='date' THEN 10 WHEN LCASE(DATA_TYPE)='time' THEN 8 WHEN LCASE(DATA_TYPE)='datetime' THEN 19 "
                + "WHEN LCASE(DATA_TYPE)='timestamp' THEN 19 WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN NUMERIC_PRECISION WHEN CHARACTER_MAXIMUM_LENGTH > "
                + Integer.MAX_VALUE + " THEN " + Integer.MAX_VALUE + " ELSE CHARACTER_MAXIMUM_LENGTH END AS LENGTH, ");

        // SCALE</B> short => scale
        sqlBuf.append("NUMERIC_SCALE AS `SCALE`, ");
        // RADIX</B> short => radix
        sqlBuf.append("10 AS RADIX,");
        sqlBuf.append(procedureNullable + " AS `NULLABLE`, NULL AS `REMARKS`, NULL AS `COLUMN_DEF`, NULL AS `SQL_DATA_TYPE`, "
                + "NULL AS `SQL_DATETIME_SUB`, CHARACTER_OCTET_LENGTH AS `CHAR_OCTET_LENGTH`, ORDINAL_POSITION, 'YES' AS `IS_NULLABLE`, "
                + "SPECIFIC_NAME FROM INFORMATION_SCHEMA.PARAMETERS WHERE " + getRoutineTypeConditionForGetProcedureColumns()
                + "SPECIFIC_SCHEMA LIKE ? AND SPECIFIC_NAME LIKE ? AND (PARAMETER_NAME LIKE ? OR PARAMETER_NAME IS NULL) "
                + "ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ROUTINE_TYPE, ORDINAL_POSITION");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());

            if (db != null) {
                pStmt.setString(1, db);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, procedureNamePattern);
            pStmt.setString(3, columnNamePattern);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createProcedureColumnsFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Returns a condition to be injected in the query that returns metadata for procedure columns only. Overrides
     * DatabaseMetaDataUsingInfoSchema#injectRoutineTypeConditionForGetProcedureColumns. When not empty must end with
     * "AND ".
     * 
     * @return String with the condition to be injected.
     */
    protected String getRoutineTypeConditionForGetProcedureColumns() {
        return this.conn.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).getValue() ? ""
                : "ROUTINE_TYPE = 'PROCEDURE' AND ";
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                catalog = this.database;
            }
        }

        if (tableNamePattern == null) {
            if (this.nullNamePatternMatchesAll) {
                tableNamePattern = "%";
            } else {
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.13"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        final String tableNamePat;
        String tmpCat = "";

        if ((catalog == null) || (catalog.length() == 0)) {
            if (this.nullCatalogMeansCurrent) {
                tmpCat = this.database;
            }
        } else {
            tmpCat = catalog;
        }

        List<String> parseList = StringUtils.splitDBdotName(tableNamePattern, tmpCat, this.quotedId, this.conn.isNoBackslashEscapesSet());
        //There *should* be 2 rows, if any.
        if (parseList.size() == 2) {
            tableNamePat = parseList.get(1);
        } else {
            tableNamePat = tableNamePattern;
        }

        java.sql.PreparedStatement pStmt = null;

        String sql = "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, "
                + "CASE WHEN TABLE_TYPE='BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' "
                + "ELSE 'TABLE' END WHEN TABLE_TYPE='TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE, "
                + "TABLE_COMMENT AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, "
                + "NULL AS REF_GENERATION FROM INFORMATION_SCHEMA.TABLES WHERE ";

        final boolean operatingOnInformationSchema = "information_schema".equalsIgnoreCase(catalog);
        if (catalog != null) {
            if ((operatingOnInformationSchema)
                    || ((StringUtils.indexOfIgnoreCase(0, catalog, "%") == -1) && (StringUtils.indexOfIgnoreCase(0, catalog, "_") == -1))) {
                sql += "TABLE_SCHEMA = ? ";
            } else {
                sql += "TABLE_SCHEMA LIKE ? ";
            }

        } else {
            sql += "TABLE_SCHEMA LIKE ? ";
        }

        if (tableNamePat != null) {
            if ((StringUtils.indexOfIgnoreCase(0, tableNamePat, "%") == -1) && (StringUtils.indexOfIgnoreCase(0, tableNamePat, "_") == -1)) {
                sql += "AND TABLE_NAME = ? ";
            } else {
                sql += "AND TABLE_NAME LIKE ? ";
            }

        } else {
            sql += "AND TABLE_NAME LIKE ? ";
        }
        sql = sql + "HAVING TABLE_TYPE IN (?,?,?,?,?) ";
        sql = sql + "ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME";
        try {
            pStmt = prepareMetaDataSafeStatement(sql);

            if (catalog != null) {
                pStmt.setString(1, catalog);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, tableNamePat);

            // This overloading of IN (...) allows us to cache this prepared statement
            if (types == null || types.length == 0) {
                TableType[] tableTypes = TableType.values();
                for (int i = 0; i < 5; i++) {
                    pStmt.setString(3 + i, tableTypes[i].getName());
                }
            } else {
                for (int i = 0; i < 5; i++) {
                    pStmt.setNull(3 + i, MysqlType.VARCHAR.getJdbcType());
                }

                int idx = 3;
                for (int i = 0; i < types.length; i++) {
                    TableType tableType = TableType.getTableTypeEqualTo(types[i]);
                    if (tableType != TableType.UNKNOWN) {
                        pStmt.setString(idx++, tableType.getName());
                    }
                }
            }

            ResultSet rs = executeMetadataQuery(pStmt);

            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).setColumnDefinition(createTablesFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {

        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                catalog = this.database;
            }
        }

        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        StringBuilder sqlBuf = new StringBuilder("SELECT NULL AS SCOPE, COLUMN_NAME, ");

        appendJdbcTypeMappingQuery(sqlBuf, "DATA_TYPE", "COLUMN_TYPE");
        sqlBuf.append(" AS DATA_TYPE, ");

        sqlBuf.append("UPPER(COLUMN_TYPE) AS TYPE_NAME, ");
        sqlBuf.append("CASE WHEN LCASE(DATA_TYPE)='date' THEN 10 WHEN LCASE(DATA_TYPE)='time' THEN 8 "
                + "WHEN LCASE(DATA_TYPE)='datetime' THEN 19 WHEN LCASE(DATA_TYPE)='timestamp' THEN 19 "
                + "WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN NUMERIC_PRECISION WHEN CHARACTER_MAXIMUM_LENGTH > " + Integer.MAX_VALUE + " THEN "
                + Integer.MAX_VALUE + " ELSE CHARACTER_MAXIMUM_LENGTH END AS COLUMN_SIZE, ");
        sqlBuf.append(maxBufferSize + " AS BUFFER_LENGTH,NUMERIC_SCALE AS DECIMAL_DIGITS, " + Integer.toString(java.sql.DatabaseMetaData.versionColumnNotPseudo)
                + " AS PSEUDO_COLUMN FROM INFORMATION_SCHEMA.COLUMNS "
                + "WHERE TABLE_SCHEMA LIKE ? AND TABLE_NAME LIKE ? AND EXTRA LIKE '%on update CURRENT_TIMESTAMP%'");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());

            if (catalog != null) {
                pStmt.setString(1, catalog);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, table);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition()
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
        if ((functionNamePattern == null) || (functionNamePattern.length() == 0)) {
            if (this.nullNamePatternMatchesAll) {
                functionNamePattern = "%";
            } else {
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.11"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        String db = null;

        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                db = this.database;
            }
        } else {
            db = catalog;
        }

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
        // SPECIFIC_NAME *
        sqlBuf.append(getFunctionConstant(FunctionConstant.FUNCTION_NULLABLE) + " AS `NULLABLE`,  NULL AS `REMARKS`, "
                + "CHARACTER_OCTET_LENGTH AS `CHAR_OCTET_LENGTH`,  ORDINAL_POSITION, 'YES' AS `IS_NULLABLE`, SPECIFIC_NAME "
                + "FROM INFORMATION_SCHEMA.PARAMETERS WHERE "
                + "SPECIFIC_SCHEMA LIKE ? AND SPECIFIC_NAME LIKE ? AND (PARAMETER_NAME LIKE ? OR PARAMETER_NAME IS NULL) "
                + "AND ROUTINE_TYPE='FUNCTION' ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION");

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sqlBuf.toString());

            if (db != null) {
                pStmt.setString(1, db);
            } else {
                pStmt.setString(1, "%");
            }

            pStmt.setString(2, functionNamePattern);
            pStmt.setString(3, columnNamePattern);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition().setFields(createFunctionColumnsFields());

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

        if ((functionNamePattern == null) || (functionNamePattern.length() == 0)) {
            if (this.nullNamePatternMatchesAll) {
                functionNamePattern = "%";
            } else {
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.22"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        String db = null;

        if (catalog == null) {
            if (this.nullCatalogMeansCurrent) {
                db = this.database;
            }
        } else {
            db = catalog;
        }

        String sql = "SELECT ROUTINE_SCHEMA AS FUNCTION_CAT, NULL AS FUNCTION_SCHEM, ROUTINE_NAME AS FUNCTION_NAME, ROUTINE_COMMENT AS REMARKS, "
                + getFunctionNoTableConstant() + " AS FUNCTION_TYPE, ROUTINE_NAME AS SPECIFIC_NAME FROM INFORMATION_SCHEMA.ROUTINES "
                + "WHERE ROUTINE_TYPE LIKE 'FUNCTION' AND ROUTINE_SCHEMA LIKE ? AND "
                + "ROUTINE_NAME LIKE ? ORDER BY FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME, SPECIFIC_NAME";

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sql);

            pStmt.setString(1, db != null ? db : "%");
            pStmt.setString(2, functionNamePattern);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.cj.api.jdbc.result.ResultSetInternalMethods) rs).getColumnDefinition()
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

        buf.append(" ELSE 1111");
        buf.append(" END ");

    }
}
