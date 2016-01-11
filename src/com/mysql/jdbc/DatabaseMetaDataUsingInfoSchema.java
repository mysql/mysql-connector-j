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

package com.mysql.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * DatabaseMetaData implementation that uses INFORMATION_SCHEMA available in MySQL-5.0 and newer.
 */
public class DatabaseMetaDataUsingInfoSchema extends DatabaseMetaData {

    protected enum JDBC4FunctionConstant {
        // COLUMN_TYPE values
        FUNCTION_COLUMN_UNKNOWN, FUNCTION_COLUMN_IN, FUNCTION_COLUMN_INOUT, FUNCTION_COLUMN_OUT, FUNCTION_COLUMN_RETURN, FUNCTION_COLUMN_RESULT,
        // NULLABLE values
        FUNCTION_NO_NULLS, FUNCTION_NULLABLE, FUNCTION_NULLABLE_UNKNOWN;
    }

    private boolean hasReferentialConstraintsView;
    private final boolean hasParametersView;

    protected DatabaseMetaDataUsingInfoSchema(MySQLConnection connToSet, String databaseToSet) throws SQLException {
        super(connToSet, databaseToSet);

        this.hasReferentialConstraintsView = this.conn.versionMeetsMinimum(5, 1, 10);

        ResultSet rs = null;

        try {
            rs = super.getTables("INFORMATION_SCHEMA", null, "PARAMETERS", new String[0]);

            this.hasParametersView = rs.next();
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    protected ResultSet executeMetadataQuery(java.sql.PreparedStatement pStmt) throws SQLException {
        ResultSet rs = pStmt.executeQuery();
        ((com.mysql.jdbc.ResultSetInternalMethods) rs).setOwningStatement(null);

        return rs;
    }

    /**
     * Get a description of the access rights for a table's columns.
     * <P>
     * Only privileges matching the column name criteria are returned. They are ordered by COLUMN_NAME and PRIVILEGE.
     * </p>
     * <P>
     * Each privilige description has the following columns:
     * <OL>
     * <li><B>TABLE_CAT</B> String => table catalog (may be null)</li>
     * <li><B>TABLE_SCHEM</B> String => table schema (may be null)</li>
     * <li><B>TABLE_NAME</B> String => table name</li>
     * <li><B>COLUMN_NAME</B> String => column name</li>
     * <li><B>GRANTOR</B> => grantor of access (may be null)</li>
     * <li><B>GRANTEE</B> String => grantee of access</li>
     * <li><B>PRIVILEGE</B> String => name of access (SELECT, INSERT, UPDATE, REFRENCES, ...)</li>
     * <li><B>IS_GRANTABLE</B> String => "YES" if grantee is permitted to grant to others; "NO" if not; null if unknown</li>
     * </ol>
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schema
     *            a schema name; "" retrieves those without a schema
     * @param table
     *            a table name
     * @param columnNamePattern
     *            a column name pattern
     * @return ResultSet each row is a column privilege description
     * @throws SQLException
     *             if a database access error occurs
     * @see #getSearchStringEscape
     */
    @Override
    public java.sql.ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        if (columnNamePattern == null) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                columnNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Column name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
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
            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(new Field[] { new Field("", "TABLE_CAT", Types.CHAR, 64),
                    new Field("", "TABLE_SCHEM", Types.CHAR, 1), new Field("", "TABLE_NAME", Types.CHAR, 64), new Field("", "COLUMN_NAME", Types.CHAR, 64),
                    new Field("", "GRANTOR", Types.CHAR, 77), new Field("", "GRANTEE", Types.CHAR, 77), new Field("", "PRIVILEGE", Types.CHAR, 64),
                    new Field("", "IS_GRANTABLE", Types.CHAR, 3) });

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Get a description of table columns available in a catalog.
     * <P>
     * Only column descriptions matching the catalog, schema, table and column name criteria are returned. They are ordered by TABLE_SCHEM, TABLE_NAME and
     * ORDINAL_POSITION.
     * </p>
     * <P>
     * Each column description has the following columns:
     * <OL>
     * <li><B>TABLE_CAT</B> String => table catalog (may be null)</li>
     * <li><B>TABLE_SCHEM</B> String => table schema (may be null)</li>
     * <li><B>TABLE_NAME</B> String => table name</li>
     * <li><B>COLUMN_NAME</B> String => column name</li>
     * <li><B>DATA_TYPE</B> short => SQL type from java.sql.Types</li>
     * <li><B>TYPE_NAME</B> String => Data source dependent type name</li>
     * <li><B>COLUMN_SIZE</B> int => column size. For char or date types this is the maximum number of characters, for numeric or decimal types this is
     * precision.</li>
     * <li><B>BUFFER_LENGTH</B> is not used.</li>
     * <li><B>DECIMAL_DIGITS</B> int => the number of fractional digits</li>
     * <li><B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or 2)</li>
     * <li><B>NULLABLE</B> int => is NULL allowed?
     * <UL>
     * <li>columnNoNulls - might not allow NULL values</li>
     * <li>columnNullable - definitely allows NULL values</li>
     * <li>columnNullableUnknown - nullability unknown</li>
     * </ul>
     * </li>
     * <li><B>REMARKS</B> String => comment describing column (may be null)</li>
     * <li><B>COLUMN_DEF</B> String => default value (may be null)</li>
     * <li><B>SQL_DATA_TYPE</B> int => unused</li>
     * <li><B>SQL_DATETIME_SUB</B> int => unused</li>
     * <li><B>CHAR_OCTET_LENGTH</B> int => for char types the maximum number of bytes in the column</li>
     * <li><B>ORDINAL_POSITION</B> int => index of column in table (starting at 1)</li>
     * <li><B>IS_NULLABLE</B> String => "NO" means column definitely does not allow NULL values; "YES" means the column might allow NULL values. An empty string
     * means nobody knows.</li>
     * </ol>
     * </p>
     */
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableName, String columnNamePattern) throws SQLException {
        if (columnNamePattern == null) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                columnNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Column name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
                catalog = this.database;
            }
        }

        StringBuilder sqlBuf = new StringBuilder("SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME,");
        MysqlDefs.appendJdbcTypeMappingQuery(sqlBuf, "DATA_TYPE");

        sqlBuf.append(" AS DATA_TYPE, ");

        if (this.conn.getCapitalizeTypeNames()) {
            sqlBuf.append("UPPER(CASE WHEN LOCATE('unsigned', COLUMN_TYPE) != 0 AND LOCATE('unsigned', DATA_TYPE) = 0 AND LOCATE('set', DATA_TYPE) <> 1 AND "
                    + "LOCATE('enum', DATA_TYPE) <> 1 THEN CONCAT(DATA_TYPE, ' unsigned') ELSE DATA_TYPE END) AS TYPE_NAME,");
        } else {
            sqlBuf.append("CASE WHEN LOCATE('unsigned', COLUMN_TYPE) != 0 AND LOCATE('unsigned', DATA_TYPE) = 0 AND LOCATE('set', DATA_TYPE) <> 1 AND "
                    + "LOCATE('enum', DATA_TYPE) <> 1 THEN CONCAT(DATA_TYPE, ' unsigned') ELSE DATA_TYPE END AS TYPE_NAME,");
        }

        sqlBuf.append("CASE WHEN LCASE(DATA_TYPE)='date' THEN 10 WHEN LCASE(DATA_TYPE)='time' THEN 8 WHEN LCASE(DATA_TYPE)='datetime' THEN 19 "
                + "WHEN LCASE(DATA_TYPE)='timestamp' THEN 19 WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN NUMERIC_PRECISION WHEN CHARACTER_MAXIMUM_LENGTH > "
                + Integer.MAX_VALUE + " THEN " + Integer.MAX_VALUE + " ELSE CHARACTER_MAXIMUM_LENGTH END AS COLUMN_SIZE, " + MysqlIO.getMaxBuf()
                + " AS BUFFER_LENGTH," + "NUMERIC_SCALE AS DECIMAL_DIGITS," + "10 AS NUM_PREC_RADIX," + "CASE WHEN IS_NULLABLE='NO' THEN " + columnNoNulls
                + " ELSE CASE WHEN IS_NULLABLE='YES' THEN " + columnNullable + " ELSE " + columnNullableUnknown + " END END AS NULLABLE,"
                + "COLUMN_COMMENT AS REMARKS," + "COLUMN_DEFAULT AS COLUMN_DEF," + "0 AS SQL_DATA_TYPE," + "0 AS SQL_DATETIME_SUB,"
                + "CASE WHEN CHARACTER_OCTET_LENGTH > " + Integer.MAX_VALUE + " THEN " + Integer.MAX_VALUE
                + " ELSE CHARACTER_OCTET_LENGTH END AS CHAR_OCTET_LENGTH," + "ORDINAL_POSITION," + "IS_NULLABLE," + "NULL AS SCOPE_CATALOG,"
                + "NULL AS SCOPE_SCHEMA," + "NULL AS SCOPE_TABLE," + "NULL AS SOURCE_DATA_TYPE,"
                + "IF (EXTRA LIKE '%auto_increment%','YES','NO') AS IS_AUTOINCREMENT, "
                + "IF (EXTRA LIKE '%GENERATED%','YES','NO') AS IS_GENERATEDCOLUMN FROM INFORMATION_SCHEMA.COLUMNS WHERE ");

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

            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(createColumnsFields());
            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Get a description of the foreign key columns in the foreign key table
     * that reference the primary key columns of the primary key table (describe
     * how one table imports another's key.) This should normally return a
     * single foreign key/primary key pair (most tables only import a foreign
     * key from a table once.) They are ordered by FKTABLE_CAT, FKTABLE_SCHEM,
     * FKTABLE_NAME, and KEY_SEQ.
     * <P>
     * Each foreign key column description has the following columns:
     * <OL>
     * <li><B>PKTABLE_CAT</B> String => primary key table catalog (may be null)</li>
     * <li><B>PKTABLE_SCHEM</B> String => primary key table schema (may be null)</li>
     * <li><B>PKTABLE_NAME</B> String => primary key table name</li>
     * <li><B>PKCOLUMN_NAME</B> String => primary key column name</li>
     * <li><B>FKTABLE_CAT</B> String => foreign key table catalog (may be null) being exported (may be null)</li>
     * <li><B>FKTABLE_SCHEM</B> String => foreign key table schema (may be null) being exported (may be null)</li>
     * <li><B>FKTABLE_NAME</B> String => foreign key table name being exported</li>
     * <li><B>FKCOLUMN_NAME</B> String => foreign key column name being exported</li>
     * <li><B>KEY_SEQ</B> short => sequence number within foreign key</li>
     * <li><B>UPDATE_RULE</B> short => What happens to foreign key when primary is updated:
     * <UL>
     * <li>importedKeyCascade - change imported key to agree with primary key update</li>
     * <li>importedKeyRestrict - do not allow update of primary key if it has been imported</li>
     * <li>importedKeySetNull - change imported key to NULL if its primary key has been updated</li>
     * </ul>
     * </li>
     * <li><B>DELETE_RULE</B> short => What happens to the foreign key when primary is deleted.
     * <UL>
     * <li>importedKeyCascade - delete rows that import a deleted key</li>
     * <li>importedKeyRestrict - do not allow delete of primary key if it has been imported</li>
     * <li>importedKeySetNull - change imported key to NULL if its primary key has been deleted</li>
     * </ul>
     * </li>
     * <li><B>FK_NAME</B> String => foreign key identifier (may be null)</li>
     * <li><B>PK_NAME</B> String => primary key identifier (may be null)</li>
     * </ol>
     * </p>
     * 
     * @param primaryCatalog
     *            a catalog name; "" retrieves those without a catalog
     * @param primarySchema
     *            a schema name pattern; "" retrieves those without a schema
     * @param primaryTable
     *            a table name
     * @param foreignCatalog
     *            a catalog name; "" retrieves those without a catalog
     * @param foreignSchema
     *            a schema name pattern; "" retrieves those without a schema
     * @param foreignTable
     *            a table name
     * @return ResultSet each row is a foreign key column description
     * @throws SQLException
     *             if a database access error occurs
     */
    @Override
    public java.sql.ResultSet getCrossReference(String primaryCatalog, String primarySchema, String primaryTable, String foreignCatalog, String foreignSchema,
            String foreignTable) throws SQLException {
        if (primaryTable == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        if (primaryCatalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
                primaryCatalog = this.database;
            }
        }

        if (foreignCatalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
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
            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(createFkMetadataFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Get a description of a foreign key columns that reference a table's
     * primary key columns (the foreign keys exported by a table). They are
     * ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ.
     * <P>
     * Each foreign key column description has the following columns:
     * <OL>
     * <li><B>PKTABLE_CAT</B> String => primary key table catalog (may be null)</li>
     * <li><B>PKTABLE_SCHEM</B> String => primary key table schema (may be null)</li>
     * <li><B>PKTABLE_NAME</B> String => primary key table name</li>
     * <li><B>PKCOLUMN_NAME</B> String => primary key column name</li>
     * <li><B>FKTABLE_CAT</B> String => foreign key table catalog (may be null) being exported (may be null)</li>
     * <li><B>FKTABLE_SCHEM</B> String => foreign key table schema (may be null) being exported (may be null)</li>
     * <li><B>FKTABLE_NAME</B> String => foreign key table name being exported</li>
     * <li><B>FKCOLUMN_NAME</B> String => foreign key column name being exported</li>
     * <li><B>KEY_SEQ</B> short => sequence number within foreign key</li>
     * <li><B>UPDATE_RULE</B> short => What happens to foreign key when primary is updated:
     * <UL>
     * <li>importedKeyCascade - change imported key to agree with primary key update</li>
     * <li>importedKeyRestrict - do not allow update of primary key if it has been imported</li>
     * <li>importedKeySetNull - change imported key to NULL if its primary key has been updated</li>
     * </ul>
     * </li>
     * <li><B>DELETE_RULE</B> short => What happens to the foreign key when primary is deleted.
     * <UL>
     * <li>importedKeyCascade - delete rows that import a deleted key</li>
     * <li>importedKeyRestrict - do not allow delete of primary key if it has been imported</li>
     * <li>importedKeySetNull - change imported key to NULL if its primary key has been deleted</li>
     * </ul>
     * </li>
     * <li><B>FK_NAME</B> String => foreign key identifier (may be null)</li>
     * <li><B>PK_NAME</B> String => primary key identifier (may be null)</li>
     * </ol>
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schema
     *            a schema name pattern; "" retrieves those without a schema
     * @param table
     *            a table name
     * @return ResultSet each row is a foreign key column description
     * @throws SQLException
     *             if a database access error occurs
     * @see #getImportedKeys
     */
    @Override
    public java.sql.ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        // TODO: Can't determine actions using INFORMATION_SCHEMA yet...

        if (table == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
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

            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(createFkMetadataFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }

    }

    private String generateOptionalRefContraintsJoin() {
        return ((this.hasReferentialConstraintsView) ? "JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS R ON (R.CONSTRAINT_NAME = B.CONSTRAINT_NAME "
                + "AND R.TABLE_NAME = B.TABLE_NAME AND R.CONSTRAINT_SCHEMA = B.TABLE_SCHEMA) " : "");
    }

    private String generateDeleteRuleClause() {
        return ((this.hasReferentialConstraintsView) ? "CASE WHEN R.DELETE_RULE='CASCADE' THEN " + String.valueOf(importedKeyCascade)
                + " WHEN R.DELETE_RULE='SET NULL' THEN " + String.valueOf(importedKeySetNull) + " WHEN R.DELETE_RULE='SET DEFAULT' THEN "
                + String.valueOf(importedKeySetDefault) + " WHEN R.DELETE_RULE='RESTRICT' THEN " + String.valueOf(importedKeyRestrict)
                + " WHEN R.DELETE_RULE='NO ACTION' THEN " + String.valueOf(importedKeyNoAction) + " ELSE " + String.valueOf(importedKeyNoAction) + " END "
                : String.valueOf(importedKeyRestrict));
    }

    private String generateUpdateRuleClause() {
        return ((this.hasReferentialConstraintsView) ? "CASE WHEN R.UPDATE_RULE='CASCADE' THEN " + String.valueOf(importedKeyCascade)
                + " WHEN R.UPDATE_RULE='SET NULL' THEN " + String.valueOf(importedKeySetNull) + " WHEN R.UPDATE_RULE='SET DEFAULT' THEN "
                + String.valueOf(importedKeySetDefault) + " WHEN R.UPDATE_RULE='RESTRICT' THEN " + String.valueOf(importedKeyRestrict)
                + " WHEN R.UPDATE_RULE='NO ACTION' THEN " + String.valueOf(importedKeyNoAction) + " ELSE " + String.valueOf(importedKeyNoAction) + " END "
                : String.valueOf(importedKeyRestrict));
    }

    /**
     * Get a description of the primary key columns that are referenced by a
     * table's foreign key columns (the primary keys imported by a table). They
     * are ordered by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
     * <P>
     * Each primary key column description has the following columns:
     * <OL>
     * <li><B>PKTABLE_CAT</B> String => primary key table catalog being imported (may be null)</li>
     * <li><B>PKTABLE_SCHEM</B> String => primary key table schema being imported (may be null)</li>
     * <li><B>PKTABLE_NAME</B> String => primary key table name being imported</li>
     * <li><B>PKCOLUMN_NAME</B> String => primary key column name being imported</li>
     * <li><B>FKTABLE_CAT</B> String => foreign key table catalog (may be null)</li>
     * <li><B>FKTABLE_SCHEM</B> String => foreign key table schema (may be null)</li>
     * <li><B>FKTABLE_NAME</B> String => foreign key table name</li>
     * <li><B>FKCOLUMN_NAME</B> String => foreign key column name</li>
     * <li><B>KEY_SEQ</B> short => sequence number within foreign key</li>
     * <li><B>UPDATE_RULE</B> short => What happens to foreign key when primary is updated:
     * <UL>
     * <li>importedKeyCascade - change imported key to agree with primary key update</li>
     * <li>importedKeyRestrict - do not allow update of primary key if it has been imported</li>
     * <li>importedKeySetNull - change imported key to NULL if its primary key has been updated</li>
     * </ul>
     * </li>
     * <li><B>DELETE_RULE</B> short => What happens to the foreign key when primary is deleted.
     * <UL>
     * <li>importedKeyCascade - delete rows that import a deleted key</li>
     * <li>importedKeyRestrict - do not allow delete of primary key if it has been imported</li>
     * <li>importedKeySetNull - change imported key to NULL if its primary key has been deleted</li>
     * </ul>
     * </li>
     * <li><B>FK_NAME</B> String => foreign key name (may be null)</li>
     * <li><B>PK_NAME</B> String => primary key name (may be null)</li>
     * </ol>
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schema
     *            a schema name pattern; "" retrieves those without a schema
     * @param table
     *            a table name
     * @return ResultSet each row is a primary key column description
     * @throws SQLException
     *             if a database access error occurs
     * @see #getExportedKeys
     */
    @Override
    public java.sql.ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        if (table == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
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

            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(createFkMetadataFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Get a description of a table's indices and statistics. They are ordered
     * by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
     * <P>
     * Each index column description has the following columns:
     * <OL>
     * <li><B>TABLE_CAT</B> String => table catalog (may be null)</li>
     * <li><B>TABLE_SCHEM</B> String => table schema (may be null)</li>
     * <li><B>TABLE_NAME</B> String => table name</li>
     * <li><B>NON_UNIQUE</B> boolean => Can index values be non-unique? false when TYPE is tableIndexStatistic</li>
     * <li><B>INDEX_QUALIFIER</B> String => index catalog (may be null); null when TYPE is tableIndexStatistic</li>
     * <li><B>INDEX_NAME</B> String => index name; null when TYPE is tableIndexStatistic</li>
     * <li><B>TYPE</B> short => index type:
     * <UL>
     * <li>tableIndexStatistic - this identifies table statistics that are returned in conjuction with a table's index descriptions</li>
     * <li>tableIndexClustered - this is a clustered index</li>
     * <li>tableIndexHashed - this is a hashed index</li>
     * <li>tableIndexOther - this is some other style of index</li>
     * </ul>
     * </li>
     * <li><B>ORDINAL_POSITION</B> short => column sequence number within index; zero when TYPE is tableIndexStatistic</li>
     * <li><B>COLUMN_NAME</B> String => column name; null when TYPE is tableIndexStatistic</li>
     * <li><B>ASC_OR_DESC</B> String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not supported; null when TYPE
     * is tableIndexStatistic</li>
     * <li><B>CARDINALITY</B> int => When TYPE is tableIndexStatisic then this is the number of rows in the table; otherwise it is the number of unique values
     * in the index.</li>
     * <li><B>PAGES</B> int => When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages used for
     * the current index.</li>
     * <li><B>FILTER_CONDITION</B> String => Filter condition, if any. (may be null)</li>
     * </ol>
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schema
     *            a schema name pattern; "" retrieves those without a schema
     * @param table
     *            a table name
     * @param unique
     *            when true, return only indices for unique values; when false,
     *            return indices regardless of whether unique or not
     * @param approximate
     *            when true, result is allowed to reflect approximate or out of
     *            data values; when false, results are requested to be accurate
     * @return ResultSet each row is an index column description
     * @throws SQLException
     */
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
                if (this.conn.getNullCatalogMeansCurrent()) {
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

            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(createIndexInfoFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Get a description of a table's primary key columns. They are ordered by
     * COLUMN_NAME.
     * <P>
     * Each column description has the following columns:
     * <OL>
     * <li><B>TABLE_CAT</B> String => table catalog (may be null)</li>
     * <li><B>TABLE_SCHEM</B> String => table schema (may be null)</li>
     * <li><B>TABLE_NAME</B> String => table name</li>
     * <li><B>COLUMN_NAME</B> String => column name</li>
     * <li><B>KEY_SEQ</B> short => sequence number within primary key</li>
     * <li><B>PK_NAME</B> String => primary key name (may be null)</li>
     * </ol>
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schema
     *            a schema name pattern; "" retrieves those without a schema
     * @param table
     *            a table name
     * @return ResultSet each row is a primary key column description
     * @throws SQLException
     */
    @Override
    public java.sql.ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {

        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
                catalog = this.database;
            }
        }

        if (table == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
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
            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(new Field[] { new Field("", "TABLE_CAT", Types.CHAR, 255),
                    new Field("", "TABLE_SCHEM", Types.CHAR, 0), new Field("", "TABLE_NAME", Types.CHAR, 255), new Field("", "COLUMN_NAME", Types.CHAR, 32),
                    new Field("", "KEY_SEQ", Types.SMALLINT, 5), new Field("", "PK_NAME", Types.CHAR, 32) });

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Get a description of stored procedures available in a catalog.
     * <P>
     * Only procedure descriptions matching the schema and procedure name criteria are returned. They are ordered by PROCEDURE_SCHEM, and PROCEDURE_NAME.
     * </p>
     * <P>
     * Each procedure description has the the following columns:
     * <OL>
     * <li><B>PROCEDURE_CAT</B> String => procedure catalog (may be null)</li>
     * <li><B>PROCEDURE_SCHEM</B> String => procedure schema (may be null)</li>
     * <li><B>PROCEDURE_NAME</B> String => procedure name</li>
     * <li>reserved for future use</li>
     * <li>reserved for future use</li>
     * <li>reserved for future use</li>
     * <li><B>REMARKS</B> String => explanatory comment on the procedure</li>
     * <li><B>PROCEDURE_TYPE</B> short => kind of procedure:
     * <UL>
     * <li>procedureResultUnknown - May return a result</li>
     * <li>procedureNoResult - Does not return a result</li>
     * <li>procedureReturnsResult - Returns a result</li>
     * </ul>
     * </li>
     * </ol>
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schemaPattern
     *            a schema name pattern; "" retrieves those without a schema
     * @param procedureNamePattern
     *            a procedure name pattern
     * @return ResultSet each row is a procedure description
     * @throws SQLException
     *             if a database access error occurs
     * @see #getSearchStringEscape
     */
    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {

        if ((procedureNamePattern == null) || (procedureNamePattern.length() == 0)) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                procedureNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Procedure name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        String db = null;

        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
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
            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(createFieldMetadataForGetProcedures());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Returns a condition to be injected in the query that returns metadata for procedures only. Overridden by
     * subclasses when needed. When not empty must end with "AND ".
     * 
     * @return String with the condition to be injected.
     */
    protected String getRoutineTypeConditionForGetProcedures() {
        return "";
    }

    /**
     * Retrieves a description of the given catalog's stored procedure parameter
     * and result columns.
     * 
     * <P>
     * Only descriptions matching the schema, procedure and parameter name criteria are returned. They are ordered by PROCEDURE_SCHEM and PROCEDURE_NAME. Within
     * this, the return value, if any, is first. Next are the parameter descriptions in call order. The column descriptions follow in column number order.
     * 
     * <P>
     * Each row in the <code>ResultSet</code> is a parameter description or column description with the following fields:
     * <OL>
     * <LI><B>PROCEDURE_CAT</B> String => procedure catalog (may be <code>null</code>)
     * <LI><B>PROCEDURE_SCHEM</B> String => procedure schema (may be <code>null</code>)
     * <LI><B>PROCEDURE_NAME</B> String => procedure name
     * <LI><B>COLUMN_NAME</B> String => column/parameter name
     * <LI><B>COLUMN_TYPE</B> Short => kind of column/parameter:
     * <UL>
     * <LI>procedureColumnUnknown - nobody knows
     * <LI>procedureColumnIn - IN parameter
     * <LI>procedureColumnInOut - INOUT parameter
     * <LI>procedureColumnOut - OUT parameter
     * <LI>procedureColumnReturn - procedure return value
     * <LI>procedureColumnResult - result column in <code>ResultSet</code>
     * </UL>
     * <LI><B>DATA_TYPE</B> int => SQL type from java.sql.Types
     * <LI><B>TYPE_NAME</B> String => SQL type name, for a UDT type the type name is fully qualified
     * <LI><B>PRECISION</B> int => precision
     * <LI><B>LENGTH</B> int => length in bytes of data
     * <LI><B>SCALE</B> short => scale
     * <LI><B>RADIX</B> short => radix
     * <LI><B>NULLABLE</B> short => can it contain NULL.
     * <UL>
     * <LI>procedureNoNulls - does not allow NULL values
     * <LI>procedureNullable - allows NULL values
     * <LI>procedureNullableUnknown - nullability unknown
     * </UL>
     * <LI><B>REMARKS</B> String => comment describing parameter/column
     * </OL>
     * 
     * <P>
     * <B>Note:</B> Some databases may not return the column descriptions for a procedure. Additional columns beyond REMARKS can be defined by the database.
     * 
     * @param catalog
     *            a catalog name; must match the catalog name as it
     *            is stored in the database; "" retrieves those without a catalog; <code>null</code> means that the catalog name should not be used to narrow
     *            the search
     * @param schemaPattern
     *            a schema name pattern; must match the schema name
     *            as it is stored in the database; "" retrieves those without a schema; <code>null</code> means that the schema name should not be used to
     *            narrow
     *            the search
     * @param procedureNamePattern
     *            a procedure name pattern; must match the
     *            procedure name as it is stored in the database
     * @param columnNamePattern
     *            a column name pattern; must match the column name
     *            as it is stored in the database
     * @return <code>ResultSet</code> - each row describes a stored procedure parameter or
     *         column
     * @exception SQLException
     *                if a database access error occurs
     * @see #getSearchStringEscape
     */
    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        if (!this.hasParametersView) {
            return getProcedureColumnsNoISParametersView(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
        }

        if ((procedureNamePattern == null) || (procedureNamePattern.length() == 0)) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                procedureNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Procedure name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        String db = null;

        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
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
        MysqlDefs.appendJdbcTypeMappingQuery(sqlBuf, "DATA_TYPE");

        sqlBuf.append(" AS `DATA_TYPE`, ");

        // TYPE_NAME
        if (this.conn.getCapitalizeTypeNames()) {
            sqlBuf.append("UPPER(CASE WHEN LOCATE('unsigned', DATA_TYPE) != 0 AND LOCATE('unsigned', DATA_TYPE) = 0 THEN CONCAT(DATA_TYPE, ' unsigned') "
                    + "ELSE DATA_TYPE END) AS `TYPE_NAME`,");
        } else {
            sqlBuf.append("CASE WHEN LOCATE('unsigned', DATA_TYPE) != 0 AND LOCATE('unsigned', DATA_TYPE) = 0 THEN CONCAT(DATA_TYPE, ' unsigned') "
                    + "ELSE DATA_TYPE END AS `TYPE_NAME`,");
        }

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
            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(createProcedureColumnsFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Redirects to another implementation of #getProcedureColumns. Subclasses may need to override this method.
     * 
     * @see getProcedureColumns
     */
    protected ResultSet getProcedureColumnsNoISParametersView(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
            throws SQLException {
        return super.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    }

    /**
     * Returns a condition to be injected in the query that returns metadata for procedure columns only. Overridden by
     * subclasses when needed. When not empty must end with "AND ".
     * 
     * @return String with the condition to be injected.
     */
    protected String getRoutineTypeConditionForGetProcedureColumns() {
        return "";
    }

    /**
     * Get a description of tables available in a catalog.
     * <P>
     * Only table descriptions matching the catalog, schema, table name and type criteria are returned. They are ordered by TABLE_TYPE, TABLE_SCHEM and
     * TABLE_NAME.
     * </p>
     * <P>
     * Each table description has the following columns:
     * <OL>
     * <li><B>TABLE_CAT</B> String => table catalog (may be null)</li>
     * <li><B>TABLE_SCHEM</B> String => table schema (may be null)</li>
     * <li><B>TABLE_NAME</B> String => table name</li>
     * <li><B>TABLE_TYPE</B> String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     * </li>
     * <li><B>REMARKS</B> String => explanatory comment on the table</li>
     * </ol>
     * </p>
     * <P>
     * <B>Note:</B> Some databases may not return information for all tables.
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schemaPattern
     *            a schema name pattern; "" retrieves those without a schema
     * @param tableNamePattern
     *            a table name pattern
     * @param types
     *            a list of table types to include; null returns all types
     * @return ResultSet each row is a table description
     * @throws SQLException
     * @see #getSearchStringEscape
     */
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
                catalog = this.database;
            }
        }

        if (tableNamePattern == null) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                tableNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Table name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        final String tableNamePat;
        String tmpCat = "";

        if ((catalog == null) || (catalog.length() == 0)) {
            if (this.conn.getNullCatalogMeansCurrent()) {
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
                    pStmt.setNull(3 + i, Types.VARCHAR);
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

            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(createTablesFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    public boolean gethasParametersView() {
        return this.hasParametersView;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {

        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
                catalog = this.database;
            }
        }

        if (table == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        StringBuilder sqlBuf = new StringBuilder("SELECT NULL AS SCOPE, COLUMN_NAME, ");

        MysqlDefs.appendJdbcTypeMappingQuery(sqlBuf, "DATA_TYPE");
        sqlBuf.append(" AS DATA_TYPE, ");

        sqlBuf.append("COLUMN_TYPE AS TYPE_NAME, ");
        sqlBuf.append("CASE WHEN LCASE(DATA_TYPE)='date' THEN 10 WHEN LCASE(DATA_TYPE)='time' THEN 8 "
                + "WHEN LCASE(DATA_TYPE)='datetime' THEN 19 WHEN LCASE(DATA_TYPE)='timestamp' THEN 19 "
                + "WHEN CHARACTER_MAXIMUM_LENGTH IS NULL THEN NUMERIC_PRECISION WHEN CHARACTER_MAXIMUM_LENGTH > " + Integer.MAX_VALUE + " THEN "
                + Integer.MAX_VALUE + " ELSE CHARACTER_MAXIMUM_LENGTH END AS COLUMN_SIZE, ");
        sqlBuf.append(MysqlIO.getMaxBuf() + " AS BUFFER_LENGTH,NUMERIC_SCALE AS DECIMAL_DIGITS, "
                + Integer.toString(java.sql.DatabaseMetaData.versionColumnNotPseudo) + " AS PSEUDO_COLUMN FROM INFORMATION_SCHEMA.COLUMNS "
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
            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(new Field[] { new Field("", "SCOPE", Types.SMALLINT, 5),
                    new Field("", "COLUMN_NAME", Types.CHAR, 32), new Field("", "DATA_TYPE", Types.INTEGER, 5), new Field("", "TYPE_NAME", Types.CHAR, 16),
                    new Field("", "COLUMN_SIZE", Types.INTEGER, 16), new Field("", "BUFFER_LENGTH", Types.INTEGER, 16),
                    new Field("", "DECIMAL_DIGITS", Types.SMALLINT, 16), new Field("", "PSEUDO_COLUMN", Types.SMALLINT, 5) });

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    //
    // JDBC-4.0 functions that aren't reliant on Java6
    //

    /**
     * Retrieves a description of the given catalog's system or user
     * function parameters and return type.
     * 
     * <P>
     * Only descriptions matching the schema, function and parameter name criteria are returned. They are ordered by <code>FUNCTION_CAT</code>,
     * <code>FUNCTION_SCHEM</code>, <code>FUNCTION_NAME</code> and <code>SPECIFIC_ NAME</code>. Within this, the return value, if any, is first. Next are the
     * parameter descriptions in call order. The column descriptions follow in column number order.
     * 
     * <P>
     * Each row in the <code>ResultSet</code> is a parameter description, column description or return type description with the following fields:
     * <OL>
     * <LI><B>FUNCTION_CAT</B> String => function catalog (may be <code>null</code>)
     * <LI><B>FUNCTION_SCHEM</B> String => function schema (may be <code>null</code>)
     * <LI><B>FUNCTION_NAME</B> String => function name. This is the name used to invoke the function
     * <LI><B>COLUMN_NAME</B> String => column/parameter name
     * <LI><B>COLUMN_TYPE</B> Short => kind of column/parameter:
     * <UL>
     * <LI>functionColumnUnknown - nobody knows
     * <LI>functionColumnIn - IN parameter
     * <LI>functionColumnInOut - INOUT parameter
     * <LI>functionColumnOut - OUT parameter
     * <LI>functionColumnReturn - function return value
     * <LI>functionColumnResult - Indicates that the parameter or column is a column in the <code>ResultSet</code>
     * </UL>
     * <LI><B>DATA_TYPE</B> int => SQL type from java.sql.Types
     * <LI><B>TYPE_NAME</B> String => SQL type name, for a UDT type the type name is fully qualified
     * <LI><B>PRECISION</B> int => precision
     * <LI><B>LENGTH</B> int => length in bytes of data
     * <LI><B>SCALE</B> short => scale - null is returned for data types where SCALE is not applicable.
     * <LI><B>RADIX</B> short => radix
     * <LI><B>NULLABLE</B> short => can it contain NULL.
     * <UL>
     * <LI>functionNoNulls - does not allow NULL values
     * <LI>functionNullable - allows NULL values
     * <LI>functionNullableUnknown - nullability unknown
     * </UL>
     * <LI><B>REMARKS</B> String => comment describing column/parameter
     * <LI><B>CHAR_OCTET_LENGTH</B> int => the maximum length of binary and character based parameters or columns. For any other datatype the returned value is
     * a NULL
     * <LI><B>ORDINAL_POSITION</B> int => the ordinal position, starting from 1, for the input and output parameters. A value of 0 is returned if this row
     * describes the function's return value. For result set columns, it is the ordinal position of the column in the result set starting from 1.
     * <LI><B>IS_NULLABLE</B> String => ISO rules are used to determine the nullability for a parameter or column.
     * <UL>
     * <LI>YES --- if the parameter or column can include NULLs
     * <LI>NO --- if the parameter or column cannot include NULLs
     * <LI>empty string --- if the nullability for the parameter or column is unknown
     * </UL>
     * <LI><B>SPECIFIC_NAME</B> String => the name which uniquely identifies this function within its schema. This is a user specified, or DBMS generated, name
     * that may be different then the <code>FUNCTION_NAME</code> for example with overload functions
     * </OL>
     * 
     * <p>
     * The PRECISION column represents the specified column size for the given parameter or column. For numeric data, this is the maximum precision. For
     * character data, this is the length in characters. For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes. For the ROWID datatype, this is the length
     * in bytes. Null is returned for data types where the column size is not applicable.
     * 
     * @param catalog
     *            a catalog name; must match the catalog name as it
     *            is stored in the database; "" retrieves those without a catalog; <code>null</code> means that the catalog name should not be used to narrow
     *            the search
     * @param schemaPattern
     *            a schema name pattern; must match the schema name
     *            as it is stored in the database; "" retrieves those without a schema; <code>null</code> means that the schema name should not be used to
     *            narrow
     *            the search
     * @param functionNamePattern
     *            a procedure name pattern; must match the
     *            function name as it is stored in the database
     * @param columnNamePattern
     *            a parameter name pattern; must match the
     *            parameter or column name as it is stored in the database
     * @return <code>ResultSet</code> - each row describes a
     *         user function parameter, column or return type
     * 
     * @exception SQLException
     *                if a database access error occurs
     * @see #getSearchStringEscape
     * @since 1.6
     */
    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        if (!this.hasParametersView) {
            return super.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
        }

        if ((functionNamePattern == null) || (functionNamePattern.length() == 0)) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                functionNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Procedure name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        String db = null;

        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
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
        sqlBuf.append(getJDBC4FunctionConstant(JDBC4FunctionConstant.FUNCTION_COLUMN_IN));
        sqlBuf.append(" WHEN PARAMETER_MODE = 'OUT' THEN ");
        sqlBuf.append(getJDBC4FunctionConstant(JDBC4FunctionConstant.FUNCTION_COLUMN_OUT));
        sqlBuf.append(" WHEN PARAMETER_MODE = 'INOUT' THEN ");
        sqlBuf.append(getJDBC4FunctionConstant(JDBC4FunctionConstant.FUNCTION_COLUMN_INOUT));
        sqlBuf.append(" WHEN ORDINAL_POSITION = 0 THEN ");
        sqlBuf.append(getJDBC4FunctionConstant(JDBC4FunctionConstant.FUNCTION_COLUMN_RETURN));
        sqlBuf.append(" ELSE ");
        sqlBuf.append(getJDBC4FunctionConstant(JDBC4FunctionConstant.FUNCTION_COLUMN_UNKNOWN));
        sqlBuf.append(" END AS `COLUMN_TYPE`, ");

        //DATA_TYPE
        MysqlDefs.appendJdbcTypeMappingQuery(sqlBuf, "DATA_TYPE");

        sqlBuf.append(" AS `DATA_TYPE`, ");

        // TYPE_NAME
        if (this.conn.getCapitalizeTypeNames()) {
            sqlBuf.append("UPPER(CASE WHEN LOCATE('unsigned', DATA_TYPE) != 0 AND LOCATE('unsigned', DATA_TYPE) = 0 THEN CONCAT(DATA_TYPE, ' unsigned') "
                    + "ELSE DATA_TYPE END) AS `TYPE_NAME`,");
        } else {
            sqlBuf.append("CASE WHEN LOCATE('unsigned', DATA_TYPE) != 0 AND LOCATE('unsigned', DATA_TYPE) = 0 THEN CONCAT(DATA_TYPE, ' unsigned') "
                    + "ELSE DATA_TYPE END AS `TYPE_NAME`,");
        }

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
        sqlBuf.append(getJDBC4FunctionConstant(JDBC4FunctionConstant.FUNCTION_NULLABLE) + " AS `NULLABLE`,  NULL AS `REMARKS`, "
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
            ((com.mysql.jdbc.ResultSetInternalMethods) rs).redefineFieldsForDBMD(createFunctionColumnsFields());

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Getter to JDBC4 DatabaseMetaData.function* constants.
     * This method must be overridden by JDBC4 subclasses. this implementation should never be called.
     * 
     * @param constant
     *            the constant id from DatabaseMetaData fields to return.
     * 
     * @return 0
     */
    protected int getJDBC4FunctionConstant(JDBC4FunctionConstant constant) {
        return 0;
    }

    /**
     * Retrieves a description of the system and user functions available
     * in the given catalog.
     * <P>
     * Only system and user function descriptions matching the schema and function name criteria are returned. They are ordered by <code>FUNCTION_CAT</code>,
     * <code>FUNCTION_SCHEM</code>, <code>FUNCTION_NAME</code> and <code>SPECIFIC_ NAME</code>.
     * 
     * <P>
     * Each function description has the the following columns:
     * <OL>
     * <LI><B>FUNCTION_CAT</B> String => function catalog (may be <code>null</code>)
     * <LI><B>FUNCTION_SCHEM</B> String => function schema (may be <code>null</code>)
     * <LI><B>FUNCTION_NAME</B> String => function name. This is the name used to invoke the function
     * <LI><B>REMARKS</B> String => explanatory comment on the function
     * <LI><B>FUNCTION_TYPE</B> short => kind of function:
     * <UL>
     * <LI>functionResultUnknown - Cannot determine if a return value or table will be returned
     * <LI>functionNoTable- Does not return a table
     * <LI>functionReturnsTable - Returns a table
     * </UL>
     * <LI><B>SPECIFIC_NAME</B> String => the name which uniquely identifies this function within its schema. This is a user specified, or DBMS generated, name
     * that may be different then the <code>FUNCTION_NAME</code> for example with overload functions
     * </OL>
     * <p>
     * A user may not have permission to execute any of the functions that are returned by <code>getFunctions</code>
     * 
     * @param catalog
     *            a catalog name; must match the catalog name as it
     *            is stored in the database; "" retrieves those without a catalog; <code>null</code> means that the catalog name should not be used to narrow
     *            the search
     * @param schemaPattern
     *            a schema name pattern; must match the schema name
     *            as it is stored in the database; "" retrieves those without a schema; <code>null</code> means that the schema name should not be used to
     *            narrow
     *            the search
     * @param functionNamePattern
     *            a function name pattern; must match the
     *            function name as it is stored in the database
     * @return <code>ResultSet</code> - each row is a function description
     * @exception SQLException
     *                if a database access error occurs
     * @see #getSearchStringEscape
     * @since 1.6
     */
    @Override
    public java.sql.ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {

        if ((functionNamePattern == null) || (functionNamePattern.length() == 0)) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                functionNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Function name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        String db = null;

        if (catalog == null) {
            if (this.conn.getNullCatalogMeansCurrent()) {
                db = this.database;
            }
        } else {
            db = catalog;
        }

        String sql = "SELECT ROUTINE_SCHEMA AS FUNCTION_CAT, NULL AS FUNCTION_SCHEM, ROUTINE_NAME AS FUNCTION_NAME, ROUTINE_COMMENT AS REMARKS, "
                + getJDBC4FunctionNoTableConstant() + " AS FUNCTION_TYPE, ROUTINE_NAME AS SPECIFIC_NAME FROM INFORMATION_SCHEMA.ROUTINES "
                + "WHERE ROUTINE_TYPE LIKE 'FUNCTION' AND ROUTINE_SCHEMA LIKE ? AND "
                + "ROUTINE_NAME LIKE ? ORDER BY FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME, SPECIFIC_NAME";

        java.sql.PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(sql);

            pStmt.setString(1, db != null ? db : "%");
            pStmt.setString(2, functionNamePattern);

            ResultSet rs = executeMetadataQuery(pStmt);
            ((com.mysql.jdbc.ResultSetInternalMethods) rs)
                    .redefineFieldsForDBMD(new Field[] { new Field("", "FUNCTION_CAT", Types.CHAR, 255), new Field("", "FUNCTION_SCHEM", Types.CHAR, 255),
                            new Field("", "FUNCTION_NAME", Types.CHAR, 255), new Field("", "REMARKS", Types.CHAR, 255),
                            new Field("", "FUNCTION_TYPE", Types.SMALLINT, 6), new Field("", "SPECIFIC_NAME", Types.CHAR, 255) });

            return rs;
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Getter to JDBC4 DatabaseMetaData.functionNoTable constant.
     * This method must be overridden by JDBC4 subclasses. this implementation should never be called.
     * 
     * @return 0
     */
    @Override
    protected int getJDBC4FunctionNoTableConstant() {
        return 0;
    }
}
