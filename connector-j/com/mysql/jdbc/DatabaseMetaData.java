/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
package com.mysql.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;


/**
 * JDBC Interface to Mysql functions
 *
 * <p>
 * This class provides information about the database as a whole.
 * </p>
 *
 * <p>
 * Many of the methods here return lists of information in ResultSets. You can
 * use the normal ResultSet methods such as getString and getInt to retrieve
 * the data from these ResultSets.  If a given form of metadata is not
 * available, these methods show throw a java.sql.SQLException.
 * </p>
 *
 * <p>
 * Some of these methods take arguments that are String patterns.  These
 * methods all have names such as fooPattern.  Within a pattern String "%"
 * means match any substring of 0 or more characters and "_" means match any
 * one character.
 * </p>
 *
 * @version $Id: DatabaseMetaData.java,v 1.20 2002/12/06 21:27:07 mmatthew Exp
 *          $
 * @author Mark Matthews
 */
public class DatabaseMetaData implements java.sql.DatabaseMetaData {
    private static final byte[] TABLE_AS_BYTES = "TABLE".getBytes();

    /**
     * The table type for generic tables that support foreign
     * keys.
     */
    private static final String SUPPORTS_FK = "SUPPORTS_FK";

    //
    // Column indexes used by all DBMD foreign key
    // ResultSets
    //
    private static final int PKTABLE_CAT = 0;
    private static final int PKTABLE_SCHEM = 1;
    private static final int PKTABLE_NAME = 2;
    private static final int PKCOLUMN_NAME = 3;
    private static final int FKTABLE_CAT = 4;
    private static final int FKTABLE_SCHEM = 5;
    private static final int FKTABLE_NAME = 6;
    private static final int FKCOLUMN_NAME = 7;
    private static final int KEY_SEQ = 8;
    private static final int UPDATE_RULE = 9;
    private static final int DELETE_RULE = 10;
    private static final int FK_NAME = 11;
    private static final int PK_NAME = 12;
    private static final int DEFERRABILITY = 13;

    /**
     * The connection to the database
     */
    private Connection conn;

    /**
     * The 'current' database name being used
     */
	private String database = null;

    /**
     * What character to use when quoting identifiers
     */
	private String quotedId = null;

    /**
     * Creates a new DatabaseMetaData object.
     *
     * @param conn DOCUMENT ME!
     * @param database DOCUMENT ME!
     */
    public DatabaseMetaData(Connection conn, String database) {
        this.conn = conn;
        this.database = database;

        try {
            this.quotedId = this.conn.supportsQuotedIdentifiers()
                ? getIdentifierQuoteString() : "";
        } catch (SQLException sqlEx) {
            // Forced by API, never thrown from getIdentifierQuoteString() in this
            // implementation.
        }
    }

    /**
     * @see DatabaseMetaData#getAttributes(String, String, String, String)
     */
    public java.sql.ResultSet getAttributes(String arg0, String arg1,
        String arg2, String arg3) throws SQLException {
        Field[] fields = new Field[21];
        fields[0] = new Field("", "TYPE_CAT", Types.CHAR, 32);
        fields[1] = new Field("", "TYPE_SCHEM", Types.CHAR, 32);
        fields[2] = new Field("", "TYPE_NAME", Types.CHAR, 32);
        fields[3] = new Field("", "ATTR_NAME", Types.CHAR, 32);
        fields[4] = new Field("", "DATA_TYPE", Types.SMALLINT, 32);
        fields[5] = new Field("", "ATTR_TYPE_NAME", Types.CHAR, 32);
        fields[6] = new Field("", "ATTR_SIZE", Types.INTEGER, 32);
        fields[7] = new Field("", "DECIMAL_DIGITS", Types.INTEGER, 32);
        fields[8] = new Field("", "NUM_PREC_RADIX", Types.INTEGER, 32);
        fields[9] = new Field("", "NULLABLE ", Types.INTEGER, 32);
        fields[10] = new Field("", "REMARKS", Types.CHAR, 32);
        fields[11] = new Field("", "ATTR_DEF", Types.CHAR, 32);
        fields[12] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 32);
        fields[13] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 32);
        fields[14] = new Field("", "CHAR_OCTET_LENGTH", Types.INTEGER, 32);
        fields[15] = new Field("", "ORDINAL_POSITION", Types.INTEGER, 32);
        fields[16] = new Field("", "IS_NULLABLE", Types.CHAR, 32);
        fields[17] = new Field("", "SCOPE_CATALOG", Types.CHAR, 32);
        fields[18] = new Field("", "SCOPE_SCHEMA", Types.CHAR, 32);
        fields[19] = new Field("", "SCOPE_TABLE", Types.CHAR, 32);
        fields[20] = new Field("", "SOURCE_DATA_TYPE", Types.SMALLINT, 32);

        return buildResultSet(fields, new ArrayList());
    }

    /**
     * Get a description of a table's optimal set of columns that uniquely
     * identifies a row. They are ordered by SCOPE.
     *
     * <P>
     * Each column description has the following columns:
     *
     * <OL>
     * <li>
     * <B>SCOPE</B> short => actual scope of result
     *
     * <UL>
     * <li>
     * bestRowTemporary - very temporary, while using row
     * </li>
     * <li>
     * bestRowTransaction - valid for remainder of current transaction
     * </li>
     * <li>
     * bestRowSession - valid for remainder of current session
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>COLUMN_NAME</B> String => column name
     * </li>
     * <li>
     * <B>DATA_TYPE</B> short => SQL data type from java.sql.Types
     * </li>
     * <li>
     * <B>TYPE_NAME</B> String => Data source dependent type name
     * </li>
     * <li>
     * <B>COLUMN_SIZE</B> int => precision
     * </li>
     * <li>
     * <B>BUFFER_LENGTH</B> int => not used
     * </li>
     * <li>
     * <B>DECIMAL_DIGITS</B> short  => scale
     * </li>
     * <li>
     * <B>PSEUDO_COLUMN</B> short => is this a pseudo column like an Oracle
     * ROWID
     *
     * <UL>
     * <li>
     * bestRowUnknown - may or may not be pseudo column
     * </li>
     * <li>
     * bestRowNotPseudo - is NOT a pseudo column
     * </li>
     * <li>
     * bestRowPseudo - is a pseudo column
     * </li>
     * </ul>
     * </li>
     * </ol>
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schema a schema name; "" retrieves those without a schema
     * @param table a table name
     * @param scope the scope of interest; use same values as SCOPE
     * @param nullable include columns that are nullable?
     * @return ResultSet each row is a column description
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public java.sql.ResultSet getBestRowIdentifier(String catalog,
        String schema, String table, int scope, boolean nullable)
        throws java.sql.SQLException {
        Field[] fields = new Field[8];
        fields[0] = new Field("", "SCOPE", Types.SMALLINT, 5);
        fields[1] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
        fields[2] = new Field("", "DATA_TYPE", Types.SMALLINT, 32);
        fields[3] = new Field("", "TYPE_NAME", Types.CHAR, 32);
        fields[4] = new Field("", "COLUMN_SIZE", Types.INTEGER, 10);
        fields[5] = new Field("", "BUFFER_LENGTH", Types.INTEGER, 10);
        fields[6] = new Field("", "DECIMAL_DIGITS", Types.INTEGER, 10);
        fields[7] = new Field("", "PSEUDO_COLUMN", Types.SMALLINT, 5);

        String databasePart = "";

        if (catalog != null) {
            if (!catalog.equals("")) {
                databasePart = " FROM " + this.quotedId + catalog
                    + this.quotedId;
            }
        } else {
            databasePart = " FROM " + this.quotedId + this.database
                + this.quotedId;
        }

        if (table == null) {
            throw new java.sql.SQLException("Table not specified.", "S1009");
        }

        ResultSet results = null;
        Statement stmt = null;

        try {
            stmt = this.conn.createStatement();
            results = stmt.executeQuery("show columns from " + table
                    + databasePart);

            ArrayList tuples = new ArrayList();

            while (results.next()) {
                String keyType = results.getString("Key");

                if (keyType != null) {
                    if (StringUtils.startsWithIgnoreCase(keyType, "PRI")) {
                        byte[][] rowVal = new byte[8][];
                        rowVal[0] = Integer.toString(java.sql.DatabaseMetaData.bestRowSession)
                                           .getBytes();
                        rowVal[1] = results.getBytes("Field");

                        String type = results.getString("Type");
                        int size = MysqlIO.getMaxBuf();
                        int decimals = 0;

                        /*
                         * Parse the Type column from MySQL
                         */
                        if (type.indexOf("enum") != -1) {
                            String temp = type.substring(type.indexOf("("),
                                    type.indexOf(")"));
                            java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(temp,
                                    ",");
                            int maxLength = 0;

                            while (tokenizer.hasMoreTokens()) {
                                maxLength = Math.max(maxLength,
                                        (tokenizer.nextToken().length() - 2));
                            }

                            size = maxLength;
                            decimals = 0;
                            type = "enum";
                        } else if (type.indexOf("(") != -1) {
                            if (type.indexOf(",") != -1) {
                                size = Integer.parseInt(type.substring(type
                                            .indexOf("(") + 1, type.indexOf(",")));
                                decimals = Integer.parseInt(type.substring(type
                                            .indexOf(",") + 1, type.indexOf(")")));
                            } else {
                                size = Integer.parseInt(type.substring(type
                                            .indexOf("(") + 1, type.indexOf(")")));
                            }

                            type = type.substring(type.indexOf("("));
                        }

                        rowVal[2] = new byte[0]; // FIXME!
                        rowVal[3] = s2b(type);
                        rowVal[4] = Integer.toString(size + decimals).getBytes();
                        rowVal[5] = Integer.toString(size + decimals).getBytes();
                        rowVal[6] = Integer.toString(decimals).getBytes();
                        rowVal[7] = Integer.toString(java.sql.DatabaseMetaData.bestRowNotPseudo)
                                           .getBytes();
                        tuples.add(rowVal);
                    }
                }
            }

            return buildResultSet(fields, tuples);
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (Exception ex) {
                    ;
                }

                results = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ex) {
                    ;
                }

                stmt = null;
            }
        }
    }

    /**
     * Does a catalog appear at the start of a qualified table name?
     * (Otherwise it appears at the end)
     *
     * @return true if it appears at the start
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean isCatalogAtStart() throws java.sql.SQLException {
        return true;
    }

    /**
     * What's the separator between catalog and table name?
     *
     * @return the separator string
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getCatalogSeparator() throws java.sql.SQLException {
        return ".";
    }

    /**
     * What's the database vendor's preferred term for "catalog"?
     *
     * @return the vendor term
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getCatalogTerm() throws java.sql.SQLException {
        return "database";
    }

    /**
     * Get the catalog names available in this database.  The results are
     * ordered by catalog name.
     *
     * <P>
     * The catalog column is:
     *
     * <OL>
     * <li>
     * <B>TABLE_CAT</B> String => catalog name
     * </li>
     * </ol>
     * </p>
     *
     * @return ResultSet each row has a single String column that is a catalog
     *         name
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public java.sql.ResultSet getCatalogs() throws java.sql.SQLException {
        java.sql.ResultSet results = null;
        java.sql.Statement stmt = null;

        try {
            stmt = this.conn.createStatement();
            results = stmt.executeQuery("SHOW DATABASES");

            java.sql.ResultSetMetaData resultsMD = results.getMetaData();
            Field[] fields = new Field[1];
            fields[0] = new Field("", "TABLE_CAT", Types.VARCHAR,
                    resultsMD.getColumnDisplaySize(1));

            ArrayList tuples = new ArrayList();

            while (results.next()) {
                byte[][] rowVal = new byte[1][];
                rowVal[0] = results.getBytes(1);
                tuples.add(rowVal);
            }

            return buildResultSet(fields, tuples);
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }

                results = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }

                stmt = null;
            }
        }
    }

    /**
     * Get a description of the access rights for a table's columns.
     *
     * <P>
     * Only privileges matching the column name criteria are returned.  They
     * are ordered by COLUMN_NAME and PRIVILEGE.
     * </p>
     *
     * <P>
     * Each privilige description has the following columns:
     *
     * <OL>
     * <li>
     * <B>TABLE_CAT</B> String => table catalog (may be null)
     * </li>
     * <li>
     * <B>TABLE_SCHEM</B> String => table schema (may be null)
     * </li>
     * <li>
     * <B>TABLE_NAME</B> String => table name
     * </li>
     * <li>
     * <B>COLUMN_NAME</B> String => column name
     * </li>
     * <li>
     * <B>GRANTOR</B> => grantor of access (may be null)
     * </li>
     * <li>
     * <B>GRANTEE</B> String => grantee of access
     * </li>
     * <li>
     * <B>PRIVILEGE</B> String => name of access (SELECT, INSERT, UPDATE,
     * REFRENCES, ...)
     * </li>
     * <li>
     * <B>IS_GRANTABLE</B> String => "YES" if grantee is permitted to grant to
     * others; "NO" if not; null if unknown
     * </li>
     * </ol>
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schema a schema name; "" retrieves those without a schema
     * @param table a table name
     * @param columnNamePattern a column name pattern
     * @return ResultSet each row is a column privilege description
     * @see #getSearchStringEscape
     * @throws java.sql.SQLException if a database access error occurs
     */
    public java.sql.ResultSet getColumnPrivileges(String catalog,
        String schema, String table, String columnNamePattern)
        throws java.sql.SQLException {
        Field[] fields = new Field[8];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 64);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 1);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 64);
        fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 64);
        fields[4] = new Field("", "GRANTOR", Types.CHAR, 77);
        fields[5] = new Field("", "GRANTEE", Types.CHAR, 77);
        fields[6] = new Field("", "PRIVILEGE", Types.CHAR, 64);
        fields[7] = new Field("", "IS_GRANTABLE", Types.CHAR, 3);

        StringBuffer grantQuery = new StringBuffer(
                "SELECT c.host, c.db, t.grantor, c.user, "
                + "c.table_name, c.column_name, c.column_priv "
                + "from mysql.columns_priv c, mysql.tables_priv t "
                + "where c.host = t.host and c.db = t.db and "
                + "c.table_name = t.table_name ");

        if ((catalog != null) && (catalog.length() != 0)) {
            grantQuery.append(" AND c.db='");
            grantQuery.append(catalog);
            grantQuery.append("' ");
            ;
        }

        grantQuery.append(" AND c.table_name ='");
        grantQuery.append(table);
        grantQuery.append("' AND c.column_name like '");
        grantQuery.append(columnNamePattern);
        grantQuery.append("'");

        Statement stmt = null;
        ResultSet results = null;
        ArrayList grantRows = new ArrayList();

        try {
            stmt = this.conn.createStatement();
            results = stmt.executeQuery(grantQuery.toString());

            while (results.next()) {
                String host = results.getString(1);
                String database = results.getString(2);
                String grantor = results.getString(3);
                String user = results.getString(4);

                if ((user == null) || (user.length() == 0)) {
                    user = "%";
                }

                StringBuffer fullUser = new StringBuffer(user);

                if ((host != null) && this.conn.useHostsInPrivileges()) {
                    fullUser.append("@");
                    fullUser.append(host);
                }

                String columnName = results.getString(6);
                String allPrivileges = results.getString(7);

                if (allPrivileges != null) {
                    allPrivileges = allPrivileges.toUpperCase();

                    StringTokenizer st = new StringTokenizer(allPrivileges, ",");

                    while (st.hasMoreTokens()) {
                        String privilege = st.nextToken().trim();
                        byte[][] tuple = new byte[8][];
                        tuple[0] = s2b(database);
                        tuple[1] = null;
                        tuple[2] = s2b(table);
                        tuple[3] = s2b(columnName);

                        if (grantor != null) {
                            tuple[4] = s2b(grantor);
                        } else {
                            tuple[4] = null;
                        }

                        tuple[5] = s2b(fullUser.toString());
                        tuple[6] = s2b(privilege);
                        tuple[7] = null;
                        grantRows.add(tuple);
                    }
                }
            }
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (Exception ex) {
                    ;
                }

                results = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ex) {
                    ;
                }

                stmt = null;
            }
        }

        return buildResultSet(fields, grantRows);
    }

    /**
     * Get a description of table columns available in a catalog.
     *
     * <P>
     * Only column descriptions matching the catalog, schema, table and column
     * name criteria are returned.  They are ordered by TABLE_SCHEM,
     * TABLE_NAME and ORDINAL_POSITION.
     * </p>
     *
     * <P>
     * Each column description has the following columns:
     *
     * <OL>
     * <li>
     * <B>TABLE_CAT</B> String => table catalog (may be null)
     * </li>
     * <li>
     * <B>TABLE_SCHEM</B> String => table schema (may be null)
     * </li>
     * <li>
     * <B>TABLE_NAME</B> String => table name
     * </li>
     * <li>
     * <B>COLUMN_NAME</B> String => column name
     * </li>
     * <li>
     * <B>DATA_TYPE</B> short => SQL type from java.sql.Types
     * </li>
     * <li>
     * <B>TYPE_NAME</B> String => Data source dependent type name
     * </li>
     * <li>
     * <B>COLUMN_SIZE</B> int => column size.  For char or date types this is
     * the maximum number of characters, for numeric or decimal types this is
     * precision.
     * </li>
     * <li>
     * <B>BUFFER_LENGTH</B> is not used.
     * </li>
     * <li>
     * <B>DECIMAL_DIGITS</B> int => the number of fractional digits
     * </li>
     * <li>
     * <B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or 2)
     * </li>
     * <li>
     * <B>NULLABLE</B> int => is NULL allowed?
     *
     * <UL>
     * <li>
     * columnNoNulls - might not allow NULL values
     * </li>
     * <li>
     * columnNullable - definitely allows NULL values
     * </li>
     * <li>
     * columnNullableUnknown - nullability unknown
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>REMARKS</B> String => comment describing column (may be null)
     * </li>
     * <li>
     * <B>COLUMN_DEF</B> String => default value (may be null)
     * </li>
     * <li>
     * <B>SQL_DATA_TYPE</B> int => unused
     * </li>
     * <li>
     * <B>SQL_DATETIME_SUB</B> int => unused
     * </li>
     * <li>
     * <B>CHAR_OCTET_LENGTH</B> int => for char types the maximum number of
     * bytes in the column
     * </li>
     * <li>
     * <B>ORDINAL_POSITION</B> int => index of column in table (starting at 1)
     * </li>
     * <li>
     * <B>IS_NULLABLE</B> String => "NO" means column definitely does not
     * allow NULL values; "YES" means the column might allow NULL values.  An
     * empty string means nobody knows.
     * </li>
     * </ol>
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schemaPattern a schema name pattern; "" retrieves those without
     *        a schema
     * @param tableName a table name pattern
     * @param columnNamePattern a column name pattern
     * @return ResultSet each row is a column description
     * @see #getSearchStringEscape
     * @throws java.sql.SQLException if a database access error occurs
     */
    public java.sql.ResultSet getColumns(String catalog, String schemaPattern,
        String tableName, String columnNamePattern)
        throws java.sql.SQLException {
        String databasePart = "";

        if (columnNamePattern == null) {
            columnNamePattern = "%";
        }

        if (catalog != null) {
            if (!catalog.equals("")) {
                databasePart = " FROM " + this.quotedId + catalog
                    + this.quotedId;
            }
        } else {
            databasePart = " FROM " + this.quotedId + this.database
                + this.quotedId;
        }

        ArrayList tableNameList = new ArrayList();
        int tablenameLength = 0;

        if (tableName == null) {
            // Select from all tables
            java.sql.ResultSet tables = null;

            try {
                tables = getTables(catalog, schemaPattern, "%", new String[0]);

                while (tables.next()) {
                    String tableNameFromList = tables.getString("TABLE_NAME");
                    tableNameList.add(tableNameFromList);

                    if (tableNameFromList.length() > tablenameLength) {
                        tablenameLength = tableNameFromList.length();
                    }
                }
            } finally {
                if (tables != null) {
                    try {
                        tables.close();
                    } catch (Exception sqlEx) {
                        // ignore
                    }

                    tables = null;
                }
            }
        } else {
            java.sql.ResultSet tables = null;

            try {
                tables = getTables(catalog, schemaPattern, tableName,
                        new String[0]);

                while (tables.next()) {
                    String tableNameFromList = tables.getString("TABLE_NAME");
                    tableNameList.add(tableNameFromList);

                    if (tableNameFromList.length() > tablenameLength) {
                        tablenameLength = tableNameFromList.length();
                    }
                }
            } finally {
                if (tables != null) {
                    try {
                        tables.close();
                    } catch (SQLException sqlEx) {
                        // ignore
                    }

                    tables = null;
                }
            }
        }

        int catalogLength = 0;

        if (catalog != null) {
            catalogLength = catalog.length();
        } else {
            catalog = "";
            catalogLength = 0;
        }

        java.util.Iterator tableNames = tableNameList.iterator();
        Field[] fields = new Field[18];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, catalogLength);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, tablenameLength);
        fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
        fields[4] = new Field("", "DATA_TYPE", Types.SMALLINT, 5);
        fields[5] = new Field("", "TYPE_NAME", Types.CHAR, 16);
        fields[6] = new Field("", "COLUMN_SIZE", Types.INTEGER,
                Integer.toString(Integer.MAX_VALUE).length());
        fields[7] = new Field("", "BUFFER_LENGTH", Types.INTEGER, 10);
        fields[8] = new Field("", "DECIMAL_DIGITS", Types.INTEGER, 10);
        fields[9] = new Field("", "NUM_PREC_RADIX", Types.INTEGER, 10);
        fields[10] = new Field("", "NULLABLE", Types.INTEGER, 10);
        fields[11] = new Field("", "REMARKS", Types.CHAR, 0);
        fields[12] = new Field("", "COLUMN_DEF", Types.CHAR, 0);
        fields[13] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 10);
        fields[14] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 10);
        fields[15] = new Field("", "CHAR_OCTET_LENGTH", Types.INTEGER,
                Integer.toString(Integer.MAX_VALUE).length());
        fields[16] = new Field("", "ORDINAL_POSITION", Types.INTEGER, 10);
        fields[17] = new Field("", "IS_NULLABLE", Types.CHAR, 3);

        ArrayList tuples = new ArrayList();

        while (tableNames.hasNext()) {
            String tableNamePattern = (String) tableNames.next();
            Statement stmt = null;
            ResultSet results = null;

            try {
                stmt = this.conn.createStatement();
                results = stmt.executeQuery("show columns from "
                        + tableNamePattern + databasePart + " like '"
                        + columnNamePattern + "'");

                int ordPos = 1;

                while (results.next()) {
                    byte[][] rowVal = new byte[18][];
                    rowVal[0] = s2b(catalog); // TABLE_CAT
                    rowVal[1] = null;

                    // TABLE_SCHEM (No schemas in MySQL)
                    rowVal[2] = s2b(tableNamePattern); // TABLE_NAME
                    rowVal[3] = results.getBytes("Field");

                    String typeInfo = results.getString("Type");

                    if (Driver.DEBUG) {
                        System.out.println("Type: " + typeInfo);
                    }

                    String mysqlType = "";

                    if (typeInfo.indexOf("(") != -1) {
                        mysqlType = typeInfo.substring(0, typeInfo.indexOf("("));
                    } else {
                        mysqlType = typeInfo;
                    }

                    if (this.conn.capitalizeDBMDTypes()) {
                        mysqlType = mysqlType.toUpperCase();
                    }

                    /*
                     * Convert to XOPEN (thanks JK)
                     */
                    rowVal[4] = Integer.toString(MysqlDefs.mysqlToJavaType(
                                mysqlType)).getBytes();

                    // DATA_TYPE (jdbc)
                    rowVal[5] = s2b(mysqlType); // TYPE_NAME (native)

                    // Figure Out the Size
                    if (typeInfo != null) {
                        if ((typeInfo.indexOf("enum") != -1)
                                || (typeInfo.indexOf("set") != -1)) {
                            String temp = typeInfo.substring(typeInfo.indexOf(
                                        "("), typeInfo.lastIndexOf(")"));
                            java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(temp,
                                    ",");
                            int maxLength = 0;

                            while (tokenizer.hasMoreTokens()) {
                                maxLength = Math.max(maxLength,
                                        (tokenizer.nextToken().length() - 2));
                            }

                            rowVal[6] = Integer.toString(maxLength).getBytes();
                            rowVal[8] = new byte[] { (byte) '0' };
                        } else if (typeInfo.indexOf(",") != -1) {
                            // Numeric with decimals
                            String size = typeInfo.substring((typeInfo.indexOf(
                                        "(") + 1), (typeInfo.indexOf(",")));
                            String decimals = typeInfo.substring((typeInfo
                                    .indexOf(",") + 1), (typeInfo.indexOf(")")));
                            rowVal[6] = s2b(size);
                            rowVal[8] = s2b(decimals);
                        } else {
                            String size = "0";

                            /* If the size is specified with the DDL, use that */
                            if (typeInfo.indexOf("(") != -1) {
                                size = typeInfo.substring((typeInfo.indexOf("(")
                                        + 1), (typeInfo.indexOf(")")));
                            } else if (typeInfo.equalsIgnoreCase("tinyint")) {
                                size = "1";
                            } else if (typeInfo.equalsIgnoreCase("smallint")) {
                                size = "6";
                            } else if (typeInfo.equalsIgnoreCase("mediumint")) {
                                size = "6";
                            } else if (typeInfo.equalsIgnoreCase("int")) {
                                size = "11";
                            } else if (typeInfo.equalsIgnoreCase("integer")) {
                                size = "11";
                            } else if (typeInfo.equalsIgnoreCase("bigint")) {
                                size = "25";
                            } else if (typeInfo.equalsIgnoreCase("int24")) {
                                size = "25";
                            } else if (typeInfo.equalsIgnoreCase("real")) {
                                size = "12";
                            } else if (typeInfo.equalsIgnoreCase("float")) {
                                size = "12";
                            } else if (typeInfo.equalsIgnoreCase("decimal")) {
                                size = "12";
                            } else if (typeInfo.equalsIgnoreCase("numeric")) {
                                size = "12";
                            } else if (typeInfo.equalsIgnoreCase("double")) {
                                size = "22";
                            } else if (typeInfo.equalsIgnoreCase("char")) {
                                size = "1";
                            } else if (typeInfo.equalsIgnoreCase("varchar")) {
                                size = "255";
                            } else if (typeInfo.equalsIgnoreCase("date")) {
                                size = "10";
                            } else if (typeInfo.equalsIgnoreCase("time")) {
                                size = "8";
                            } else if (typeInfo.equalsIgnoreCase("timestamp")) {
                                size = "19";
                            } else if (typeInfo.equalsIgnoreCase("datetime")) {
                                size = "19";
                            } else if (typeInfo.equalsIgnoreCase("tinyblob")) {
                                size = "255";
                            } else if (typeInfo.equalsIgnoreCase("blob")) {
                                size = Integer.toString(Math.min(65535,
                                            MysqlIO.getMaxBuf()));
                            } else if (typeInfo.equalsIgnoreCase("mediumblob")) {
                                size = Integer.toString(Math.min(16277215,
                                            MysqlIO.getMaxBuf()));
                            } else if (typeInfo.equalsIgnoreCase("longblob")) {
                                size = Integer.toString(Integer.MAX_VALUE);
                            } else if (typeInfo.equalsIgnoreCase("tinytext")) {
                                size = "255";
                            } else if (typeInfo.equalsIgnoreCase("text")) {
                                size = "65535";
                            } else if (typeInfo.equalsIgnoreCase("mediumtext")) {
                                size = Integer.toString(Math.min(16277215,
                                            MysqlIO.getMaxBuf()));
                            } else if (typeInfo.equalsIgnoreCase("enum")) {
                                size = "255";
                            } else if (typeInfo.equalsIgnoreCase("set")) {
                                size = "255";
                            }

                            rowVal[6] = size.getBytes();
                            rowVal[8] = new byte[] { (byte) '0' };
                        }
                    } else {
                        rowVal[8] = new byte[] { (byte) '0' };
                        rowVal[6] = new byte[] { (byte) '0' };
                    }

                    rowVal[7] = Integer.toString(MysqlIO.getMaxBuf()).getBytes();

                    // BUFFER_LENGTH
                    rowVal[9] = new byte[] { (byte) '1', (byte) '0' };

                    // NUM_PREC_RADIX (is this right for char?)
                    String nullable = results.getString("Null");

                    // Nullable?
                    if (nullable != null) {
                        if (nullable.equals("YES")) {
                            rowVal[10] = Integer.toString(java.sql.DatabaseMetaData.columnNullable)
                                                .getBytes();
                            rowVal[17] = new String("YES").getBytes();

                            // IS_NULLABLE
                        } else {
                            rowVal[10] = Integer.toString(java.sql.DatabaseMetaData.columnNoNulls)
                                                .getBytes();
                            rowVal[17] = "NO".getBytes();
                        }
                    } else {
                        rowVal[10] = Integer.toString(java.sql.DatabaseMetaData.columnNoNulls)
                                            .getBytes();
                        rowVal[17] = "NO".getBytes();
                    }

                    //
                    // Doesn't always have this field, depending on version
                    //
                    //
                    // REMARK column
                    //
                    try {
                        rowVal[11] = results.getBytes("Extra");
                    } catch (Exception E) {
                        rowVal[11] = new byte[0];
                    }

                    // COLUMN_DEF
                    byte[] defaultVal = results.getBytes("Default");

                    if (defaultVal != null) {
                        rowVal[12] = defaultVal;
                    } else {
                        rowVal[12] = new byte[0];
                    }

                    rowVal[13] = new byte[] { (byte) '0' }; // SQL_DATA_TYPE
                    rowVal[14] = new byte[] { (byte) '0' }; // SQL_DATE_TIME_SUB
                    rowVal[15] = rowVal[6]; // CHAR_OCTET_LENGTH
                    rowVal[16] = Integer.toString(ordPos++).getBytes();

                    // ORDINAL_POSITION
                    tuples.add(rowVal);
                }
            } finally {
                if (results != null) {
                    try {
                        results.close();
                    } catch (Exception ex) {
                        ;
                    }

                    results = null;
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ex) {
                        ;
                    }

                    stmt = null;
                }
            }
        }

        java.sql.ResultSet results = buildResultSet(fields, tuples);

        return results;
    }

    /**
     * JDBC 2.0 Return the connection that produced this metadata object.
     *
     * @return the connection that produced this metadata object.
     * @throws SQLException if a database error occurs
     */
    public java.sql.Connection getConnection() throws SQLException {
        return (java.sql.Connection) this.conn;
    }

    /**
     * Get a description of the foreign key columns in the foreign key table
     * that reference the primary key columns of the primary key table
     * (describe how one table imports another's key.) This should normally
     * return a single foreign key/primary key pair (most tables only import
     * a foreign key from a table once.)  They are ordered by FKTABLE_CAT,
     * FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ.
     *
     * <P>
     * Each foreign key column description has the following columns:
     *
     * <OL>
     * <li>
     * <B>PKTABLE_CAT</B> String => primary key table catalog (may be null)
     * </li>
     * <li>
     * <B>PKTABLE_SCHEM</B> String => primary key table schema (may be null)
     * </li>
     * <li>
     * <B>PKTABLE_NAME</B> String => primary key table name
     * </li>
     * <li>
     * <B>PKCOLUMN_NAME</B> String => primary key column name
     * </li>
     * <li>
     * <B>FKTABLE_CAT</B> String => foreign key table catalog (may be null)
     * being exported (may be null)
     * </li>
     * <li>
     * <B>FKTABLE_SCHEM</B> String => foreign key table schema (may be null)
     * being exported (may be null)
     * </li>
     * <li>
     * <B>FKTABLE_NAME</B> String => foreign key table name being exported
     * </li>
     * <li>
     * <B>FKCOLUMN_NAME</B> String => foreign key column name being exported
     * </li>
     * <li>
     * <B>KEY_SEQ</B> short => sequence number within foreign key
     * </li>
     * <li>
     * <B>UPDATE_RULE</B> short => What happens to foreign key when primary is
     * updated:
     *
     * <UL>
     * <li>
     * importedKeyCascade - change imported key to agree with primary key
     * update
     * </li>
     * <li>
     * importedKeyRestrict - do not allow update of primary key if it has been
     * imported
     * </li>
     * <li>
     * importedKeySetNull - change imported key to NULL if its primary key has
     * been updated
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>DELETE_RULE</B> short => What happens to the foreign key when
     * primary is deleted.
     *
     * <UL>
     * <li>
     * importedKeyCascade - delete rows that import a deleted key
     * </li>
     * <li>
     * importedKeyRestrict - do not allow delete of primary key if it has been
     * imported
     * </li>
     * <li>
     * importedKeySetNull - change imported key to NULL if its primary key has
     * been deleted
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>FK_NAME</B> String => foreign key identifier (may be null)
     * </li>
     * <li>
     * <B>PK_NAME</B> String => primary key identifier (may be null)
     * </li>
     * </ol>
     * </p>
     *
     * @param primaryCatalog a catalog name; "" retrieves those without a catalog
     * @param primarySchema a schema name pattern; "" retrieves those without a schema
     * @param primaryTable a table name
     * @param foreignCatalog a catalog name; "" retrieves those without a catalog
     * @param foreignSchema a schema name pattern; "" retrieves those without a schema
     * @param foreignTable a table name
     * @return ResultSet each row is a foreign key column description
     * @throws java.sql.SQLException if a database access error occurs
     */
    public java.sql.ResultSet getCrossReference(String primaryCatalog,
        String primarySchema, String primaryTable, String foreignCatalog,
        String foreignSchema, String foreignTable) throws java.sql.SQLException {
        if (Driver.TRACE) {
            Object[] args = {
                primaryCatalog, primarySchema, primaryTable, foreignCatalog,
                foreignSchema, foreignTable
            };
            Debug.methodCall(this, "getCrossReference", args);
        }

        if (primaryTable == null) {
            throw new java.sql.SQLException("Table not specified.", "S1009");
        }

        Field[] fields = new Field[14];
        fields[0] = new Field("", "PKTABLE_CAT", Types.CHAR, 255);
        fields[1] = new Field("", "PKTABLE_SCHEM", Types.CHAR, 0);
        fields[2] = new Field("", "PKTABLE_NAME", Types.CHAR, 255);
        fields[3] = new Field("", "PKCOLUMN_NAME", Types.CHAR, 32);
        fields[4] = new Field("", "FKTABLE_CAT", Types.CHAR, 255);
        fields[5] = new Field("", "FKTABLE_SCHEM", Types.CHAR, 0);
        fields[6] = new Field("", "FKTABLE_NAME", Types.CHAR, 255);
        fields[7] = new Field("", "FKCOLUMN_NAME", Types.CHAR, 32);
        fields[8] = new Field("", "KEY_SEQ", Types.SMALLINT, 2);
        fields[9] = new Field("", "UPDATE_RULE", Types.SMALLINT, 2);
        fields[10] = new Field("", "DELETE_RULE", Types.SMALLINT, 2);
        fields[11] = new Field("", "FK_NAME", Types.CHAR, 0);
        fields[12] = new Field("", "PK_NAME", Types.CHAR, 0);
        fields[13] = new Field("", "DEFERRABILITY", Types.INTEGER, 2);

        if (this.conn.getIO().versionMeetsMinimum(3, 23, 0)) {
            Statement stmt = null;
            ResultSet fkresults = null;

            try {
                /*
                 * Get foreign key information for table
                 */
                if (this.conn.getIO().versionMeetsMinimum(3, 23, 50)) {
                    // we can use 'SHOW CREATE TABLE'
                    String database = this.database;

                    if (foreignCatalog != null) {
                        if (!foreignCatalog.equals("")) {
                            database = foreignCatalog;
                        }
                    }

                    fkresults = extractForeignKeyFromCreateTable(this.conn,
                            this, database, null);
                } else {
                    String databasePart = "";

                    if (foreignCatalog != null) {
                        if (!foreignCatalog.equals("")) {
                            databasePart = " FROM " + foreignCatalog;
                        }
                    } else {
                        databasePart = " FROM " + this.database;
                    }

                    stmt = this.conn.createStatement();
                    fkresults = stmt.executeQuery("show table status "
                            + databasePart);
                }

                String foreignTableWithCase = getTableNameWithCase(foreignTable);
                String primaryTableWithCase = getTableNameWithCase(primaryTable);

                /*
                * Parse imported foreign key information
                */
                ArrayList tuples = new ArrayList();
                String dummy;

                while (fkresults.next()) {
                    String tableType = fkresults.getString("Type");

                    if ((tableType != null)
                            && (tableType.equalsIgnoreCase("innodb")
                            || tableType.equalsIgnoreCase(SUPPORTS_FK))) {
                        String comment = fkresults.getString("Comment").trim();

                        if (comment != null) {
                            StringTokenizer commentTokens = new StringTokenizer(comment,
                                    ";", false);

                            if (commentTokens.hasMoreTokens()) {
                                dummy = commentTokens.nextToken();

                                // Skip InnoDB comment
                            }

                            while (commentTokens.hasMoreTokens()) {
                                String keys = commentTokens.nextToken();

                                // simple-columned keys: (m) REFER airline/tt(a)
                                // multi-columned keys : (m n) REFER airline/vv(a b)
                                int firstLeftParenIndex = keys.indexOf('(');
                                int firstRightParenIndex = keys.indexOf(')');
                                String referencingColumns = keys.substring(firstLeftParenIndex
                                        + 1, firstRightParenIndex);
                                StringTokenizer referencingColumnsTokenizer = new StringTokenizer(referencingColumns,
                                        ", ");
                                int secondLeftParenIndex = keys.indexOf('(',
                                        firstRightParenIndex + 1);
                                int secondRightParenIndex = keys.indexOf(')',
                                        firstRightParenIndex + 1);
                                String referencedColumns = keys.substring(secondLeftParenIndex
                                        + 1, secondRightParenIndex);
                                StringTokenizer referencedColumnsTokenizer = new StringTokenizer(referencedColumns,
                                        ", ");
                                int slashIndex = keys.indexOf('/');
                                String referencedTable = keys.substring(slashIndex
                                        + 1, secondLeftParenIndex);
                                int keySeq = 0;

                                while (referencingColumnsTokenizer
                                        .hasMoreTokens()) {
                                    String referencingColumn = referencingColumnsTokenizer
                                        .nextToken();

                                    // one tuple for each table between parenthesis
                                    byte[][] tuple = new byte[14][];
                                    tuple[4] = ((foreignCatalog == null) ? null
                                                                         : s2b(foreignCatalog));
                                    tuple[5] = ((foreignSchema == null) ? null
                                                                        : s2b(foreignSchema));
                                    dummy = fkresults.getString("Name"); // FKTABLE_NAME

                                    if (dummy.compareTo(foreignTableWithCase) != 0) {
                                        continue;
                                    } else {
                                        tuple[6] = s2b(dummy);
                                    }

                                    tuple[7] = s2b(referencingColumn); // FKCOLUMN_NAME
                                    tuple[0] = ((primaryCatalog == null) ? null
                                                                         : s2b(primaryCatalog));
                                    tuple[1] = ((primarySchema == null) ? null
                                                                        : s2b(primarySchema));

                                    // Skip foreign key if it doesn't refer to the right table
                                    if (referencedTable.compareTo(
                                                primaryTableWithCase) != 0) {
                                        continue;
                                    }

                                    tuple[2] = s2b(referencedTable); // PKTABLE_NAME
                                    tuple[3] = s2b(referencedColumnsTokenizer
                                            .nextToken()); // PKCOLUMN_NAME
                                    tuple[8] = Integer.toString(keySeq)
                                                      .getBytes(); // KEY_SEQ
                                    tuple[9] = Integer.toString(java.sql.DatabaseMetaData.importedKeySetDefault)
                                                      .getBytes();
                                    tuple[10] = Integer.toString(getCascadeDeleteOption(
                                                keys)).getBytes();
                                    tuple[11] = null; // FK_NAME
                                    tuple[12] = null; // PK_NAME
                                    tuple[13] = Integer.toString(java.sql.DatabaseMetaData.importedKeyNotDeferrable)
                                                       .getBytes();
                                    tuples.add(tuple);
                                    keySeq++;
                                }
                            }
                        }
                    }
                }

                if (Driver.TRACE) {
                    StringBuffer rows = new StringBuffer();
                    rows.append("\n");

                    for (int i = 0; i < tuples.size(); i++) {
                        byte[][] b = (byte[][]) tuples.get(i);
                        rows.append("[Row] ");

                        boolean firstTime = true;

                        for (int j = 0; j < b.length; j++) {
                            if (!firstTime) {
                                rows.append(", ");
                            } else {
                                firstTime = false;
                            }

                            if (b[j] == null) {
                                rows.append("null");
                            } else {
                                rows.append(new String(b[j]));
                            }
                        }

                        rows.append("\n");
                    }

                    Debug.returnValue(this, "getCrossReference", rows.toString());
                }

                return buildResultSet(fields, tuples);
            } finally {
                if (fkresults != null) {
                    try {
                        fkresults.close();
                    } catch (Exception sqlEx) {
                        // ignore
                    }

                    fkresults = null;
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ex) {
                        ;
                    }

                    stmt = null;
                }
            }
        } else {
            return buildResultSet(fields, new ArrayList());
        }
    }

    /**
     * @see DatabaseMetaData#getDatabaseMajorVersion()
     */
    public int getDatabaseMajorVersion() throws SQLException {
        return this.conn.getServerMajorVersion();
    }

    /**
     * @see DatabaseMetaData#getDatabaseMinorVersion()
     */
    public int getDatabaseMinorVersion() throws SQLException {
        return this.conn.getServerMinorVersion();
    }

    /**
     * What's the name of this database product?
     *
     * @return database product name
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getDatabaseProductName() throws java.sql.SQLException {
        return "MySQL";
    }

    /**
     * What's the version of this database product?
     *
     * @return database version
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getDatabaseProductVersion() throws java.sql.SQLException {
        return this.conn.getServerVersion();
    }

    //----------------------------------------------------------------------

    /**
     * What's the database's default transaction isolation level?  The values
     * are defined in java.sql.Connection.
     *
     * @return the default isolation level
     * @see Connection
     * @throws java.sql.SQLException if a database access error occurs
     */
    public int getDefaultTransactionIsolation() throws java.sql.SQLException {
        if (this.conn.supportsIsolationLevel()) {
            return java.sql.Connection.TRANSACTION_READ_COMMITTED;
        } else {
            return java.sql.Connection.TRANSACTION_NONE;
        }
    }

    /**
     * What's this JDBC driver's major version number?
     *
     * @return JDBC driver major version
     */
    public int getDriverMajorVersion() {
        return Driver.MAJORVERSION;
    }

    /**
     * What's this JDBC driver's minor version number?
     *
     * @return JDBC driver minor version number
     */
    public int getDriverMinorVersion() {
        return Driver.MINORVERSION;
    }

    /**
     * What's the name of this JDBC driver?
     *
     * @return JDBC driver name
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getDriverName() throws java.sql.SQLException {
        return "MySQL-AB JDBC Driver";
    }

    /**
     * What's the version of this JDBC driver?
     *
     * @return JDBC driver version
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getDriverVersion() throws java.sql.SQLException {
        return "3.0.4-gamma";
    }

    /**
     * Adds to the tuples list the exported keys of exportingTable based on the
     * keysComment from the 'show table status' sql command. KeysComment is that part of
     * the comment field that follows the "InnoDB free ...;" prefix.
     *
     * @param catalog the database to use
     * @param exportingTable the table keys are being exported from
     * @param keysComment the comment from 'show table status'
     * @param fkTableName the foreign key table name
     * @param tuples the rows to add results to
     *
     * @throws SQLException if a database access error occurs
     */
    public void getExportKeyResults(String catalog, String exportingTable,
        String keysComment, List tuples, String fkTableName)
        throws SQLException {
        getResultsImpl(catalog, exportingTable, keysComment, tuples,
            fkTableName, true);
    }

    /**
     * Get a description of a foreign key columns that reference a table's
     * primary key columns (the foreign keys exported by a table).  They are
     * ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ.
     *
     * <P>
     * Each foreign key column description has the following columns:
     *
     * <OL>
     * <li>
     * <B>PKTABLE_CAT</B> String => primary key table catalog (may be null)
     * </li>
     * <li>
     * <B>PKTABLE_SCHEM</B> String => primary key table schema (may be null)
     * </li>
     * <li>
     * <B>PKTABLE_NAME</B> String => primary key table name
     * </li>
     * <li>
     * <B>PKCOLUMN_NAME</B> String => primary key column name
     * </li>
     * <li>
     * <B>FKTABLE_CAT</B> String => foreign key table catalog (may be null)
     * being exported (may be null)
     * </li>
     * <li>
     * <B>FKTABLE_SCHEM</B> String => foreign key table schema (may be null)
     * being exported (may be null)
     * </li>
     * <li>
     * <B>FKTABLE_NAME</B> String => foreign key table name being exported
     * </li>
     * <li>
     * <B>FKCOLUMN_NAME</B> String => foreign key column name being exported
     * </li>
     * <li>
     * <B>KEY_SEQ</B> short => sequence number within foreign key
     * </li>
     * <li>
     * <B>UPDATE_RULE</B> short => What happens to foreign key when primary is
     * updated:
     *
     * <UL>
     * <li>
     * importedKeyCascade - change imported key to agree with primary key
     * update
     * </li>
     * <li>
     * importedKeyRestrict - do not allow update of primary key if it has been
     * imported
     * </li>
     * <li>
     * importedKeySetNull - change imported key to NULL if its primary key has
     * been updated
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>DELETE_RULE</B> short => What happens to the foreign key when
     * primary is deleted.
     *
     * <UL>
     * <li>
     * importedKeyCascade - delete rows that import a deleted key
     * </li>
     * <li>
     * importedKeyRestrict - do not allow delete of primary key if it has been
     * imported
     * </li>
     * <li>
     * importedKeySetNull - change imported key to NULL if its primary key has
     * been deleted
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>FK_NAME</B> String => foreign key identifier (may be null)
     * </li>
     * <li>
     * <B>PK_NAME</B> String => primary key identifier (may be null)
     * </li>
     * </ol>
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schema a schema name pattern; "" retrieves those without a schema
     * @param table a table name
     * @return ResultSet each row is a foreign key column description
     * @see #getImportedKeys
     * @throws java.sql.SQLException if a database access error occurs
     */
    public java.sql.ResultSet getExportedKeys(String catalog, String schema,
        String table) throws java.sql.SQLException {
        if (Driver.TRACE) {
            Object[] args = { catalog, schema, table };
            Debug.methodCall(this, "getExportedKeys", args);
        }

        if (table == null) {
            throw new java.sql.SQLException("Table not specified.", "S1009");
        }

        Field[] fields = new Field[14];
        fields[0] = new Field("", "PKTABLE_CAT", Types.CHAR, 255);
        fields[1] = new Field("", "PKTABLE_SCHEM", Types.CHAR, 0);
        fields[2] = new Field("", "PKTABLE_NAME", Types.CHAR, 255);
        fields[3] = new Field("", "PKCOLUMN_NAME", Types.CHAR, 32);
        fields[4] = new Field("", "FKTABLE_CAT", Types.CHAR, 255);
        fields[5] = new Field("", "FKTABLE_SCHEM", Types.CHAR, 0);
        fields[6] = new Field("", "FKTABLE_NAME", Types.CHAR, 255);
        fields[7] = new Field("", "FKCOLUMN_NAME", Types.CHAR, 32);
        fields[8] = new Field("", "KEY_SEQ", Types.SMALLINT, 2);
        fields[9] = new Field("", "UPDATE_RULE", Types.SMALLINT, 2);
        fields[10] = new Field("", "DELETE_RULE", Types.SMALLINT, 2);
        fields[11] = new Field("", "FK_NAME", Types.CHAR, 0);
        fields[12] = new Field("", "PK_NAME", Types.CHAR, 0);
        fields[13] = new Field("", "DEFERRABILITY", Types.INTEGER, 2);

        if (this.conn.getIO().versionMeetsMinimum(3, 23, 0)) {
            Statement stmt = null;
            ResultSet fkresults = null;

            try {
                /*
                 * Get foreign key information for table
                 */
                if (this.conn.getIO().versionMeetsMinimum(3, 23, 50)) {
                    // we can use 'SHOW CREATE TABLE'
                    String database = this.database;

                    if (catalog != null) {
                        if (!catalog.equals("")) {
                            database = catalog;
                        }
                    }

                    fkresults = extractForeignKeyFromCreateTable(this.conn,
                            this, database, null);
                } else {
                    String databasePart = "";

                    if (catalog != null) {
                        if (!catalog.equals("")) {
                            databasePart = " FROM " + catalog;
                        }
                    } else {
                        databasePart = " FROM " + this.database;
                    }

                    stmt = this.conn.createStatement();
                    fkresults = stmt.executeQuery("show table status "
                            + databasePart);
                }

                // lower-case table name might be turned on
                String tableNameWithCase = getTableNameWithCase(table);

                /*
                * Parse imported foreign key information
                */
                ArrayList tuples = new ArrayList();
                String dummy;

                while (fkresults.next()) {
                    String tableType = fkresults.getString("Type");

                    if ((tableType != null)
                            && (tableType.equalsIgnoreCase("innodb")
                            || tableType.equalsIgnoreCase(SUPPORTS_FK))) {
                        String comment = fkresults.getString("Comment").trim();

                        if (comment != null) {
                            StringTokenizer commentTokens = new StringTokenizer(comment,
                                    ";", false);

                            if (commentTokens.hasMoreTokens()) {
                                dummy = commentTokens.nextToken(); // Skip InnoDB comment

                                while (commentTokens.hasMoreTokens()) {
                                    String keys = commentTokens.nextToken();
                                    getExportKeyResults(catalog,
                                        tableNameWithCase, keys, tuples,
                                        fkresults.getString("Name"));
                                }
                            }
                        }
                    }
                }

                if (Driver.TRACE) {
                    StringBuffer rows = new StringBuffer();
                    rows.append("\n");

                    for (int i = 0; i < tuples.size(); i++) {
                        byte[][] b = (byte[][]) tuples.get(i);
                        rows.append("[Row] ");

                        boolean firstTime = true;

                        for (int j = 0; j < b.length; j++) {
                            if (!firstTime) {
                                rows.append(", ");
                            } else {
                                firstTime = false;
                            }

                            if (b[j] == null) {
                                rows.append("null");
                            } else {
                                rows.append(new String(b[j]));
                            }
                        }

                        rows.append("\n");
                    }

                    Debug.returnValue(this, "getImportedKeys", rows.toString());
                }

                return buildResultSet(fields, tuples);
            } finally {
                if (fkresults != null) {
                    try {
                        fkresults.close();
                    } catch (SQLException sqlEx) {
                        // ignore
                    }

                    fkresults = null;
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ex) {
                        // ignore
                    }

                    stmt = null;
                }
            }
        } else {
            return buildResultSet(fields, new ArrayList());
        }
    }

    /**
     * Get all the "extra" characters that can be used in unquoted identifier
     * names (those beyond a-z, 0-9 and _).
     *
     * @return the string containing the extra characters
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getExtraNameCharacters() throws java.sql.SQLException {
        return "#@";
    }

    /**
     * What's the string used to quote SQL identifiers? This returns a space "
     * " if identifier quoting isn't supported. A JDBC compliant driver
     * always uses a double quote character.
     *
     * @return the quoting string
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getIdentifierQuoteString() throws java.sql.SQLException {
        if (this.conn.supportsQuotedIdentifiers()) {
            if (!this.conn.useAnsiQuotedIdentifiers()) {
                return "`";
            } else {
                return "\"";
            }
        } else {
            return " ";
        }
    }

    /**
     * Populates the tuples list with the imported keys of importingTable based on the
     * keysComment from the 'show table status' sql command. KeysComment is that part of
     * the comment field that follows the "InnoDB free ...;" prefix.
     *
     * @param catalog the database to use
     * @param importingTable the table keys are being imported to
     * @param keysComment the comment from 'show table status'
     * @param tuples the rows to add results to
     *
     * @throws SQLException if a database access error occurs
     */
    public void getImportKeyResults(String catalog, String importingTable,
        String keysComment, List tuples) throws SQLException {
        getResultsImpl(catalog, importingTable, keysComment, tuples, null, false);
    }

    /**
     * Get a description of the primary key columns that are referenced by a
     * table's foreign key columns (the primary keys imported by a table).
     * They are ordered by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, and
     * KEY_SEQ.
     *
     * <P>
     * Each primary key column description has the following columns:
     *
     * <OL>
     * <li>
     * <B>PKTABLE_CAT</B> String => primary key table catalog being imported
     * (may be null)
     * </li>
     * <li>
     * <B>PKTABLE_SCHEM</B> String => primary key table schema being imported
     * (may be null)
     * </li>
     * <li>
     * <B>PKTABLE_NAME</B> String => primary key table name being imported
     * </li>
     * <li>
     * <B>PKCOLUMN_NAME</B> String => primary key column name being imported
     * </li>
     * <li>
     * <B>FKTABLE_CAT</B> String => foreign key table catalog (may be null)
     * </li>
     * <li>
     * <B>FKTABLE_SCHEM</B> String => foreign key table schema (may be null)
     * </li>
     * <li>
     * <B>FKTABLE_NAME</B> String => foreign key table name
     * </li>
     * <li>
     * <B>FKCOLUMN_NAME</B> String => foreign key column name
     * </li>
     * <li>
     * <B>KEY_SEQ</B> short => sequence number within foreign key
     * </li>
     * <li>
     * <B>UPDATE_RULE</B> short => What happens to foreign key when primary is
     * updated:
     *
     * <UL>
     * <li>
     * importedKeyCascade - change imported key to agree with primary key
     * update
     * </li>
     * <li>
     * importedKeyRestrict - do not allow update of primary key if it has been
     * imported
     * </li>
     * <li>
     * importedKeySetNull - change imported key to NULL if its primary key has
     * been updated
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>DELETE_RULE</B> short => What happens to the foreign key when
     * primary is deleted.
     *
     * <UL>
     * <li>
     * importedKeyCascade - delete rows that import a deleted key
     * </li>
     * <li>
     * importedKeyRestrict - do not allow delete of primary key if it has been
     * imported
     * </li>
     * <li>
     * importedKeySetNull - change imported key to NULL if its primary key has
     * been deleted
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>FK_NAME</B> String => foreign key name (may be null)
     * </li>
     * <li>
     * <B>PK_NAME</B> String => primary key name (may be null)
     * </li>
     * </ol>
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schema a schema name pattern; "" retrieves those without a schema
     * @param table a table name
     * @return ResultSet each row is a primary key column description
     * @see #getExportedKeys
     * @throws java.sql.SQLException if a database access error occurs
     */
    public java.sql.ResultSet getImportedKeys(String catalog, String schema,
        String table) throws java.sql.SQLException {
        if (Driver.TRACE) {
            Object[] args = { catalog, schema, table };
            Debug.methodCall(this, "getImportedKeys", args);
        }

        if (table == null) {
            throw new java.sql.SQLException("Table not specified.", "S1009");
        }

        Field[] fields = new Field[14];
        fields[0] = new Field("", "PKTABLE_CAT", Types.CHAR, 255);
        fields[1] = new Field("", "PKTABLE_SCHEM", Types.CHAR, 0);
        fields[2] = new Field("", "PKTABLE_NAME", Types.CHAR, 255);
        fields[3] = new Field("", "PKCOLUMN_NAME", Types.CHAR, 32);
        fields[4] = new Field("", "FKTABLE_CAT", Types.CHAR, 255);
        fields[5] = new Field("", "FKTABLE_SCHEM", Types.CHAR, 0);
        fields[6] = new Field("", "FKTABLE_NAME", Types.CHAR, 255);
        fields[7] = new Field("", "FKCOLUMN_NAME", Types.CHAR, 32);
        fields[8] = new Field("", "KEY_SEQ", Types.SMALLINT, 2);
        fields[9] = new Field("", "UPDATE_RULE", Types.SMALLINT, 2);
        fields[10] = new Field("", "DELETE_RULE", Types.SMALLINT, 2);
        fields[11] = new Field("", "FK_NAME", Types.CHAR, 0);
        fields[12] = new Field("", "PK_NAME", Types.CHAR, 0);
        fields[13] = new Field("", "DEFERRABILITY", Types.INTEGER, 2);

        if (this.conn.getIO().versionMeetsMinimum(3, 23, 0)) {
            Statement stmt = null;
            ResultSet fkresults = null;

            try {
                /*
                 * Get foreign key information for table
                 */
                if (this.conn.getIO().versionMeetsMinimum(3, 23, 50)) {
                    // we can use 'SHOW CREATE TABLE'
                    String database = this.database;

                    if (catalog != null) {
                        if (!catalog.equals("")) {
                            database = catalog;
                        }
                    }

                    fkresults = extractForeignKeyFromCreateTable(this.conn,
                            this, database, table);
                } else {
                    String databasePart = "";

                    if (catalog != null) {
                        if (!catalog.equals("")) {
                            databasePart = " FROM " + catalog;
                        }
                    } else {
                        databasePart = " FROM " + this.database;
                    }

                    stmt = this.conn.createStatement();
                    fkresults = stmt.executeQuery("show table status "
                            + databasePart + " like '" + table + "'");
                }

                /*
                * Parse imported foreign key information
                */
                ArrayList tuples = new ArrayList();
                String dummy;

                while (fkresults.next()) {
                    String tableType = fkresults.getString("Type");

                    if ((tableType != null)
                            && (tableType.equalsIgnoreCase("innodb")
                            || tableType.equalsIgnoreCase(SUPPORTS_FK))) {
                        String comment = fkresults.getString("Comment").trim();

                        if (comment != null) {
                            StringTokenizer commentTokens = new StringTokenizer(comment,
                                    ";", false);

                            if (commentTokens.hasMoreTokens()) {
                                dummy = commentTokens.nextToken(); // Skip InnoDB comment

                                while (commentTokens.hasMoreTokens()) {
                                    String keys = commentTokens.nextToken();
                                    getImportKeyResults(catalog, table, keys,
                                        tuples);
                                }
                            }
                        }
                    }
                }

                if (Driver.TRACE) {
                    StringBuffer rows = new StringBuffer();
                    rows.append("\n");

                    for (int i = 0; i < tuples.size(); i++) {
                        byte[][] b = (byte[][]) tuples.get(i);
                        rows.append("[Row] ");

                        boolean firstTime = true;

                        for (int j = 0; j < b.length; j++) {
                            if (!firstTime) {
                                rows.append(", ");
                            } else {
                                firstTime = false;
                            }

                            if (b[j] == null) {
                                rows.append("null");
                            } else {
                                rows.append(new String(b[j]));
                            }
                        }

                        rows.append("\n");
                    }

                    Debug.returnValue(this, "getImportedKeys", rows.toString());
                }

                return buildResultSet(fields, tuples);
            } finally {
                if (fkresults != null) {
                    try {
                        fkresults.close();
                    } catch (SQLException sqlEx) {
                        // ignore
                    }

                    fkresults = null;
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ex) {
                        // ignore
                    }

                    stmt = null;
                }
            }
        } else {
            return buildResultSet(fields, new ArrayList());
        }
    }

    /**
     * Get a description of a table's indices and statistics. They are ordered
     * by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
     *
     * <P>
     * Each index column description has the following columns:
     *
     * <OL>
     * <li>
     * <B>TABLE_CAT</B> String => table catalog (may be null)
     * </li>
     * <li>
     * <B>TABLE_SCHEM</B> String => table schema (may be null)
     * </li>
     * <li>
     * <B>TABLE_NAME</B> String => table name
     * </li>
     * <li>
     * <B>NON_UNIQUE</B> boolean => Can index values be non-unique? false when
     * TYPE is tableIndexStatistic
     * </li>
     * <li>
     * <B>INDEX_QUALIFIER</B> String => index catalog (may be null); null when
     * TYPE is tableIndexStatistic
     * </li>
     * <li>
     * <B>INDEX_NAME</B> String => index name; null when TYPE is
     * tableIndexStatistic
     * </li>
     * <li>
     * <B>TYPE</B> short => index type:
     *
     * <UL>
     * <li>
     * tableIndexStatistic - this identifies table statistics that are
     * returned in conjuction with a table's index descriptions
     * </li>
     * <li>
     * tableIndexClustered - this is a clustered index
     * </li>
     * <li>
     * tableIndexHashed - this is a hashed index
     * </li>
     * <li>
     * tableIndexOther - this is some other style of index
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>ORDINAL_POSITION</B> short => column sequence number within index;
     * zero when TYPE is tableIndexStatistic
     * </li>
     * <li>
     * <B>COLUMN_NAME</B> String => column name; null when TYPE is
     * tableIndexStatistic
     * </li>
     * <li>
     * <B>ASC_OR_DESC</B> String => column sort sequence, "A" => ascending,
     * "D" => descending, may be null if sort sequence is not supported; null
     * when TYPE is tableIndexStatistic
     * </li>
     * <li>
     * <B>CARDINALITY</B> int => When TYPE is tableIndexStatisic then this is
     * the number of rows in the table; otherwise it is the number of unique
     * values in the index.
     * </li>
     * <li>
     * <B>PAGES</B> int => When TYPE is  tableIndexStatisic then this is the
     * number of pages used for the table, otherwise it is the number of
     * pages used for the current index.
     * </li>
     * <li>
     * <B>FILTER_CONDITION</B> String => Filter condition, if any. (may be
     * null)
     * </li>
     * </ol>
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schema a schema name pattern; "" retrieves those without a schema
     * @param table a table name
     * @param unique when true, return only indices for unique values; when
     *        false, return indices regardless of whether unique or not
     * @param approximate when true, result is allowed to reflect approximate
     *        or out of data values; when false, results are requested to be
     *        accurate
     * @return ResultSet each row is an index column description
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public java.sql.ResultSet getIndexInfo(String catalog, String schema,
        String table, boolean unique, boolean approximate)
        throws java.sql.SQLException {
        /*
         * MySQL stores index information in the following fields:
         *
         * Table
         * Non_unique
         * Key_name
         * Seq_in_index
         * Column_name
         * Collation
         * Cardinality
         * Sub_part
         */
        String databasePart = "";

        if (catalog != null) {
            if (!catalog.equals("")) {
                databasePart = " FROM " + this.quotedId + catalog
                    + this.quotedId;
            }
        } else {
            databasePart = " FROM " + this.quotedId + this.database
                + this.quotedId;
        }

        Statement stmt = null;
        ResultSet results = null;

        try {
            stmt = this.conn.createStatement();
            results = stmt.executeQuery("SHOW INDEX FROM " + table
                    + databasePart);

            Field[] fields = new Field[13];
            fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 255);
            fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
            fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 255);
            fields[3] = new Field("", "NON_UNIQUE", Types.CHAR, 3);
            fields[4] = new Field("", "INDEX_QUALIFIER", Types.CHAR, 1);
            fields[5] = new Field("", "INDEX_NAME", Types.CHAR, 32);
            fields[6] = new Field("", "TYPE", Types.CHAR, 32);
            fields[7] = new Field("", "ORDINAL_POSITION", Types.SMALLINT, 5);
            fields[8] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
            fields[9] = new Field("", "ASC_OR_DESC", Types.CHAR, 1);
            fields[10] = new Field("", "CARDINALITY", Types.INTEGER, 10);
            fields[11] = new Field("", "PAGES", Types.INTEGER, 10);
            fields[12] = new Field("", "FILTER_CONDITION", Types.CHAR, 32);

            ArrayList rows = new ArrayList();

            while (results.next()) {
                byte[][] row = new byte[14][];
                row[0] = ((catalog == null) ? new byte[0] : s2b(catalog));
                row[1] = null;
                row[2] = results.getBytes("Table");
                row[3] = ((results.getInt("Non_unique") != 0) ? s2b("true")
                                                              : s2b("false"));
                row[4] = new byte[0];
                row[5] = results.getBytes("Key_name");
                row[6] = Integer.toString(java.sql.DatabaseMetaData.tableIndexOther)
                                .getBytes();
                row[7] = results.getBytes("Seq_in_index");
                row[8] = results.getBytes("Column_name");
                row[9] = results.getBytes("Collation");
                row[10] = results.getBytes("Cardinality");
                row[11] = s2b("0");
                row[12] = null;
                rows.add(row);
            }

            java.sql.ResultSet indexInfo = buildResultSet(fields, rows);

            return indexInfo;
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (Exception ex) {
                    ;
                }

                results = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ex) {
                    ;
                }

                stmt = null;
            }
        }
    }

    /**
     * @see DatabaseMetaData#getJDBCMajorVersion()
     */
    public int getJDBCMajorVersion() throws SQLException {
        return 3;
    }

    /**
     * @see DatabaseMetaData#getJDBCMinorVersion()
     */
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    //----------------------------------------------------------------------
    // The following group of methods exposes various limitations
    // based on the target database with the current driver.
    // Unless otherwise specified, a result of zero means there is no
    // limit, or the limit is not known.

    /**
     * How many hex characters can you have in an inline binary literal?
     *
     * @return max literal length
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxBinaryLiteralLength() throws java.sql.SQLException {
        return 16777208;
    }

    /**
     * What's the maximum length of a catalog name?
     *
     * @return max name length in bytes
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxCatalogNameLength() throws java.sql.SQLException {
        return 32;
    }

    /**
     * What's the max length for a character literal?
     *
     * @return max literal length
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxCharLiteralLength() throws java.sql.SQLException {
        return 16777208;
    }

    /**
     * What's the limit on column name length?
     *
     * @return max literal length
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxColumnNameLength() throws java.sql.SQLException {
        return 64;
    }

    /**
     * What's the maximum number of columns in a "GROUP BY" clause?
     *
     * @return max number of columns
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxColumnsInGroupBy() throws java.sql.SQLException {
        return 64;
    }

    /**
     * What's the maximum number of columns allowed in an index?
     *
     * @return max columns
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxColumnsInIndex() throws java.sql.SQLException {
        return 16;
    }

    /**
     * What's the maximum number of columns in an "ORDER BY" clause?
     *
     * @return max columns
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxColumnsInOrderBy() throws java.sql.SQLException {
        return 64;
    }

    /**
     * What's the maximum number of columns in a "SELECT" list?
     *
     * @return max columns
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxColumnsInSelect() throws java.sql.SQLException {
        return 256;
    }

    /**
     * What's maximum number of columns in a table?
     *
     * @return max columns
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxColumnsInTable() throws java.sql.SQLException {
        return 512;
    }

    /**
     * How many active connections can we have at a time to this database?
     *
     * @return max connections
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxConnections() throws java.sql.SQLException {
        return 0;
    }

    /**
     * What's the maximum cursor name length?
     *
     * @return max cursor name length in bytes
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxCursorNameLength() throws java.sql.SQLException {
        return 64;
    }

    /**
     * What's the maximum length of an index (in bytes)?
     *
     * @return max index length in bytes
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxIndexLength() throws java.sql.SQLException {
        return 256;
    }

    /**
     * What's the maximum length of a procedure name?
     *
     * @return max name length in bytes
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxProcedureNameLength() throws java.sql.SQLException {
        return 0;
    }

    /**
     * What's the maximum length of a single row?
     *
     * @return max row size in bytes
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxRowSize() throws java.sql.SQLException {
        return Integer.MAX_VALUE - 8; // Max buffer size - HEADER
    }

    /**
     * What's the maximum length allowed for a schema name?
     *
     * @return max name length in bytes
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxSchemaNameLength() throws java.sql.SQLException {
        return 0;
    }

    /**
     * What's the maximum length of a SQL statement?
     *
     * @return max length in bytes
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxStatementLength() throws java.sql.SQLException {
        return MysqlIO.getMaxBuf() - 4; // Max buffer - header
    }

    /**
     * How many active statements can we have open at one time to this
     * database?
     *
     * @return the maximum
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxStatements() throws java.sql.SQLException {
        return 0;
    }

    /**
     * What's the maximum length of a table name?
     *
     * @return max name length in bytes
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxTableNameLength() throws java.sql.SQLException {
        return 64;
    }

    /**
     * What's the maximum number of tables in a SELECT?
     *
     * @return the maximum
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxTablesInSelect() throws java.sql.SQLException {
        return 256;
    }

    /**
     * What's the maximum length of a user name?
     *
     * @return max name length  in bytes
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public int getMaxUserNameLength() throws java.sql.SQLException {
        return 16;
    }

    /**
     * Get a comma separated list of math functions.
     *
     * @return the list
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getNumericFunctions() throws java.sql.SQLException {
        return "ABS,ACOS,ASIN,ATAN,ATAN2,BIT_COUNT,CEILING,COS,"
        + "COT,DEGREES,EXP,FLOOR,LOG,LOG10,MAX,MIN,MOD,PI,POW,"
        + "POWER,RADIANS,RAND,ROUND,SIN,SQRT,TAN,TRUNCATE";
    }

    /**
     * Get a description of a table's primary key columns.  They are ordered
     * by COLUMN_NAME.
     *
     * <P>
     * Each column description has the following columns:
     *
     * <OL>
     * <li>
     * <B>TABLE_CAT</B> String => table catalog (may be null)
     * </li>
     * <li>
     * <B>TABLE_SCHEM</B> String => table schema (may be null)
     * </li>
     * <li>
     * <B>TABLE_NAME</B> String => table name
     * </li>
     * <li>
     * <B>COLUMN_NAME</B> String => column name
     * </li>
     * <li>
     * <B>KEY_SEQ</B> short => sequence number within primary key
     * </li>
     * <li>
     * <B>PK_NAME</B> String => primary key name (may be null)
     * </li>
     * </ol>
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schema a schema name pattern; "" retrieves those without a schema
     * @param table a table name
     * @return ResultSet each row is a primary key column description
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public java.sql.ResultSet getPrimaryKeys(String catalog, String schema,
        String table) throws java.sql.SQLException {
        Field[] fields = new Field[6];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 255);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 255);
        fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
        fields[4] = new Field("", "KEY_SEQ", Types.SMALLINT, 5);
        fields[5] = new Field("", "PK_NAME", Types.CHAR, 32);

        String dbSub = "";

        if (catalog != null) {
            if (!catalog.equals("")) {
                dbSub = " FROM " + this.quotedId + catalog + this.quotedId;
            }
        } else {
            dbSub = " FROM " + this.quotedId + this.database + this.quotedId;
        }

        if (table == null) {
            throw new java.sql.SQLException("Table not specified.", "S1009");
        }

        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = this.conn.createStatement();
            rs = stmt.executeQuery("show keys from " + table + dbSub);

            ArrayList tuples = new ArrayList();
            TreeMap sortMap = new TreeMap();

            while (rs.next()) {
                String keyType = rs.getString("Key_name");

                if (keyType != null) {
                    if (keyType.toUpperCase().startsWith("PRI")) {
                        byte[][] tuple = new byte[6][];
                        tuple[0] = ((catalog == null) ? new byte[0] : s2b(catalog));
                        tuple[1] = null;
                        tuple[2] = s2b(table);

                        String columnName = rs.getString("Column_name");
                        tuple[3] = s2b(columnName);
                        tuple[4] = s2b(rs.getString("Seq_in_index"));
                        tuple[5] = s2b(keyType);
                        sortMap.put(columnName, tuple);
                    }
                }
            }

            // Now pull out in column name sorted order
            Iterator sortedIterator = sortMap.values().iterator();

            while (sortedIterator.hasNext()) {
                tuples.add(sortedIterator.next());
            }

            return buildResultSet(fields, tuples);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ex) {
                    ;
                }

                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ex) {
                    ;
                }

                stmt = null;
            }
        }
    }

    /**
     * Get a description of a catalog's stored procedure parameters and result
     * columns.
     *
     * <P>
     * Only descriptions matching the schema, procedure and parameter name
     * criteria are returned.  They are ordered by PROCEDURE_SCHEM and
     * PROCEDURE_NAME. Within this, the return value, if any, is first. Next
     * are the parameter descriptions in call order. The column descriptions
     * follow in column number order.
     * </p>
     *
     * <P>
     * Each row in the ResultSet is a parameter desription or column
     * description with the following fields:
     *
     * <OL>
     * <li>
     * <B>PROCEDURE_CAT</B> String => procedure catalog (may be null)
     * </li>
     * <li>
     * <B>PROCEDURE_SCHEM</B> String => procedure schema (may be null)
     * </li>
     * <li>
     * <B>PROCEDURE_NAME</B> String => procedure name
     * </li>
     * <li>
     * <B>COLUMN_NAME</B> String => column/parameter name
     * </li>
     * <li>
     * <B>COLUMN_TYPE</B> Short => kind of column/parameter:
     *
     * <UL>
     * <li>
     * procedureColumnUnknown - nobody knows
     * </li>
     * <li>
     * procedureColumnIn - IN parameter
     * </li>
     * <li>
     * procedureColumnInOut - INOUT parameter
     * </li>
     * <li>
     * procedureColumnOut - OUT parameter
     * </li>
     * <li>
     * procedureColumnReturn - procedure return value
     * </li>
     * <li>
     * procedureColumnResult - result column in ResultSet
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>DATA_TYPE</B> short => SQL type from java.sql.Types
     * </li>
     * <li>
     * <B>TYPE_NAME</B> String => SQL type name
     * </li>
     * <li>
     * <B>PRECISION</B> int => precision
     * </li>
     * <li>
     * <B>LENGTH</B> int => length in bytes of data
     * </li>
     * <li>
     * <B>SCALE</B> short => scale
     * </li>
     * <li>
     * <B>RADIX</B> short => radix
     * </li>
     * <li>
     * <B>NULLABLE</B> short => can it contain NULL?
     *
     * <UL>
     * <li>
     * procedureNoNulls - does not allow NULL values
     * </li>
     * <li>
     * procedureNullable - allows NULL values
     * </li>
     * <li>
     * procedureNullableUnknown - nullability unknown
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>REMARKS</B> String => comment describing parameter/column
     * </li>
     * </ol>
     * </p>
     *
     * <P>
     * <B>Note:</B> Some databases may not return the column descriptions for
     * a procedure. Additional columns beyond REMARKS can be defined by the
     * database.
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schemaPattern a schema name pattern; "" retrieves those without
     *        a schema
     * @param procedureNamePattern a procedure name pattern
     * @param columnNamePattern a column name pattern
     * @return ResultSet each row is a stored procedure parameter or column
     *         description
     * @see #getSearchStringEscape
     * @throws java.sql.SQLException if a database access error occurs
     */
    public java.sql.ResultSet getProcedureColumns(String catalog,
        String schemaPattern, String procedureNamePattern,
        String columnNamePattern) throws java.sql.SQLException {
        Field[] fields = new Field[14];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 0);
        fields[1] = new Field("", "PROCEDURE_CAT", Types.CHAR, 0);
        fields[2] = new Field("", "PROCEDURE_SCHEM", Types.CHAR, 0);
        fields[3] = new Field("", "PROCEDURE_NAME", Types.CHAR, 0);
        fields[4] = new Field("", "COLUMN_NAME", Types.CHAR, 0);
        fields[5] = new Field("", "COLUMN_TYPE", Types.CHAR, 0);
        fields[6] = new Field("", "DATA_TYPE", Types.SMALLINT, 0);
        fields[7] = new Field("", "TYPE_NAME", Types.CHAR, 0);
        fields[8] = new Field("", "PRECISION", Types.INTEGER, 0);
        fields[9] = new Field("", "LENGTH", Types.INTEGER, 0);
        fields[10] = new Field("", "SCALE", Types.SMALLINT, 0);
        fields[11] = new Field("", "RADIX", Types.SMALLINT, 0);
        fields[12] = new Field("", "NULLABLE", Types.SMALLINT, 0);
        fields[13] = new Field("", "REMARKS", Types.CHAR, 0);

        return buildResultSet(fields, new ArrayList());
    }

    /**
     * What's the database vendor's preferred term for "procedure"?
     *
     * @return the vendor term
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getProcedureTerm() throws java.sql.SQLException {
        return "";
    }

    /**
     * Get a description of stored procedures available in a catalog.
     *
     * <P>
     * Only procedure descriptions matching the schema and procedure name
     * criteria are returned.  They are ordered by PROCEDURE_SCHEM, and
     * PROCEDURE_NAME.
     * </p>
     *
     * <P>
     * Each procedure description has the the following columns:
     *
     * <OL>
     * <li>
     * <B>PROCEDURE_CAT</B> String => procedure catalog (may be null)
     * </li>
     * <li>
     * <B>PROCEDURE_SCHEM</B> String => procedure schema (may be null)
     * </li>
     * <li>
     * <B>PROCEDURE_NAME</B> String => procedure name
     * </li>
     * <li>
     * reserved for future use
     * </li>
     * <li>
     * reserved for future use
     * </li>
     * <li>
     * reserved for future use
     * </li>
     * <li>
     * <B>REMARKS</B> String => explanatory comment on the procedure
     * </li>
     * <li>
     * <B>PROCEDURE_TYPE</B> short => kind of procedure:
     *
     * <UL>
     * <li>
     * procedureResultUnknown - May return a result
     * </li>
     * <li>
     * procedureNoResult - Does not return a result
     * </li>
     * <li>
     * procedureReturnsResult - Returns a result
     * </li>
     * </ul>
     * </li>
     * </ol>
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schemaPattern a schema name pattern; "" retrieves those without
     *        a schema
     * @param procedureNamePattern a procedure name pattern
     * @return ResultSet each row is a procedure description
     * @see #getSearchStringEscape
     * @throws java.sql.SQLException if a database access error occurs
     */
    public java.sql.ResultSet getProcedures(String catalog,
        String schemaPattern, String procedureNamePattern)
        throws java.sql.SQLException {
        Field[] fields = new Field[8];
        fields[0] = new Field("", "PROCEDURE_CAT", Types.CHAR, 0);
        fields[1] = new Field("", "PROCEDURE_SCHEM", Types.CHAR, 0);
        fields[2] = new Field("", "PROCEDURE_NAME", Types.CHAR, 0);
        fields[3] = new Field("", "resTABLE_CAT", Types.CHAR, 0);
        fields[4] = new Field("", "resTABLE_CAT", Types.CHAR, 0);
        fields[5] = new Field("", "resTABLE_CAT", Types.CHAR, 0);
        fields[6] = new Field("", "REMARKS", Types.CHAR, 0);
        fields[7] = new Field("", "PROCEDURE_TYPE", Types.SMALLINT, 0);

        return buildResultSet(fields, new ArrayList());
    }

    /**
     * Is the database in read-only mode?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean isReadOnly() throws java.sql.SQLException {
        return false;
    }

    /**
     * @see DatabaseMetaData#getResultSetHoldability()
     */
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * Get a comma separated list of all a database's SQL keywords that are
     * NOT also SQL92 keywords.
     *
     * @return the list
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getSQLKeywords() throws java.sql.SQLException {
        return "AUTO_INCREMENT,BINARY,BLOB,ENUM,INFILE,LOAD,MEDIUMINT,OPTION,OUTFILE,REPLACE,SET,TEXT,UNSIGNED,ZEROFILL";
    }

    /**
     * @see DatabaseMetaData#getSQLStateType()
     */
    public int getSQLStateType() throws SQLException {
        return 0;
    }

    /**
     * What's the database vendor's preferred term for "schema"?
     *
     * @return the vendor term
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getSchemaTerm() throws java.sql.SQLException {
        return "";
    }

    /**
     * Get the schema names available in this database.  The results are
     * ordered by schema name.
     *
     * <P>
     * The schema column is:
     *
     * <OL>
     * <li>
     * <B>TABLE_SCHEM</B> String => schema name
     * </li>
     * </ol>
     * </p>
     *
     * @return ResultSet each row has a single String column that is a schema
     *         name
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public java.sql.ResultSet getSchemas() throws java.sql.SQLException {
        Field[] fields = new Field[1];
        fields[0] = new Field("", "TABLE_SCHEM", java.sql.Types.CHAR, 0);

        ArrayList tuples = new ArrayList();
        java.sql.ResultSet results = buildResultSet(fields, tuples);

        return results;
    }

    /**
     * This is the string that can be used to escape '_' or '%' in the string
     * pattern style catalog search parameters.
     *
     * <P>
     * The '_' character represents any single character.
     * </p>
     *
     * <P>
     * The '%' character represents any sequence of zero or more characters.
     * </p>
     *
     * @return the string used to escape wildcard characters
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getSearchStringEscape() throws java.sql.SQLException {
        return "\\";
    }

    /**
     * Get a comma separated list of string functions.
     *
     * @return the list
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getStringFunctions() throws java.sql.SQLException {
        return "ASCII,BIN,BIT_LENGTH,CHAR,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,"
        + "CONCAT_WS,CONV,ELT,EXPORT_SET,FIELD,FIND_IN_SET,HEX,INSERT,"
        + "INSTR,LCASE,LEFT,LENGTH,LOAD_FILE,LOCATE,LOCATE,LOWER,LPAD,"
        + "LTRIM,MAKE_SET,MATCH,MID,OCT,OCTET_LENGTH,ORD,POSITION,"
        + "QUOTE,REPEAT,REPLACE,REVERSE,RIGHT,RPAD,RTRIM,SOUNDEX,"
        + "SPACE,STRCMP,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING,"
        + "SUBSTRING_INDEX,TRIM,UCASE,UPPER";
    }

    /**
     * @see DatabaseMetaData#getSuperTables(String, String, String)
     */
    public java.sql.ResultSet getSuperTables(String arg0, String arg1,
        String arg2) throws SQLException {
        Field[] fields = new Field[4];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 32);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 32);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 32);
        fields[3] = new Field("", "SUPERTABLE_NAME", Types.CHAR, 32);

        return buildResultSet(fields, new ArrayList());
    }

    /**
     * @see DatabaseMetaData#getSuperTypes(String, String, String)
     */
    public java.sql.ResultSet getSuperTypes(String arg0, String arg1,
        String arg2) throws SQLException {
        Field[] fields = new Field[6];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 32);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 32);
        fields[2] = new Field("", "TYPE_NAME", Types.CHAR, 32);
        fields[3] = new Field("", "SUPERTYPE_CAT", Types.CHAR, 32);
        fields[4] = new Field("", "SUPERTYPE_SCHEM", Types.CHAR, 32);
        fields[5] = new Field("", "SUPERTYPE_NAME", Types.CHAR, 32);

        return buildResultSet(fields, new ArrayList());
    }

    /**
     * Get a comma separated list of system functions.
     *
     * @return the list
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getSystemFunctions() throws java.sql.SQLException {
        return "DATABASE,USER,SYSTEM_USER,SESSION_USER,PASSWORD,ENCRYPT,LAST_INSERT_ID,VEresultsION";
    }

    /**
     * Get a description of the access rights for each table available in a
     * catalog.
     *
     * <P>
     * Only privileges matching the schema and table name criteria are
     * returned.  They are ordered by TABLE_SCHEM, TABLE_NAME, and PRIVILEGE.
     * </p>
     *
     * <P>
     * Each privilige description has the following columns:
     *
     * <OL>
     * <li>
     * <B>TABLE_CAT</B> String => table catalog (may be null)
     * </li>
     * <li>
     * <B>TABLE_SCHEM</B> String => table schema (may be null)
     * </li>
     * <li>
     * <B>TABLE_NAME</B> String => table name
     * </li>
     * <li>
     * <B>COLUMN_NAME</B> String => column name
     * </li>
     * <li>
     * <B>GRANTOR</B> => grantor of access (may be null)
     * </li>
     * <li>
     * <B>GRANTEE</B> String => grantee of access
     * </li>
     * <li>
     * <B>PRIVILEGE</B> String => name of access (SELECT, INSERT, UPDATE,
     * REFRENCES, ...)
     * </li>
     * <li>
     * <B>IS_GRANTABLE</B> String => "YES" if grantee is permitted to grant to
     * others; "NO" if not; null if unknown
     * </li>
     * </ol>
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schemaPattern a schema name pattern; "" retrieves those without
     *        a schema
     * @param tableNamePattern a table name pattern
     * @return ResultSet each row is a table privilege description
     * @see #getSearchStringEscape
     * @throws java.sql.SQLException if a database access error occurs
     */
    public java.sql.ResultSet getTablePrivileges(String catalog,
        String schemaPattern, String tableNamePattern)
        throws java.sql.SQLException {
        Field[] fields = new Field[7];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 64);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 1);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 64);
        fields[3] = new Field("", "GRANTOR", Types.CHAR, 77);
        fields[4] = new Field("", "GRANTEE", Types.CHAR, 77);
        fields[5] = new Field("", "PRIVILEGE", Types.CHAR, 64);
        fields[6] = new Field("", "IS_GRANTABLE", Types.CHAR, 3);

        StringBuffer grantQuery = new StringBuffer(
                "SELECT host,db,table_name,grantor,user,table_priv from mysql.tables_priv ");
        grantQuery.append(" WHERE ");

        if ((catalog != null) && (catalog.length() != 0)) {
            grantQuery.append(" db='");
            grantQuery.append(catalog);
            grantQuery.append("' AND ");
        }

        grantQuery.append("table_name like '");
        grantQuery.append(tableNamePattern);
        grantQuery.append("'");

        ResultSet results = null;
        ArrayList grantRows = new ArrayList();
        Statement stmt = null;

        try {
            stmt = this.conn.createStatement();
            results = stmt.executeQuery(grantQuery.toString());

            while (results.next()) {
                String host = results.getString(1);
                String database = results.getString(2);
                String table = results.getString(3);
                String grantor = results.getString(4);
                String user = results.getString(5);

                if ((user == null) || (user.length() == 0)) {
                    user = "%";
                }

                StringBuffer fullUser = new StringBuffer(user);

                if ((host != null) && this.conn.useHostsInPrivileges()) {
                    fullUser.append("@");
                    fullUser.append(host);
                }

                String allPrivileges = results.getString(6);

                if (allPrivileges != null) {
                    allPrivileges = allPrivileges.toUpperCase();

                    StringTokenizer st = new StringTokenizer(allPrivileges, ",");

                    while (st.hasMoreTokens()) {
                        String privilege = st.nextToken().trim();

                        // Loop through every column in the table
                        java.sql.ResultSet columnResults = null;

                        try {
                            columnResults = getColumns(catalog, schemaPattern,
                                    table, "%");

                            while (columnResults.next()) {
                                byte[][] tuple = new byte[8][];
                                tuple[0] = s2b(database);
                                tuple[1] = null;
                                tuple[2] = s2b(table);

                                if (grantor != null) {
                                    tuple[3] = s2b(grantor);
                                } else {
                                    tuple[3] = null;
                                }

                                tuple[4] = s2b(fullUser.toString());
                                tuple[5] = s2b(privilege);
                                tuple[6] = null;
                                grantRows.add(tuple);
                            }
                        } finally {
                            if (columnResults != null) {
                                try {
                                    columnResults.close();
                                } catch (Exception ex) {
                                    ;
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (Exception ex) {
                    ;
                }

                results = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ex) {
                    ;
                }

                stmt = null;
            }
        }

        return buildResultSet(fields, grantRows);
    }

    /**
     * Get the table types available in this database.  The results are
     * ordered by table type.
     *
     * <P>
     * The table type is:
     *
     * <OL>
     * <li>
     * <B>TABLE_TYPE</B> String => table type.  Typical types are "TABLE",
     * "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY",
     * "ALIAS", "SYNONYM".
     * </li>
     * </ol>
     * </p>
     *
     * @return ResultSet each row has a single String column that is a table
     *         type
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public java.sql.ResultSet getTableTypes() throws java.sql.SQLException {
        ArrayList tuples = new ArrayList();
        Field[] fields = new Field[1];
        fields[0] = new Field("", "TABLE_TYPE", Types.VARCHAR, 5);

        byte[][] tableTypeRow = new byte[1][];
        tableTypeRow[0] = TABLE_AS_BYTES;
        tuples.add(tableTypeRow);

        byte[][] tempTypeRow = new byte[1][];
        tempTypeRow[0] = s2b("LOCAL TEMPORARY");
        tuples.add(tempTypeRow);

        return buildResultSet(fields, tuples);
    }

    /**
     * Get a description of tables available in a catalog.
     *
     * <P>
     * Only table descriptions matching the catalog, schema, table name and
     * type criteria are returned.  They are ordered by TABLE_TYPE,
     * TABLE_SCHEM and TABLE_NAME.
     * </p>
     *
     * <P>
     * Each table description has the following columns:
     *
     * <OL>
     * <li>
     * <B>TABLE_CAT</B> String => table catalog (may be null)
     * </li>
     * <li>
     * <B>TABLE_SCHEM</B> String => table schema (may be null)
     * </li>
     * <li>
     * <B>TABLE_NAME</B> String => table name
     * </li>
     * <li>
     * <B>TABLE_TYPE</B> String => table type.  Typical types are "TABLE",
     * "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY",
     * "ALIAS", "SYNONYM".
     * </li>
     * <li>
     * <B>REMARKS</B> String => explanatory comment on the table
     * </li>
     * </ol>
     * </p>
     *
     * <P>
     * <B>Note:</B> Some databases may not return information for all tables.
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schemaPattern a schema name pattern; "" retrieves those without
     *        a schema
     * @param tableNamePattern a table name pattern
     * @param types a list of table types to include; null returns all types
     * @return ResultSet each row is a table description
     * @throws java.sql.SQLException DOCUMENT ME!
     * @see #getSearchStringEscape
     */
    public java.sql.ResultSet getTables(String catalog, String schemaPattern,
        String tableNamePattern, String[] types) throws java.sql.SQLException {
        String databasePart = "";

        if (catalog != null) {
            if (!catalog.equals("")) {
                databasePart = " FROM " + this.quotedId + catalog
                    + this.quotedId;
            }
        } else {
            databasePart = " FROM " + this.quotedId + this.database
                + this.quotedId;
        }

        if (tableNamePattern == null) {
            tableNamePattern = "%";
        }

        Statement stmt = null;
        ResultSet results = null;

        try {
            stmt = this.conn.createStatement();
            results = stmt.executeQuery("show tables " + databasePart
                    + " like '" + tableNamePattern + "'");

            Field[] fields = new Field[5];
            fields[0] = new Field("", "TABLE_CAT", java.sql.Types.VARCHAR,
                    (catalog == null) ? 0 : catalog.length());
            fields[1] = new Field("", "TABLE_SCHEM", java.sql.Types.VARCHAR, 0);
            fields[2] = new Field("", "TABLE_NAME", java.sql.Types.VARCHAR, 255);
            fields[3] = new Field("", "TABLE_TYPE", java.sql.Types.VARCHAR, 5);
            fields[4] = new Field("", "REMARKS", java.sql.Types.VARCHAR, 0);

            ArrayList tuples = new ArrayList();
            byte[][] row = null;

            while (results.next()) {
                String name = results.getString(1);
                row = new byte[5][];
                row[0] = (catalog == null) ? new byte[0] : s2b(catalog);
                row[1] = null;
                row[2] = name.getBytes();
                row[3] = TABLE_AS_BYTES;
                row[4] = new byte[0];
                tuples.add(row);
            }

            java.sql.ResultSet tables = buildResultSet(fields, tuples);

            return tables;
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (Exception ex) {
                    ;
                }

                results = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ex) {
                    ;
                }

                stmt = null;
            }
        }
    }

    /**
     * Get a comma separated list of time and date functions.
     *
     * @return the list
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getTimeDateFunctions() throws java.sql.SQLException {
        return "DAYOFWEEK,WEEKDAY,DAYOFMONTH,DAYOFYEAR,MONTH,DAYNAME,"
        + "MONTHNAME,QUARTER,WEEK,YEAR,HOUR,MINUTE,SECOND,PERIOD_ADD,"
        + "PERIOD_DIFF,TO_DAYS,FROM_DAYS,DATE_FORMAT,TIME_FORMAT,"
        + "CURDATE,CURRENT_DATE,CURTIME,CURRENT_TIME,NOW,SYSDATE,"
        + "CURRENT_TIMESTAMP,UNIX_TIMESTAMP,FROM_UNIXTIME,"
        + "SEC_TO_TIME,TIME_TO_SEC";
    }

    /**
     * Get a description of all the standard SQL types supported by this
     * database. They are ordered by DATA_TYPE and then by how closely the
     * data type maps to the corresponding JDBC SQL type.
     *
     * <P>
     * Each type description has the following columns:
     *
     * <OL>
     * <li>
     * <B>TYPE_NAME</B> String => Type name
     * </li>
     * <li>
     * <B>DATA_TYPE</B> short => SQL data type from java.sql.Types
     * </li>
     * <li>
     * <B>PRECISION</B> int => maximum precision
     * </li>
     * <li>
     * <B>LITERAL_PREFIX</B> String => prefix used to quote a literal (may be
     * null)
     * </li>
     * <li>
     * <B>LITERAL_SUFFIX</B> String => suffix used to quote a literal (may be
     * null)
     * </li>
     * <li>
     * <B>CREATE_PARAMS</B> String => parameters used in creating the type
     * (may be null)
     * </li>
     * <li>
     * <B>NULLABLE</B> short => can you use NULL for this type?
     *
     * <UL>
     * <li>
     * typeNoNulls - does not allow NULL values
     * </li>
     * <li>
     * typeNullable - allows NULL values
     * </li>
     * <li>
     * typeNullableUnknown - nullability unknown
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>CASE_SENSITIVE</B> boolean=> is it case sensitive?
     * </li>
     * <li>
     * <B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
     *
     * <UL>
     * <li>
     * typePredNone - No support
     * </li>
     * <li>
     * typePredChar - Only supported with WHERE .. LIKE
     * </li>
     * <li>
     * typePredBasic - Supported except for WHERE .. LIKE
     * </li>
     * <li>
     * typeSearchable - Supported for all WHERE ..
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned?
     * </li>
     * <li>
     * <B>FIXED_PREC_SCALE</B> boolean => can it be a money value?
     * </li>
     * <li>
     * <B>AUTO_INCREMENT</B> boolean => can it be used for an auto-increment
     * value?
     * </li>
     * <li>
     * <B>LOCAL_TYPE_NAME</B> String => localized version of type name (may be
     * null)
     * </li>
     * <li>
     * <B>MINIMUM_SCALE</B> short => minimum scale supported
     * </li>
     * <li>
     * <B>MAXIMUM_SCALE</B> short => maximum scale supported
     * </li>
     * <li>
     * <B>SQL_DATA_TYPE</B> int => unused
     * </li>
     * <li>
     * <B>SQL_DATETIME_SUB</B> int => unused
     * </li>
     * <li>
     * <B>NUM_PREC_RADIX</B> int => usually 2 or 10
     * </li>
     * </ol>
     * </p>
     *
     * @return ResultSet each row is a SQL type description
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    /**
     * Get a description of all the standard SQL types supported by this
     * database. They are ordered by DATA_TYPE and then by how closely the
     * data type maps to the corresponding JDBC SQL type.
     *
     * <P>
     * Each type description has the following columns:
     *
     * <OL>
     * <li>
     * <B>TYPE_NAME</B> String => Type name
     * </li>
     * <li>
     * <B>DATA_TYPE</B> short => SQL data type from java.sql.Types
     * </li>
     * <li>
     * <B>PRECISION</B> int => maximum precision
     * </li>
     * <li>
     * <B>LITERAL_PREFIX</B> String => prefix used to quote a literal (may be
     * null)
     * </li>
     * <li>
     * <B>LITERAL_SUFFIX</B> String => suffix used to quote a literal (may be
     * null)
     * </li>
     * <li>
     * <B>CREATE_PARAMS</B> String => parameters used in creating the type
     * (may be null)
     * </li>
     * <li>
     * <B>NULLABLE</B> short => can you use NULL for this type?
     *
     * <UL>
     * <li>
     * typeNoNulls - does not allow NULL values
     * </li>
     * <li>
     * typeNullable - allows NULL values
     * </li>
     * <li>
     * typeNullableUnknown - nullability unknown
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>CASE_SENSITIVE</B> boolean=> is it case sensitive?
     * </li>
     * <li>
     * <B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
     *
     * <UL>
     * <li>
     * typePredNone - No support
     * </li>
     * <li>
     * typePredChar - Only supported with WHERE .. LIKE
     * </li>
     * <li>
     * typePredBasic - Supported except for WHERE .. LIKE
     * </li>
     * <li>
     * typeSearchable - Supported for all WHERE ..
     * </li>
     * </ul>
     * </li>
     * <li>
     * <B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned?
     * </li>
     * <li>
     * <B>FIXED_PREC_SCALE</B> boolean => can it be a money value?
     * </li>
     * <li>
     * <B>AUTO_INCREMENT</B> boolean => can it be used for an auto-increment
     * value?
     * </li>
     * <li>
     * <B>LOCAL_TYPE_NAME</B> String => localized version of type name (may be
     * null)
     * </li>
     * <li>
     * <B>MINIMUM_SCALE</B> short => minimum scale supported
     * </li>
     * <li>
     * <B>MAXIMUM_SCALE</B> short => maximum scale supported
     * </li>
     * <li>
     * <B>SQL_DATA_TYPE</B> int => unused
     * </li>
     * <li>
     * <B>SQL_DATETIME_SUB</B> int => unused
     * </li>
     * <li>
     * <B>NUM_PREC_RADIX</B> int => usually 2 or 10
     * </li>
     * </ol>
     * </p>
     *
     * @return ResultSet each row is a SQL type description
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public java.sql.ResultSet getTypeInfo() throws java.sql.SQLException {
        Field[] fields = new Field[18];
        fields[0] = new Field("", "TYPE_NAME", Types.CHAR, 32);
        fields[1] = new Field("", "DATA_TYPE", Types.SMALLINT, 5);
        fields[2] = new Field("", "PRECISION", Types.INTEGER, 10);
        fields[3] = new Field("", "LITERAL_PREFIX", Types.CHAR, 4);
        fields[4] = new Field("", "LITERAL_SUFFIX", Types.CHAR, 4);
        fields[5] = new Field("", "CREATE_PARAMS", Types.CHAR, 32);
        fields[6] = new Field("", "NULLABLE", Types.SMALLINT, 5);
        fields[7] = new Field("", "CASE_SENSITIVE", Types.CHAR, 3);
        fields[8] = new Field("", "SEARCHABLE", Types.SMALLINT, 3);
        fields[9] = new Field("", "UNSIGNED_ATTRIBUTE", Types.CHAR, 3);
        fields[10] = new Field("", "FIXED_PREC_SCALE", Types.CHAR, 3);
        fields[11] = new Field("", "AUTO_INCREMENT", Types.CHAR, 3);
        fields[12] = new Field("", "LOCAL_TYPE_NAME", Types.CHAR, 32);
        fields[13] = new Field("", "MINIMUM_SCALE", Types.SMALLINT, 5);
        fields[14] = new Field("", "MAXIMUM_SCALE", Types.SMALLINT, 5);
        fields[15] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 10);
        fields[16] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 10);
        fields[17] = new Field("", "NUM_PREC_RADIX", Types.INTEGER, 10);

        byte[][] rowVal = null;
        ArrayList tuples = new ArrayList();

        /*
         * The following are ordered by java.sql.Types, and
         * then by how closely the MySQL type matches the
         * JDBC Type (per spec)
         */
        /*
         * MySQL Type: BIT (silently converted to TINYINT(1))
         * JDBC  Type: BIT
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("BIT");
        rowVal[1] = Integer.toString(java.sql.Types.BIT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("1"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("BIT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: BOOL (silently converted to TINYINT(1))
         * JDBC  Type: BIT
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("BOOL");
        rowVal[1] = Integer.toString(java.sql.Types.BIT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("1"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("BOOL"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: TINYINT
         * JDBC  Type: TINYINT
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TINYINT");
        rowVal[1] = Integer.toString(java.sql.Types.TINYINT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("3"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("TINYINT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: BIGINT
         * JDBC  Type: BIGINT
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("BIGINT");
        rowVal[1] = Integer.toString(java.sql.Types.BIGINT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("19"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("BIGINT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: LONG VARBINARY
         * JDBC  Type: LONGVARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("LONG VARBINARY");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("16777215"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("LONG VARBINARY"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: MEDIUMBLOB
         * JDBC  Type: LONGVARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("MEDIUMBLOB");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("16777215"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("MEDIUMBLOB"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: LONGBLOB
         * JDBC  Type: LONGVARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("LONGBLOB");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = Integer.toString(Integer.MAX_VALUE).getBytes();

        // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("LONGBLOB"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: BLOB
         * JDBC  Type: LONGVARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("BLOB");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("65535"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("BLOB"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: TINYBLOB
         * JDBC  Type: LONGVARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TINYBLOB");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("TINYBLOB"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: VARBINARY (sliently converted to VARCHAR(M) BINARY)
         * JDBC  Type: VARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("VARBINARY");
        rowVal[1] = Integer.toString(java.sql.Types.VARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b("(M)"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("VARBINARY"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: BINARY (silently converted to CHAR(M) BINARY)
         * JDBC  Type: BINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("BINARY");
        rowVal[1] = Integer.toString(java.sql.Types.BINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b("(M)"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("BINARY"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: LONG VARCHAR
         * JDBC  Type: LONGVARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("LONG VARCHAR");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("16777215"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("LONG VARCHAR"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: MEDIUMTEXT
         * JDBC  Type: LONGVARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("MEDIUMTEXT");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("16777215"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("MEDIUMTEXT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: LONGTEXT
         * JDBC  Type: LONGVARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("LONGTEXT");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = Integer.toString(Integer.MAX_VALUE).getBytes();

        // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("LONGTEXT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: TEXT
         * JDBC  Type: LONGVARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TEXT");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("65535"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("TEXT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: TINYTEXT
         * JDBC  Type: LONGVARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TINYTEXT");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("TINYTEXT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: CHAR
         * JDBC  Type: CHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("CHAR");
        rowVal[1] = Integer.toString(java.sql.Types.CHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b("(M)"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("CHAR"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: NUMERIC (silently converted to DECIMAL)
         * JDBC  Type: NUMERIC
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("NUMERIC");
        rowVal[1] = Integer.toString(java.sql.Types.NUMERIC).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("17"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M[,D])] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("NUMERIC"); // Locale Type Name
        rowVal[13] = s2b("308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: DECIMAL
         * JDBC  Type: DECIMAL
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DECIMAL");
        rowVal[1] = Integer.toString(java.sql.Types.DECIMAL).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("17"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M[,D])] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("DECIMAL"); // Locale Type Name
        rowVal[13] = s2b("-308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: INTEGER
         * JDBC  Type: INTEGER
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("INTEGER");
        rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("10"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("INTEGER"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: INT
         * JDBC  Type: INTEGER
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("INT");
        rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("10"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("INT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: MEDIUMINT
         * JDBC  Type: INTEGER
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("MEDIUMINT");
        rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("7"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("MEDIUMINT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: SMALLINT
         * JDBC  Type: SMALLINT
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("SMALLINT");
        rowVal[1] = Integer.toString(java.sql.Types.SMALLINT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("5"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("SMALLINT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: DOUBLE
         * JDBC  Type: FLOAT (is really an alias for DOUBLE from JDBC's perspective)
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DOUBLE");
        rowVal[1] = Integer.toString(java.sql.Types.FLOAT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("17"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("DOUBLE"); // Locale Type Name
        rowVal[13] = s2b("-308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: FLOAT
         * JDBC  Type: REAL (this is the SINGLE PERCISION floating point type)
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("FLOAT");
        rowVal[1] = Integer.toString(java.sql.Types.REAL).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("10"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("FLOAT"); // Locale Type Name
        rowVal[13] = s2b("-38"); // Minimum Scale
        rowVal[14] = s2b("38"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: DOUBLE
         * JDBC  Type: DOUBLE
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DOUBLE");
        rowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("17"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("DOUBLE"); // Locale Type Name
        rowVal[13] = s2b("-308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: DOUBLE PRECISION
         * JDBC  Type: DOUBLE
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DOUBLE PRECISION");
        rowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("17"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("DOUBLE PRECISION"); // Locale Type Name
        rowVal[13] = s2b("-308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: REAL (does not map to Types.REAL)
         * JDBC  Type: DOUBLE
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("REAL");
        rowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("17"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("REAL"); // Locale Type Name
        rowVal[13] = s2b("-308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: VARCHAR
         * JDBC  Type: VARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("VARCHAR");
        rowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b("(M)"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("VARCHAR"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: ENUM
         * JDBC  Type: VARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("ENUM");
        rowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("65535"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("ENUM"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: SET
         * JDBC  Type: VARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("SET");
        rowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("64"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("SET"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: DATE
         * JDBC  Type: DATE
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DATE");
        rowVal[1] = Integer.toString(java.sql.Types.DATE).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("0"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("DATE"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: TIME
         * JDBC  Type: TIME
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TIME");
        rowVal[1] = Integer.toString(java.sql.Types.TIME).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("0"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("TIME"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: DATETIME
         * JDBC  Type: TIMESTAMP
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DATETIME");
        rowVal[1] = Integer.toString(java.sql.Types.TIMESTAMP).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("0"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("DATETIME"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        /*
         * MySQL Type: TIMESTAMP
         * JDBC  Type: TIMESTAMP
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TIMESTAMP");
        rowVal[1] = Integer.toString(java.sql.Types.TIMESTAMP).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("0"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b("[(M)]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable)
                           .getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable)
                           .getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("TIMESTAMP"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); //  NUM_PREC_RADIX (2 or 10)
        tuples.add(rowVal);

        return buildResultSet(fields, tuples);
    }

    /**
     * JDBC 2.0 Get a description of the user-defined types defined in a
     * particular schema.  Schema specific UDTs may have type JAVA_OBJECT,
     * STRUCT,  or DISTINCT.
     *
     * <P>
     * Only types matching the catalog, schema, type name and type   criteria
     * are returned.  They are ordered by DATA_TYPE, TYPE_SCHEM  and
     * TYPE_NAME.  The type name parameter may be a fully qualified  name.
     * In this case, the catalog and schemaPattern parameters are ignored.
     * </p>
     *
     * <P>
     * Each type description has the following columns:
     *
     * <OL>
     * <li>
     * <B>TYPE_CAT</B> String => the type's catalog (may be null)
     * </li>
     * <li>
     * <B>TYPE_SCHEM</B> String => type's schema (may be null)
     * </li>
     * <li>
     * <B>TYPE_NAME</B> String => type name
     * </li>
     * <li>
     * <B>CLASS_NAME</B> String => Java class name
     * </li>
     * <li>
     * <B>DATA_TYPE</B> String => type value defined in java.sql.Types.   One
     * of JAVA_OBJECT, STRUCT, or DISTINCT
     * </li>
     * <li>
     * <B>REMARKS</B> String => explanatory comment on the type
     * </li>
     * </ol>
     * </p>
     *
     * <P>
     * <B>Note:</B> If the driver does not support UDTs then an empty result
     * set is returned.
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog;
     *        null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those without
     *        a schema
     * @param typeNamePattern a type name pattern; may be a fully qualified
     *        name
     * @param types a list of user-named types to include (JAVA_OBJECT,
     *        STRUCT, or DISTINCT); null returns all types
     * @return ResultSet - each row is a type description
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.ResultSet getUDTs(String catalog, String schemaPattern,
        String typeNamePattern, int[] types) throws SQLException {
        Field[] fields = new Field[6];
        fields[0] = new Field("", "TYPE_CAT", Types.VARCHAR, 32);
        fields[1] = new Field("", "TYPE_SCHEM", Types.VARCHAR, 32);
        fields[2] = new Field("", "TYPE_NAME", Types.VARCHAR, 32);
        fields[3] = new Field("", "CLASS_NAME", Types.VARCHAR, 32);
        fields[4] = new Field("", "DATA_TYPE", Types.VARCHAR, 32);
        fields[5] = new Field("", "REMARKS", Types.VARCHAR, 32);

        ArrayList tuples = new ArrayList();

        return buildResultSet(fields, tuples);
    }

    /**
     * What's the url for this database?
     *
     * @return the url or null if it can't be generated
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getURL() throws java.sql.SQLException {
        return this.conn.getURL();
    }

    /**
     * What's our user name as known to the database?
     *
     * @return our database user name
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public String getUserName() throws java.sql.SQLException {
        if (this.conn.useHostsInPrivileges()) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = this.conn.createStatement();
                rs = stmt.executeQuery("SELECT USER()");
                rs.next();

                return rs.getString(1);
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (Exception ex) {
                        // ignore
                    }

                    rs = null;
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ex) {
                        // ignore
                    }

                    stmt = null;
                }
            }
        } else {
            return this.conn.getUser();
        }
    }

    /**
     * Get a description of a table's columns that are automatically updated
     * when any value in a row is updated.  They are unordered.
     *
     * <P>
     * Each column description has the following columns:
     *
     * <OL>
     * <li>
     * <B>SCOPE</B> short => is not used
     * </li>
     * <li>
     * <B>COLUMN_NAME</B> String => column name
     * </li>
     * <li>
     * <B>DATA_TYPE</B> short => SQL data type from java.sql.Types
     * </li>
     * <li>
     * <B>TYPE_NAME</B> String => Data source dependent type name
     * </li>
     * <li>
     * <B>COLUMN_SIZE</B> int => precision
     * </li>
     * <li>
     * <B>BUFFER_LENGTH</B> int => length of column value in bytes
     * </li>
     * <li>
     * <B>DECIMAL_DIGITS</B> short  => scale
     * </li>
     * <li>
     * <B>PSEUDO_COLUMN</B> short => is this a pseudo column like an Oracle
     * ROWID
     *
     * <UL>
     * <li>
     * versionColumnUnknown - may or may not be pseudo column
     * </li>
     * <li>
     * versionColumnNotPseudo - is NOT a pseudo column
     * </li>
     * <li>
     * versionColumnPseudo - is a pseudo column
     * </li>
     * </ul>
     * </li>
     * </ol>
     * </p>
     *
     * @param catalog a catalog name; "" retrieves those without a catalog
     * @param schema a schema name; "" retrieves those without a schema
     * @param table a table name
     * @return ResultSet each row is a column description
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public java.sql.ResultSet getVersionColumns(String catalog, String schema,
        String table) throws java.sql.SQLException {
        Field[] fields = new Field[8];
        fields[0] = new Field("", "SCOPE", Types.SMALLINT, 5);
        fields[1] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
        fields[2] = new Field("", "DATA_TYPE", Types.SMALLINT, 5);
        fields[3] = new Field("", "TYPE_NAME", Types.CHAR, 16);
        fields[4] = new Field("", "COLUMN_SIZE", Types.CHAR, 16);
        fields[5] = new Field("", "BUFFER_LENGTH", Types.CHAR, 16);
        fields[6] = new Field("", "DECIMAL_DIGITS", Types.CHAR, 16);
        fields[7] = new Field("", "PSEUDO_COLUMN", Types.SMALLINT, 5);

        return buildResultSet(fields, new ArrayList());

        // do TIMESTAMP columns count?
    }

    /**
     * Can all the procedures returned by getProcedures be called by the
     * current user?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean allProceduresAreCallable() throws java.sql.SQLException {
        return false;
    }

    /**
     * Can all the tables returned by getTable be SELECTed by the current user?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean allTablesAreSelectable() throws java.sql.SQLException {
        return false;
    }

    /**
     * Does a data definition statement within a transaction force the
     * transaction to commit?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean dataDefinitionCausesTransactionCommit()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Is a data definition statement within a transaction ignored?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean dataDefinitionIgnoredInTransactions()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * JDBC 2.0 Determine whether or not a visible row delete can be detected
     * by  calling ResultSet.rowDeleted().  If deletesAreDetected() returns
     * false, then deleted rows are removed from the result set.
     *
     * @param type set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are detected by the resultset type
     * @exception SQLException if a database-access error occurs.
     */
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * Did getMaxRowSize() include LONGVARCHAR and LONGVARBINARY blobs?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean doesMaxRowSizeIncludeBlobs() throws java.sql.SQLException {
        return true;
    }

    /**
     * Extracts foreign key info for one table.
     *
     * @param rows the list of rows to add to
     * @param rs the result set from 'SHOW CREATE TABLE'
     * @param catalog the database name
     *
     * @throws SQLException if a database access error occurs
     * @return the list of rows with new rows added
     */
    public List extractForeignKeyForTable(ArrayList rows,
        java.sql.ResultSet rs, String catalog) throws SQLException {
        byte[][] row = new byte[3][];
        row[0] = rs.getBytes(1);
        row[1] = s2b(SUPPORTS_FK);

        String createTableString = rs.getString(2);
        StringTokenizer lineTokenizer = new StringTokenizer(createTableString,
                "\n");
        StringBuffer commentBuf = new StringBuffer("comment; ");
        boolean firstTime = true;

        while (lineTokenizer.hasMoreTokens()) {
            String line = lineTokenizer.nextToken().trim();

            if (line.startsWith("FOREIGN KEY")) {
                if (line.endsWith(",")) {
                    line = line.substring(0, line.length() - 1);
                }

                // Remove all back-ticks
                int lineLength = line.length();
                StringBuffer lineBuf = new StringBuffer(lineLength);

                for (int i = 0; i < lineLength; i++) {
                    char c = line.charAt(i);

                    if (c != '`') {
                        lineBuf.append(c);
                    }
                }

                line = lineBuf.toString();

                StringTokenizer keyTokens = new StringTokenizer(line, "()",
                        false);
                keyTokens.nextToken(); // eat 'FOREIGN KEY'

                String localColumnNamesString = keyTokens.nextToken();
                String referCatalogTableString = keyTokens.nextToken();
                StringTokenizer referSchemaTable = new StringTokenizer(referCatalogTableString
                        .trim(), " .");
                String referColumnNamesString = keyTokens.nextToken();
                referSchemaTable.nextToken(); //discard the REFERENCES token

                int numTokensLeft = referSchemaTable.countTokens();

                String referCatalog = null;
                String referTable = null;

                if (numTokensLeft == 2) {
                    // some versions of MySQL don't report the database name
                    referCatalog = referSchemaTable.nextToken();
                    referTable = referSchemaTable.nextToken();
                } else {
                    referTable = referSchemaTable.nextToken();
                    referCatalog = catalog;
                }

                if (!firstTime) {
                    commentBuf.append("; ");
                } else {
                    firstTime = false;
                }

                commentBuf.append("(");
                commentBuf.append(localColumnNamesString);
                commentBuf.append(") REFER ");
                commentBuf.append(referCatalog);
                commentBuf.append("/");
                commentBuf.append(referTable);
                commentBuf.append("(");
                commentBuf.append(referColumnNamesString);
                commentBuf.append(")");

                int lastParenIndex = line.lastIndexOf(")");

                if (lastParenIndex != (line.length() - 1)) {
                    String cascadeOptions = cascadeOptions = line.substring(lastParenIndex
                                + 1);
                    commentBuf.append(" ");
                    commentBuf.append(cascadeOptions);
                }
            }
        }

        row[2] = s2b(commentBuf.toString());
        rows.add(row);

        return rows;
    }

    /**
     * Creates a result set similar enough to 'SHOW TABLE STATUS' to allow the
     * same code to work on extracting the foreign key data
     *
     * @param conn the database connection to use
     * @param metadata the DatabaseMetaData instance calling this method
     * @param catalog the database name to extract foreign key info for
     * @param tableName the table to extract foreign key info for
     *
     * @throws SQLException if a database access error occurs.
     * @return A result set that has the structure of 'show table status'
     */
    public ResultSet extractForeignKeyFromCreateTable(
        java.sql.Connection conn, java.sql.DatabaseMetaData metadata,
        String catalog, String tableName) throws SQLException {
        ArrayList tableList = new ArrayList();
        java.sql.ResultSet rs = null;
        java.sql.Statement stmt = null;

        if (tableName != null) {
            tableList.add(tableName);
        } else {
            try {
                rs = metadata.getTables(catalog, "", "%",
                        new String[] { "TABLE" });

                while (rs.next()) {
                    tableList.add(rs.getString("TABLE_NAME"));
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }

                rs = null;
            }
        }

        ArrayList rows = new ArrayList();
        Field[] fields = new Field[3];
        fields[0] = new Field("", "Name", Types.CHAR, Integer.MAX_VALUE);
        fields[1] = new Field("", "Type", Types.CHAR, 255);
        fields[2] = new Field("", "Comment", Types.CHAR, Integer.MAX_VALUE);

        int numTables = tableList.size();

        try {
            for (int i = 0; i < numTables; i++) {
                String tableToExtract = (String) tableList.get(i);
                stmt = conn.createStatement();

                String query = new StringBuffer("SHOW CREATE TABLE ").append(
                        "`").append(catalog).append("`.`").append(tableToExtract)
                                                                     .append("`")
                                                                     .toString();
                rs = stmt.executeQuery(query);

                while (rs.next()) {
                    extractForeignKeyForTable(rows, rs, catalog);
                }
            }
        } finally {
            if (rs != null) {
                rs.close();
            }

            rs = null;

            if (stmt != null) {
                stmt.close();
            }

            stmt = null;
        }

        return buildResultSet(fields, rows);
    }

    /**
     * JDBC 2.0 Determine whether or not a visible row insert can be detected
     * by calling ResultSet.rowInserted().
     *
     * @param type set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are detected by the resultset type
     * @exception SQLException if a database-access error occurs.
     */
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * @see DatabaseMetaData#locatorsUpdateCopy()
     */
    public boolean locatorsUpdateCopy() throws SQLException {
        return true;
    }

    /**
     * Are concatenations between NULL and non-NULL values NULL? A JDBC
     * compliant driver always returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean nullPlusNonNullIsNull() throws java.sql.SQLException {
        return true;
    }

    /**
     * Are NULL values sorted at the end regardless of sort order?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean nullsAreSortedAtEnd() throws java.sql.SQLException {
        return false;
    }

    /**
     * Are NULL values sorted at the start regardless of sort order?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean nullsAreSortedAtStart() throws java.sql.SQLException {
        return false;
    }

    /**
     * Are NULL values sorted high?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean nullsAreSortedHigh() throws java.sql.SQLException {
        return false;
    }

    /**
     * Are NULL values sorted low?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean nullsAreSortedLow() throws java.sql.SQLException {
        return !nullsAreSortedHigh();
    }

    /**
     * DOCUMENT ME!
     *
     * @param type DOCUMENT ME!
     * @return DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * DOCUMENT ME!
     *
     * @param type DOCUMENT ME!
     * @return DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * JDBC 2.0 Determine whether changes made by others are visible.
     *
     * @param type set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are visible for the result set type
     * @exception SQLException if a database-access error occurs.
     */
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * DOCUMENT ME!
     *
     * @param type DOCUMENT ME!
     * @return DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * DOCUMENT ME!
     *
     * @param type DOCUMENT ME!
     * @return DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * JDBC 2.0 Determine whether a result set's own changes visible.
     *
     * @param type set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are visible for the result set type
     * @exception SQLException if a database-access error occurs.
     */
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Does the database store mixed case unquoted SQL identifiers in lower
     * case?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean storesLowerCaseIdentifiers() throws java.sql.SQLException {
        return false;
    }

    /**
     * Does the database store mixed case quoted SQL identifiers in lower
     * case? A JDBC compliant driver will always return false.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean storesLowerCaseQuotedIdentifiers()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Does the database store mixed case unquoted SQL identifiers in mixed
     * case?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean storesMixedCaseIdentifiers() throws java.sql.SQLException {
        return true;
    }

    /**
     * Does the database store mixed case quoted SQL identifiers in mixed
     * case? A JDBC compliant driver will always return false.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean storesMixedCaseQuotedIdentifiers()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Does the database store mixed case unquoted SQL identifiers in upper
     * case?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean storesUpperCaseIdentifiers() throws java.sql.SQLException {
        return false;
    }

    /**
     * Does the database store mixed case quoted SQL identifiers in upper
     * case? A JDBC compliant driver will always return true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean storesUpperCaseQuotedIdentifiers()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Is the ANSI92 entry level SQL grammar supported? All JDBC compliant
     * drivers must return true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsANSI92EntryLevelSQL() throws java.sql.SQLException {
        return true;
    }

    /**
     * Is the ANSI92 full SQL grammar supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsANSI92FullSQL() throws java.sql.SQLException {
        return false;
    }

    /**
     * Is the ANSI92 intermediate SQL grammar supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsANSI92IntermediateSQL() throws java.sql.SQLException {
        return false;
    }

    /**
     * Is "ALTER TABLE" with add column supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsAlterTableWithAddColumn()
        throws java.sql.SQLException {
        return true;
    }

    /**
     * Is "ALTER TABLE" with drop column supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsAlterTableWithDropColumn()
        throws java.sql.SQLException {
        return true;
    }

    /**
     * JDBC 2.0 Return true if the driver supports batch updates, else return
     * false.
     * @return DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    /**
     * Can a catalog name be used in a data manipulation statement?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsCatalogsInDataManipulation()
        throws java.sql.SQLException {
        // Servers before 3.22 could not do this
        return this.conn.getIO().versionMeetsMinimum(3, 3, 22);
    }

    /**
     * Can a catalog name be used in a index definition statement?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsCatalogsInIndexDefinitions()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can a catalog name be used in a privilege definition statement?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsCatalogsInPrivilegeDefinitions()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can a catalog name be used in a procedure call statement?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsCatalogsInProcedureCalls()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can a catalog name be used in a table definition statement?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsCatalogsInTableDefinitions()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Is column aliasing supported?
     *
     * <P>
     * If so, the SQL AS clause can be used to provide names for computed
     * columns or to provide alias names for columns as required. A JDBC
     * compliant driver always returns true.
     * </p>
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsColumnAliasing() throws java.sql.SQLException {
        return true;
    }

    /**
     * Is the CONVERT function between SQL types supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsConvert() throws java.sql.SQLException {
        return false;
    }

    /**
     * Is CONVERT between the given SQL types supported?
     *
     * @param fromType the type to convert from
     * @param toType the type to convert to
     * @return true if so
     * @see Types
     * @throws java.sql.SQLException if an error occurs
     */
    public boolean supportsConvert(int fromType, int toType)
        throws java.sql.SQLException {
        switch (fromType) {
        /* The char/binary types can be converted
         * to pretty much anything.
         */
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.BINARY:
        case java.sql.Types.VARBINARY:
        case java.sql.Types.LONGVARBINARY:

            switch (toType) {
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.REAL:
            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.BIGINT:
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.OTHER:
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
                return true;

            default:
                return false;
            }

        /* We don't handle the BIT type
         * yet.
         */
        case java.sql.Types.BIT:
            return false;

        /* The numeric types. Basically they can convert
         * among themselves, and with char/binary types.
         */
        case java.sql.Types.DECIMAL:
        case java.sql.Types.NUMERIC:
        case java.sql.Types.REAL:
        case java.sql.Types.TINYINT:
        case java.sql.Types.SMALLINT:
        case java.sql.Types.INTEGER:
        case java.sql.Types.BIGINT:
        case java.sql.Types.FLOAT:
        case java.sql.Types.DOUBLE:

            switch (toType) {
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.REAL:
            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
            case java.sql.Types.BIGINT:
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE:
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                return true;

            default:
                return false;
            }

        /* MySQL doesn't support a NULL type. */
        case java.sql.Types.NULL:
            return false;

        /* With this driver, this will always be a serialized
         * object, so the char/binary types will work.
         */
        case java.sql.Types.OTHER:

            switch (toType) {
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                return true;

            default:
                return false;
            }

        /* Dates can be converted to char/binary types. */
        case java.sql.Types.DATE:

            switch (toType) {
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                return true;

            default:
                return false;
            }

        /* Time can be converted to char/binary types */
        case java.sql.Types.TIME:

            switch (toType) {
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                return true;

            default:
                return false;
            }

        /* Timestamp can be converted to char/binary types
         * and date/time types (with loss of precision).
         */
        case java.sql.Types.TIMESTAMP:

            switch (toType) {
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.TIME:
            case java.sql.Types.DATE:
                return true;

            default:
                return false;
            }

        /* We shouldn't get here! */
        default:
            return false; // not sure
        }
    }

    /**
     * Is the ODBC Core SQL grammar supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsCoreSQLGrammar() throws java.sql.SQLException {
        return true;
    }

    /**
     * Are correlated subqueries supported? A JDBC compliant driver always
     * returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsCorrelatedSubqueries() throws java.sql.SQLException {
        return false;
    }

    /**
     * Are both data definition and data manipulation statements within a
     * transaction supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Are only data manipulation statements within a transaction supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsDataManipulationTransactionsOnly()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * If table correlation names are supported, are they restricted to be
     * different from the names of the tables? A JDBC compliant driver always
     * returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsDifferentTableCorrelationNames()
        throws java.sql.SQLException {
        return true;
    }

    /**
     * Are expressions in "ORDER BY" lists supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsExpressionsInOrderBy() throws java.sql.SQLException {
        return true;
    }

    /**
     * Is the ODBC Extended SQL grammar supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsExtendedSQLGrammar() throws java.sql.SQLException {
        return false;
    }

    /**
     * Are full nested outer joins supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsFullOuterJoins() throws java.sql.SQLException {
        return false;
    }

    /**
     * JDBC 3.0
     * @return DOCUMENT ME!
     */
    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    /**
     * Is some form of "GROUP BY" clause supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsGroupBy() throws java.sql.SQLException {
        return true;
    }

    /**
     * Can a "GROUP BY" clause add columns not in the SELECT provided it
     * specifies all the columns in the SELECT?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsGroupByBeyondSelect() throws java.sql.SQLException {
        return true;
    }

    /**
     * Can a "GROUP BY" clause use columns not in the SELECT?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsGroupByUnrelated() throws java.sql.SQLException {
        return false;
    }

    /**
     * Is the SQL Integrity Enhancement Facility supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsIntegrityEnhancementFacility()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Is the escape character in "LIKE" clauses supported? A JDBC compliant
     * driver always returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsLikeEscapeClause() throws java.sql.SQLException {
        return true;
    }

    /**
     * Is there limited support for outer joins?  (This will be true if
     * supportFullOuterJoins is true.)
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsLimitedOuterJoins() throws java.sql.SQLException {
        return true;
    }

    /**
     * Is the ODBC Minimum SQL grammar supported? All JDBC compliant drivers
     * must return true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsMinimumSQLGrammar() throws java.sql.SQLException {
        return true;
    }

    /**
     * Does the database support mixed case unquoted SQL identifiers?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsMixedCaseIdentifiers() throws java.sql.SQLException {
        return false;
    }

    /**
     * Does the database support mixed case quoted SQL identifiers? A JDBC
     * compliant driver will always return true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsMixedCaseQuotedIdentifiers()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * @see DatabaseMetaData#supportsMultipleOpenResults()
     */
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    /**
     * Are multiple ResultSets from a single execute supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsMultipleResultSets() throws java.sql.SQLException {
        return false;
    }

    /**
     * Can we have multiple transactions open at once (on different
     * connections)?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsMultipleTransactions() throws java.sql.SQLException {
        return true;
    }

    /**
     * @see DatabaseMetaData#supportsNamedParameters()
     */
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    /**
     * Can columns be defined as non-nullable? A JDBC compliant driver always
     * returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsNonNullableColumns() throws java.sql.SQLException {
        return true;
    }

    /**
     * Can cursors remain open across commits?
     *
     * @return true if so
     * @see Connection#disableAutoClose
     * @throws java.sql.SQLException if a database access error occurs
     */
    public boolean supportsOpenCursorsAcrossCommit()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can cursors remain open across rollbacks?
     *
     * @return true if so
     * @see Connection#disableAutoClose
     * @throws java.sql.SQLException if an error occurs
     */
    public boolean supportsOpenCursorsAcrossRollback()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can statements remain open across commits?
     *
     * @return true if so
     * @see Connection#disableAutoClose
     * @throws java.sql.SQLException if an error occurs
     */
    public boolean supportsOpenStatementsAcrossCommit()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can statements remain open across rollbacks?
     *
     * @return true if so
     * @see Connection#disableAutoClose
     * @throws java.sql.SQLException if an error occurs
     */
    public boolean supportsOpenStatementsAcrossRollback()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can an "ORDER BY" clause use columns not in the SELECT?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsOrderByUnrelated() throws java.sql.SQLException {
        return false;
    }

    /**
     * Is some form of outer join supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsOuterJoins() throws java.sql.SQLException {
        return true;
    }

    /**
     * Is positioned DELETE supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsPositionedDelete() throws java.sql.SQLException {
        return false;
    }

    /**
     * Is positioned UPDATE supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsPositionedUpdate() throws java.sql.SQLException {
        return false;
    }

    /**
     * JDBC 2.0 Does the database support the concurrency type in combination
     * with the given result set type?
     *
     * @param type defined in java.sql.ResultSet
     * @param concurrency type defined in java.sql.ResultSet
     * @return true if so
     * @exception SQLException if a database-access error occurs.
     * @see Connection
     */
    public boolean supportsResultSetConcurrency(int type, int concurrency)
        throws SQLException {
        return ((type == ResultSet.TYPE_SCROLL_INSENSITIVE)
        && ((concurrency == ResultSet.CONCUR_READ_ONLY)
        || (concurrency == ResultSet.CONCUR_UPDATABLE)));
    }

    /**
     * @see DatabaseMetaData#supportsResultSetHoldability(int)
     */
    public boolean supportsResultSetHoldability(int holdability)
        throws SQLException {
        return (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * JDBC 2.0 Does the database support the given result set type?
     *
     * @param type defined in java.sql.ResultSet
     * @return true if so
     * @exception SQLException if a database-access error occurs.
     * @see Connection
     */
    public boolean supportsResultSetType(int type) throws SQLException {
        return (type == ResultSet.TYPE_SCROLL_INSENSITIVE);
    }

    /**
     * @see DatabaseMetaData#supportsSavepoints()
     */
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    /**
     * Can a schema name be used in a data manipulation statement?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsSchemasInDataManipulation()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can a schema name be used in an index definition statement?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsSchemasInIndexDefinitions()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can a schema name be used in a privilege definition statement?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsSchemasInPrivilegeDefinitions()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can a schema name be used in a procedure call statement?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsSchemasInProcedureCalls()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Can a schema name be used in a table definition statement?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsSchemasInTableDefinitions()
        throws java.sql.SQLException {
        return false;
    }

    /**
     * Is SELECT for UPDATE supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsSelectForUpdate() throws java.sql.SQLException {
        return false;
    }

    /**
     * @see DatabaseMetaData#supportsStatementPooling()
     */
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    /**
     * Are stored procedure calls using the stored procedure escape syntax
     * supported?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsStoredProcedures() throws java.sql.SQLException {
        return false;
    }

    /**
     * Are subqueries in comparison expressions supported? A JDBC compliant
     * driver always returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsSubqueriesInComparisons()
        throws java.sql.SQLException {
        return this.conn.getIO().versionMeetsMinimum(4, 1, 0);
    }

    /**
     * Are subqueries in exists expressions supported? A JDBC compliant driver
     * always returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsSubqueriesInExists() throws java.sql.SQLException {
        return this.conn.getIO().versionMeetsMinimum(4, 1, 0);
    }

    /**
     * Are subqueries in "in" statements supported? A JDBC compliant driver
     * always returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsSubqueriesInIns() throws java.sql.SQLException {
        return this.conn.getIO().versionMeetsMinimum(4, 1, 0);
    }

    /**
     * Are subqueries in quantified expressions supported? A JDBC compliant
     * driver always returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsSubqueriesInQuantifieds()
        throws java.sql.SQLException {
        return this.conn.getIO().versionMeetsMinimum(4, 1, 0);
    }

    /**
     * Are table correlation names supported? A JDBC compliant driver always
     * returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsTableCorrelationNames() throws java.sql.SQLException {
        return true;
    }

    /**
     * Does the database support the given transaction isolation level?
     *
     * @param level the values are defined in java.sql.Connection
     * @return true if so
     * @see Connection
     * @throws java.sql.SQLException if a database access error occurs
     */
    public boolean supportsTransactionIsolationLevel(int level)
        throws java.sql.SQLException {
        if (this.conn.supportsIsolationLevel()) {
            switch (level) {
            case java.sql.Connection.TRANSACTION_READ_COMMITTED:
            case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
            case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
            case java.sql.Connection.TRANSACTION_SERIALIZABLE:
                return true;

            default:
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * Are transactions supported? If not, commit is a noop and the isolation
     * level is TRANSACTION_NONE.
     *
     * @return true if transactions are supported
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsTransactions() throws java.sql.SQLException {
        return this.conn.supportsTransactions();
    }

    /**
     * Is SQL UNION supported? A JDBC compliant driver always returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsUnion() throws java.sql.SQLException {
        return this.conn.getIO().versionMeetsMinimum(4, 0, 0);
    }

    /**
     * Is SQL UNION ALL supported? A JDBC compliant driver always returns true.
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean supportsUnionAll() throws java.sql.SQLException {
        return this.conn.getIO().versionMeetsMinimum(4, 0, 0);
    }

    /**
     * JDBC 2.0 Determine whether or not a visible row update can be detected
     * by  calling ResultSet.rowUpdated().
     *
     * @param type set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are detected by the resultset type
     * @exception SQLException if a database-access error occurs.
     */
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * Does the database use a file for each table?
     *
     * @return true if the database uses a local file for each table
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean usesLocalFilePerTable() throws java.sql.SQLException {
        return false;
    }

    /**
     * Does the database store tables in a local file?
     *
     * @return true if so
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public boolean usesLocalFiles() throws java.sql.SQLException {
        return false;
    }

    /**
     * Parses the cascade option string and returns the DBMD constant
     * that represents it
     *
     * @param commentString the comment from 'SHOW TABLE STATUS'
     * @return the DBMD constant that represents the cascade option
     */
    private int getCascadeDeleteOption(String commentString) {
        int lastParenIndex = commentString.lastIndexOf(")");

        if (lastParenIndex != (commentString.length() - 1)) {
            String cascadeOptions = cascadeOptions = commentString.substring(lastParenIndex
                        + 1).trim();

            if (cascadeOptions.equalsIgnoreCase("ON DELETE CASCADE")) {
                return DatabaseMetaData.importedKeyCascade;
            } else if (cascadeOptions.equalsIgnoreCase("ON DELETE SET NULL")) {
                return DatabaseMetaData.importedKeySetNull;
            }
        }

        return DatabaseMetaData.importedKeySetDefault;
    }

    private void getResultsImpl(String catalog, String table,
        String keysComment, List tuples, String fkTableName, boolean isExport)
        throws SQLException {
        // keys will equal something like this:
        // (parent_service_id child_service_id) REFER ds/subservices(parent_service_id child_service_id)
        // parse of the string into three phases:
        // 1: parse the opening parentheses to determine how many results there will be
        // 2: read in the schema name/table name
        // 3: parse the closing parentheses
        StringTokenizer keyTokens = new StringTokenizer(keysComment.trim(),
                "()", false);
        String localColumnNamesString = keyTokens.nextToken();
        StringTokenizer localColumnNames = new StringTokenizer(localColumnNamesString,
                " ,");
        String referCatalogTableString = keyTokens.nextToken();
        StringTokenizer referSchemaTable = new StringTokenizer(referCatalogTableString,
                " /");
        String referColumnNamesString = keyTokens.nextToken();
        StringTokenizer referColumnNames = new StringTokenizer(referColumnNamesString,
                " ,");
        referSchemaTable.nextToken(); //discard the REFER token

        String referCatalog = referSchemaTable.nextToken();
        String referTable = referSchemaTable.nextToken();

        if (isExport && !referTable.equals(table)) {
            return;
        }

        if (localColumnNames.countTokens() != referColumnNames.countTokens()) {
            throw new SQLException("Error parsing foriegn keys definition",
                "S1000");
        }

        while (localColumnNames.hasMoreTokens()) {
            byte[][] tuple = new byte[14][];
            String localColumnName = localColumnNames.nextToken();
            String referColumnName = referColumnNames.nextToken();
            tuple[FKTABLE_CAT] = ((catalog == null) ? new byte[0] : s2b(catalog));
            tuple[FKTABLE_SCHEM] = null;
            tuple[FKTABLE_NAME] = s2b((isExport) ? fkTableName : table);
            tuple[FKCOLUMN_NAME] = s2b(localColumnName);
            tuple[PKTABLE_CAT] = s2b(referCatalog);
            tuple[PKTABLE_SCHEM] = null;
            tuple[PKTABLE_NAME] = s2b((isExport) ? table : referTable);
            tuple[PKCOLUMN_NAME] = s2b(referColumnName);
            tuple[KEY_SEQ] = s2b(Integer.toString(tuples.size()));
            tuple[UPDATE_RULE] = s2b(Integer.toString(
                        java.sql.DatabaseMetaData.importedKeySetDefault));
            tuple[DELETE_RULE] = s2b(Integer.toString(getCascadeDeleteOption(
                            keysComment)));
            tuple[FK_NAME] = null; //not available from show table status
            tuple[PK_NAME] = null; //not available from show table status
            tuple[DEFERRABILITY] = s2b(Integer.toString(
                        java.sql.DatabaseMetaData.importedKeyNotDeferrable));
            tuples.add(tuple);
        }
    }

    private void getResultsImplFromShowCreate(String catalog, String table,
        String keysComment, List tuples, String fkTableName, boolean isExport)
        throws SQLException {
        // keys will equal something like this:
        // (parent_service_id child_service_id) REFER ds/subservices(parent_service_id child_service_id)
        // parse of the string into three phases:
        // 1: parse the opening parentheses to determine how many results there will be
        // 2: read in the schema name/table name
        // 3: parse the closing parentheses
        StringTokenizer keyTokens = new StringTokenizer(keysComment.trim(),
                "()", false);
        keyTokens.nextToken(); // eat 'FOREIGN KEY'

        String localColumnNamesString = keyTokens.nextToken();
        StringTokenizer localColumnNames = new StringTokenizer(localColumnNamesString,
                ",");
        String referCatalogTableString = keyTokens.nextToken();
        StringTokenizer referSchemaTable = new StringTokenizer(referCatalogTableString
                .trim(), " .");
        String referColumnNamesString = keyTokens.nextToken();
        StringTokenizer referColumnNames = new StringTokenizer(referColumnNamesString,
                ",");
        referSchemaTable.nextToken(); //discard the REFERENCES token

        String referCatalog = referSchemaTable.nextToken();
        String referTable = referSchemaTable.nextToken();
        int lastParenIndex = keysComment.lastIndexOf(")");
        String cascadeOptions = null;

        if (lastParenIndex != (keysComment.length() - 1)) {
            cascadeOptions = keysComment.substring(lastParenIndex + 1);
        }

        if (isExport && !referTable.equals(table)) {
            return;
        }

        if (localColumnNames.countTokens() != referColumnNames.countTokens()) {
            throw new SQLException("Error parsing foriegn keys definition",
                "S1000");
        }

        while (localColumnNames.hasMoreTokens()) {
            byte[][] tuple = new byte[14][];
            String localColumnName = localColumnNames.nextToken();
            String referColumnName = referColumnNames.nextToken();
            tuple[FKTABLE_CAT] = ((catalog == null) ? new byte[0] : s2b(catalog));
            tuple[FKTABLE_SCHEM] = null;
            tuple[FKTABLE_NAME] = s2b((isExport) ? fkTableName : table);
            tuple[FKCOLUMN_NAME] = s2b(localColumnName);
            tuple[PKTABLE_CAT] = s2b(referCatalog);
            tuple[PKTABLE_SCHEM] = null;
            tuple[PKTABLE_NAME] = s2b((isExport) ? table : referTable);
            tuple[PKCOLUMN_NAME] = s2b(referColumnName);
            tuple[KEY_SEQ] = s2b(Integer.toString(tuples.size()));
            tuple[UPDATE_RULE] = s2b(Integer.toString(
                        java.sql.DatabaseMetaData.importedKeySetDefault));

            if (cascadeOptions == null) {
                tuple[DELETE_RULE] = s2b(Integer.toString(
                            java.sql.DatabaseMetaData.importedKeyNoAction));
            } else if (cascadeOptions.equalsIgnoreCase("ON DELETE CASCADE")) {
                tuple[DELETE_RULE] = s2b(Integer.toString(
                            java.sql.DatabaseMetaData.importedKeyCascade));
            } else if (cascadeOptions.equalsIgnoreCase("ON DELETE SET NULL")) {
                tuple[DELETE_RULE] = s2b(Integer.toString(
                            java.sql.DatabaseMetaData.importedKeySetNull));
            } else {
                // shouldn't ever reach here, but it's a fallback in case the cascade  
                // options change. 
                tuple[DELETE_RULE] = s2b(Integer.toString(
                            java.sql.DatabaseMetaData.importedKeyNoAction));
            }

            tuple[FK_NAME] = null; //not available from show table status
            tuple[PK_NAME] = null; //not available from show table status
            tuple[DEFERRABILITY] = s2b(Integer.toString(
                        java.sql.DatabaseMetaData.importedKeyNotDeferrable));
            tuples.add(tuple);
        }
    }

    private String getTableNameWithCase(String table) {
        String tableNameWithCase = (this.conn.lowerCaseTableNames()
            ? table.toLowerCase() : table);

        return tableNameWithCase;
    }

    private java.sql.ResultSet buildResultSet(com.mysql.jdbc.Field[] fields,
        java.util.ArrayList rows) throws SQLException {
        int fieldsLength = fields.length;

        for (int i = 0; i < fieldsLength; i++) {
            fields[i].setConnection(this.conn);
        }

        return new com.mysql.jdbc.ResultSet(this.conn.getCatalog(), fields,
            new RowDataStatic(rows), this.conn);
    }

    /**
     * Converts the given string to bytes, using the connection's character
     * encoding, or if not available, the JVM default encoding.
     * @param s DOCUMENT ME!
     * @return DOCUMENT ME!
     */
    private byte[] s2b(String s) {
        if ((this.conn != null) && this.conn.useUnicode()) {
            try {
                String encoding = this.conn.getEncoding();

                if (encoding == null) {
                    return s.getBytes();
                } else {
                    SingleByteCharsetConverter converter = SingleByteCharsetConverter
                        .getInstance(encoding);

                    if (converter != null) {
                        return converter.toBytes(s);
                    } else {
                        return s.getBytes(encoding);
                    }
                }
            } catch (java.io.UnsupportedEncodingException E) {
                return s.getBytes();
            }
        } else {
            return s.getBytes();
        }
    }
}
