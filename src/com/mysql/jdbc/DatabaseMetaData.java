/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

import static com.mysql.jdbc.DatabaseMetaData.ProcedureType.FUNCTION;
import static com.mysql.jdbc.DatabaseMetaData.ProcedureType.PROCEDURE;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * JDBC Interface to Mysql functions
 * <p>
 * This class provides information about the database as a whole.
 * </p>
 * <p>
 * Many of the methods here return lists of information in ResultSets. You can use the normal ResultSet methods such as getString and getInt to retrieve the
 * data from these ResultSets. If a given form of metadata is not available, these methods show throw a SQLException.
 * </p>
 * <p>
 * Some of these methods take arguments that are String patterns. These methods all have names such as fooPattern. Within a pattern String "%" means match any
 * substring of 0 or more characters and "_" means match any one character.
 * </p>
 */
public class DatabaseMetaData implements java.sql.DatabaseMetaData {

    protected abstract class IteratorWithCleanup<T> {
        abstract void close() throws SQLException;

        abstract boolean hasNext() throws SQLException;

        abstract T next() throws SQLException;
    }

    class LocalAndReferencedColumns {
        String constraintName;

        List<String> localColumnsList;

        String referencedCatalog;

        List<String> referencedColumnsList;

        String referencedTable;

        LocalAndReferencedColumns(List<String> localColumns, List<String> refColumns, String constName, String refCatalog, String refTable) {
            this.localColumnsList = localColumns;
            this.referencedColumnsList = refColumns;
            this.constraintName = constName;
            this.referencedTable = refTable;
            this.referencedCatalog = refCatalog;
        }
    }

    protected class ResultSetIterator extends IteratorWithCleanup<String> {
        int colIndex;

        ResultSet resultSet;

        ResultSetIterator(ResultSet rs, int index) {
            this.resultSet = rs;
            this.colIndex = index;
        }

        @Override
        void close() throws SQLException {
            this.resultSet.close();
        }

        @Override
        boolean hasNext() throws SQLException {
            return this.resultSet.next();
        }

        @Override
        String next() throws SQLException {
            return this.resultSet.getObject(this.colIndex).toString();
        }
    }

    protected class SingleStringIterator extends IteratorWithCleanup<String> {
        boolean onFirst = true;

        String value;

        SingleStringIterator(String s) {
            this.value = s;
        }

        @Override
        void close() throws SQLException {
            // not needed

        }

        @Override
        boolean hasNext() throws SQLException {
            return this.onFirst;
        }

        @Override
        String next() throws SQLException {
            this.onFirst = false;
            return this.value;
        }
    }

    /**
     * Parses and represents common data type information used by various
     * column/parameter methods.
     */
    class TypeDescriptor {
        int bufferLength;

        int charOctetLength;

        Integer columnSize;

        short dataType;

        Integer decimalDigits;

        String isNullable;

        int nullability;

        int numPrecRadix = 10;

        String typeName;

        TypeDescriptor(String typeInfo, String nullabilityInfo) throws SQLException {
            if (typeInfo == null) {
                throw SQLError.createSQLException("NULL typeinfo not supported.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            String mysqlType = "";
            String fullMysqlType = null;

            if (typeInfo.indexOf("(") != -1) {
                mysqlType = typeInfo.substring(0, typeInfo.indexOf("(")).trim();
            } else {
                mysqlType = typeInfo;
            }

            int indexOfUnsignedInMysqlType = StringUtils.indexOfIgnoreCase(mysqlType, "unsigned");

            if (indexOfUnsignedInMysqlType != -1) {
                mysqlType = mysqlType.substring(0, (indexOfUnsignedInMysqlType - 1));
            }

            // Add unsigned to typename reported to enduser as 'native type', if present

            boolean isUnsigned = false;

            if ((StringUtils.indexOfIgnoreCase(typeInfo, "unsigned") != -1) && (StringUtils.indexOfIgnoreCase(typeInfo, "set") != 0)
                    && (StringUtils.indexOfIgnoreCase(typeInfo, "enum") != 0)) {
                fullMysqlType = mysqlType + " unsigned";
                isUnsigned = true;
            } else {
                fullMysqlType = mysqlType;
            }

            if (DatabaseMetaData.this.conn.getCapitalizeTypeNames()) {
                fullMysqlType = fullMysqlType.toUpperCase(Locale.ENGLISH);
            }

            this.dataType = (short) MysqlDefs.mysqlToJavaType(mysqlType);

            this.typeName = fullMysqlType;

            // Figure Out the Size

            if (StringUtils.startsWithIgnoreCase(typeInfo, "enum")) {
                String temp = typeInfo.substring(typeInfo.indexOf("("), typeInfo.lastIndexOf(")"));
                java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(temp, ",");
                int maxLength = 0;

                while (tokenizer.hasMoreTokens()) {
                    maxLength = Math.max(maxLength, (tokenizer.nextToken().length() - 2));
                }

                this.columnSize = Integer.valueOf(maxLength);
                this.decimalDigits = null;
            } else if (StringUtils.startsWithIgnoreCase(typeInfo, "set")) {
                String temp = typeInfo.substring(typeInfo.indexOf("(") + 1, typeInfo.lastIndexOf(")"));
                java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(temp, ",");
                int maxLength = 0;

                int numElements = tokenizer.countTokens();

                if (numElements > 0) {
                    maxLength += (numElements - 1);
                }

                while (tokenizer.hasMoreTokens()) {
                    String setMember = tokenizer.nextToken().trim();

                    if (setMember.startsWith("'") && setMember.endsWith("'")) {
                        maxLength += setMember.length() - 2;
                    } else {
                        maxLength += setMember.length();
                    }
                }

                this.columnSize = Integer.valueOf(maxLength);
                this.decimalDigits = null;
            } else if (typeInfo.indexOf(",") != -1) {
                // Numeric with decimals
                this.columnSize = Integer.valueOf(typeInfo.substring((typeInfo.indexOf("(") + 1), (typeInfo.indexOf(","))).trim());
                this.decimalDigits = Integer.valueOf(typeInfo.substring((typeInfo.indexOf(",") + 1), (typeInfo.indexOf(")"))).trim());
            } else {
                this.columnSize = null;
                this.decimalDigits = null;

                /* If the size is specified with the DDL, use that */
                if ((StringUtils.indexOfIgnoreCase(typeInfo, "char") != -1 || StringUtils.indexOfIgnoreCase(typeInfo, "text") != -1
                        || StringUtils.indexOfIgnoreCase(typeInfo, "blob") != -1 || StringUtils.indexOfIgnoreCase(typeInfo, "binary") != -1 || StringUtils
                        .indexOfIgnoreCase(typeInfo, "bit") != -1) && typeInfo.indexOf("(") != -1) {
                    int endParenIndex = typeInfo.indexOf(")");

                    if (endParenIndex == -1) {
                        endParenIndex = typeInfo.length();
                    }

                    this.columnSize = Integer.valueOf(typeInfo.substring((typeInfo.indexOf("(") + 1), endParenIndex).trim());

                    // Adjust for pseudo-boolean
                    if (DatabaseMetaData.this.conn.getTinyInt1isBit() && this.columnSize.intValue() == 1
                            && StringUtils.startsWithIgnoreCase(typeInfo, 0, "tinyint")) {
                        if (DatabaseMetaData.this.conn.getTransformedBitIsBoolean()) {
                            this.dataType = Types.BOOLEAN;
                            this.typeName = "BOOLEAN";
                        } else {
                            this.dataType = Types.BIT;
                            this.typeName = "BIT";
                        }
                    }
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "tinyint")) {
                    if (DatabaseMetaData.this.conn.getTinyInt1isBit() && typeInfo.indexOf("(1)") != -1) {
                        if (DatabaseMetaData.this.conn.getTransformedBitIsBoolean()) {
                            this.dataType = Types.BOOLEAN;
                            this.typeName = "BOOLEAN";
                        } else {
                            this.dataType = Types.BIT;
                            this.typeName = "BIT";
                        }
                    } else {
                        this.columnSize = Integer.valueOf(3);
                        this.decimalDigits = Integer.valueOf(0);
                    }
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "smallint")) {
                    this.columnSize = Integer.valueOf(5);
                    this.decimalDigits = Integer.valueOf(0);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "mediumint")) {
                    this.columnSize = Integer.valueOf(isUnsigned ? 8 : 7);
                    this.decimalDigits = Integer.valueOf(0);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "int")) {
                    this.columnSize = Integer.valueOf(10);
                    this.decimalDigits = Integer.valueOf(0);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "integer")) {
                    this.columnSize = Integer.valueOf(10);
                    this.decimalDigits = Integer.valueOf(0);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "bigint")) {
                    this.columnSize = Integer.valueOf(isUnsigned ? 20 : 19);
                    this.decimalDigits = Integer.valueOf(0);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "int24")) {
                    this.columnSize = Integer.valueOf(19);
                    this.decimalDigits = Integer.valueOf(0);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "real")) {
                    this.columnSize = Integer.valueOf(12);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "float")) {
                    this.columnSize = Integer.valueOf(12);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "decimal")) {
                    this.columnSize = Integer.valueOf(12);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "numeric")) {
                    this.columnSize = Integer.valueOf(12);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "double")) {
                    this.columnSize = Integer.valueOf(22);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "char")) {
                    this.columnSize = Integer.valueOf(1);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "varchar")) {
                    this.columnSize = Integer.valueOf(255);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "timestamp")) {
                    this.columnSize = Integer.valueOf(19);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "datetime")) {
                    this.columnSize = Integer.valueOf(19);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "date")) {
                    this.columnSize = Integer.valueOf(10);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "time")) {
                    this.columnSize = Integer.valueOf(8);

                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "tinyblob")) {
                    this.columnSize = Integer.valueOf(255);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "blob")) {
                    this.columnSize = Integer.valueOf(65535);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "mediumblob")) {
                    this.columnSize = Integer.valueOf(16777215);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "longblob")) {
                    this.columnSize = Integer.valueOf(Integer.MAX_VALUE);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "tinytext")) {
                    this.columnSize = Integer.valueOf(255);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "text")) {
                    this.columnSize = Integer.valueOf(65535);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "mediumtext")) {
                    this.columnSize = Integer.valueOf(16777215);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "longtext")) {
                    this.columnSize = Integer.valueOf(Integer.MAX_VALUE);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "enum")) {
                    this.columnSize = Integer.valueOf(255);
                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "set")) {
                    this.columnSize = Integer.valueOf(255);
                }

            }

            // BUFFER_LENGTH
            this.bufferLength = MysqlIO.getMaxBuf();

            // NUM_PREC_RADIX (is this right for char?)
            this.numPrecRadix = 10;

            // Nullable?
            if (nullabilityInfo != null) {
                if (nullabilityInfo.equals("YES")) {
                    this.nullability = java.sql.DatabaseMetaData.columnNullable;
                    this.isNullable = "YES";

                } else if (nullabilityInfo.equals("UNKNOWN")) {
                    this.nullability = java.sql.DatabaseMetaData.columnNullableUnknown;
                    this.isNullable = "";

                    // IS_NULLABLE
                } else {
                    this.nullability = java.sql.DatabaseMetaData.columnNoNulls;
                    this.isNullable = "NO";
                }
            } else {
                this.nullability = java.sql.DatabaseMetaData.columnNoNulls;
                this.isNullable = "NO";
            }
        }
    }

    /**
     * Helper class to provide means of comparing indexes by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
     */
    protected class IndexMetaDataKey implements Comparable<IndexMetaDataKey> {
        Boolean columnNonUnique;
        Short columnType;
        String columnIndexName;
        Short columnOrdinalPosition;

        IndexMetaDataKey(boolean columnNonUnique, short columnType, String columnIndexName, short columnOrdinalPosition) {
            this.columnNonUnique = columnNonUnique;
            this.columnType = columnType;
            this.columnIndexName = columnIndexName;
            this.columnOrdinalPosition = columnOrdinalPosition;
        }

        public int compareTo(IndexMetaDataKey indexInfoKey) {
            int compareResult;

            if ((compareResult = this.columnNonUnique.compareTo(indexInfoKey.columnNonUnique)) != 0) {
                return compareResult;
            }
            if ((compareResult = this.columnType.compareTo(indexInfoKey.columnType)) != 0) {
                return compareResult;
            }
            if ((compareResult = this.columnIndexName.compareTo(indexInfoKey.columnIndexName)) != 0) {
                return compareResult;
            }
            return this.columnOrdinalPosition.compareTo(indexInfoKey.columnOrdinalPosition);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (!(obj instanceof IndexMetaDataKey)) {
                return false;
            }
            return compareTo((IndexMetaDataKey) obj) == 0;
        }
    }

    /**
     * Helper class to provide means of comparing tables by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and TABLE_NAME.
     */
    protected class TableMetaDataKey implements Comparable<TableMetaDataKey> {
        String tableType;
        String tableCat;
        String tableSchem;
        String tableName;

        TableMetaDataKey(String tableType, String tableCat, String tableSchem, String tableName) {
            this.tableType = tableType == null ? "" : tableType;
            this.tableCat = tableCat == null ? "" : tableCat;
            this.tableSchem = tableSchem == null ? "" : tableSchem;
            this.tableName = tableName == null ? "" : tableName;
        }

        public int compareTo(TableMetaDataKey tablesKey) {
            int compareResult;

            if ((compareResult = this.tableType.compareTo(tablesKey.tableType)) != 0) {
                return compareResult;
            }
            if ((compareResult = this.tableCat.compareTo(tablesKey.tableCat)) != 0) {
                return compareResult;
            }
            if ((compareResult = this.tableSchem.compareTo(tablesKey.tableSchem)) != 0) {
                return compareResult;
            }
            return this.tableName.compareTo(tablesKey.tableName);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (!(obj instanceof TableMetaDataKey)) {
                return false;
            }
            return compareTo((TableMetaDataKey) obj) == 0;
        }
    }

    /**
     * Helper/wrapper class to provide means of sorting objects by using a sorting key.
     */
    protected class ComparableWrapper<K extends Object & Comparable<? super K>, V> implements Comparable<ComparableWrapper<K, V>> {
        K key;
        V value;

        public ComparableWrapper(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return this.key;
        }

        public V getValue() {
            return this.value;
        }

        public int compareTo(ComparableWrapper<K, V> other) {
            return getKey().compareTo(other.getKey());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (!(obj instanceof ComparableWrapper<?, ?>)) {
                return false;
            }

            Object otherKey = ((ComparableWrapper<?, ?>) obj).getKey();
            return this.key.equals(otherKey);
        }

        @Override
        public String toString() {
            return "{KEY:" + this.key + "; VALUE:" + this.value + "}";
        }
    }

    /**
     * Enumeration for Table Types
     */
    protected enum TableType {
        LOCAL_TEMPORARY("LOCAL TEMPORARY"), SYSTEM_TABLE("SYSTEM TABLE"), SYSTEM_VIEW("SYSTEM VIEW"), TABLE("TABLE", new String[] { "BASE TABLE" }), VIEW(
                "VIEW"), UNKNOWN("UNKNOWN");

        private String name;
        private byte[] nameAsBytes;
        private String[] synonyms;

        TableType(String tableTypeName) {
            this(tableTypeName, null);
        }

        TableType(String tableTypeName, String[] tableTypeSynonyms) {
            this.name = tableTypeName;
            this.nameAsBytes = tableTypeName.getBytes();
            this.synonyms = tableTypeSynonyms;
        }

        String getName() {
            return this.name;
        }

        byte[] asBytes() {
            return this.nameAsBytes;
        }

        boolean equalsTo(String tableTypeName) {
            return this.name.equalsIgnoreCase(tableTypeName);
        }

        static TableType getTableTypeEqualTo(String tableTypeName) {
            for (TableType tableType : TableType.values()) {
                if (tableType.equalsTo(tableTypeName)) {
                    return tableType;
                }
            }
            return UNKNOWN;
        }

        boolean compliesWith(String tableTypeName) {
            if (equalsTo(tableTypeName)) {
                return true;
            }
            if (this.synonyms != null) {
                for (String synonym : this.synonyms) {
                    if (synonym.equalsIgnoreCase(tableTypeName)) {
                        return true;
                    }
                }
            }
            return false;
        }

        static TableType getTableTypeCompliantWith(String tableTypeName) {
            for (TableType tableType : TableType.values()) {
                if (tableType.compliesWith(tableTypeName)) {
                    return tableType;
                }
            }
            return UNKNOWN;
        }
    }

    /**
     * Enumeration for Procedure Types
     */
    protected enum ProcedureType {
        PROCEDURE, FUNCTION;
    }

    protected static final int MAX_IDENTIFIER_LENGTH = 64;

    private static final int DEFERRABILITY = 13;

    private static final int DELETE_RULE = 10;

    private static final int FK_NAME = 11;

    private static final int FKCOLUMN_NAME = 7;

    private static final int FKTABLE_CAT = 4;

    private static final int FKTABLE_NAME = 6;

    private static final int FKTABLE_SCHEM = 5;

    private static final int KEY_SEQ = 8;

    private static final int PK_NAME = 12;

    private static final int PKCOLUMN_NAME = 3;

    //
    // Column indexes used by all DBMD foreign key ResultSets
    //
    private static final int PKTABLE_CAT = 0;

    private static final int PKTABLE_NAME = 2;

    private static final int PKTABLE_SCHEM = 1;

    /** The table type for generic tables that support foreign keys. */
    private static final String SUPPORTS_FK = "SUPPORTS_FK";

    protected static final byte[] TABLE_AS_BYTES = "TABLE".getBytes();

    protected static final byte[] SYSTEM_TABLE_AS_BYTES = "SYSTEM TABLE".getBytes();

    private static final int UPDATE_RULE = 9;

    protected static final byte[] VIEW_AS_BYTES = "VIEW".getBytes();

    private static final Constructor<?> JDBC_4_DBMD_SHOW_CTOR;

    private static final Constructor<?> JDBC_4_DBMD_IS_CTOR;

    static {
        if (Util.isJdbc4()) {
            try {
                JDBC_4_DBMD_SHOW_CTOR = Class.forName("com.mysql.jdbc.JDBC4DatabaseMetaData").getConstructor(
                        new Class[] { com.mysql.jdbc.MySQLConnection.class, String.class });
                JDBC_4_DBMD_IS_CTOR = Class.forName("com.mysql.jdbc.JDBC4DatabaseMetaDataUsingInfoSchema").getConstructor(
                        new Class[] { com.mysql.jdbc.MySQLConnection.class, String.class });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            JDBC_4_DBMD_IS_CTOR = null;
            JDBC_4_DBMD_SHOW_CTOR = null;
        }
    }

    // MySQL reserved words (all versions superset)
    private static final String[] MYSQL_KEYWORDS = new String[] { "ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE",
            "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN",
            "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
            "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE",
            "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL", "EACH", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED",
            "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT", "GENERATED", "GET",
            "GRANT", "GROUP", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER",
            "INOUT", "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERVAL", "INTO", "IO_AFTER_GTIDS",
            "IO_BEFORE_GTIDS", "IS", "ITERATE", "JOIN", "KEY", "KEYS", "KILL", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINEAR", "LINES", "LOAD",
            "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MASTER_BIND", "MASTER_SSL_VERIFY_SERVER_CERT",
            "MATCH", "MAXVALUE", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL",
            "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE", "OPTIMIZER_COSTS", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER",
            "OUTFILE", "PARTITION", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE", "RANGE", "READ", "READS", "READ_WRITE", "REAL", "REFERENCES", "REGEXP",
            "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESIGNAL", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "SCHEMA", "SCHEMAS",
            "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SIGNAL", "SMALLINT", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION",
            "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STORED", "STRAIGHT_JOIN", "TABLE",
            "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED",
            "UPDATE", "USAGE", "USE", "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "VIRTUAL",
            "WHEN", "WHERE", "WHILE", "WITH", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL" };

    // SQL:92 reserved words from 'ANSI X3.135-1992, January 4, 1993'
    private static final String[] SQL92_KEYWORDS = new String[] { "ABSOLUTE", "ACTION", "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "AS", "ASC",
            "ASSERTION", "AT", "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN", "BIT", "BIT_LENGTH", "BOTH", "BY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG",
            "CHAR", "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLUMN", "COMMIT", "CONNECT",
            "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONTINUE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE",
            "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE",
            "DEFERRED", "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR", "DIAGNOSTICS", "DISCONNECT", "DISTINCT", "DOMAIN", "DOUBLE", "DROP", "ELSE", "END",
            "END-EXEC", "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FIRST", "FLOAT", "FOR",
            "FOREIGN", "FOUND", "FROM", "FULL", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GROUP", "HAVING", "HOUR", "IDENTITY", "IMMEDIATE", "IN", "INDICATOR",
            "INITIALLY", "INNER", "INPUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION", "JOIN", "KEY",
            "LANGUAGE", "LAST", "LEADING", "LEFT", "LEVEL", "LIKE", "LOCAL", "LOWER", "MATCH", "MAX", "MIN", "MINUTE", "MODULE", "MONTH", "NAMES", "NATIONAL",
            "NATURAL", "NCHAR", "NEXT", "NO", "NOT", "NULL", "NULLIF", "NUMERIC", "OCTET_LENGTH", "OF", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER", "OUTER",
            "OUTPUT", "OVERLAPS", "PAD", "PARTIAL", "POSITION", "PRECISION", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC",
            "READ", "REAL", "REFERENCES", "RELATIVE", "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK", "ROWS", "SCHEMA", "SCROLL", "SECOND", "SECTION", "SELECT",
            "SESSION", "SESSION_USER", "SET", "SIZE", "SMALLINT", "SOME", "SPACE", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE", "SUBSTRING", "SUM", "SYSTEM_USER",
            "TABLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION", "TRANSLATE", "TRANSLATION",
            "TRIM", "TRUE", "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USAGE", "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARYING", "VIEW", "WHEN",
            "WHENEVER", "WHERE", "WITH", "WORK", "WRITE", "YEAR", "ZONE" };

    // SQL:2003 reserved words from 'ISO/IEC 9075-2:2003 (E), 2003-07-25'
    private static final String[] SQL2003_KEYWORDS = new String[] { "ABS", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ARRAY", "AS", "ASENSITIVE",
            "ASYMMETRIC", "AT", "ATOMIC", "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOOLEAN", "BOTH", "BY", "CALL", "CALLED",
            "CARDINALITY", "CASCADED", "CASE", "CAST", "CEIL", "CEILING", "CHAR", "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOB", "CLOSE",
            "COALESCE", "COLLATE", "COLLECT", "COLUMN", "COMMIT", "CONDITION", "CONNECT", "CONSTRAINT", "CONVERT", "CORR", "CORRESPONDING", "COUNT",
            "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH",
            "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CYCLE", "DATE", "DAY",
            "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DENSE_RANK", "DEREF", "DESCRIBE", "DETERMINISTIC", "DISCONNECT", "DISTINCT",
            "DOUBLE", "DROP", "DYNAMIC", "EACH", "ELEMENT", "ELSE", "END", "END-EXEC", "ESCAPE", "EVERY", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXP",
            "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FILTER", "FLOAT", "FLOOR", "FOR", "FOREIGN", "FREE", "FROM", "FULL", "FUNCTION", "FUSION", "GET",
            "GLOBAL", "GRANT", "GROUP", "GROUPING", "HAVING", "HOLD", "HOUR", "IDENTITY", "IN", "INDICATOR", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT",
            "INTEGER", "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "IS", "JOIN", "LANGUAGE", "LARGE", "LATERAL", "LEADING", "LEFT", "LIKE", "LN", "LOCAL",
            "LOCALTIME", "LOCALTIMESTAMP", "LOWER", "MATCH", "MAX", "MEMBER", "MERGE", "METHOD", "MIN", "MINUTE", "MOD", "MODIFIES", "MODULE", "MONTH",
            "MULTISET", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NO", "NONE", "NORMALIZE", "NOT", "NULL", "NULLIF", "NUMERIC", "OCTET_LENGTH", "OF",
            "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUT", "OUTER", "OVER", "OVERLAPS", "OVERLAY", "PARAMETER", "PARTITION", "PERCENTILE_CONT",
            "PERCENTILE_DISC", "PERCENT_RANK", "POSITION", "POWER", "PRECISION", "PREPARE", "PRIMARY", "PROCEDURE", "RANGE", "RANK", "READS", "REAL",
            "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX",
            "REGR_SXY", "REGR_SYY", "RELEASE", "RESULT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "ROW_NUMBER",
            "SAVEPOINT", "SCOPE", "SCROLL", "SEARCH", "SECOND", "SELECT", "SENSITIVE", "SESSION_USER", "SET", "SIMILAR", "SMALLINT", "SOME", "SPECIFIC",
            "SPECIFICTYPE", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQRT", "START", "STATIC", "STDDEV_POP", "STDDEV_SAMP", "SUBMULTISET",
            "SUBSTRING", "SUM", "SYMMETRIC", "SYSTEM", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE",
            "TO", "TRAILING", "TRANSLATE", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE", "UESCAPE", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE",
            "UPPER", "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARYING", "VAR_POP", "VAR_SAMP", "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW",
            "WITH", "WITHIN", "WITHOUT", "YEAR" };

    private static volatile String mysqlKeywords = null;

    /** The connection to the database */
    protected MySQLConnection conn;

    /** The 'current' database name being used */
    protected String database = null;

    /** What character to use when quoting identifiers */
    protected final String quotedId;

    // We need to provide factory-style methods so we can support both JDBC3 (and older) and JDBC4 runtimes, otherwise the class verifier complains...

    protected static DatabaseMetaData getInstance(MySQLConnection connToSet, String databaseToSet, boolean checkForInfoSchema) throws SQLException {
        if (!Util.isJdbc4()) {
            if (checkForInfoSchema && connToSet != null && connToSet.getUseInformationSchema() && connToSet.versionMeetsMinimum(5, 0, 7)) {
                return new DatabaseMetaDataUsingInfoSchema(connToSet, databaseToSet);
            }

            return new DatabaseMetaData(connToSet, databaseToSet);
        }

        if (checkForInfoSchema && connToSet != null && connToSet.getUseInformationSchema() && connToSet.versionMeetsMinimum(5, 0, 7)) {

            return (DatabaseMetaData) Util.handleNewInstance(JDBC_4_DBMD_IS_CTOR, new Object[] { connToSet, databaseToSet },
                    connToSet.getExceptionInterceptor());
        }

        return (DatabaseMetaData) Util.handleNewInstance(JDBC_4_DBMD_SHOW_CTOR, new Object[] { connToSet, databaseToSet }, connToSet.getExceptionInterceptor());
    }

    /**
     * Creates a new DatabaseMetaData object.
     * 
     * @param connToSet
     * @param databaseToSet
     */
    protected DatabaseMetaData(MySQLConnection connToSet, String databaseToSet) {
        this.conn = connToSet;
        this.database = databaseToSet;
        this.exceptionInterceptor = this.conn.getExceptionInterceptor();

        String identifierQuote = null;
        try {
            identifierQuote = getIdentifierQuoteString();
        } catch (SQLException sqlEx) {
            // Forced by API, never thrown from getIdentifierQuoteString() in this implementation.
            AssertionFailedException.shouldNotHappen(sqlEx);
        } finally {
            this.quotedId = identifierQuote;
        }
    }

    /**
     * Can all the procedures returned by getProcedures be called by the current
     * user?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    /**
     * Can all the tables returned by getTable be SELECTed by the current user?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    private java.sql.ResultSet buildResultSet(com.mysql.jdbc.Field[] fields, java.util.ArrayList<ResultSetRow> rows) throws SQLException {
        return buildResultSet(fields, rows, this.conn);
    }

    static java.sql.ResultSet buildResultSet(com.mysql.jdbc.Field[] fields, java.util.ArrayList<ResultSetRow> rows, MySQLConnection c) throws SQLException {
        int fieldsLength = fields.length;

        for (int i = 0; i < fieldsLength; i++) {
            int jdbcType = fields[i].getSQLType();

            switch (jdbcType) {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    fields[i].setEncoding(c.getCharacterSetMetadata(), c);
                    break;
                default:
                    // do nothing
            }

            fields[i].setConnection(c);
            fields[i].setUseOldNameMetadata(true);
        }

        return com.mysql.jdbc.ResultSetImpl.getInstance(c.getCatalog(), fields, new RowDataStatic(rows), c, null, false);
    }

    protected void convertToJdbcFunctionList(String catalog, ResultSet proceduresRs, boolean needsClientFiltering, String db,
            List<ComparableWrapper<String, ResultSetRow>> procedureRows, int nameIndex, Field[] fields) throws SQLException {
        while (proceduresRs.next()) {
            boolean shouldAdd = true;

            if (needsClientFiltering) {
                shouldAdd = false;

                String procDb = proceduresRs.getString(1);

                if (db == null && procDb == null) {
                    shouldAdd = true;
                } else if (db != null && db.equals(procDb)) {
                    shouldAdd = true;
                }
            }

            if (shouldAdd) {
                String functionName = proceduresRs.getString(nameIndex);

                byte[][] rowData = null;

                if (fields != null && fields.length == 9) {

                    rowData = new byte[9][];
                    rowData[0] = catalog == null ? null : s2b(catalog);         // PROCEDURE_CAT
                    rowData[1] = null;                                          // PROCEDURE_SCHEM
                    rowData[2] = s2b(functionName);                             // PROCEDURE_NAME
                    rowData[3] = null;                                          // reserved1
                    rowData[4] = null;                                          // reserved2
                    rowData[5] = null;                                          // reserved3
                    rowData[6] = s2b(proceduresRs.getString("comment"));        // REMARKS
                    rowData[7] = s2b(Integer.toString(procedureReturnsResult)); // PROCEDURE_TYPE
                    rowData[8] = s2b(functionName);
                } else {

                    rowData = new byte[6][];

                    rowData[0] = catalog == null ? null : s2b(catalog);  // FUNCTION_CAT
                    rowData[1] = null;                                   // FUNCTION_SCHEM
                    rowData[2] = s2b(functionName);                      // FUNCTION_NAME
                    rowData[3] = s2b(proceduresRs.getString("comment")); // REMARKS
                    rowData[4] = s2b(Integer.toString(getJDBC4FunctionNoTableConstant())); // FUNCTION_TYPE
                    rowData[5] = s2b(functionName);                      // SPECFIC NAME
                }

                procedureRows.add(new ComparableWrapper<String, ResultSetRow>(getFullyQualifiedName(catalog, functionName), new ByteArrayRow(rowData,
                        getExceptionInterceptor())));
            }
        }
    }

    /**
     * Builds and returns a fully qualified name, quoted if necessary, for the given catalog and database entity.
     */
    protected String getFullyQualifiedName(String catalog, String entity) {
        StringBuilder fullyQualifiedName = new StringBuilder(
                StringUtils.quoteIdentifier(catalog == null ? "" : catalog, this.quotedId, this.conn.getPedantic()));
        fullyQualifiedName.append('.');
        fullyQualifiedName.append(StringUtils.quoteIdentifier(entity, this.quotedId, this.conn.getPedantic()));
        return fullyQualifiedName.toString();
    }

    /**
     * Getter to JDBC4 DatabaseMetaData.functionNoTable constant.
     * This method must be overridden by JDBC4 subclasses. This implementation should never be called.
     * 
     * @return 0
     */
    protected int getJDBC4FunctionNoTableConstant() {
        return 0;
    }

    protected void convertToJdbcProcedureList(boolean fromSelect, String catalog, ResultSet proceduresRs, boolean needsClientFiltering, String db,
            List<ComparableWrapper<String, ResultSetRow>> procedureRows, int nameIndex) throws SQLException {
        while (proceduresRs.next()) {
            boolean shouldAdd = true;

            if (needsClientFiltering) {
                shouldAdd = false;

                String procDb = proceduresRs.getString(1);

                if (db == null && procDb == null) {
                    shouldAdd = true;
                } else if (db != null && db.equals(procDb)) {
                    shouldAdd = true;
                }
            }

            if (shouldAdd) {
                String procedureName = proceduresRs.getString(nameIndex);
                byte[][] rowData = new byte[9][];
                rowData[0] = catalog == null ? null : s2b(catalog);
                rowData[1] = null;
                rowData[2] = s2b(procedureName);
                rowData[3] = null;
                rowData[4] = null;
                rowData[5] = null;
                rowData[6] = s2b(proceduresRs.getString("comment"));

                boolean isFunction = fromSelect ? "FUNCTION".equalsIgnoreCase(proceduresRs.getString("type")) : false;
                rowData[7] = s2b(isFunction ? Integer.toString(procedureReturnsResult) : Integer.toString(procedureNoResult));

                rowData[8] = s2b(procedureName);

                procedureRows.add(new ComparableWrapper<String, ResultSetRow>(getFullyQualifiedName(catalog, procedureName), new ByteArrayRow(rowData,
                        getExceptionInterceptor())));
            }
        }
    }

    private ResultSetRow convertTypeDescriptorToProcedureRow(byte[] procNameAsBytes, byte[] procCatAsBytes, String paramName, boolean isOutParam,
            boolean isInParam, boolean isReturnParam, TypeDescriptor typeDesc, boolean forGetFunctionColumns, int ordinal) throws SQLException {
        byte[][] row = forGetFunctionColumns ? new byte[17][] : new byte[20][];
        row[0] = procCatAsBytes; // PROCEDURE_CAT
        row[1] = null; // PROCEDURE_SCHEM
        row[2] = procNameAsBytes; // PROCEDURE/NAME
        row[3] = s2b(paramName); // COLUMN_NAME
        row[4] = s2b(String.valueOf(getColumnType(isOutParam, isInParam, isReturnParam, forGetFunctionColumns))); // COLUMN_TYPE
        row[5] = s2b(Short.toString(typeDesc.dataType)); // DATA_TYPE
        row[6] = s2b(typeDesc.typeName); // TYPE_NAME
        row[7] = typeDesc.columnSize == null ? null : s2b(typeDesc.columnSize.toString()); // PRECISION
        row[8] = row[7]; // LENGTH
        row[9] = typeDesc.decimalDigits == null ? null : s2b(typeDesc.decimalDigits.toString()); // SCALE
        row[10] = s2b(Integer.toString(typeDesc.numPrecRadix)); // RADIX
        // Map 'column****' to 'procedure****'
        switch (typeDesc.nullability) {
            case columnNoNulls:
                row[11] = s2b(String.valueOf(procedureNoNulls)); // NULLABLE
                break;

            case columnNullable:
                row[11] = s2b(String.valueOf(procedureNullable)); // NULLABLE
                break;

            case columnNullableUnknown:
                row[11] = s2b(String.valueOf(procedureNullableUnknown)); // NULLABLE
                break;

            default:
                throw SQLError.createSQLException("Internal error while parsing callable statement metadata (unknown nullability value fount)",
                        SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        row[12] = null;

        if (forGetFunctionColumns) {
            // CHAR_OCTECT_LENGTH
            row[13] = null;

            // ORDINAL_POSITION
            row[14] = s2b(String.valueOf(ordinal));

            // IS_NULLABLE
            row[15] = s2b(typeDesc.isNullable);

            // SPECIFIC_NAME
            row[16] = procNameAsBytes;
        } else {
            // COLUMN_DEF
            row[13] = null;

            // SQL_DATA_TYPE (future use)
            row[14] = null;

            // SQL_DATETIME_SUB (future use)
            row[15] = null;

            // CHAR_OCTET_LENGTH
            row[16] = null;

            // ORDINAL_POSITION
            row[17] = s2b(String.valueOf(ordinal));

            // IS_NULLABLE
            row[18] = s2b(typeDesc.isNullable);

            // SPECIFIC_NAME
            row[19] = procNameAsBytes;
        }

        return new ByteArrayRow(row, getExceptionInterceptor());
    }

    /**
     * Determines the COLUMN_TYPE information based on parameter type (IN, OUT or INOUT) or function return parameter.
     * 
     * @param isOutParam
     *            Indicates whether it's an output parameter.
     * @param isInParam
     *            Indicates whether it's an input parameter.
     * @param isReturnParam
     *            Indicates whether it's a function return parameter.
     * @param forGetFunctionColumns
     *            Indicates whether the column belong to a function. This argument is required for JDBC4, in which case
     *            this method must be overridden to provide the correct functionality.
     * 
     * @return The corresponding COLUMN_TYPE as in java.sql.getProcedureColumns API.
     */
    protected int getColumnType(boolean isOutParam, boolean isInParam, boolean isReturnParam, boolean forGetFunctionColumns) {
        if (isInParam && isOutParam) {
            return procedureColumnInOut;
        } else if (isInParam) {
            return procedureColumnIn;
        } else if (isOutParam) {
            return procedureColumnOut;
        } else if (isReturnParam) {
            return procedureColumnReturn;
        } else {
            return procedureColumnUnknown;
        }
    }

    private ExceptionInterceptor exceptionInterceptor;

    protected ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    /**
     * Does a data definition statement within a transaction force the
     * transaction to commit?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return true;
    }

    /**
     * Is a data definition statement within a transaction ignored?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    /**
     * JDBC 2.0 Determine whether or not a visible row delete can be detected by
     * calling ResultSet.rowDeleted(). If deletesAreDetected() returns false,
     * then deleted rows are removed from the result set.
     * 
     * @param type
     *            set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are detected by the resultset type
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    // ----------------------------------------------------------------------

    /**
     * Did getMaxRowSize() include LONGVARCHAR and LONGVARBINARY blobs?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    /**
     * Extracts foreign key info for one table.
     * 
     * @param rows
     *            the list of rows to add to
     * @param rs
     *            the result set from 'SHOW CREATE TABLE'
     * @param catalog
     *            the database name
     * @return the list of rows with new rows added
     * @throws SQLException
     *             if a database access error occurs
     */
    public List<ResultSetRow> extractForeignKeyForTable(ArrayList<ResultSetRow> rows, java.sql.ResultSet rs, String catalog) throws SQLException {
        byte[][] row = new byte[3][];
        row[0] = rs.getBytes(1);
        row[1] = s2b(SUPPORTS_FK);

        String createTableString = rs.getString(2);
        StringTokenizer lineTokenizer = new StringTokenizer(createTableString, "\n");
        StringBuilder commentBuf = new StringBuilder("comment; ");
        boolean firstTime = true;

        while (lineTokenizer.hasMoreTokens()) {
            String line = lineTokenizer.nextToken().trim();

            String constraintName = null;

            if (StringUtils.startsWithIgnoreCase(line, "CONSTRAINT")) {
                boolean usingBackTicks = true;
                int beginPos = StringUtils.indexOfQuoteDoubleAware(line, this.quotedId, 0);

                if (beginPos == -1) {
                    beginPos = line.indexOf("\"");
                    usingBackTicks = false;
                }

                if (beginPos != -1) {
                    int endPos = -1;

                    if (usingBackTicks) {
                        endPos = StringUtils.indexOfQuoteDoubleAware(line, this.quotedId, beginPos + 1);
                    } else {
                        endPos = StringUtils.indexOfQuoteDoubleAware(line, "\"", beginPos + 1);
                    }

                    if (endPos != -1) {
                        constraintName = line.substring(beginPos + 1, endPos);
                        line = line.substring(endPos + 1, line.length()).trim();
                    }
                }
            }

            if (line.startsWith("FOREIGN KEY")) {
                if (line.endsWith(",")) {
                    line = line.substring(0, line.length() - 1);
                }

                int indexOfFK = line.indexOf("FOREIGN KEY");

                String localColumnName = null;
                String referencedCatalogName = StringUtils.quoteIdentifier(catalog, this.quotedId, this.conn.getPedantic());
                String referencedTableName = null;
                String referencedColumnName = null;

                if (indexOfFK != -1) {
                    int afterFk = indexOfFK + "FOREIGN KEY".length();

                    int indexOfRef = StringUtils.indexOfIgnoreCase(afterFk, line, "REFERENCES", this.quotedId, this.quotedId, StringUtils.SEARCH_MODE__ALL);

                    if (indexOfRef != -1) {

                        int indexOfParenOpen = line.indexOf('(', afterFk);
                        int indexOfParenClose = StringUtils.indexOfIgnoreCase(indexOfParenOpen, line, ")", this.quotedId, this.quotedId,
                                StringUtils.SEARCH_MODE__ALL);

                        if (indexOfParenOpen == -1 || indexOfParenClose == -1) {
                            // throw SQLError.createSQLException();
                        }

                        localColumnName = line.substring(indexOfParenOpen + 1, indexOfParenClose);

                        int afterRef = indexOfRef + "REFERENCES".length();

                        int referencedColumnBegin = StringUtils.indexOfIgnoreCase(afterRef, line, "(", this.quotedId, this.quotedId,
                                StringUtils.SEARCH_MODE__ALL);

                        if (referencedColumnBegin != -1) {
                            referencedTableName = line.substring(afterRef, referencedColumnBegin);

                            int referencedColumnEnd = StringUtils.indexOfIgnoreCase(referencedColumnBegin + 1, line, ")", this.quotedId, this.quotedId,
                                    StringUtils.SEARCH_MODE__ALL);

                            if (referencedColumnEnd != -1) {
                                referencedColumnName = line.substring(referencedColumnBegin + 1, referencedColumnEnd);
                            }

                            int indexOfCatalogSep = StringUtils.indexOfIgnoreCase(0, referencedTableName, ".", this.quotedId, this.quotedId,
                                    StringUtils.SEARCH_MODE__ALL);

                            if (indexOfCatalogSep != -1) {
                                referencedCatalogName = referencedTableName.substring(0, indexOfCatalogSep);
                                referencedTableName = referencedTableName.substring(indexOfCatalogSep + 1);
                            }
                        }
                    }
                }

                if (!firstTime) {
                    commentBuf.append("; ");
                } else {
                    firstTime = false;
                }

                if (constraintName != null) {
                    commentBuf.append(constraintName);
                } else {
                    commentBuf.append("not_available");
                }

                commentBuf.append("(");
                commentBuf.append(localColumnName);
                commentBuf.append(") REFER ");
                commentBuf.append(referencedCatalogName);
                commentBuf.append("/");
                commentBuf.append(referencedTableName);
                commentBuf.append("(");
                commentBuf.append(referencedColumnName);
                commentBuf.append(")");

                int lastParenIndex = line.lastIndexOf(")");

                if (lastParenIndex != (line.length() - 1)) {
                    String cascadeOptions = line.substring(lastParenIndex + 1);
                    commentBuf.append(" ");
                    commentBuf.append(cascadeOptions);
                }
            }
        }

        row[2] = s2b(commentBuf.toString());
        rows.add(new ByteArrayRow(row, getExceptionInterceptor()));

        return rows;
    }

    /**
     * Creates a result set similar enough to 'SHOW TABLE STATUS' to allow the
     * same code to work on extracting the foreign key data
     * 
     * @param connToUse
     *            the database connection to use
     * @param metadata
     *            the DatabaseMetaData instance calling this method
     * @param catalog
     *            the database name to extract foreign key info for
     * @param tableName
     *            the table to extract foreign key info for
     * @return A result set that has the structure of 'show table status'
     * @throws SQLException
     *             if a database access error occurs.
     */
    public ResultSet extractForeignKeyFromCreateTable(String catalog, String tableName) throws SQLException {
        ArrayList<String> tableList = new ArrayList<String>();
        java.sql.ResultSet rs = null;
        java.sql.Statement stmt = null;

        if (tableName != null) {
            tableList.add(tableName);
        } else {
            try {
                rs = getTables(catalog, "", "%", new String[] { "TABLE" });

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

        ArrayList<ResultSetRow> rows = new ArrayList<ResultSetRow>();
        Field[] fields = new Field[3];
        fields[0] = new Field("", "Name", Types.CHAR, Integer.MAX_VALUE);
        fields[1] = new Field("", "Type", Types.CHAR, 255);
        fields[2] = new Field("", "Comment", Types.CHAR, Integer.MAX_VALUE);

        int numTables = tableList.size();
        stmt = this.conn.getMetadataSafeStatement();

        try {
            for (int i = 0; i < numTables; i++) {
                String tableToExtract = tableList.get(i);

                String query = new StringBuilder("SHOW CREATE TABLE ").append(getFullyQualifiedName(catalog, tableToExtract)).toString();

                try {
                    rs = stmt.executeQuery(query);
                } catch (SQLException sqlEx) {
                    // Table might've disappeared on us, not really an error
                    String sqlState = sqlEx.getSQLState();

                    if (!"42S02".equals(sqlState) && sqlEx.getErrorCode() != MysqlErrorNumbers.ER_NO_SUCH_TABLE) {
                        throw sqlEx;
                    }

                    continue;
                }

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
     * @see DatabaseMetaData#getAttributes(String, String, String, String)
     */
    public java.sql.ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3) throws SQLException {
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

        return buildResultSet(fields, new ArrayList<ResultSetRow>());
    }

    /**
     * Get a description of a table's optimal set of columns that uniquely
     * identifies a row. They are ordered by SCOPE.
     * <P>
     * Each column description has the following columns:
     * <OL>
     * <li><B>SCOPE</B> short => actual scope of result
     * <UL>
     * <li>bestRowTemporary - very temporary, while using row</li>
     * <li>bestRowTransaction - valid for remainder of current transaction</li>
     * <li>bestRowSession - valid for remainder of current session</li>
     * </ul>
     * </li>
     * <li><B>COLUMN_NAME</B> String => column name</li>
     * <li><B>DATA_TYPE</B> short => SQL data type from java.sql.Types</li>
     * <li><B>TYPE_NAME</B> String => Data source dependent type name</li>
     * <li><B>COLUMN_SIZE</B> int => precision</li>
     * <li><B>BUFFER_LENGTH</B> int => not used</li>
     * <li><B>DECIMAL_DIGITS</B> short => scale</li>
     * <li><B>PSEUDO_COLUMN</B> short => is this a pseudo column like an Oracle ROWID
     * <UL>
     * <li>bestRowUnknown - may or may not be pseudo column</li>
     * <li>bestRowNotPseudo - is NOT a pseudo column</li>
     * <li>bestRowPseudo - is a pseudo column</li>
     * </ul>
     * </li>
     * </ol>
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schema
     *            a schema name; "" retrieves those without a schema
     * @param table
     *            a table name
     * @param scope
     *            the scope of interest; use same values as SCOPE
     * @param nullable
     *            include columns that are nullable?
     * @return ResultSet each row is a column description
     * @throws SQLException
     */
    public java.sql.ResultSet getBestRowIdentifier(String catalog, String schema, final String table, int scope, boolean nullable) throws SQLException {
        if (table == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        Field[] fields = new Field[8];
        fields[0] = new Field("", "SCOPE", Types.SMALLINT, 5);
        fields[1] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
        fields[2] = new Field("", "DATA_TYPE", Types.INTEGER, 32);
        fields[3] = new Field("", "TYPE_NAME", Types.CHAR, 32);
        fields[4] = new Field("", "COLUMN_SIZE", Types.INTEGER, 10);
        fields[5] = new Field("", "BUFFER_LENGTH", Types.INTEGER, 10);
        fields[6] = new Field("", "DECIMAL_DIGITS", Types.SMALLINT, 10);
        fields[7] = new Field("", "PSEUDO_COLUMN", Types.SMALLINT, 5);

        final ArrayList<ResultSetRow> rows = new ArrayList<ResultSetRow>();
        final Statement stmt = this.conn.getMetadataSafeStatement();

        try {

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {
                    ResultSet results = null;

                    try {
                        StringBuilder queryBuf = new StringBuilder("SHOW COLUMNS FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(table, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic()));
                        queryBuf.append(" FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic()));

                        results = stmt.executeQuery(queryBuf.toString());

                        while (results.next()) {
                            String keyType = results.getString("Key");

                            if (keyType != null) {
                                if (StringUtils.startsWithIgnoreCase(keyType, "PRI")) {
                                    byte[][] rowVal = new byte[8][];
                                    rowVal[0] = Integer.toString(java.sql.DatabaseMetaData.bestRowSession).getBytes();
                                    rowVal[1] = results.getBytes("Field");

                                    String type = results.getString("Type");
                                    int size = MysqlIO.getMaxBuf();
                                    int decimals = 0;

                                    /*
                                     * Parse the Type column from MySQL
                                     */
                                    if (type.indexOf("enum") != -1) {
                                        String temp = type.substring(type.indexOf("("), type.indexOf(")"));
                                        java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(temp, ",");
                                        int maxLength = 0;

                                        while (tokenizer.hasMoreTokens()) {
                                            maxLength = Math.max(maxLength, (tokenizer.nextToken().length() - 2));
                                        }

                                        size = maxLength;
                                        decimals = 0;
                                        type = "enum";
                                    } else if (type.indexOf("(") != -1) {
                                        if (type.indexOf(",") != -1) {
                                            size = Integer.parseInt(type.substring(type.indexOf("(") + 1, type.indexOf(",")));
                                            decimals = Integer.parseInt(type.substring(type.indexOf(",") + 1, type.indexOf(")")));
                                        } else {
                                            size = Integer.parseInt(type.substring(type.indexOf("(") + 1, type.indexOf(")")));
                                        }

                                        type = type.substring(0, type.indexOf("("));
                                    }

                                    rowVal[2] = s2b(String.valueOf(MysqlDefs.mysqlToJavaType(type)));
                                    rowVal[3] = s2b(type);
                                    rowVal[4] = Integer.toString(size + decimals).getBytes();
                                    rowVal[5] = Integer.toString(size + decimals).getBytes();
                                    rowVal[6] = Integer.toString(decimals).getBytes();
                                    rowVal[7] = Integer.toString(java.sql.DatabaseMetaData.bestRowNotPseudo).getBytes();

                                    rows.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
                                }
                            }
                        }
                    } catch (SQLException sqlEx) {
                        if (!SQLError.SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND.equals(sqlEx.getSQLState())) {
                            throw sqlEx;
                        }
                    } finally {
                        if (results != null) {
                            try {
                                results.close();
                            } catch (Exception ex) {
                            }

                            results = null;
                        }
                    }
                }
            }.doForAll();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        java.sql.ResultSet results = buildResultSet(fields, rows);

        return results;

    }

    /*
     * Extract parameter details for Procedures and Functions by parsing the DDL query obtained from SHOW CREATE [PROCEDURE|FUNCTION] ... statements.
     * The result rows returned follow the required structure for getProcedureColumns() and getFunctionColumns() methods.
     * 
     * Internal use only.
     */
    private void getCallStmtParameterTypes(String catalog, String quotedProcName, ProcedureType procType, String parameterNamePattern,
            List<ResultSetRow> resultRows, boolean forGetFunctionColumns) throws SQLException {
        java.sql.Statement paramRetrievalStmt = null;
        java.sql.ResultSet paramRetrievalRs = null;

        if (parameterNamePattern == null) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                parameterNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Parameter/Column name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        String parameterDef = null;

        byte[] procNameAsBytes = null;
        byte[] procCatAsBytes = null;

        boolean isProcedureInAnsiMode = false;
        String storageDefnDelims = null;
        String storageDefnClosures = null;

        try {
            paramRetrievalStmt = this.conn.getMetadataSafeStatement();

            String oldCatalog = this.conn.getCatalog();
            if (this.conn.lowerCaseTableNames() && catalog != null && catalog.length() != 0 && oldCatalog != null && oldCatalog.length() != 0) {
                // Workaround for bug in server wrt. to  SHOW CREATE PROCEDURE not respecting lower-case table names

                ResultSet rs = null;

                try {
                    this.conn.setCatalog(StringUtils.unQuoteIdentifier(catalog, this.quotedId));
                    rs = paramRetrievalStmt.executeQuery("SELECT DATABASE()");
                    rs.next();

                    catalog = rs.getString(1);

                } finally {

                    this.conn.setCatalog(oldCatalog);

                    if (rs != null) {
                        rs.close();
                    }
                }
            }

            if (paramRetrievalStmt.getMaxRows() != 0) {
                paramRetrievalStmt.setMaxRows(0);
            }

            int dotIndex = -1;

            if (!" ".equals(this.quotedId)) {
                dotIndex = StringUtils.indexOfIgnoreCase(0, quotedProcName, ".", this.quotedId, this.quotedId,
                        this.conn.isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);
            } else {
                dotIndex = quotedProcName.indexOf(".");
            }

            String dbName = null;

            if (dotIndex != -1 && (dotIndex + 1) < quotedProcName.length()) {
                dbName = quotedProcName.substring(0, dotIndex);
                quotedProcName = quotedProcName.substring(dotIndex + 1);
            } else {
                dbName = StringUtils.quoteIdentifier(catalog, this.quotedId, this.conn.getPedantic());
            }

            // Moved from above so that procName is *without* database as expected by the rest of code
            // Removing QuoteChar to get output as it was before PROC_CAT fixes
            String tmpProcName = StringUtils.unQuoteIdentifier(quotedProcName, this.quotedId);
            try {
                procNameAsBytes = StringUtils.getBytes(tmpProcName, "UTF-8");
            } catch (UnsupportedEncodingException ueEx) {
                procNameAsBytes = s2b(tmpProcName);

                // Set all fields to connection encoding
            }

            tmpProcName = StringUtils.unQuoteIdentifier(dbName, this.quotedId);
            try {
                procCatAsBytes = StringUtils.getBytes(tmpProcName, "UTF-8");
            } catch (UnsupportedEncodingException ueEx) {
                procCatAsBytes = s2b(tmpProcName);

                // Set all fields to connection encoding
            }

            // there is no need to quote the identifier here since 'dbName' and 'procName' are guaranteed to be already quoted.
            StringBuilder procNameBuf = new StringBuilder();
            procNameBuf.append(dbName);
            procNameBuf.append('.');
            procNameBuf.append(quotedProcName);

            String fieldName = null;
            if (procType == PROCEDURE) {
                paramRetrievalRs = paramRetrievalStmt.executeQuery("SHOW CREATE PROCEDURE " + procNameBuf.toString());
                fieldName = "Create Procedure";
            } else {
                paramRetrievalRs = paramRetrievalStmt.executeQuery("SHOW CREATE FUNCTION " + procNameBuf.toString());
                fieldName = "Create Function";
            }

            if (paramRetrievalRs.next()) {
                String procedureDef = paramRetrievalRs.getString(fieldName);

                if (!this.conn.getNoAccessToProcedureBodies() && (procedureDef == null || procedureDef.length() == 0)) {
                    throw SQLError.createSQLException("User does not have access to metadata required to determine "
                            + "stored procedure parameter types. If rights can not be granted, configure connection with \"noAccessToProcedureBodies=true\" "
                            + "to have driver generate parameters that represent INOUT strings irregardless of actual parameter types.",
                            SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                }

                try {
                    String sqlMode = paramRetrievalRs.getString("sql_mode");

                    if (StringUtils.indexOfIgnoreCase(sqlMode, "ANSI") != -1) {
                        isProcedureInAnsiMode = true;
                    }
                } catch (SQLException sqlEx) {
                    // doesn't exist
                }

                String identifierMarkers = isProcedureInAnsiMode ? "`\"" : "`";
                String identifierAndStringMarkers = "'" + identifierMarkers;
                storageDefnDelims = "(" + identifierMarkers;
                storageDefnClosures = ")" + identifierMarkers;

                if (procedureDef != null && procedureDef.length() != 0) {
                    // sanitize/normalize by stripping out comments
                    procedureDef = StringUtils.stripComments(procedureDef, identifierAndStringMarkers, identifierAndStringMarkers, true, false, true, true);

                    int openParenIndex = StringUtils.indexOfIgnoreCase(0, procedureDef, "(", this.quotedId, this.quotedId,
                            this.conn.isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);
                    int endOfParamDeclarationIndex = 0;

                    endOfParamDeclarationIndex = endPositionOfParameterDeclaration(openParenIndex, procedureDef, this.quotedId);

                    if (procType == FUNCTION) {

                        // Grab the return column since it needs
                        // to go first in the output result set
                        int returnsIndex = StringUtils.indexOfIgnoreCase(0, procedureDef, " RETURNS ", this.quotedId, this.quotedId,
                                this.conn.isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);

                        int endReturnsDef = findEndOfReturnsClause(procedureDef, returnsIndex);

                        // Trim off whitespace after "RETURNS"

                        int declarationStart = returnsIndex + "RETURNS ".length();

                        while (declarationStart < procedureDef.length()) {
                            if (Character.isWhitespace(procedureDef.charAt(declarationStart))) {
                                declarationStart++;
                            } else {
                                break;
                            }
                        }

                        String returnsDefn = procedureDef.substring(declarationStart, endReturnsDef).trim();
                        TypeDescriptor returnDescriptor = new TypeDescriptor(returnsDefn, "YES");

                        resultRows.add(convertTypeDescriptorToProcedureRow(procNameAsBytes, procCatAsBytes, "", false, false, true, returnDescriptor,
                                forGetFunctionColumns, 0));
                    }

                    if ((openParenIndex == -1) || (endOfParamDeclarationIndex == -1)) {
                        // parse error?
                        throw SQLError.createSQLException("Internal error when parsing callable statement metadata", SQLError.SQL_STATE_GENERAL_ERROR,
                                getExceptionInterceptor());
                    }

                    parameterDef = procedureDef.substring(openParenIndex + 1, endOfParamDeclarationIndex);
                }

            }
        } finally {
            SQLException sqlExRethrow = null;

            if (paramRetrievalRs != null) {
                try {
                    paramRetrievalRs.close();
                } catch (SQLException sqlEx) {
                    sqlExRethrow = sqlEx;
                }

                paramRetrievalRs = null;
            }

            if (paramRetrievalStmt != null) {
                try {
                    paramRetrievalStmt.close();
                } catch (SQLException sqlEx) {
                    sqlExRethrow = sqlEx;
                }

                paramRetrievalStmt = null;
            }

            if (sqlExRethrow != null) {
                throw sqlExRethrow;
            }
        }

        if (parameterDef != null) {
            int ordinal = 1;

            List<String> parseList = StringUtils.split(parameterDef, ",", storageDefnDelims, storageDefnClosures, true);

            int parseListLen = parseList.size();

            for (int i = 0; i < parseListLen; i++) {
                String declaration = parseList.get(i);

                if (declaration.trim().length() == 0) {
                    break; // no parameters actually declared, but whitespace spans lines
                }

                // Bug#52167, tokenizer will break if declaration contains special characters like \n
                declaration = declaration.replaceAll("[\\t\\n\\x0B\\f\\r]", " ");
                StringTokenizer declarationTok = new StringTokenizer(declaration, " \t");

                String paramName = null;
                boolean isOutParam = false;
                boolean isInParam = false;

                if (declarationTok.hasMoreTokens()) {
                    String possibleParamName = declarationTok.nextToken();

                    if (possibleParamName.equalsIgnoreCase("OUT")) {
                        isOutParam = true;

                        if (declarationTok.hasMoreTokens()) {
                            paramName = declarationTok.nextToken();
                        } else {
                            throw SQLError.createSQLException("Internal error when parsing callable statement metadata (missing parameter name)",
                                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                        }
                    } else if (possibleParamName.equalsIgnoreCase("INOUT")) {
                        isOutParam = true;
                        isInParam = true;

                        if (declarationTok.hasMoreTokens()) {
                            paramName = declarationTok.nextToken();
                        } else {
                            throw SQLError.createSQLException("Internal error when parsing callable statement metadata (missing parameter name)",
                                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                        }
                    } else if (possibleParamName.equalsIgnoreCase("IN")) {
                        isOutParam = false;
                        isInParam = true;

                        if (declarationTok.hasMoreTokens()) {
                            paramName = declarationTok.nextToken();
                        } else {
                            throw SQLError.createSQLException("Internal error when parsing callable statement metadata (missing parameter name)",
                                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                        }
                    } else {
                        isOutParam = false;
                        isInParam = true;

                        paramName = possibleParamName;
                    }

                    TypeDescriptor typeDesc = null;

                    if (declarationTok.hasMoreTokens()) {
                        StringBuilder typeInfoBuf = new StringBuilder(declarationTok.nextToken());

                        while (declarationTok.hasMoreTokens()) {
                            typeInfoBuf.append(" ");
                            typeInfoBuf.append(declarationTok.nextToken());
                        }

                        String typeInfo = typeInfoBuf.toString();

                        typeDesc = new TypeDescriptor(typeInfo, "YES");
                    } else {
                        throw SQLError.createSQLException("Internal error when parsing callable statement metadata (missing parameter type)",
                                SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                    }

                    if ((paramName.startsWith("`") && paramName.endsWith("`"))
                            || (isProcedureInAnsiMode && paramName.startsWith("\"") && paramName.endsWith("\""))) {
                        paramName = paramName.substring(1, paramName.length() - 1);
                    }

                    int wildCompareRes = StringUtils.wildCompare(paramName, parameterNamePattern);

                    if (wildCompareRes != StringUtils.WILD_COMPARE_NO_MATCH) {
                        ResultSetRow row = convertTypeDescriptorToProcedureRow(procNameAsBytes, procCatAsBytes, paramName, isOutParam, isInParam, false,
                                typeDesc, forGetFunctionColumns, ordinal++);

                        resultRows.add(row);
                    }
                } else {
                    throw SQLError.createSQLException("Internal error when parsing callable statement metadata (unknown output from 'SHOW CREATE PROCEDURE')",
                            SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                }
            }
        } else {
            // Is this an error? JDBC spec doesn't make it clear if stored procedure doesn't exist, is it an error....
        }
    }

    /**
     * Finds the end of the parameter declaration from the output of "SHOW
     * CREATE PROCEDURE".
     * 
     * @param beginIndex
     *            should be the index of the procedure body that contains the
     *            first "(".
     * @param procedureDef
     *            the procedure body
     * @param quoteChar
     *            the identifier quote character in use
     * @return the ending index of the parameter declaration, not including the
     *         closing ")"
     * @throws SQLException
     *             if a parse error occurs.
     */
    private int endPositionOfParameterDeclaration(int beginIndex, String procedureDef, String quoteChar) throws SQLException {
        int currentPos = beginIndex + 1;
        int parenDepth = 1; // counting the first openParen

        while (parenDepth > 0 && currentPos < procedureDef.length()) {
            int closedParenIndex = StringUtils.indexOfIgnoreCase(currentPos, procedureDef, ")", quoteChar, quoteChar,
                    this.conn.isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);

            if (closedParenIndex != -1) {
                int nextOpenParenIndex = StringUtils.indexOfIgnoreCase(currentPos, procedureDef, "(", quoteChar, quoteChar,
                        this.conn.isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);

                if (nextOpenParenIndex != -1 && nextOpenParenIndex < closedParenIndex) {
                    parenDepth++;
                    currentPos = closedParenIndex + 1; // set after closed paren that increases depth
                } else {
                    parenDepth--;
                    currentPos = closedParenIndex; // start search from same position
                }
            } else {
                // we should always get closed paren of some sort
                throw SQLError.createSQLException("Internal error when parsing callable statement metadata", SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
            }
        }

        return currentPos;
    }

    /**
     * Finds the end of the RETURNS clause for SQL Functions by using any of the
     * keywords allowed after the RETURNS clause, or a label.
     * 
     * @param procedureDefn
     *            the function body containing the definition of the function
     * @param quoteChar
     *            the identifier quote string in use
     * @param positionOfReturnKeyword
     *            the position of "RETURNS" in the definition
     * @return the end of the returns clause
     * @throws SQLException
     *             if a parse error occurs
     */
    private int findEndOfReturnsClause(String procedureDefn, int positionOfReturnKeyword) throws SQLException {
        /*
         * characteristic: LANGUAGE SQL | [NOT] DETERMINISTIC | { CONTAINS SQL |
         * NO SQL | READS SQL DATA | MODIFIES SQL DATA } | SQL SECURITY {
         * DEFINER | INVOKER } | COMMENT 'string'
         */
        String openingMarkers = this.quotedId + "(";
        String closingMarkers = this.quotedId + ")";

        String[] tokens = new String[] { "LANGUAGE", "NOT", "DETERMINISTIC", "CONTAINS", "NO", "READ", "MODIFIES", "SQL", "COMMENT", "BEGIN", "RETURN" };

        int startLookingAt = positionOfReturnKeyword + "RETURNS".length() + 1;

        int endOfReturn = -1;

        for (int i = 0; i < tokens.length; i++) {
            int nextEndOfReturn = StringUtils.indexOfIgnoreCase(startLookingAt, procedureDefn, tokens[i], openingMarkers, closingMarkers,
                    this.conn.isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);

            if (nextEndOfReturn != -1) {
                if (endOfReturn == -1 || (nextEndOfReturn < endOfReturn)) {
                    endOfReturn = nextEndOfReturn;
                }
            }
        }

        if (endOfReturn != -1) {
            return endOfReturn;
        }

        // Label?
        endOfReturn = StringUtils.indexOfIgnoreCase(startLookingAt, procedureDefn, ":", openingMarkers, closingMarkers,
                this.conn.isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);

        if (endOfReturn != -1) {
            // seek back until whitespace
            for (int i = endOfReturn; i > 0; i--) {
                if (Character.isWhitespace(procedureDefn.charAt(i))) {
                    return i;
                }
            }
        }

        // We can't parse it.

        throw SQLError.createSQLException("Internal error when parsing callable statement metadata", SQLError.SQL_STATE_GENERAL_ERROR,
                getExceptionInterceptor());
    }

    /**
     * Parses the cascade option string and returns the DBMD constant that
     * represents it (for deletes)
     * 
     * @param cascadeOptions
     *            the comment from 'SHOW TABLE STATUS'
     * @return the DBMD constant that represents the cascade option
     */
    private int getCascadeDeleteOption(String cascadeOptions) {
        int onDeletePos = cascadeOptions.indexOf("ON DELETE");

        if (onDeletePos != -1) {
            String deleteOptions = cascadeOptions.substring(onDeletePos, cascadeOptions.length());

            if (deleteOptions.startsWith("ON DELETE CASCADE")) {
                return java.sql.DatabaseMetaData.importedKeyCascade;
            } else if (deleteOptions.startsWith("ON DELETE SET NULL")) {
                return java.sql.DatabaseMetaData.importedKeySetNull;
            } else if (deleteOptions.startsWith("ON DELETE RESTRICT")) {
                return java.sql.DatabaseMetaData.importedKeyRestrict;
            } else if (deleteOptions.startsWith("ON DELETE NO ACTION")) {
                return java.sql.DatabaseMetaData.importedKeyNoAction;
            }
        }

        return java.sql.DatabaseMetaData.importedKeyNoAction;
    }

    /**
     * Parses the cascade option string and returns the DBMD constant that
     * represents it (for Updates)
     * 
     * @param cascadeOptions
     *            the comment from 'SHOW TABLE STATUS'
     * @return the DBMD constant that represents the cascade option
     */
    private int getCascadeUpdateOption(String cascadeOptions) {
        int onUpdatePos = cascadeOptions.indexOf("ON UPDATE");

        if (onUpdatePos != -1) {
            String updateOptions = cascadeOptions.substring(onUpdatePos, cascadeOptions.length());

            if (updateOptions.startsWith("ON UPDATE CASCADE")) {
                return java.sql.DatabaseMetaData.importedKeyCascade;
            } else if (updateOptions.startsWith("ON UPDATE SET NULL")) {
                return java.sql.DatabaseMetaData.importedKeySetNull;
            } else if (updateOptions.startsWith("ON UPDATE RESTRICT")) {
                return java.sql.DatabaseMetaData.importedKeyRestrict;
            } else if (updateOptions.startsWith("ON UPDATE NO ACTION")) {
                return java.sql.DatabaseMetaData.importedKeyNoAction;
            }
        }

        return java.sql.DatabaseMetaData.importedKeyNoAction;
    }

    protected IteratorWithCleanup<String> getCatalogIterator(String catalogSpec) throws SQLException {
        IteratorWithCleanup<String> allCatalogsIter;
        if (catalogSpec != null) {
            if (!catalogSpec.equals("")) {
                if (this.conn.getPedantic()) {
                    allCatalogsIter = new SingleStringIterator(catalogSpec);
                } else {
                    allCatalogsIter = new SingleStringIterator(StringUtils.unQuoteIdentifier(catalogSpec, this.quotedId));
                }
            } else {
                // legacy mode of operation
                allCatalogsIter = new SingleStringIterator(this.database);
            }
        } else if (this.conn.getNullCatalogMeansCurrent()) {

            allCatalogsIter = new SingleStringIterator(this.database);
        } else {
            allCatalogsIter = new ResultSetIterator(getCatalogs(), 1);
        }

        return allCatalogsIter;
    }

    /**
     * Get the catalog names available in this database. The results are ordered
     * by catalog name.
     * <P>
     * The catalog column is:
     * <OL>
     * <li><B>TABLE_CAT</B> String => catalog name</li>
     * </ol>
     * </p>
     * 
     * @return ResultSet each row has a single String column that is a catalog
     *         name
     * @throws SQLException
     */
    public java.sql.ResultSet getCatalogs() throws SQLException {
        java.sql.ResultSet results = null;
        java.sql.Statement stmt = null;

        try {
            stmt = this.conn.getMetadataSafeStatement();
            results = stmt.executeQuery("SHOW DATABASES");

            int catalogsCount = 0;
            if (results.last()) {
                catalogsCount = results.getRow();
                results.beforeFirst();
            }

            List<String> resultsAsList = new ArrayList<String>(catalogsCount);
            while (results.next()) {
                resultsAsList.add(results.getString(1));
            }
            Collections.sort(resultsAsList);

            Field[] fields = new Field[1];
            fields[0] = new Field("", "TABLE_CAT", Types.VARCHAR, results.getMetaData().getColumnDisplaySize(1));

            ArrayList<ResultSetRow> tuples = new ArrayList<ResultSetRow>(catalogsCount);
            for (String cat : resultsAsList) {
                byte[][] rowVal = new byte[1][];
                rowVal[0] = s2b(cat);
                tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
            }

            return buildResultSet(fields, tuples);
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (SQLException sqlEx) {
                    AssertionFailedException.shouldNotHappen(sqlEx);
                }

                results = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                    AssertionFailedException.shouldNotHappen(sqlEx);
                }

                stmt = null;
            }
        }
    }

    /**
     * What's the separator between catalog and table name?
     * 
     * @return the separator string
     * @throws SQLException
     */
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    // The following group of methods exposes various limitations based on the target database with the current driver. Unless otherwise specified, a result of
    // zero means there is no limit, or the limit is not known.

    /**
     * What's the database vendor's preferred term for "catalog"?
     * 
     * @return the vendor term
     * @throws SQLException
     */
    public String getCatalogTerm() throws SQLException {
        return "database";
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
    public java.sql.ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        Field[] fields = new Field[8];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 64);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 1);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 64);
        fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 64);
        fields[4] = new Field("", "GRANTOR", Types.CHAR, 77);
        fields[5] = new Field("", "GRANTEE", Types.CHAR, 77);
        fields[6] = new Field("", "PRIVILEGE", Types.CHAR, 64);
        fields[7] = new Field("", "IS_GRANTABLE", Types.CHAR, 3);

        String grantQuery = "SELECT c.host, c.db, t.grantor, c.user, c.table_name, c.column_name, c.column_priv "
                + "FROM mysql.columns_priv c, mysql.tables_priv t WHERE c.host = t.host AND c.db = t.db AND "
                + "c.table_name = t.table_name AND c.db LIKE ? AND c.table_name = ? AND c.column_name LIKE ?";

        PreparedStatement pStmt = null;
        ResultSet results = null;
        ArrayList<ResultSetRow> grantRows = new ArrayList<ResultSetRow>();

        try {
            pStmt = prepareMetaDataSafeStatement(grantQuery);

            pStmt.setString(1, (catalog != null) && (catalog.length() != 0) ? catalog : "%");
            pStmt.setString(2, table);
            pStmt.setString(3, columnNamePattern);

            results = pStmt.executeQuery();

            while (results.next()) {
                String host = results.getString(1);
                String db = results.getString(2);
                String grantor = results.getString(3);
                String user = results.getString(4);

                if ((user == null) || (user.length() == 0)) {
                    user = "%";
                }

                StringBuilder fullUser = new StringBuilder(user);

                if ((host != null) && this.conn.getUseHostsInPrivileges()) {
                    fullUser.append("@");
                    fullUser.append(host);
                }

                String columnName = results.getString(6);
                String allPrivileges = results.getString(7);

                if (allPrivileges != null) {
                    allPrivileges = allPrivileges.toUpperCase(Locale.ENGLISH);

                    StringTokenizer st = new StringTokenizer(allPrivileges, ",");

                    while (st.hasMoreTokens()) {
                        String privilege = st.nextToken().trim();
                        byte[][] tuple = new byte[8][];
                        tuple[0] = s2b(db);
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
                        grantRows.add(new ByteArrayRow(tuple, getExceptionInterceptor()));
                    }
                }
            }
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (Exception ex) {
                }

                results = null;
            }

            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (Exception ex) {
                }

                pStmt = null;
            }
        }

        return buildResultSet(fields, grantRows);
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
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schemaPattern
     *            a schema name pattern; "" retrieves those without a schema
     * @param tableNamePattern
     *            a table name pattern
     * @param columnNamePattern
     *            a column name pattern
     * @return ResultSet each row is a column description
     * @throws SQLException
     *             if a database access error occurs
     * @see #getSearchStringEscape
     */
    public java.sql.ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern, String columnNamePattern)
            throws SQLException {

        if (columnNamePattern == null) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                columnNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Column name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        final String colPattern = columnNamePattern;

        Field[] fields = createColumnsFields();

        final ArrayList<ResultSetRow> rows = new ArrayList<ResultSetRow>();
        final Statement stmt = this.conn.getMetadataSafeStatement();

        try {

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {

                    ArrayList<String> tableNameList = new ArrayList<String>();

                    if (tableNamePattern == null) {
                        // Select from all tables
                        java.sql.ResultSet tables = null;

                        try {
                            tables = getTables(catalogStr, schemaPattern, "%", new String[0]);

                            while (tables.next()) {
                                String tableNameFromList = tables.getString("TABLE_NAME");
                                tableNameList.add(tableNameFromList);
                            }
                        } finally {
                            if (tables != null) {
                                try {
                                    tables.close();
                                } catch (Exception sqlEx) {
                                    AssertionFailedException.shouldNotHappen(sqlEx);
                                }

                                tables = null;
                            }
                        }
                    } else {
                        java.sql.ResultSet tables = null;

                        try {
                            tables = getTables(catalogStr, schemaPattern, tableNamePattern, new String[0]);

                            while (tables.next()) {
                                String tableNameFromList = tables.getString("TABLE_NAME");
                                tableNameList.add(tableNameFromList);
                            }
                        } finally {
                            if (tables != null) {
                                try {
                                    tables.close();
                                } catch (SQLException sqlEx) {
                                    AssertionFailedException.shouldNotHappen(sqlEx);
                                }

                                tables = null;
                            }
                        }
                    }

                    for (String tableName : tableNameList) {

                        ResultSet results = null;

                        try {
                            StringBuilder queryBuf = new StringBuilder("SHOW ");

                            if (DatabaseMetaData.this.conn.versionMeetsMinimum(4, 1, 0)) {
                                queryBuf.append("FULL ");
                            }

                            queryBuf.append("COLUMNS FROM ");
                            queryBuf.append(StringUtils.quoteIdentifier(tableName, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic()));
                            queryBuf.append(" FROM ");
                            queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic()));
                            queryBuf.append(" LIKE ");
                            queryBuf.append(StringUtils.quoteIdentifier(colPattern, "'", true));

                            // Return correct ordinals if column name pattern is not '%'
                            // Currently, MySQL doesn't show enough data to do this, so we do it the 'hard' way...Once _SYSTEM tables are in, this should be
                            // much easier
                            boolean fixUpOrdinalsRequired = false;
                            Map<String, Integer> ordinalFixUpMap = null;

                            if (!colPattern.equals("%")) {
                                fixUpOrdinalsRequired = true;

                                StringBuilder fullColumnQueryBuf = new StringBuilder("SHOW ");

                                if (DatabaseMetaData.this.conn.versionMeetsMinimum(4, 1, 0)) {
                                    fullColumnQueryBuf.append("FULL ");
                                }

                                fullColumnQueryBuf.append("COLUMNS FROM ");
                                fullColumnQueryBuf.append(StringUtils.quoteIdentifier(tableName, DatabaseMetaData.this.quotedId,
                                        DatabaseMetaData.this.conn.getPedantic()));
                                fullColumnQueryBuf.append(" FROM ");
                                fullColumnQueryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId,
                                        DatabaseMetaData.this.conn.getPedantic()));

                                results = stmt.executeQuery(fullColumnQueryBuf.toString());

                                ordinalFixUpMap = new HashMap<String, Integer>();

                                int fullOrdinalPos = 1;

                                while (results.next()) {
                                    String fullOrdColName = results.getString("Field");

                                    ordinalFixUpMap.put(fullOrdColName, Integer.valueOf(fullOrdinalPos++));
                                }
                            }

                            results = stmt.executeQuery(queryBuf.toString());

                            int ordPos = 1;

                            while (results.next()) {
                                byte[][] rowVal = new byte[24][];
                                rowVal[0] = s2b(catalogStr); // TABLE_CAT
                                rowVal[1] = null; // TABLE_SCHEM (No schemas
                                // in MySQL)

                                rowVal[2] = s2b(tableName); // TABLE_NAME
                                rowVal[3] = results.getBytes("Field");

                                TypeDescriptor typeDesc = new TypeDescriptor(results.getString("Type"), results.getString("Null"));

                                rowVal[4] = Short.toString(typeDesc.dataType).getBytes();

                                // DATA_TYPE (jdbc)
                                rowVal[5] = s2b(typeDesc.typeName); // TYPE_NAME
                                // (native)
                                if (typeDesc.columnSize == null) {
                                    rowVal[6] = null;
                                } else {
                                    String collation = results.getString("Collation");
                                    int mbminlen = 1;
                                    if (collation != null
                                            && ("TEXT".equals(typeDesc.typeName) || "TINYTEXT".equals(typeDesc.typeName) || "MEDIUMTEXT"
                                                    .equals(typeDesc.typeName))) {
                                        if (collation.indexOf("ucs2") > -1 || collation.indexOf("utf16") > -1) {
                                            mbminlen = 2;
                                        } else if (collation.indexOf("utf32") > -1) {
                                            mbminlen = 4;
                                        }
                                    }
                                    rowVal[6] = mbminlen == 1 ? s2b(typeDesc.columnSize.toString()) : s2b(((Integer) (typeDesc.columnSize / mbminlen))
                                            .toString());
                                }
                                rowVal[7] = s2b(Integer.toString(typeDesc.bufferLength));
                                rowVal[8] = typeDesc.decimalDigits == null ? null : s2b(typeDesc.decimalDigits.toString());
                                rowVal[9] = s2b(Integer.toString(typeDesc.numPrecRadix));
                                rowVal[10] = s2b(Integer.toString(typeDesc.nullability));

                                //
                                // Doesn't always have this field, depending on version
                                //
                                //
                                // REMARK column
                                //
                                try {
                                    if (DatabaseMetaData.this.conn.versionMeetsMinimum(4, 1, 0)) {
                                        rowVal[11] = results.getBytes("Comment");
                                    } else {
                                        rowVal[11] = results.getBytes("Extra");
                                    }
                                } catch (Exception E) {
                                    rowVal[11] = new byte[0];
                                }

                                // COLUMN_DEF
                                rowVal[12] = results.getBytes("Default");

                                rowVal[13] = new byte[] { (byte) '0' }; // SQL_DATA_TYPE
                                rowVal[14] = new byte[] { (byte) '0' }; // SQL_DATE_TIME_SUB

                                if (StringUtils.indexOfIgnoreCase(typeDesc.typeName, "CHAR") != -1
                                        || StringUtils.indexOfIgnoreCase(typeDesc.typeName, "BLOB") != -1
                                        || StringUtils.indexOfIgnoreCase(typeDesc.typeName, "TEXT") != -1
                                        || StringUtils.indexOfIgnoreCase(typeDesc.typeName, "BINARY") != -1) {
                                    rowVal[15] = rowVal[6]; // CHAR_OCTET_LENGTH
                                } else {
                                    rowVal[15] = null;
                                }

                                // ORDINAL_POSITION
                                if (!fixUpOrdinalsRequired) {
                                    rowVal[16] = Integer.toString(ordPos++).getBytes();
                                } else {
                                    String origColName = results.getString("Field");
                                    Integer realOrdinal = ordinalFixUpMap.get(origColName);

                                    if (realOrdinal != null) {
                                        rowVal[16] = realOrdinal.toString().getBytes();
                                    } else {
                                        throw SQLError.createSQLException("Can not find column in full column list to determine true ordinal position.",
                                                SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                                    }
                                }

                                rowVal[17] = s2b(typeDesc.isNullable);

                                // We don't support REF or DISTINCT types
                                rowVal[18] = null;
                                rowVal[19] = null;
                                rowVal[20] = null;
                                rowVal[21] = null;

                                rowVal[22] = s2b("");

                                String extra = results.getString("Extra");

                                if (extra != null) {
                                    rowVal[22] = s2b(StringUtils.indexOfIgnoreCase(extra, "auto_increment") != -1 ? "YES" : "NO");
                                    rowVal[23] = s2b(StringUtils.indexOfIgnoreCase(extra, "generated") != -1 ? "YES" : "NO");
                                }

                                rows.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
                            }
                        } finally {
                            if (results != null) {
                                try {
                                    results.close();
                                } catch (Exception ex) {
                                }

                                results = null;
                            }
                        }
                    }
                }
            }.doForAll();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        java.sql.ResultSet results = buildResultSet(fields, rows);

        return results;
    }

    protected Field[] createColumnsFields() {
        Field[] fields = new Field[24];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 255);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 255);
        fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
        fields[4] = new Field("", "DATA_TYPE", Types.INTEGER, 5);
        fields[5] = new Field("", "TYPE_NAME", Types.CHAR, 16);
        fields[6] = new Field("", "COLUMN_SIZE", Types.INTEGER, Integer.toString(Integer.MAX_VALUE).length());
        fields[7] = new Field("", "BUFFER_LENGTH", Types.INTEGER, 10);
        fields[8] = new Field("", "DECIMAL_DIGITS", Types.INTEGER, 10);
        fields[9] = new Field("", "NUM_PREC_RADIX", Types.INTEGER, 10);
        fields[10] = new Field("", "NULLABLE", Types.INTEGER, 10);
        fields[11] = new Field("", "REMARKS", Types.CHAR, 0);
        fields[12] = new Field("", "COLUMN_DEF", Types.CHAR, 0);
        fields[13] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 10);
        fields[14] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 10);
        fields[15] = new Field("", "CHAR_OCTET_LENGTH", Types.INTEGER, Integer.toString(Integer.MAX_VALUE).length());
        fields[16] = new Field("", "ORDINAL_POSITION", Types.INTEGER, 10);
        fields[17] = new Field("", "IS_NULLABLE", Types.CHAR, 3);
        fields[18] = new Field("", "SCOPE_CATALOG", Types.CHAR, 255);
        fields[19] = new Field("", "SCOPE_SCHEMA", Types.CHAR, 255);
        fields[20] = new Field("", "SCOPE_TABLE", Types.CHAR, 255);
        fields[21] = new Field("", "SOURCE_DATA_TYPE", Types.SMALLINT, 10);
        fields[22] = new Field("", "IS_AUTOINCREMENT", Types.CHAR, 3); // JDBC 4
        fields[23] = new Field("", "IS_GENERATEDCOLUMN", Types.CHAR, 3); // JDBC 4.1
        return fields;
    }

    /**
     * JDBC 2.0 Return the connection that produced this metadata object.
     * 
     * @return the connection that produced this metadata object.
     * @throws SQLException
     *             if a database error occurs
     */
    public java.sql.Connection getConnection() throws SQLException {
        return this.conn;
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
    public java.sql.ResultSet getCrossReference(final String primaryCatalog, final String primarySchema, final String primaryTable,
            final String foreignCatalog, final String foreignSchema, final String foreignTable) throws SQLException {
        if (primaryTable == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        Field[] fields = createFkMetadataFields();

        final ArrayList<ResultSetRow> tuples = new ArrayList<ResultSetRow>();

        if (this.conn.versionMeetsMinimum(3, 23, 0)) {

            final Statement stmt = this.conn.getMetadataSafeStatement();

            try {

                new IterateBlock<String>(getCatalogIterator(foreignCatalog)) {
                    @Override
                    void forEach(String catalogStr) throws SQLException {

                        ResultSet fkresults = null;

                        try {

                            /*
                             * Get foreign key information for table
                             */
                            if (DatabaseMetaData.this.conn.versionMeetsMinimum(3, 23, 50)) {
                                fkresults = extractForeignKeyFromCreateTable(catalogStr, null);
                            } else {
                                StringBuilder queryBuf = new StringBuilder("SHOW TABLE STATUS FROM ");
                                queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId,
                                        DatabaseMetaData.this.conn.getPedantic()));

                                fkresults = stmt.executeQuery(queryBuf.toString());
                            }

                            String foreignTableWithCase = getTableNameWithCase(foreignTable);
                            String primaryTableWithCase = getTableNameWithCase(primaryTable);

                            /*
                             * Parse imported foreign key information
                             */

                            String dummy;

                            while (fkresults.next()) {
                                String tableType = fkresults.getString("Type");

                                if ((tableType != null) && (tableType.equalsIgnoreCase("innodb") || tableType.equalsIgnoreCase(SUPPORTS_FK))) {
                                    String comment = fkresults.getString("Comment").trim();

                                    if (comment != null) {
                                        StringTokenizer commentTokens = new StringTokenizer(comment, ";", false);

                                        if (commentTokens.hasMoreTokens()) {
                                            dummy = commentTokens.nextToken();

                                            // Skip InnoDB comment
                                        }

                                        while (commentTokens.hasMoreTokens()) {
                                            String keys = commentTokens.nextToken();
                                            LocalAndReferencedColumns parsedInfo = parseTableStatusIntoLocalAndReferencedColumns(keys);

                                            int keySeq = 0;

                                            Iterator<String> referencingColumns = parsedInfo.localColumnsList.iterator();
                                            Iterator<String> referencedColumns = parsedInfo.referencedColumnsList.iterator();

                                            while (referencingColumns.hasNext()) {
                                                String referencingColumn = StringUtils.unQuoteIdentifier(referencingColumns.next(),
                                                        DatabaseMetaData.this.quotedId);

                                                // one tuple for each table between parenthesis
                                                byte[][] tuple = new byte[14][];
                                                tuple[4] = ((foreignCatalog == null) ? null : s2b(foreignCatalog));
                                                tuple[5] = ((foreignSchema == null) ? null : s2b(foreignSchema));
                                                dummy = fkresults.getString("Name"); // FKTABLE_NAME

                                                if (dummy.compareTo(foreignTableWithCase) != 0) {
                                                    continue;
                                                }

                                                tuple[6] = s2b(dummy);

                                                tuple[7] = s2b(referencingColumn); // FKCOLUMN_NAME
                                                tuple[0] = ((primaryCatalog == null) ? null : s2b(primaryCatalog));
                                                tuple[1] = ((primarySchema == null) ? null : s2b(primarySchema));

                                                // Skip foreign key if it doesn't refer to the right table
                                                if (parsedInfo.referencedTable.compareTo(primaryTableWithCase) != 0) {
                                                    continue;
                                                }

                                                tuple[2] = s2b(parsedInfo.referencedTable); // PKTABLE_NAME
                                                tuple[3] = s2b(StringUtils.unQuoteIdentifier(referencedColumns.next(), DatabaseMetaData.this.quotedId)); // PKCOLUMN_NAME
                                                tuple[8] = Integer.toString(keySeq).getBytes(); // KEY_SEQ

                                                int[] actions = getForeignKeyActions(keys);

                                                tuple[9] = Integer.toString(actions[1]).getBytes();
                                                tuple[10] = Integer.toString(actions[0]).getBytes();
                                                tuple[11] = null; // FK_NAME
                                                tuple[12] = null; // PK_NAME
                                                tuple[13] = Integer.toString(java.sql.DatabaseMetaData.importedKeyNotDeferrable).getBytes();
                                                tuples.add(new ByteArrayRow(tuple, getExceptionInterceptor()));
                                                keySeq++;
                                            }
                                        }
                                    }
                                }
                            }

                        } finally {
                            if (fkresults != null) {
                                try {
                                    fkresults.close();
                                } catch (Exception sqlEx) {
                                    AssertionFailedException.shouldNotHappen(sqlEx);
                                }

                                fkresults = null;
                            }
                        }
                    }
                }.doForAll();
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        }

        java.sql.ResultSet results = buildResultSet(fields, tuples);

        return results;
    }

    protected Field[] createFkMetadataFields() {
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
        fields[13] = new Field("", "DEFERRABILITY", Types.SMALLINT, 2);
        return fields;
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
     * @throws SQLException
     */
    public String getDatabaseProductName() throws SQLException {
        return "MySQL";
    }

    /**
     * What's the version of this database product?
     * 
     * @return database version
     * @throws SQLException
     */
    public String getDatabaseProductVersion() throws SQLException {
        return this.conn.getServerVersion();
    }

    /**
     * What's the database's default transaction isolation level? The values are
     * defined in java.sql.Connection.
     * 
     * @return the default isolation level
     * @throws SQLException
     *             if a database access error occurs
     * @see Connection
     */
    public int getDefaultTransactionIsolation() throws SQLException {
        if (this.conn.supportsIsolationLevel()) {
            return java.sql.Connection.TRANSACTION_READ_COMMITTED;
        }

        return java.sql.Connection.TRANSACTION_NONE;
    }

    /**
     * What's this JDBC driver's major version number?
     * 
     * @return JDBC driver major version
     */
    public int getDriverMajorVersion() {
        return NonRegisteringDriver.getMajorVersionInternal();
    }

    /**
     * What's this JDBC driver's minor version number?
     * 
     * @return JDBC driver minor version number
     */
    public int getDriverMinorVersion() {
        return NonRegisteringDriver.getMinorVersionInternal();
    }

    /**
     * What's the name of this JDBC driver?
     * 
     * @return JDBC driver name
     * @throws SQLException
     */
    public String getDriverName() throws SQLException {
        return NonRegisteringDriver.NAME;
    }

    /**
     * What's the version of this JDBC driver?
     * 
     * @return JDBC driver version
     * @throws java.sql.SQLException
     */
    public String getDriverVersion() throws java.sql.SQLException {
        return "@MYSQL_CJ_FULL_PROD_NAME@ ( Revision: @MYSQL_CJ_REVISION@ )";
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
    public java.sql.ResultSet getExportedKeys(String catalog, String schema, final String table) throws SQLException {
        if (table == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        Field[] fields = createFkMetadataFields();

        final ArrayList<ResultSetRow> rows = new ArrayList<ResultSetRow>();

        if (this.conn.versionMeetsMinimum(3, 23, 0)) {

            final Statement stmt = this.conn.getMetadataSafeStatement();

            try {

                new IterateBlock<String>(getCatalogIterator(catalog)) {
                    @Override
                    void forEach(String catalogStr) throws SQLException {
                        ResultSet fkresults = null;

                        try {

                            /*
                             * Get foreign key information for table
                             */
                            if (DatabaseMetaData.this.conn.versionMeetsMinimum(3, 23, 50)) {
                                // we can use 'SHOW CREATE TABLE'

                                fkresults = extractForeignKeyFromCreateTable(catalogStr, null);
                            } else {
                                StringBuilder queryBuf = new StringBuilder("SHOW TABLE STATUS FROM ");
                                queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId,
                                        DatabaseMetaData.this.conn.getPedantic()));

                                fkresults = stmt.executeQuery(queryBuf.toString());
                            }

                            // lower-case table name might be turned on
                            String tableNameWithCase = getTableNameWithCase(table);

                            /*
                             * Parse imported foreign key information
                             */

                            while (fkresults.next()) {
                                String tableType = fkresults.getString("Type");

                                if ((tableType != null) && (tableType.equalsIgnoreCase("innodb") || tableType.equalsIgnoreCase(SUPPORTS_FK))) {
                                    String comment = fkresults.getString("Comment").trim();

                                    if (comment != null) {
                                        StringTokenizer commentTokens = new StringTokenizer(comment, ";", false);

                                        if (commentTokens.hasMoreTokens()) {
                                            commentTokens.nextToken(); // Skip
                                            // InnoDB
                                            // comment

                                            while (commentTokens.hasMoreTokens()) {
                                                String keys = commentTokens.nextToken();
                                                getExportKeyResults(catalogStr, tableNameWithCase, keys, rows, fkresults.getString("Name"));
                                            }
                                        }
                                    }
                                }
                            }

                        } finally {
                            if (fkresults != null) {
                                try {
                                    fkresults.close();
                                } catch (SQLException sqlEx) {
                                    AssertionFailedException.shouldNotHappen(sqlEx);
                                }

                                fkresults = null;
                            }
                        }
                    }
                }.doForAll();
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        }

        java.sql.ResultSet results = buildResultSet(fields, rows);

        return results;
    }

    /**
     * Adds to the tuples list the exported keys of exportingTable based on the
     * keysComment from the 'show table status' sql command. KeysComment is that
     * part of the comment field that follows the "InnoDB free ...;" prefix.
     * 
     * @param catalog
     *            the database to use
     * @param exportingTable
     *            the table keys are being exported from
     * @param keysComment
     *            the comment from 'show table status'
     * @param tuples
     *            the rows to add results to
     * @param fkTableName
     *            the foreign key table name
     * @throws SQLException
     *             if a database access error occurs
     */
    protected void getExportKeyResults(String catalog, String exportingTable, String keysComment, List<ResultSetRow> tuples, String fkTableName)
            throws SQLException {
        getResultsImpl(catalog, exportingTable, keysComment, tuples, fkTableName, true);
    }

    /**
     * Get all the "extra" characters that can be used in unquoted identifier
     * names (those beyond a-z, 0-9 and _).
     * 
     * @return the string containing the extra characters
     * @throws SQLException
     */
    public String getExtraNameCharacters() throws SQLException {
        return "#@";
    }

    /**
     * Returns the DELETE and UPDATE foreign key actions from the given 'SHOW
     * TABLE STATUS' string, with the DELETE action being the first item in the
     * array, and the UPDATE action being the second.
     * 
     * @param commentString
     *            the comment from 'SHOW TABLE STATUS'
     * @return int[] [0] = delete action, [1] = update action
     */
    protected int[] getForeignKeyActions(String commentString) {
        int[] actions = new int[] { java.sql.DatabaseMetaData.importedKeyNoAction, java.sql.DatabaseMetaData.importedKeyNoAction };

        int lastParenIndex = commentString.lastIndexOf(")");

        if (lastParenIndex != (commentString.length() - 1)) {
            String cascadeOptions = commentString.substring(lastParenIndex + 1).trim().toUpperCase(Locale.ENGLISH);

            actions[0] = getCascadeDeleteOption(cascadeOptions);
            actions[1] = getCascadeUpdateOption(cascadeOptions);
        }

        return actions;
    }

    /**
     * What's the string used to quote SQL identifiers? This returns a space " "
     * if identifier quoting isn't supported. A JDBC compliant driver always
     * uses a double quote character.
     * 
     * @return the quoting string
     * @throws SQLException
     */
    public String getIdentifierQuoteString() throws SQLException {
        if (this.conn.supportsQuotedIdentifiers()) {
            return this.conn.useAnsiQuotedIdentifiers() ? "\"" : "`";
        }

        // only versions below 3.23.6 don't support quoted identifiers.
        return " ";
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
    public java.sql.ResultSet getImportedKeys(String catalog, String schema, final String table) throws SQLException {
        if (table == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        Field[] fields = createFkMetadataFields();

        final ArrayList<ResultSetRow> rows = new ArrayList<ResultSetRow>();

        if (this.conn.versionMeetsMinimum(3, 23, 0)) {

            final Statement stmt = this.conn.getMetadataSafeStatement();

            try {

                new IterateBlock<String>(getCatalogIterator(catalog)) {
                    @Override
                    void forEach(String catalogStr) throws SQLException {
                        ResultSet fkresults = null;

                        try {

                            /*
                             * Get foreign key information for table
                             */
                            if (DatabaseMetaData.this.conn.versionMeetsMinimum(3, 23, 50)) {
                                // we can use 'SHOW CREATE TABLE'

                                fkresults = extractForeignKeyFromCreateTable(catalogStr, table);
                            } else {
                                StringBuilder queryBuf = new StringBuilder("SHOW TABLE STATUS ");
                                queryBuf.append(" FROM ");
                                queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId,
                                        DatabaseMetaData.this.conn.getPedantic()));
                                queryBuf.append(" LIKE ");
                                queryBuf.append(StringUtils.quoteIdentifier(table, "'", true));

                                fkresults = stmt.executeQuery(queryBuf.toString());
                            }

                            /*
                             * Parse imported foreign key information
                             */

                            while (fkresults.next()) {
                                String tableType = fkresults.getString("Type");

                                if ((tableType != null) && (tableType.equalsIgnoreCase("innodb") || tableType.equalsIgnoreCase(SUPPORTS_FK))) {
                                    String comment = fkresults.getString("Comment").trim();

                                    if (comment != null) {
                                        StringTokenizer commentTokens = new StringTokenizer(comment, ";", false);

                                        if (commentTokens.hasMoreTokens()) {
                                            commentTokens.nextToken(); // Skip InnoDB comment

                                            while (commentTokens.hasMoreTokens()) {
                                                String keys = commentTokens.nextToken();
                                                getImportKeyResults(catalogStr, table, keys, rows);
                                            }
                                        }
                                    }
                                }
                            }
                        } finally {
                            if (fkresults != null) {
                                try {
                                    fkresults.close();
                                } catch (SQLException sqlEx) {
                                    AssertionFailedException.shouldNotHappen(sqlEx);
                                }

                                fkresults = null;
                            }
                        }
                    }
                }.doForAll();
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        }

        java.sql.ResultSet results = buildResultSet(fields, rows);

        return results;
    }

    /**
     * Populates the tuples list with the imported keys of importingTable based
     * on the keysComment from the 'show table status' sql command. KeysComment
     * is that part of the comment field that follows the "InnoDB free ...;"
     * prefix.
     * 
     * @param catalog
     *            the database to use
     * @param importingTable
     *            the table keys are being imported to
     * @param keysComment
     *            the comment from 'show table status'
     * @param tuples
     *            the rows to add results to
     * @throws SQLException
     *             if a database access error occurs
     */
    protected void getImportKeyResults(String catalog, String importingTable, String keysComment, List<ResultSetRow> tuples) throws SQLException {
        getResultsImpl(catalog, importingTable, keysComment, tuples, null, false);
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
     * <li><B>CARDINALITY</B> int/long => When TYPE is tableIndexStatisic then this is the number of rows in the table; otherwise it is the number of unique
     * values in the index.</li>
     * <li><B>PAGES</B> int/long => When TYPE is tableIndexStatisic then this is the number of pages used for the table, otherwise it is the number of pages
     * used for the current index.</li>
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
    public java.sql.ResultSet getIndexInfo(String catalog, String schema, final String table, final boolean unique, boolean approximate) throws SQLException {
        /*
         * MySQL stores index information in the following fields: Table Non_unique Key_name Seq_in_index Column_name Collation Cardinality Sub_part
         */

        Field[] fields = createIndexInfoFields();

        final SortedMap<IndexMetaDataKey, ResultSetRow> sortedRows = new TreeMap<IndexMetaDataKey, ResultSetRow>();
        final ArrayList<ResultSetRow> rows = new ArrayList<ResultSetRow>();
        final Statement stmt = this.conn.getMetadataSafeStatement();

        try {

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {

                    ResultSet results = null;

                    try {
                        StringBuilder queryBuf = new StringBuilder("SHOW INDEX FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(table, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic()));
                        queryBuf.append(" FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic()));

                        try {
                            results = stmt.executeQuery(queryBuf.toString());
                        } catch (SQLException sqlEx) {
                            int errorCode = sqlEx.getErrorCode();

                            // If SQLState is 42S02, ignore this SQLException it means the table doesn't exist....
                            if (!"42S02".equals(sqlEx.getSQLState())) {
                                // Sometimes not mapped correctly for pre-4.1 so use error code instead.
                                if (errorCode != MysqlErrorNumbers.ER_NO_SUCH_TABLE) {
                                    throw sqlEx;
                                }
                            }
                        }

                        while (results != null && results.next()) {
                            byte[][] row = new byte[14][];
                            row[0] = ((catalogStr == null) ? new byte[0] : s2b(catalogStr));
                            row[1] = null;
                            row[2] = results.getBytes("Table");

                            boolean indexIsUnique = results.getInt("Non_unique") == 0;

                            row[3] = (!indexIsUnique ? s2b("true") : s2b("false"));
                            row[4] = new byte[0];
                            row[5] = results.getBytes("Key_name");
                            short indexType = java.sql.DatabaseMetaData.tableIndexOther;
                            row[6] = Integer.toString(indexType).getBytes();
                            row[7] = results.getBytes("Seq_in_index");
                            row[8] = results.getBytes("Column_name");
                            row[9] = results.getBytes("Collation");

                            long cardinality = results.getLong("Cardinality");

                            // Prior to JDBC 4.2, cardinality can be much larger than Integer's range, so we clamp it to conform to the API
                            if (!Util.isJdbc42() && cardinality > Integer.MAX_VALUE) {
                                cardinality = Integer.MAX_VALUE;
                            }

                            row[10] = s2b(String.valueOf(cardinality));
                            row[11] = s2b("0");
                            row[12] = null;

                            IndexMetaDataKey indexInfoKey = new IndexMetaDataKey(!indexIsUnique, indexType, results.getString("Key_name").toLowerCase(),
                                    results.getShort("Seq_in_index"));

                            if (unique) {
                                if (indexIsUnique) {
                                    sortedRows.put(indexInfoKey, new ByteArrayRow(row, getExceptionInterceptor()));
                                }
                            } else {
                                // All rows match
                                sortedRows.put(indexInfoKey, new ByteArrayRow(row, getExceptionInterceptor()));
                            }
                        }
                    } finally {
                        if (results != null) {
                            try {
                                results.close();
                            } catch (Exception ex) {
                            }

                            results = null;
                        }
                    }
                }
            }.doForAll();

            Iterator<ResultSetRow> sortedRowsIterator = sortedRows.values().iterator();
            while (sortedRowsIterator.hasNext()) {
                rows.add(sortedRowsIterator.next());
            }

            java.sql.ResultSet indexInfo = buildResultSet(fields, rows);

            return indexInfo;
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    protected Field[] createIndexInfoFields() {
        Field[] fields = new Field[13];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 255);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 255);
        fields[3] = new Field("", "NON_UNIQUE", Types.BOOLEAN, 4);
        fields[4] = new Field("", "INDEX_QUALIFIER", Types.CHAR, 1);
        fields[5] = new Field("", "INDEX_NAME", Types.CHAR, 32);
        fields[6] = new Field("", "TYPE", Types.SMALLINT, 32);
        fields[7] = new Field("", "ORDINAL_POSITION", Types.SMALLINT, 5);
        fields[8] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
        fields[9] = new Field("", "ASC_OR_DESC", Types.CHAR, 1);
        if (Util.isJdbc42()) {
            fields[10] = new Field("", "CARDINALITY", Types.BIGINT, 20);
            fields[11] = new Field("", "PAGES", Types.BIGINT, 20);
        } else {
            fields[10] = new Field("", "CARDINALITY", Types.INTEGER, 20);
            fields[11] = new Field("", "PAGES", Types.INTEGER, 10);
        }
        fields[12] = new Field("", "FILTER_CONDITION", Types.CHAR, 32);
        return fields;
    }

    /**
     * @see DatabaseMetaData#getJDBCMajorVersion()
     */
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    /**
     * @see DatabaseMetaData#getJDBCMinorVersion()
     */
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    /**
     * How many hex characters can you have in an inline binary literal?
     * 
     * @return max literal length
     * @throws SQLException
     */
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 16777208;
    }

    /**
     * What's the maximum length of a catalog name?
     * 
     * @return max name length in bytes
     * @throws SQLException
     */
    public int getMaxCatalogNameLength() throws SQLException {
        return 32;
    }

    /**
     * What's the max length for a character literal?
     * 
     * @return max literal length
     * @throws SQLException
     */
    public int getMaxCharLiteralLength() throws SQLException {
        return 16777208;
    }

    /**
     * What's the limit on column name length?
     * 
     * @return max literal length
     * @throws SQLException
     */
    public int getMaxColumnNameLength() throws SQLException {
        return 64;
    }

    /**
     * What's the maximum number of columns in a "GROUP BY" clause?
     * 
     * @return max number of columns
     * @throws SQLException
     */
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 64;
    }

    /**
     * What's the maximum number of columns allowed in an index?
     * 
     * @return max columns
     * @throws SQLException
     */
    public int getMaxColumnsInIndex() throws SQLException {
        return 16;
    }

    /**
     * What's the maximum number of columns in an "ORDER BY" clause?
     * 
     * @return max columns
     * @throws SQLException
     */
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 64;
    }

    /**
     * What's the maximum number of columns in a "SELECT" list?
     * 
     * @return max columns
     * @throws SQLException
     */
    public int getMaxColumnsInSelect() throws SQLException {
        return 256;
    }

    /**
     * What's maximum number of columns in a table?
     * 
     * @return max columns
     * @throws SQLException
     */
    public int getMaxColumnsInTable() throws SQLException {
        return 512;
    }

    /**
     * How many active connections can we have at a time to this database?
     * 
     * @return max connections
     * @throws SQLException
     */
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    /**
     * What's the maximum cursor name length?
     * 
     * @return max cursor name length in bytes
     * @throws SQLException
     */
    public int getMaxCursorNameLength() throws SQLException {
        return 64;
    }

    /**
     * What's the maximum length of an index (in bytes)?
     * 
     * @return max index length in bytes
     * @throws SQLException
     */
    public int getMaxIndexLength() throws SQLException {
        return 256;
    }

    /**
     * What's the maximum length of a procedure name?
     * 
     * @return max name length in bytes
     * @throws SQLException
     */
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    /**
     * What's the maximum length of a single row?
     * 
     * @return max row size in bytes
     * @throws SQLException
     */
    public int getMaxRowSize() throws SQLException {
        return Integer.MAX_VALUE - 8; // Max buffer size - HEADER
    }

    /**
     * What's the maximum length allowed for a schema name?
     * 
     * @return max name length in bytes
     * @throws SQLException
     */
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    /**
     * What's the maximum length of a SQL statement?
     * 
     * @return max length in bytes
     * @throws SQLException
     */
    public int getMaxStatementLength() throws SQLException {
        return MysqlIO.getMaxBuf() - 4; // Max buffer - header
    }

    /**
     * How many active statements can we have open at one time to this database?
     * 
     * @return the maximum
     * @throws SQLException
     */
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    /**
     * What's the maximum length of a table name?
     * 
     * @return max name length in bytes
     * @throws SQLException
     */
    public int getMaxTableNameLength() throws SQLException {
        return 64;
    }

    /**
     * What's the maximum number of tables in a SELECT?
     * 
     * @return the maximum
     * @throws SQLException
     */
    public int getMaxTablesInSelect() throws SQLException {
        return 256;
    }

    /**
     * What's the maximum length of a user name?
     * 
     * @return max name length in bytes
     * @throws SQLException
     */
    public int getMaxUserNameLength() throws SQLException {
        return 16;
    }

    /**
     * Get a comma separated list of math functions.
     * 
     * @return the list
     * @throws SQLException
     */
    public String getNumericFunctions() throws SQLException {
        return "ABS,ACOS,ASIN,ATAN,ATAN2,BIT_COUNT,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MAX,MIN,MOD,PI,POW,"
                + "POWER,RADIANS,RAND,ROUND,SIN,SQRT,TAN,TRUNCATE";
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
    public java.sql.ResultSet getPrimaryKeys(String catalog, String schema, final String table) throws SQLException {
        Field[] fields = new Field[6];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 255);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 0);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 255);
        fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
        fields[4] = new Field("", "KEY_SEQ", Types.SMALLINT, 5);
        fields[5] = new Field("", "PK_NAME", Types.CHAR, 32);

        if (table == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        final ArrayList<ResultSetRow> rows = new ArrayList<ResultSetRow>();
        final Statement stmt = this.conn.getMetadataSafeStatement();

        try {

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {
                    ResultSet rs = null;

                    try {

                        StringBuilder queryBuf = new StringBuilder("SHOW KEYS FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(table, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic()));
                        queryBuf.append(" FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic()));

                        rs = stmt.executeQuery(queryBuf.toString());

                        TreeMap<String, byte[][]> sortMap = new TreeMap<String, byte[][]>();

                        while (rs.next()) {
                            String keyType = rs.getString("Key_name");

                            if (keyType != null) {
                                if (keyType.equalsIgnoreCase("PRIMARY") || keyType.equalsIgnoreCase("PRI")) {
                                    byte[][] tuple = new byte[6][];
                                    tuple[0] = ((catalogStr == null) ? new byte[0] : s2b(catalogStr));
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
                        Iterator<byte[][]> sortedIterator = sortMap.values().iterator();

                        while (sortedIterator.hasNext()) {
                            rows.add(new ByteArrayRow(sortedIterator.next(), getExceptionInterceptor()));
                        }

                    } finally {
                        if (rs != null) {
                            try {
                                rs.close();
                            } catch (Exception ex) {
                            }

                            rs = null;
                        }
                    }
                }
            }.doForAll();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        java.sql.ResultSet results = buildResultSet(fields, rows);

        return results;
    }

    /**
     * Get a description of a catalog's stored procedure parameters and result
     * columns.
     * <P>
     * Only descriptions matching the schema, procedure and parameter name criteria are returned. They are ordered by PROCEDURE_SCHEM and PROCEDURE_NAME. Within
     * this, the return value, if any, is first. Next are the parameter descriptions in call order. The column descriptions follow in column number order.
     * </p>
     * <P>
     * Each row in the ResultSet is a parameter description or column description with the following fields:
     * <OL>
     * <li><B>PROCEDURE_CAT</B> String => procedure catalog (may be null)</li>
     * <li><B>PROCEDURE_SCHEM</B> String => procedure schema (may be null)</li>
     * <li><B>PROCEDURE_NAME</B> String => procedure name</li>
     * <li><B>COLUMN_NAME</B> String => column/parameter name</li>
     * <li><B>COLUMN_TYPE</B> Short => kind of column/parameter:
     * <UL>
     * <li>procedureColumnUnknown - nobody knows</li>
     * <li>procedureColumnIn - IN parameter</li>
     * <li>procedureColumnInOut - INOUT parameter</li>
     * <li>procedureColumnOut - OUT parameter</li>
     * <li>procedureColumnReturn - procedure return value</li>
     * <li>procedureColumnResult - result column in ResultSet</li>
     * </ul>
     * </li>
     * <li><B>DATA_TYPE</B> short => SQL type from java.sql.Types</li>
     * <li><B>TYPE_NAME</B> String => SQL type name</li>
     * <li><B>PRECISION</B> int => precision</li>
     * <li><B>LENGTH</B> int => length in bytes of data</li>
     * <li><B>SCALE</B> short => scale</li>
     * <li><B>RADIX</B> short => radix</li>
     * <li><B>NULLABLE</B> short => can it contain NULL?
     * <UL>
     * <li>procedureNoNulls - does not allow NULL values</li>
     * <li>procedureNullable - allows NULL values</li>
     * <li>procedureNullableUnknown - nullability unknown</li>
     * </ul>
     * </li>
     * <li><B>REMARKS</B> String => comment describing parameter/column</li>
     * <li><b>COLUMN_DEF</b> String => default value for the column (may be null)</li>
     * <li><b>SQL_DATA_TYPE</b> int => reserved for future use</li>
     * <li><b>SQL_DATETIME_SUB</b> int => reserved for future use</li>
     * <li><b>CHAR_OCTET_LENGTH</b> int => the maximum length of binary and character based columns. For any other datatype the returned value is a NULL</li>
     * <li><b>ORDINAL_POSITION</b> int => the ordinal position, starting from 1. A value of 0 is returned if this row describes the procedure's return value.</li>
     * <li><b>IS_NULLABLE</b> String => ISO rules are used to determine the nullability for a column.
     * <ul>
     * <li>YES --- if the parameter can include NULLs</li>
     * <li>NO --- if the parameter cannot include NULLs</li>
     * <li>empty string --- if the nullability for the parameter is unknown</li>
     * </ul>
     * </li>
     * <li>SPECIFIC_NAME String => the name which uniquely identifies this procedure within its schema.</li>
     * </ol>
     * </p>
     * <P>
     * <B>Note:</B> Some databases may not return the column descriptions for a procedure.
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schemaPattern
     *            a schema name pattern; "" retrieves those without a schema
     * @param procedureNamePattern
     *            a procedure name pattern
     * @param columnNamePattern
     *            a column name pattern
     * @return ResultSet each row is a stored procedure parameter or column
     *         description
     * @throws SQLException
     *             if a database access error occurs
     * @see #getSearchStringEscape
     */
    public java.sql.ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
            throws SQLException {
        Field[] fields = createProcedureColumnsFields();

        return getProcedureOrFunctionColumns(fields, catalog, schemaPattern, procedureNamePattern, columnNamePattern, true, true);
    }

    protected Field[] createProcedureColumnsFields() {
        Field[] fields = new Field[20];

        fields[0] = new Field("", "PROCEDURE_CAT", Types.CHAR, 512);
        fields[1] = new Field("", "PROCEDURE_SCHEM", Types.CHAR, 512);
        fields[2] = new Field("", "PROCEDURE_NAME", Types.CHAR, 512);
        fields[3] = new Field("", "COLUMN_NAME", Types.CHAR, 512);
        fields[4] = new Field("", "COLUMN_TYPE", Types.CHAR, 64);
        fields[5] = new Field("", "DATA_TYPE", Types.SMALLINT, 6);
        fields[6] = new Field("", "TYPE_NAME", Types.CHAR, 64);
        fields[7] = new Field("", "PRECISION", Types.INTEGER, 12);
        fields[8] = new Field("", "LENGTH", Types.INTEGER, 12);
        fields[9] = new Field("", "SCALE", Types.SMALLINT, 12);
        fields[10] = new Field("", "RADIX", Types.SMALLINT, 6);
        fields[11] = new Field("", "NULLABLE", Types.SMALLINT, 6);
        fields[12] = new Field("", "REMARKS", Types.CHAR, 512);
        fields[13] = new Field("", "COLUMN_DEF", Types.CHAR, 512);
        fields[14] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 12);
        fields[15] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 12);
        fields[16] = new Field("", "CHAR_OCTET_LENGTH", Types.INTEGER, 12);
        fields[17] = new Field("", "ORDINAL_POSITION", Types.INTEGER, 12);
        fields[18] = new Field("", "IS_NULLABLE", Types.CHAR, 512);
        fields[19] = new Field("", "SPECIFIC_NAME", Types.CHAR, 512);
        return fields;
    }

    protected java.sql.ResultSet getProcedureOrFunctionColumns(Field[] fields, String catalog, String schemaPattern, String procedureOrFunctionNamePattern,
            String columnNamePattern, boolean returnProcedures, boolean returnFunctions) throws SQLException {

        List<ComparableWrapper<String, ProcedureType>> procsOrFuncsToExtractList = new ArrayList<ComparableWrapper<String, ProcedureType>>();
        //Main container to be passed to getProceduresAndOrFunctions
        ResultSet procsAndOrFuncsRs = null;

        if (supportsStoredProcedures()) {
            try {
                //getProceduresAndOrFunctions does NOT expect procedureOrFunctionNamePattern  in form of DB_NAME.SP_NAME thus we need to remove it
                String tmpProcedureOrFunctionNamePattern = null;
                //Check if NOT a pattern first, then "sanitize"
                if ((procedureOrFunctionNamePattern != null) && (!procedureOrFunctionNamePattern.equals("%"))) {
                    tmpProcedureOrFunctionNamePattern = StringUtils.sanitizeProcOrFuncName(procedureOrFunctionNamePattern);
                }

                //Sanity check, if NamePattern is still NULL, we have a wildcard and not the name
                if (tmpProcedureOrFunctionNamePattern == null) {
                    tmpProcedureOrFunctionNamePattern = procedureOrFunctionNamePattern;
                } else {
                    //So we have a name to check meaning more actual processing
                    //Keep the Catalog parsed, maybe we'll need it at some point in the future...
                    String tmpCatalog = catalog;
                    List<String> parseList = StringUtils.splitDBdotName(tmpProcedureOrFunctionNamePattern, tmpCatalog, this.quotedId,
                            this.conn.isNoBackslashEscapesSet());

                    //There *should* be 2 rows, if any.
                    if (parseList.size() == 2) {
                        tmpCatalog = parseList.get(0);
                        tmpProcedureOrFunctionNamePattern = parseList.get(1);
                    } else {
                        //keep values as they are
                    }
                }

                procsAndOrFuncsRs = getProceduresAndOrFunctions(createFieldMetadataForGetProcedures(), catalog, schemaPattern,
                        tmpProcedureOrFunctionNamePattern, returnProcedures, returnFunctions);

                boolean hasResults = false;
                while (procsAndOrFuncsRs.next()) {
                    procsOrFuncsToExtractList.add(new ComparableWrapper<String, ProcedureType>(getFullyQualifiedName(procsAndOrFuncsRs.getString(1),
                            procsAndOrFuncsRs.getString(3)), procsAndOrFuncsRs.getShort(8) == procedureNoResult ? PROCEDURE : FUNCTION));
                    hasResults = true;
                }

                // FIX for Bug#56305, allowing the code to proceed with empty fields causing NPE later
                if (!hasResults) {
                    // throw SQLError.createSQLException(
                    // "User does not have access to metadata required to determine " +
                    // "stored procedure parameter types. If rights can not be granted, configure connection with \"noAccessToProcedureBodies=true\" " +
                    // "to have driver generate parameters that represent INOUT strings irregardless of actual parameter types.",
                    // SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());		
                } else {
                    Collections.sort(procsOrFuncsToExtractList);
                }

                // Required to be sorted in name-order by JDBC spec, in 'normal' case getProcedures takes care of this for us, but if system tables are
                // inaccessible, we need to sort... so just do this to be safe...
                // Collections.sort(proceduresToExtractList);
            } finally {
                SQLException rethrowSqlEx = null;

                if (procsAndOrFuncsRs != null) {
                    try {
                        procsAndOrFuncsRs.close();
                    } catch (SQLException sqlEx) {
                        rethrowSqlEx = sqlEx;
                    }
                }

                if (rethrowSqlEx != null) {
                    throw rethrowSqlEx;
                }
            }
        }

        ArrayList<ResultSetRow> resultRows = new ArrayList<ResultSetRow>();
        int idx = 0;
        String procNameToCall = "";

        for (ComparableWrapper<String, ProcedureType> procOrFunc : procsOrFuncsToExtractList) {
            String procName = procOrFunc.getKey();
            ProcedureType procType = procOrFunc.getValue();

            //Continuing from above (database_name.sp_name)
            if (!" ".equals(this.quotedId)) {
                idx = StringUtils.indexOfIgnoreCase(0, procName, ".", this.quotedId, this.quotedId,
                        this.conn.isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);
            } else {
                idx = procName.indexOf(".");
            }

            if (idx > 0) {
                catalog = StringUtils.unQuoteIdentifier(procName.substring(0, idx), this.quotedId);
                procNameToCall = procName; // Leave as CAT.PROC, needed later
            } else {
                //No catalog. Not sure how to handle right now...
                procNameToCall = procName;
            }
            getCallStmtParameterTypes(catalog, procNameToCall, procType, columnNamePattern, resultRows, fields.length == 17 /* for getFunctionColumns */);
        }

        return buildResultSet(fields, resultRows);
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
    public java.sql.ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        Field[] fields = createFieldMetadataForGetProcedures();

        return getProceduresAndOrFunctions(fields, catalog, schemaPattern, procedureNamePattern, true, true);
    }

    protected Field[] createFieldMetadataForGetProcedures() {
        Field[] fields = new Field[9];
        fields[0] = new Field("", "PROCEDURE_CAT", Types.CHAR, 255);
        fields[1] = new Field("", "PROCEDURE_SCHEM", Types.CHAR, 255);
        fields[2] = new Field("", "PROCEDURE_NAME", Types.CHAR, 255);
        fields[3] = new Field("", "reserved1", Types.CHAR, 0);
        fields[4] = new Field("", "reserved2", Types.CHAR, 0);
        fields[5] = new Field("", "reserved3", Types.CHAR, 0);
        fields[6] = new Field("", "REMARKS", Types.CHAR, 255);
        fields[7] = new Field("", "PROCEDURE_TYPE", Types.SMALLINT, 6);
        fields[8] = new Field("", "SPECIFIC_NAME", Types.CHAR, 255);

        return fields;
    }

    /**
     * @param fields
     * @param catalog
     * @param schemaPattern
     * @param procedureNamePattern
     * @param returnProcedures
     * @param returnFunctions
     * @throws SQLException
     */
    protected java.sql.ResultSet getProceduresAndOrFunctions(final Field[] fields, String catalog, String schemaPattern, String procedureNamePattern,
            final boolean returnProcedures, final boolean returnFunctions) throws SQLException {
        if ((procedureNamePattern == null) || (procedureNamePattern.length() == 0)) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                procedureNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Procedure name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        final ArrayList<ResultSetRow> procedureRows = new ArrayList<ResultSetRow>();

        if (supportsStoredProcedures()) {
            final String procNamePattern = procedureNamePattern;

            final List<ComparableWrapper<String, ResultSetRow>> procedureRowsToSort = new ArrayList<ComparableWrapper<String, ResultSetRow>>();

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {
                    String db = catalogStr;

                    boolean fromSelect = false;
                    ResultSet proceduresRs = null;
                    boolean needsClientFiltering = true;

                    StringBuilder selectFromMySQLProcSQL = new StringBuilder();

                    selectFromMySQLProcSQL.append("SELECT name, type, comment FROM mysql.proc WHERE ");
                    if (returnProcedures && !returnFunctions) {
                        selectFromMySQLProcSQL.append("type = 'PROCEDURE' and ");
                    } else if (!returnProcedures && returnFunctions) {
                        selectFromMySQLProcSQL.append("type = 'FUNCTION' and ");
                    }
                    selectFromMySQLProcSQL.append("name like ? and db <=> ? ORDER BY name, type");

                    java.sql.PreparedStatement proceduresStmt = prepareMetaDataSafeStatement(selectFromMySQLProcSQL.toString());

                    try {
                        //
                        // Try using system tables first, as this is a little bit more efficient....
                        //

                        boolean hasTypeColumn = false;

                        if (db != null) {
                            if (DatabaseMetaData.this.conn.lowerCaseTableNames()) {
                                db = db.toLowerCase();
                            }
                            proceduresStmt.setString(2, db);
                        } else {
                            proceduresStmt.setNull(2, Types.VARCHAR);
                        }

                        int nameIndex = 1;

                        proceduresStmt.setString(1, procNamePattern);

                        try {
                            proceduresRs = proceduresStmt.executeQuery();
                            fromSelect = true;
                            needsClientFiltering = false;
                            hasTypeColumn = true;
                        } catch (SQLException sqlEx) {

                            //
                            // Okay, system tables aren't accessible, so use 'SHOW ....'....
                            //
                            proceduresStmt.close();

                            fromSelect = false;

                            if (DatabaseMetaData.this.conn.versionMeetsMinimum(5, 0, 1)) {
                                nameIndex = 2;
                            } else {
                                nameIndex = 1;
                            }

                            proceduresStmt = prepareMetaDataSafeStatement("SHOW PROCEDURE STATUS LIKE ?");

                            proceduresStmt.setString(1, procNamePattern);

                            proceduresRs = proceduresStmt.executeQuery();
                        }

                        if (returnProcedures) {
                            convertToJdbcProcedureList(fromSelect, db, proceduresRs, needsClientFiltering, db, procedureRowsToSort, nameIndex);
                        }

                        if (!hasTypeColumn) {
                            // need to go after functions too...
                            if (proceduresStmt != null) {
                                proceduresStmt.close();
                            }

                            proceduresStmt = prepareMetaDataSafeStatement("SHOW FUNCTION STATUS LIKE ?");

                            proceduresStmt.setString(1, procNamePattern);

                            proceduresRs = proceduresStmt.executeQuery();

                        }
                        //Should be here, not in IF block!
                        if (returnFunctions) {
                            convertToJdbcFunctionList(db, proceduresRs, needsClientFiltering, db, procedureRowsToSort, nameIndex, fields);
                        }

                    } finally {
                        SQLException rethrowSqlEx = null;

                        if (proceduresRs != null) {
                            try {
                                proceduresRs.close();
                            } catch (SQLException sqlEx) {
                                rethrowSqlEx = sqlEx;
                            }
                        }

                        if (proceduresStmt != null) {
                            try {
                                proceduresStmt.close();
                            } catch (SQLException sqlEx) {
                                rethrowSqlEx = sqlEx;
                            }
                        }

                        if (rethrowSqlEx != null) {
                            throw rethrowSqlEx;
                        }
                    }
                }
            }.doForAll();

            Collections.sort(procedureRowsToSort);
            for (ComparableWrapper<String, ResultSetRow> procRow : procedureRowsToSort) {
                procedureRows.add(procRow.getValue());
            }
        }

        return buildResultSet(fields, procedureRows);
    }

    /**
     * What's the database vendor's preferred term for "procedure"?
     * 
     * @return the vendor term
     * @throws SQLException
     *             if an error occurs (don't know why it would in this case...)
     */
    public String getProcedureTerm() throws SQLException {
        return "PROCEDURE";
    }

    /**
     * @see DatabaseMetaData#getResultSetHoldability()
     */
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    private void getResultsImpl(String catalog, String table, String keysComment, List<ResultSetRow> tuples, String fkTableName, boolean isExport)
            throws SQLException {

        LocalAndReferencedColumns parsedInfo = parseTableStatusIntoLocalAndReferencedColumns(keysComment);

        if (isExport && !parsedInfo.referencedTable.equals(table)) {
            return;
        }

        if (parsedInfo.localColumnsList.size() != parsedInfo.referencedColumnsList.size()) {
            throw SQLError.createSQLException("Error parsing foreign keys definition, number of local and referenced columns is not the same.",
                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        Iterator<String> localColumnNames = parsedInfo.localColumnsList.iterator();
        Iterator<String> referColumnNames = parsedInfo.referencedColumnsList.iterator();

        int keySeqIndex = 1;

        while (localColumnNames.hasNext()) {
            byte[][] tuple = new byte[14][];
            String lColumnName = StringUtils.unQuoteIdentifier(localColumnNames.next(), this.quotedId);
            String rColumnName = StringUtils.unQuoteIdentifier(referColumnNames.next(), this.quotedId);
            tuple[FKTABLE_CAT] = ((catalog == null) ? new byte[0] : s2b(catalog));
            tuple[FKTABLE_SCHEM] = null;
            tuple[FKTABLE_NAME] = s2b((isExport) ? fkTableName : table);
            tuple[FKCOLUMN_NAME] = s2b(lColumnName);
            tuple[PKTABLE_CAT] = s2b(parsedInfo.referencedCatalog);
            tuple[PKTABLE_SCHEM] = null;
            tuple[PKTABLE_NAME] = s2b((isExport) ? table : parsedInfo.referencedTable);
            tuple[PKCOLUMN_NAME] = s2b(rColumnName);
            tuple[KEY_SEQ] = s2b(Integer.toString(keySeqIndex++));

            int[] actions = getForeignKeyActions(keysComment);

            tuple[UPDATE_RULE] = s2b(Integer.toString(actions[1]));
            tuple[DELETE_RULE] = s2b(Integer.toString(actions[0]));
            tuple[FK_NAME] = s2b(parsedInfo.constraintName);
            tuple[PK_NAME] = null; // not available from show table status
            tuple[DEFERRABILITY] = s2b(Integer.toString(java.sql.DatabaseMetaData.importedKeyNotDeferrable));
            tuples.add(new ByteArrayRow(tuple, getExceptionInterceptor()));
        }
    }

    /**
     * Get the schema names available in this database. The results are ordered
     * by schema name.
     * <P>
     * The schema column is:
     * <OL>
     * <li><B>TABLE_SCHEM</B> String => schema name</li>
     * </ol>
     * </p>
     * 
     * @return ResultSet each row has a single String column that is a schema
     *         name
     * @throws SQLException
     */
    public java.sql.ResultSet getSchemas() throws SQLException {
        Field[] fields = new Field[2];
        fields[0] = new Field("", "TABLE_SCHEM", java.sql.Types.CHAR, 0);
        fields[1] = new Field("", "TABLE_CATALOG", java.sql.Types.CHAR, 0);

        ArrayList<ResultSetRow> tuples = new ArrayList<ResultSetRow>();
        java.sql.ResultSet results = buildResultSet(fields, tuples);

        return results;
    }

    /**
     * What's the database vendor's preferred term for "schema"?
     * 
     * @return the vendor term
     * @throws SQLException
     */
    public String getSchemaTerm() throws SQLException {
        return "";
    }

    /**
     * This is the string that can be used to escape '_' or '%' in the string
     * pattern style catalog search parameters.
     * <P>
     * The '_' character represents any single character.
     * </p>
     * <P>
     * The '%' character represents any sequence of zero or more characters.
     * </p>
     * 
     * @return the string used to escape wildcard characters
     * @throws SQLException
     */
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    /**
     * Get a comma separated list of all a database's SQL keywords that are NOT also SQL92/SQL2003 keywords.
     * 
     * @return the list
     * @throws SQLException
     */
    public String getSQLKeywords() throws SQLException {
        if (mysqlKeywords != null) {
            return mysqlKeywords;
        }

        synchronized (DatabaseMetaData.class) {
            // double check, maybe it's already set
            if (mysqlKeywords != null) {
                return mysqlKeywords;
            }

            Set<String> mysqlKeywordSet = new TreeSet<String>();
            StringBuilder mysqlKeywordsBuffer = new StringBuilder();

            Collections.addAll(mysqlKeywordSet, MYSQL_KEYWORDS);
            mysqlKeywordSet.removeAll(Arrays.asList(Util.isJdbc4() ? SQL2003_KEYWORDS : SQL92_KEYWORDS));

            for (String keyword : mysqlKeywordSet) {
                mysqlKeywordsBuffer.append(",").append(keyword);
            }

            mysqlKeywords = mysqlKeywordsBuffer.substring(1);
            return mysqlKeywords;
        }
    }

    /**
     * @see DatabaseMetaData#getSQLStateType()
     */
    public int getSQLStateType() throws SQLException {
        if (this.conn.versionMeetsMinimum(4, 1, 0)) {
            return java.sql.DatabaseMetaData.sqlStateSQL99;
        }

        if (this.conn.getUseSqlStateCodes()) {
            return java.sql.DatabaseMetaData.sqlStateSQL99;
        }

        return java.sql.DatabaseMetaData.sqlStateXOpen;
    }

    /**
     * Get a comma separated list of string functions.
     * 
     * @return the list
     * @throws SQLException
     */
    public String getStringFunctions() throws SQLException {
        return "ASCII,BIN,BIT_LENGTH,CHAR,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,CONCAT_WS,CONV,ELT,EXPORT_SET,FIELD,FIND_IN_SET,HEX,INSERT,"
                + "INSTR,LCASE,LEFT,LENGTH,LOAD_FILE,LOCATE,LOCATE,LOWER,LPAD,LTRIM,MAKE_SET,MATCH,MID,OCT,OCTET_LENGTH,ORD,POSITION,"
                + "QUOTE,REPEAT,REPLACE,REVERSE,RIGHT,RPAD,RTRIM,SOUNDEX,SPACE,STRCMP,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING,"
                + "SUBSTRING_INDEX,TRIM,UCASE,UPPER";
    }

    /**
     * @see DatabaseMetaData#getSuperTables(String, String, String)
     */
    public java.sql.ResultSet getSuperTables(String arg0, String arg1, String arg2) throws SQLException {
        Field[] fields = new Field[4];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 32);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 32);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 32);
        fields[3] = new Field("", "SUPERTABLE_NAME", Types.CHAR, 32);

        return buildResultSet(fields, new ArrayList<ResultSetRow>());
    }

    /**
     * @see DatabaseMetaData#getSuperTypes(String, String, String)
     */
    public java.sql.ResultSet getSuperTypes(String arg0, String arg1, String arg2) throws SQLException {
        Field[] fields = new Field[6];
        fields[0] = new Field("", "TYPE_CAT", Types.CHAR, 32);
        fields[1] = new Field("", "TYPE_SCHEM", Types.CHAR, 32);
        fields[2] = new Field("", "TYPE_NAME", Types.CHAR, 32);
        fields[3] = new Field("", "SUPERTYPE_CAT", Types.CHAR, 32);
        fields[4] = new Field("", "SUPERTYPE_SCHEM", Types.CHAR, 32);
        fields[5] = new Field("", "SUPERTYPE_NAME", Types.CHAR, 32);

        return buildResultSet(fields, new ArrayList<ResultSetRow>());
    }

    /**
     * Get a comma separated list of system functions.
     * 
     * @return the list
     * @throws SQLException
     */
    public String getSystemFunctions() throws SQLException {
        return "DATABASE,USER,SYSTEM_USER,SESSION_USER,PASSWORD,ENCRYPT,LAST_INSERT_ID,VERSION";
    }

    protected String getTableNameWithCase(String table) {
        String tableNameWithCase = (this.conn.lowerCaseTableNames() ? table.toLowerCase() : table);

        return tableNameWithCase;
    }

    /**
     * Get a description of the access rights for each table available in a
     * catalog.
     * <P>
     * Only privileges matching the schema and table name criteria are returned. They are ordered by TABLE_SCHEM, TABLE_NAME, and PRIVILEGE.
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
     * @param schemaPattern
     *            a schema name pattern; "" retrieves those without a schema
     * @param tableNamePattern
     *            a table name pattern
     * @return ResultSet each row is a table privilege description
     * @throws SQLException
     *             if a database access error occurs
     * @see #getSearchStringEscape
     */
    public java.sql.ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {

        if (tableNamePattern == null) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                tableNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Table name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        Field[] fields = new Field[7];
        fields[0] = new Field("", "TABLE_CAT", Types.CHAR, 64);
        fields[1] = new Field("", "TABLE_SCHEM", Types.CHAR, 1);
        fields[2] = new Field("", "TABLE_NAME", Types.CHAR, 64);
        fields[3] = new Field("", "GRANTOR", Types.CHAR, 77);
        fields[4] = new Field("", "GRANTEE", Types.CHAR, 77);
        fields[5] = new Field("", "PRIVILEGE", Types.CHAR, 64);
        fields[6] = new Field("", "IS_GRANTABLE", Types.CHAR, 3);

        String grantQuery = "SELECT host,db,table_name,grantor,user,table_priv FROM mysql.tables_priv WHERE db LIKE ? AND table_name LIKE ?";

        ResultSet results = null;
        ArrayList<ResultSetRow> grantRows = new ArrayList<ResultSetRow>();
        PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(grantQuery);

            pStmt.setString(1, ((catalog != null) && (catalog.length() != 0)) ? catalog : "%");
            pStmt.setString(2, tableNamePattern);

            results = pStmt.executeQuery();

            while (results.next()) {
                String host = results.getString(1);
                String db = results.getString(2);
                String table = results.getString(3);
                String grantor = results.getString(4);
                String user = results.getString(5);

                if ((user == null) || (user.length() == 0)) {
                    user = "%";
                }

                StringBuilder fullUser = new StringBuilder(user);

                if ((host != null) && this.conn.getUseHostsInPrivileges()) {
                    fullUser.append("@");
                    fullUser.append(host);
                }

                String allPrivileges = results.getString(6);

                if (allPrivileges != null) {
                    allPrivileges = allPrivileges.toUpperCase(Locale.ENGLISH);

                    StringTokenizer st = new StringTokenizer(allPrivileges, ",");

                    while (st.hasMoreTokens()) {
                        String privilege = st.nextToken().trim();

                        // Loop through every column in the table
                        java.sql.ResultSet columnResults = null;

                        try {
                            columnResults = getColumns(catalog, schemaPattern, table, "%");

                            while (columnResults.next()) {
                                byte[][] tuple = new byte[8][];
                                tuple[0] = s2b(db);
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
                                grantRows.add(new ByteArrayRow(tuple, getExceptionInterceptor()));
                            }
                        } finally {
                            if (columnResults != null) {
                                try {
                                    columnResults.close();
                                } catch (Exception ex) {
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
                }

                results = null;
            }

            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (Exception ex) {
                }

                pStmt = null;
            }
        }

        return buildResultSet(fields, grantRows);
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
    public java.sql.ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, final String[] types) throws SQLException {

        if (tableNamePattern == null) {
            if (this.conn.getNullNamePatternMatchesAll()) {
                tableNamePattern = "%";
            } else {
                throw SQLError.createSQLException("Table name pattern can not be NULL or empty.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        final SortedMap<TableMetaDataKey, ResultSetRow> sortedRows = new TreeMap<TableMetaDataKey, ResultSetRow>();
        final ArrayList<ResultSetRow> tuples = new ArrayList<ResultSetRow>();

        final Statement stmt = this.conn.getMetadataSafeStatement();

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

        try {
            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {
                    boolean operatingOnSystemDB = "information_schema".equalsIgnoreCase(catalogStr) || "mysql".equalsIgnoreCase(catalogStr)
                            || "performance_schema".equalsIgnoreCase(catalogStr);

                    ResultSet results = null;

                    try {

                        try {
                            results = stmt.executeQuery((!DatabaseMetaData.this.conn.versionMeetsMinimum(5, 0, 2) ? "SHOW TABLES FROM "
                                    : "SHOW FULL TABLES FROM ")
                                    + StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic())
                                    + " LIKE " + StringUtils.quoteIdentifier(tableNamePat, "'", true));
                        } catch (SQLException sqlEx) {
                            if (SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlEx.getSQLState())) {
                                throw sqlEx;
                            }

                            return;
                        }

                        boolean shouldReportTables = false;
                        boolean shouldReportViews = false;
                        boolean shouldReportSystemTables = false;
                        boolean shouldReportSystemViews = false;
                        boolean shouldReportLocalTemporaries = false;

                        if (types == null || types.length == 0) {
                            shouldReportTables = true;
                            shouldReportViews = true;
                            shouldReportSystemTables = true;
                            shouldReportSystemViews = true;
                            shouldReportLocalTemporaries = true;
                        } else {
                            for (int i = 0; i < types.length; i++) {
                                if (TableType.TABLE.equalsTo(types[i])) {
                                    shouldReportTables = true;

                                } else if (TableType.VIEW.equalsTo(types[i])) {
                                    shouldReportViews = true;

                                } else if (TableType.SYSTEM_TABLE.equalsTo(types[i])) {
                                    shouldReportSystemTables = true;

                                } else if (TableType.SYSTEM_VIEW.equalsTo(types[i])) {
                                    shouldReportSystemViews = true;

                                } else if (TableType.LOCAL_TEMPORARY.equalsTo(types[i])) {
                                    shouldReportLocalTemporaries = true;
                                }
                            }
                        }

                        int typeColumnIndex = 0;
                        boolean hasTableTypes = false;

                        if (DatabaseMetaData.this.conn.versionMeetsMinimum(5, 0, 2)) {
                            try {
                                // Both column names have been in use in the source tree so far....
                                typeColumnIndex = results.findColumn("table_type");
                                hasTableTypes = true;
                            } catch (SQLException sqlEx) {

                                // We should probably check SQLState here, but that can change depending on the server version and user properties, however,
                                // we'll get a 'true' SQLException when we actually try to find the 'Type' column
                                // 
                                try {
                                    typeColumnIndex = results.findColumn("Type");
                                    hasTableTypes = true;
                                } catch (SQLException sqlEx2) {
                                    hasTableTypes = false;
                                }
                            }
                        }

                        while (results.next()) {
                            byte[][] row = new byte[10][];
                            row[0] = (catalogStr == null) ? null : s2b(catalogStr);
                            row[1] = null;
                            row[2] = results.getBytes(1);
                            row[4] = new byte[0];
                            row[5] = null;
                            row[6] = null;
                            row[7] = null;
                            row[8] = null;
                            row[9] = null;

                            if (hasTableTypes) {
                                String tableType = results.getString(typeColumnIndex);

                                switch (TableType.getTableTypeCompliantWith(tableType)) {
                                    case TABLE:
                                        boolean reportTable = false;
                                        TableMetaDataKey tablesKey = null;

                                        if (operatingOnSystemDB && shouldReportSystemTables) {
                                            row[3] = TableType.SYSTEM_TABLE.asBytes();
                                            tablesKey = new TableMetaDataKey(TableType.SYSTEM_TABLE.getName(), catalogStr, null, results.getString(1));
                                            reportTable = true;

                                        } else if (!operatingOnSystemDB && shouldReportTables) {
                                            row[3] = TableType.TABLE.asBytes();
                                            tablesKey = new TableMetaDataKey(TableType.TABLE.getName(), catalogStr, null, results.getString(1));
                                            reportTable = true;
                                        }

                                        if (reportTable) {
                                            sortedRows.put(tablesKey, new ByteArrayRow(row, getExceptionInterceptor()));
                                        }
                                        break;

                                    case VIEW:
                                        if (shouldReportViews) {
                                            row[3] = TableType.VIEW.asBytes();
                                            sortedRows.put(new TableMetaDataKey(TableType.VIEW.getName(), catalogStr, null, results.getString(1)),
                                                    new ByteArrayRow(row, getExceptionInterceptor()));
                                        }
                                        break;

                                    case SYSTEM_TABLE:
                                        if (shouldReportSystemTables) {
                                            row[3] = TableType.SYSTEM_TABLE.asBytes();
                                            sortedRows.put(new TableMetaDataKey(TableType.SYSTEM_TABLE.getName(), catalogStr, null, results.getString(1)),
                                                    new ByteArrayRow(row, getExceptionInterceptor()));
                                        }
                                        break;

                                    case SYSTEM_VIEW:
                                        if (shouldReportSystemViews) {
                                            row[3] = TableType.SYSTEM_VIEW.asBytes();
                                            sortedRows.put(new TableMetaDataKey(TableType.SYSTEM_VIEW.getName(), catalogStr, null, results.getString(1)),
                                                    new ByteArrayRow(row, getExceptionInterceptor()));
                                        }
                                        break;

                                    case LOCAL_TEMPORARY:
                                        if (shouldReportLocalTemporaries) {
                                            row[3] = TableType.LOCAL_TEMPORARY.asBytes();
                                            sortedRows.put(new TableMetaDataKey(TableType.LOCAL_TEMPORARY.getName(), catalogStr, null, results.getString(1)),
                                                    new ByteArrayRow(row, getExceptionInterceptor()));
                                        }
                                        break;

                                    default:
                                        row[3] = TableType.TABLE.asBytes();
                                        sortedRows.put(new TableMetaDataKey(TableType.TABLE.getName(), catalogStr, null, results.getString(1)),
                                                new ByteArrayRow(row, getExceptionInterceptor()));
                                        break;
                                }
                            } else {
                                if (shouldReportTables) {
                                    // Pre-MySQL-5.0.1, tables only
                                    row[3] = TableType.TABLE.asBytes();
                                    sortedRows.put(new TableMetaDataKey(TableType.TABLE.getName(), catalogStr, null, results.getString(1)), new ByteArrayRow(
                                            row, getExceptionInterceptor()));
                                }
                            }
                        }

                    } finally {
                        if (results != null) {
                            try {
                                results.close();
                            } catch (Exception ex) {
                            }

                            results = null;
                        }
                    }
                }
            }.doForAll();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        tuples.addAll(sortedRows.values());
        java.sql.ResultSet tables = buildResultSet(createTablesFields(), tuples);

        return tables;
    }

    protected Field[] createTablesFields() {
        Field[] fields = new Field[10];
        fields[0] = new Field("", "TABLE_CAT", java.sql.Types.VARCHAR, 255);
        fields[1] = new Field("", "TABLE_SCHEM", java.sql.Types.VARCHAR, 0);
        fields[2] = new Field("", "TABLE_NAME", java.sql.Types.VARCHAR, 255);
        fields[3] = new Field("", "TABLE_TYPE", java.sql.Types.VARCHAR, 5);
        fields[4] = new Field("", "REMARKS", java.sql.Types.VARCHAR, 0);
        fields[5] = new Field("", "TYPE_CAT", java.sql.Types.VARCHAR, 0);
        fields[6] = new Field("", "TYPE_SCHEM", java.sql.Types.VARCHAR, 0);
        fields[7] = new Field("", "TYPE_NAME", java.sql.Types.VARCHAR, 0);
        fields[8] = new Field("", "SELF_REFERENCING_COL_NAME", java.sql.Types.VARCHAR, 0);
        fields[9] = new Field("", "REF_GENERATION", java.sql.Types.VARCHAR, 0);
        return fields;
    }

    /**
     * Get the table types available in this database. The results are ordered
     * by table type.
     * <P>
     * The table type is:
     * <OL>
     * <li><B>TABLE_TYPE</B> String => table type. Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     * </li>
     * </ol>
     * </p>
     * 
     * @return ResultSet each row has a single String column that is a table
     *         type
     * @throws SQLException
     */
    public java.sql.ResultSet getTableTypes() throws SQLException {
        ArrayList<ResultSetRow> tuples = new ArrayList<ResultSetRow>();
        Field[] fields = new Field[] { new Field("", "TABLE_TYPE", Types.VARCHAR, 256) };

        boolean minVersion5_0_1 = this.conn.versionMeetsMinimum(5, 0, 1);

        tuples.add(new ByteArrayRow(new byte[][] { TableType.LOCAL_TEMPORARY.asBytes() }, getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(new byte[][] { TableType.SYSTEM_TABLE.asBytes() }, getExceptionInterceptor()));
        if (minVersion5_0_1) {
            tuples.add(new ByteArrayRow(new byte[][] { TableType.SYSTEM_VIEW.asBytes() }, getExceptionInterceptor()));
        }
        tuples.add(new ByteArrayRow(new byte[][] { TableType.TABLE.asBytes() }, getExceptionInterceptor()));
        if (minVersion5_0_1) {
            tuples.add(new ByteArrayRow(new byte[][] { TableType.VIEW.asBytes() }, getExceptionInterceptor()));
        }

        return buildResultSet(fields, tuples);
    }

    /**
     * Get a comma separated list of time and date functions.
     * 
     * @return the list
     * @throws SQLException
     */
    public String getTimeDateFunctions() throws SQLException {
        return "DAYOFWEEK,WEEKDAY,DAYOFMONTH,DAYOFYEAR,MONTH,DAYNAME,MONTHNAME,QUARTER,WEEK,YEAR,HOUR,MINUTE,SECOND,PERIOD_ADD,"
                + "PERIOD_DIFF,TO_DAYS,FROM_DAYS,DATE_FORMAT,TIME_FORMAT,CURDATE,CURRENT_DATE,CURTIME,CURRENT_TIME,NOW,SYSDATE,"
                + "CURRENT_TIMESTAMP,UNIX_TIMESTAMP,FROM_UNIXTIME,SEC_TO_TIME,TIME_TO_SEC";
    }

    /**
     * Get a description of all the standard SQL types supported by this
     * database. They are ordered by DATA_TYPE and then by how closely the data
     * type maps to the corresponding JDBC SQL type.
     * <P>
     * Each type description has the following columns:
     * <OL>
     * <li><B>TYPE_NAME</B> String => Type name</li>
     * <li><B>DATA_TYPE</B> short => SQL data type from java.sql.Types</li>
     * <li><B>PRECISION</B> int => maximum precision</li>
     * <li><B>LITERAL_PREFIX</B> String => prefix used to quote a literal (may be null)</li>
     * <li><B>LITERAL_SUFFIX</B> String => suffix used to quote a literal (may be null)</li>
     * <li><B>CREATE_PARAMS</B> String => parameters used in creating the type (may be null)</li>
     * <li><B>NULLABLE</B> short => can you use NULL for this type?
     * <UL>
     * <li>typeNoNulls - does not allow NULL values</li>
     * <li>typeNullable - allows NULL values</li>
     * <li>typeNullableUnknown - nullability unknown</li>
     * </ul>
     * </li>
     * <li><B>CASE_SENSITIVE</B> boolean=> is it case sensitive?</li>
     * <li><B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
     * <UL>
     * <li>typePredNone - No support</li>
     * <li>typePredChar - Only supported with WHERE .. LIKE</li>
     * <li>typePredBasic - Supported except for WHERE .. LIKE</li>
     * <li>typeSearchable - Supported for all WHERE ..</li>
     * </ul>
     * </li>
     * <li><B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned?</li>
     * <li><B>FIXED_PREC_SCALE</B> boolean => can it be a money value?</li>
     * <li><B>AUTO_INCREMENT</B> boolean => can it be used for an auto-increment value?</li>
     * <li><B>LOCAL_TYPE_NAME</B> String => localized version of type name (may be null)</li>
     * <li><B>MINIMUM_SCALE</B> short => minimum scale supported</li>
     * <li><B>MAXIMUM_SCALE</B> short => maximum scale supported</li>
     * <li><B>SQL_DATA_TYPE</B> int => unused</li>
     * <li><B>SQL_DATETIME_SUB</B> int => unused</li>
     * <li><B>NUM_PREC_RADIX</B> int => usually 2 or 10</li>
     * </ol>
     * </p>
     * 
     * @return ResultSet each row is a SQL type description
     * @throws SQLException
     */
    /**
     * Get a description of all the standard SQL types supported by this
     * database. They are ordered by DATA_TYPE and then by how closely the data
     * type maps to the corresponding JDBC SQL type.
     * <P>
     * Each type description has the following columns:
     * <OL>
     * <li><B>TYPE_NAME</B> String => Type name</li>
     * <li><B>DATA_TYPE</B> short => SQL data type from java.sql.Types</li>
     * <li><B>PRECISION</B> int => maximum precision</li>
     * <li><B>LITERAL_PREFIX</B> String => prefix used to quote a literal (may be null)</li>
     * <li><B>LITERAL_SUFFIX</B> String => suffix used to quote a literal (may be null)</li>
     * <li><B>CREATE_PARAMS</B> String => parameters used in creating the type (may be null)</li>
     * <li><B>NULLABLE</B> short => can you use NULL for this type?
     * <UL>
     * <li>typeNoNulls - does not allow NULL values</li>
     * <li>typeNullable - allows NULL values</li>
     * <li>typeNullableUnknown - nullability unknown</li>
     * </ul>
     * </li>
     * <li><B>CASE_SENSITIVE</B> boolean=> is it case sensitive?</li>
     * <li><B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
     * <UL>
     * <li>typePredNone - No support</li>
     * <li>typePredChar - Only supported with WHERE .. LIKE</li>
     * <li>typePredBasic - Supported except for WHERE .. LIKE</li>
     * <li>typeSearchable - Supported for all WHERE ..</li>
     * </ul>
     * </li>
     * <li><B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned?</li>
     * <li><B>FIXED_PREC_SCALE</B> boolean => can it be a money value?</li>
     * <li><B>AUTO_INCREMENT</B> boolean => can it be used for an auto-increment value?</li>
     * <li><B>LOCAL_TYPE_NAME</B> String => localized version of type name (may be null)</li>
     * <li><B>MINIMUM_SCALE</B> short => minimum scale supported</li>
     * <li><B>MAXIMUM_SCALE</B> short => maximum scale supported</li>
     * <li><B>SQL_DATA_TYPE</B> int => unused</li>
     * <li><B>SQL_DATETIME_SUB</B> int => unused</li>
     * <li><B>NUM_PREC_RADIX</B> int => usually 2 or 10</li>
     * </ol>
     * </p>
     * 
     * @return ResultSet each row is a SQL type description
     * @throws SQLException
     */
    public java.sql.ResultSet getTypeInfo() throws SQLException {
        Field[] fields = new Field[18];
        fields[0] = new Field("", "TYPE_NAME", Types.CHAR, 32);
        fields[1] = new Field("", "DATA_TYPE", Types.INTEGER, 5);
        fields[2] = new Field("", "PRECISION", Types.INTEGER, 10);
        fields[3] = new Field("", "LITERAL_PREFIX", Types.CHAR, 4);
        fields[4] = new Field("", "LITERAL_SUFFIX", Types.CHAR, 4);
        fields[5] = new Field("", "CREATE_PARAMS", Types.CHAR, 32);
        fields[6] = new Field("", "NULLABLE", Types.SMALLINT, 5);
        fields[7] = new Field("", "CASE_SENSITIVE", Types.BOOLEAN, 3);
        fields[8] = new Field("", "SEARCHABLE", Types.SMALLINT, 3);
        fields[9] = new Field("", "UNSIGNED_ATTRIBUTE", Types.BOOLEAN, 3);
        fields[10] = new Field("", "FIXED_PREC_SCALE", Types.BOOLEAN, 3);
        fields[11] = new Field("", "AUTO_INCREMENT", Types.BOOLEAN, 3);
        fields[12] = new Field("", "LOCAL_TYPE_NAME", Types.CHAR, 32);
        fields[13] = new Field("", "MINIMUM_SCALE", Types.SMALLINT, 5);
        fields[14] = new Field("", "MAXIMUM_SCALE", Types.SMALLINT, 5);
        fields[15] = new Field("", "SQL_DATA_TYPE", Types.INTEGER, 10);
        fields[16] = new Field("", "SQL_DATETIME_SUB", Types.INTEGER, 10);
        fields[17] = new Field("", "NUM_PREC_RADIX", Types.INTEGER, 10);

        byte[][] rowVal = null;
        ArrayList<ResultSetRow> tuples = new ArrayList<ResultSetRow>();

        /*
         * The following are ordered by java.sql.Types, and then by how closely the MySQL type matches the JDBC Type (per spec)
         */
        /*
         * MySQL Type: BIT (silently converted to TINYINT(1)) JDBC Type: BIT
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("BIT");
        rowVal[1] = Integer.toString(java.sql.Types.BIT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("1"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("BIT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: BOOL (silently converted to TINYINT(1)) JDBC Type: BIT
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("BOOL");
        rowVal[1] = Integer.toString(java.sql.Types.BIT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("1"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("BOOL"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: TINYINT JDBC Type: TINYINT
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TINYINT");
        rowVal[1] = Integer.toString(java.sql.Types.TINYINT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("3"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("TINYINT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        rowVal = new byte[18][];
        rowVal[0] = s2b("TINYINT UNSIGNED");
        rowVal[1] = Integer.toString(java.sql.Types.TINYINT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("3"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("TINYINT UNSIGNED"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: BIGINT JDBC Type: BIGINT
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("BIGINT");
        rowVal[1] = Integer.toString(java.sql.Types.BIGINT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("19"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("BIGINT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        rowVal = new byte[18][];
        rowVal[0] = s2b("BIGINT UNSIGNED");
        rowVal[1] = Integer.toString(java.sql.Types.BIGINT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("20"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("BIGINT UNSIGNED"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: LONG VARBINARY JDBC Type: LONGVARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("LONG VARBINARY");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("16777215"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("LONG VARBINARY"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: MEDIUMBLOB JDBC Type: LONGVARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("MEDIUMBLOB");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("16777215"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("MEDIUMBLOB"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: LONGBLOB JDBC Type: LONGVARBINARY
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
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("LONGBLOB"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: BLOB JDBC Type: LONGVARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("BLOB");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("65535"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("BLOB"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: TINYBLOB JDBC Type: LONGVARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TINYBLOB");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("TINYBLOB"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: VARBINARY (sliently converted to VARCHAR(M) BINARY) JDBC
         * Type: VARBINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("VARBINARY");
        rowVal[1] = Integer.toString(java.sql.Types.VARBINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b("(M)"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("VARBINARY"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: BINARY (silently converted to CHAR(M) BINARY) JDBC Type:
         * BINARY
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("BINARY");
        rowVal[1] = Integer.toString(java.sql.Types.BINARY).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b("(M)"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("true"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("BINARY"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: LONG VARCHAR JDBC Type: LONGVARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("LONG VARCHAR");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("16777215"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("LONG VARCHAR"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: MEDIUMTEXT JDBC Type: LONGVARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("MEDIUMTEXT");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("16777215"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("MEDIUMTEXT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: LONGTEXT JDBC Type: LONGVARCHAR
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
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("LONGTEXT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: TEXT JDBC Type: LONGVARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TEXT");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("65535"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("TEXT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: TINYTEXT JDBC Type: LONGVARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TINYTEXT");
        rowVal[1] = Integer.toString(java.sql.Types.LONGVARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("TINYTEXT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: CHAR JDBC Type: CHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("CHAR");
        rowVal[1] = Integer.toString(java.sql.Types.CHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b("(M)"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("CHAR"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        // The maximum number of digits for DECIMAL or NUMERIC is 65 (64 from MySQL 5.0.3 to 5.0.5). 

        int decimalPrecision = 254;

        if (this.conn.versionMeetsMinimum(5, 0, 3)) {
            if (this.conn.versionMeetsMinimum(5, 0, 6)) {
                decimalPrecision = 65;
            } else {
                decimalPrecision = 64;
            }
        }

        /*
         * MySQL Type: NUMERIC (silently converted to DECIMAL) JDBC Type: NUMERIC
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("NUMERIC");
        rowVal[1] = Integer.toString(java.sql.Types.NUMERIC).getBytes();

        // JDBC Data type
        rowVal[2] = s2b(String.valueOf(decimalPrecision)); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M[,D])] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("NUMERIC"); // Locale Type Name
        rowVal[13] = s2b("-308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: DECIMAL JDBC Type: DECIMAL
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DECIMAL");
        rowVal[1] = Integer.toString(java.sql.Types.DECIMAL).getBytes();

        // JDBC Data type
        rowVal[2] = s2b(String.valueOf(decimalPrecision)); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M[,D])] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("DECIMAL"); // Locale Type Name
        rowVal[13] = s2b("-308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: INTEGER JDBC Type: INTEGER
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("INTEGER");
        rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("10"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("INTEGER"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        rowVal = new byte[18][];
        rowVal[0] = s2b("INTEGER UNSIGNED");
        rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("10"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("INTEGER UNSIGNED"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: INT JDBC Type: INTEGER
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("INT");
        rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("10"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("INT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        rowVal = new byte[18][];
        rowVal[0] = s2b("INT UNSIGNED");
        rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("10"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("INT UNSIGNED"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: MEDIUMINT JDBC Type: INTEGER
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("MEDIUMINT");
        rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("7"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("MEDIUMINT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        rowVal = new byte[18][];
        rowVal[0] = s2b("MEDIUMINT UNSIGNED");
        rowVal[1] = Integer.toString(java.sql.Types.INTEGER).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("8"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("MEDIUMINT UNSIGNED"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: SMALLINT JDBC Type: SMALLINT
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("SMALLINT");
        rowVal[1] = Integer.toString(java.sql.Types.SMALLINT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("5"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [UNSIGNED] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("SMALLINT"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        rowVal = new byte[18][];
        rowVal[0] = s2b("SMALLINT UNSIGNED");
        rowVal[1] = Integer.toString(java.sql.Types.SMALLINT).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("5"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("true"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("SMALLINT UNSIGNED"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: FLOAT JDBC Type: REAL (this is the SINGLE PERCISION
         * floating point type)
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("FLOAT");
        rowVal[1] = Integer.toString(java.sql.Types.REAL).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("10"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("FLOAT"); // Locale Type Name
        rowVal[13] = s2b("-38"); // Minimum Scale
        rowVal[14] = s2b("38"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: DOUBLE JDBC Type: DOUBLE
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DOUBLE");
        rowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("17"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("DOUBLE"); // Locale Type Name
        rowVal[13] = s2b("-308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: DOUBLE PRECISION JDBC Type: DOUBLE
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DOUBLE PRECISION");
        rowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("17"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("DOUBLE PRECISION"); // Locale Type Name
        rowVal[13] = s2b("-308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: REAL (does not map to Types.REAL) JDBC Type: DOUBLE
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("REAL");
        rowVal[1] = Integer.toString(java.sql.Types.DOUBLE).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("17"); // Precision
        rowVal[3] = s2b(""); // Literal Prefix
        rowVal[4] = s2b(""); // Literal Suffix
        rowVal[5] = s2b("[(M,D)] [ZEROFILL]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("true"); // Auto Increment
        rowVal[12] = s2b("REAL"); // Locale Type Name
        rowVal[13] = s2b("-308"); // Minimum Scale
        rowVal[14] = s2b("308"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: VARCHAR JDBC Type: VARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("VARCHAR");
        rowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b(this.conn.versionMeetsMinimum(5, 0, 3) ? "65535" : "255"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b("(M)"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("VARCHAR"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: ENUM JDBC Type: VARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("ENUM");
        rowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("65535"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("ENUM"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: SET JDBC Type: VARCHAR
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("SET");
        rowVal[1] = Integer.toString(java.sql.Types.VARCHAR).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("64"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("SET"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: DATE JDBC Type: DATE
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DATE");
        rowVal[1] = Integer.toString(java.sql.Types.DATE).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("0"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("DATE"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: TIME JDBC Type: TIME
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TIME");
        rowVal[1] = Integer.toString(java.sql.Types.TIME).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("0"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("TIME"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: DATETIME JDBC Type: TIMESTAMP
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("DATETIME");
        rowVal[1] = Integer.toString(java.sql.Types.TIMESTAMP).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("0"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b(""); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("DATETIME"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        /*
         * MySQL Type: TIMESTAMP JDBC Type: TIMESTAMP
         */
        rowVal = new byte[18][];
        rowVal[0] = s2b("TIMESTAMP");
        rowVal[1] = Integer.toString(java.sql.Types.TIMESTAMP).getBytes();

        // JDBC Data type
        rowVal[2] = s2b("0"); // Precision
        rowVal[3] = s2b("'"); // Literal Prefix
        rowVal[4] = s2b("'"); // Literal Suffix
        rowVal[5] = s2b("[(M)]"); // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();

        // Nullable
        rowVal[7] = s2b("false"); // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();

        // Searchable
        rowVal[9] = s2b("false"); // Unsignable
        rowVal[10] = s2b("false"); // Fixed Prec Scale
        rowVal[11] = s2b("false"); // Auto Increment
        rowVal[12] = s2b("TIMESTAMP"); // Locale Type Name
        rowVal[13] = s2b("0"); // Minimum Scale
        rowVal[14] = s2b("0"); // Maximum Scale
        rowVal[15] = s2b("0"); // SQL Data Type (not used)
        rowVal[16] = s2b("0"); // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10"); // NUM_PREC_RADIX (2 or 10)
        tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));

        return buildResultSet(fields, tuples);
    }

    /**
     * JDBC 2.0 Get a description of the user-defined types defined in a
     * particular schema. Schema specific UDTs may have type JAVA_OBJECT,
     * STRUCT, or DISTINCT.
     * <P>
     * Only types matching the catalog, schema, type name and type criteria are returned. They are ordered by DATA_TYPE, TYPE_SCHEM and TYPE_NAME. The type name
     * parameter may be a fully qualified name. In this case, the catalog and schemaPattern parameters are ignored.
     * </p>
     * <P>
     * Each type description has the following columns:
     * <OL>
     * <li><B>TYPE_CAT</B> String => the type's catalog (may be null)</li>
     * <li><B>TYPE_SCHEM</B> String => type's schema (may be null)</li>
     * <li><B>TYPE_NAME</B> String => type name</li>
     * <li><B>CLASS_NAME</B> String => Java class name</li>
     * <li><B>DATA_TYPE</B> String => type value defined in java.sql.Types. One of JAVA_OBJECT, STRUCT, or DISTINCT</li>
     * <li><B>REMARKS</B> String => explanatory comment on the type</li>
     * </ol>
     * </p>
     * <P>
     * <B>Note:</B> If the driver does not support UDTs then an empty result set is returned.
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog; null
     *            means drop catalog name from the selection criteria
     * @param schemaPattern
     *            a schema name pattern; "" retrieves those without a schema
     * @param typeNamePattern
     *            a type name pattern; may be a fully qualified name
     * @param types
     *            a list of user-named types to include (JAVA_OBJECT, STRUCT, or
     *            DISTINCT); null returns all types
     * @return ResultSet - each row is a type description
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public java.sql.ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        Field[] fields = new Field[7];
        fields[0] = new Field("", "TYPE_CAT", Types.VARCHAR, 32);
        fields[1] = new Field("", "TYPE_SCHEM", Types.VARCHAR, 32);
        fields[2] = new Field("", "TYPE_NAME", Types.VARCHAR, 32);
        fields[3] = new Field("", "CLASS_NAME", Types.VARCHAR, 32);
        fields[4] = new Field("", "DATA_TYPE", Types.INTEGER, 10);
        fields[5] = new Field("", "REMARKS", Types.VARCHAR, 32);
        fields[6] = new Field("", "BASE_TYPE", Types.SMALLINT, 10);

        ArrayList<ResultSetRow> tuples = new ArrayList<ResultSetRow>();

        return buildResultSet(fields, tuples);
    }

    /**
     * What's the url for this database?
     * 
     * @return the url or null if it can't be generated
     * @throws SQLException
     */
    public String getURL() throws SQLException {
        return this.conn.getURL();
    }

    /**
     * What's our user name as known to the database?
     * 
     * @return our database user name
     * @throws SQLException
     */
    public String getUserName() throws SQLException {
        if (this.conn.getUseHostsInPrivileges()) {
            Statement stmt = null;
            ResultSet rs = null;

            try {
                stmt = this.conn.getMetadataSafeStatement();

                rs = stmt.executeQuery("SELECT USER()");
                rs.next();

                return rs.getString(1);
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (Exception ex) {
                        AssertionFailedException.shouldNotHappen(ex);
                    }

                    rs = null;
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ex) {
                        AssertionFailedException.shouldNotHappen(ex);
                    }

                    stmt = null;
                }
            }
        }

        return this.conn.getUser();
    }

    /**
     * Get a description of a table's columns that are automatically updated
     * when any value in a row is updated. They are unordered.
     * <P>
     * Each column description has the following columns:
     * <OL>
     * <li><B>SCOPE</B> short => is not used</li>
     * <li><B>COLUMN_NAME</B> String => column name</li>
     * <li><B>DATA_TYPE</B> short => SQL data type from java.sql.Types</li>
     * <li><B>TYPE_NAME</B> String => Data source dependent type name</li>
     * <li><B>COLUMN_SIZE</B> int => precision</li>
     * <li><B>BUFFER_LENGTH</B> int => length of column value in bytes</li>
     * <li><B>DECIMAL_DIGITS</B> short => scale</li>
     * <li><B>PSEUDO_COLUMN</B> short => is this a pseudo column like an Oracle ROWID
     * <UL>
     * <li>versionColumnUnknown - may or may not be pseudo column</li>
     * <li>versionColumnNotPseudo - is NOT a pseudo column</li>
     * <li>versionColumnPseudo - is a pseudo column</li>
     * </ul>
     * </li>
     * </ol>
     * </p>
     * 
     * @param catalog
     *            a catalog name; "" retrieves those without a catalog
     * @param schema
     *            a schema name; "" retrieves those without a schema
     * @param table
     *            a table name
     * @return ResultSet each row is a column description
     * @throws SQLException
     */
    public java.sql.ResultSet getVersionColumns(String catalog, String schema, final String table) throws SQLException {

        if (table == null) {
            throw SQLError.createSQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        Field[] fields = new Field[8];
        fields[0] = new Field("", "SCOPE", Types.SMALLINT, 5);
        fields[1] = new Field("", "COLUMN_NAME", Types.CHAR, 32);
        fields[2] = new Field("", "DATA_TYPE", Types.INTEGER, 5);
        fields[3] = new Field("", "TYPE_NAME", Types.CHAR, 16);
        fields[4] = new Field("", "COLUMN_SIZE", Types.INTEGER, 16);
        fields[5] = new Field("", "BUFFER_LENGTH", Types.INTEGER, 16);
        fields[6] = new Field("", "DECIMAL_DIGITS", Types.SMALLINT, 16);
        fields[7] = new Field("", "PSEUDO_COLUMN", Types.SMALLINT, 5);

        final ArrayList<ResultSetRow> rows = new ArrayList<ResultSetRow>();

        final Statement stmt = this.conn.getMetadataSafeStatement();

        try {

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {

                    ResultSet results = null;
                    boolean with_where = DatabaseMetaData.this.conn.versionMeetsMinimum(5, 0, 0);

                    try {
                        StringBuilder whereBuf = new StringBuilder(" Extra LIKE '%on update CURRENT_TIMESTAMP%'");
                        List<String> rsFields = new ArrayList<String>();

                        // for versions prior to 5.1.23 we can get "on update CURRENT_TIMESTAMP"
                        // only from SHOW CREATE TABLE
                        if (!DatabaseMetaData.this.conn.versionMeetsMinimum(5, 1, 23)) {

                            whereBuf = new StringBuilder();
                            boolean firstTime = true;

                            String query = new StringBuilder("SHOW CREATE TABLE ").append(getFullyQualifiedName(catalogStr, table)).toString();

                            results = stmt.executeQuery(query);
                            while (results.next()) {
                                String createTableString = results.getString(2);
                                StringTokenizer lineTokenizer = new StringTokenizer(createTableString, "\n");

                                while (lineTokenizer.hasMoreTokens()) {
                                    String line = lineTokenizer.nextToken().trim();
                                    if (StringUtils.indexOfIgnoreCase(line, "on update CURRENT_TIMESTAMP") > -1) {
                                        boolean usingBackTicks = true;
                                        int beginPos = line.indexOf(DatabaseMetaData.this.quotedId);

                                        if (beginPos == -1) {
                                            beginPos = line.indexOf("\"");
                                            usingBackTicks = false;
                                        }

                                        if (beginPos != -1) {
                                            int endPos = -1;

                                            if (usingBackTicks) {
                                                endPos = line.indexOf(DatabaseMetaData.this.quotedId, beginPos + 1);
                                            } else {
                                                endPos = line.indexOf("\"", beginPos + 1);
                                            }

                                            if (endPos != -1) {
                                                if (with_where) {
                                                    if (!firstTime) {
                                                        whereBuf.append(" or");
                                                    } else {
                                                        firstTime = false;
                                                    }
                                                    whereBuf.append(" Field='");
                                                    whereBuf.append(line.substring(beginPos + 1, endPos));
                                                    whereBuf.append("'");
                                                } else {
                                                    rsFields.add(line.substring(beginPos + 1, endPos));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if (whereBuf.length() > 0 || rsFields.size() > 0) {
                            StringBuilder queryBuf = new StringBuilder("SHOW COLUMNS FROM ");
                            queryBuf.append(StringUtils.quoteIdentifier(table, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic()));
                            queryBuf.append(" FROM ");
                            queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.conn.getPedantic()));
                            if (with_where) {
                                queryBuf.append(" WHERE");
                                queryBuf.append(whereBuf.toString());
                            }

                            results = stmt.executeQuery(queryBuf.toString());

                            while (results.next()) {
                                if (with_where || rsFields.contains(results.getString("Field"))) {
                                    TypeDescriptor typeDesc = new TypeDescriptor(results.getString("Type"), results.getString("Null"));
                                    byte[][] rowVal = new byte[8][];
                                    // SCOPE is not used
                                    rowVal[0] = null;
                                    // COLUMN_NAME
                                    rowVal[1] = results.getBytes("Field");
                                    // DATA_TYPE
                                    rowVal[2] = Short.toString(typeDesc.dataType).getBytes();
                                    // TYPE_NAME
                                    rowVal[3] = s2b(typeDesc.typeName);
                                    // COLUMN_SIZE
                                    rowVal[4] = typeDesc.columnSize == null ? null : s2b(typeDesc.columnSize.toString());
                                    // BUFFER_LENGTH
                                    rowVal[5] = s2b(Integer.toString(typeDesc.bufferLength));
                                    // DECIMAL_DIGITS
                                    rowVal[6] = typeDesc.decimalDigits == null ? null : s2b(typeDesc.decimalDigits.toString());
                                    // PSEUDO_COLUMN
                                    rowVal[7] = Integer.toString(java.sql.DatabaseMetaData.versionColumnNotPseudo).getBytes();

                                    rows.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
                                }
                            }
                        }
                    } catch (SQLException sqlEx) {
                        if (!SQLError.SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND.equals(sqlEx.getSQLState())) {
                            throw sqlEx;
                        }
                    } finally {
                        if (results != null) {
                            try {
                                results.close();
                            } catch (Exception ex) {
                            }

                            results = null;
                        }
                    }

                }
            }.doForAll();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        return buildResultSet(fields, rows);
    }

    /**
     * JDBC 2.0 Determine whether or not a visible row insert can be detected by
     * calling ResultSet.rowInserted().
     * 
     * @param type
     *            set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are detected by the resultset type
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * Does a catalog appear at the start of a qualified table name? (Otherwise
     * it appears at the end)
     * 
     * @return true if it appears at the start
     * @throws SQLException
     */
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    /**
     * Is the database in read-only mode?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    /**
     * @see DatabaseMetaData#locatorsUpdateCopy()
     */
    public boolean locatorsUpdateCopy() throws SQLException {
        return !this.conn.getEmulateLocators();
    }

    /**
     * Are concatenations between NULL and non-NULL values NULL? A JDBC
     * compliant driver always returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    /**
     * Are NULL values sorted at the end regardless of sort order?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    /**
     * Are NULL values sorted at the start regardless of sort order?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean nullsAreSortedAtStart() throws SQLException {
        return (this.conn.versionMeetsMinimum(4, 0, 2) && !this.conn.versionMeetsMinimum(4, 0, 11));
    }

    /**
     * Are NULL values sorted high?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    /**
     * Are NULL values sorted low?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean nullsAreSortedLow() throws SQLException {
        return !nullsAreSortedHigh();
    }

    /**
     * @param type
     * @throws SQLException
     */
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * @param type
     * @throws SQLException
     */
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * JDBC 2.0 Determine whether changes made by others are visible.
     * 
     * @param type
     *            set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are visible for the result set type
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * @param type
     * @throws SQLException
     */
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * @param type
     * @throws SQLException
     */
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * JDBC 2.0 Determine whether a result set's own changes visible.
     * 
     * @param type
     *            set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are visible for the result set type
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    protected LocalAndReferencedColumns parseTableStatusIntoLocalAndReferencedColumns(String keysComment) throws SQLException {
        // keys will equal something like this: (parent_service_id child_service_id) REFER ds/subservices(parent_service_id child_service_id)
        //
        // simple-columned keys: (m) REFER airline/tt(a)
        //
        // multi-columned keys : (m n) REFER airline/vv(a b)
        //
        // parse of the string into three phases:
        // 1: parse the opening parentheses to determine how many results there will be
        // 2: read in the schema name/table name
        // 3: parse the closing parentheses

        String columnsDelimitter = ","; // what version did this change in?

        int indexOfOpenParenLocalColumns = StringUtils.indexOfIgnoreCase(0, keysComment, "(", this.quotedId, this.quotedId, StringUtils.SEARCH_MODE__ALL);

        if (indexOfOpenParenLocalColumns == -1) {
            throw SQLError.createSQLException("Error parsing foreign keys definition, couldn't find start of local columns list.",
                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        String constraintName = StringUtils.unQuoteIdentifier(keysComment.substring(0, indexOfOpenParenLocalColumns).trim(), this.quotedId);
        keysComment = keysComment.substring(indexOfOpenParenLocalColumns, keysComment.length());

        String keysCommentTrimmed = keysComment.trim();

        int indexOfCloseParenLocalColumns = StringUtils.indexOfIgnoreCase(0, keysCommentTrimmed, ")", this.quotedId, this.quotedId,
                StringUtils.SEARCH_MODE__ALL);

        if (indexOfCloseParenLocalColumns == -1) {
            throw SQLError.createSQLException("Error parsing foreign keys definition, couldn't find end of local columns list.",
                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        String localColumnNamesString = keysCommentTrimmed.substring(1, indexOfCloseParenLocalColumns);

        int indexOfRefer = StringUtils.indexOfIgnoreCase(0, keysCommentTrimmed, "REFER ", this.quotedId, this.quotedId, StringUtils.SEARCH_MODE__ALL);

        if (indexOfRefer == -1) {
            throw SQLError.createSQLException("Error parsing foreign keys definition, couldn't find start of referenced tables list.",
                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        int indexOfOpenParenReferCol = StringUtils.indexOfIgnoreCase(indexOfRefer, keysCommentTrimmed, "(", this.quotedId, this.quotedId,
                StringUtils.SEARCH_MODE__MRK_COM_WS);

        if (indexOfOpenParenReferCol == -1) {
            throw SQLError.createSQLException("Error parsing foreign keys definition, couldn't find start of referenced columns list.",
                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        String referCatalogTableString = keysCommentTrimmed.substring(indexOfRefer + "REFER ".length(), indexOfOpenParenReferCol);

        int indexOfSlash = StringUtils.indexOfIgnoreCase(0, referCatalogTableString, "/", this.quotedId, this.quotedId, StringUtils.SEARCH_MODE__MRK_COM_WS);

        if (indexOfSlash == -1) {
            throw SQLError.createSQLException("Error parsing foreign keys definition, couldn't find name of referenced catalog.",
                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        String referCatalog = StringUtils.unQuoteIdentifier(referCatalogTableString.substring(0, indexOfSlash), this.quotedId);
        String referTable = StringUtils.unQuoteIdentifier(referCatalogTableString.substring(indexOfSlash + 1).trim(), this.quotedId);

        int indexOfCloseParenRefer = StringUtils.indexOfIgnoreCase(indexOfOpenParenReferCol, keysCommentTrimmed, ")", this.quotedId, this.quotedId,
                StringUtils.SEARCH_MODE__ALL);

        if (indexOfCloseParenRefer == -1) {
            throw SQLError.createSQLException("Error parsing foreign keys definition, couldn't find end of referenced columns list.",
                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        String referColumnNamesString = keysCommentTrimmed.substring(indexOfOpenParenReferCol + 1, indexOfCloseParenRefer);

        List<String> referColumnsList = StringUtils.split(referColumnNamesString, columnsDelimitter, this.quotedId, this.quotedId, false);
        List<String> localColumnsList = StringUtils.split(localColumnNamesString, columnsDelimitter, this.quotedId, this.quotedId, false);

        return new LocalAndReferencedColumns(localColumnsList, referColumnsList, constraintName, referCatalog, referTable);
    }

    /**
     * Converts the given string to bytes, using the connection's character
     * encoding, or if not available, the JVM default encoding.
     * 
     * @param s
     */
    protected byte[] s2b(String s) throws SQLException {
        if (s == null) {
            return null;
        }

        return StringUtils.getBytes(s, this.conn.getCharacterSetMetadata(), this.conn.getServerCharset(), this.conn.parserKnowsUnicode(), this.conn,
                getExceptionInterceptor());
    }

    /**
     * Does the database store mixed case unquoted SQL identifiers in lower
     * case?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return this.conn.storesLowerCaseTableName();
    }

    /**
     * Does the database store mixed case quoted SQL identifiers in lower case?
     * A JDBC compliant driver will always return false.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return this.conn.storesLowerCaseTableName();
    }

    /**
     * Does the database store mixed case unquoted SQL identifiers in mixed
     * case?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return !this.conn.storesLowerCaseTableName();
    }

    /**
     * Does the database store mixed case quoted SQL identifiers in mixed case?
     * A JDBC compliant driver will always return false.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return !this.conn.storesLowerCaseTableName();
    }

    /**
     * Does the database store mixed case unquoted SQL identifiers in upper
     * case?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Does the database store mixed case quoted SQL identifiers in upper case?
     * A JDBC compliant driver will always return true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return true; // not actually true, but required by JDBC spec!?
    }

    /**
     * Is "ALTER TABLE" with add column supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    /**
     * Is "ALTER TABLE" with drop column supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    /**
     * Is the ANSI92 entry level SQL grammar supported? All JDBC compliant
     * drivers must return true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    /**
     * Is the ANSI92 full SQL grammar supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    /**
     * Is the ANSI92 intermediate SQL grammar supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    /**
     * JDBC 2.0 Return true if the driver supports batch updates, else return
     * false.
     * 
     * @throws SQLException
     */
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    /**
     * Can a catalog name be used in a data manipulation statement?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        // Servers before 3.22 could not do this
        return this.conn.versionMeetsMinimum(3, 22, 0);
    }

    /**
     * Can a catalog name be used in a index definition statement?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        // Servers before 3.22 could not do this
        return this.conn.versionMeetsMinimum(3, 22, 0);
    }

    /**
     * Can a catalog name be used in a privilege definition statement?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        // Servers before 3.22 could not do this
        return this.conn.versionMeetsMinimum(3, 22, 0);
    }

    /**
     * Can a catalog name be used in a procedure call statement?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        // Servers before 3.22 could not do this
        return this.conn.versionMeetsMinimum(3, 22, 0);
    }

    /**
     * Can a catalog name be used in a table definition statement?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        // Servers before 3.22 could not do this
        return this.conn.versionMeetsMinimum(3, 22, 0);
    }

    /**
     * Is column aliasing supported?
     * <P>
     * If so, the SQL AS clause can be used to provide names for computed columns or to provide alias names for columns as required. A JDBC compliant driver
     * always returns true.
     * </p>
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    /**
     * Is the CONVERT function between SQL types supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    /**
     * Is CONVERT between the given SQL types supported?
     * 
     * @param fromType
     *            the type to convert from
     * @param toType
     *            the type to convert to
     * @return true if so
     * @throws SQLException
     *             if an error occurs
     * @see Types
     */
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        switch (fromType) {
        /*
         * The char/binary types can be converted to pretty much anything.
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

                /*
                 * We don't handle the BIT type yet.
                 */
            case java.sql.Types.BIT:
                return false;

                /*
                 * The numeric types. Basically they can convert among themselves, and with char/binary types.
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

                /*
                 * With this driver, this will always be a serialized object, so the char/binary types will work.
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

                /*
                 * Timestamp can be converted to char/binary types and date/time types (with loss of precision).
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
     * @throws SQLException
     */
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    /**
     * Are correlated subqueries supported? A JDBC compliant driver always
     * returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return this.conn.versionMeetsMinimum(4, 1, 0);
    }

    /**
     * Are both data definition and data manipulation statements within a
     * transaction supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    /**
     * Are only data manipulation statements within a transaction supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    /**
     * If table correlation names are supported, are they restricted to be
     * different from the names of the tables? A JDBC compliant driver always
     * returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return true;
    }

    /**
     * Are expressions in "ORDER BY" lists supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    /**
     * Is the ODBC Extended SQL grammar supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    /**
     * Are full nested outer joins supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    /**
     * JDBC 3.0
     */
    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    /**
     * Is some form of "GROUP BY" clause supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    /**
     * Can a "GROUP BY" clause add columns not in the SELECT provided it
     * specifies all the columns in the SELECT?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    /**
     * Can a "GROUP BY" clause use columns not in the SELECT?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    /**
     * Is the SQL Integrity Enhancement Facility supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        if (!this.conn.getOverrideSupportsIntegrityEnhancementFacility()) {
            return false;
        }

        return true;
    }

    /**
     * Is the escape character in "LIKE" clauses supported? A JDBC compliant
     * driver always returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    /**
     * Is there limited support for outer joins? (This will be true if
     * supportFullOuterJoins is true.)
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    /**
     * Is the ODBC Minimum SQL grammar supported? All JDBC compliant drivers
     * must return true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    /**
     * Does the database support mixed case unquoted SQL identifiers?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return !this.conn.lowerCaseTableNames();
    }

    /**
     * Does the database support mixed case quoted SQL identifiers? A JDBC
     * compliant driver will always return true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return !this.conn.lowerCaseTableNames();
    }

    /**
     * @see DatabaseMetaData#supportsMultipleOpenResults()
     */
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    /**
     * Are multiple ResultSets from a single execute supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsMultipleResultSets() throws SQLException {
        return this.conn.versionMeetsMinimum(4, 1, 0);
    }

    /**
     * Can we have multiple transactions open at once (on different
     * connections)?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsMultipleTransactions() throws SQLException {
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
     * @throws SQLException
     */
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    /**
     * Can cursors remain open across commits?
     * 
     * @return true if so
     * @throws SQLException
     *             if a database access error occurs
     * @see Connection#disableAutoClose
     */
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    /**
     * Can cursors remain open across rollbacks?
     * 
     * @return true if so
     * @throws SQLException
     *             if an error occurs
     * @see Connection#disableAutoClose
     */
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    /**
     * Can statements remain open across commits?
     * 
     * @return true if so
     * @throws SQLException
     *             if an error occurs
     * @see Connection#disableAutoClose
     */
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    /**
     * Can statements remain open across rollbacks?
     * 
     * @return true if so
     * @throws SQLException
     *             if an error occurs
     * @see Connection#disableAutoClose
     */
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    /**
     * Can an "ORDER BY" clause use columns not in the SELECT?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    /**
     * Is some form of outer join supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    /**
     * Is positioned DELETE supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    /**
     * Is positioned UPDATE supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    /**
     * JDBC 2.0 Does the database support the concurrency type in combination
     * with the given result set type?
     * 
     * @param type
     *            defined in java.sql.ResultSet
     * @param concurrency
     *            type defined in java.sql.ResultSet
     * @return true if so
     * @exception SQLException
     *                if a database-access error occurs.
     * @see Connection
     */
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        switch (type) {
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
                if ((concurrency == ResultSet.CONCUR_READ_ONLY) || (concurrency == ResultSet.CONCUR_UPDATABLE)) {
                    return true;
                }
                throw SQLError.createSQLException("Illegal arguments to supportsResultSetConcurrency()", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());

            case ResultSet.TYPE_FORWARD_ONLY:
                if ((concurrency == ResultSet.CONCUR_READ_ONLY) || (concurrency == ResultSet.CONCUR_UPDATABLE)) {
                    return true;
                }
                throw SQLError.createSQLException("Illegal arguments to supportsResultSetConcurrency()", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());

            case ResultSet.TYPE_SCROLL_SENSITIVE:
                return false;
            default:
                throw SQLError.createSQLException("Illegal arguments to supportsResultSetConcurrency()", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
        }

    }

    /**
     * @see DatabaseMetaData#supportsResultSetHoldability(int)
     */
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * JDBC 2.0 Does the database support the given result set type?
     * 
     * @param type
     *            defined in java.sql.ResultSet
     * @return true if so
     * @exception SQLException
     *                if a database-access error occurs.
     * @see Connection
     */
    public boolean supportsResultSetType(int type) throws SQLException {
        return (type == ResultSet.TYPE_SCROLL_INSENSITIVE);
    }

    /**
     * @see DatabaseMetaData#supportsSavepoints()
     */
    public boolean supportsSavepoints() throws SQLException {

        return (this.conn.versionMeetsMinimum(4, 0, 14) || this.conn.versionMeetsMinimum(4, 1, 1));
    }

    /**
     * Can a schema name be used in a data manipulation statement?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    /**
     * Can a schema name be used in an index definition statement?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    /**
     * Can a schema name be used in a privilege definition statement?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    /**
     * Can a schema name be used in a procedure call statement?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    /**
     * Can a schema name be used in a table definition statement?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    /**
     * Is SELECT for UPDATE supported?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsSelectForUpdate() throws SQLException {
        return this.conn.versionMeetsMinimum(4, 0, 0);
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
     * @throws SQLException
     */
    public boolean supportsStoredProcedures() throws SQLException {
        return this.conn.versionMeetsMinimum(5, 0, 0);
    }

    /**
     * Are subqueries in comparison expressions supported? A JDBC compliant
     * driver always returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return this.conn.versionMeetsMinimum(4, 1, 0);
    }

    /**
     * Are subqueries in exists expressions supported? A JDBC compliant driver
     * always returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsSubqueriesInExists() throws SQLException {
        return this.conn.versionMeetsMinimum(4, 1, 0);
    }

    /**
     * Are subqueries in "in" statements supported? A JDBC compliant driver
     * always returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsSubqueriesInIns() throws SQLException {
        return this.conn.versionMeetsMinimum(4, 1, 0);
    }

    /**
     * Are subqueries in quantified expressions supported? A JDBC compliant
     * driver always returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return this.conn.versionMeetsMinimum(4, 1, 0);
    }

    /**
     * Are table correlation names supported? A JDBC compliant driver always
     * returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    /**
     * Does the database support the given transaction isolation level?
     * 
     * @param level
     *            the values are defined in java.sql.Connection
     * @return true if so
     * @throws SQLException
     *             if a database access error occurs
     * @see Connection
     */
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
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
        }

        return false;
    }

    /**
     * Are transactions supported? If not, commit is a noop and the isolation
     * level is TRANSACTION_NONE.
     * 
     * @return true if transactions are supported
     * @throws SQLException
     */
    public boolean supportsTransactions() throws SQLException {
        return this.conn.supportsTransactions();
    }

    /**
     * Is SQL UNION supported? A JDBC compliant driver always returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsUnion() throws SQLException {
        return this.conn.versionMeetsMinimum(4, 0, 0);
    }

    /**
     * Is SQL UNION ALL supported? A JDBC compliant driver always returns true.
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean supportsUnionAll() throws SQLException {
        return this.conn.versionMeetsMinimum(4, 0, 0);
    }

    /**
     * JDBC 2.0 Determine whether or not a visible row update can be detected by
     * calling ResultSet.rowUpdated().
     * 
     * @param type
     *            set type, i.e. ResultSet.TYPE_XXX
     * @return true if changes are detected by the resultset type
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * Does the database use a file for each table?
     * 
     * @return true if the database uses a local file for each table
     * @throws SQLException
     */
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    /**
     * Does the database store tables in a local file?
     * 
     * @return true if so
     * @throws SQLException
     */
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    //
    // JDBC-4.0 functions that aren't reliant on Java6
    //

    /**
     * Retrieves a list of the client info properties that the driver supports. The result set contains the following
     * columns
     * <p>
     * <ol>
     * <li><b>NAME</b> String=> The name of the client info property<br>
     * <li><b>MAX_LEN</b> int=> The maximum length of the value for the property<br>
     * <li><b>DEFAULT_VALUE</b> String=> The default value of the property<br>
     * <li><b>DESCRIPTION</b> String=> A description of the property. This will typically contain information as to where this property is stored in the
     * database.
     * </ol>
     * <p>
     * The <code>ResultSet</code> is sorted by the NAME column
     * <p>
     * 
     * @return A <code>ResultSet</code> object; each row is a supported client info property
     *         <p>
     * @exception SQLException
     *                if a database access error occurs
     *                <p>
     * @since 1.6
     */
    public ResultSet getClientInfoProperties() throws SQLException {
        // We don't have any built-ins, we actually support whatever the client wants to provide, however we don't have a way to express this with the interface
        // given
        Field[] fields = new Field[4];
        fields[0] = new Field("", "NAME", Types.VARCHAR, 255);
        fields[1] = new Field("", "MAX_LEN", Types.INTEGER, 10);
        fields[2] = new Field("", "DEFAULT_VALUE", Types.VARCHAR, 255);
        fields[3] = new Field("", "DESCRIPTION", Types.VARCHAR, 255);

        return buildResultSet(fields, new ArrayList<ResultSetRow>(), this.conn);
    }

    /**
     * Retrieves a description of the given catalog's system or user
     * function parameters and return type.
     * 
     * @see java.sql.DatabaseMetaData#getFunctionColumns(String, String, String, String)
     * @since 1.6
     */
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        Field[] fields = createFunctionColumnsFields();

        return getProcedureOrFunctionColumns(fields, catalog, schemaPattern, functionNamePattern, columnNamePattern, false, true);
    }

    protected Field[] createFunctionColumnsFields() {
        Field[] fields = { new Field("", "FUNCTION_CAT", Types.VARCHAR, 512), new Field("", "FUNCTION_SCHEM", Types.VARCHAR, 512),
                new Field("", "FUNCTION_NAME", Types.VARCHAR, 512), new Field("", "COLUMN_NAME", Types.VARCHAR, 512),
                new Field("", "COLUMN_TYPE", Types.VARCHAR, 64), new Field("", "DATA_TYPE", Types.SMALLINT, 6), new Field("", "TYPE_NAME", Types.VARCHAR, 64),
                new Field("", "PRECISION", Types.INTEGER, 12), new Field("", "LENGTH", Types.INTEGER, 12), new Field("", "SCALE", Types.SMALLINT, 12),
                new Field("", "RADIX", Types.SMALLINT, 6), new Field("", "NULLABLE", Types.SMALLINT, 6), new Field("", "REMARKS", Types.VARCHAR, 512),
                new Field("", "CHAR_OCTET_LENGTH", Types.INTEGER, 32), new Field("", "ORDINAL_POSITION", Types.INTEGER, 32),
                new Field("", "IS_NULLABLE", Types.VARCHAR, 12), new Field("", "SPECIFIC_NAME", Types.VARCHAR, 64) };
        return fields;
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
    public java.sql.ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        Field[] fields = new Field[6];

        fields[0] = new Field("", "FUNCTION_CAT", Types.CHAR, 255);
        fields[1] = new Field("", "FUNCTION_SCHEM", Types.CHAR, 255);
        fields[2] = new Field("", "FUNCTION_NAME", Types.CHAR, 255);
        fields[3] = new Field("", "REMARKS", Types.CHAR, 255);
        fields[4] = new Field("", "FUNCTION_TYPE", Types.SMALLINT, 6);
        fields[5] = new Field("", "SPECIFIC_NAME", Types.CHAR, 255);

        return getProceduresAndOrFunctions(fields, catalog, schemaPattern, functionNamePattern, false, true);
    }

    public boolean providesQueryObjectGenerator() throws SQLException {
        return false;
    }

    /**
     * @param catalog
     * @param schemaPattern
     * @throws SQLException
     */
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        Field[] fields = { new Field("", "TABLE_SCHEM", Types.VARCHAR, 255), new Field("", "TABLE_CATALOG", Types.VARCHAR, 255) };

        return buildResultSet(fields, new ArrayList<ResultSetRow>());
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }

    /**
     * Get a prepared statement to query information_schema tables.
     * 
     * @return PreparedStatement
     * @throws SQLException
     */
    protected java.sql.PreparedStatement prepareMetaDataSafeStatement(String sql) throws SQLException {
        // Can't use server-side here as we coerce a lot of types to match the spec.
        java.sql.PreparedStatement pStmt = this.conn.clientPrepareStatement(sql);

        if (pStmt.getMaxRows() != 0) {
            pStmt.setMaxRows(0);
        }

        ((com.mysql.jdbc.Statement) pStmt).setHoldResultsOpenOverClose(true);

        return pStmt;
    }

    /**
     * JDBC-4.1
     * 
     * @param catalog
     * @param schemaPattern
     * @param tableNamePattern
     * @param columnNamePattern
     * @throws SQLException
     */
    public java.sql.ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        Field[] fields = { new Field("", "TABLE_CAT", Types.VARCHAR, 512), new Field("", "TABLE_SCHEM", Types.VARCHAR, 512),
                new Field("", "TABLE_NAME", Types.VARCHAR, 512), new Field("", "COLUMN_NAME", Types.VARCHAR, 512),
                new Field("", "DATA_TYPE", Types.INTEGER, 12), new Field("", "COLUMN_SIZE", Types.INTEGER, 12),
                new Field("", "DECIMAL_DIGITS", Types.INTEGER, 12), new Field("", "NUM_PREC_RADIX", Types.INTEGER, 12),
                new Field("", "COLUMN_USAGE", Types.VARCHAR, 512), new Field("", "REMARKS", Types.VARCHAR, 512),
                new Field("", "CHAR_OCTET_LENGTH", Types.INTEGER, 12), new Field("", "IS_NULLABLE", Types.VARCHAR, 512) };

        return buildResultSet(fields, new ArrayList<ResultSetRow>());
    }

    // JDBC-4.1
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }
}
