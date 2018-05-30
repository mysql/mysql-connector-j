/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
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

import static com.mysql.cj.jdbc.DatabaseMetaData.ProcedureType.FUNCTION;
import static com.mysql.cj.jdbc.DatabaseMetaData.ProcedureType.PROCEDURE;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
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

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.NativeSession;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.exceptions.AssertionFailedException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.result.ResultSetFactory;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ResultsetRow;
import com.mysql.cj.protocol.a.result.ByteArrayRow;
import com.mysql.cj.protocol.a.result.ResultsetRowsStatic;
import com.mysql.cj.result.DefaultColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.Row;
import com.mysql.cj.util.StringUtils;

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

    /**
     * Default max buffer size. See {@link PropertyDefinitions#PNAME_maxAllowedPacket}.
     */
    protected static int maxBufferSize = 65535; // TODO find a way to use actual (not default) value

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

        Integer columnSize = null;

        Integer decimalDigits = null;

        String isNullable;

        int nullability;

        int numPrecRadix = 10;

        String mysqlTypeName;
        MysqlType mysqlType;

        TypeDescriptor(String typeInfo, String nullabilityInfo) throws SQLException {
            if (typeInfo == null) {
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.0"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }

            this.mysqlType = MysqlType.getByName(typeInfo);

            // Figure Out the Size

            String temp;
            java.util.StringTokenizer tokenizer;
            int maxLength = 0;

            switch (this.mysqlType) {
                case ENUM:
                    temp = typeInfo.substring(typeInfo.indexOf("(") + 1, typeInfo.lastIndexOf(")"));
                    tokenizer = new java.util.StringTokenizer(temp, ",");
                    while (tokenizer.hasMoreTokens()) {
                        String nextToken = tokenizer.nextToken();
                        maxLength = Math.max(maxLength, (nextToken.length() - 2));
                    }
                    this.columnSize = Integer.valueOf(maxLength);
                    break;

                case SET:
                    temp = typeInfo.substring(typeInfo.indexOf("(") + 1, typeInfo.lastIndexOf(")"));
                    tokenizer = new java.util.StringTokenizer(temp, ",");

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
                    break;

                case DECIMAL:
                case DECIMAL_UNSIGNED:
                case FLOAT:
                case FLOAT_UNSIGNED:
                case DOUBLE:
                case DOUBLE_UNSIGNED:
                    if (typeInfo.indexOf(",") != -1) {
                        // Numeric with decimals
                        this.columnSize = Integer.valueOf(typeInfo.substring((typeInfo.indexOf("(") + 1), (typeInfo.indexOf(","))).trim());
                        this.decimalDigits = Integer.valueOf(typeInfo.substring((typeInfo.indexOf(",") + 1), (typeInfo.indexOf(")"))).trim());
                    } else {
                        switch (this.mysqlType) {
                            case DECIMAL:
                            case DECIMAL_UNSIGNED:
                                this.columnSize = Integer.valueOf(65);
                                break;
                            case FLOAT:
                            case FLOAT_UNSIGNED:
                                this.columnSize = Integer.valueOf(12);
                                break;
                            case DOUBLE:
                            case DOUBLE_UNSIGNED:
                                this.columnSize = Integer.valueOf(22);
                                break;
                            default:
                                break;
                        }
                        this.decimalDigits = 0;
                    }
                    break;

                case CHAR:
                case VARCHAR:
                case TINYTEXT:
                case MEDIUMTEXT:
                case LONGTEXT:
                case JSON:
                case TEXT:
                case TINYBLOB:
                case MEDIUMBLOB:
                case LONGBLOB:
                case BLOB:
                case BINARY:
                case VARBINARY:
                case BIT:
                    if (this.mysqlType == MysqlType.CHAR) {
                        this.columnSize = Integer.valueOf(1);
                    }
                    if (typeInfo.indexOf("(") != -1) {
                        int endParenIndex = typeInfo.indexOf(")");

                        if (endParenIndex == -1) {
                            endParenIndex = typeInfo.length();
                        }

                        this.columnSize = Integer.valueOf(typeInfo.substring((typeInfo.indexOf("(") + 1), endParenIndex).trim());

                        // Adjust for pseudo-boolean
                        if (DatabaseMetaData.this.tinyInt1isBit && this.columnSize.intValue() == 1
                                && StringUtils.startsWithIgnoreCase(typeInfo, 0, "tinyint")) {
                            if (DatabaseMetaData.this.transformedBitIsBoolean) {
                                this.mysqlType = MysqlType.BOOLEAN;
                            } else {
                                this.mysqlType = MysqlType.BIT;
                            }
                        }
                    }

                    break;

                case TINYINT:
                case TINYINT_UNSIGNED:
                    if (DatabaseMetaData.this.tinyInt1isBit && typeInfo.indexOf("(1)") != -1) {
                        if (DatabaseMetaData.this.transformedBitIsBoolean) {
                            this.mysqlType = MysqlType.BOOLEAN;
                        } else {
                            this.mysqlType = MysqlType.BIT;
                        }
                    } else {
                        this.columnSize = Integer.valueOf(3);
                    }
                    break;

                case BOOLEAN:
                case GEOMETRY:
                case NULL:
                case UNKNOWN:
                case YEAR:

                default:
            }

            // if not defined explicitly take the max precision
            if (this.columnSize == null) {
                // JDBC spec reserved only 'int' type for precision, thus we need to cut longer values
                this.columnSize = this.mysqlType.getPrecision() > Integer.MAX_VALUE ? Integer.MAX_VALUE : this.mysqlType.getPrecision().intValue();
            }

            // BUFFER_LENGTH
            this.bufferLength = maxBufferSize;

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

        @Override
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

        @Override
        public int hashCode() {
            assert false : "hashCode not designed";
            return 0;
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

        @Override
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

        @Override
        public int hashCode() {
            assert false : "hashCode not designed";
            return 0;
        }
    }

    /**
     * Helper/wrapper class to provide means of sorting objects by using a sorting key.
     * 
     * @param <K>
     *            key type
     * @param <V>
     *            value type
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
        public int hashCode() {
            assert false : "hashCode not designed";
            return 0;
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
        LOCAL_TEMPORARY("LOCAL TEMPORARY"), SYSTEM_TABLE("SYSTEM TABLE"), SYSTEM_VIEW("SYSTEM VIEW"), TABLE("TABLE", new String[] { "BASE TABLE" }),
        VIEW("VIEW"), UNKNOWN("UNKNOWN");

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

    // MySQL reserved words (all versions superset)
    private static final String[] MYSQL_KEYWORDS = new String[] { "ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE", "BEFORE",
            "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN",
            "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
            "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL", "DECLARE",
            "DEFAULT", "DELAYED", "DELETE", "DENSE_RANK", "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL",
            "EACH", "ELSE", "ELSEIF", "EMPTY", "ENCLOSED", "ESCAPED", "EXCEPT", "EXISTS", "EXIT", "EXPLAIN", "FALSE", "FETCH", "FIRST_VALUE", "FLOAT", "FLOAT4",
            "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT", "FUNCTION", "GENERATED", "GET", "GRANT", "GROUP", "GROUPING", "GROUPS", "HAVING",
            "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE",
            "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERVAL", "INTO", "IO_AFTER_GTIDS", "IO_BEFORE_GTIDS", "IS", "ITERATE",
            "JOIN", "JSON_TABLE", "KEY", "KEYS", "KILL", "LAG", "LAST_VALUE", "LEAD", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINEAR", "LINES", "LOAD",
            "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY", "MASTER_BIND", "MASTER_SSL_VERIFY_SERVER_CERT",
            "MATCH", "MAXVALUE", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES", "NATURAL",
            "NOT", "NO_WRITE_TO_BINLOG", "NTH_VALUE", "NTILE", "NULL", "NUMERIC", "OF", "ON", "OPTIMIZE", "OPTIMIZER_COSTS", "OPTION", "OPTIONALLY", "OR",
            "ORDER", "OUT", "OUTER", "OUTFILE", "OVER", "PARTITION", "PERCENT_RANK", "PERSIST", "PERSIST_ONLY", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE",
            "RANGE", "RANK", "READ", "READS", "READ_WRITE", "REAL", "RECURSIVE", "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE",
            "RESIGNAL", "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE", "ROW", "ROWS", "ROW_NUMBER", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT",
            "SENSITIVE", "SEPARATOR", "SET", "SHOW", "SIGNAL", "SMALLINT", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING",
            "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STORED", "STRAIGHT_JOIN", "SYSTEM", "TABLE", "TERMINATED", "THEN",
            "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING", "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE",
            "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "VIRTUAL", "WHEN", "WHERE", "WHILE",
            "WINDOW", "WITH", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL" };

    // SQL:2003 reserved words from 'ISO/IEC 9075-2:2003 (E), 2003-07-25'
    /* package private */ static final List<String> SQL2003_KEYWORDS = Arrays.asList("ABS", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ARRAY", "AS",
            "ASENSITIVE", "ASYMMETRIC", "AT", "ATOMIC", "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOOLEAN", "BOTH", "BY", "CALL",
            "CALLED", "CARDINALITY", "CASCADED", "CASE", "CAST", "CEIL", "CEILING", "CHAR", "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOB",
            "CLOSE", "COALESCE", "COLLATE", "COLLECT", "COLUMN", "COMMIT", "CONDITION", "CONNECT", "CONSTRAINT", "CONVERT", "CORR", "CORRESPONDING", "COUNT",
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
            "REGR_SXY", "REGR_SYY", "RELEASE", "RESULT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "ROW_NUMBER", "SAVEPOINT",
            "SCOPE", "SCROLL", "SEARCH", "SECOND", "SELECT", "SENSITIVE", "SESSION_USER", "SET", "SIMILAR", "SMALLINT", "SOME", "SPECIFIC", "SPECIFICTYPE",
            "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQRT", "START", "STATIC", "STDDEV_POP", "STDDEV_SAMP", "SUBMULTISET", "SUBSTRING", "SUM",
            "SYMMETRIC", "SYSTEM", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING",
            "TRANSLATE", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE", "UESCAPE", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "UPPER", "USER",
            "USING", "VALUE", "VALUES", "VARCHAR", "VARYING", "VAR_POP", "VAR_SAMP", "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN",
            "WITHOUT", "YEAR");

    private static volatile String mysqlKeywords = null;

    /** The connection to the database */
    protected JdbcConnection conn;

    protected NativeSession session;

    /** The 'current' database name being used */
    protected String database = null;

    /** What character to use when quoting identifiers */
    protected final String quotedId;

    protected boolean nullCatalogMeansCurrent;
    protected boolean pedantic;
    protected boolean tinyInt1isBit;
    protected boolean transformedBitIsBoolean;
    protected boolean useHostsInPrivileges;

    protected ResultSetFactory resultSetFactory;

    private String metadataEncoding;
    private int metadataCollationIndex;

    protected static DatabaseMetaData getInstance(JdbcConnection connToSet, String databaseToSet, boolean checkForInfoSchema, ResultSetFactory resultSetFactory)
            throws SQLException {
        if (checkForInfoSchema && connToSet.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_useInformationSchema).getValue()) {
            return new DatabaseMetaDataUsingInfoSchema(connToSet, databaseToSet, resultSetFactory);
        }

        return new DatabaseMetaData(connToSet, databaseToSet, resultSetFactory);
    }

    /**
     * Creates a new DatabaseMetaData object.
     * 
     * @param connToSet
     *            Connection object
     * @param databaseToSet
     *            database name
     * @param resultSetFactory
     *            {@link ResultSetFactory}
     */
    protected DatabaseMetaData(JdbcConnection connToSet, String databaseToSet, ResultSetFactory resultSetFactory) {
        this.conn = connToSet;
        this.session = (NativeSession) connToSet.getSession();
        this.database = databaseToSet;
        this.resultSetFactory = resultSetFactory;
        this.exceptionInterceptor = this.conn.getExceptionInterceptor();
        this.nullCatalogMeansCurrent = this.conn.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent).getValue();
        this.pedantic = this.conn.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_pedantic).getValue();
        this.tinyInt1isBit = this.conn.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_tinyInt1isBit).getValue();
        this.transformedBitIsBoolean = this.conn.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean).getValue();
        this.useHostsInPrivileges = this.conn.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_useHostsInPrivileges).getValue();
        this.quotedId = this.session.getIdentifierQuoteString();
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    protected void convertToJdbcFunctionList(String catalog, ResultSet proceduresRs, boolean needsClientFiltering, String db,
            List<ComparableWrapper<String, Row>> procedureRows, int nameIndex, Field[] fields) throws SQLException {
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
                    rowData[4] = s2b(Integer.toString(getFunctionNoTableConstant())); // FUNCTION_TYPE
                    rowData[5] = s2b(functionName);                      // SPECFIC NAME
                }

                procedureRows.add(
                        new ComparableWrapper<String, Row>(getFullyQualifiedName(catalog, functionName), new ByteArrayRow(rowData, getExceptionInterceptor())));
            }
        }
    }

    /**
     * Builds and returns a fully qualified name, quoted if necessary, for the given catalog and database entity.
     * 
     * @param catalog
     *            database name
     * @param entity
     *            identifier
     * @return fully qualified name
     */
    protected String getFullyQualifiedName(String catalog, String entity) {
        StringBuilder fullyQualifiedName = new StringBuilder(StringUtils.quoteIdentifier(catalog == null ? "" : catalog, this.quotedId, this.pedantic));
        fullyQualifiedName.append('.');
        fullyQualifiedName.append(StringUtils.quoteIdentifier(entity, this.quotedId, this.pedantic));
        return fullyQualifiedName.toString();
    }

    /**
     * Getter to DatabaseMetaData.functionNoTable constant.
     * 
     * @return java.sql.DatabaseMetaData#functionNoTable
     */
    protected int getFunctionNoTableConstant() {
        return functionNoTable;
    }

    protected void convertToJdbcProcedureList(boolean fromSelect, String catalog, ResultSet proceduresRs, boolean needsClientFiltering, String db,
            List<ComparableWrapper<String, Row>> procedureRows, int nameIndex) throws SQLException {
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

                procedureRows.add(new ComparableWrapper<String, Row>(getFullyQualifiedName(catalog, procedureName),
                        new ByteArrayRow(rowData, getExceptionInterceptor())));
            }
        }
    }

    private Row convertTypeDescriptorToProcedureRow(byte[] procNameAsBytes, byte[] procCatAsBytes, String paramName, boolean isOutParam, boolean isInParam,
            boolean isReturnParam, TypeDescriptor typeDesc, boolean forGetFunctionColumns, int ordinal) throws SQLException {
        byte[][] row = forGetFunctionColumns ? new byte[17][] : new byte[20][];
        row[0] = procCatAsBytes;                                                                                    // PROCEDURE_CAT
        row[1] = null;                                                                                              // PROCEDURE_SCHEM
        row[2] = procNameAsBytes;                                                                                   // PROCEDURE/NAME
        row[3] = s2b(paramName);                                                                                    // COLUMN_NAME
        row[4] = s2b(String.valueOf(getColumnType(isOutParam, isInParam, isReturnParam, forGetFunctionColumns)));   // COLUMN_TYPE
        row[5] = s2b(Short.toString((short) typeDesc.mysqlType.getJdbcType()));                                                            // DATA_TYPE
        row[6] = s2b(typeDesc.mysqlType.getName());                                                                            // TYPE_NAME
        row[7] = typeDesc.columnSize == null ? null : s2b(typeDesc.columnSize.toString());                          // PRECISION
        row[8] = row[7];                                                                                            // LENGTH
        row[9] = typeDesc.decimalDigits == null ? null : s2b(typeDesc.decimalDigits.toString());                    // SCALE
        row[10] = s2b(Integer.toString(typeDesc.numPrecRadix));                                                     // RADIX
        // Map 'column****' to 'procedure****'
        switch (typeDesc.nullability) {
            case columnNoNulls:
                row[11] = s2b(String.valueOf(procedureNoNulls));                                                    // NULLABLE
                break;

            case columnNullable:
                row[11] = s2b(String.valueOf(procedureNullable));                                                   // NULLABLE
                break;

            case columnNullableUnknown:
                row[11] = s2b(String.valueOf(procedureNullableUnknown));                                            // NULLABLE
                break;

            default:
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.1"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
        }

        row[12] = null;

        if (forGetFunctionColumns) {
            row[13] = null;                                                                                         // CHAR_OCTECT_LENGTH
            row[14] = s2b(String.valueOf(ordinal));                                                                 // ORDINAL_POSITION
            row[15] = s2b(typeDesc.isNullable);                                                                     // IS_NULLABLE
            row[16] = procNameAsBytes;                                                                              // SPECIFIC_NAME

        } else {
            row[13] = null;                                                                                         // COLUMN_DEF
            row[14] = null;                                                                                         // SQL_DATA_TYPE (future use)
            row[15] = null;                                                                                         // SQL_DATETIME_SUB (future use)
            row[16] = null;                                                                                         // CHAR_OCTET_LENGTH
            row[17] = s2b(String.valueOf(ordinal));                                                                 // ORDINAL_POSITION
            row[18] = s2b(typeDesc.isNullable);                                                                     // IS_NULLABLE
            row[19] = procNameAsBytes;                                                                              // SPECIFIC_NAME
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
        return getProcedureOrFunctionColumnType(isOutParam, isInParam, isReturnParam, forGetFunctionColumns);
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
     *            Indicates whether the column belong to a function.
     * 
     * @return The corresponding COLUMN_TYPE as in java.sql.getProcedureColumns API.
     */
    protected static int getProcedureOrFunctionColumnType(boolean isOutParam, boolean isInParam, boolean isReturnParam, boolean forGetFunctionColumns) {

        if (isInParam && isOutParam) {
            return forGetFunctionColumns ? functionColumnInOut : procedureColumnInOut;
        } else if (isInParam) {
            return forGetFunctionColumns ? functionColumnIn : procedureColumnIn;
        } else if (isOutParam) {
            return forGetFunctionColumns ? functionColumnOut : procedureColumnOut;
        } else if (isReturnParam) {
            return forGetFunctionColumns ? functionReturn : procedureColumnReturn;
        } else {
            return forGetFunctionColumns ? functionColumnUnknown : procedureColumnUnknown;
        }
    }

    private ExceptionInterceptor exceptionInterceptor;

    protected ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return true;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
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
    public List<Row> extractForeignKeyForTable(ArrayList<Row> rows, java.sql.ResultSet rs, String catalog) throws SQLException {
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
                String referencedCatalogName = StringUtils.quoteIdentifier(catalog, this.quotedId, this.pedantic);
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
     * @param catalog
     *            the database name to extract foreign key info for
     * @param tableName
     *            the table to extract foreign key info for
     * @return A result set that has the structure of 'show table status'
     * @throws SQLException
     *             if a database access error occurs.
     */
    public ResultSet extractForeignKeyFromCreateTable(String catalog, String tableName) throws SQLException {
        ArrayList<String> tableList = new ArrayList<>();
        java.sql.ResultSet rs = null;
        java.sql.Statement stmt = null;

        if (tableName != null) {
            tableList.add(tableName);
        } else {
            try {
                rs = getTables(catalog, null, null, new String[] { "TABLE" });

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

        ArrayList<Row> rows = new ArrayList<>();
        Field[] fields = new Field[3];
        fields[0] = new Field("", "Name", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, Integer.MAX_VALUE);
        fields[1] = new Field("", "Type", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[2] = new Field("", "Comment", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, Integer.MAX_VALUE);

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

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(rows, new DefaultColumnDefinition(fields)));
    }

    @Override
    public java.sql.ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3) throws SQLException {
        Field[] fields = new Field[21];
        fields[0] = new Field("", "TYPE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[1] = new Field("", "TYPE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[2] = new Field("", "TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[3] = new Field("", "ATTR_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[4] = new Field("", "DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 32);
        fields[5] = new Field("", "ATTR_TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[6] = new Field("", "ATTR_SIZE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32);
        fields[7] = new Field("", "DECIMAL_DIGITS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32);
        fields[8] = new Field("", "NUM_PREC_RADIX", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32);
        fields[9] = new Field("", "NULLABLE ", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32);
        fields[10] = new Field("", "REMARKS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[11] = new Field("", "ATTR_DEF", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[12] = new Field("", "SQL_DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32);
        fields[13] = new Field("", "SQL_DATETIME_SUB", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32);
        fields[14] = new Field("", "CHAR_OCTET_LENGTH", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32);
        fields[15] = new Field("", "ORDINAL_POSITION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32);
        fields[16] = new Field("", "IS_NULLABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[17] = new Field("", "SCOPE_CATALOG", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[18] = new Field("", "SCOPE_SCHEMA", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[19] = new Field("", "SCOPE_TABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[20] = new Field("", "SOURCE_DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 32);

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(new ArrayList<ResultsetRow>(), new DefaultColumnDefinition(fields)));
    }

    @Override
    public java.sql.ResultSet getBestRowIdentifier(String catalog, String schema, final String table, int scope, boolean nullable) throws SQLException {
        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        Field[] fields = new Field[8];
        fields[0] = new Field("", "SCOPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 5);
        fields[1] = new Field("", "COLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[2] = new Field("", "DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32);
        fields[3] = new Field("", "TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[4] = new Field("", "COLUMN_SIZE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[5] = new Field("", "BUFFER_LENGTH", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[6] = new Field("", "DECIMAL_DIGITS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 10);
        fields[7] = new Field("", "PSEUDO_COLUMN", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 5);

        final ArrayList<Row> rows = new ArrayList<>();
        final Statement stmt = this.conn.getMetadataSafeStatement();

        try {

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {
                    ResultSet results = null;

                    try {
                        StringBuilder queryBuf = new StringBuilder("SHOW COLUMNS FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(table, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));
                        queryBuf.append(" FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));

                        results = stmt.executeQuery(queryBuf.toString());

                        while (results.next()) {
                            String keyType = results.getString("Key");

                            if (keyType != null) {
                                if (StringUtils.startsWithIgnoreCase(keyType, "PRI")) {
                                    byte[][] rowVal = new byte[8][];
                                    rowVal[0] = Integer.toString(java.sql.DatabaseMetaData.bestRowSession).getBytes();
                                    rowVal[1] = results.getBytes("Field");

                                    String type = results.getString("Type");
                                    int size = stmt.getMaxFieldSize();
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

                                    MysqlType ft = MysqlType.getByName(type.toUpperCase());
                                    rowVal[2] = s2b(String.valueOf(ft.getJdbcType()));
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
                        if (!MysqlErrorNumbers.SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND.equals(sqlEx.getSQLState())) {
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

        java.sql.ResultSet results = this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(rows, new DefaultColumnDefinition(fields)));

        return results;

    }

    /*
     * Extract parameter details for Procedures and Functions by parsing the DDL query obtained from SHOW CREATE [PROCEDURE|FUNCTION] ... statements.
     * The result rows returned follow the required structure for getProcedureColumns() and getFunctionColumns() methods.
     * 
     * Internal use only.
     */
    private void getCallStmtParameterTypes(String catalog, String quotedProcName, ProcedureType procType, String parameterNamePattern, List<Row> resultRows,
            boolean forGetFunctionColumns) throws SQLException {
        java.sql.Statement paramRetrievalStmt = null;
        java.sql.ResultSet paramRetrievalRs = null;

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
                        this.session.getServerSession().isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);
            } else {
                dotIndex = quotedProcName.indexOf(".");
            }

            String dbName = null;

            if (dotIndex != -1 && (dotIndex + 1) < quotedProcName.length()) {
                dbName = quotedProcName.substring(0, dotIndex);
                quotedProcName = quotedProcName.substring(dotIndex + 1);
            } else {
                dbName = StringUtils.quoteIdentifier(catalog, this.quotedId, this.pedantic);
            }

            // Moved from above so that procName is *without* database as expected by the rest of code
            // Removing QuoteChar to get output as it was before PROC_CAT fixes
            String tmpProcName = StringUtils.unQuoteIdentifier(quotedProcName, this.quotedId);
            procNameAsBytes = StringUtils.getBytes(tmpProcName, "UTF-8");

            tmpProcName = StringUtils.unQuoteIdentifier(dbName, this.quotedId);
            procCatAsBytes = StringUtils.getBytes(tmpProcName, "UTF-8");

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

                if (!this.conn.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies).getValue()
                        && (procedureDef == null || procedureDef.length() == 0)) {
                    throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.4"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                            getExceptionInterceptor());
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
                            this.session.getServerSession().isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);
                    int endOfParamDeclarationIndex = 0;

                    endOfParamDeclarationIndex = endPositionOfParameterDeclaration(openParenIndex, procedureDef, this.quotedId);

                    if (procType == FUNCTION) {

                        // Grab the return column since it needs
                        // to go first in the output result set
                        int returnsIndex = StringUtils.indexOfIgnoreCase(0, procedureDef, " RETURNS ", this.quotedId, this.quotedId,
                                this.session.getServerSession().isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);

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
                        throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.5"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
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
                            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.6"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                                    getExceptionInterceptor());
                        }
                    } else if (possibleParamName.equalsIgnoreCase("INOUT")) {
                        isOutParam = true;
                        isInParam = true;

                        if (declarationTok.hasMoreTokens()) {
                            paramName = declarationTok.nextToken();
                        } else {
                            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.6"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                                    getExceptionInterceptor());
                        }
                    } else if (possibleParamName.equalsIgnoreCase("IN")) {
                        isOutParam = false;
                        isInParam = true;

                        if (declarationTok.hasMoreTokens()) {
                            paramName = declarationTok.nextToken();
                        } else {
                            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.6"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                                    getExceptionInterceptor());
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
                        throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.7"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                                getExceptionInterceptor());
                    }

                    if ((paramName.startsWith("`") && paramName.endsWith("`"))
                            || (isProcedureInAnsiMode && paramName.startsWith("\"") && paramName.endsWith("\""))) {
                        paramName = paramName.substring(1, paramName.length() - 1);
                    }

                    if (parameterNamePattern == null || StringUtils.wildCompareIgnoreCase(paramName, parameterNamePattern)) {
                        Row row = convertTypeDescriptorToProcedureRow(procNameAsBytes, procCatAsBytes, paramName, isOutParam, isInParam, false, typeDesc,
                                forGetFunctionColumns, ordinal++);

                        resultRows.add(row);
                    }
                } else {
                    throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.8"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                            getExceptionInterceptor());
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
                    this.session.getServerSession().isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);

            if (closedParenIndex != -1) {
                int nextOpenParenIndex = StringUtils.indexOfIgnoreCase(currentPos, procedureDef, "(", quoteChar, quoteChar,
                        this.session.getServerSession().isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);

                if (nextOpenParenIndex != -1 && nextOpenParenIndex < closedParenIndex) {
                    parenDepth++;
                    currentPos = closedParenIndex + 1; // set after closed paren that increases depth
                } else {
                    parenDepth--;
                    currentPos = closedParenIndex; // start search from same position
                }
            } else {
                // we should always get closed paren of some sort
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.5"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
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
                    this.session.getServerSession().isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);

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
                this.session.getServerSession().isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);

        if (endOfReturn != -1) {
            // seek back until whitespace
            for (int i = endOfReturn; i > 0; i--) {
                if (Character.isWhitespace(procedureDefn.charAt(i))) {
                    return i;
                }
            }
        }

        // We can't parse it.

        throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.5"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
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
            allCatalogsIter = new SingleStringIterator(this.pedantic ? catalogSpec : StringUtils.unQuoteIdentifier(catalogSpec, this.quotedId));
        } else if (this.nullCatalogMeansCurrent) {
            allCatalogsIter = new SingleStringIterator(this.database);
        } else {
            allCatalogsIter = new ResultSetIterator(getCatalogs(), 1);
        }

        return allCatalogsIter;
    }

    @Override
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

            List<String> resultsAsList = new ArrayList<>(catalogsCount);
            while (results.next()) {
                resultsAsList.add(results.getString(1));
            }
            Collections.sort(resultsAsList);

            Field[] fields = new Field[1];
            fields[0] = new Field("", "TABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR,
                    results.getMetaData().getColumnDisplaySize(1));

            ArrayList<Row> tuples = new ArrayList<>(catalogsCount);
            for (String cat : resultsAsList) {
                byte[][] rowVal = new byte[1][];
                rowVal[0] = s2b(cat);
                tuples.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
            }

            return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    new ResultsetRowsStatic(tuples, new DefaultColumnDefinition(fields)));
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

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
    }

    @Override
    public java.sql.ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        Field[] fields = new Field[8];
        fields[0] = new Field("", "TABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 64);
        fields[1] = new Field("", "TABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 1);
        fields[2] = new Field("", "TABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 64);
        fields[3] = new Field("", "COLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 64);
        fields[4] = new Field("", "GRANTOR", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 77);
        fields[5] = new Field("", "GRANTEE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 77);
        fields[6] = new Field("", "PRIVILEGE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 64);
        fields[7] = new Field("", "IS_GRANTABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 3);

        StringBuilder grantQueryBuf = new StringBuilder("SELECT c.host, c.db, t.grantor, c.user, c.table_name, c.column_name, c.column_priv");
        grantQueryBuf.append(" FROM mysql.columns_priv c, mysql.tables_priv t");
        grantQueryBuf.append(" WHERE c.host = t.host AND c.db = t.db AND c.table_name = t.table_name");
        if (catalog != null) {
            grantQueryBuf.append(" AND c.db LIKE ?");
        }
        grantQueryBuf.append(" AND c.table_name = ?");
        if (columnNamePattern != null) {
            grantQueryBuf.append(" AND c.column_name LIKE ?");
        }

        PreparedStatement pStmt = null;
        ResultSet results = null;
        ArrayList<Row> grantRows = new ArrayList<>();

        try {
            pStmt = prepareMetaDataSafeStatement(grantQueryBuf.toString());
            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            pStmt.setString(nextId++, table);
            if (columnNamePattern != null) {
                pStmt.setString(nextId, columnNamePattern);
            }

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

                if ((host != null) && this.useHostsInPrivileges) {
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
                        tuple[4] = grantor != null ? s2b(grantor) : null;
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

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(grantRows, new DefaultColumnDefinition(fields)));
    }

    @Override
    public java.sql.ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern, String columnNamePattern)
            throws SQLException {

        final String colPattern = columnNamePattern;

        Field[] fields = createColumnsFields();

        final ArrayList<Row> rows = new ArrayList<>();
        final Statement stmt = this.conn.getMetadataSafeStatement();

        try {

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {

                    ArrayList<String> tableNameList = new ArrayList<>();

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
                            } catch (Exception sqlEx) {
                                AssertionFailedException.shouldNotHappen(sqlEx);
                            }

                            tables = null;
                        }
                    }

                    for (String tableName : tableNameList) {

                        ResultSet results = null;

                        try {
                            StringBuilder queryBuf = new StringBuilder("SHOW FULL COLUMNS FROM ");
                            queryBuf.append(StringUtils.quoteIdentifier(tableName, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));
                            queryBuf.append(" FROM ");
                            queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));
                            if (colPattern != null) {
                                queryBuf.append(" LIKE ");
                                queryBuf.append(StringUtils.quoteIdentifier(colPattern, "'", true));
                            }

                            // Return correct ordinals if column name pattern is not '%'
                            // Currently, MySQL doesn't show enough data to do this, so we do it the 'hard' way...Once _SYSTEM tables are in, this should be
                            // much easier
                            boolean fixUpOrdinalsRequired = false;
                            Map<String, Integer> ordinalFixUpMap = null;

                            if (colPattern != null && !colPattern.equals("%")) {
                                fixUpOrdinalsRequired = true;

                                StringBuilder fullColumnQueryBuf = new StringBuilder("SHOW FULL COLUMNS FROM ");
                                fullColumnQueryBuf
                                        .append(StringUtils.quoteIdentifier(tableName, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));
                                fullColumnQueryBuf.append(" FROM ");
                                fullColumnQueryBuf
                                        .append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));

                                results = stmt.executeQuery(fullColumnQueryBuf.toString());

                                ordinalFixUpMap = new HashMap<>();

                                int fullOrdinalPos = 1;

                                while (results.next()) {
                                    String fullOrdColName = results.getString("Field");

                                    ordinalFixUpMap.put(fullOrdColName, Integer.valueOf(fullOrdinalPos++));
                                }
                                results.close();
                            }

                            results = stmt.executeQuery(queryBuf.toString());

                            int ordPos = 1;

                            while (results.next()) {
                                TypeDescriptor typeDesc = new TypeDescriptor(results.getString("Type"), results.getString("Null"));

                                byte[][] rowVal = new byte[24][];
                                rowVal[0] = s2b(catalogStr);                    // TABLE_CAT
                                rowVal[1] = null;                               // TABLE_SCHEM (No schemas in MySQL)
                                rowVal[2] = s2b(tableName);                     // TABLE_NAME
                                rowVal[3] = results.getBytes("Field");
                                rowVal[4] = Short.toString((short) typeDesc.mysqlType.getJdbcType()).getBytes();// DATA_TYPE (jdbc)
                                rowVal[5] = s2b(typeDesc.mysqlType.getName());  // TYPE_NAME (native)
                                if (typeDesc.columnSize == null) {              // COLUMN_SIZE
                                    rowVal[6] = null;
                                } else {
                                    String collation = results.getString("Collation");
                                    int mbminlen = 1;
                                    if (collation != null) {
                                        // not null collation could only be returned by server for character types, so we don't need to check type name
                                        if (collation.indexOf("ucs2") > -1 || collation.indexOf("utf16") > -1) {
                                            mbminlen = 2;
                                        } else if (collation.indexOf("utf32") > -1) {
                                            mbminlen = 4;
                                        }
                                    }
                                    rowVal[6] = mbminlen == 1 ? s2b(typeDesc.columnSize.toString())
                                            : s2b(((Integer) (typeDesc.columnSize / mbminlen)).toString());
                                }
                                rowVal[7] = s2b(Integer.toString(typeDesc.bufferLength));
                                rowVal[8] = typeDesc.decimalDigits == null ? null : s2b(typeDesc.decimalDigits.toString());
                                rowVal[9] = s2b(Integer.toString(typeDesc.numPrecRadix));
                                rowVal[10] = s2b(Integer.toString(typeDesc.nullability));

                                //
                                // Doesn't always have this field, depending on version
                                //
                                try {
                                    rowVal[11] = results.getBytes("Comment");   // REMARK column
                                } catch (Exception E) {
                                    rowVal[11] = new byte[0];                   // REMARK column
                                }

                                rowVal[12] = results.getBytes("Default");       // COLUMN_DEF
                                rowVal[13] = new byte[] { (byte) '0' };         // SQL_DATA_TYPE
                                rowVal[14] = new byte[] { (byte) '0' };         // SQL_DATE_TIME_SUB

                                if (StringUtils.indexOfIgnoreCase(typeDesc.mysqlType.getName(), "CHAR") != -1
                                        || StringUtils.indexOfIgnoreCase(typeDesc.mysqlType.getName(), "BLOB") != -1
                                        || StringUtils.indexOfIgnoreCase(typeDesc.mysqlType.getName(), "TEXT") != -1
                                        || StringUtils.indexOfIgnoreCase(typeDesc.mysqlType.getName(), "ENUM") != -1
                                        || StringUtils.indexOfIgnoreCase(typeDesc.mysqlType.getName(), "SET") != -1
                                        || StringUtils.indexOfIgnoreCase(typeDesc.mysqlType.getName(), "BINARY") != -1) {
                                    rowVal[15] = rowVal[6];                     // CHAR_OCTET_LENGTH
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
                                        throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.10"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                                                getExceptionInterceptor());
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

        java.sql.ResultSet results = this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(rows, new DefaultColumnDefinition(fields)));

        return results;
    }

    protected Field[] createColumnsFields() {
        Field[] fields = new Field[24];
        fields[0] = new Field("", "TABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[1] = new Field("", "TABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[2] = new Field("", "TABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[3] = new Field("", "COLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[4] = new Field("", "DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 5);
        fields[5] = new Field("", "TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 16); // TODO why is it 16 bytes long? we have longer types specifications 
        fields[6] = new Field("", "COLUMN_SIZE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT,
                Integer.toString(Integer.MAX_VALUE).length());
        fields[7] = new Field("", "BUFFER_LENGTH", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[8] = new Field("", "DECIMAL_DIGITS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[9] = new Field("", "NUM_PREC_RADIX", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[10] = new Field("", "NULLABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[11] = new Field("", "REMARKS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[12] = new Field("", "COLUMN_DEF", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[13] = new Field("", "SQL_DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[14] = new Field("", "SQL_DATETIME_SUB", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[15] = new Field("", "CHAR_OCTET_LENGTH", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT,
                Integer.toString(Integer.MAX_VALUE).length());
        fields[16] = new Field("", "ORDINAL_POSITION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[17] = new Field("", "IS_NULLABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 3);
        fields[18] = new Field("", "SCOPE_CATALOG", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[19] = new Field("", "SCOPE_SCHEMA", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[20] = new Field("", "SCOPE_TABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[21] = new Field("", "SOURCE_DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 10);
        fields[22] = new Field("", "IS_AUTOINCREMENT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 3);
        fields[23] = new Field("", "IS_GENERATEDCOLUMN", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 3);
        return fields;
    }

    @Override
    public java.sql.Connection getConnection() throws SQLException {
        return this.conn;
    }

    @Override
    public java.sql.ResultSet getCrossReference(final String primaryCatalog, final String primarySchema, final String primaryTable, final String foreignCatalog,
            final String foreignSchema, final String foreignTable) throws SQLException {
        if (primaryTable == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        Field[] fields = createFkMetadataFields();

        final ArrayList<Row> tuples = new ArrayList<>();

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
                        fkresults = extractForeignKeyFromCreateTable(catalogStr, null);

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
                                            String referencingColumn = StringUtils.unQuoteIdentifier(referencingColumns.next(), DatabaseMetaData.this.quotedId);

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

        java.sql.ResultSet results = this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(tuples, new DefaultColumnDefinition(fields)));

        return results;
    }

    protected Field[] createFkMetadataFields() {
        Field[] fields = new Field[14];
        fields[0] = new Field("", "PKTABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[1] = new Field("", "PKTABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[2] = new Field("", "PKTABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[3] = new Field("", "PKCOLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[4] = new Field("", "FKTABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[5] = new Field("", "FKTABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[6] = new Field("", "FKTABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[7] = new Field("", "FKCOLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[8] = new Field("", "KEY_SEQ", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 2);
        fields[9] = new Field("", "UPDATE_RULE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 2);
        fields[10] = new Field("", "DELETE_RULE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 2);
        fields[11] = new Field("", "FK_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[12] = new Field("", "PK_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[13] = new Field("", "DEFERRABILITY", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 2);
        return fields;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return this.conn.getServerVersion().getMajor();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return this.conn.getServerVersion().getMinor();
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "MySQL";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return this.conn.getServerVersion().toString();
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return java.sql.Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public int getDriverMajorVersion() {
        return NonRegisteringDriver.getMajorVersionInternal();
    }

    @Override
    public int getDriverMinorVersion() {
        return NonRegisteringDriver.getMinorVersionInternal();
    }

    @Override
    public String getDriverName() throws SQLException {
        return Constants.CJ_NAME;
    }

    @Override
    public String getDriverVersion() throws java.sql.SQLException {
        return Constants.CJ_FULL_NAME + " (Revision: " + Constants.CJ_REVISION + ")";
    }

    @Override
    public java.sql.ResultSet getExportedKeys(String catalog, String schema, final String table) throws SQLException {
        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        Field[] fields = createFkMetadataFields();

        final ArrayList<Row> rows = new ArrayList<>();

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
                        // we can use 'SHOW CREATE TABLE'
                        fkresults = extractForeignKeyFromCreateTable(catalogStr, null);

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

        java.sql.ResultSet results = this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(rows, new DefaultColumnDefinition(fields)));

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
    protected void getExportKeyResults(String catalog, String exportingTable, String keysComment, List<Row> tuples, String fkTableName) throws SQLException {
        getResultsImpl(catalog, exportingTable, keysComment, tuples, fkTableName, true);
    }

    @Override
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

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        // NOTE: A JDBC compliant driver always uses a double quote character.
        return this.session.getIdentifierQuoteString();
    }

    @Override
    public java.sql.ResultSet getImportedKeys(String catalog, String schema, final String table) throws SQLException {
        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        Field[] fields = createFkMetadataFields();

        final ArrayList<Row> rows = new ArrayList<>();

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
                        // we can use 'SHOW CREATE TABLE'
                        fkresults = extractForeignKeyFromCreateTable(catalogStr, table);

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

        java.sql.ResultSet results = this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(rows, new DefaultColumnDefinition(fields)));

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
    protected void getImportKeyResults(String catalog, String importingTable, String keysComment, List<Row> tuples) throws SQLException {
        getResultsImpl(catalog, importingTable, keysComment, tuples, null, false);
    }

    @Override
    public java.sql.ResultSet getIndexInfo(String catalog, String schema, final String table, final boolean unique, boolean approximate) throws SQLException {
        /*
         * MySQL stores index information in the following fields: Table Non_unique Key_name Seq_in_index Column_name Collation Cardinality Sub_part
         */

        Field[] fields = createIndexInfoFields();

        final SortedMap<IndexMetaDataKey, Row> sortedRows = new TreeMap<>();
        final ArrayList<Row> rows = new ArrayList<>();
        final Statement stmt = this.conn.getMetadataSafeStatement();

        try {

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {

                    ResultSet results = null;

                    try {
                        StringBuilder queryBuf = new StringBuilder("SHOW INDEX FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(table, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));
                        queryBuf.append(" FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));

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

            Iterator<Row> sortedRowsIterator = sortedRows.values().iterator();
            while (sortedRowsIterator.hasNext()) {
                rows.add(sortedRowsIterator.next());
            }

            java.sql.ResultSet indexInfo = this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                    new ResultsetRowsStatic(rows, new DefaultColumnDefinition(fields)));

            return indexInfo;
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    protected Field[] createIndexInfoFields() {
        Field[] fields = new Field[13];
        fields[0] = new Field("", "TABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[1] = new Field("", "TABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[2] = new Field("", "TABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[3] = new Field("", "NON_UNIQUE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.BOOLEAN, 4);
        fields[4] = new Field("", "INDEX_QUALIFIER", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 1);
        fields[5] = new Field("", "INDEX_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[6] = new Field("", "TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 32);
        fields[7] = new Field("", "ORDINAL_POSITION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 5);
        fields[8] = new Field("", "COLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[9] = new Field("", "ASC_OR_DESC", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 1);
        fields[10] = new Field("", "CARDINALITY", this.metadataCollationIndex, this.metadataEncoding, MysqlType.BIGINT, 20);
        fields[11] = new Field("", "PAGES", this.metadataCollationIndex, this.metadataEncoding, MysqlType.BIGINT, 20);
        fields[12] = new Field("", "FILTER_CONDITION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        return fields;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 2;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 16777208;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 32;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 16777208;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 16;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 256;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 512;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 256;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return Integer.MAX_VALUE - 8; // Max buffer size - HEADER
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return maxBufferSize - 4; // Max buffer - header
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 256;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 16;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "ABS,ACOS,ASIN,ATAN,ATAN2,BIT_COUNT,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MAX,MIN,MOD,PI,POW,"
                + "POWER,RADIANS,RAND,ROUND,SIN,SQRT,TAN,TRUNCATE";
    }

    @Override
    public java.sql.ResultSet getPrimaryKeys(String catalog, String schema, final String table) throws SQLException {
        Field[] fields = new Field[6];
        fields[0] = new Field("", "TABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[1] = new Field("", "TABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[2] = new Field("", "TABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[3] = new Field("", "COLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[4] = new Field("", "KEY_SEQ", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 5);
        fields[5] = new Field("", "PK_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);

        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        final ArrayList<Row> rows = new ArrayList<>();
        final Statement stmt = this.conn.getMetadataSafeStatement();

        try {

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {
                    ResultSet rs = null;

                    try {

                        StringBuilder queryBuf = new StringBuilder("SHOW KEYS FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(table, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));
                        queryBuf.append(" FROM ");
                        queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));

                        rs = stmt.executeQuery(queryBuf.toString());

                        TreeMap<String, byte[][]> sortMap = new TreeMap<>();

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

        java.sql.ResultSet results = this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(rows, new DefaultColumnDefinition(fields)));

        return results;
    }

    @Override
    public java.sql.ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
            throws SQLException {
        Field[] fields = createProcedureColumnsFields();

        return getProcedureOrFunctionColumns(fields, catalog, schemaPattern, procedureNamePattern, columnNamePattern, true,
                this.conn.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).getValue());
    }

    protected Field[] createProcedureColumnsFields() {
        Field[] fields = new Field[20];
        fields[0] = new Field("", "PROCEDURE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 512);
        fields[1] = new Field("", "PROCEDURE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 512);
        fields[2] = new Field("", "PROCEDURE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 512);
        fields[3] = new Field("", "COLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 512);
        fields[4] = new Field("", "COLUMN_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 64);
        fields[5] = new Field("", "DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 6);
        fields[6] = new Field("", "TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 64);
        fields[7] = new Field("", "PRECISION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12);
        fields[8] = new Field("", "LENGTH", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12);
        fields[9] = new Field("", "SCALE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 12);
        fields[10] = new Field("", "RADIX", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 6);
        fields[11] = new Field("", "NULLABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 6);
        fields[12] = new Field("", "REMARKS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 512);
        fields[13] = new Field("", "COLUMN_DEF", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 512);
        fields[14] = new Field("", "SQL_DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12);
        fields[15] = new Field("", "SQL_DATETIME_SUB", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12);
        fields[16] = new Field("", "CHAR_OCTET_LENGTH", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12);
        fields[17] = new Field("", "ORDINAL_POSITION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12);
        fields[18] = new Field("", "IS_NULLABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 512);
        fields[19] = new Field("", "SPECIFIC_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 512);
        return fields;
    }

    protected java.sql.ResultSet getProcedureOrFunctionColumns(Field[] fields, String catalog, String schemaPattern, String procedureOrFunctionNamePattern,
            String columnNamePattern, boolean returnProcedures, boolean returnFunctions) throws SQLException {

        List<ComparableWrapper<String, ProcedureType>> procsOrFuncsToExtractList = new ArrayList<>();
        //Main container to be passed to getProceduresAndOrFunctions
        ResultSet procsAndOrFuncsRs = null;

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
                        this.session.getServerSession().isNoBackslashEscapesSet());

                //There *should* be 2 rows, if any.
                if (parseList.size() == 2) {
                    tmpCatalog = parseList.get(0);
                    tmpProcedureOrFunctionNamePattern = parseList.get(1);
                } else {
                    //keep values as they are
                }
            }

            procsAndOrFuncsRs = getProceduresAndOrFunctions(createFieldMetadataForGetProcedures(), catalog, schemaPattern, tmpProcedureOrFunctionNamePattern,
                    returnProcedures, returnFunctions);

            boolean hasResults = false;
            while (procsAndOrFuncsRs.next()) {
                procsOrFuncsToExtractList.add(new ComparableWrapper<>(getFullyQualifiedName(procsAndOrFuncsRs.getString(1), procsAndOrFuncsRs.getString(3)),
                        procsAndOrFuncsRs.getShort(8) == procedureNoResult ? PROCEDURE : FUNCTION));
                hasResults = true;
            }

            // FIX for Bug#56305, allowing the code to proceed with empty fields causing NPE later
            if (!hasResults) {
                // throw SQLError.createSQLException(
                // "User does not have access to metadata required to determine " +
                // "stored procedure parameter types. If rights can not be granted, configure connection with \"noAccessToProcedureBodies=true\" " +
                // "to have driver generate parameters that represent INOUT strings irregardless of actual parameter types.",
                // MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
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

        ArrayList<Row> resultRows = new ArrayList<>();
        int idx = 0;
        String procNameToCall = "";

        for (ComparableWrapper<String, ProcedureType> procOrFunc : procsOrFuncsToExtractList) {
            String procName = procOrFunc.getKey();
            ProcedureType procType = procOrFunc.getValue();

            //Continuing from above (database_name.sp_name)
            if (!" ".equals(this.quotedId)) {
                idx = StringUtils.indexOfIgnoreCase(0, procName, ".", this.quotedId, this.quotedId,
                        this.session.getServerSession().isNoBackslashEscapesSet() ? StringUtils.SEARCH_MODE__MRK_COM_WS : StringUtils.SEARCH_MODE__ALL);
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

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(resultRows, new DefaultColumnDefinition(fields)));
    }

    @Override
    public java.sql.ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        Field[] fields = createFieldMetadataForGetProcedures();

        return getProceduresAndOrFunctions(fields, catalog, schemaPattern, procedureNamePattern, true,
                this.conn.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions).getValue());
    }

    protected Field[] createFieldMetadataForGetProcedures() {
        Field[] fields = new Field[9];
        fields[0] = new Field("", "PROCEDURE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[1] = new Field("", "PROCEDURE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[2] = new Field("", "PROCEDURE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[3] = new Field("", "reserved1", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[4] = new Field("", "reserved2", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[5] = new Field("", "reserved3", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[6] = new Field("", "REMARKS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[7] = new Field("", "PROCEDURE_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 6);
        fields[8] = new Field("", "SPECIFIC_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);

        return fields;
    }

    /**
     * @param fields
     *            fields
     * @param catalog
     *            catalog
     * @param schemaPattern
     *            schema pattern
     * @param procedureNamePattern
     *            procedure name pattern
     * @param returnProcedures
     *            true if procedures should be included into result
     * @param returnFunctions
     *            true if functions should be included into result
     * @return result set
     * @throws SQLException
     *             if a database access error occurs
     */
    protected java.sql.ResultSet getProceduresAndOrFunctions(final Field[] fields, String catalog, String schemaPattern, String procedureNamePattern,
            final boolean returnProcedures, final boolean returnFunctions) throws SQLException {
        final ArrayList<Row> procedureRows = new ArrayList<>();

        final String procNamePattern = procedureNamePattern;

        final List<ComparableWrapper<String, Row>> procedureRowsToSort = new ArrayList<>();

        new IterateBlock<String>(getCatalogIterator(catalog)) {
            @Override
            void forEach(String catalogStr) throws SQLException {
                String db = catalogStr;

                ResultSet proceduresRs = null;
                boolean needsClientFiltering = true;

                StringBuilder selectFromMySQLProcSQL = new StringBuilder();

                selectFromMySQLProcSQL.append("SELECT name, type, comment FROM mysql.proc WHERE");
                if (returnProcedures && !returnFunctions) {
                    selectFromMySQLProcSQL.append(" type = 'PROCEDURE' AND ");
                } else if (!returnProcedures && returnFunctions) {
                    selectFromMySQLProcSQL.append(" type = 'FUNCTION' AND ");
                }

                selectFromMySQLProcSQL.append(" db <=> ?");

                if (procNamePattern != null && procNamePattern.length() > 0) {
                    selectFromMySQLProcSQL.append(" AND name LIKE ?");
                }

                selectFromMySQLProcSQL.append(" ORDER BY name, type");

                java.sql.PreparedStatement proceduresStmt = prepareMetaDataSafeStatement(selectFromMySQLProcSQL.toString());

                try {
                    //
                    // Try using system tables first, as this is a little bit more efficient....
                    //
                    if (db != null) {
                        if (DatabaseMetaData.this.conn.lowerCaseTableNames()) {
                            db = db.toLowerCase();
                        }
                        proceduresStmt.setString(1, db);
                    } else {
                        proceduresStmt.setNull(1, MysqlType.VARCHAR.getJdbcType());
                    }

                    int nameIndex = 1;

                    if (procNamePattern != null && procNamePattern.length() > 0) {
                        proceduresStmt.setString(2, procNamePattern);
                    }

                    try {
                        proceduresRs = proceduresStmt.executeQuery();
                        needsClientFiltering = false;

                        if (returnProcedures) {
                            convertToJdbcProcedureList(true, db, proceduresRs, needsClientFiltering, db, procedureRowsToSort, nameIndex);
                        }

                        if (returnFunctions) {
                            convertToJdbcFunctionList(db, proceduresRs, needsClientFiltering, db, procedureRowsToSort, nameIndex, fields);
                        }

                    } catch (SQLException sqlEx) {
                        nameIndex = 2;

                        // System tables aren't accessible, so use 'SHOW [FUNCTION|PROCEDURE] STATUS instead.
                        // Functions first:
                        if (returnFunctions) {
                            proceduresStmt.close();

                            String sql = procNamePattern != null && procNamePattern.length() > 0 ? "SHOW FUNCTION STATUS LIKE ?" : "SHOW FUNCTION STATUS";
                            proceduresStmt = prepareMetaDataSafeStatement(sql);
                            if (procNamePattern != null && procNamePattern.length() > 0) {
                                proceduresStmt.setString(1, procNamePattern);
                            }
                            proceduresRs = proceduresStmt.executeQuery();

                            convertToJdbcFunctionList(db, proceduresRs, needsClientFiltering, db, procedureRowsToSort, nameIndex, fields);
                        }

                        // Procedures next:
                        if (returnProcedures) {
                            proceduresStmt.close();

                            String sql = procNamePattern != null && procNamePattern.length() > 0 ? "SHOW PROCEDURE STATUS LIKE ?" : "SHOW PROCEDURE STATUS";
                            proceduresStmt = prepareMetaDataSafeStatement(sql);
                            if (procNamePattern != null && procNamePattern.length() > 0) {
                                proceduresStmt.setString(1, procNamePattern);
                            }
                            proceduresRs = proceduresStmt.executeQuery();

                            convertToJdbcProcedureList(false, db, proceduresRs, needsClientFiltering, db, procedureRowsToSort, nameIndex);
                        }
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
        for (ComparableWrapper<String, Row> procRow : procedureRowsToSort) {
            procedureRows.add(procRow.getValue());
        }

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(procedureRows, new DefaultColumnDefinition(fields)));
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "PROCEDURE";
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    private void getResultsImpl(String catalog, String table, String keysComment, List<Row> tuples, String fkTableName, boolean isExport) throws SQLException {

        LocalAndReferencedColumns parsedInfo = parseTableStatusIntoLocalAndReferencedColumns(keysComment);

        if (isExport && !parsedInfo.referencedTable.equals(table)) {
            return;
        }

        if (parsedInfo.localColumnsList.size() != parsedInfo.referencedColumnsList.size()) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.12"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
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

    @Override
    public java.sql.ResultSet getSchemas() throws SQLException {
        Field[] fields = new Field[2];
        fields[0] = new Field("", "TABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);
        fields[1] = new Field("", "TABLE_CATALOG", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 0);

        ArrayList<Row> tuples = new ArrayList<>();
        java.sql.ResultSet results = this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(tuples, new DefaultColumnDefinition(fields)));

        return results;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    /**
     * Get a comma separated list of all a database's SQL keywords that are NOT also SQL92/SQL2003 keywords.
     * 
     * @return the list
     * @throws SQLException
     *             if a database access error occurs
     */
    @Override
    public String getSQLKeywords() throws SQLException {
        if (mysqlKeywords != null) {
            return mysqlKeywords;
        }

        synchronized (DatabaseMetaData.class) {
            // double check, maybe it's already set
            if (mysqlKeywords != null) {
                return mysqlKeywords;
            }

            Set<String> mysqlKeywordSet = new TreeSet<>();
            StringBuilder mysqlKeywordsBuffer = new StringBuilder();

            Collections.addAll(mysqlKeywordSet, MYSQL_KEYWORDS);
            mysqlKeywordSet.removeAll(SQL2003_KEYWORDS);

            for (String keyword : mysqlKeywordSet) {
                mysqlKeywordsBuffer.append(",").append(keyword);
            }

            mysqlKeywords = mysqlKeywordsBuffer.substring(1);
            return mysqlKeywords;
        }
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return java.sql.DatabaseMetaData.sqlStateSQL99;
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "ASCII,BIN,BIT_LENGTH,CHAR,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,CONCAT_WS,CONV,ELT,EXPORT_SET,FIELD,FIND_IN_SET,HEX,INSERT,"
                + "INSTR,LCASE,LEFT,LENGTH,LOAD_FILE,LOCATE,LOCATE,LOWER,LPAD,LTRIM,MAKE_SET,MATCH,MID,OCT,OCTET_LENGTH,ORD,POSITION,"
                + "QUOTE,REPEAT,REPLACE,REVERSE,RIGHT,RPAD,RTRIM,SOUNDEX,SPACE,STRCMP,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING,"
                + "SUBSTRING_INDEX,TRIM,UCASE,UPPER";
    }

    @Override
    public java.sql.ResultSet getSuperTables(String arg0, String arg1, String arg2) throws SQLException {
        Field[] fields = new Field[4];
        fields[0] = new Field("", "TABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[1] = new Field("", "TABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[2] = new Field("", "TABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[3] = new Field("", "SUPERTABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(new ArrayList<Row>(), new DefaultColumnDefinition(fields)));
    }

    @Override
    public java.sql.ResultSet getSuperTypes(String arg0, String arg1, String arg2) throws SQLException {
        Field[] fields = new Field[6];
        fields[0] = new Field("", "TYPE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[1] = new Field("", "TYPE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[2] = new Field("", "TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[3] = new Field("", "SUPERTYPE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[4] = new Field("", "SUPERTYPE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[5] = new Field("", "SUPERTYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(new ArrayList<Row>(), new DefaultColumnDefinition(fields)));
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "DATABASE,USER,SYSTEM_USER,SESSION_USER,PASSWORD,ENCRYPT,LAST_INSERT_ID,VERSION";
    }

    protected String getTableNameWithCase(String table) {
        String tableNameWithCase = (this.conn.lowerCaseTableNames() ? table.toLowerCase() : table);

        return tableNameWithCase;
    }

    @Override
    public java.sql.ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {

        Field[] fields = new Field[7];
        fields[0] = new Field("", "TABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 64);
        fields[1] = new Field("", "TABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 1);
        fields[2] = new Field("", "TABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 64);
        fields[3] = new Field("", "GRANTOR", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 77);
        fields[4] = new Field("", "GRANTEE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 77);
        fields[5] = new Field("", "PRIVILEGE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 64);
        fields[6] = new Field("", "IS_GRANTABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 3);

        StringBuilder grantQueryBuf = new StringBuilder("SELECT host,db,table_name,grantor,user,table_priv FROM mysql.tables_priv");

        StringBuilder conditionBuf = new StringBuilder();
        if (catalog != null) {
            conditionBuf.append(" db LIKE ?");
        }
        if (tableNamePattern != null) {
            if (conditionBuf.length() > 0) {
                conditionBuf.append(" AND");
            }
            conditionBuf.append(" table_name LIKE ?");
        }
        if (conditionBuf.length() > 0) {
            grantQueryBuf.append(" WHERE");
            grantQueryBuf.append(conditionBuf);
        }

        ResultSet results = null;
        ArrayList<Row> grantRows = new ArrayList<>();
        PreparedStatement pStmt = null;

        try {
            pStmt = prepareMetaDataSafeStatement(grantQueryBuf.toString());
            int nextId = 1;
            if (catalog != null) {
                pStmt.setString(nextId++, catalog);
            }
            if (tableNamePattern != null) {
                pStmt.setString(nextId, tableNamePattern);
            }

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

                if ((host != null) && this.useHostsInPrivileges) {
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
                            columnResults = getColumns(catalog, schemaPattern, table, null);

                            while (columnResults.next()) {
                                byte[][] tuple = new byte[8][];
                                tuple[0] = s2b(db);
                                tuple[1] = null;
                                tuple[2] = s2b(table);
                                tuple[3] = grantor != null ? s2b(grantor) : null;
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

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(grantRows, new DefaultColumnDefinition(fields)));
    }

    @Override
    public java.sql.ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, final String[] types) throws SQLException {

        final SortedMap<TableMetaDataKey, Row> sortedRows = new TreeMap<>();
        final ArrayList<Row> tuples = new ArrayList<>();

        final Statement stmt = this.conn.getMetadataSafeStatement();

        if (catalog == null && this.nullCatalogMeansCurrent) {
            catalog = this.database;
        }

        if (tableNamePattern != null) {
            List<String> parseList = StringUtils.splitDBdotName(tableNamePattern, catalog, this.quotedId,
                    this.session.getServerSession().isNoBackslashEscapesSet());
            //There *should* be 2 rows, if any.
            if (parseList.size() == 2) {
                tableNamePattern = parseList.get(1);
            }
        }

        final String tableNamePat = tableNamePattern;

        try {
            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {
                    boolean operatingOnSystemDB = "information_schema".equalsIgnoreCase(catalogStr) || "mysql".equalsIgnoreCase(catalogStr)
                            || "performance_schema".equalsIgnoreCase(catalogStr);

                    ResultSet results = null;

                    try {

                        try {
                            StringBuilder sqlBuf = new StringBuilder("SHOW FULL TABLES FROM ");
                            sqlBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));
                            if (tableNamePat != null) {
                                sqlBuf.append(" LIKE ");
                                sqlBuf.append(StringUtils.quoteIdentifier(tableNamePat, "'", true));
                            }

                            results = stmt.executeQuery(sqlBuf.toString());
                        } catch (SQLException sqlEx) {
                            if (MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlEx.getSQLState())) {
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
                                // TODO: Check if this branch is needed for 5.7 server (maybe refactor hasTableTypes)
                                if (shouldReportTables) {
                                    // Pre-MySQL-5.0.1, tables only
                                    row[3] = TableType.TABLE.asBytes();
                                    sortedRows.put(new TableMetaDataKey(TableType.TABLE.getName(), catalogStr, null, results.getString(1)),
                                            new ByteArrayRow(row, getExceptionInterceptor()));
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
        java.sql.ResultSet tables = this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(tuples, createTablesFields()));

        return tables;
    }

    protected ColumnDefinition createTablesFields() {
        Field[] fields = new Field[10];
        fields[0] = new Field("", "TABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 255);
        fields[1] = new Field("", "TABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 0);
        fields[2] = new Field("", "TABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 255);
        fields[3] = new Field("", "TABLE_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 5);
        fields[4] = new Field("", "REMARKS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 0);
        fields[5] = new Field("", "TYPE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 0);
        fields[6] = new Field("", "TYPE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 0);
        fields[7] = new Field("", "TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 0);
        fields[8] = new Field("", "SELF_REFERENCING_COL_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 0);
        fields[9] = new Field("", "REF_GENERATION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 0);
        return new DefaultColumnDefinition(fields);
    }

    @Override
    public java.sql.ResultSet getTableTypes() throws SQLException {
        ArrayList<Row> tuples = new ArrayList<>();
        Field[] fields = new Field[] { new Field("", "TABLE_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 256) };

        tuples.add(new ByteArrayRow(new byte[][] { TableType.LOCAL_TEMPORARY.asBytes() }, getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(new byte[][] { TableType.SYSTEM_TABLE.asBytes() }, getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(new byte[][] { TableType.SYSTEM_VIEW.asBytes() }, getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(new byte[][] { TableType.TABLE.asBytes() }, getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(new byte[][] { TableType.VIEW.asBytes() }, getExceptionInterceptor()));

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(tuples, new DefaultColumnDefinition(fields)));
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "DAYOFWEEK,WEEKDAY,DAYOFMONTH,DAYOFYEAR,MONTH,DAYNAME,MONTHNAME,QUARTER,WEEK,YEAR,HOUR,MINUTE,SECOND,PERIOD_ADD,"
                + "PERIOD_DIFF,TO_DAYS,FROM_DAYS,DATE_FORMAT,TIME_FORMAT,CURDATE,CURRENT_DATE,CURTIME,CURRENT_TIME,NOW,SYSDATE,"
                + "CURRENT_TIMESTAMP,UNIX_TIMESTAMP,FROM_UNIXTIME,SEC_TO_TIME,TIME_TO_SEC";
    }

    /**
     * 
     * @param mysqlTypeName
     *            we use a string name here to allow aliases for the same MysqlType to be listed too
     * @return bytes
     * @throws SQLException
     *             if a conversion error occurs
     */
    private byte[][] getTypeInfo(String mysqlTypeName) throws SQLException {

        MysqlType mt = MysqlType.getByName(mysqlTypeName);
        byte[][] rowVal = new byte[18][];

        rowVal[0] = s2b(mysqlTypeName);                                                     // Type name
        rowVal[1] = Integer.toString(mt.getJdbcType()).getBytes();                          // JDBC Data type
        // JDBC spec reserved only 'int' type for precision, thus we need to cut longer values
        rowVal[2] = Integer.toString(mt.getPrecision() > Integer.MAX_VALUE ? Integer.MAX_VALUE : mt.getPrecision().intValue()).getBytes(); // Precision
        switch (mt) {
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case BINARY:
            case VARBINARY:
            case CHAR:
            case VARCHAR:
            case ENUM:
            case SET:
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case GEOMETRY:
            case UNKNOWN:
                rowVal[3] = s2b("'");                                                       // Literal Prefix
                rowVal[4] = s2b("'");                                                       // Literal Suffix
                break;
            default:
                rowVal[3] = s2b("");                                                        // Literal Prefix
                rowVal[4] = s2b("");                                                        // Literal Suffix
        }
        rowVal[5] = s2b(mt.getCreateParams());                                              // Create Params
        rowVal[6] = Integer.toString(java.sql.DatabaseMetaData.typeNullable).getBytes();    // Nullable
        rowVal[7] = s2b("true");                                                            // Case Sensitive
        rowVal[8] = Integer.toString(java.sql.DatabaseMetaData.typeSearchable).getBytes();  // Searchable
        rowVal[9] = s2b(mt.isAllowed(MysqlType.FIELD_FLAG_UNSIGNED) ? "true" : "false");    // Unsignable
        rowVal[10] = s2b("false");                                                          // Fixed Prec Scale
        rowVal[11] = s2b("false");                                                          // Auto Increment
        rowVal[12] = s2b(mt.getName());                                                     // Locale Type Name
        switch (mt) {
            case DECIMAL: // TODO is it right? DECIMAL isn't a floating-point number...
            case DECIMAL_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                rowVal[13] = s2b("-308");                                                   // Minimum Scale
                rowVal[14] = s2b("308");                                                    // Maximum Scale
                break;
            case FLOAT:
            case FLOAT_UNSIGNED:
                rowVal[13] = s2b("-38");                                                    // Minimum Scale
                rowVal[14] = s2b("38");                                                     // Maximum Scale
                break;
            default:
                rowVal[13] = s2b("0");                                                      // Minimum Scale
                rowVal[14] = s2b("0");                                                      // Maximum Scale
        }

        rowVal[15] = s2b("0");                                                              // SQL Data Type (not used)
        rowVal[16] = s2b("0");                                                              // SQL DATETIME SUB (not used)
        rowVal[17] = s2b("10");                                                             // NUM_PREC_RADIX (2 or 10)

        return rowVal;
    }

    @Override
    public java.sql.ResultSet getTypeInfo() throws SQLException {
        Field[] fields = new Field[18];
        fields[0] = new Field("", "TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[1] = new Field("", "DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 5);
        fields[2] = new Field("", "PRECISION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[3] = new Field("", "LITERAL_PREFIX", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 4);
        fields[4] = new Field("", "LITERAL_SUFFIX", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 4);
        fields[5] = new Field("", "CREATE_PARAMS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[6] = new Field("", "NULLABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 5);
        fields[7] = new Field("", "CASE_SENSITIVE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.BOOLEAN, 3);
        fields[8] = new Field("", "SEARCHABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 3);
        fields[9] = new Field("", "UNSIGNED_ATTRIBUTE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.BOOLEAN, 3);
        fields[10] = new Field("", "FIXED_PREC_SCALE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.BOOLEAN, 3);
        fields[11] = new Field("", "AUTO_INCREMENT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.BOOLEAN, 3);
        fields[12] = new Field("", "LOCAL_TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[13] = new Field("", "MINIMUM_SCALE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 5);
        fields[14] = new Field("", "MAXIMUM_SCALE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 5);
        fields[15] = new Field("", "SQL_DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[16] = new Field("", "SQL_DATETIME_SUB", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[17] = new Field("", "NUM_PREC_RADIX", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);

        ArrayList<Row> tuples = new ArrayList<>();

        /*
         * The following are ordered by java.sql.Types, and then by how closely the MySQL type matches the JDBC Type (per spec)
         */
        tuples.add(new ByteArrayRow(getTypeInfo("BIT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("BOOL"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("TINYINT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("TINYINT UNSIGNED"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("BIGINT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("BIGINT UNSIGNED"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("LONG VARBINARY"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("MEDIUMBLOB"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("LONGBLOB"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("BLOB"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("VARBINARY"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("TINYBLOB"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("BINARY"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("LONG VARCHAR"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("MEDIUMTEXT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("LONGTEXT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("TEXT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("CHAR"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("ENUM"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("SET"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("DECIMAL"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("NUMERIC"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("INTEGER"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("INTEGER UNSIGNED"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("INT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("INT UNSIGNED"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("MEDIUMINT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("MEDIUMINT UNSIGNED"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("SMALLINT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("SMALLINT UNSIGNED"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("FLOAT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("DOUBLE"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("DOUBLE PRECISION"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("REAL"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("VARCHAR"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("TINYTEXT"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("DATE"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("YEAR"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("TIME"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("DATETIME"), getExceptionInterceptor()));
        tuples.add(new ByteArrayRow(getTypeInfo("TIMESTAMP"), getExceptionInterceptor()));

        // TODO add missed types (aliases)

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(tuples, new DefaultColumnDefinition(fields)));
    }

    @Override
    public java.sql.ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        Field[] fields = new Field[7];
        fields[0] = new Field("", "TYPE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 32);
        fields[1] = new Field("", "TYPE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 32);
        fields[2] = new Field("", "TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 32);
        fields[3] = new Field("", "CLASS_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 32);
        fields[4] = new Field("", "DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[5] = new Field("", "REMARKS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 32);
        fields[6] = new Field("", "BASE_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 10);

        ArrayList<Row> tuples = new ArrayList<>();

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(tuples, new DefaultColumnDefinition(fields)));
    }

    @Override
    public String getURL() throws SQLException {
        return this.conn.getURL();
    }

    @Override
    public String getUserName() throws SQLException {
        if (this.useHostsInPrivileges) {
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

    @Override
    public java.sql.ResultSet getVersionColumns(String catalog, String schema, final String table) throws SQLException {

        if (table == null) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.2"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }

        Field[] fields = new Field[8];
        fields[0] = new Field("", "SCOPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 5);
        fields[1] = new Field("", "COLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 32);
        fields[2] = new Field("", "DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 5);
        fields[3] = new Field("", "TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 16);
        fields[4] = new Field("", "COLUMN_SIZE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 16);
        fields[5] = new Field("", "BUFFER_LENGTH", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 16);
        fields[6] = new Field("", "DECIMAL_DIGITS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 16);
        fields[7] = new Field("", "PSEUDO_COLUMN", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 5);

        final ArrayList<Row> rows = new ArrayList<>();

        final Statement stmt = this.conn.getMetadataSafeStatement();

        try {

            new IterateBlock<String>(getCatalogIterator(catalog)) {
                @Override
                void forEach(String catalogStr) throws SQLException {

                    ResultSet results = null;

                    try {
                        StringBuilder whereBuf = new StringBuilder(" Extra LIKE '%on update CURRENT_TIMESTAMP%'");
                        List<String> rsFields = new ArrayList<>();

                        if (whereBuf.length() > 0 || rsFields.size() > 0) {
                            StringBuilder queryBuf = new StringBuilder("SHOW COLUMNS FROM ");
                            queryBuf.append(StringUtils.quoteIdentifier(table, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));
                            queryBuf.append(" FROM ");
                            queryBuf.append(StringUtils.quoteIdentifier(catalogStr, DatabaseMetaData.this.quotedId, DatabaseMetaData.this.pedantic));
                            queryBuf.append(" WHERE");
                            queryBuf.append(whereBuf.toString());

                            results = stmt.executeQuery(queryBuf.toString());

                            while (results.next()) {
                                TypeDescriptor typeDesc = new TypeDescriptor(results.getString("Type"), results.getString("Null"));
                                byte[][] rowVal = new byte[8][];
                                rowVal[0] = null;                                                                           // SCOPE is not used
                                rowVal[1] = results.getBytes("Field");                                                      // COLUMN_NAME
                                rowVal[2] = Short.toString((short) typeDesc.mysqlType.getJdbcType()).getBytes();                                   // DATA_TYPE
                                rowVal[3] = s2b(typeDesc.mysqlType.getName());                                                         // TYPE_NAME
                                rowVal[4] = typeDesc.columnSize == null ? null : s2b(typeDesc.columnSize.toString());       // COLUMN_SIZE
                                rowVal[5] = s2b(Integer.toString(typeDesc.bufferLength));                                   // BUFFER_LENGTH
                                rowVal[6] = typeDesc.decimalDigits == null ? null : s2b(typeDesc.decimalDigits.toString()); // DECIMAL_DIGITS
                                rowVal[7] = Integer.toString(java.sql.DatabaseMetaData.versionColumnNotPseudo).getBytes();  // PSEUDO_COLUMN
                                rows.add(new ByteArrayRow(rowVal, getExceptionInterceptor()));
                            }
                        }
                    } catch (SQLException sqlEx) {
                        if (!MysqlErrorNumbers.SQL_STATE_BASE_TABLE_OR_VIEW_NOT_FOUND.equals(sqlEx.getSQLState())) {
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

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(rows, new DefaultColumnDefinition(fields)));
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return !this.conn.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_emulateLocators).getValue();
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return !nullsAreSortedHigh();
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
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
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
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.14"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        String constraintName = StringUtils.unQuoteIdentifier(keysComment.substring(0, indexOfOpenParenLocalColumns).trim(), this.quotedId);
        keysComment = keysComment.substring(indexOfOpenParenLocalColumns, keysComment.length());

        String keysCommentTrimmed = keysComment.trim();

        int indexOfCloseParenLocalColumns = StringUtils.indexOfIgnoreCase(0, keysCommentTrimmed, ")", this.quotedId, this.quotedId,
                StringUtils.SEARCH_MODE__ALL);

        if (indexOfCloseParenLocalColumns == -1) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.15"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        String localColumnNamesString = keysCommentTrimmed.substring(1, indexOfCloseParenLocalColumns);

        int indexOfRefer = StringUtils.indexOfIgnoreCase(0, keysCommentTrimmed, "REFER ", this.quotedId, this.quotedId, StringUtils.SEARCH_MODE__ALL);

        if (indexOfRefer == -1) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.16"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        int indexOfOpenParenReferCol = StringUtils.indexOfIgnoreCase(indexOfRefer, keysCommentTrimmed, "(", this.quotedId, this.quotedId,
                StringUtils.SEARCH_MODE__MRK_COM_WS);

        if (indexOfOpenParenReferCol == -1) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.17"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        String referCatalogTableString = keysCommentTrimmed.substring(indexOfRefer + "REFER ".length(), indexOfOpenParenReferCol);

        int indexOfSlash = StringUtils.indexOfIgnoreCase(0, referCatalogTableString, "/", this.quotedId, this.quotedId, StringUtils.SEARCH_MODE__MRK_COM_WS);

        if (indexOfSlash == -1) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.18"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
        }

        String referCatalog = StringUtils.unQuoteIdentifier(referCatalogTableString.substring(0, indexOfSlash), this.quotedId);
        String referTable = StringUtils.unQuoteIdentifier(referCatalogTableString.substring(indexOfSlash + 1).trim(), this.quotedId);

        int indexOfCloseParenRefer = StringUtils.indexOfIgnoreCase(indexOfOpenParenReferCol, keysCommentTrimmed, ")", this.quotedId, this.quotedId,
                StringUtils.SEARCH_MODE__ALL);

        if (indexOfCloseParenRefer == -1) {
            throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.19"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
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
     *            string
     * @return bytes
     * @throws SQLException
     *             if a conversion error occurs
     */
    protected byte[] s2b(String s) throws SQLException {
        if (s == null) {
            return null;
        }

        try {
            return StringUtils.getBytes(s, this.conn.getCharacterSetMetadata());
        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e, getExceptionInterceptor());
        }
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return this.conn.storesLowerCaseTableName();
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        // NOTE: A JDBC compliant driver will always return false.
        return this.conn.storesLowerCaseTableName();
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return !this.conn.storesLowerCaseTableName();
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        // NOTE: A JDBC compliant driver will always return false.
        return !this.conn.storesLowerCaseTableName();
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        // NOTE: A JDBC compliant driver will always return true.
        return true; // not actually true, but required by JDBC spec!?
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
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        // NOTE: All JDBC compliant drivers must return true.
        return true;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        // TODO MySQL has a CONVERT() function, is it irrelevant here?
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return MysqlType.supportsConvert(fromType, toType);
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
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
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        if (!this.conn.getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_overrideSupportsIntegrityEnhancementFacility).getValue()) {
            return false;
        }

        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        // NOTE: All JDBC compliant drivers must return true.
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return !this.conn.lowerCaseTableNames();
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return !this.conn.lowerCaseTableNames();
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
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
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
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
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        switch (type) {
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
                if ((concurrency == ResultSet.CONCUR_READ_ONLY) || (concurrency == ResultSet.CONCUR_UPDATABLE)) {
                    return true;
                }
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.20"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());

            case ResultSet.TYPE_FORWARD_ONLY:
                if ((concurrency == ResultSet.CONCUR_READ_ONLY) || (concurrency == ResultSet.CONCUR_UPDATABLE)) {
                    return true;
                }
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.20"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());

            case ResultSet.TYPE_SCROLL_SENSITIVE:
                return false;
            default:
                throw SQLError.createSQLException(Messages.getString("DatabaseMetaData.20"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
        }

    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return (type == ResultSet.TYPE_SCROLL_INSENSITIVE);
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
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
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
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

    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        // NOTE: A JDBC compliant driver always returns true.
        return true;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        // We don't have any built-ins, we actually support whatever the client wants to provide, however we don't have a way to express this with the interface
        // given
        Field[] fields = new Field[4];
        fields[0] = new Field("", "NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 255);
        fields[1] = new Field("", "MAX_LEN", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 10);
        fields[2] = new Field("", "DEFAULT_VALUE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 255);
        fields[3] = new Field("", "DESCRIPTION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 255);

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(new ArrayList<Row>(), new DefaultColumnDefinition(fields)));
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        Field[] fields = createFunctionColumnsFields();

        return getProcedureOrFunctionColumns(fields, catalog, schemaPattern, functionNamePattern, columnNamePattern, false, true);
    }

    protected Field[] createFunctionColumnsFields() {
        Field[] fields = { new Field("", "FUNCTION_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "FUNCTION_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "FUNCTION_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "COLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "COLUMN_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 64),
                new Field("", "DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 6),
                new Field("", "TYPE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 64),
                new Field("", "PRECISION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12),
                new Field("", "LENGTH", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12),
                new Field("", "SCALE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 12),
                new Field("", "RADIX", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 6),
                new Field("", "NULLABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 6),
                new Field("", "REMARKS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "CHAR_OCTET_LENGTH", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32),
                new Field("", "ORDINAL_POSITION", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 32),
                new Field("", "IS_NULLABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 12),
                new Field("", "SPECIFIC_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 64) };
        return fields;
    }

    @Override
    public java.sql.ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        Field[] fields = new Field[6];
        fields[0] = new Field("", "FUNCTION_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[1] = new Field("", "FUNCTION_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[2] = new Field("", "FUNCTION_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[3] = new Field("", "REMARKS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);
        fields[4] = new Field("", "FUNCTION_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.SMALLINT, 6);
        fields[5] = new Field("", "SPECIFIC_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.CHAR, 255);

        return getProceduresAndOrFunctions(fields, catalog, schemaPattern, functionNamePattern, false, true);
    }

    public boolean providesQueryObjectGenerator() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        Field[] fields = { new Field("", "TABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 255),
                new Field("", "TABLE_CATALOG", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 255) };

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(new ArrayList<Row>(), new DefaultColumnDefinition(fields)));
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }

    /**
     * Get a prepared statement to query information_schema tables.
     * 
     * @param sql
     *            query
     * @return PreparedStatement
     * @throws SQLException
     *             if a database access error occurs
     */
    protected java.sql.PreparedStatement prepareMetaDataSafeStatement(String sql) throws SQLException {
        // Can't use server-side here as we coerce a lot of types to match the spec.
        java.sql.PreparedStatement pStmt = this.conn.clientPrepareStatement(sql);

        if (pStmt.getMaxRows() != 0) {
            pStmt.setMaxRows(0);
        }

        ((com.mysql.cj.jdbc.JdbcStatement) pStmt).setHoldResultsOpenOverClose(true);

        return pStmt;
    }

    @Override
    public java.sql.ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        Field[] fields = { new Field("", "TABLE_CAT", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "TABLE_SCHEM", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "TABLE_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "COLUMN_NAME", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "DATA_TYPE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12),
                new Field("", "COLUMN_SIZE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12),
                new Field("", "DECIMAL_DIGITS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12),
                new Field("", "NUM_PREC_RADIX", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12),
                new Field("", "COLUMN_USAGE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "REMARKS", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512),
                new Field("", "CHAR_OCTET_LENGTH", this.metadataCollationIndex, this.metadataEncoding, MysqlType.INT, 12),
                new Field("", "IS_NULLABLE", this.metadataCollationIndex, this.metadataEncoding, MysqlType.VARCHAR, 512) };

        return this.resultSetFactory.createFromResultsetRows(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                new ResultsetRowsStatic(new ArrayList<Row>(), new DefaultColumnDefinition(fields)));
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping
            // anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.conn.getExceptionInterceptor());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    public String getMetadataEncoding() {
        return this.metadataEncoding;
    }

    public void setMetadataEncoding(String metadataEncoding) {
        this.metadataEncoding = metadataEncoding;
    }

    public int getMetadataCollationIndex() {
        return this.metadataCollationIndex;
    }

    public void setMetadataCollationIndex(int metadataCollationIndex) {
        this.metadataCollationIndex = metadataCollationIndex;
    }
}
