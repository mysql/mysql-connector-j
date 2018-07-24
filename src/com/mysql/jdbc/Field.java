/*
  Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.

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

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.regex.PatternSyntaxException;

/**
 * Field is a class used to describe fields in a ResultSet
 */
public class Field {

    private static final int AUTO_INCREMENT_FLAG = 512;

    private static final int NO_CHARSET_INFO = -1;

    private byte[] buffer;

    private int collationIndex = 0;

    private String encoding = null;

    private int colDecimals;

    private short colFlag;

    private String collationName = null;

    private MySQLConnection connection = null;

    private String databaseName = null;

    private int databaseNameLength = -1;

    // database name info
    private int databaseNameStart = -1;

    protected int defaultValueLength = -1;

    // default value info - from COM_LIST_FIELDS execution
    protected int defaultValueStart = -1;

    private String fullName = null;

    private String fullOriginalName = null;

    private boolean isImplicitTempTable = false;

    private long length; // Internal length of the field;

    private int mysqlType = -1; // the MySQL type

    private String name; // The Field name

    private int nameLength;

    private int nameStart;

    private String originalColumnName = null;

    private int originalColumnNameLength = -1;

    // column name info (before aliasing)
    private int originalColumnNameStart = -1;

    private String originalTableName = null;

    private int originalTableNameLength = -1;

    // table name info (before aliasing)
    private int originalTableNameStart = -1;

    private int precisionAdjustFactor = 0;

    private int sqlType = -1; // the java.sql.Type

    private String tableName; // The Name of the Table

    private int tableNameLength;

    private int tableNameStart;

    private boolean useOldNameMetadata = false;

    private boolean isSingleBit;

    private int maxBytesPerChar;

    private final boolean valueNeedsQuoting;

    /**
     * Constructor used when communicating with 4.1 and newer servers
     */
    Field(MySQLConnection conn, byte[] buffer, int databaseNameStart, int databaseNameLength, int tableNameStart, int tableNameLength,
            int originalTableNameStart, int originalTableNameLength, int nameStart, int nameLength, int originalColumnNameStart, int originalColumnNameLength,
            long length, int mysqlType, short colFlag, int colDecimals, int defaultValueStart, int defaultValueLength, int charsetIndex) throws SQLException {
        this.connection = conn;
        this.buffer = buffer;
        this.nameStart = nameStart;
        this.nameLength = nameLength;
        this.tableNameStart = tableNameStart;
        this.tableNameLength = tableNameLength;
        this.length = length;
        this.colFlag = colFlag;
        this.colDecimals = colDecimals;
        this.mysqlType = mysqlType;

        // 4.1 field info...
        this.databaseNameStart = databaseNameStart;
        this.databaseNameLength = databaseNameLength;

        this.originalTableNameStart = originalTableNameStart;
        this.originalTableNameLength = originalTableNameLength;

        this.originalColumnNameStart = originalColumnNameStart;
        this.originalColumnNameLength = originalColumnNameLength;

        this.defaultValueStart = defaultValueStart;
        this.defaultValueLength = defaultValueLength;

        // If we're not running 4.1 or newer, use the connection's charset
        this.collationIndex = charsetIndex;

        // Map MySqlTypes to java.sql Types
        this.sqlType = MysqlDefs.mysqlToJavaType(this.mysqlType);

        checkForImplicitTemporaryTable();
        // Re-map to 'real' blob type, if we're a BLOB
        boolean isFromFunction = this.originalTableNameLength == 0;

        if (this.mysqlType == MysqlDefs.FIELD_TYPE_BLOB) {
            if (this.connection.getBlobsAreStrings() || (this.connection.getFunctionsNeverReturnBlobs() && isFromFunction)) {
                this.sqlType = Types.VARCHAR;
                this.mysqlType = MysqlDefs.FIELD_TYPE_VARCHAR;
            } else if (this.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary || !this.connection.versionMeetsMinimum(4, 1, 0)) {
                if (this.connection.getUseBlobToStoreUTF8OutsideBMP() && shouldSetupForUtf8StringInBlob()) {
                    setupForUtf8StringInBlob();
                } else {
                    setBlobTypeBasedOnLength();
                    this.sqlType = MysqlDefs.mysqlToJavaType(this.mysqlType);
                }
            } else {
                // *TEXT masquerading as blob
                this.mysqlType = MysqlDefs.FIELD_TYPE_VAR_STRING;
                this.sqlType = Types.LONGVARCHAR;
            }
        }

        if (this.sqlType == Types.TINYINT && this.length == 1 && this.connection.getTinyInt1isBit()) {
            // Adjust for pseudo-boolean
            if (conn.getTinyInt1isBit()) {
                if (conn.getTransformedBitIsBoolean()) {
                    this.sqlType = Types.BOOLEAN;
                } else {
                    this.sqlType = Types.BIT;
                }
            }

        }

        if (!isNativeNumericType() && !isNativeDateTimeType()) {
            this.encoding = this.connection.getEncodingForIndex(this.collationIndex);

            // ucs2, utf16, and utf32 cannot be used as a client character set, but if it was received from server under some circumstances we can parse them as
            // utf16
            if ("UnicodeBig".equals(this.encoding)) {
                this.encoding = "UTF-16";
            }

            // MySQL encodes JSON data with utf8mb4.
            if (this.mysqlType == MysqlDefs.FIELD_TYPE_JSON) {
                this.encoding = "UTF-8";
            }

            // Handle VARBINARY/BINARY (server doesn't have a different type for this

            boolean isBinary = isBinary();

            if (this.connection.versionMeetsMinimum(4, 1, 0) && this.mysqlType == MysqlDefs.FIELD_TYPE_VAR_STRING && isBinary
                    && this.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary) {
                if (this.connection.getFunctionsNeverReturnBlobs() && isFromFunction) {
                    this.sqlType = Types.VARCHAR;
                    this.mysqlType = MysqlDefs.FIELD_TYPE_VARCHAR;
                } else if (this.isOpaqueBinary()) {
                    this.sqlType = Types.VARBINARY;
                }
            }

            if (this.connection.versionMeetsMinimum(4, 1, 0) && this.mysqlType == MysqlDefs.FIELD_TYPE_STRING && isBinary
                    && this.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary) {
                //
                // Okay, this is a hack, but there's currently no way to easily distinguish something like DATE_FORMAT( ..) from the "BINARY" column type, other
                // than looking at the original column name.
                //

                if (isOpaqueBinary() && !this.connection.getBlobsAreStrings()) {
                    this.sqlType = Types.BINARY;
                }
            }

            if (this.mysqlType == MysqlDefs.FIELD_TYPE_BIT) {
                this.isSingleBit = this.length == 0
                        || this.length == 1 && (this.connection.versionMeetsMinimum(5, 0, 21) || this.connection.versionMeetsMinimum(5, 1, 10));

                if (!this.isSingleBit) {
                    this.colFlag |= 128; // Pretend this is a full binary(128) and blob(16) so that this field is de-serializable.
                    this.colFlag |= 16;
                    isBinary = true;
                }
            }

            //
            // Handle TEXT type (special case), Fix proposed by Peter McKeown
            //
            if ((this.sqlType == java.sql.Types.LONGVARBINARY) && !isBinary) {
                this.sqlType = java.sql.Types.LONGVARCHAR;
            } else if ((this.sqlType == java.sql.Types.VARBINARY) && !isBinary) {
                this.sqlType = java.sql.Types.VARCHAR;
            }
        } else {
            this.encoding = "US-ASCII";
        }

        //
        // Handle odd values for 'M' for floating point/decimal numbers
        //
        if (!isUnsigned()) {
            switch (this.mysqlType) {
                case MysqlDefs.FIELD_TYPE_DECIMAL:
                case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
                    this.precisionAdjustFactor = -1;

                    break;
                case MysqlDefs.FIELD_TYPE_DOUBLE:
                case MysqlDefs.FIELD_TYPE_FLOAT:
                    this.precisionAdjustFactor = 1;

                    break;
            }
        } else {
            switch (this.mysqlType) {
                case MysqlDefs.FIELD_TYPE_DOUBLE:
                case MysqlDefs.FIELD_TYPE_FLOAT:
                    this.precisionAdjustFactor = 1;

                    break;
            }
        }
        this.valueNeedsQuoting = determineNeedsQuoting();
    }

    private boolean shouldSetupForUtf8StringInBlob() throws SQLException {
        String includePattern = this.connection.getUtf8OutsideBmpIncludedColumnNamePattern();
        String excludePattern = this.connection.getUtf8OutsideBmpExcludedColumnNamePattern();

        if (excludePattern != null && !StringUtils.isEmptyOrWhitespaceOnly(excludePattern)) {
            try {
                if (getOriginalName().matches(excludePattern)) {
                    if (includePattern != null && !StringUtils.isEmptyOrWhitespaceOnly(includePattern)) {
                        try {
                            if (getOriginalName().matches(includePattern)) {
                                return true;
                            }
                        } catch (PatternSyntaxException pse) {
                            SQLException sqlEx = SQLError.createSQLException("Illegal regex specified for \"utf8OutsideBmpIncludedColumnNamePattern\"",
                                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.connection.getExceptionInterceptor());

                            if (!this.connection.getParanoid()) {
                                sqlEx.initCause(pse);
                            }

                            throw sqlEx;
                        }
                    }

                    return false;
                }
            } catch (PatternSyntaxException pse) {
                SQLException sqlEx = SQLError.createSQLException("Illegal regex specified for \"utf8OutsideBmpExcludedColumnNamePattern\"",
                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.connection.getExceptionInterceptor());

                if (!this.connection.getParanoid()) {
                    sqlEx.initCause(pse);
                }

                throw sqlEx;
            }
        }

        return true;
    }

    private void setupForUtf8StringInBlob() {
        if (this.length == MysqlDefs.LENGTH_TINYBLOB || this.length == MysqlDefs.LENGTH_BLOB) {
            this.mysqlType = MysqlDefs.FIELD_TYPE_VARCHAR;
            this.sqlType = Types.VARCHAR;
        } else {
            this.mysqlType = MysqlDefs.FIELD_TYPE_VAR_STRING;
            this.sqlType = Types.LONGVARCHAR;
        }

        this.collationIndex = CharsetMapping.MYSQL_COLLATION_INDEX_utf8;
    }

    /**
     * Constructor used when communicating with pre 4.1 servers
     */
    Field(MySQLConnection conn, byte[] buffer, int nameStart, int nameLength, int tableNameStart, int tableNameLength, int length, int mysqlType, short colFlag,
            int colDecimals) throws SQLException {
        this(conn, buffer, -1, -1, tableNameStart, tableNameLength, -1, -1, nameStart, nameLength, -1, -1, length, mysqlType, colFlag, colDecimals, -1, -1,
                NO_CHARSET_INFO);
    }

    /**
     * Constructor used by DatabaseMetaData methods.
     */
    Field(String tableName, String columnName, int jdbcType, int length) {
        this.tableName = tableName;
        this.name = columnName;
        this.length = length;
        this.sqlType = jdbcType;
        this.colFlag = 0;
        this.colDecimals = 0;
        this.valueNeedsQuoting = determineNeedsQuoting();
    }

    /**
     * Used by prepared statements to re-use result set data conversion methods
     * when generating bound parmeter retrieval instance for statement
     * interceptors.
     * 
     * @param tableName
     *            not used
     * @param columnName
     *            not used
     * @param charsetIndex
     *            the MySQL collation/character set index
     * @param jdbcType
     *            from java.sql.Types
     * @param length
     *            length in characters or bytes (for BINARY data).
     */
    Field(String tableName, String columnName, int charsetIndex, int jdbcType, int length) {
        this.tableName = tableName;
        this.name = columnName;
        this.length = length;
        this.sqlType = jdbcType;
        this.colFlag = 0;
        this.colDecimals = 0;
        this.collationIndex = charsetIndex;
        this.valueNeedsQuoting = determineNeedsQuoting();

        switch (this.sqlType) {
            case Types.BINARY:
            case Types.VARBINARY:
                this.colFlag |= 128;
                this.colFlag |= 16;
                break;
        }
    }

    private void checkForImplicitTemporaryTable() {
        this.isImplicitTempTable = this.tableNameLength > 5 && this.buffer[this.tableNameStart] == (byte) '#'
                && this.buffer[this.tableNameStart + 1] == (byte) 's' && this.buffer[this.tableNameStart + 2] == (byte) 'q'
                && this.buffer[this.tableNameStart + 3] == (byte) 'l' && this.buffer[this.tableNameStart + 4] == (byte) '_';
    }

    /**
     * Returns the Java encoding (if known) for this field.
     * 
     * @return the Java encoding
     */
    public String getEncoding() throws SQLException {
        return this.encoding;
    }

    public void setEncoding(String javaEncodingName, Connection conn) throws SQLException {
        this.encoding = javaEncodingName;
        try {
            this.collationIndex = CharsetMapping.getCollationIndexForJavaEncoding(javaEncodingName, conn);
        } catch (RuntimeException ex) {
            SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
            sqlEx.initCause(ex);
            throw sqlEx;
        }
    }

    public synchronized String getCollation() throws SQLException {
        if (this.collationName == null) {
            if (this.connection != null) {
                if (this.connection.versionMeetsMinimum(4, 1, 0)) {
                    if (this.connection.getUseDynamicCharsetInfo()) {
                        java.sql.DatabaseMetaData dbmd = this.connection.getMetaData();

                        String quotedIdStr = dbmd.getIdentifierQuoteString();

                        if (" ".equals(quotedIdStr)) {
                            quotedIdStr = "";
                        }

                        String csCatalogName = getDatabaseName();
                        String csTableName = getOriginalTableName();
                        String csColumnName = getOriginalName();

                        if (csCatalogName != null && csCatalogName.length() != 0 && csTableName != null && csTableName.length() != 0 && csColumnName != null
                                && csColumnName.length() != 0) {
                            StringBuilder queryBuf = new StringBuilder(csCatalogName.length() + csTableName.length() + 28);
                            queryBuf.append("SHOW FULL COLUMNS FROM ");
                            queryBuf.append(quotedIdStr);
                            queryBuf.append(csCatalogName);
                            queryBuf.append(quotedIdStr);
                            queryBuf.append(".");
                            queryBuf.append(quotedIdStr);
                            queryBuf.append(csTableName);
                            queryBuf.append(quotedIdStr);

                            java.sql.Statement collationStmt = null;
                            java.sql.ResultSet collationRs = null;

                            try {
                                collationStmt = this.connection.createStatement();

                                collationRs = collationStmt.executeQuery(queryBuf.toString());

                                while (collationRs.next()) {
                                    if (csColumnName.equals(collationRs.getString("Field"))) {
                                        this.collationName = collationRs.getString("Collation");

                                        break;
                                    }
                                }
                            } finally {
                                if (collationRs != null) {
                                    collationRs.close();
                                    collationRs = null;
                                }

                                if (collationStmt != null) {
                                    collationStmt.close();
                                    collationStmt = null;
                                }
                            }
                        }
                    } else {
                        try {
                            this.collationName = CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[this.collationIndex];
                        } catch (RuntimeException ex) {
                            SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
                            sqlEx.initCause(ex);
                            throw sqlEx;
                        }
                    }
                }
            }
        }

        return this.collationName;
    }

    public String getColumnLabel() throws SQLException {
        return getName(); // column name if not aliased, alias if used
    }

    public String getDatabaseName() throws SQLException {
        if ((this.databaseName == null) && (this.databaseNameStart != -1) && (this.databaseNameLength != -1)) {
            this.databaseName = getStringFromBytes(this.databaseNameStart, this.databaseNameLength);
        }

        return this.databaseName;
    }

    int getDecimals() {
        return this.colDecimals;
    }

    public String getFullName() throws SQLException {
        if (this.fullName == null) {
            StringBuilder fullNameBuf = new StringBuilder(getTableName().length() + 1 + getName().length());
            fullNameBuf.append(this.tableName);

            // much faster to append a char than a String
            fullNameBuf.append('.');
            fullNameBuf.append(this.name);
            this.fullName = fullNameBuf.toString();
            fullNameBuf = null;
        }

        return this.fullName;
    }

    public String getFullOriginalName() throws SQLException {
        getOriginalName();

        if (this.originalColumnName == null) {
            return null; // we don't have this information
        }

        if (this.fullName == null) {
            StringBuilder fullOriginalNameBuf = new StringBuilder(getOriginalTableName().length() + 1 + getOriginalName().length());
            fullOriginalNameBuf.append(this.originalTableName);

            // much faster to append a char than a String
            fullOriginalNameBuf.append('.');
            fullOriginalNameBuf.append(this.originalColumnName);
            this.fullOriginalName = fullOriginalNameBuf.toString();
            fullOriginalNameBuf = null;
        }

        return this.fullOriginalName;
    }

    public long getLength() {
        return this.length;
    }

    public synchronized int getMaxBytesPerCharacter() throws SQLException {
        if (this.maxBytesPerChar == 0) {
            this.maxBytesPerChar = this.connection.getMaxBytesPerChar(this.collationIndex, getEncoding());
        }
        return this.maxBytesPerChar;
    }

    public int getMysqlType() {
        return this.mysqlType;
    }

    public String getName() throws SQLException {
        if (this.name == null) {
            this.name = getStringFromBytes(this.nameStart, this.nameLength);
        }

        return this.name;
    }

    public String getNameNoAliases() throws SQLException {
        if (this.useOldNameMetadata) {
            return getName();
        }

        if (this.connection != null && this.connection.versionMeetsMinimum(4, 1, 0)) {
            return getOriginalName();
        }

        return getName();
    }

    public String getOriginalName() throws SQLException {
        if ((this.originalColumnName == null) && (this.originalColumnNameStart != -1) && (this.originalColumnNameLength != -1)) {
            this.originalColumnName = getStringFromBytes(this.originalColumnNameStart, this.originalColumnNameLength);
        }

        return this.originalColumnName;
    }

    public String getOriginalTableName() throws SQLException {
        if ((this.originalTableName == null) && (this.originalTableNameStart != -1) && (this.originalTableNameLength != -1)) {
            this.originalTableName = getStringFromBytes(this.originalTableNameStart, this.originalTableNameLength);
        }

        return this.originalTableName;
    }

    /**
     * Returns amount of correction that should be applied to the precision
     * value.
     * 
     * Different versions of MySQL report different precision values.
     * 
     * @return the amount to adjust precision value by.
     */
    public int getPrecisionAdjustFactor() {
        return this.precisionAdjustFactor;
    }

    public int getSQLType() {
        return this.sqlType;
    }

    /**
     * Create a string with the correct charset encoding from the byte-buffer
     * that contains the data for this field
     */
    private String getStringFromBytes(int stringStart, int stringLength) throws SQLException {
        if ((stringStart == -1) || (stringLength == -1)) {
            return null;
        }

        if (stringLength == 0) {
            return "";
        }

        String stringVal = null;

        if (this.connection != null) {
            if (this.connection.getUseUnicode()) {
                String javaEncoding = this.connection.getCharacterSetMetadata();

                if (javaEncoding == null) {
                    javaEncoding = this.connection.getEncoding();
                }

                if (javaEncoding != null) {
                    SingleByteCharsetConverter converter = null;

                    if (this.connection != null) {
                        converter = this.connection.getCharsetConverter(javaEncoding);
                    }

                    if (converter != null) { // we have a converter
                        stringVal = converter.toString(this.buffer, stringStart, stringLength);
                    } else {
                        // we have no converter, use JVM converter
                        try {
                            stringVal = StringUtils.toString(this.buffer, stringStart, stringLength, javaEncoding);
                        } catch (UnsupportedEncodingException ue) {
                            throw new RuntimeException(Messages.getString("Field.12") + javaEncoding + Messages.getString("Field.13"));
                        }
                    }
                } else {
                    // we have no encoding, use JVM standard charset
                    stringVal = StringUtils.toAsciiString(this.buffer, stringStart, stringLength);
                }
            } else {
                // we are not using unicode, so use JVM standard charset
                stringVal = StringUtils.toAsciiString(this.buffer, stringStart, stringLength);
            }
        } else {
            // we don't have a connection, so punt
            stringVal = StringUtils.toAsciiString(this.buffer, stringStart, stringLength);
        }

        return stringVal;
    }

    public String getTable() throws SQLException {
        return getTableName();
    }

    public String getTableName() throws SQLException {
        if (this.tableName == null) {
            this.tableName = getStringFromBytes(this.tableNameStart, this.tableNameLength);
        }

        return this.tableName;
    }

    public String getTableNameNoAliases() throws SQLException {
        if (this.connection.versionMeetsMinimum(4, 1, 0)) {
            return getOriginalTableName();
        }

        return getTableName(); // pre-4.1, no aliases returned
    }

    public boolean isAutoIncrement() {
        return ((this.colFlag & AUTO_INCREMENT_FLAG) > 0);
    }

    public boolean isBinary() {
        return ((this.colFlag & 128) > 0);
    }

    public boolean isBlob() {
        return ((this.colFlag & 16) > 0);
    }

    /**
     * Is this field owned by a server-created temporary table?
     */
    private boolean isImplicitTemporaryTable() {
        return this.isImplicitTempTable;
    }

    public boolean isMultipleKey() {
        return ((this.colFlag & 8) > 0);
    }

    boolean isNotNull() {
        return ((this.colFlag & 1) > 0);
    }

    boolean isOpaqueBinary() throws SQLException {

        //
        // Detect CHAR(n) CHARACTER SET BINARY which is a synonym for fixed-length binary types
        //

        if (this.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary && isBinary()
                && (this.getMysqlType() == MysqlDefs.FIELD_TYPE_STRING || this.getMysqlType() == MysqlDefs.FIELD_TYPE_VAR_STRING)) {

            if (this.originalTableNameLength == 0 && (this.connection != null && !this.connection.versionMeetsMinimum(5, 0, 25))) {
                return false; // Probably from function
            }

            // Okay, queries resolved by temp tables also have this 'signature', check for that

            return !isImplicitTemporaryTable();
        }

        return (this.connection.versionMeetsMinimum(4, 1, 0) && "binary".equalsIgnoreCase(getEncoding()));

    }

    public boolean isPrimaryKey() {
        return ((this.colFlag & 2) > 0);
    }

    /**
     * Is this field _definitely_ not writable?
     * 
     * @return true if this field can not be written to in an INSERT/UPDATE
     *         statement.
     */
    boolean isReadOnly() throws SQLException {
        if (this.connection.versionMeetsMinimum(4, 1, 0)) {
            String orgColumnName = getOriginalName();
            String orgTableName = getOriginalTableName();

            return !(orgColumnName != null && orgColumnName.length() > 0 && orgTableName != null && orgTableName.length() > 0);
        }

        return false; // we can't tell definitively in this case.
    }

    public boolean isUniqueKey() {
        return ((this.colFlag & 4) > 0);
    }

    public boolean isUnsigned() {
        return ((this.colFlag & 32) > 0);
    }

    public void setUnsigned() {
        this.colFlag |= 32;
    }

    public boolean isZeroFill() {
        return ((this.colFlag & 64) > 0);
    }

    //
    // MySQL only has one protocol-level BLOB type that it exposes which is FIELD_TYPE_BLOB, although we can divine what the actual type is by the length
    // reported ...
    //
    private void setBlobTypeBasedOnLength() {
        if (this.length == MysqlDefs.LENGTH_TINYBLOB) {
            this.mysqlType = MysqlDefs.FIELD_TYPE_TINY_BLOB;
        } else if (this.length == MysqlDefs.LENGTH_BLOB) {
            this.mysqlType = MysqlDefs.FIELD_TYPE_BLOB;
        } else if (this.length == MysqlDefs.LENGTH_MEDIUMBLOB) {
            this.mysqlType = MysqlDefs.FIELD_TYPE_MEDIUM_BLOB;
        } else if (this.length == MysqlDefs.LENGTH_LONGBLOB) {
            this.mysqlType = MysqlDefs.FIELD_TYPE_LONG_BLOB;
        }
    }

    private boolean isNativeNumericType() {
        return ((this.mysqlType >= MysqlDefs.FIELD_TYPE_TINY && this.mysqlType <= MysqlDefs.FIELD_TYPE_DOUBLE)
                || this.mysqlType == MysqlDefs.FIELD_TYPE_LONGLONG || this.mysqlType == MysqlDefs.FIELD_TYPE_YEAR);
    }

    private boolean isNativeDateTimeType() {
        return (this.mysqlType == MysqlDefs.FIELD_TYPE_DATE || this.mysqlType == MysqlDefs.FIELD_TYPE_NEWDATE || this.mysqlType == MysqlDefs.FIELD_TYPE_DATETIME
                || this.mysqlType == MysqlDefs.FIELD_TYPE_TIME || this.mysqlType == MysqlDefs.FIELD_TYPE_TIMESTAMP);
    }

    public boolean isCharsetApplicableType() {
        return (this.mysqlType == MysqlDefs.FIELD_TYPE_ENUM || this.mysqlType == MysqlDefs.FIELD_TYPE_JSON || this.mysqlType == MysqlDefs.FIELD_TYPE_SET
                || this.mysqlType == MysqlDefs.FIELD_TYPE_STRING || this.mysqlType == MysqlDefs.FIELD_TYPE_VAR_STRING
                || this.mysqlType == MysqlDefs.FIELD_TYPE_VARCHAR);
    }

    public void setConnection(MySQLConnection conn) {
        this.connection = conn;

        if (this.encoding == null || this.collationIndex == 0) {
            this.encoding = this.connection.getEncoding();
        }
    }

    void setMysqlType(int type) {
        this.mysqlType = type;
        this.sqlType = MysqlDefs.mysqlToJavaType(this.mysqlType);
    }

    protected void setUseOldNameMetadata(boolean useOldNameMetadata) {
        this.useOldNameMetadata = useOldNameMetadata;
    }

    @Override
    public String toString() {
        try {
            StringBuilder asString = new StringBuilder();
            asString.append(super.toString());
            asString.append("[");
            asString.append("catalog=");
            asString.append(this.getDatabaseName());
            asString.append(",tableName=");
            asString.append(this.getTableName());
            asString.append(",originalTableName=");
            asString.append(this.getOriginalTableName());
            asString.append(",columnName=");
            asString.append(this.getName());
            asString.append(",originalColumnName=");
            asString.append(this.getOriginalName());
            asString.append(",mysqlType=");
            asString.append(getMysqlType());
            asString.append("(");
            asString.append(MysqlDefs.typeToName(getMysqlType()));
            asString.append(")");
            asString.append(",flags=");

            if (isAutoIncrement()) {
                asString.append(" AUTO_INCREMENT");
            }

            if (isPrimaryKey()) {
                asString.append(" PRIMARY_KEY");
            }

            if (isUniqueKey()) {
                asString.append(" UNIQUE_KEY");
            }

            if (isBinary()) {
                asString.append(" BINARY");
            }

            if (isBlob()) {
                asString.append(" BLOB");
            }

            if (isMultipleKey()) {
                asString.append(" MULTI_KEY");
            }

            if (isUnsigned()) {
                asString.append(" UNSIGNED");
            }

            if (isZeroFill()) {
                asString.append(" ZEROFILL");
            }

            asString.append(", charsetIndex=");
            asString.append(this.collationIndex);
            asString.append(", charsetName=");
            asString.append(this.encoding);

            //if (this.buffer != null) {
            //	asString.append("\n\nData as received from server:\n\n");
            //	asString.append(StringUtils.dumpAsHex(this.buffer,
            //			this.buffer.length));
            //}

            asString.append("]");

            return asString.toString();
        } catch (Throwable t) {
            return super.toString();
        }
    }

    protected boolean isSingleBit() {
        return this.isSingleBit;
    }

    protected boolean getvalueNeedsQuoting() {
        return this.valueNeedsQuoting;
    }

    private boolean determineNeedsQuoting() {
        boolean retVal = false;

        switch (this.sqlType) {
            case Types.BIGINT:
            case Types.BIT:
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.INTEGER:
            case Types.NUMERIC:
            case Types.REAL:
            case Types.SMALLINT:
            case Types.TINYINT:
                retVal = false;
                break;
            default:
                retVal = true;
        }
        return retVal;

    }
}
