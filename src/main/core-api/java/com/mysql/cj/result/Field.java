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

package com.mysql.cj.result;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.MysqlType;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.util.LazyString;

/**
 * Field is a class used to describe fields in a ResultSet
 */
public class Field {

    private int collationIndex = 0;

    private String encoding = "US-ASCII";

    private int colDecimals;

    private short colFlag;

    private LazyString databaseName = null;
    private LazyString tableName = null;
    private LazyString originalTableName = null;
    private LazyString columnName = null;
    private LazyString originalColumnName = null;

    private String fullName = null;

    private long length; // Internal length of the field;

    private int mysqlTypeId = -1; // the MySQL type ID in legacy protocol

    private MysqlType mysqlType = MysqlType.UNKNOWN;

    public Field(LazyString databaseName, LazyString tableName, LazyString originalTableName, LazyString columnName, LazyString originalColumnName, long length,
            int mysqlTypeId, short colFlag, int colDecimals, int collationIndex, String encoding, MysqlType mysqlType) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.originalTableName = originalTableName;
        this.columnName = columnName;
        this.originalColumnName = originalColumnName;
        this.length = length;
        this.colFlag = colFlag;
        this.colDecimals = colDecimals;
        this.mysqlTypeId = mysqlTypeId;
        this.collationIndex = collationIndex;

        // ucs2, utf16, and utf32 cannot be used as a client character set, but if it was received from server under some circumstances we can parse them as utf16
        this.encoding = "UnicodeBig".equals(encoding) ? "UTF-16" : encoding;

        // MySQL encodes JSON data with utf8mb4.
        if (mysqlType == MysqlType.JSON) {
            this.encoding = "UTF-8";
        }

        this.mysqlType = mysqlType;

        adjustFlagsByMysqlType();
    }

    private void adjustFlagsByMysqlType() {

        switch (this.mysqlType) {
            case BIT:
                if (this.length > 1) {
                    this.colFlag |= MysqlType.FIELD_FLAG_BINARY;
                    this.colFlag |= MysqlType.FIELD_FLAG_BLOB;
                }
                break;

            case BINARY:
            case VARBINARY:
                this.colFlag |= MysqlType.FIELD_FLAG_BINARY;
                this.colFlag |= MysqlType.FIELD_FLAG_BLOB;
                break;

            case DECIMAL_UNSIGNED:
            case TINYINT_UNSIGNED:
            case SMALLINT_UNSIGNED:
            case INT_UNSIGNED:
            case FLOAT_UNSIGNED:
            case DOUBLE_UNSIGNED:
            case BIGINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED:
                this.colFlag |= MysqlType.FIELD_FLAG_UNSIGNED;
                break;

            default:
                break;
        }

    }

    /**
     * Used by prepared statements to re-use result set data conversion methods
     * when generating bound parameter retrieval instance for statement interceptors.
     *
     * @param tableName
     *            not used
     * @param columnName
     *            not used
     * @param collationIndex
     *            the MySQL collation/character set index
     * @param encoding
     *            encoding of data in this field
     * @param mysqlType
     *            {@link MysqlType}
     * @param length
     *            length in characters or bytes (for BINARY data).
     */
    public Field(String tableName, String columnName, int collationIndex, String encoding, MysqlType mysqlType, int length) {

        this.databaseName = new LazyString(null);
        this.tableName = new LazyString(tableName);
        this.originalTableName = new LazyString(null);
        this.columnName = new LazyString(columnName);
        this.originalColumnName = new LazyString(null);
        this.length = length;
        this.mysqlType = mysqlType;
        this.colFlag = 0;
        this.colDecimals = 0;

        adjustFlagsByMysqlType();

        switch (mysqlType) {
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
                // TODO: this becomes moot when DBMD results aren't built from ByteArrayRow
                // it possibly overrides correct encoding already existing in the Field instance
                this.collationIndex = collationIndex;

                // ucs2, utf16, and utf32 cannot be used as a client character set, but if it was received from server under some circumstances we can parse them as utf16
                this.encoding = "UnicodeBig".equals(encoding) ? "UTF-16" : encoding;

                break;
            default:
                // ignoring charsets for non-string types
        }
    }

    /**
     * Returns the Java encoding for this field.
     *
     * @return the Java encoding
     */
    public String getEncoding() {
        return this.encoding;
    }

    // TODO Remove this after DBMD isn't using ByteArrayRow results.
    public void setEncoding(String javaEncodingName, ServerVersion version) {
        this.encoding = javaEncodingName;
        this.collationIndex = CharsetMapping.getCollationIndexForJavaEncoding(javaEncodingName, version);
    }

    public String getColumnLabel() {
        return getName();
    }

    public String getDatabaseName() {
        return this.databaseName.toString();
    }

    public int getDecimals() {
        return this.colDecimals;
    }

    public String getFullName() {
        if (this.fullName == null) {
            StringBuilder fullNameBuf = new StringBuilder(this.tableName.length() + 1 + this.columnName.length());
            fullNameBuf.append(this.tableName.toString());
            fullNameBuf.append('.');
            fullNameBuf.append(this.columnName.toString());
            this.fullName = fullNameBuf.toString();
        }

        return this.fullName;
    }

    public long getLength() {
        return this.length;
    }

    public int getMysqlTypeId() {
        return this.mysqlTypeId;
    }

    public void setMysqlTypeId(int id) {
        this.mysqlTypeId = id;
    }

    public String getName() {
        return this.columnName.toString();
    }

    public String getOriginalName() {
        return this.originalColumnName.toString();
    }

    public String getOriginalTableName() {
        return this.originalTableName.toString();
    }

    public int getJavaType() {
        return this.mysqlType.getJdbcType();
    }

    public String getTableName() {
        return this.tableName.toString();
    }

    public boolean isAutoIncrement() {
        return ((this.colFlag & MysqlType.FIELD_FLAG_AUTO_INCREMENT) > 0);
    }

    public boolean isBinary() {
        return ((this.colFlag & MysqlType.FIELD_FLAG_BINARY) > 0);
    }

    public void setBinary() {
        this.colFlag |= MysqlType.FIELD_FLAG_BINARY;
    }

    public boolean isBlob() {
        return ((this.colFlag & MysqlType.FIELD_FLAG_BLOB) > 0);
    }

    public void setBlob() {
        this.colFlag |= MysqlType.FIELD_FLAG_BLOB;
    }

    public boolean isMultipleKey() {
        return ((this.colFlag & MysqlType.FIELD_FLAG_MULTIPLE_KEY) > 0);
    }

    public boolean isNotNull() {
        return ((this.colFlag & MysqlType.FIELD_FLAG_NOT_NULL) > 0);
    }

    public boolean isPrimaryKey() {
        return ((this.colFlag & MysqlType.FIELD_FLAG_PRIMARY_KEY) > 0);
    }

    public boolean isFromFunction() {
        return this.originalTableName.length() == 0;
    }

    /**
     * Is this field _definitely_ not writable?
     *
     * @return true if this field can not be written to in an INSERT/UPDATE
     *         statement.
     */
    public boolean isReadOnly() {
        return this.originalColumnName.length() == 0 && this.originalTableName.length() == 0;
    }

    public boolean isUniqueKey() {
        return ((this.colFlag & MysqlType.FIELD_FLAG_UNIQUE_KEY) > 0);
    }

    public boolean isUnsigned() {
        return ((this.colFlag & MysqlType.FIELD_FLAG_UNSIGNED) > 0);
    }

    public boolean isZeroFill() {
        return ((this.colFlag & MysqlType.FIELD_FLAG_ZEROFILL) > 0);
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
            asString.append(getMysqlTypeId());
            asString.append("(");
            MysqlType ft = getMysqlType();
            if (ft.equals(MysqlType.UNKNOWN)) {
                asString.append(" Unknown MySQL Type # ");
                asString.append(getMysqlTypeId());
            } else {
                asString.append("FIELD_TYPE_");
                asString.append(ft.getName());
            }
            asString.append(")");
            asString.append(",sqlType=");
            asString.append(ft.getJdbcType());
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

            asString.append("]");

            return asString.toString();
        } catch (Throwable t) {
            return super.toString() + "[<unable to generate contents>]";
        }
    }

    public boolean isSingleBit() {
        return (this.length <= 1);
    }

    public boolean getValueNeedsQuoting() {
        switch (this.mysqlType) {
            case BIGINT:
            case BIGINT_UNSIGNED:
            case BIT:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case INT:
            case INT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case TINYINT:
            case TINYINT_UNSIGNED:
                return false;
            default:
                return true;
        }
    }

    public int getCollationIndex() {
        return this.collationIndex;
    }

    public MysqlType getMysqlType() {
        return this.mysqlType;
    }

    public void setMysqlType(MysqlType mysqlType) {
        this.mysqlType = mysqlType;
    }

    public short getFlags() {
        return this.colFlag;
    }

    public void setFlags(short colFlag) {
        this.colFlag = colFlag;
    }
}
