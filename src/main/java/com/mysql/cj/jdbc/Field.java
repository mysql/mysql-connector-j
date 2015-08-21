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

package com.mysql.cj.jdbc;

import java.sql.Types;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.core.CharsetMapping;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.util.LazyString;
import com.mysql.cj.mysqla.MysqlaConstants;

/**
 * Field is a class used to describe fields in a ResultSet
 */
public class Field {

    private static final int AUTO_INCREMENT_FLAG = 512;

    private int collationIndex = 0;

    private String encoding = "US-ASCII";

    private int colDecimals;

    private short colFlag;

    private PropertySet propertySet;

    private LazyString databaseName = null;
    private LazyString tableName = null;
    private LazyString originalTableName = null;
    private LazyString columnName = null;
    private LazyString originalColumnName = null;

    private String fullName = null;

    private long length; // Internal length of the field;

    private int mysqlType = -1; // the MySQL type

    private int precisionAdjustFactor = 0;

    private int sqlType = -1; // the java.sql.Type

    private boolean isSingleBit;

    public Field(PropertySet propertySet, LazyString databaseName, LazyString tableName, LazyString originalTableName, LazyString columnName,
            LazyString originalColumnName, long length, int mysqlType, short colFlag, int colDecimals, int collationIndex, String encoding) {
        this.propertySet = propertySet;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.originalTableName = originalTableName;
        this.columnName = columnName;
        this.originalColumnName = originalColumnName;
        this.length = length;
        this.colFlag = colFlag;
        this.colDecimals = colDecimals;
        this.mysqlType = mysqlType;
        this.collationIndex = collationIndex;
        this.encoding = encoding;

        // Map MySqlTypes to java.sql Types
        this.sqlType = MysqlDefs.mysqlToJavaType(this.mysqlType);

        // Re-map to 'real' blob type, if we're a BLOB
        boolean isFromFunction = this.originalTableName.length() == 0;

        if (this.mysqlType == MysqlaConstants.FIELD_TYPE_BLOB) {
            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_blobsAreStrings).getValue()
                    || (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).getValue() && isFromFunction)) {
                if (this.length == MysqlDefs.LENGTH_TINYBLOB || this.length == MysqlDefs.LENGTH_BLOB) {
                    this.mysqlType = MysqlaConstants.FIELD_TYPE_VARCHAR;
                    this.sqlType = Types.VARCHAR;
                } else {
                    this.mysqlType = MysqlaConstants.FIELD_TYPE_VAR_STRING;
                    this.sqlType = Types.LONGVARCHAR;
                }
            } else if (this.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary) {
                // MySQL only has one protocol-level BLOB type that it exposes which is FIELD_TYPE_BLOB, although we can divine what the actual type is by the
                // length reported ...
                if (this.length == MysqlDefs.LENGTH_TINYBLOB) {
                    this.mysqlType = MysqlaConstants.FIELD_TYPE_TINY_BLOB;
                } else if (this.length == MysqlDefs.LENGTH_BLOB) {
                    this.mysqlType = MysqlaConstants.FIELD_TYPE_BLOB;
                } else if (this.length == MysqlDefs.LENGTH_MEDIUMBLOB) {
                    this.mysqlType = MysqlaConstants.FIELD_TYPE_MEDIUM_BLOB;
                } else if (this.length == MysqlDefs.LENGTH_LONGBLOB) {
                    this.mysqlType = MysqlaConstants.FIELD_TYPE_LONG_BLOB;
                }
                this.sqlType = MysqlDefs.mysqlToJavaType(this.mysqlType);
            } else {
                // *TEXT masquerading as blob
                this.mysqlType = MysqlaConstants.FIELD_TYPE_VAR_STRING;
                this.sqlType = Types.LONGVARCHAR;
            }
        }

        boolean tinyInt1isBit = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_tinyInt1isBit).getValue();
        if (this.sqlType == Types.TINYINT && this.length == 1 && tinyInt1isBit) {
            // Adjust for pseudo-boolean
            if (tinyInt1isBit) {
                if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean).getValue()) {
                    this.sqlType = Types.BOOLEAN;
                } else {
                    this.sqlType = Types.BIT;
                }
            }

        }

        if (!isNativeNumericType() && !isNativeDateTimeType()) {
            // ucs2, utf16, and utf32 cannot be used as a client character set, but if it was received from server under some circumstances we can parse them as
            // utf16
            if ("UnicodeBig".equals(this.encoding)) {
                this.encoding = "UTF-16";
            }

            // Handle VARBINARY/BINARY (server doesn't have a different type for this

            boolean isBinary = isBinary();

            if (this.mysqlType == MysqlaConstants.FIELD_TYPE_VAR_STRING && isBinary && this.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary) {
                if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs).getValue() && isFromFunction) {
                    this.sqlType = Types.VARCHAR;
                    this.mysqlType = MysqlaConstants.FIELD_TYPE_VARCHAR;
                } else if (this.isOpaqueBinary()) {
                    this.sqlType = Types.VARBINARY;
                }
            }

            if (this.mysqlType == MysqlaConstants.FIELD_TYPE_STRING && isBinary && this.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary) {
                //
                // Okay, this is a hack, but there's currently no way to easily distinguish something like DATE_FORMAT( ..) from the "BINARY" column type, other
                // than looking at the original column name.
                //

                if (isOpaqueBinary() && !this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_blobsAreStrings).getValue()) {
                    this.sqlType = Types.BINARY;
                }
            }

            if (this.mysqlType == MysqlaConstants.FIELD_TYPE_BIT) {
                this.isSingleBit = (this.length == 0);

                if (this.length == 1) {
                    this.isSingleBit = true;
                }

                if (this.isSingleBit) {
                    this.sqlType = Types.BIT;
                } else {
                    this.sqlType = Types.VARBINARY;
                    this.colFlag |= 128; // we need to pretend this is a full
                    this.colFlag |= 16; // binary blob
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
        }

        //
        // Handle odd values for 'M' for floating point/decimal numbers
        //
        if (!isUnsigned()) {
            switch (this.mysqlType) {
                case MysqlaConstants.FIELD_TYPE_DECIMAL:
                case MysqlaConstants.FIELD_TYPE_NEW_DECIMAL:
                    this.precisionAdjustFactor = -1;

                    break;
                case MysqlaConstants.FIELD_TYPE_DOUBLE:
                case MysqlaConstants.FIELD_TYPE_FLOAT:
                    this.precisionAdjustFactor = 1;

                    break;
            }
        } else {
            switch (this.mysqlType) {
                case MysqlaConstants.FIELD_TYPE_DOUBLE:
                case MysqlaConstants.FIELD_TYPE_FLOAT:
                    this.precisionAdjustFactor = 1;

                    break;
            }
        }
    }

    /**
     * Constructor used by DatabaseMetaData methods.
     */
    public Field(String tableName, String columnName, int jdbcType, int length) {
        this.databaseName = new LazyString(null);
        this.tableName = new LazyString(tableName);
        this.originalTableName = new LazyString(null);
        this.columnName = new LazyString(columnName);
        this.originalColumnName = new LazyString(null);
        this.length = length;
        this.sqlType = jdbcType;
        this.colFlag = 0;
        this.colDecimals = 0;
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
     * @param collationIndex
     *            the MySQL collation/character set index
     * @param encoding
     *            encoding of data in this field
     * @param jdbcType
     *            from java.sql.Types
     * @param length
     *            length in characters or bytes (for BINARY data).
     */
    public Field(String tableName, String columnName, int collationIndex, String encoding, int jdbcType, int length) {
        this.databaseName = new LazyString(null);
        this.tableName = new LazyString(tableName);
        this.originalTableName = new LazyString(null);
        this.columnName = new LazyString(columnName);
        this.originalColumnName = new LazyString(null);
        this.length = length;
        this.sqlType = jdbcType;
        this.colFlag = 0;
        this.colDecimals = 0;
        this.collationIndex = collationIndex;
        this.encoding = encoding;

        switch (this.sqlType) {
            case Types.BINARY:
            case Types.VARBINARY:
                this.colFlag |= 128;
                this.colFlag |= 16;
                break;
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

    /**
     * @todo Remove this after DBMD isn't using ByteArrayRow results.
     */
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

    public int getMysqlType() {
        return this.mysqlType;
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

    public String getTableName() {
        return this.tableName.toString();
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
        return this.tableName.length() > 0 && this.tableName.toString().startsWith("#sql_");
    }

    public boolean isMultipleKey() {
        return ((this.colFlag & 8) > 0);
    }

    public boolean isNotNull() {
        return ((this.colFlag & 1) > 0);
    }

    public boolean isOpaqueBinary() {
        // Detect CHAR(n) CHARACTER SET BINARY which is a synonym for fixed-length binary types
        if (this.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary && isBinary()
                && (this.getMysqlType() == MysqlaConstants.FIELD_TYPE_STRING || this.getMysqlType() == MysqlaConstants.FIELD_TYPE_VAR_STRING)) {
            // queries resolved by temp tables also have this 'signature', check for that
            return !isImplicitTemporaryTable();
        }

        return "binary".equalsIgnoreCase(getEncoding());
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
    public boolean isReadOnly() {
        return this.originalColumnName.length() == 0 && this.originalTableName.length() == 0;
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

    private boolean isNativeNumericType() {
        return ((this.mysqlType >= MysqlaConstants.FIELD_TYPE_TINY && this.mysqlType <= MysqlaConstants.FIELD_TYPE_DOUBLE)
                || this.mysqlType == MysqlaConstants.FIELD_TYPE_LONGLONG || this.mysqlType == MysqlaConstants.FIELD_TYPE_YEAR);
    }

    private boolean isNativeDateTimeType() {
        return (this.mysqlType == MysqlaConstants.FIELD_TYPE_DATE || this.mysqlType == MysqlaConstants.FIELD_TYPE_NEWDATE
                || this.mysqlType == MysqlaConstants.FIELD_TYPE_DATETIME || this.mysqlType == MysqlaConstants.FIELD_TYPE_TIME || this.mysqlType == MysqlaConstants.FIELD_TYPE_TIMESTAMP);
    }

    void setMysqlType(int type) {
        this.mysqlType = type;
        this.sqlType = MysqlDefs.mysqlToJavaType(this.mysqlType);
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
            asString.append(",sqlType=");
            asString.append(this.sqlType);
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
        return this.isSingleBit;
    }

    public boolean getvalueNeedsQuoting() {
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
                return false;
        }
        return true;
    }

    public int getCollationIndex() {
        return this.collationIndex;
    }
}
