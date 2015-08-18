/*
  Copyright (c) 2007, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.sql.SQLException;
import java.sql.Types;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.ValueDecoder;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.DataReadException;
import com.mysql.cj.core.io.StringConverter;
import com.mysql.cj.mysqla.MysqlaConstants;

/**
 * Classes that implement this interface represent one row of data from the MySQL server that might be stored in different ways depending on whether the result
 * set was streaming (so they wrap a reusable packet), or whether the result set was cached or via a server-side cursor (so they represent a byte[][]).
 * 
 * Notice that <strong>no</strong> bounds checking is expected for implementors of this interface, it happens in ResultSetImpl.
 */
public abstract class ResultSetRow {
    protected ExceptionInterceptor exceptionInterceptor;

    private StringConverter stringConverter;

    protected ResultSetRow(ExceptionInterceptor exceptionInterceptor) {
        this.exceptionInterceptor = exceptionInterceptor;
    }

    public void setStringConverter(StringConverter stringConverter) {
        this.stringConverter = stringConverter;
    }

    /**
     * The metadata of the fields of this result set.
     */
    protected Field[] metadata;

    protected ValueDecoder valueDecoder;

    /** Did the previous value retrieval find a NULL? */
    private boolean wasNull;

    /**
     * Called during navigation to next row to close all open
     * streams.
     */
    public abstract void closeOpenStreams();

    /**
     * Returns the value at the given column (index starts at 0) "raw" (i.e.
     * as-returned by the server).
     * 
     * @param index
     *            of the column value (starting at 0) to return.
     * @return the value for the given column (including NULL if it is)
     * @throws SQLException
     *             if an error occurs while retrieving the value.
     */
    public abstract byte[] getColumnValue(int index) throws SQLException;

    /**
     * Check whether a column is NULL and update the 'wasNull' status.
     */
    public boolean getNull(int columnIndex) throws SQLException {
        this.wasNull = isNull(columnIndex);
        return this.wasNull;
    }

    /**
     * Retrieve a value for the given column. This is the main facility to access values from the ResultSetRow.
     *
     * @param columnIndex
     *            index of column to retrieve value from (0-indexed, not JDBC 1-indexed)
     * @param vf
     *            value factory used to create the return value after decoding
     * @return The return value from the value factory
     */
    public abstract <T> T getValue(int columnIndex, ValueFactory<T> vf) throws SQLException;

    /**
     * Decode the wire-level result bytes and call the value factory.
     */
    private <T> T decodeAndCreateReturnValue(int columnIndex, byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        Field f = this.metadata[columnIndex];

        // figure out which decoder method to call based on the value type from metadata
        switch (f.getMysqlType()) {
            case MysqlaConstants.FIELD_TYPE_DATETIME:
            case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
                return this.valueDecoder.decodeTimestamp(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_NEWDATE:
            case MysqlaConstants.FIELD_TYPE_DATE:
                return this.valueDecoder.decodeDate(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_TIME:
                return this.valueDecoder.decodeTime(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_TINY:
                if (f.isUnsigned()) {
                    return this.valueDecoder.decodeUInt1(bytes, offset, length, vf);
                } else {
                    return this.valueDecoder.decodeInt1(bytes, offset, length, vf);
                }

            case MysqlaConstants.FIELD_TYPE_YEAR:
            case MysqlaConstants.FIELD_TYPE_SHORT:
                if (f.isUnsigned()) {
                    return this.valueDecoder.decodeUInt2(bytes, offset, length, vf);
                } else {
                    return this.valueDecoder.decodeInt2(bytes, offset, length, vf);
                }

            case MysqlaConstants.FIELD_TYPE_LONG:
                if (f.isUnsigned()) {
                    return this.valueDecoder.decodeUInt4(bytes, offset, length, vf);
                } else {
                    return this.valueDecoder.decodeInt4(bytes, offset, length, vf);
                }

            case MysqlaConstants.FIELD_TYPE_INT24:
                return this.valueDecoder.decodeInt4(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_LONGLONG:
                if (f.isUnsigned()) {
                    return this.valueDecoder.decodeUInt8(bytes, offset, length, vf);
                } else {
                    return this.valueDecoder.decodeInt8(bytes, offset, length, vf);
                }

            case MysqlaConstants.FIELD_TYPE_FLOAT:
                return this.valueDecoder.decodeFloat(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_DOUBLE:
                return this.valueDecoder.decodeDouble(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_NEW_DECIMAL:
            case MysqlaConstants.FIELD_TYPE_DECIMAL:
                return this.valueDecoder.decodeDecimal(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_VAR_STRING:
            case MysqlaConstants.FIELD_TYPE_VARCHAR:
            case MysqlaConstants.FIELD_TYPE_STRING:
            case MysqlaConstants.FIELD_TYPE_TINY_BLOB:
            case MysqlaConstants.FIELD_TYPE_MEDIUM_BLOB:
            case MysqlaConstants.FIELD_TYPE_LONG_BLOB:
            case MysqlaConstants.FIELD_TYPE_BLOB:
            case MysqlaConstants.FIELD_TYPE_ENUM:
            case MysqlaConstants.FIELD_TYPE_SET:
            case MysqlaConstants.FIELD_TYPE_GEOMETRY:
                return this.valueDecoder.decodeByteArray(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_BIT:
                return this.valueDecoder.decodeBit(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_NULL:
                return vf.createFromNull();
        }

        // hack for some internal code that creates rows without MySQL types, only JDBC types incl ps bindings as RS, DBMD
        switch (f.getSQLType()) {
            case Types.BOOLEAN:
                return this.valueDecoder.decodeByteArray(bytes, offset, length, vf);
            case Types.INTEGER:
                return this.valueDecoder.decodeInt4(bytes, offset, length, vf);
            case Types.SMALLINT:
                return this.valueDecoder.decodeInt2(bytes, offset, length, vf);
            case Types.BIGINT:
                return this.valueDecoder.decodeUInt8(bytes, offset, length, vf);

            case Types.DOUBLE:
            case Types.FLOAT:
                return this.valueDecoder.decodeDouble(bytes, offset, length, vf);
            case Types.DECIMAL:
            case Types.REAL:
                return this.valueDecoder.decodeDecimal(bytes, offset, length, vf);

            case Types.VARCHAR:
            case Types.CHAR:
                return this.valueDecoder.decodeByteArray(bytes, offset, length, vf);

            case Types.ARRAY:
            case Types.BINARY:
            case Types.BIT:
            case Types.BLOB:
            case Types.CLOB:
            case Types.DATALINK:
            case Types.DATE:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.LONGNVARCHAR:
            case Types.LONGVARBINARY:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.NULL:
            case Types.NUMERIC:
            case Types.NVARCHAR:
            case Types.OTHER:
            case Types.REF:
            case Types.ROWID:
            case Types.SQLXML:
            case Types.STRUCT:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.TINYINT:
            case Types.VARBINARY:
        }

        throw new DataReadException(Messages.getString("ResultSet.UnknownSourceType"));
    }

    /**
     * Get a value from a byte array. The byte array is interpreted by the {@link com.mysql.cj.api.io.ValueDecoder} which uses the value factory create the
     * return value.
     *
     * @param columnIndex
     *            The (internal) index of the column
     * @param bytes
     *            byte array
     * @param offset
     *            offset into byte array
     * @param length
     *            length of value in byte array
     * @param vf
     *            value factory
     */
    protected <T> T getValueFromBytes(int columnIndex, byte[] bytes, int offset, int length, ValueFactory<T> vf) throws SQLException {
        if (isNull(columnIndex)) {
            this.wasNull = true;
            return vf.createFromNull();
        }

        // value factory may return null for zeroDateTimeBehavior=convertToNull so check the return value
        T retVal = decodeAndCreateReturnValue(columnIndex, bytes, offset, length, vf);
        this.wasNull = (retVal == null);
        return retVal;
    }

    /**
     * Is the column value at the given index (which starts at 0) NULL?
     * 
     * @param index
     *            of the column value (starting at 0) to check.
     * 
     * @return true if the column value is NULL, false if not.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public abstract boolean isNull(int index) throws SQLException;

    /**
     * Returns the length of the column at the given index (which starts at 0).
     * 
     * @param index
     *            of the column value (starting at 0) for which to return the
     *            length.
     * @return the length of the requested column, 0 if null (clients of this
     *         interface should use isNull() beforehand to determine status of
     *         NULL values in the column).
     * 
     * @throws SQLException
     */
    public abstract long length(int index) throws SQLException;

    /**
     * Sets the given column value (only works currently with
     * ByteArrayRowHolder).
     * 
     * @param index
     *            index of the column value (starting at 0) to set.
     * @param value
     *            the (raw) value to set
     * 
     * @throws SQLException
     *             if an error occurs, or the concrete RowHolder doesn't support
     *             this operation.
     */
    public abstract void setColumnValue(int index, byte[] value) throws SQLException;

    public ResultSetRow setMetadata(Field[] f) throws SQLException {
        this.metadata = f;

        return this;
    }

    public boolean wasNull() {
        return this.wasNull;
    }
}
