/*
 * Copyright (c) 2007, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol.result;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.ResultsetRow;
import com.mysql.cj.protocol.ValueDecoder;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.ValueFactory;

public abstract class AbstractResultsetRow implements ResultsetRow {

    protected ExceptionInterceptor exceptionInterceptor;

    protected AbstractResultsetRow(ExceptionInterceptor exceptionInterceptor) {
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * The metadata of the fields of this result set.
     */
    protected ColumnDefinition metadata;

    protected ValueDecoder valueDecoder;

    /** Did the previous value retrieval find a NULL? */
    protected boolean wasNull;

    /**
     * Decode the wire-level result bytes and call the value factory.
     *
     * @param columnIndex
     *            column index
     * @param bytes
     *            bytes array with result data
     * @param offset
     *            offset in array
     * @param length
     *            data length
     * @param vf
     *            {@link ValueFactory}
     * @param <T>
     *            value type
     * @return value
     */
    private <T> T decodeAndCreateReturnValue(int columnIndex, byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        Field f = this.metadata.getFields()[columnIndex];

        // First, figure out which decoder method to call basing on the protocol value type from metadata;
        // it's the best way to find the appropriate decoder, we can't rely completely on MysqlType here
        // because the same MysqlType can be represented by different protocol types and also DatabaseMetaData methods,
        // eg. buildResultSet(), could imply unexpected conversions when substitutes RowData in ResultSet;
        switch (f.getMysqlTypeId()) {
            case MysqlType.FIELD_TYPE_DATETIME:
                return this.valueDecoder.decodeDatetime(bytes, offset, length, f.getDecimals(), vf);

            case MysqlType.FIELD_TYPE_TIMESTAMP:
                return this.valueDecoder.decodeTimestamp(bytes, offset, length, f.getDecimals(), vf);

            case MysqlType.FIELD_TYPE_DATE:
                return this.valueDecoder.decodeDate(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_TIME:
                return this.valueDecoder.decodeTime(bytes, offset, length, f.getDecimals(), vf);

            case MysqlType.FIELD_TYPE_TINY:
                return f.isUnsigned() ? this.valueDecoder.decodeUInt1(bytes, offset, length, vf) : this.valueDecoder.decodeInt1(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_YEAR:
                return this.valueDecoder.decodeYear(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_SHORT:
                return f.isUnsigned() ? this.valueDecoder.decodeUInt2(bytes, offset, length, vf) : this.valueDecoder.decodeInt2(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_LONG:
                return f.isUnsigned() ? this.valueDecoder.decodeUInt4(bytes, offset, length, vf) : this.valueDecoder.decodeInt4(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_INT24:
                return this.valueDecoder.decodeInt4(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_LONGLONG:
                return f.isUnsigned() ? this.valueDecoder.decodeUInt8(bytes, offset, length, vf) : this.valueDecoder.decodeInt8(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_FLOAT:
                return this.valueDecoder.decodeFloat(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_DOUBLE:
                return this.valueDecoder.decodeDouble(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_NEWDECIMAL:
            case MysqlType.FIELD_TYPE_DECIMAL:
                return this.valueDecoder.decodeDecimal(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_VAR_STRING:
            case MysqlType.FIELD_TYPE_VARCHAR:
            case MysqlType.FIELD_TYPE_STRING:
            case MysqlType.FIELD_TYPE_TINY_BLOB:
            case MysqlType.FIELD_TYPE_MEDIUM_BLOB:
            case MysqlType.FIELD_TYPE_LONG_BLOB:
            case MysqlType.FIELD_TYPE_BLOB:
            case MysqlType.FIELD_TYPE_ENUM:
            case MysqlType.FIELD_TYPE_GEOMETRY:
            case MysqlType.FIELD_TYPE_JSON:
                return this.valueDecoder.decodeByteArray(bytes, offset, length, f, vf);

            case MysqlType.FIELD_TYPE_SET:
                return this.valueDecoder.decodeSet(bytes, offset, length, f, vf);

            case MysqlType.FIELD_TYPE_BIT:
                return this.valueDecoder.decodeBit(bytes, offset, length, vf);

            case MysqlType.FIELD_TYPE_NULL:
                return vf.createFromNull();
        }

        // If the protocol type isn't available then select decoder basing on MysqlType; that's for some internal
        // code that creates rows without MySQL protocol types, only MysqlType types, including PS bindings as RS, DBMD
        switch (f.getMysqlType()) {
            case TINYINT:
                return this.valueDecoder.decodeInt1(bytes, offset, length, vf);
            case TINYINT_UNSIGNED:
                return this.valueDecoder.decodeUInt1(bytes, offset, length, vf);
            case SMALLINT:
                return this.valueDecoder.decodeInt2(bytes, offset, length, vf);
            case YEAR:
                return this.valueDecoder.decodeYear(bytes, offset, length, vf);
            case SMALLINT_UNSIGNED:
                return this.valueDecoder.decodeUInt2(bytes, offset, length, vf);
            case INT:
            case MEDIUMINT:
                return this.valueDecoder.decodeInt4(bytes, offset, length, vf);
            case INT_UNSIGNED:
            case MEDIUMINT_UNSIGNED:
                return this.valueDecoder.decodeUInt4(bytes, offset, length, vf);
            case BIGINT:
                return this.valueDecoder.decodeInt8(bytes, offset, length, vf);
            case BIGINT_UNSIGNED:
                return this.valueDecoder.decodeUInt8(bytes, offset, length, vf);
            case FLOAT:
            case FLOAT_UNSIGNED:
                return this.valueDecoder.decodeFloat(bytes, offset, length, vf);
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return this.valueDecoder.decodeDouble(bytes, offset, length, vf);
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                return this.valueDecoder.decodeDecimal(bytes, offset, length, vf);

            case BOOLEAN:
            case VARBINARY:
            case VARCHAR:
            case BINARY:
            case CHAR:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
            case SET:
            case GEOMETRY:
            case UNKNOWN:
                return this.valueDecoder.decodeByteArray(bytes, offset, length, f, vf);

            case BIT:
                return this.valueDecoder.decodeBit(bytes, offset, length, vf);

            case DATETIME:
            case TIMESTAMP:
                return this.valueDecoder.decodeTimestamp(bytes, offset, length, f.getDecimals(), vf);
            case DATE:
                return this.valueDecoder.decodeDate(bytes, offset, length, vf);
            case TIME:
                return this.valueDecoder.decodeTime(bytes, offset, length, f.getDecimals(), vf);

            case NULL:
                return vf.createFromNull();

        }

        throw new DataReadException(Messages.getString("ResultSet.UnknownSourceType"));
    }

    /**
     * Get a value from a byte array. The byte array is interpreted by the {@link com.mysql.cj.protocol.ValueDecoder} which uses the value factory create the
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
     * @param <T>
     *            value type
     * @return value
     */
    protected <T> T getValueFromBytes(int columnIndex, byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (getNull(columnIndex)) {
            return vf.createFromNull();
        }

        // value factory may return null for zeroDateTimeBehavior=CONVERT_TO_NULL so check the return value
        T retVal = decodeAndCreateReturnValue(columnIndex, bytes, offset, length, vf);
        this.wasNull = retVal == null;
        return retVal;
    }

    @Override
    public Row setMetadata(ColumnDefinition f) {
        this.metadata = f;

        return this;
    }

    @Override
    public boolean wasNull() {
        return this.wasNull;
    }

}
