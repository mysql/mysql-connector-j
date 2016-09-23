/*
  Copyright (c) 2007, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla.result;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.ValueDecoder;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.ResultsetRow;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.DataReadException;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.mysqla.MysqlaConstants;

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
     */
    private <T> T decodeAndCreateReturnValue(int columnIndex, byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        Field f = this.metadata.getFields()[columnIndex];

        // First, figure out which decoder method to call basing on the protocol value type from metadata;
        // it's the best way to find the appropriate decoder, we can't rely completely on MysqlType here
        // because the same MysqlType can be represented by different protocol types and also DatabaseMetaData methods,
        // eg. buildResultSet(), could imply unexpected conversions when substitutes RowData in ResultSet;
        switch (f.getMysqlTypeId()) {
            case MysqlaConstants.FIELD_TYPE_DATETIME:
            case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
                return this.valueDecoder.decodeTimestamp(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_DATE:
                return this.valueDecoder.decodeDate(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_TIME:
                return this.valueDecoder.decodeTime(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_TINY:
                if (f.isUnsigned()) {
                    return this.valueDecoder.decodeUInt1(bytes, offset, length, vf);
                }
                return this.valueDecoder.decodeInt1(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_YEAR:
            case MysqlaConstants.FIELD_TYPE_SHORT:
                if (f.isUnsigned()) {
                    return this.valueDecoder.decodeUInt2(bytes, offset, length, vf);
                }
                return this.valueDecoder.decodeInt2(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_LONG:
                if (f.isUnsigned()) {
                    return this.valueDecoder.decodeUInt4(bytes, offset, length, vf);
                }
                return this.valueDecoder.decodeInt4(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_INT24:
                return this.valueDecoder.decodeInt4(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_LONGLONG:
                if (f.isUnsigned()) {
                    return this.valueDecoder.decodeUInt8(bytes, offset, length, vf);
                }
                return this.valueDecoder.decodeInt8(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_FLOAT:
                return this.valueDecoder.decodeFloat(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_DOUBLE:
                return this.valueDecoder.decodeDouble(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_NEWDECIMAL:
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
            case MysqlaConstants.FIELD_TYPE_JSON:
                return this.valueDecoder.decodeByteArray(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_BIT:
                return this.valueDecoder.decodeBit(bytes, offset, length, vf);

            case MysqlaConstants.FIELD_TYPE_NULL:
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
            case YEAR:
                return this.valueDecoder.decodeInt2(bytes, offset, length, vf);
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
                return this.valueDecoder.decodeByteArray(bytes, offset, length, vf);

            case BIT:
                return this.valueDecoder.decodeBit(bytes, offset, length, vf);

            case DATETIME:
            case TIMESTAMP:
                return this.valueDecoder.decodeTimestamp(bytes, offset, length, vf);
            case DATE:
                return this.valueDecoder.decodeDate(bytes, offset, length, vf);
            case TIME:
                return this.valueDecoder.decodeTime(bytes, offset, length, vf);

            case NULL:
                return vf.createFromNull();

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
    protected <T> T getValueFromBytes(int columnIndex, byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (getNull(columnIndex)) {
            return vf.createFromNull();
        }

        // value factory may return null for zeroDateTimeBehavior=convertToNull so check the return value
        T retVal = decodeAndCreateReturnValue(columnIndex, bytes, offset, length, vf);
        this.wasNull = (retVal == null);
        return retVal;
    }

    @Override
    public Row setMetadata(ColumnDefinition f) {
        this.metadata = f;

        return this;
    }

    public boolean wasNull() {
        return this.wasNull;
    }
}
