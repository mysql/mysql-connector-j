/*
 * Copyright (c) 2018, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.x;

import java.io.UnsupportedEncodingException;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.MysqlType;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.result.Field;
import com.mysql.cj.util.LazyString;
import com.mysql.cj.x.protobuf.MysqlxResultset.ColumnMetaData;
import com.mysql.cj.x.protobuf.MysqlxResultset.ColumnMetaData.FieldType;

/**
 * Factory producing {@link Field} from protocol message.
 */
public class FieldFactory implements ProtocolEntityFactory<Field, XMessage> {

    /**
     * Content-type used in type mapping.
     * c.f. mysqlx_resultset.proto
     */
    private static final int XPROTOCOL_COLUMN_BYTES_CONTENT_TYPE_GEOMETRY = 0x0001;
    private static final int XPROTOCOL_COLUMN_BYTES_CONTENT_TYPE_JSON = 0x0002;

    private static final int XPROTOCOL_COLUMN_FLAGS_UINT_ZEROFILL = 0x0001;
    private static final int XPROTOCOL_COLUMN_FLAGS_DOUBLE_UNSIGNED = 0x0001;
    private static final int XPROTOCOL_COLUMN_FLAGS_FLOAT_UNSIGNED = 0x0001;
    private static final int XPROTOCOL_COLUMN_FLAGS_DECIMAL_UNSIGNED = 0x0001;
    private static final int XPROTOCOL_COLUMN_FLAGS_BYTES_RIGHTPAD = 0x0001;
    private static final int XPROTOCOL_COLUMN_FLAGS_DATETIME_TIMESTAMP = 0x0001;
    private static final int XPROTOCOL_COLUMN_FLAGS_NOT_NULL = 0x0010;
    private static final int XPROTOCOL_COLUMN_FLAGS_PRIMARY_KEY = 0x0020;
    private static final int XPROTOCOL_COLUMN_FLAGS_UNIQUE_KEY = 0x0040;
    private static final int XPROTOCOL_COLUMN_FLAGS_MULTIPLE_KEY = 0x0080;
    private static final int XPROTOCOL_COLUMN_FLAGS_AUTO_INCREMENT = 0x0100;

    String metadataCharacterSet;

    public FieldFactory(String metadataCharSet) {
        this.metadataCharacterSet = metadataCharSet;
    }

    @Override
    public Field createFromMessage(XMessage message) {
        return columnMetaDataToField((ColumnMetaData) message.getMessage(), this.metadataCharacterSet);
    }

    /**
     * Convert a X Protocol {@link ColumnMetaData} message to a C/J {@link Field} object.
     *
     * @param col
     *            the message from the server
     * @param characterSet
     *            the encoding of the strings in the message
     * @return {@link Field}
     */
    private Field columnMetaDataToField(ColumnMetaData col, String characterSet) {
        try {
            LazyString databaseName = new LazyString(col.getSchema().toString(characterSet));
            LazyString tableName = new LazyString(col.getTable().toString(characterSet));
            LazyString originalTableName = new LazyString(col.getOriginalTable().toString(characterSet));
            LazyString columnName = new LazyString(col.getName().toString(characterSet));
            LazyString originalColumnName = new LazyString(col.getOriginalName().toString(characterSet));

            long length = Integer.toUnsignedLong(col.getLength());
            int decimals = col.getFractionalDigits();
            int collationIndex = 0;
            if (col.hasCollation()) {
                // TODO: support custom character set
                collationIndex = (int) col.getCollation();
            }

            String encoding = CharsetMapping.getJavaEncodingForCollationIndex(collationIndex);

            MysqlType mysqlType = findMysqlType(col.getType(), col.getContentType(), col.getFlags(), collationIndex);
            int mysqlTypeId = xProtocolTypeToMysqlType(col.getType(), col.getContentType());

            // flags translation; unsigned is handled in Field by checking the MysqlType, so here we check others
            short flags = (short) 0;
            if (col.getType().equals(FieldType.UINT) && 0 < (col.getFlags() & XPROTOCOL_COLUMN_FLAGS_UINT_ZEROFILL)) {
                flags |= MysqlType.FIELD_FLAG_ZEROFILL;
            } else if (col.getType().equals(FieldType.BYTES) && 0 < (col.getFlags() & XPROTOCOL_COLUMN_FLAGS_BYTES_RIGHTPAD)) {
                mysqlType = MysqlType.CHAR;
            } else if (col.getType().equals(FieldType.DATETIME) && 0 < (col.getFlags() & XPROTOCOL_COLUMN_FLAGS_DATETIME_TIMESTAMP)) {
                mysqlType = MysqlType.TIMESTAMP;
            }
            if ((col.getFlags() & XPROTOCOL_COLUMN_FLAGS_NOT_NULL) > 0) {
                flags |= MysqlType.FIELD_FLAG_NOT_NULL;
            }
            if ((col.getFlags() & XPROTOCOL_COLUMN_FLAGS_PRIMARY_KEY) > 0) {
                flags |= MysqlType.FIELD_FLAG_PRIMARY_KEY;
            }
            if ((col.getFlags() & XPROTOCOL_COLUMN_FLAGS_UNIQUE_KEY) > 0) {
                flags |= MysqlType.FIELD_FLAG_UNIQUE_KEY;
            }
            if ((col.getFlags() & XPROTOCOL_COLUMN_FLAGS_MULTIPLE_KEY) > 0) {
                flags |= MysqlType.FIELD_FLAG_MULTIPLE_KEY;
            }
            if ((col.getFlags() & XPROTOCOL_COLUMN_FLAGS_AUTO_INCREMENT) > 0) {
                flags |= MysqlType.FIELD_FLAG_AUTO_INCREMENT;
            }

            // According to SQL standard, NUMERIC_SCALE should be NULL for approximate numeric data types.
            // DECIMAL_NOT_SPECIFIED=31 is the MySQL internal constant value used to indicate that NUMERIC_SCALE is not applicable.
            // It's probably a mistake that it's exposed by protocol as a decimals and it should be replaced with 0. 
            switch (mysqlType) {
                case FLOAT:
                case FLOAT_UNSIGNED:
                case DOUBLE:
                case DOUBLE_UNSIGNED:
                    if (decimals == 31) {
                        decimals = 0;
                    }
                    break;

                default:
                    break;
            }

            Field f = new Field(databaseName, tableName, originalTableName, columnName, originalColumnName, length, mysqlTypeId, flags, decimals,
                    collationIndex, encoding, mysqlType);
            return f;
        } catch (UnsupportedEncodingException ex) {
            throw new WrongArgumentException("Unable to decode metadata strings", ex);
        }
    }

    private MysqlType findMysqlType(FieldType type, int contentType, int flags, int collationIndex) {
        switch (type) {
            case SINT:
                return MysqlType.BIGINT;
            case UINT:
                return MysqlType.BIGINT_UNSIGNED;
            case FLOAT:
                return 0 < (flags & XPROTOCOL_COLUMN_FLAGS_FLOAT_UNSIGNED) ? MysqlType.FLOAT_UNSIGNED : MysqlType.FLOAT;
            case DOUBLE:
                return 0 < (flags & XPROTOCOL_COLUMN_FLAGS_DOUBLE_UNSIGNED) ? MysqlType.DOUBLE_UNSIGNED : MysqlType.DOUBLE;
            case DECIMAL:
                return 0 < (flags & XPROTOCOL_COLUMN_FLAGS_DECIMAL_UNSIGNED) ? MysqlType.DECIMAL_UNSIGNED : MysqlType.DECIMAL;
            case BYTES:
                switch (contentType) {
                    case XPROTOCOL_COLUMN_BYTES_CONTENT_TYPE_GEOMETRY:
                        return MysqlType.GEOMETRY;
                    case XPROTOCOL_COLUMN_BYTES_CONTENT_TYPE_JSON:
                        return MysqlType.JSON;
                    default:
                        if (collationIndex == 33) {
                            return MysqlType.VARBINARY;
                        }
                        return MysqlType.VARCHAR;
                }
            case TIME:
                return MysqlType.TIME;
            case DATETIME:
                return MysqlType.DATETIME;
            case SET:
                return MysqlType.SET;
            case ENUM:
                return MysqlType.ENUM;
            case BIT:
                return MysqlType.BIT;
            // TODO: longlong
        }
        throw new WrongArgumentException("TODO: unknown field type: " + type);
    }

    /**
     * Map a X Protocol type code from `ColumnMetaData.FieldType' to a MySQL type constant. These are the only types that will be present in
     * {@link XProtocolRow}
     * results.
     *
     * @param type
     *            the type as the ColumnMetaData.FieldType
     * @param contentType
     *            the inner type
     * @return A <b>FIELD_TYPE</b> constant from {@link MysqlType} corresponding to the combination of input parameters.
     */
    private int xProtocolTypeToMysqlType(FieldType type, int contentType) {
        switch (type) {
            case SINT:
                // TODO: figure out ranges in detail and test them
                return MysqlType.FIELD_TYPE_LONGLONG;
            case UINT:
                return MysqlType.FIELD_TYPE_LONGLONG;
            case FLOAT:
                return MysqlType.FIELD_TYPE_FLOAT;
            case DOUBLE:
                return MysqlType.FIELD_TYPE_DOUBLE;
            case DECIMAL:
                return MysqlType.FIELD_TYPE_NEWDECIMAL;
            case BYTES:
                switch (contentType) {
                    case XPROTOCOL_COLUMN_BYTES_CONTENT_TYPE_GEOMETRY:
                        return MysqlType.FIELD_TYPE_GEOMETRY;
                    case XPROTOCOL_COLUMN_BYTES_CONTENT_TYPE_JSON:
                        return MysqlType.FIELD_TYPE_JSON;
                    default:
                        return MysqlType.FIELD_TYPE_VARCHAR;
                }
            case TIME:
                return MysqlType.FIELD_TYPE_TIME;
            case DATETIME:
                // may be a timestamp or just a date if time values are missing. metadata doesn't distinguish between the two
                return MysqlType.FIELD_TYPE_DATETIME;
            case SET:
                return MysqlType.FIELD_TYPE_SET;
            case ENUM:
                return MysqlType.FIELD_TYPE_ENUM;
            case BIT:
                return MysqlType.FIELD_TYPE_BIT;
            // TODO: longlong
        }
        throw new WrongArgumentException("TODO: unknown field type: " + type);
    }
}
