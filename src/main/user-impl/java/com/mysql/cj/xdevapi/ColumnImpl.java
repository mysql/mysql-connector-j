/*
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.MysqlType;
import com.mysql.cj.result.Field;

public class ColumnImpl implements Column {
    private Field field;

    /**
     * Constructor.
     * 
     * @param f
     *            {@link Field} object
     */
    public ColumnImpl(Field f) {
        this.field = f;
    }

    public String getSchemaName() {
        return this.field.getDatabaseName();
    }

    public String getTableName() {
        return this.field.getOriginalTableName();
    }

    public String getTableLabel() {
        return this.field.getTableName();
    }

    public String getColumnName() {
        return this.field.getOriginalName();
    }

    public String getColumnLabel() {
        return this.field.getName();
    }

    public Type getType() {
        switch (this.field.getMysqlType()) {
            case BIT:
                return Type.BIT;
            case BIGINT:
                int len = (int) this.field.getLength();
                if (len < 5) {
                    return Type.TINYINT;
                } else if (len < 7) {
                    return Type.SMALLINT;
                } else if (len < 10) {
                    return Type.MEDIUMINT;
                } else if (len < 12) {
                    return Type.INT;
                } else if (len < 21) {
                    return Type.BIGINT;
                }
                throw new IllegalArgumentException("Unknown field length `" + this.field.getLength() + "` for signed int");
            case BIGINT_UNSIGNED:
                len = (int) this.field.getLength();
                if (len < 4) {
                    return Type.TINYINT;
                } else if (len < 6) {
                    return Type.SMALLINT;
                } else if (len < 9) {
                    return Type.MEDIUMINT;
                } else if (len < 11) {
                    return Type.INT;
                } else if (len < 21) {
                    return Type.BIGINT;
                }
                throw new IllegalArgumentException("Unknown field length `" + this.field.getLength() + "` for unsigned int");
            case FLOAT:
            case FLOAT_UNSIGNED:
                return Type.FLOAT;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                return Type.DECIMAL;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return Type.DOUBLE;
            case CHAR:
            case VARCHAR:
                return Type.STRING;
            case JSON:
                return Type.JSON;
            case VARBINARY:
                return Type.BYTES;
            case TIME:
                return Type.TIME;
            case DATETIME:
                len = (int) this.field.getLength();
                if (len == 10) {
                    return Type.DATE;
                } else if (len > 18 && len < 27) {
                    return Type.DATETIME;
                }
                throw new IllegalArgumentException("Unknown field length `" + this.field.getLength() + "` for datetime");
            case TIMESTAMP:
                return Type.TIMESTAMP;
            case SET:
                return Type.SET;
            case ENUM:
                return Type.ENUM;
            case GEOMETRY:
                return Type.GEOMETRY;
            default:
                break;
        }
        throw new IllegalArgumentException("Unknown type in metadata: " + this.field.getMysqlType());
    }

    public long getLength() {
        return this.field.getLength();
    }

    public int getFractionalDigits() {
        return this.field.getDecimals();
    }

    public boolean isNumberSigned() {
        return MysqlType.isSigned(this.field.getMysqlType());
    }

    public String getCollationName() {
        return CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[this.field.getCollationIndex()];
    }

    public String getCharacterSetName() {
        return CharsetMapping.getMysqlCharsetNameForCollationIndex(this.field.getCollationIndex());
    }

    public boolean isPadded() {
        return this.field.isZeroFill() || (this.field.getMysqlType() == MysqlType.CHAR);
    }

    public boolean isNullable() {
        return !this.field.isNotNull();
    }

    public boolean isAutoIncrement() {
        return this.field.isAutoIncrement();
    }

    public boolean isPrimaryKey() {
        return this.field.isPrimaryKey();
    }

    public boolean isUniqueKey() {
        return this.field.isUniqueKey();
    }

    public boolean isPartKey() {
        return this.field.isMultipleKey();
    }
}
