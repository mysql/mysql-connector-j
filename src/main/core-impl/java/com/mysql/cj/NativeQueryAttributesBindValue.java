/*
 * Copyright (c) 2021, Oracle and/or its affiliates.
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

package com.mysql.cj;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.mysql.cj.protocol.a.NativeConstants;

public class NativeQueryAttributesBindValue implements QueryAttributesBindValue {
    private static final Map<Class<?>, Integer> JAVA_TO_MYSQL_FIELD_TYPE = new HashMap<>();
    static {
        JAVA_TO_MYSQL_FIELD_TYPE.put(String.class, MysqlType.FIELD_TYPE_STRING);
        JAVA_TO_MYSQL_FIELD_TYPE.put(Boolean.class, MysqlType.FIELD_TYPE_TINY);
        JAVA_TO_MYSQL_FIELD_TYPE.put(Byte.class, MysqlType.FIELD_TYPE_TINY);
        JAVA_TO_MYSQL_FIELD_TYPE.put(Short.class, MysqlType.FIELD_TYPE_SHORT);
        JAVA_TO_MYSQL_FIELD_TYPE.put(Integer.class, MysqlType.FIELD_TYPE_LONG);
        JAVA_TO_MYSQL_FIELD_TYPE.put(Long.class, MysqlType.FIELD_TYPE_LONGLONG);
        JAVA_TO_MYSQL_FIELD_TYPE.put(BigInteger.class, MysqlType.FIELD_TYPE_LONGLONG);
        JAVA_TO_MYSQL_FIELD_TYPE.put(Float.class, MysqlType.FIELD_TYPE_FLOAT);
        JAVA_TO_MYSQL_FIELD_TYPE.put(Double.class, MysqlType.FIELD_TYPE_DOUBLE);
        JAVA_TO_MYSQL_FIELD_TYPE.put(BigDecimal.class, MysqlType.FIELD_TYPE_DOUBLE);
        JAVA_TO_MYSQL_FIELD_TYPE.put(java.sql.Date.class, MysqlType.FIELD_TYPE_DATE);
        JAVA_TO_MYSQL_FIELD_TYPE.put(LocalDate.class, MysqlType.FIELD_TYPE_DATE);
        JAVA_TO_MYSQL_FIELD_TYPE.put(java.sql.Time.class, MysqlType.FIELD_TYPE_TIME);
        JAVA_TO_MYSQL_FIELD_TYPE.put(LocalTime.class, MysqlType.FIELD_TYPE_TIME);
        JAVA_TO_MYSQL_FIELD_TYPE.put(OffsetTime.class, MysqlType.FIELD_TYPE_TIME);
        JAVA_TO_MYSQL_FIELD_TYPE.put(Duration.class, MysqlType.FIELD_TYPE_TIME);
        JAVA_TO_MYSQL_FIELD_TYPE.put(LocalDateTime.class, MysqlType.FIELD_TYPE_DATETIME);
        JAVA_TO_MYSQL_FIELD_TYPE.put(java.sql.Timestamp.class, MysqlType.FIELD_TYPE_TIMESTAMP);
        JAVA_TO_MYSQL_FIELD_TYPE.put(Instant.class, MysqlType.FIELD_TYPE_TIMESTAMP);
        JAVA_TO_MYSQL_FIELD_TYPE.put(OffsetDateTime.class, MysqlType.FIELD_TYPE_TIMESTAMP);
        JAVA_TO_MYSQL_FIELD_TYPE.put(ZonedDateTime.class, MysqlType.FIELD_TYPE_TIMESTAMP);
        JAVA_TO_MYSQL_FIELD_TYPE.put(java.util.Date.class, MysqlType.FIELD_TYPE_TIMESTAMP);
        JAVA_TO_MYSQL_FIELD_TYPE.put(java.util.Calendar.class, MysqlType.FIELD_TYPE_TIMESTAMP);
    }

    /** The attribute name */
    private String name;

    /** The attribute value */
    public Object value;

    /** The attribute MySQL type */
    protected int type = MysqlType.FIELD_TYPE_NULL;

    protected NativeQueryAttributesBindValue(String name, Object value) {
        this.name = name;
        this.value = value;
        this.type = getMysqlFieldType(value);
    }

    private int getMysqlFieldType(Object obj) {
        if (obj == null) {
            return MysqlType.FIELD_TYPE_NULL;
        }

        Integer mysqlFieldType = JAVA_TO_MYSQL_FIELD_TYPE.get(obj.getClass());
        if (mysqlFieldType != null) {
            return mysqlFieldType;
        }

        Optional<Integer> mysqlType = JAVA_TO_MYSQL_FIELD_TYPE.entrySet().stream().filter(m -> m.getKey().isAssignableFrom(obj.getClass()))
                .map(m -> m.getValue()).findFirst();
        if (mysqlType.isPresent()) {
            return mysqlType.get();
        }

        // Fall-back to String.
        return MysqlType.FIELD_TYPE_STRING;
    }

    @Override
    public boolean isNull() {
        return this.type == MysqlType.FIELD_TYPE_NULL;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public int getType() {
        return this.type;
    }

    @Override
    public Object getValue() {
        return this.value;
    }

    @Override
    public long getBoundLength() {
        if (isNull()) {
            return 0;
        }

        switch (this.type) {
            case MysqlType.FIELD_TYPE_TINY:
                return NativeConstants.BIN_LEN_INT1;
            case MysqlType.FIELD_TYPE_SHORT:
                return NativeConstants.BIN_LEN_INT2;
            case MysqlType.FIELD_TYPE_LONG:
                return NativeConstants.BIN_LEN_INT4;
            case MysqlType.FIELD_TYPE_LONGLONG:
                return NativeConstants.BIN_LEN_INT8;
            case MysqlType.FIELD_TYPE_FLOAT:
                return NativeConstants.BIN_LEN_FLOAT;
            case MysqlType.FIELD_TYPE_DOUBLE:
                return NativeConstants.BIN_LEN_DOUBLE;
            case MysqlType.FIELD_TYPE_DATE:
                return NativeConstants.BIN_LEN_DATE + 1;
            case MysqlType.FIELD_TYPE_TIME:
                return NativeConstants.BIN_LEN_TIME_WITH_MICROS + 1;
            case MysqlType.FIELD_TYPE_DATETIME:
                return NativeConstants.BIN_LEN_TIMESTAMP_WITH_MICROS + 1;
            case MysqlType.FIELD_TYPE_TIMESTAMP:
                return NativeConstants.BIN_LEN_TIMESTAMP_WITH_TZ + 1;
            case MysqlType.FIELD_TYPE_STRING:
                return this.value.toString().length() + 9;
            default:
                return 0;
        }
    }
}
