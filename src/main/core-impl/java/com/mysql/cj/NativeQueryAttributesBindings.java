/*
 * Copyright (c) 2021, 2024, Oracle and/or its affiliates.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Consumer;

public class NativeQueryAttributesBindings implements QueryAttributesBindings {

    // Query attributes use a different type mapping than other parameter bindings.
    private static final Map<Class<?>, MysqlType> DEFAULT_MYSQL_TYPES = new HashMap<>();
    static {
        DEFAULT_MYSQL_TYPES.put(String.class, MysqlType.CHAR);
        DEFAULT_MYSQL_TYPES.put(Boolean.class, MysqlType.TINYINT);
        DEFAULT_MYSQL_TYPES.put(Byte.class, MysqlType.TINYINT);
        DEFAULT_MYSQL_TYPES.put(Short.class, MysqlType.SMALLINT);
        DEFAULT_MYSQL_TYPES.put(Integer.class, MysqlType.INT);
        DEFAULT_MYSQL_TYPES.put(Long.class, MysqlType.BIGINT);
        DEFAULT_MYSQL_TYPES.put(BigInteger.class, MysqlType.BIGINT);
        DEFAULT_MYSQL_TYPES.put(Float.class, MysqlType.FLOAT);
        DEFAULT_MYSQL_TYPES.put(Double.class, MysqlType.DOUBLE);
        DEFAULT_MYSQL_TYPES.put(BigDecimal.class, MysqlType.DOUBLE);
        DEFAULT_MYSQL_TYPES.put(java.sql.Date.class, MysqlType.DATE);
        DEFAULT_MYSQL_TYPES.put(LocalDate.class, MysqlType.DATE);
        DEFAULT_MYSQL_TYPES.put(java.sql.Time.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(LocalTime.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(OffsetTime.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(Duration.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(LocalDateTime.class, MysqlType.DATETIME);
        DEFAULT_MYSQL_TYPES.put(java.sql.Timestamp.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(Instant.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(OffsetDateTime.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(ZonedDateTime.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(java.util.Date.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(java.util.Calendar.class, MysqlType.TIMESTAMP);
    }

    Session session = null;
    private List<NativeQueryBindValue> bindAttributes = new ArrayList<>();

    public NativeQueryAttributesBindings(Session sess) {
        this.session = sess;
    }

    @Override
    public void setAttribute(String name, Object value) {
        MysqlType defaultMysqlType = value == null ? MysqlType.NULL : DEFAULT_MYSQL_TYPES.get(value.getClass());
        Object val = value;
        if (defaultMysqlType == null) {
            Optional<MysqlType> mysqlType = DEFAULT_MYSQL_TYPES.entrySet().stream().filter(m -> m.getKey().isAssignableFrom(value.getClass()))
                    .map(Entry::getValue).findFirst();
            if (mysqlType.isPresent()) {
                defaultMysqlType = mysqlType.get();
            } else {
                defaultMysqlType = MysqlType.CHAR;
                val = value.toString();
            }
        }

        NativeQueryBindValue bv = new NativeQueryBindValue(this.session);
        bv.setName(name);
        bv.setBinding(val, defaultMysqlType, 0, null);
        this.bindAttributes.add(bv);
    }

    @Override
    public void removeAttribute(String name) {
        Optional<NativeQueryBindValue> bindValue = this.bindAttributes.stream().filter(b -> name.equalsIgnoreCase(b.getName())).findAny();
        if (bindValue.isPresent()) {
            this.bindAttributes.remove(bindValue.get());
        }
    }

    @Override
    public int getCount() {
        return this.bindAttributes.size();
    }

    @Override
    public BindValue getAttributeValue(int index) {
        return this.bindAttributes.get(index);
    }

    @Override
    public boolean containsAttribute(String name) {
        return this.bindAttributes.stream().filter(b -> name.equalsIgnoreCase(b.getName())).findAny().isPresent();
    }

    @Override
    public void runThroughAll(Consumer<BindValue> bindAttribute) {
        this.bindAttributes.forEach(bindAttribute::accept);
    }

    @Override
    public void clearAttributes() {
        this.bindAttributes.clear();
    }

}
