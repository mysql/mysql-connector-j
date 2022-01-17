/*
 * Copyright (c) 2021, 2022, Oracle and/or its affiliates.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class NativeQueryAttributesBindings implements QueryAttributesBindings {
    Session session = null;
    private List<NativeQueryBindValue> bindAttributes = new ArrayList<>();

    public NativeQueryAttributesBindings(Session sess) {
        this.session = sess;
    }

    @Override
    public void setAttribute(String name, Object value) {
        MysqlType defaultMysqlType = value == null ? MysqlType.NULL : NativeQueryBindings.DEFAULT_MYSQL_TYPES.get(value.getClass());
        Object val = value;
        if (defaultMysqlType == null) {
            Optional<MysqlType> mysqlType = NativeQueryBindings.DEFAULT_MYSQL_TYPES.entrySet().stream()
                    .filter(m -> m.getKey().isAssignableFrom(value.getClass())).map(m -> m.getValue()).findFirst();
            if (mysqlType.isPresent()) {
                defaultMysqlType = mysqlType.get();
            } else {
                defaultMysqlType = MysqlType.VARCHAR;
                val = value.toString();
            }
        }

        NativeQueryBindValue bv = new NativeQueryBindValue(this.session);
        bv.setName(name);
        bv.setBinding(val, defaultMysqlType, 0, null);
        this.bindAttributes.add(bv);
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
    public void runThroughAll(Consumer<BindValue> bindAttribute) {
        this.bindAttributes.forEach(bindAttribute::accept);
    }

    @Override
    public void clearAttributes() {
        this.bindAttributes.clear();
    }
}
