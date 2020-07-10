/*
 * Copyright (c) 2017, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.conf;

import java.util.Arrays;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.util.StringUtils;

public class EnumPropertyDefinition<T extends Enum<T>> extends AbstractPropertyDefinition<T> {

    private static final long serialVersionUID = -3297521968759540444L;

    private Class<T> enumType;

    public EnumPropertyDefinition(PropertyKey key, T defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory) {
        super(key, defaultValue, isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
        if (defaultValue == null) {
            throw ExceptionFactory.createException("Enum property '" + key.getKeyName() + "' cannot be initialized with null.");
        }
        this.enumType = defaultValue.getDeclaringClass();
    }

    @Override
    public String[] getAllowableValues() {
        return Arrays.stream(this.enumType.getEnumConstants()).map(T::toString).sorted().toArray(String[]::new);
    }

    @Override
    public T parseObject(String value, ExceptionInterceptor exceptionInterceptor) {
        try {
            return Enum.valueOf(this.enumType, value.toUpperCase());
        } catch (Exception e) {
            throw ExceptionFactory.createException(
                    Messages.getString("PropertyDefinition.1",
                            new Object[] { getName(), StringUtils.stringArrayToString(getAllowableValues(), "'", "', '", "' or '", "'"), value }),
                    e, exceptionInterceptor);
        }
    }

    /**
     * Creates an instance of EnumProperty.
     * 
     * @return RuntimeProperty
     */
    @Override
    public RuntimeProperty<T> createRuntimeProperty() {
        return new EnumProperty<>(this);
    }
}
