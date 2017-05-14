/*
  Copyright (c) 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.conf;

import java.util.Arrays;

import com.mysql.cj.api.conf.RuntimeProperty;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.util.StringUtils;

public class EnumPropertyDefinition<T extends Enum<T>> extends AbstractPropertyDefinition<T> {

    private static final long serialVersionUID = -3297521968759540444L;

    private Class<T> enumType;

    public EnumPropertyDefinition(String name, T defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory) {
        super(name, defaultValue, isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
        if (defaultValue == null) {
            throw ExceptionFactory.createException("Enum property '" + name + "' cannot be initialized with null.");
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
     * Creates an instance of ReadableEnumProperty or ModifiableEnumProperty depending on isRuntimeModifiable() result.
     * 
     * @return
     */
    @Override
    public RuntimeProperty<T> createRuntimeProperty() {
        return isRuntimeModifiable() ? new ModifiableEnumProperty<>(this) : new ReadableEnumProperty<>(this);
    }
}
