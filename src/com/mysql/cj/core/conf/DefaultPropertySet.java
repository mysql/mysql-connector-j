/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.conf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.mysql.cj.api.conf.ModifiableProperty;
import com.mysql.cj.api.conf.PropertyDefinition;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.conf.RuntimeProperty;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.PropertyIsNotModiableException;
import com.mysql.cj.core.exception.WrongArgumentException;

public class DefaultPropertySet implements PropertySet, Serializable {

    private static final long serialVersionUID = -5156024634430650528L;

    private final Map<String, RuntimeProperty<?>> PROPERTY_NAME_TO_RUNTIME_PROPERTY = new HashMap<String, RuntimeProperty<?>>();

    public DefaultPropertySet() {

        for (PropertyDefinition<?> pdef : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.values()) {
            addProperty(pdef.createRuntimeProperty());
        }

    }

    @Override
    public void addProperty(RuntimeProperty<?> prop) {
        this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.put(prop.getPropertyDefinition().getName(), prop);
        this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.put(PropertyDefinitions.PROPERTY_NAME_TO_ALIAS.get(prop.getPropertyDefinition().getName()), prop);
    }

    @Override
    public void removeProperty(String name) {
        RuntimeProperty<?> prop = this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(name);
        if (prop != null) {
            if (name.equals(prop.getPropertyDefinition().getName())) {
                this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(PropertyDefinitions.PROPERTY_NAME_TO_ALIAS.get(prop.getPropertyDefinition().getName()));
            } else {
                this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(prop.getPropertyDefinition().getName());
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ReadableProperty<T> getReadableProperty(Class<T> clazz, String name) {
        try {
            ReadableProperty<T> prop = (ReadableProperty<T>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
            if (prop != null) {
                return prop;
            }
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionProperties.notFound", new Object[] { name }));

        } catch (ClassCastException ex) {
            // TODO improve message
            throw ExceptionFactory.createException(WrongArgumentException.class, ex.getMessage(), ex);
        }
    }

    @Override
    public ReadableProperty<Boolean> getBooleanReadableProperty(String name) {
        return getReadableProperty(Boolean.class, name);
    }

    @Override
    public ReadableProperty<Integer> getIntegerReadableProperty(String name) {
        return getReadableProperty(Integer.class, name);
    }

    @Override
    public ReadableProperty<Long> getLongReadableProperty(String name) {
        return getReadableProperty(Long.class, name);
    }

    @Override
    public ReadableProperty<Integer> getMemorySizeReadableProperty(String name) {
        return getReadableProperty(Integer.class, name);
    }

    @Override
    public ReadableProperty<String> getStringReadableProperty(String name) {
        return getReadableProperty(String.class, name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ModifiableProperty<T> getModifiableProperty(Class<T> clazz, String name) {
        RuntimeProperty<?> prop = this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);

        if (prop != null) {
            if (ModifiableProperty.class.isAssignableFrom(prop.getClass())) {
                try {
                    return (ModifiableProperty<T>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);

                } catch (ClassCastException ex) {
                    // TODO improve message
                    throw ExceptionFactory.createException(WrongArgumentException.class, ex.getMessage(), ex);
                }
            }
            throw ExceptionFactory.createException(PropertyIsNotModiableException.class,
                    Messages.getString("ConnectionProperties.dynamicChangeIsNotAllowed", new Object[] { "'" + prop.getPropertyDefinition().getName() + "'" }));
        }

        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionProperties.notFound", new Object[] { name }));

    }

    @Override
    public ModifiableProperty<Boolean> getBooleanModifiableProperty(String name) {
        return getModifiableProperty(Boolean.class, name);
    }

    @Override
    public ModifiableProperty<Integer> getIntegerModifiableProperty(String name) {
        return getModifiableProperty(Integer.class, name);
    }

    @Override
    public ModifiableProperty<Long> getLongModifiableProperty(String name) {
        return getModifiableProperty(Long.class, name);
    }

    @Override
    public ModifiableProperty<Integer> getMemorySizeModifiableProperty(String name) {
        return getModifiableProperty(Integer.class, name);
    }

    @Override
    public ModifiableProperty<String> getStringModifiableProperty(String name) {
        return getModifiableProperty(String.class, name);
    }

}
