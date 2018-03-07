/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.PropertyNotModifiableException;
import com.mysql.cj.exceptions.WrongArgumentException;

public class DefaultPropertySet implements PropertySet, Serializable {

    private static final long serialVersionUID = -5156024634430650528L;

    private final Map<String, RuntimeProperty<?>> PROPERTY_NAME_TO_RUNTIME_PROPERTY = new HashMap<>();

    public DefaultPropertySet() {

        for (PropertyDefinition<?> pdef : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.values()) {
            addProperty(pdef.createRuntimeProperty());
        }

    }

    @Override
    public void addProperty(RuntimeProperty<?> prop) {
        PropertyDefinition<?> def = prop.getPropertyDefinition();
        this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.put(def.getName(), prop);
        if (def.hasCcAlias()) {
            this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.put(def.getCcAlias(), prop);
        }
    }

    @Override
    public void removeProperty(String name) {
        RuntimeProperty<?> prop = this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(name);
        if (prop != null) {
            if (!name.equals(prop.getPropertyDefinition().getName())) {
                this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(prop.getPropertyDefinition().getName());
            } else if (prop.getPropertyDefinition().hasCcAlias()) {
                this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(prop.getPropertyDefinition().getCcAlias());
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ReadableProperty<T> getReadableProperty(String name) {
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
        return getReadableProperty(name);
    }

    @Override
    public ReadableProperty<Integer> getIntegerReadableProperty(String name) {
        return getReadableProperty(name);
    }

    @Override
    public ReadableProperty<Long> getLongReadableProperty(String name) {
        return getReadableProperty(name);
    }

    @Override
    public ReadableProperty<Integer> getMemorySizeReadableProperty(String name) {
        return getReadableProperty(name);
    }

    @Override
    public ReadableProperty<String> getStringReadableProperty(String name) {
        return getReadableProperty(name);
    }

    @Override
    public <T extends Enum<T>> ReadableProperty<T> getEnumReadableProperty(String name) {
        return getReadableProperty(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ModifiableProperty<T> getModifiableProperty(String name) {
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
            throw ExceptionFactory.createException(PropertyNotModifiableException.class,
                    Messages.getString("ConnectionProperties.dynamicChangeIsNotAllowed", new Object[] { "'" + prop.getPropertyDefinition().getName() + "'" }));
        }

        throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionProperties.notFound", new Object[] { name }));

    }

    @Override
    public ModifiableProperty<Boolean> getBooleanModifiableProperty(String name) {
        return getModifiableProperty(name);
    }

    @Override
    public ModifiableProperty<Integer> getIntegerModifiableProperty(String name) {
        return getModifiableProperty(name);
    }

    @Override
    public ModifiableProperty<Long> getLongModifiableProperty(String name) {
        return getModifiableProperty(name);
    }

    @Override
    public ModifiableProperty<Integer> getMemorySizeModifiableProperty(String name) {
        return getModifiableProperty(name);
    }

    @Override
    public ModifiableProperty<String> getStringModifiableProperty(String name) {
        return getModifiableProperty(name);
    }

    @Override
    public <T extends Enum<T>> ModifiableProperty<T> getEnumModifiableProperty(String name) {
        return getModifiableProperty(name);
    }

    public void initializeProperties(Properties props) {
        if (props != null) {
            Properties infoCopy = (Properties) props.clone();

            // TODO do we need to remove next properties (as it was before)?
            infoCopy.remove(PropertyDefinitions.HOST_PROPERTY_KEY);
            infoCopy.remove(PropertyDefinitions.PNAME_user);
            infoCopy.remove(PropertyDefinitions.PNAME_password);
            infoCopy.remove(PropertyDefinitions.DBNAME_PROPERTY_KEY);
            infoCopy.remove(PropertyDefinitions.PORT_PROPERTY_KEY);

            for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
                try {
                    ReadableProperty<?> propToSet = getReadableProperty(propName);
                    propToSet.initializeFrom(infoCopy, null);

                } catch (CJException e) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
                }
            }

            // add user-defined properties
            for (Object key : infoCopy.keySet()) {
                String val = infoCopy.getProperty((String) key);
                PropertyDefinition<String> def = new StringPropertyDefinition((String) key, null, val, PropertyDefinitions.RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.unknown"), "8.0.10", PropertyDefinitions.CATEGORY_USER_DEFINED, Integer.MIN_VALUE);
                RuntimeProperty<String> p = new ModifiableStringProperty(def);
                addProperty(p);
            }
            postInitialization();
        }
    }

    @Override
    public void postInitialization() {
        // no-op
    }

    @Override
    public Properties exposeAsProperties() {
        Properties props = new Properties();

        for (String propName : this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.keySet()) {
            if (!props.containsKey(propName)) {
                ReadableProperty<?> propToGet = getReadableProperty(propName);
                String propValue = propToGet.getStringValue();
                if (propValue != null) {
                    props.setProperty(propToGet.getPropertyDefinition().getName(), propValue);
                }
            }
        }
        return props;
    }
}
