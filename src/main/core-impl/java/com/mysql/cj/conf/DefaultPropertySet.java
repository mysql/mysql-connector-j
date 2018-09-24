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
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;

public class DefaultPropertySet implements PropertySet, Serializable {

    private static final long serialVersionUID = -5156024634430650528L;

    private final Map<PropertyKey, RuntimeProperty<?>> PROPERTY_KEY_TO_RUNTIME_PROPERTY = new HashMap<>();
    private final Map<String, RuntimeProperty<?>> PROPERTY_NAME_TO_RUNTIME_PROPERTY = new HashMap<>();

    public DefaultPropertySet() {
        for (PropertyDefinition<?> pdef : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.values()) {
            addProperty(pdef.createRuntimeProperty());
        }
    }

    @Override
    public void addProperty(RuntimeProperty<?> prop) {
        PropertyDefinition<?> def = prop.getPropertyDefinition();
        if (def.getPropertyKey() != null) {
            this.PROPERTY_KEY_TO_RUNTIME_PROPERTY.put(def.getPropertyKey(), prop);
        } else {
            this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.put(def.getName(), prop);
            if (def.hasCcAlias()) {
                this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.put(def.getCcAlias(), prop);
            }
        }
    }

    @Override
    public void removeProperty(String name) {
        PropertyKey key = PropertyKey.fromValue(name);
        if (key != null) {
            this.PROPERTY_KEY_TO_RUNTIME_PROPERTY.remove(key);
        } else {
            RuntimeProperty<?> prop = this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(name);
            if (prop != null) {
                if (!name.equals(prop.getPropertyDefinition().getName())) {
                    this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(prop.getPropertyDefinition().getName());
                } else if (prop.getPropertyDefinition().hasCcAlias()) {
                    this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.remove(prop.getPropertyDefinition().getCcAlias());
                }
            }
        }
    }

    @Override
    public void removeProperty(PropertyKey key) {
        this.PROPERTY_KEY_TO_RUNTIME_PROPERTY.remove(key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> RuntimeProperty<T> getProperty(String name) {
        try {
            RuntimeProperty<T> prop = null;
            PropertyKey key = PropertyKey.fromValue(name);
            if (key != null) {
                prop = (RuntimeProperty<T>) this.PROPERTY_KEY_TO_RUNTIME_PROPERTY.get(key);
            }
            // for some of PropertyKey values we don't have property definitions, thus they are cached as custom properties
            if (prop == null) {
                prop = (RuntimeProperty<T>) this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.get(name);
            }
            if (prop != null) {
                return prop;
            }
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ConnectionProperties.notFound", new Object[] { name }));

        } catch (ClassCastException ex) {
            // TODO improve message
            throw ExceptionFactory.createException(WrongArgumentException.class, ex.getMessage(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> RuntimeProperty<T> getProperty(PropertyKey key) {
        return (RuntimeProperty<T>) this.PROPERTY_KEY_TO_RUNTIME_PROPERTY.get(key);
    }

    @Override
    public RuntimeProperty<Boolean> getBooleanProperty(String name) {
        return getProperty(name);
    }

    @Override
    public RuntimeProperty<Boolean> getBooleanProperty(PropertyKey key) {
        return getProperty(key);
    }

    @Override
    public RuntimeProperty<Integer> getIntegerProperty(String name) {
        return getProperty(name);
    }

    @Override
    public RuntimeProperty<Integer> getIntegerProperty(PropertyKey key) {
        return getProperty(key);
    }

    @Override
    public RuntimeProperty<Long> getLongProperty(String name) {
        return getProperty(name);
    }

    @Override
    public RuntimeProperty<Long> getLongProperty(PropertyKey key) {
        return getProperty(key);
    }

    @Override
    public RuntimeProperty<Integer> getMemorySizeProperty(String name) {
        return getProperty(name);
    }

    @Override
    public RuntimeProperty<Integer> getMemorySizeProperty(PropertyKey key) {
        return getProperty(key);
    }

    @Override
    public RuntimeProperty<String> getStringProperty(String name) {
        return getProperty(name);
    }

    @Override
    public RuntimeProperty<String> getStringProperty(PropertyKey key) {
        return getProperty(key);
    }

    @Override
    public <T extends Enum<T>> RuntimeProperty<T> getEnumProperty(String name) {
        return getProperty(name);
    }

    @Override
    public <T extends Enum<T>> RuntimeProperty<T> getEnumProperty(PropertyKey key) {
        return getProperty(key);
    }

    public void initializeProperties(Properties props) {
        if (props != null) {
            Properties infoCopy = (Properties) props.clone();

            // TODO do we need to remove next properties (as it was before)?
            infoCopy.remove(PropertyKey.HOST.getKeyName());
            infoCopy.remove(PropertyKey.PORT.getKeyName());
            infoCopy.remove(PropertyKey.USER.getKeyName());
            infoCopy.remove(PropertyKey.PASSWORD.getKeyName());
            infoCopy.remove(PropertyKey.DBNAME.getKeyName());

            for (PropertyKey propKey : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.keySet()) {
                try {
                    RuntimeProperty<?> propToSet = getProperty(propKey);
                    propToSet.initializeFrom(infoCopy, null);

                } catch (CJException e) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
                }
            }

            // Translate legacy SSL properties if sslMode isn't explicitly set. Default sslMode is PREFERRED.
            RuntimeProperty<SslMode> sslMode = this.<SslMode> getEnumProperty(PropertyKey.sslMode);
            if (!sslMode.isExplicitlySet()) {
                RuntimeProperty<Boolean> useSSL = this.getBooleanProperty(PropertyKey.useSSL);
                RuntimeProperty<Boolean> verifyServerCertificate = this.getBooleanProperty(PropertyKey.verifyServerCertificate);
                RuntimeProperty<Boolean> requireSSL = this.getBooleanProperty(PropertyKey.requireSSL);
                if (useSSL.isExplicitlySet() || verifyServerCertificate.isExplicitlySet() || requireSSL.isExplicitlySet()) {
                    if (!useSSL.getValue()) {
                        sslMode.setValue(SslMode.DISABLED);
                    } else if (verifyServerCertificate.getValue()) {
                        sslMode.setValue(SslMode.VERIFY_CA);
                    } else if (requireSSL.getValue()) {
                        sslMode.setValue(SslMode.REQUIRED);
                    }
                }
            }

            // add user-defined properties
            for (Object key : infoCopy.keySet()) {
                String val = infoCopy.getProperty((String) key);
                PropertyDefinition<String> def = new StringPropertyDefinition((String) key, null, val, PropertyDefinitions.RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.unknown"), "8.0.10", PropertyDefinitions.CATEGORY_USER_DEFINED, Integer.MIN_VALUE);
                RuntimeProperty<String> p = new StringProperty(def);
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
        for (PropertyKey propKey : this.PROPERTY_KEY_TO_RUNTIME_PROPERTY.keySet()) {
            if (!props.containsKey(propKey.getKeyName())) {
                RuntimeProperty<?> propToGet = getProperty(propKey);
                String propValue = propToGet.getStringValue();
                if (propValue != null) {
                    props.setProperty(propToGet.getPropertyDefinition().getName(), propValue);
                }
            }
        }

        for (String propName : this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.keySet()) {
            if (!props.containsKey(propName)) {
                RuntimeProperty<?> propToGet = getProperty(propName);
                String propValue = propToGet.getStringValue();
                if (propValue != null) {
                    props.setProperty(propToGet.getPropertyDefinition().getName(), propValue);
                }
            }
        }
        return props;
    }

    @Override
    public void reset() {
        this.PROPERTY_KEY_TO_RUNTIME_PROPERTY.values().forEach(p -> p.resetValue());
        this.PROPERTY_NAME_TO_RUNTIME_PROPERTY.values().forEach(p -> p.resetValue());
        postInitialization();
    }
}
