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
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.naming.RefAddr;
import javax.naming.Reference;

import com.mysql.cj.exceptions.ExceptionInterceptor;

public abstract class AbstractRuntimeProperty<T> implements RuntimeProperty<T>, Serializable {

    private static final long serialVersionUID = -3424722534876438236L;

    private PropertyDefinition<T> propertyDefinition;

    protected T value;

    protected T initialValue;

    protected boolean wasExplicitlySet = false;

    private List<WeakReference<RuntimePropertyListener>> listeners;

    public AbstractRuntimeProperty() {
    }

    protected AbstractRuntimeProperty(PropertyDefinition<T> propertyDefinition) {
        this.propertyDefinition = propertyDefinition;
        this.value = propertyDefinition.getDefaultValue();
        this.initialValue = propertyDefinition.getDefaultValue();
    }

    @Override
    public PropertyDefinition<T> getPropertyDefinition() {
        return this.propertyDefinition;
    }

    @Override
    public void initializeFrom(Properties extractFrom, ExceptionInterceptor exceptionInterceptor) {
        String extractedValue = extractFrom.getProperty(getPropertyDefinition().getName());
        extractFrom.remove(getPropertyDefinition().getName());
        initializeFrom(extractedValue, exceptionInterceptor);
    }

    @Override
    public void initializeFrom(Reference ref, ExceptionInterceptor exceptionInterceptor) {
        RefAddr refAddr = ref.get(getPropertyDefinition().getName());

        if (refAddr != null) {
            String refContentAsString = (String) refAddr.getContent();

            initializeFrom(refContentAsString, exceptionInterceptor);
        }
    }

    protected void initializeFrom(String extractedValue, ExceptionInterceptor exceptionInterceptor) {
        if (extractedValue != null) {
            setFromString(extractedValue, exceptionInterceptor);
        }
    }

    public void setFromString(String value, ExceptionInterceptor exceptionInterceptor) {
        this.value = getPropertyDefinition().parseObject(value, exceptionInterceptor);
        this.wasExplicitlySet = true;
    }

    @Override
    public void resetValue() {
        // no-op for readable properties
    }

    public boolean isExplicitlySet() {
        return this.wasExplicitlySet;
    }

    @Override
    public void addListener(RuntimePropertyListener l) {
        if (this.listeners == null) {
            this.listeners = new ArrayList<>();
        }
        if (!this.listeners.contains(l)) {
            this.listeners.add(new WeakReference<>(l));
        }
    }

    @Override
    public void removeListener(RuntimePropertyListener listener) {
        if (this.listeners != null) {
            for (WeakReference<RuntimePropertyListener> wr : this.listeners) {
                RuntimePropertyListener l = wr.get();
                if (l == listener) {
                    this.listeners.remove(wr);
                    break;
                }
            }
        }
    }

    protected void invokeListeners() {
        if (this.listeners != null) {
            for (WeakReference<RuntimePropertyListener> wr : this.listeners) {
                RuntimePropertyListener l = wr.get();
                if (l != null) {
                    l.handlePropertyChange(this);
                } else {
                    this.listeners.remove(wr);
                }
            }
        }
    }

}
