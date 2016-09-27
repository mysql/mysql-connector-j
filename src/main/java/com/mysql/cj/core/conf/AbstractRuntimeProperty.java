/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.naming.RefAddr;
import javax.naming.Reference;

import com.mysql.cj.api.conf.PropertyDefinition;
import com.mysql.cj.api.conf.RuntimeProperty;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;

public abstract class AbstractRuntimeProperty<T> implements RuntimeProperty<T>, Serializable {

    private static final long serialVersionUID = -3424722534876438236L;

    private PropertyDefinition<T> propertyDefinition;

    protected T valueAsObject;

    protected T initialValueAsObject;

    protected boolean wasExplicitlySet = false;

    private List<WeakReference<RuntimePropertyListener>> listeners;

    public AbstractRuntimeProperty() {
    }

    protected AbstractRuntimeProperty(PropertyDefinition<T> propertyDefinition) {
        this.propertyDefinition = propertyDefinition;
        this.valueAsObject = getPropertyDefinition().getDefaultValue();
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
        this.valueAsObject = getPropertyDefinition().parseObject(value, exceptionInterceptor);
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
            this.listeners.add(new WeakReference<RuntimePropertyListener>(l));
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
