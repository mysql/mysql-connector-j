/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

import com.mysql.cj.api.conf.PropertyDefinition;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.ExceptionFactory;

public abstract class AbstractPropertyDefinition<T> implements PropertyDefinition<T>, Serializable {

    private static final long serialVersionUID = 2696624840927848766L;

    private String name;
    private T defaultValue;
    private boolean isRuntimeModifiable;
    private String description;
    private String sinceVersion;
    private String category;
    private int order;

    private String[] allowableValues = null;

    private int lowerBound;
    private int upperBound;

    public AbstractPropertyDefinition(String name, T defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory) {

        this.setName(name);
        this.setDefaultValue(defaultValue);
        this.setRuntimeModifiable(isRuntimeModifiable);
        this.setDescription(description);
        this.setSinceVersion(sinceVersion);
        this.setCategory(category);
        this.setOrder(orderInCategory);
        //this.valueAsObject = defaultValueToSet;
        //this.required = false;
    }

    public AbstractPropertyDefinition(String name, T defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory, String[] allowableValues) {
        this(name, defaultValue, isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
        this.setAllowableValues(allowableValues);
    }

    public AbstractPropertyDefinition(String name, T defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion, String category,
            int orderInCategory, int lowerBound, int upperBound) {
        this(name, defaultValue, isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
        this.setLowerBound(lowerBound);
        this.setUpperBound(upperBound);
    }

    public boolean hasValueConstraints() {
        return (getAllowableValues() != null) && (getAllowableValues().length > 0);
    }

    public boolean isRangeBased() {
        return false;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getDefaultValue() {
        return this.defaultValue;
    }

    public void setDefaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isRuntimeModifiable() {
        return this.isRuntimeModifiable;
    }

    public void setRuntimeModifiable(boolean isRuntimeModifiable) {
        this.isRuntimeModifiable = isRuntimeModifiable;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getSinceVersion() {
        return this.sinceVersion;
    }

    public void setSinceVersion(String sinceVersion) {
        this.sinceVersion = sinceVersion;
    }

    public String getCategory() {
        return this.category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public int getOrder() {
        return this.order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public String[] getAllowableValues() {
        return this.allowableValues;
    }

    public void setAllowableValues(String[] allowableValues) {
        this.allowableValues = allowableValues;
    }

    public int getLowerBound() {
        return this.lowerBound;
    }

    public void setLowerBound(int lowerBound) {
        this.lowerBound = lowerBound;
    }

    public int getUpperBound() {
        return this.upperBound;
    }

    public void setUpperBound(int upperBound) {
        this.upperBound = upperBound;
    }

    public abstract T parseObject(String value, ExceptionInterceptor exceptionInterceptor);

    public void validateAllowableValues(String valueToValidate, ExceptionInterceptor exceptionInterceptor) {
        String[] validateAgainst = getAllowableValues();

        if (valueToValidate == null || validateAgainst == null || validateAgainst.length == 0) {
            return;
        }

        for (int i = 0; i < validateAgainst.length; i++) {
            if ((validateAgainst[i] != null) && validateAgainst[i].equalsIgnoreCase(valueToValidate)) {
                return;
            }
        }

        StringBuilder errorMessageBuf = new StringBuilder();
        errorMessageBuf.append(Messages.getString("PropertyDefinition.1", new Object[] { getName(), validateAgainst[0] }));
        for (int i = 1; i < (validateAgainst.length - 1); i++) {
            errorMessageBuf.append(Messages.getString("PropertyDefinition.2", new Object[] { validateAgainst[i] }));
        }
        errorMessageBuf.append(Messages.getString("PropertyDefinition.3", new Object[] { validateAgainst[validateAgainst.length - 1], valueToValidate }));
        throw ExceptionFactory.createException(errorMessageBuf.toString(), exceptionInterceptor);
    }
}
