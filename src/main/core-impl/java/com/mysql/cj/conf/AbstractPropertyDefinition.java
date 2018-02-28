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

import com.mysql.cj.exceptions.ExceptionInterceptor;

public abstract class AbstractPropertyDefinition<T> implements PropertyDefinition<T>, Serializable {

    private static final long serialVersionUID = 2696624840927848766L;

    private String name;
    private String ccAlias;
    private T defaultValue;
    private boolean isRuntimeModifiable;
    private String description;
    private String sinceVersion;
    private String category;
    private int order;

    private int lowerBound;
    private int upperBound;

    public AbstractPropertyDefinition(String name, String camelCaseAlias, T defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion,
            String category, int orderInCategory) {

        this.name = name;
        this.ccAlias = camelCaseAlias;
        this.setDefaultValue(defaultValue);
        this.setRuntimeModifiable(isRuntimeModifiable);
        this.setDescription(description);
        this.setSinceVersion(sinceVersion);
        this.setCategory(category);
        this.setOrder(orderInCategory);
    }

    public AbstractPropertyDefinition(String name, String alias, T defaultValue, boolean isRuntimeModifiable, String description, String sinceVersion,
            String category, int orderInCategory, int lowerBound, int upperBound) {
        this(name, alias, defaultValue, isRuntimeModifiable, description, sinceVersion, category, orderInCategory);
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

    @Override
    public String getCcAlias() {
        return this.ccAlias;
    }

    @Override
    public boolean hasCcAlias() {
        return this.ccAlias != null && this.ccAlias.length() > 0;
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
        return null;
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
}
