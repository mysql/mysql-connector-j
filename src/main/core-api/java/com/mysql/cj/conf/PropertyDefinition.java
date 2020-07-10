/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

import com.mysql.cj.exceptions.ExceptionInterceptor;

public interface PropertyDefinition<T> {

    /**
     * Does the property have fixed values based constraints.
     * 
     * @return true if property has fixed values based constraints.
     */
    boolean hasValueConstraints();

    /**
     * Returns true if property has range-based constraints
     * 
     * @return true if property has range-based constraints
     */
    boolean isRangeBased();

    /**
     * Get the property key.
     * 
     * @return {@link PropertyKey} or null if it's a custom property.
     */
    PropertyKey getPropertyKey();

    /**
     * Returns the property name.
     * 
     * @return the property name
     */
    String getName();

    /**
     * Returns the property camel-case alias.
     * 
     * @return the property camel-case alias.
     */
    String getCcAlias();

    /**
     * Returns true if property has a camel-case alias.
     * 
     * @return true if property has a camel-case alias.
     */
    boolean hasCcAlias();

    /**
     * Returns the default value.
     * 
     * @return default value
     */
    T getDefaultValue();

    /**
     * May the property be changed after initialization.
     * 
     * @return true if the property value may be changed after initialization.
     */
    boolean isRuntimeModifiable();

    /**
     * Returns the property description. Used for documentation.
     * 
     * @return property description
     */
    String getDescription();

    /**
     * Returns the driver version where the property was introduced first. Used for documentation.
     * 
     * @return the driver version where the property was introduced first
     */
    String getSinceVersion();

    /**
     * Returns the property category.
     * 
     * @return property category
     */
    String getCategory();

    /**
     * Returns the property order. Used as preferred property position in properties table in documentation.
     * 
     * @return property order
     */
    int getOrder();

    /**
     * Returns the list of allowable values.
     * 
     * @return the list of allowable values
     */
    String[] getAllowableValues();

    /**
     * The lowest possible value of range-based property
     * 
     * @return the lowest possible value of range-based property
     */
    int getLowerBound();

    /**
     * The highest possible value of range-based property
     * 
     * @return the highest possible value of range-based property
     */
    int getUpperBound();

    /**
     * Returns the value object parsed from it's string representation and checked against allowable values.
     * 
     * @param value
     *            value
     * @param exceptionInterceptor
     *            exception interceptor
     * 
     * @return the value object
     */
    T parseObject(String value, ExceptionInterceptor exceptionInterceptor);

    /**
     * Creates instance of ReadableProperty or ModifiableProperty depending on isRuntimeModifiable() result.
     * 
     * @return {@link RuntimeProperty} instance
     */
    RuntimeProperty<T> createRuntimeProperty();

}
