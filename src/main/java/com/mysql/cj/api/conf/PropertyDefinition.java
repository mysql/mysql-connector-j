/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.api.conf;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;

public interface PropertyDefinition<T> {

    /**
     * Returns true if property has fixed values based constraints.
     * 
     * @return
     */
    boolean hasValueConstraints();

    /**
     * Returns true if property has range-based constraints
     * 
     * @return
     */
    boolean isRangeBased();

    /**
     * Returns the property name.
     * 
     * @return
     */
    String getName();

    /**
     * Returns the property camel-case alias.
     * 
     * @return
     */
    String getCcAlias();

    /**
     * Returns true if property has a camel-case alias.
     * 
     * @return
     */
    boolean hasCcAlias();

    /**
     * Returns the default value.
     * 
     * @return
     */
    T getDefaultValue();

    /**
     * May the property be changed after initialization.
     * 
     * @return
     */
    boolean isRuntimeModifiable();

    /**
     * Returns the property description. Used for documentation.
     * 
     * @return
     */
    String getDescription();

    /**
     * Returns the driver version where the property was introduced first. Used for documentation.
     * 
     * @return
     */
    String getSinceVersion();

    /**
     * Returns the property category.
     * 
     * @return
     */
    String getCategory();

    /**
     * Returns the property order. Used as preferred property position in properties table in documentation.
     * 
     * @return
     */
    int getOrder();

    /**
     * Returns the list of allowable values.
     * 
     * @return
     */
    String[] getAllowableValues();

    /**
     * The lowest possible value of range-based property
     * 
     * @return
     */
    int getLowerBound();

    /**
     * The highest possible value of range-based property
     * 
     * @return
     */
    int getUpperBound();

    /**
     * Returns the value object parsed from it's string representation and checked against allowable values.
     * 
     * @return
     */
    T parseObject(String value, ExceptionInterceptor exceptionInterceptor);

    /**
     * Creates instance of ReadableProperty or ModifiableProperty depending on isRuntimeModifiable() result.
     * 
     * @return
     */
    RuntimeProperty<T> createRuntimeProperty();

}
