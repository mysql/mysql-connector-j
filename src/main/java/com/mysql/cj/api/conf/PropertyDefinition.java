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
     * Checks that valueToValidate is one of allowable values. Throws exception if that's not true.
     * 
     * @param valueToValidate
     * @param exceptionInterceptor
     */
    void validateAllowableValues(String valueToValidate, ExceptionInterceptor exceptionInterceptor);

    /**
     * Creates instance of ReadableProperty or ModifiableProperty depending on isRuntimeModifiable() result.
     * 
     * @return
     */
    RuntimeProperty<T> createRuntimeProperty();

}
