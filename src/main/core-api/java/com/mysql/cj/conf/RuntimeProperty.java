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

import java.util.Properties;

import javax.naming.Reference;

import com.mysql.cj.exceptions.ExceptionInterceptor;

public interface RuntimeProperty<T> {

    PropertyDefinition<T> getPropertyDefinition();

    /**
     * Explicitly set value of this RuntimeProperty according to the self-titled property value contained in extractFrom.
     * This method is called during PropertySet initialization thus ignores the RUNTIME_NOT_MODIFIABLE flag.
     * <p>
     * This value will also be the initial one, i.e. {@link #resetValue()} will reset to this value, not the default one.
     * <p>
     * If extractFrom does not contain such property then this RuntimeProperty remains unchanged.
     * 
     * @param extractFrom
     *            {@link Properties} object containing key-value pairs usually passed from connection string.
     * @param exceptionInterceptor
     *            exceptionInterceptor
     */
    void initializeFrom(Properties extractFrom, ExceptionInterceptor exceptionInterceptor);

    void initializeFrom(Reference ref, ExceptionInterceptor exceptionInterceptor);

    /**
     * Reset to initial value (default or defined in connection string/Properties)
     */
    void resetValue();

    boolean isExplicitlySet();

    /**
     * Add listener for this property changes.
     * 
     * @param l
     *            {@link RuntimePropertyListener}
     */
    void addListener(RuntimePropertyListener l);

    void removeListener(RuntimePropertyListener l);

    @FunctionalInterface
    public static interface RuntimePropertyListener {
        void handlePropertyChange(RuntimeProperty<?> prop);
    }

    /**
     * Get internal value representation as Object.
     * 
     * @return value
     */
    T getValue();

    /**
     * Get initial value (default or defined in connection string/Properties)
     * 
     * @return value
     */
    T getInitialValue();

    /**
     * Get internal value representation as String.
     * 
     * @return value
     */
    String getStringValue();

    /**
     * Set the object value of a property directly. Validation against allowable values will be performed.
     * 
     * @param value
     *            value
     */
    void setValue(T value);

    /**
     * Set the object value of a property directly. Validation against allowable values will be performed.
     * 
     * @param value
     *            value
     * @param exceptionInterceptor
     *            exception interceptor
     */
    void setValue(T value, ExceptionInterceptor exceptionInterceptor);

}
