/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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

public interface PropertySet {

    void addProperty(RuntimeProperty<?> prop);

    void removeProperty(String name);

    void removeProperty(PropertyKey key);

    <T> RuntimeProperty<T> getProperty(String name);

    <T> RuntimeProperty<T> getProperty(PropertyKey key);

    RuntimeProperty<Boolean> getBooleanProperty(String name);

    RuntimeProperty<Boolean> getBooleanProperty(PropertyKey key);

    RuntimeProperty<Integer> getIntegerProperty(String name);

    RuntimeProperty<Integer> getIntegerProperty(PropertyKey key);

    RuntimeProperty<Long> getLongProperty(String name);

    RuntimeProperty<Long> getLongProperty(PropertyKey key);

    RuntimeProperty<Integer> getMemorySizeProperty(String name);

    RuntimeProperty<Integer> getMemorySizeProperty(PropertyKey key);

    RuntimeProperty<String> getStringProperty(String name);

    RuntimeProperty<String> getStringProperty(PropertyKey key);

    <T extends Enum<T>> RuntimeProperty<T> getEnumProperty(String name);

    <T extends Enum<T>> RuntimeProperty<T> getEnumProperty(PropertyKey key);

    /**
     * Initializes the property set with driver properties that come from URL or passed to
     * the driver manager.
     *
     * @param props
     *            properties
     */
    void initializeProperties(Properties props);

    void postInitialization();

    Properties exposeAsProperties();

    /**
     * Reset all properties to their initial values.
     */
    void reset();

}
