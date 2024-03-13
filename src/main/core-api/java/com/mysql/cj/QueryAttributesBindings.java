/*
 * Copyright (c) 2021, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj;

import java.sql.Statement;
import java.util.function.Consumer;

/**
 * Instances of this interface keep the list of query attributes assigned to a {@link Statement} object.
 */
public interface QueryAttributesBindings {

    /**
     * Adds a new query attribute to the list of query attributes. Implementations must validate the type of the given the object and reject it or replace it by
     * another representation if not supported, by its String version, for example. Query attribute names are not checked for duplication.
     *
     * @param name
     *            the query attribute name.
     * @param value
     *            the query attribute value.
     */
    void setAttribute(String name, Object value);

    /**
     * Removes the specified query attribute from the list of query attributes.
     *
     * @param name
     *            the query attribute name.
     */
    void removeAttribute(String name);

    /**
     * Get the count of query attributes in the list.
     *
     * @return
     *         the number of query attributes existing in the list.
     */
    int getCount();

    /**
     * Returns an internal representation of the query attribute in the given position of the query attributes list. It's implementation dependent what to do
     * when the index value is invalid.
     *
     * @param index
     *            the position of the query attribute value to return.
     *
     * @return
     *         the {@link BindValue} in the given position of the query attributes list.
     */
    BindValue getAttributeValue(int index);

    /**
     * Runs through all query attributes while feeding the given {@link Consumer} with each one of them.
     *
     * @param bindAttribute
     *            A {@link Consumer} for each one of the single query attributes.
     */
    void runThroughAll(Consumer<BindValue> bindAttribute);

    /**
     * Checks if there is already an attribute with the specified name.
     *
     * @param name
     *            the query attribute name.
     * @return
     *         <code>true</code> if the specified attribute name already exists.
     */
    boolean containsAttribute(String name);

    /**
     * Removes all query attributes from the query attributes list.
     */
    void clearAttributes();

}
