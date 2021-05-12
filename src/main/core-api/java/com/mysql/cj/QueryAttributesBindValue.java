/*
 * Copyright (c) 2021, Oracle and/or its affiliates.
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

package com.mysql.cj;

/**
 * An internal representation of a query attribute bind value.
 * 
 */
public interface QueryAttributesBindValue {
    /**
     * Checks whether this query attribute is the <code>null</code> value.
     * 
     * @return
     *         <code>true</code> if this query attribute value is <code>null</code>.
     */
    boolean isNull();

    /**
     * Gets the name of this query attribute.
     * 
     * @return
     *         the name of this query attribute.
     */
    String getName();

    /**
     * Gets the type of this query attribute. Query attributes types are one of the {@link MysqlType}.FIELD_TYPE_*.
     * 
     * @return
     *         the type of this query attribute.
     */
    int getType();

    /**
     * Gets the value of this query attribute.
     * 
     * @return
     *         the value of this query attribute.
     */
    Object getValue();

    /**
     * Gets the length of this query attribute.
     * 
     * @return
     *         the expected length, in Bytes, of this query attribute value after being encoded.
     */
    long getBoundLength();
}