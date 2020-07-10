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

package com.mysql.cj.xdevapi;

import java.util.Map;

/**
 * A client-side representation of a database table. Provides access to the table through standard INSERT/SELECT/UPDATE/DELETE statements.
 */
public interface Table extends DatabaseObject {
    /**
     * Create an insert statement using the list of all columns in the table.
     * 
     * @return {@link InsertStatement}
     */
    InsertStatement insert();

    /**
     * Create an insert statement using the given list columns.
     * 
     * @param projection
     *            one or more projection expressions
     * @return {@link InsertStatement}
     */
    InsertStatement insert(String... projection);

    /**
     * Create an insert statement using the given key/value pairs.
     * 
     * @param fieldsAndValues
     *            table name-value pairs
     * @return {@link InsertStatement}
     */
    InsertStatement insert(Map<String, Object> fieldsAndValues);

    /**
     * Create a new select statement using the given projections.
     * 
     * @param projections
     *            one or more projection expressions
     * @return {@link SelectStatement}
     */
    SelectStatement select(String... projections);

    /**
     * Create a new update statement.
     * 
     * @return {@link UpdateStatement}
     */
    UpdateStatement update();

    /**
     * Create a new delete statement.
     * 
     * @return {@link DeleteStatement}
     */
    DeleteStatement delete();

    /**
     * Query the number of rows in this table.
     * 
     * @return Number of rows in this table
     */
    long count();

    /**
     * Check if the underlying object is a view or not.
     * 
     * @return true if this Table is a View
     */
    boolean isView();
}
