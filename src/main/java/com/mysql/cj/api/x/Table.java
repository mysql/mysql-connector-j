/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.x;

import java.util.Map;

/**
 * A client-side representation of a database table. Provides access to the table through standard INSERT/SELECT/UPDATE/DELETE statements.
 */
public interface Table extends DatabaseObject {
    /**
     * Create an insert statement using the list of all columns in the table.
     */
    InsertStatement insert();

    /**
     * Create an insert statement using the given list columns.
     */
    InsertStatement insert(String... projection);

    /**
     * Create an insert statement using the given key/value pairs.
     */
    InsertStatement insert(Map<String, Object> fieldsAndValues);

    /**
     * Create a new select statement using the given projections.
     */
    SelectStatement select(String... projections);

    /**
     * Create a new update statement.
     */
    UpdateStatement update();

    /**
     * Create a new delete statement.
     */
    DeleteStatement delete();

    /**
     * Query the number of rows in this table.
     */
    long count();

    /**
     * Check if the underlying object is a view or not.
     * 
     * @return true if this Table is a View
     */
    boolean isView();
}
