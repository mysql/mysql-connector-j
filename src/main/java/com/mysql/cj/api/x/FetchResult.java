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

import java.util.Iterator;
import java.util.List;

/**
 * A set of elements from a query command.
 *
 * @param <T>
 *            the type of element returned from the query (doc or row)
 */
public interface FetchResult<T> extends Iterator<T>, Iterable<T> {
    /**
     * Does this result have data? This indicates that the result was produced from a data-returning query. It does not indicate whether there are more than 0
     * rows in the result.
     */
    default boolean hasData() {
        return true;
    }

    /**
     * Fetch the next element.
     */
    default T fetchOne() {
        if (hasNext()) {
            return next();
        }
        return null;
    }

    /**
     * Create an iterator over all elements of the result.
     */
    default Iterator<T> iterator() {
        return fetchAll().iterator();
    }

    /**
     * How many items are in this result? This method forces internal buffering of the entire result.
     */
    long count();

    /**
     * Create a list of all elements in the result forcing internal buffering.
     */
    List<T> fetchAll();

    /**
     * Count of warnings generated during statement execution. This method forces internal buffering of the result.
     */
    int getWarningsCount();

    /**
     * Warnings generated during statement execution. This method forces internal buffering of the result.
     */
    Iterator<Warning> getWarnings();
}
