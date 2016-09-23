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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/**
 * A statement is a query or state-affecting command against a database that returns a result.
 */
public interface Statement<STMT_T, RES_T> {
    /**
     * Execute the statement synchronously.
     */
    RES_T execute();

    /**
     * Execute the statement asynchronously.
     */
    CompletableFuture<RES_T> executeAsync();

    /**
     * Clear the bindings for this statement.
     */
    default STMT_T clearBindings() {
        throw new UnsupportedOperationException("This statement doesn't support bound parameters");
    }

    /**
     * Bind the named argument to the given value.
     * 
     * @param argName
     * @param value
     * @return
     */
    default STMT_T bind(String argName, Object value) {
        throw new UnsupportedOperationException("This statement doesn't support bound parameters");
    }

    /**
     * Bind the set of arguments named by the keys in the map to the associated values in the map.
     */
    @SuppressWarnings("unchecked")
    default STMT_T bind(Map<String, Object> values) {
        clearBindings();
        values.entrySet().forEach(e -> bind(e.getKey(), e.getValue()));
        return (STMT_T) this;
    }

    /**
     * Bind a list of objects numerically starting at 0.
     */
    @SuppressWarnings("unchecked")
    default STMT_T bind(List<Object> values) {
        clearBindings();
        IntStream.range(0, values.size()).forEach(i -> bind(String.valueOf(i), values.get(i)));
        return (STMT_T) this;
    }

    /**
     * Bind an array of objects numerically starting at 0.
     */
    default STMT_T bind(Object... values) {
        return bind(Arrays.asList(values));
    }
}
