/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/**
 * A statement is a query or state-affecting command against a database that returns a result.
 *
 * @param <STMT_T>
 *            statement type
 * @param <RES_T>
 *            result type
 */
public interface Statement<STMT_T, RES_T> {

    /**
     * The lock contention options for the locking modes available.
     */
    enum LockContention {
        /**
         * Default behavior. Wait until the row lock is released.
         */
        DEFAULT,
        /**
         * Do not wait to acquire row lock. Fail with an error if a requested row is locked.
         */
        NOWAIT,
        /**
         * Do not wait to acquire a row lock. Remove locked rows from the result set.
         */
        SKIP_LOCKED;
    }

    /**
     * Execute the statement synchronously.
     *
     * @return result of statement execution
     */
    RES_T execute();

    /**
     * Execute the statement asynchronously.
     *
     * @return {@link CompletableFuture} for result
     */
    CompletableFuture<RES_T> executeAsync();

    /**
     * Clear all bindings for this statement.
     *
     * @return this statement
     */
    default STMT_T clearBindings() {
        throw new UnsupportedOperationException("This statement doesn't support bound parameters");
    }

    /**
     * Bind the named argument to the given value.
     *
     * @param argName
     *            argument name
     * @param value
     *            object to bind
     * @return this statement
     */
    default STMT_T bind(String argName, Object value) {
        throw new UnsupportedOperationException("This statement doesn't support bound parameters");
    }

    /**
     * Bind the set of arguments named by the keys in the map to the associated values in the map.
     *
     * @param values
     *            the map containing key-value pairs to bind
     * @return this statement
     */
    @SuppressWarnings("unchecked")
    default STMT_T bind(Map<String, Object> values) {
        clearBindings();
        values.entrySet().forEach(e -> bind(e.getKey(), e.getValue()));
        return (STMT_T) this;
    }

    /**
     * Bind a list of objects numerically starting at 0.
     *
     * @param values
     *            list of objects to bind
     * @return this statement
     */
    @SuppressWarnings("unchecked")
    default STMT_T bind(List<Object> values) {
        clearBindings();
        IntStream.range(0, values.size()).forEach(i -> bind(String.valueOf(i), values.get(i)));
        return (STMT_T) this;
    }

    /**
     * Bind an array of objects numerically starting at 0.
     *
     * @param values
     *            one or more objects to bind
     * @return this statement
     */
    default STMT_T bind(Object... values) {
        return bind(Arrays.asList(values));
    }

}
