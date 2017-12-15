/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.api.xdevapi;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * A statement that results in a data set of elements as it's result. This interface adds a row-wise callback for async execution.
 */
public interface DataStatement<STMT_T, RES_T, RES_ELEMENT_T> extends Statement<STMT_T, RES_T> {
    /**
     * A function that takes the accumulator and a data element and returns the new value of the accumulator.
     *
     * @param <RES_ELEMENT_T>
     *            The type of each result element
     * @param <R>
     *            The accumulator type.
     */
    public static interface Reducer<RES_ELEMENT_T, R> extends BiFunction<R, RES_ELEMENT_T, R> {
    }

    /**
     * Execute this statement asynchronously reducing the set of elements using the given accumulator.
     *
     * @param identity
     *            the initial element passed to the accumulating function
     * @param accumulator
     *            the function which accepts a pair (element, accumulator value) for every element in the result
     * @param <R>
     *            identity type
     * @return a future which is completed with the accumulator value after all elements have been processed
     */
    <R> CompletableFuture<R> executeAsync(R identity, Reducer<RES_ELEMENT_T, R> accumulator);
}
