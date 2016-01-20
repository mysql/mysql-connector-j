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
     * @return a future which is completed with the accumulator value after all elements have been processed
     */
    <R> CompletableFuture<R> executeAsync(R identity, Reducer<RES_ELEMENT_T, R> accumulator);
}
