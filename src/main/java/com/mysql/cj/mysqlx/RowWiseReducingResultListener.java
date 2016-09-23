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

package com.mysql.cj.mysqlx;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.mysql.cj.api.x.DataStatement.Reducer;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.mysqlx.io.ResultListener;
import com.mysql.cj.mysqlx.result.MysqlxRow;
import com.mysql.cj.mysqlx.result.RowToElement;

/**
 * Reduce over the rows.
 */
public class RowWiseReducingResultListener<RES_ELEMENT_T, R> implements ResultListener {
    private Reducer<RES_ELEMENT_T, R> reducer;
    private CompletableFuture<R> future;
    private R accumulator;
    private MetadataToRowToElement<RES_ELEMENT_T> metadataToRowToElement;
    private RowToElement<RES_ELEMENT_T> rowToElement;

    public static interface MetadataToRowToElement<T> extends Function<ArrayList<Field>, RowToElement<T>> {
    }

    public RowWiseReducingResultListener(R accumulator, Reducer<RES_ELEMENT_T, R> reducer, CompletableFuture<R> future,
            MetadataToRowToElement<RES_ELEMENT_T> metadataToRowToElement) {
        this.accumulator = accumulator;
        this.reducer = reducer;
        this.future = future;
        this.metadataToRowToElement = metadataToRowToElement;
    }

    public void onMetadata(ArrayList<Field> metadata) {
        this.rowToElement = this.metadataToRowToElement.apply(metadata);
    }

    public void onRow(MysqlxRow r) {
        this.accumulator = this.reducer.apply(this.accumulator, this.rowToElement.apply(r));
    }

    public void onComplete(StatementExecuteOk ok) {
        this.future.complete(this.accumulator);
    }

    public void onError(MysqlxError error) {
        this.future.completeExceptionally(error);
    }

    public void onException(Throwable t) {
        this.future.completeExceptionally(t);
    }
}
