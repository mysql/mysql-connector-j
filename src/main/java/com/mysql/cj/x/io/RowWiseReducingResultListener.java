/*
 * Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.x.io;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import com.mysql.cj.api.x.core.RowToElement;
import com.mysql.cj.api.x.io.MetadataToRowToElement;
import com.mysql.cj.api.x.io.ResultListener;
import com.mysql.cj.api.xdevapi.DataStatement.Reducer;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.x.core.StatementExecuteOk;
import com.mysql.cj.x.core.XDevAPIError;

/**
 * Reduce over the rows.
 */
public class RowWiseReducingResultListener<RES_ELEMENT_T, R> implements ResultListener {
    private Reducer<RES_ELEMENT_T, R> reducer;
    private CompletableFuture<R> future;
    private R accumulator;
    private MetadataToRowToElement<RES_ELEMENT_T> metadataToRowToElement;
    private RowToElement<RES_ELEMENT_T> rowToElement;

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

    public void onRow(XProtocolRow r) {
        this.accumulator = this.reducer.apply(this.accumulator, this.rowToElement.apply(r));
    }

    public void onComplete(StatementExecuteOk ok) {
        this.future.complete(this.accumulator);
    }

    public void onError(XDevAPIError error) {
        this.future.completeExceptionally(error);
    }

    public void onException(Throwable t) {
        this.future.completeExceptionally(t);
    }
}
