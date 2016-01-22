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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.mysql.cj.api.result.Row;
import com.mysql.cj.api.result.RowList;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.core.result.BufferedRowList;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.mysqlx.MysqlxSession.ResultCtor;
import com.mysql.cj.mysqlx.io.ResultListener;
import com.mysql.cj.mysqlx.result.MysqlxRow;

/**
 * Create an entire (buffered) result from the data fed to this result listener.
 *
 * @param <RES_T>
 *            The type of result that will be created (and posted to the future)
 */
public class ResultCreatingResultListener<RES_T> implements ResultListener {
    private ArrayList<Field> metadata;
    private List<Row> rows = new ArrayList<>();
    private ResultCtor<? extends RES_T> resultCtor;
    private CompletableFuture<RES_T> future;

    public ResultCreatingResultListener(ResultCtor<? extends RES_T> resultCtor, CompletableFuture<RES_T> future) {
        this.resultCtor = resultCtor;
        this.future = future;
    }

    public void onMetadata(ArrayList<Field> metadataFields) {
        this.metadata = metadataFields;
    }

    public void onRow(MysqlxRow r) {
        this.rows.add(r);
    }

    public void onComplete(StatementExecuteOk ok) {
        RowList rowList = new BufferedRowList(this.rows);
        RES_T result = this.resultCtor.apply(this.metadata).apply(rowList, () -> ok);
        this.future.complete(result);
    }

    public void onError(MysqlxError error) {
        this.future.completeExceptionally(error);
    }

    public void onException(Throwable t) {
        this.future.completeExceptionally(t);
    }
}
