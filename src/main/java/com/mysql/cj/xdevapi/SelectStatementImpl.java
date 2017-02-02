/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.xdevapi;

import java.util.concurrent.CompletableFuture;

import com.mysql.cj.api.xdevapi.Row;
import com.mysql.cj.api.xdevapi.RowResult;
import com.mysql.cj.api.xdevapi.SelectStatement;
import com.mysql.cj.x.core.MysqlxSession;

public class SelectStatementImpl extends FilterableStatement<SelectStatement, RowResult> implements SelectStatement {
    private MysqlxSession mysqlxSession;
    private FindParams findParams;

    /* package private */ SelectStatementImpl(MysqlxSession mysqlxSession, String schema, String table, String projection) {
        super(new TableFindParams(schema, table));
        this.findParams = (TableFindParams) this.filterParams;
        this.mysqlxSession = mysqlxSession;
        if (projection != null && projection.length() > 0) {
            this.findParams.setFields(projection);
        }
    }

    /* package private */ SelectStatementImpl(MysqlxSession mysqlxSession, String schema, String table, String... projection) {
        super(new TableFindParams(schema, table));
        this.findParams = (TableFindParams) this.filterParams;
        this.mysqlxSession = mysqlxSession;
        if (projection != null && projection.length > 0) {
            this.findParams.setFields(projection);
        }
    }

    public RowResultImpl execute() {
        return this.mysqlxSession.selectRows(this.findParams);
    }

    public CompletableFuture<RowResult> executeAsync() {
        return this.mysqlxSession.asyncSelectRows(this.findParams);
    }

    public <R> CompletableFuture<R> executeAsync(R id, Reducer<Row, R> reducer) {
        return this.mysqlxSession.asyncSelectRowsReduce(this.findParams, id, reducer);
    }

    @Override
    public SelectStatement groupBy(String... groupBy) {
        this.findParams.setGrouping(groupBy);
        return this;
    }

    public SelectStatement having(String having) {
        this.findParams.setGroupingCriteria(having);
        return this;
    }

    @Override
    public FindParams getFindParams() {
        return this.findParams;
    }
}
