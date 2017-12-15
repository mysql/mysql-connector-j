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

package com.mysql.cj.xdevapi;

import java.util.concurrent.CompletableFuture;

import com.mysql.cj.api.xdevapi.Row;
import com.mysql.cj.api.xdevapi.RowResult;
import com.mysql.cj.api.xdevapi.SelectStatement;
import com.mysql.cj.x.core.MysqlxSession;
import com.mysql.cj.x.protobuf.MysqlxCrud.Find.RowLock;

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

    @Override
    public SelectStatement lockShared() {
        this.findParams.setLock(RowLock.SHARED_LOCK);
        return this;
    }

    @Override
    public SelectStatement lockExclusive() {
        this.findParams.setLock(RowLock.EXCLUSIVE_LOCK);
        return this;
    }
}
