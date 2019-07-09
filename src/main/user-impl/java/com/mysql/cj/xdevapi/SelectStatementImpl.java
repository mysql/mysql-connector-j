/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.protocol.x.XMessage;
import com.mysql.cj.xdevapi.FilterParams.RowLock;
import com.mysql.cj.xdevapi.FilterParams.RowLockOptions;

/**
 * {@link SelectStatement} implementation.
 */
public class SelectStatementImpl extends FilterableStatement<SelectStatement, RowResult> implements SelectStatement {
    /* package private */ SelectStatementImpl(MysqlxSession mysqlxSession, String schema, String table, String... projection) {
        super(new TableFilterParams(schema, table));
        this.mysqlxSession = mysqlxSession;
        if (projection != null && projection.length > 0) {
            this.filterParams.setFields(projection);
        }
    }

    @Override
    protected RowResult executeStatement() {
        return this.mysqlxSession.query(this.getMessageBuilder().buildFind(this.filterParams), new StreamingRowResultBuilder(this.mysqlxSession));
    }

    @Override
    protected XMessage getPrepareStatementXMessage() {
        return getMessageBuilder().buildPrepareFind(this.preparedStatementId, this.filterParams);
    }

    @Override
    protected RowResult executePreparedStatement() {
        return this.mysqlxSession.query(this.getMessageBuilder().buildPrepareExecute(this.preparedStatementId, this.filterParams),
                new StreamingRowResultBuilder(this.mysqlxSession));
    }

    public CompletableFuture<RowResult> executeAsync() {
        return this.mysqlxSession.queryAsync(getMessageBuilder().buildFind(this.filterParams), new RowResultBuilder(this.mysqlxSession));
    }

    @Override
    public SelectStatement groupBy(String... groupBy) {
        resetPrepareState();
        this.filterParams.setGrouping(groupBy);
        return this;
    }

    public SelectStatement having(String having) {
        resetPrepareState();
        this.filterParams.setGroupingCriteria(having);
        return this;
    }

    @Override
    public FilterParams getFilterParams() {
        return this.filterParams;
    }

    @Override
    public SelectStatement lockShared() {
        return lockShared(LockContention.DEFAULT);
    }

    @Override
    public SelectStatement lockShared(LockContention lockContention) {
        resetPrepareState();
        this.filterParams.setLock(RowLock.SHARED_LOCK);
        switch (lockContention) {
            case NOWAIT:
                this.filterParams.setLockOption(RowLockOptions.NOWAIT);
                break;
            case SKIP_LOCKED:
                this.filterParams.setLockOption(RowLockOptions.SKIP_LOCKED);
                break;
            case DEFAULT:
        }
        return this;
    }

    @Override
    public SelectStatement lockExclusive() {
        return lockExclusive(LockContention.DEFAULT);
    }

    @Override
    public SelectStatement lockExclusive(LockContention lockContention) {
        resetPrepareState();
        this.filterParams.setLock(RowLock.EXCLUSIVE_LOCK);
        switch (lockContention) {
            case NOWAIT:
                this.filterParams.setLockOption(RowLockOptions.NOWAIT);
                break;
            case SKIP_LOCKED:
                this.filterParams.setLockOption(RowLockOptions.SKIP_LOCKED);
                break;
            case DEFAULT:
        }
        return this;
    }
}
