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
 * {@link FindStatement} implementation.
 */
public class FindStatementImpl extends FilterableStatement<FindStatement, DocResult> implements FindStatement {
    /* package private */ FindStatementImpl(MysqlxSession mysqlxSession, String schema, String collection, String criteria) {
        super(new DocFilterParams(schema, collection));
        this.mysqlxSession = mysqlxSession;
        if (criteria != null && criteria.length() > 0) {
            this.filterParams.setCriteria(criteria);
        }
        if (!this.mysqlxSession.supportsPreparedStatements()) {
            this.preparedState = PreparedState.UNSUPPORTED;
        }
    }

    @Override
    protected DocResultImpl executeStatement() {
        return this.mysqlxSession.find(this.filterParams, metadata -> (rows, task) -> new DocResultImpl(rows, task, this.mysqlxSession.getPropertySet()));
    }

    @Override
    protected XMessage getPrepareStatementXMessage() {
        return getMessageBuilder().buildPrepareFind(this.preparedStatementId, this.filterParams);
    }

    @Override
    protected DocResultImpl executePreparedStatement() {
        return this.mysqlxSession.executePreparedFind(this.preparedStatementId, this.filterParams,
                metadata -> (rows, task) -> new DocResultImpl(rows, task, this.mysqlxSession.getPropertySet()));
    }

    public CompletableFuture<DocResult> executeAsync() {
        return this.mysqlxSession.asyncFind(this.filterParams, metadata -> (rows, task) -> new DocResultImpl(rows, task, this.mysqlxSession.getPropertySet()));
    }

    @Override
    public FindStatement fields(String... projection) {
        resetPrepareState();
        this.filterParams.setFields(projection);
        return this;
    }

    public FindStatement fields(Expression docProjection) {
        resetPrepareState();
        ((DocFilterParams) this.filterParams).setFields(docProjection);
        return this;
    }

    @Override
    public FindStatement groupBy(String... groupBy) {
        resetPrepareState();
        this.filterParams.setGrouping(groupBy);
        return this;
    }

    public FindStatement having(String having) {
        resetPrepareState();
        this.filterParams.setGroupingCriteria(having);
        return this;
    }

    @Override
    public FindStatement lockShared() {
        return lockShared(LockContention.DEFAULT);
    }

    @Override
    public FindStatement lockShared(LockContention lockContention) {
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
    public FindStatement lockExclusive() {
        return lockExclusive(LockContention.DEFAULT);
    }

    @Override
    public FindStatement lockExclusive(LockContention lockContention) {
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
