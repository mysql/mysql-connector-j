/*
 * Copyright (c) 2015, 2022, Oracle and/or its affiliates.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlxSession;
import com.mysql.cj.protocol.x.XMessage;
import com.mysql.cj.protocol.x.XMessageBuilder;

/**
 * {@link ModifyStatement} implementation.
 */
public class ModifyStatementImpl extends FilterableStatement<ModifyStatement, Result> implements ModifyStatement {
    private List<UpdateSpec> updates = new ArrayList<>();

    /* package private */ ModifyStatementImpl(MysqlxSession mysqlxSession, String schema, String collection, String criteria) {
        super(new DocFilterParams(schema, collection, false));
        this.mysqlxSession = mysqlxSession;
        if (criteria == null || criteria.trim().isEmpty()) {
            throw new XDevAPIError(Messages.getString("ModifyStatement.0", new String[] { "criteria" }));
        }
        this.filterParams.setCriteria(criteria);
        if (!this.mysqlxSession.supportsPreparedStatements()) {
            this.preparedState = PreparedState.UNSUPPORTED;
        }
    }

    @Override
    protected Result executeStatement() {
        return this.mysqlxSession.query(getMessageBuilder().buildDocUpdate(this.filterParams, this.updates), new UpdateResultBuilder<>());
    }

    @Override
    protected XMessage getPrepareStatementXMessage() {
        return getMessageBuilder().buildPrepareDocUpdate(this.preparedStatementId, this.filterParams, this.updates);
    }

    @Override
    protected Result executePreparedStatement() {
        return this.mysqlxSession.query(getMessageBuilder().buildPrepareExecute(this.preparedStatementId, this.filterParams), new UpdateResultBuilder<>());
    }

    @Override
    public CompletableFuture<Result> executeAsync() {
        return this.mysqlxSession.queryAsync(
                ((XMessageBuilder) this.mysqlxSession.<XMessage>getMessageBuilder()).buildDocUpdate(this.filterParams, this.updates),
                new UpdateResultBuilder<>());
    }

    @Override
    public ModifyStatement set(String docPath, Object value) {
        resetPrepareState();
        this.updates.add(new UpdateSpec(UpdateType.ITEM_SET, docPath).setValue(value));
        return this;
    }

    @Override
    public ModifyStatement change(String docPath, Object value) {
        resetPrepareState();
        this.updates.add(new UpdateSpec(UpdateType.ITEM_REPLACE, docPath).setValue(value));
        return this;
    }

    @Override
    public ModifyStatement unset(String... docPath) {
        resetPrepareState();
        if (docPath == null) {
            throw new XDevAPIError(Messages.getString("ModifyStatement.0", new String[] { "docPath" }));
        }
        this.updates.addAll(Arrays.stream(docPath).map(dp -> new UpdateSpec(UpdateType.ITEM_REMOVE, dp)).collect(Collectors.toList()));
        return this;
    }

    @Override
    public ModifyStatement patch(DbDoc document) {
        resetPrepareState();
        return patch(document.toString());
    }

    @Override
    public ModifyStatement patch(String document) {
        resetPrepareState();
        this.updates.add(new UpdateSpec(UpdateType.MERGE_PATCH).setValue(Expression.expr(document)));
        return this;
    }

    @Override
    public ModifyStatement arrayInsert(String docPath, Object value) {
        resetPrepareState();
        this.updates.add(new UpdateSpec(UpdateType.ARRAY_INSERT, docPath).setValue(value));
        return this;
    }

    @Override
    public ModifyStatement arrayAppend(String docPath, Object value) {
        resetPrepareState();
        this.updates.add(new UpdateSpec(UpdateType.ARRAY_APPEND, docPath).setValue(value));
        return this;
    }

    /**
     * @deprecated Deprecated in Connector/J 8.0.17. Please use filter criteria in the operation starting method.
     */
    @Deprecated
    @Override
    public ModifyStatement where(String searchCondition) {
        return super.where(searchCondition);
    }
}
