/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.protocol.x.StatementExecuteOk;
import com.mysql.cj.protocol.x.XMessage;
import com.mysql.cj.protocol.x.XMessageBuilder;

public class ModifyStatementImpl extends FilterableStatement<ModifyStatement, Result> implements ModifyStatement {
    private MysqlxSession mysqlxSession;
    private List<UpdateSpec> updates = new ArrayList<>();

    /* package private */ ModifyStatementImpl(MysqlxSession mysqlxSession, String schema, String collection, String criteria) {
        super(schema, collection, false);
        if (criteria == null || criteria.trim().length() == 0) {
            throw new XDevAPIError(Messages.getString("ModifyStatement.0", new String[] { "criteria" }));
        }
        this.mysqlxSession = mysqlxSession;
        this.filterParams.setCriteria(criteria);
    }

    @Override
    public Result execute() {
        StatementExecuteOk ok = this.mysqlxSession
                .sendMessage(((XMessageBuilder) this.mysqlxSession.<XMessage> getMessageBuilder()).buildDocUpdate(this.filterParams, this.updates));
        return new UpdateResult(ok);
    }

    @Override
    public CompletableFuture<Result> executeAsync() {
        CompletableFuture<StatementExecuteOk> okF = this.mysqlxSession
                .asyncSendMessage(((XMessageBuilder) this.mysqlxSession.<XMessage> getMessageBuilder()).buildDocUpdate(this.filterParams, this.updates));
        return okF.thenApply(ok -> new UpdateResult(ok));
    }

    @Override
    public ModifyStatement set(String docPath, Object value) {
        this.updates.add(new UpdateSpec(UpdateType.ITEM_SET, docPath).setValue(value));
        return this;
    }

    @Override
    public ModifyStatement change(String docPath, Object value) {
        this.updates.add(new UpdateSpec(UpdateType.ITEM_REPLACE, docPath).setValue(value));
        return this;
    }

    @Override
    public ModifyStatement unset(String... fields) {
        this.updates.addAll(Arrays.stream(fields).map(docPath -> new UpdateSpec(UpdateType.ITEM_REMOVE, docPath)).collect(Collectors.toList()));
        return this;
    }

    @Override
    public ModifyStatement merge(String document) {
        throw new FeatureNotAvailableException("TODO: not supported in xplugin");
    }

    @Override
    public ModifyStatement patch(DbDoc document) {
        return patch(document.toString());
    }

    @Override
    public ModifyStatement patch(String document) {
        this.updates.add(new UpdateSpec(UpdateType.MERGE_PATCH, "").setValue(Expression.expr(document)));
        return this;
    }

    @Override
    public ModifyStatement arrayInsert(String field, Object value) {
        this.updates.add(new UpdateSpec(UpdateType.ARRAY_INSERT, field).setValue(value));
        return this;
    }

    @Override
    public ModifyStatement arrayAppend(String docPath, Object value) {
        this.updates.add(new UpdateSpec(UpdateType.ARRAY_APPEND, docPath).setValue(value));
        return this;
    }

    @Override
    public ModifyStatement arrayDelete(String field, int position) {
        throw new FeatureNotAvailableException("TODO: not supported in xplugin");
    }
}
