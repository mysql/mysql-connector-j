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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.mysql.cj.api.xdevapi.ModifyStatement;
import com.mysql.cj.api.xdevapi.Result;
import com.mysql.cj.api.xdevapi.UpdateType;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.x.core.MysqlxSession;
import com.mysql.cj.x.core.StatementExecuteOk;
import com.mysql.cj.x.core.XDevAPIError;

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
        StatementExecuteOk ok = this.mysqlxSession.updateDocs(this.filterParams, this.updates);
        return new UpdateResult(ok);
    }

    @Override
    public CompletableFuture<Result> executeAsync() {
        CompletableFuture<StatementExecuteOk> okF = this.mysqlxSession.asyncUpdateDocs(this.filterParams, this.updates);
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
