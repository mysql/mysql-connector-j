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

package com.mysql.cj.mysqlx.devapi;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.mysql.cj.api.x.Result;
import com.mysql.cj.api.x.UpdateStatement;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.mysqlx.UpdateParams;

public class UpdateStatementImpl extends FilterableStatement<UpdateStatement, Result> implements UpdateStatement {
    private TableImpl table;
    private UpdateParams updateParams = new UpdateParams();

    /* package private */ UpdateStatementImpl(TableImpl table) {
        super(table.getSchema().getName(), table.getName(), true);
        this.table = table;
    }

    public Result execute() {
        StatementExecuteOk ok = this.table.getSession().getMysqlxSession().updateRows(this.filterParams, this.updateParams);
        return new UpdateResult(ok, null);
    }

    public CompletableFuture<Result> executeAsync() {
        CompletableFuture<StatementExecuteOk> okF = this.table.getSession().getMysqlxSession().asyncUpdateRows(this.filterParams, this.updateParams);
        return okF.thenApply(ok -> new UpdateResult(ok, null));
    }

    public UpdateStatement set(Map<String, Object> fieldsAndValues) {
        this.updateParams.setUpdates(fieldsAndValues);
        return this;
    }

    public UpdateStatement set(String field, Object value) {
        this.updateParams.addUpdate(field, value);
        return this;
    }
}
