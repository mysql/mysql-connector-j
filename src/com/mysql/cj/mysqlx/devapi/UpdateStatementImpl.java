/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import com.mysql.cj.api.x.Result;
import com.mysql.cj.api.x.TableStatement.UpdateStatement;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.mysqlx.FilterParams;
import com.mysql.cj.mysqlx.UpdateParams;

public class UpdateStatementImpl implements UpdateStatement {
    private SessionImpl session;
    private TableImpl table;
    private FilterParams filterParams = new FilterParams();
    private UpdateParams updateParams = new UpdateParams();

    /* package private */ UpdateStatementImpl(SessionImpl session, TableImpl table) {
        this.session = session;
        this.table = table;
    }

    public Result execute() {
        StatementExecuteOk ok = this.session.getMysqlxSession().updateRows(table.getSchema().getName(), table.getName(), this.filterParams, this.updateParams);
        return new UpdateResult(ok, null);
    }

    public UpdateStatement set(String fieldsAndValues) {
        this.updateParams.setUpdates(fieldsAndValues);
        return this;
    }

    public UpdateStatement where(String searchCondition) {
        this.filterParams.setCriteria(searchCondition);
        return this;
    }

    public UpdateStatement orderBy(String sortFields) {
        this.filterParams.setOrder(sortFields);
        return this;
    }

    public UpdateStatement limit(long numberOfRows) {
        this.filterParams.setLimit(numberOfRows);
        return this;
    }
}
