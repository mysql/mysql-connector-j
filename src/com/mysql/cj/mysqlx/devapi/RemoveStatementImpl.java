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

import java.util.concurrent.Future;

import com.mysql.cj.api.x.CollectionStatement.RemoveStatement;
import com.mysql.cj.api.x.Result;
import com.mysql.cj.core.io.StatementExecuteOk;
import com.mysql.cj.mysqlx.FilterParams;

public class RemoveStatementImpl implements RemoveStatement {
    private SessionImpl session;
    private CollectionImpl collection;
    private FilterParams filterParams = new FilterParams();

    public RemoveStatementImpl(SessionImpl session, CollectionImpl collection, String criteria) {
        this.session = session;
        this.collection = collection;
        if (criteria != null && criteria.length() > 0) {
            this.filterParams.setCriteria(criteria);
        }
    }

    public Result execute() {
        StatementExecuteOk ok = this.session.getMysqlxSession().deleteDocs(this.collection.getSchema().getName(), this.collection.getName(), filterParams);
        return new UpdateResult(ok, null);
    }

    public Future<Result> executeAsync() {
        throw new NullPointerException("TODO: ");
    }

    public RemoveStatement orderBy(String sortFields) {
        this.filterParams.setOrder(sortFields);
        return this;
    }

    public RemoveStatement limit(long numberOfRows) {
        this.filterParams.setLimit(numberOfRows);
        return this;
    }
}
