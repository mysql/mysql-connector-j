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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.mysql.cj.api.x.SqlResult;
import com.mysql.cj.api.x.SqlStatement;
import com.mysql.cj.core.exceptions.FeatureNotAvailableException;
import com.mysql.cj.mysqlx.ExprUtil;
import com.mysql.cj.mysqlx.protobuf.MysqlxDatatypes.Any;

public class SqlStatementImpl implements SqlStatement {
    private NodeSessionImpl session;
    private String sql;
    // we use raw protobuf objects here. don't expose them outside of here
    private List<Any> args = new ArrayList<>();

    public SqlStatementImpl(NodeSessionImpl session, String sql) {
        this.session = session;
        this.sql = sql;
    }

    public SqlResult execute() {
        return this.session.getMysqlxSession().executeSql(this.sql, /* as Object */ this.args);
    }

    public CompletableFuture<SqlResult> executeAsync() {
        return this.session.getMysqlxSession().asyncExecuteSql(this.sql, this.args);
    }

    public SqlStatement clearBindings() {
        this.args.clear();
        return this;
    }

    public SqlStatement bind(List<Object> values) {
        values.stream().map(ExprUtil::argObjectToScalarAny)
                .forEach(a -> this.args.add(a));
        return this;
    }

    public SqlStatement bind(Map<String, Object> values) {
        throw new FeatureNotAvailableException("Cannot bind named parameters for SQL statements");
    }
}
