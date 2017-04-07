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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.mysql.cj.api.xdevapi.InsertResult;
import com.mysql.cj.api.xdevapi.InsertStatement;
import com.mysql.cj.x.core.MysqlxSession;
import com.mysql.cj.x.core.StatementExecuteOk;

public class InsertStatementImpl implements InsertStatement {
    private MysqlxSession mysqlxSession;
    private String schemaName;
    private String tableName;
    private InsertParams insertParams = new InsertParams();

    /* package private */ InsertStatementImpl(MysqlxSession mysqlxSession, String schema, String table, String[] fields) {
        this.mysqlxSession = mysqlxSession;
        this.schemaName = schema;
        this.tableName = table;
        this.insertParams.setProjection(fields);
    }

    /* package private */ InsertStatementImpl(MysqlxSession mysqlxSession, String schema, String table, Map<String, Object> fieldsAndValues) {
        this.mysqlxSession = mysqlxSession;
        this.schemaName = schema;
        this.tableName = table;
        this.insertParams.setFieldsAndValues(fieldsAndValues);
    }

    public InsertResult execute() {
        StatementExecuteOk ok = this.mysqlxSession.insertRows(this.schemaName, this.tableName, this.insertParams);
        return new InsertResultImpl(ok);
    }

    public CompletableFuture<InsertResult> executeAsync() {
        CompletableFuture<StatementExecuteOk> okF = this.mysqlxSession.asyncInsertRows(this.schemaName, this.tableName, this.insertParams);
        return okF.thenApply(ok -> new InsertResultImpl(ok));
    }

    public InsertStatement values(List<Object> row) {
        this.insertParams.addRow(row);
        return this;
    }
}
