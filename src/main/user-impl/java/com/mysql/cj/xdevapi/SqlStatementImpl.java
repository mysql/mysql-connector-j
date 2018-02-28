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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.mysql.cj.MysqlxSession;
import com.mysql.cj.exceptions.FeatureNotAvailableException;

public class SqlStatementImpl implements SqlStatement {
    private MysqlxSession mysqlxSession;
    private String sql;
    private List<Object> args = new ArrayList<>();

    public SqlStatementImpl(MysqlxSession mysqlxSession, String sql) {
        this.mysqlxSession = mysqlxSession;
        this.sql = sql;
    }

    public SqlResult execute() {
        return this.mysqlxSession.executeSql(this.sql, this.args);
    }

    public CompletableFuture<SqlResult> executeAsync() {
        return this.mysqlxSession.asyncExecuteSql(this.sql, this.args);
    }

    public SqlStatement clearBindings() {
        this.args.clear();
        return this;
    }

    public SqlStatement bind(List<Object> values) {
        this.args.addAll(values);
        return this;
    }

    public SqlStatement bind(Map<String, Object> values) {
        throw new FeatureNotAvailableException("Cannot bind named parameters for SQL statements");
    }
}
