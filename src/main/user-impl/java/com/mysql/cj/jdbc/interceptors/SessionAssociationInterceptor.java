/*
 * Copyright (c) 2010, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc.interceptors;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.Query;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerSession;

public class SessionAssociationInterceptor implements QueryInterceptor {

    protected String currentSessionKey;
    protected final static ThreadLocal<String> sessionLocal = new ThreadLocal<>();
    private JdbcConnection connection;

    public static final void setSessionKey(String key) {
        sessionLocal.set(key);
    }

    public static final void resetSessionKey() {
        sessionLocal.set(null);
    }

    public static final String getSessionKey() {
        return sessionLocal.get();
    }

    @Override
    public boolean executeTopLevelOnly() {
        return true;
    }

    @Override
    public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
        this.connection = (JdbcConnection) conn;
        return this;
    }

    @Override
    public <T extends Resultset> T postProcess(Supplier<String> sql, Query interceptedQuery, T originalResultSet, ServerSession serverSession) {
        return null;
    }

    @Override
    public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
        String key = getSessionKey();

        if (key != null && !key.equals(this.currentSessionKey)) {

            try {
                PreparedStatement pstmt = this.connection.clientPrepareStatement("SET @mysql_proxy_session=?");

                try {
                    pstmt.setString(1, key);
                    pstmt.execute();
                } finally {
                    pstmt.close();
                }
            } catch (SQLException ex) {
                throw ExceptionFactory.createException(ex.getMessage(), ex);
            }

            this.currentSessionKey = key;
        }

        return null;
    }

    @Override
    public void destroy() {
        this.connection = null;
    }
}
