/*
  Copyright (c) 2010, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.interceptors;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.Query;
import com.mysql.cj.api.interceptors.QueryInterceptor;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.core.exceptions.ExceptionFactory;

public class SessionAssociationInterceptor implements QueryInterceptor {

    protected String currentSessionKey;
    protected final static ThreadLocal<String> sessionLocal = new ThreadLocal<String>();
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

    public boolean executeTopLevelOnly() {
        return true;
    }

    @Override
    public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
        this.connection = (JdbcConnection) conn;
        return this;
    }

    @Override
    public <T extends Resultset> T postProcess(String sql, Query interceptedQuery, T originalResultSet, ServerSession serverSession) {
        return null;
    }

    public <T extends Resultset> T preProcess(String sql, Query interceptedQuery) {
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

    public void destroy() {
        this.connection = null;
    }
}