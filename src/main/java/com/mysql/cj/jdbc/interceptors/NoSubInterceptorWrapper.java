/*
  Copyright (c) 2009, 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.SQLException;
import java.util.Properties;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.jdbc.Statement;
import com.mysql.cj.api.jdbc.interceptors.StatementInterceptor;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.core.Messages;

/**
 * Wraps statement interceptors during driver startup so that they don't produce different result sets than we expect.
 */
public class NoSubInterceptorWrapper implements StatementInterceptor {

    private final StatementInterceptor underlyingInterceptor;

    public NoSubInterceptorWrapper(StatementInterceptor underlyingInterceptor) {
        if (underlyingInterceptor == null) {
            throw new RuntimeException(Messages.getString("NoSubInterceptorWrapper.0"));
        }

        this.underlyingInterceptor = underlyingInterceptor;
    }

    public void destroy() {
        this.underlyingInterceptor.destroy();
    }

    public boolean executeTopLevelOnly() {
        return this.underlyingInterceptor.executeTopLevelOnly();
    }

    public StatementInterceptor init(MysqlConnection conn, Properties props, Log log) {
        this.underlyingInterceptor.init(conn, props, log);
        return this;
    }

    public <T extends Resultset> T postProcess(String sql, Statement interceptedStatement, T originalResultSet, int warningCount, boolean noIndexUsed,
            boolean noGoodIndexUsed, Exception statementException) throws SQLException {
        this.underlyingInterceptor.postProcess(sql, interceptedStatement, originalResultSet, warningCount, noIndexUsed, noGoodIndexUsed, statementException);

        return null; // don't allow result set substitution
    }

    public <T extends Resultset> T preProcess(String sql, Statement interceptedStatement) throws SQLException {
        this.underlyingInterceptor.preProcess(sql, interceptedStatement);

        return null; // don't allow result set substitution
    }

    public StatementInterceptor getUnderlyingInterceptor() {
        return this.underlyingInterceptor;
    }
}
