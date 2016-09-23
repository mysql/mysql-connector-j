/*
  Copyright (c) 2010, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.ha;

import java.sql.SQLException;
import java.util.Properties;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.Statement;
import com.mysql.cj.api.jdbc.interceptors.StatementInterceptor;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.core.conf.PropertyDefinitions;

public class LoadBalancedAutoCommitInterceptor implements StatementInterceptor {
    private int matchingAfterStatementCount = 0;
    private int matchingAfterStatementThreshold = 0;
    private String matchingAfterStatementRegex;
    private JdbcConnection conn;
    private LoadBalancedConnectionProxy proxy = null;

    public void destroy() {
        // do nothing here
    }

    public boolean executeTopLevelOnly() {
        // always return false
        return false;
    }

    public StatementInterceptor init(MysqlConnection connection, Properties props, Log log) {
        this.conn = (JdbcConnection) connection;

        String autoCommitSwapThresholdAsString = props.getProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementThreshold, "0");
        try {
            this.matchingAfterStatementThreshold = Integer.parseInt(autoCommitSwapThresholdAsString);
        } catch (NumberFormatException nfe) {
            // nothing here, being handled in LoadBalancedConnectionProxy.
        }
        String autoCommitSwapRegex = props.getProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementRegex, "");
        if (!"".equals(autoCommitSwapRegex)) {
            this.matchingAfterStatementRegex = autoCommitSwapRegex;
        }
        return this;

    }

    @SuppressWarnings("resource")
    public <T extends Resultset> T postProcess(String sql, Statement interceptedStatement, T originalResultSet, int warningCount, boolean noIndexUsed,
            boolean noGoodIndexUsed, Exception statementException) throws SQLException {

        // don't care if auto-commit is not enabled
        if (!this.conn.getAutoCommit()) {
            this.matchingAfterStatementCount = 0;
            // auto-commit is enabled:
        } else {

            if (this.proxy == null && this.conn.isProxySet()) {
                JdbcConnection lcl_proxy = this.conn.getMultiHostSafeProxy();
                while (lcl_proxy != null && !(lcl_proxy instanceof LoadBalancedMySQLConnection)) {
                    lcl_proxy = lcl_proxy.getMultiHostSafeProxy();
                }
                if (lcl_proxy != null) {
                    this.proxy = ((LoadBalancedMySQLConnection) lcl_proxy).getThisAsProxy();
                }

            }

            if (this.proxy != null) {
                // increment the match count if no regex specified, or if matches:
                if (this.matchingAfterStatementRegex == null || sql.matches(this.matchingAfterStatementRegex)) {
                    this.matchingAfterStatementCount++;
                }
            }
            // trigger rebalance if count exceeds threshold:
            if (this.matchingAfterStatementCount >= this.matchingAfterStatementThreshold) {
                this.matchingAfterStatementCount = 0;
                try {
                    if (this.proxy != null) {
                        this.proxy.pickNewConnection();
                    }

                } catch (SQLException e) {
                    // eat this exception, the auto-commit statement completed, but we could not rebalance for some reason.  User may get exception when using
                    // connection next.
                }
            }
        }
        // always return the original result set.
        return originalResultSet;
    }

    public <T extends Resultset> T preProcess(String sql, Statement interceptedStatement) throws SQLException {
        // we do nothing before execution, it's unsafe to swap servers at this point.
        return null;
    }

}
