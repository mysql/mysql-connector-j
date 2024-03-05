/*
 * Copyright (c) 2010, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.jdbc.ha;

import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.Query;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.util.StringUtils;

public class LoadBalancedAutoCommitInterceptor implements QueryInterceptor {

    private int matchingAfterStatementCount = 0;
    private int matchingAfterStatementThreshold = 0;
    private String matchingAfterStatementRegex;
    private JdbcConnection conn;
    private LoadBalancedConnectionProxy proxy = null;

    private boolean countStatements = false;

    @Override
    public void destroy() {
        this.conn = null;
        this.proxy = null;
    }

    @Override
    public boolean executeTopLevelOnly() {
        // always return false
        return false;
    }

    @Override
    public QueryInterceptor init(MysqlConnection connection, Properties props, Log log) {
        this.conn = (JdbcConnection) connection;

        String autoCommitSwapThresholdAsString = props.getProperty(PropertyKey.loadBalanceAutoCommitStatementThreshold.getKeyName(), "0");
        try {
            this.matchingAfterStatementThreshold = Integer.parseInt(autoCommitSwapThresholdAsString);
        } catch (NumberFormatException nfe) {
            // nothing here, being handled in LoadBalancedConnectionProxy.
        }
        String autoCommitSwapRegex = props.getProperty(PropertyKey.loadBalanceAutoCommitStatementRegex.getKeyName(), "");
        if (!"".equals(autoCommitSwapRegex)) {
            this.matchingAfterStatementRegex = autoCommitSwapRegex;
        }
        return this;
    }

    @Override
    @SuppressWarnings("resource")
    public <T extends Resultset> T postProcess(Supplier<String> sql, Query interceptedQuery, T originalResultSet, ServerSession serverSession) {
        try {
            // Don't count SETs, SHOWs neither USEs. Those are mostly used internally and must not trigger a connection switch.
            if (!this.countStatements || StringUtils.startsWithIgnoreCase(sql.get(), "SET") || StringUtils.startsWithIgnoreCase(sql.get(), "SHOW")
                    || StringUtils.startsWithIgnoreCase(sql.get(), "USE")) {
                return originalResultSet;
            }

            // Don't care if auto-commit is not enabled.
            if (!this.conn.getAutoCommit()) {
                this.matchingAfterStatementCount = 0;
                return originalResultSet;
            }

            if (this.proxy == null && this.conn.isProxySet()) {
                JdbcConnection connParentProxy = this.conn.getMultiHostParentProxy();
                while (connParentProxy != null && !(connParentProxy instanceof LoadBalancedMySQLConnection)) {
                    connParentProxy = connParentProxy.getMultiHostParentProxy();
                }
                if (connParentProxy != null) {
                    this.proxy = ((LoadBalancedMySQLConnection) connParentProxy).getThisAsProxy();
                }
            }

            // Connection is not ready to rebalance yet.
            if (this.proxy == null) {
                return originalResultSet;
            }

            // Increment the match count if no regex specified, or if matches.
            if (this.matchingAfterStatementRegex == null || sql.get().matches(this.matchingAfterStatementRegex)) {
                this.matchingAfterStatementCount++;
            }

            // Trigger rebalance if count exceeds threshold.
            if (this.matchingAfterStatementCount >= this.matchingAfterStatementThreshold) {
                this.matchingAfterStatementCount = 0;
                try {
                    this.proxy.pickNewConnection();
                } catch (SQLException e) {
                    // eat this exception, the auto-commit statement completed, but we could not rebalance for some reason.  User may get exception when using
                    // connection next.
                }
            }
        } catch (SQLException ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex);
        }
        // always return the original result set.
        return originalResultSet;
    }

    @Override
    public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
        // we do nothing before execution, it's unsafe to swap servers at this point.
        return null;
    }

    void pauseCounters() {
        this.countStatements = false;
    }

    void resumeCounters() {
        this.countStatements = true;
    }

}
