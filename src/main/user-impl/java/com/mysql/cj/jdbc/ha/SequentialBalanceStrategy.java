/*
 * Copyright (c) 2007, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.jdbc.ha;

import java.lang.reflect.InvocationHandler;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;

/**
 * A balancing strategy that starts at a random point, and then advances in the list (wrapping around) for each new pickConnection() call.
 * 
 * The initial point selection, and subsequent point selections are blacklist-aware.
 */
public class SequentialBalanceStrategy implements BalanceStrategy {
    private int currentHostIndex = -1;

    public SequentialBalanceStrategy() {
    }

    @Override
    public ConnectionImpl pickConnection(InvocationHandler proxy, List<String> configuredHosts, Map<String, JdbcConnection> liveConnections,
            long[] responseTimes, int numRetries) throws SQLException {
        int numHosts = configuredHosts.size();

        SQLException ex = null;

        Map<String, Long> blackList = ((LoadBalancedConnectionProxy) proxy).getGlobalBlacklist();

        for (int attempts = 0; attempts < numRetries;) {
            if (numHosts == 1) {
                this.currentHostIndex = 0; // pathological case
            } else if (this.currentHostIndex == -1) {
                int random = (int) Math.floor((Math.random() * numHosts));

                for (int i = random; i < numHosts; i++) {
                    if (!blackList.containsKey(configuredHosts.get(i))) {
                        this.currentHostIndex = i;
                        break;
                    }
                }

                if (this.currentHostIndex == -1) {
                    for (int i = 0; i < random; i++) {
                        if (!blackList.containsKey(configuredHosts.get(i))) {
                            this.currentHostIndex = i;
                            break;
                        }
                    }
                }

                if (this.currentHostIndex == -1) {
                    blackList = ((LoadBalancedConnectionProxy) proxy).getGlobalBlacklist(); // it may have changed
                    // and the proxy returns a copy

                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                    }

                    continue; // retry
                }
            } else {

                int i = this.currentHostIndex + 1;
                boolean foundGoodHost = false;

                for (; i < numHosts; i++) {
                    if (!blackList.containsKey(configuredHosts.get(i))) {
                        this.currentHostIndex = i;
                        foundGoodHost = true;
                        break;
                    }
                }

                if (!foundGoodHost) {
                    for (i = 0; i < this.currentHostIndex; i++) {
                        if (!blackList.containsKey(configuredHosts.get(i))) {
                            this.currentHostIndex = i;
                            foundGoodHost = true;
                            break;
                        }
                    }
                }

                if (!foundGoodHost) {
                    blackList = ((LoadBalancedConnectionProxy) proxy).getGlobalBlacklist(); // it may have changed
                    // and the proxy returns a copy

                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                    }

                    continue; // retry
                }
            }

            String hostPortSpec = configuredHosts.get(this.currentHostIndex);

            ConnectionImpl conn = (ConnectionImpl) liveConnections.get(hostPortSpec);

            if (conn == null) {
                try {
                    conn = ((LoadBalancedConnectionProxy) proxy).createConnectionForHost(hostPortSpec);
                } catch (SQLException sqlEx) {
                    ex = sqlEx;

                    if (((LoadBalancedConnectionProxy) proxy).shouldExceptionTriggerConnectionSwitch(sqlEx)) {

                        ((LoadBalancedConnectionProxy) proxy).addToGlobalBlacklist(hostPortSpec);

                        try {
                            Thread.sleep(250);
                        } catch (InterruptedException e) {
                        }

                        continue;
                    }
                    throw sqlEx;
                }
            }

            return conn;
        }

        if (ex != null) {
            throw ex;
        }

        return null; // we won't get here, compiler can't tell
    }

}
