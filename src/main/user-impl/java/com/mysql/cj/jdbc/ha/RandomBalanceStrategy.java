/*
 * Copyright (c) 2007, 2020, Oracle and/or its affiliates.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mysql.cj.Messages;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.exceptions.SQLError;

public class RandomBalanceStrategy implements BalanceStrategy {

    public RandomBalanceStrategy() {
    }

    @Override
    public ConnectionImpl pickConnection(InvocationHandler proxy, List<String> configuredHosts, Map<String, JdbcConnection> liveConnections,
            long[] responseTimes, int numRetries) throws SQLException {
        int numHosts = configuredHosts.size();

        SQLException ex = null;

        List<String> whiteList = new ArrayList<>(numHosts);
        whiteList.addAll(configuredHosts);

        Map<String, Long> blackList = ((LoadBalancedConnectionProxy) proxy).getGlobalBlacklist();

        whiteList.removeAll(blackList.keySet());

        Map<String, Integer> whiteListMap = this.getArrayIndexMap(whiteList);

        for (int attempts = 0; attempts < numRetries;) {
            int random = (int) Math.floor((Math.random() * whiteList.size()));
            if (whiteList.size() == 0) {
                throw SQLError.createSQLException(Messages.getString("RandomBalanceStrategy.0"), null);
            }

            String hostPortSpec = whiteList.get(random);

            ConnectionImpl conn = (ConnectionImpl) liveConnections.get(hostPortSpec);

            if (conn == null) {
                try {
                    conn = ((LoadBalancedConnectionProxy) proxy).createConnectionForHost(hostPortSpec);
                } catch (SQLException sqlEx) {
                    ex = sqlEx;

                    if (((LoadBalancedConnectionProxy) proxy).shouldExceptionTriggerConnectionSwitch(sqlEx)) {

                        Integer whiteListIndex = whiteListMap.get(hostPortSpec);

                        // exclude this host from being picked again
                        if (whiteListIndex != null) {
                            whiteList.remove(whiteListIndex.intValue());
                            whiteListMap = this.getArrayIndexMap(whiteList);
                        }
                        ((LoadBalancedConnectionProxy) proxy).addToGlobalBlacklist(hostPortSpec);

                        if (whiteList.size() == 0) {
                            attempts++;
                            try {
                                Thread.sleep(250);
                            } catch (InterruptedException e) {
                            }

                            // start fresh
                            whiteListMap = new HashMap<>(numHosts);
                            whiteList.addAll(configuredHosts);
                            blackList = ((LoadBalancedConnectionProxy) proxy).getGlobalBlacklist();

                            whiteList.removeAll(blackList.keySet());
                            whiteListMap = this.getArrayIndexMap(whiteList);
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

    private Map<String, Integer> getArrayIndexMap(List<String> l) {
        Map<String, Integer> m = new HashMap<>(l.size());
        for (int i = 0; i < l.size(); i++) {
            m.put(l.get(i), Integer.valueOf(i));
        }
        return m;

    }

}
