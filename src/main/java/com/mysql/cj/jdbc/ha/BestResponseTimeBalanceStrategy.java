/*
 * Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.
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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.mysql.cj.api.jdbc.ha.BalanceStrategy;
import com.mysql.cj.jdbc.ConnectionImpl;

public class BestResponseTimeBalanceStrategy implements BalanceStrategy {

    public BestResponseTimeBalanceStrategy() {
    }

    public ConnectionImpl pickConnection(LoadBalancedConnectionProxy proxy, List<String> configuredHosts, Map<String, ConnectionImpl> liveConnections,
            long[] responseTimes, int numRetries) throws SQLException {

        Map<String, Long> blackList = proxy.getGlobalBlacklist();

        SQLException ex = null;

        for (int attempts = 0; attempts < numRetries;) {
            long minResponseTime = Long.MAX_VALUE;

            int bestHostIndex = 0;

            // safety
            if (blackList.size() == configuredHosts.size()) {
                blackList = proxy.getGlobalBlacklist();
            }

            for (int i = 0; i < responseTimes.length; i++) {
                long candidateResponseTime = responseTimes[i];

                if (candidateResponseTime < minResponseTime && !blackList.containsKey(configuredHosts.get(i))) {
                    if (candidateResponseTime == 0) {
                        bestHostIndex = i;

                        break;
                    }

                    bestHostIndex = i;
                    minResponseTime = candidateResponseTime;
                }
            }

            String bestHost = configuredHosts.get(bestHostIndex);

            ConnectionImpl conn = liveConnections.get(bestHost);

            if (conn == null) {
                try {
                    conn = proxy.createConnectionForHost(bestHost);
                } catch (SQLException sqlEx) {
                    ex = sqlEx;

                    if (proxy.shouldExceptionTriggerConnectionSwitch(sqlEx)) {
                        proxy.addToGlobalBlacklist(bestHost);
                        blackList.put(bestHost, null);

                        if (blackList.size() == configuredHosts.size()) {
                            attempts++;
                            try {
                                Thread.sleep(250);
                            } catch (InterruptedException e) {
                            }
                            blackList = proxy.getGlobalBlacklist(); // try again after a little bit
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
