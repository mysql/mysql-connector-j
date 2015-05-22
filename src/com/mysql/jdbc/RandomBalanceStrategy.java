/*
  Copyright (c) 2007, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RandomBalanceStrategy implements BalanceStrategy {

    public RandomBalanceStrategy() {
    }

    public void destroy() {
        // we don't have anything to clean up
    }

    public void init(Connection conn, Properties props) throws SQLException {
        // we don't have anything to initialize
    }

    public ConnectionImpl pickConnection(LoadBalancedConnectionProxy proxy, List<String> configuredHosts, Map<String, ConnectionImpl> liveConnections,
            long[] responseTimes, int numRetries) throws SQLException {
        int numHosts = configuredHosts.size();

        SQLException ex = null;

        List<String> whiteList = new ArrayList<String>(numHosts);
        whiteList.addAll(configuredHosts);

        Map<String, Long> blackList = proxy.getGlobalBlacklist();

        whiteList.removeAll(blackList.keySet());

        Map<String, Integer> whiteListMap = this.getArrayIndexMap(whiteList);

        for (int attempts = 0; attempts < numRetries;) {
            int random = (int) Math.floor((Math.random() * whiteList.size()));
            if (whiteList.size() == 0) {
                throw SQLError.createSQLException("No hosts configured", null);
            }

            String hostPortSpec = whiteList.get(random);

            ConnectionImpl conn = liveConnections.get(hostPortSpec);

            if (conn == null) {
                try {
                    conn = proxy.createConnectionForHost(hostPortSpec);
                } catch (SQLException sqlEx) {
                    ex = sqlEx;

                    if (proxy.shouldExceptionTriggerConnectionSwitch(sqlEx)) {

                        Integer whiteListIndex = whiteListMap.get(hostPortSpec);

                        // exclude this host from being picked again
                        if (whiteListIndex != null) {
                            whiteList.remove(whiteListIndex.intValue());
                            whiteListMap = this.getArrayIndexMap(whiteList);
                        }
                        proxy.addToGlobalBlacklist(hostPortSpec);

                        if (whiteList.size() == 0) {
                            attempts++;
                            try {
                                Thread.sleep(250);
                            } catch (InterruptedException e) {
                            }

                            // start fresh
                            whiteListMap = new HashMap<String, Integer>(numHosts);
                            whiteList.addAll(configuredHosts);
                            blackList = proxy.getGlobalBlacklist();

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
        Map<String, Integer> m = new HashMap<String, Integer>(l.size());
        for (int i = 0; i < l.size(); i++) {
            m.put(l.get(i), Integer.valueOf(i));
        }
        return m;

    }

}