/*
 * Copyright (c) 2017, 2023, Oracle and/or its affiliates.
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
import com.mysql.cj.util.StringUtils;

public class ServerAffinityStrategy extends RandomBalanceStrategy {

    public String[] affinityOrderedServers = null;

    public ServerAffinityStrategy(String affinityOrdervers) {
        super();
        if (!StringUtils.isNullOrEmpty(affinityOrdervers)) {
            this.affinityOrderedServers = affinityOrdervers.split(",");
        }
    }

    @Override
    public ConnectionImpl pickConnection(InvocationHandler proxy, List<String> configuredHosts, Map<String, JdbcConnection> liveConnections,
            long[] responseTimes, int numRetries) throws SQLException {
        if (this.affinityOrderedServers == null) {
            return super.pickConnection(proxy, configuredHosts, liveConnections, responseTimes, numRetries);
        }
        Map<String, Long> blockList = ((LoadBalancedConnectionProxy) proxy).getGlobalBlocklist();

        for (String host : this.affinityOrderedServers) {
            if (configuredHosts.contains(host) && !blockList.containsKey(host)) {
                ConnectionImpl conn = (ConnectionImpl) liveConnections.get(host);
                if (conn != null) {
                    return conn;
                }
                try {
                    conn = ((LoadBalancedConnectionProxy) proxy).createConnectionForHost(host);
                    return conn;
                } catch (SQLException sqlEx) {
                    if (((LoadBalancedConnectionProxy) proxy).shouldExceptionTriggerConnectionSwitch(sqlEx)) {
                        ((LoadBalancedConnectionProxy) proxy).addToGlobalBlocklist(host);
                    }
                }
            }
        }

        // Failed to connect to all hosts in the affinity list. Delegate to RandomBalanceStrategy.
        return super.pickConnection(proxy, configuredHosts, liveConnections, responseTimes, numRetries);
    }

}
