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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConnectionGroup {
    private String groupName;
    private long connections = 0;
    private long activeConnections = 0;
    private HashMap<Long, LoadBalancedConnectionProxy> connectionProxies = new HashMap<Long, LoadBalancedConnectionProxy>();
    private Set<String> hostList = new HashSet<String>();
    private boolean isInitialized = false;
    private long closedProxyTotalPhysicalConnections = 0;
    private long closedProxyTotalTransactions = 0;
    private int activeHosts = 0;
    private Set<String> closedHosts = new HashSet<String>();

    ConnectionGroup(String groupName) {
        this.groupName = groupName;
    }

    public long registerConnectionProxy(LoadBalancedConnectionProxy proxy, List<String> localHostList) {
        long currentConnectionId;

        synchronized (this) {
            if (!this.isInitialized) {
                this.hostList.addAll(localHostList);
                this.isInitialized = true;
                this.activeHosts = localHostList.size();
            }
            currentConnectionId = ++this.connections;
            this.connectionProxies.put(Long.valueOf(currentConnectionId), proxy);
        }
        this.activeConnections++;

        return currentConnectionId;
    }

    public String getGroupName() {
        return this.groupName;
    }

    public Collection<String> getInitialHosts() {
        return this.hostList;
    }

    public int getActiveHostCount() {
        return this.activeHosts;
    }

    public Collection<String> getClosedHosts() {
        return this.closedHosts;
    }

    public long getTotalLogicalConnectionCount() {
        return this.connections;
    }

    public long getActiveLogicalConnectionCount() {
        return this.activeConnections;
    }

    public long getActivePhysicalConnectionCount() {
        long result = 0;
        Map<Long, LoadBalancedConnectionProxy> proxyMap = new HashMap<Long, LoadBalancedConnectionProxy>();
        synchronized (this.connectionProxies) {
            proxyMap.putAll(this.connectionProxies);
        }
        for (LoadBalancedConnectionProxy proxy : proxyMap.values()) {
            result += proxy.getActivePhysicalConnectionCount();
        }
        return result;
    }

    public long getTotalPhysicalConnectionCount() {
        long allConnections = this.closedProxyTotalPhysicalConnections;
        Map<Long, LoadBalancedConnectionProxy> proxyMap = new HashMap<Long, LoadBalancedConnectionProxy>();
        synchronized (this.connectionProxies) {
            proxyMap.putAll(this.connectionProxies);
        }
        for (LoadBalancedConnectionProxy proxy : proxyMap.values()) {
            allConnections += proxy.getTotalPhysicalConnectionCount();
        }
        return allConnections;
    }

    public long getTotalTransactionCount() {
        // need to account for closed connection proxies
        long transactions = this.closedProxyTotalTransactions;
        Map<Long, LoadBalancedConnectionProxy> proxyMap = new HashMap<Long, LoadBalancedConnectionProxy>();
        synchronized (this.connectionProxies) {
            proxyMap.putAll(this.connectionProxies);
        }
        for (LoadBalancedConnectionProxy proxy : proxyMap.values()) {
            transactions += proxy.getTransactionCount();
        }
        return transactions;
    }

    public void closeConnectionProxy(LoadBalancedConnectionProxy proxy) {
        this.activeConnections--;
        this.connectionProxies.remove(Long.valueOf(proxy.getConnectionGroupProxyID()));
        this.closedProxyTotalPhysicalConnections += proxy.getTotalPhysicalConnectionCount();
        this.closedProxyTotalTransactions += proxy.getTransactionCount();
    }

    /**
     * Remove the given host (host:port pair) from this Connection Group.
     * 
     * @param hostPortPair
     *            The host:port pair to remove.
     * @throws SQLException
     */
    public void removeHost(String hostPortPair) throws SQLException {
        removeHost(hostPortPair, false);
    }

    /**
     * Remove the given host (host:port pair) from this Connection Group.
     * 
     * @param hostPortPair
     *            The host:port pair to remove.
     * @param removeExisting
     *            Whether affects existing load-balanced connections or only new ones.
     * @throws SQLException
     */
    public void removeHost(String hostPortPair, boolean removeExisting) throws SQLException {
        this.removeHost(hostPortPair, removeExisting, true);
    }

    /**
     * Remove the given host (host:port pair) from this Connection Group and, consequently, from all the load-balanced connections it holds.
     * 
     * @param hostPortPair
     *            The host:port pair to remove.
     * @param removeExisting
     *            Whether affects existing load-balanced connections or only new ones.
     * @param waitForGracefulFailover
     *            If true instructs the load-balanced connections to fail-over the underlying active connection before removing this host, otherwise remove
     *            immediately.
     * @throws SQLException
     */
    public synchronized void removeHost(String hostPortPair, boolean removeExisting, boolean waitForGracefulFailover) throws SQLException {
        if (this.activeHosts == 1) {
            throw SQLError.createSQLException("Cannot remove host, only one configured host active.", null);
        }

        if (this.hostList.remove(hostPortPair)) {
            this.activeHosts--;
        } else {
            throw SQLError.createSQLException("Host is not configured: " + hostPortPair, null);
        }

        if (removeExisting) {
            // make a local copy to keep synchronization overhead to minimum
            Map<Long, LoadBalancedConnectionProxy> proxyMap = new HashMap<Long, LoadBalancedConnectionProxy>();
            synchronized (this.connectionProxies) {
                proxyMap.putAll(this.connectionProxies);
            }

            for (LoadBalancedConnectionProxy proxy : proxyMap.values()) {
                if (waitForGracefulFailover) {
                    proxy.removeHostWhenNotInUse(hostPortPair);
                } else {
                    proxy.removeHost(hostPortPair);
                }
            }
        }
        this.closedHosts.add(hostPortPair);
    }

    /**
     * Add the given host (host:port pair) to this Connection Group.
     * 
     * @param hostPortPair
     *            The host:port pair to add.
     * @throws SQLException
     */
    public void addHost(String hostPortPair) {
        addHost(hostPortPair, false);
    }

    /**
     * Add the given host (host:port pair) to this Connection Group and, consequently, to all the load-balanced connections it holds.
     * 
     * @param hostPortPair
     *            The host:port pair to add.
     * @param forExisting
     *            Whether affects existing load-balanced connections or only new ones.
     */
    public void addHost(String hostPortPair, boolean forExisting) {
        synchronized (this) {
            if (this.hostList.add(hostPortPair)) {
                this.activeHosts++;
            }
        }
        // all new connections will have this host
        if (!forExisting) {
            return;
        }

        // make a local copy to keep synchronization overhead to minimum
        Map<Long, LoadBalancedConnectionProxy> proxyMap = new HashMap<Long, LoadBalancedConnectionProxy>();
        synchronized (this.connectionProxies) {
            proxyMap.putAll(this.connectionProxies);
        }

        for (LoadBalancedConnectionProxy proxy : proxyMap.values()) {
            proxy.addHost(hostPortPair);
        }
    }
}
