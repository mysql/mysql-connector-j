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

package com.mysql.cj.jdbc;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mysql.cj.Messages;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;

public class ConnectionGroup {

    private String groupName;
    private long connections = 0;
    private long activeConnections = 0;
    private HashMap<Long, LoadBalancedConnectionProxy> connectionProxies = new HashMap<>();
    private Set<String> hostList = new HashSet<>();
    private boolean isInitialized = false;
    private long closedProxyTotalPhysicalConnections = 0;
    private long closedProxyTotalTransactions = 0;
    private int activeHosts = 0;
    private Set<String> closedHosts = new HashSet<>();

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
        Map<Long, LoadBalancedConnectionProxy> proxyMap = new HashMap<>();
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
        Map<Long, LoadBalancedConnectionProxy> proxyMap = new HashMap<>();
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
        Map<Long, LoadBalancedConnectionProxy> proxyMap = new HashMap<>();
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
     *             if a database access error occurs
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
     *             if a database access error occurs
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
     *             if a database access error occurs
     */
    public synchronized void removeHost(String hostPortPair, boolean removeExisting, boolean waitForGracefulFailover) throws SQLException {
        if (this.activeHosts == 1) {
            throw SQLError.createSQLException(Messages.getString("ConnectionGroup.0"), null);
        }

        if (this.hostList.remove(hostPortPair)) {
            this.activeHosts--;
        } else {
            throw SQLError.createSQLException(Messages.getString("ConnectionGroup.1", new Object[] { hostPortPair }), null);
        }

        if (removeExisting) {
            // make a local copy to keep synchronization overhead to minimum
            Map<Long, LoadBalancedConnectionProxy> proxyMap = new HashMap<>();
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
        Map<Long, LoadBalancedConnectionProxy> proxyMap = new HashMap<>();
        synchronized (this.connectionProxies) {
            proxyMap.putAll(this.connectionProxies);
        }

        for (LoadBalancedConnectionProxy proxy : proxyMap.values()) {
            proxy.addHost(hostPortPair);
        }
    }

}
