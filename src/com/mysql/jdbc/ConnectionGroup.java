/*
  Copyright (c) 2010, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConnectionGroup {
    private String groupName;
    private long connections = 0;
    private long activeConnections = 0;
    private HashMap<Long, LoadBalancingConnectionProxy> connectionProxies = new HashMap<Long, LoadBalancingConnectionProxy>();
    private Set<String> hostList = new HashSet<String>();
    private boolean isInitialized = false;
    private long closedProxyTotalPhysicalConnections = 0;
    private long closedProxyTotalTransactions = 0;
    private int activeHosts = 0;
    private Set<String> closedHosts = new HashSet<String>();

    ConnectionGroup(String groupName) {
        this.groupName = groupName;
    }

    public long registerConnectionProxy(LoadBalancingConnectionProxy proxy, List<String> localHostList) {
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

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.ConnectionGroupMBean#getGroupName()
     */
    public String getGroupName() {
        return this.groupName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.ConnectionGroupMBean#getInitialHostList()
     */
    public Collection<String> getInitialHosts() {
        return this.hostList;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.ConnectionGroupMBean#getActiveHostCount()
     */
    public int getActiveHostCount() {
        return this.activeHosts;
    }

    public Collection<String> getClosedHosts() {
        return this.closedHosts;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.ConnectionGroupMBean#getTotalLogicalConnectionCount()
     */
    public long getTotalLogicalConnectionCount() {
        return this.connections;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.ConnectionGroupMBean#getActiveLogicalConnectionCount()
     */
    public long getActiveLogicalConnectionCount() {
        return this.activeConnections;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.ConnectionGroupMBean#getActivePhysicalConnectionCount()
     */
    public long getActivePhysicalConnectionCount() {
        long result = 0;
        Map<Long, LoadBalancingConnectionProxy> proxyMap = new HashMap<Long, LoadBalancingConnectionProxy>();
        synchronized (this.connectionProxies) {
            proxyMap.putAll(this.connectionProxies);
        }
        Iterator<Map.Entry<Long, LoadBalancingConnectionProxy>> i = proxyMap.entrySet().iterator();
        while (i.hasNext()) {
            LoadBalancingConnectionProxy proxy = i.next().getValue();
            result += proxy.getActivePhysicalConnectionCount();

        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.ConnectionGroupMBean#getTotalPhysicalConnectionCount()
     */
    public long getTotalPhysicalConnectionCount() {
        long allConnections = this.closedProxyTotalPhysicalConnections;
        Map<Long, LoadBalancingConnectionProxy> proxyMap = new HashMap<Long, LoadBalancingConnectionProxy>();
        synchronized (this.connectionProxies) {
            proxyMap.putAll(this.connectionProxies);
        }
        Iterator<Map.Entry<Long, LoadBalancingConnectionProxy>> i = proxyMap.entrySet().iterator();
        while (i.hasNext()) {
            LoadBalancingConnectionProxy proxy = i.next().getValue();
            allConnections += proxy.getTotalPhysicalConnectionCount();

        }
        return allConnections;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.ConnectionGroupMBean#getTotalTransactionCount()
     */
    public long getTotalTransactionCount() {
        // need to account for closed connection proxies
        long transactions = this.closedProxyTotalTransactions;
        Map<Long, LoadBalancingConnectionProxy> proxyMap = new HashMap<Long, LoadBalancingConnectionProxy>();
        synchronized (this.connectionProxies) {
            proxyMap.putAll(this.connectionProxies);
        }
        Iterator<Map.Entry<Long, LoadBalancingConnectionProxy>> i = proxyMap.entrySet().iterator();
        while (i.hasNext()) {
            LoadBalancingConnectionProxy proxy = i.next().getValue();
            transactions += proxy.getTransactionCount();

        }
        return transactions;
    }

    public void closeConnectionProxy(LoadBalancingConnectionProxy proxy) {
        this.activeConnections--;
        this.connectionProxies.remove(Long.valueOf(proxy.getConnectionGroupProxyID()));
        this.closedProxyTotalPhysicalConnections += proxy.getTotalPhysicalConnectionCount();
        this.closedProxyTotalTransactions += proxy.getTransactionCount();

    }

    public void removeHost(String host) throws SQLException {
        removeHost(host, false);
    }

    public void removeHost(String host, boolean killExistingConnections) throws SQLException {
        this.removeHost(host, killExistingConnections, true);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.ConnectionGroupMBean#removeHost(java.lang.String, boolean, boolean)
     */
    public synchronized void removeHost(String host, boolean killExistingConnections, boolean waitForGracefulFailover) throws SQLException {
        if (this.activeHosts == 1) {
            throw SQLError.createSQLException("Cannot remove host, only one configured host active.", null);
        }

        if (this.hostList.remove(host)) {
            this.activeHosts--;
        } else {
            throw SQLError.createSQLException("Host is not configured: " + host, null);
        }

        if (killExistingConnections) {
            // make a local copy to keep synchronization overhead to minimum
            Map<Long, LoadBalancingConnectionProxy> proxyMap = new HashMap<Long, LoadBalancingConnectionProxy>();
            synchronized (this.connectionProxies) {
                proxyMap.putAll(this.connectionProxies);
            }

            Iterator<Map.Entry<Long, LoadBalancingConnectionProxy>> i = proxyMap.entrySet().iterator();
            while (i.hasNext()) {
                LoadBalancingConnectionProxy proxy = i.next().getValue();
                if (waitForGracefulFailover) {
                    proxy.removeHostWhenNotInUse(host);
                } else {
                    proxy.removeHost(host);
                }
            }
        }
        this.closedHosts.add(host);
    }

    public void addHost(String host) {
        addHost(host, false);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.mysql.jdbc.ConnectionGroupMBean#addHost(java.lang.String, boolean)
     */
    public void addHost(String host, boolean forExisting) {

        synchronized (this) {
            if (this.hostList.add(host)) {
                this.activeHosts++;
            }
        }
        // all new connections will have this host
        if (!forExisting) {
            return;
        }

        // make a local copy to keep synchronization overhead to minimum
        Map<Long, LoadBalancingConnectionProxy> proxyMap = new HashMap<Long, LoadBalancingConnectionProxy>();
        synchronized (this.connectionProxies) {
            proxyMap.putAll(this.connectionProxies);
        }

        Iterator<Map.Entry<Long, LoadBalancingConnectionProxy>> i = proxyMap.entrySet().iterator();
        while (i.hasNext()) {
            LoadBalancingConnectionProxy proxy = i.next().getValue();
            proxy.addHost(host);
        }

    }

}
