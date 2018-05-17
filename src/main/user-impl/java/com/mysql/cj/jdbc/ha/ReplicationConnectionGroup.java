/*
 * Copyright (c) 2013, 2018, Oracle and/or its affiliates. All rights reserved.
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Group of connection objects which can be configured as a group. This is used for promotion/demotion of slaves and masters in a replication configuration,
 * and for exposing metrics around replication-aware connections.
 */
public class ReplicationConnectionGroup {
    private String groupName;
    private long connections = 0;
    private long slavesAdded = 0;
    private long slavesRemoved = 0;
    private long slavesPromoted = 0;
    private long activeConnections = 0;
    private HashMap<Long, ReplicationConnection> replicationConnections = new HashMap<>();
    private Set<String> slaveHostList = new CopyOnWriteArraySet<>();
    private boolean isInitialized = false;
    private Set<String> masterHostList = new CopyOnWriteArraySet<>();

    ReplicationConnectionGroup(String groupName) {
        this.groupName = groupName;
    }

    public long getConnectionCount() {
        return this.connections;
    }

    public long registerReplicationConnection(ReplicationConnection conn, List<String> localMasterList, List<String> localSlaveList) {
        long currentConnectionId;

        synchronized (this) {
            if (!this.isInitialized) {
                if (localMasterList != null) {
                    this.masterHostList.addAll(localMasterList);
                }
                if (localSlaveList != null) {
                    this.slaveHostList.addAll(localSlaveList);
                }
                this.isInitialized = true;
            }
            currentConnectionId = ++this.connections;
            this.replicationConnections.put(Long.valueOf(currentConnectionId), conn);
        }
        this.activeConnections++;

        return currentConnectionId;
    }

    public String getGroupName() {
        return this.groupName;
    }

    public Collection<String> getMasterHosts() {
        return this.masterHostList;
    }

    public Collection<String> getSlaveHosts() {
        return this.slaveHostList;
    }

    /**
     * Adds a host to the slaves hosts list.
     * 
     * We can safely assume that if this host was added to the slaves list, then it must be added to each one of the replication connections from this group as
     * well.
     * Unnecessary calls to {@link ReplicationConnection#addSlaveHost(String)} could result in undesirable locking issues, assuming that this method is
     * synchronized by nature.
     * 
     * This is a no-op if the group already has this host in a slave role.
     * 
     * @param hostPortPair
     *            "host:port"
     * @throws SQLException
     *             if an error occurs
     */
    public void addSlaveHost(String hostPortPair) throws SQLException {
        // only add if it's not already a slave host
        if (this.slaveHostList.add(hostPortPair)) {
            this.slavesAdded++;

            // add the slave to all connections:
            for (ReplicationConnection c : this.replicationConnections.values()) {
                c.addSlaveHost(hostPortPair);
            }
        }
    }

    public void handleCloseConnection(ReplicationConnection conn) {
        this.replicationConnections.remove(conn.getConnectionGroupId());
        this.activeConnections--;
    }

    /**
     * Removes a host from the slaves hosts list.
     * 
     * We can safely assume that if this host was removed from the slaves list, then it must be removed from each one of the replication connections from this
     * group as well.
     * Unnecessary calls to {@link ReplicationConnection#removeSlave(String, boolean)} could result in undesirable locking issues, assuming that this method is
     * synchronized by nature.
     * 
     * This is a no-op if the group doesn't have this host in a slave role.
     * 
     * @param hostPortPair
     *            "host:port"
     * @param closeGently
     *            remove host when it's not in use
     * @throws SQLException
     *             if an error occurs
     */
    public void removeSlaveHost(String hostPortPair, boolean closeGently) throws SQLException {
        if (this.slaveHostList.remove(hostPortPair)) {
            this.slavesRemoved++;

            // remove the slave from all connections:
            for (ReplicationConnection c : this.replicationConnections.values()) {
                c.removeSlave(hostPortPair, closeGently);
            }
        }
    }

    /**
     * Promotes a slave host to master.
     * 
     * We can safely assume that if this host was removed from the slaves list or added to the masters list, then the same host promotion must happen in each
     * one of the replication connections from this group as well.
     * Unnecessary calls to {@link ReplicationConnection#promoteSlaveToMaster(String)} could result in undesirable locking issues, assuming that this method is
     * synchronized by nature.
     * 
     * This is a no-op if the group already has this host in a master role and not in slave role.
     * 
     * @param hostPortPair
     *            "host:port"
     * @throws SQLException
     *             if an error occurs
     */
    public void promoteSlaveToMaster(String hostPortPair) throws SQLException {
        // remove host from slaves AND add host to masters, note that both operands need to be evaluated.
        if (this.slaveHostList.remove(hostPortPair) | this.masterHostList.add(hostPortPair)) {
            this.slavesPromoted++;

            for (ReplicationConnection c : this.replicationConnections.values()) {
                c.promoteSlaveToMaster(hostPortPair);
            }
        }
    }

    /**
     * Removes a host from the masters hosts list.
     * 
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     *             if an error occurs
     */
    public void removeMasterHost(String hostPortPair) throws SQLException {
        this.removeMasterHost(hostPortPair, true);
    }

    /**
     * Removes a host from the masters hosts list.
     * 
     * We can safely assume that if this host was removed from the masters list, then it must be removed from each one of the replication connections from this
     * group as well.
     * Unnecessary calls to {@link ReplicationConnection#removeMasterHost(String, boolean)} could result in undesirable locking issues, assuming that this
     * method is synchronized by nature.
     * 
     * This is a no-op if the group doesn't have this host in a master role.
     * 
     * @param hostPortPair
     *            "host:port"
     * @param closeGently
     *            remove host when it's not in use
     * @throws SQLException
     *             if an error occurs
     */
    public void removeMasterHost(String hostPortPair, boolean closeGently) throws SQLException {
        if (this.masterHostList.remove(hostPortPair)) {
            // remove the master from all connections:
            for (ReplicationConnection c : this.replicationConnections.values()) {
                c.removeMasterHost(hostPortPair, closeGently);
            }
        }
    }

    public int getConnectionCountWithHostAsSlave(String hostPortPair) {
        int matched = 0;

        for (ReplicationConnection c : this.replicationConnections.values()) {
            if (c.isHostSlave(hostPortPair)) {
                matched++;
            }
        }
        return matched;
    }

    public int getConnectionCountWithHostAsMaster(String hostPortPair) {
        int matched = 0;

        for (ReplicationConnection c : this.replicationConnections.values()) {
            if (c.isHostMaster(hostPortPair)) {
                matched++;
            }
        }
        return matched;
    }

    public long getNumberOfSlavesAdded() {
        return this.slavesAdded;
    }

    public long getNumberOfSlavesRemoved() {
        return this.slavesRemoved;
    }

    public long getNumberOfSlavePromotions() {
        return this.slavesPromoted;
    }

    public long getTotalConnectionCount() {
        return this.connections;
    }

    public long getActiveConnectionCount() {
        return this.activeConnections;
    }

    @Override
    public String toString() {
        return "ReplicationConnectionGroup[groupName=" + this.groupName + ",masterHostList=" + this.masterHostList + ",slaveHostList=" + this.slaveHostList
                + "]";
    }
}
