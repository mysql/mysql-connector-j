/*
 * Copyright (c) 2013, 2020, Oracle and/or its affiliates.
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
 * Group of connection objects which can be configured as a group. This is used for promotion/demotion of replicas and sources in a replication configuration,
 * and for exposing metrics around replication-aware connections.
 */
public class ReplicationConnectionGroup {
    private String groupName;
    private long connections = 0;
    private long replicasAdded = 0;
    private long replicasRemoved = 0;
    private long replicasPromoted = 0;
    private long activeConnections = 0;
    private HashMap<Long, ReplicationConnection> replicationConnections = new HashMap<>();
    private Set<String> replicaHostList = new CopyOnWriteArraySet<>();
    private boolean isInitialized = false;
    private Set<String> sourceHostList = new CopyOnWriteArraySet<>();

    ReplicationConnectionGroup(String groupName) {
        this.groupName = groupName;
    }

    public long getConnectionCount() {
        return this.connections;
    }

    public long registerReplicationConnection(ReplicationConnection conn, List<String> localSourceList, List<String> localReplicaList) {
        long currentConnectionId;

        synchronized (this) {
            if (!this.isInitialized) {
                if (localSourceList != null) {
                    this.sourceHostList.addAll(localSourceList);
                }
                if (localReplicaList != null) {
                    this.replicaHostList.addAll(localReplicaList);
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

    public Collection<String> getSourceHosts() {
        return this.sourceHostList;
    }

    /**
     * Use {@link #getSourceHosts()} instead.
     * 
     * @return source hosts
     * @deprecated
     */
    @Deprecated
    public Collection<String> getMasterHosts() {
        return getSourceHosts();
    }

    public Collection<String> getReplicaHosts() {
        return this.replicaHostList;
    }

    /**
     * Use {@link #getReplicaHosts()} instead.
     * 
     * @return replica hosts
     * @deprecated
     */
    @Deprecated
    public Collection<String> getSlaveHosts() {
        return getReplicaHosts();
    }

    /**
     * Adds a host to the replicas hosts list.
     * 
     * We can safely assume that if this host was added to the replicas list, then it must be added to each one of the replication connections from this group
     * as well.
     * Unnecessary calls to {@link ReplicationConnection#addReplicaHost(String)} could result in undesirable locking issues, assuming that this method is
     * synchronized by nature.
     * 
     * This is a no-op if the group already has this host in a replica role.
     * 
     * @param hostPortPair
     *            "host:port"
     * @throws SQLException
     *             if an error occurs
     */
    public void addReplicaHost(String hostPortPair) throws SQLException {
        // only add if it's not already a replica host
        if (this.replicaHostList.add(hostPortPair)) {
            this.replicasAdded++;

            // add the replica to all connections:
            for (ReplicationConnection c : this.replicationConnections.values()) {
                c.addReplicaHost(hostPortPair);
            }
        }
    }

    /**
     * Use {@link #addReplicaHost(String)} instead.
     * 
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public void addSlaveHost(String hostPortPair) throws SQLException {
        addReplicaHost(hostPortPair);
    }

    public void handleCloseConnection(ReplicationConnection conn) {
        this.replicationConnections.remove(conn.getConnectionGroupId());
        this.activeConnections--;
    }

    /**
     * Removes a host from the replicas hosts list.
     * 
     * We can safely assume that if this host was removed from the replicas list, then it must be removed from each one of the replication connections from this
     * group as well.
     * Unnecessary calls to {@link ReplicationConnection#removeReplica(String, boolean)} could result in undesirable locking issues, assuming that this method
     * is
     * synchronized by nature.
     * 
     * This is a no-op if the group doesn't have this host in a replica role.
     * 
     * @param hostPortPair
     *            "host:port"
     * @param closeGently
     *            remove host when it's not in use
     * @throws SQLException
     *             if an error occurs
     */
    public void removeReplicaHost(String hostPortPair, boolean closeGently) throws SQLException {
        if (this.replicaHostList.remove(hostPortPair)) {
            this.replicasRemoved++;

            // remove the replica from all connections:
            for (ReplicationConnection c : this.replicationConnections.values()) {
                c.removeReplica(hostPortPair, closeGently);
            }
        }
    }

    /**
     * Use {@link #removeReplicaHost(String, boolean)} instead.
     * 
     * @param hostPortPair
     *            host:port
     * @param closeGently
     *            option
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public void removeSlaveHost(String hostPortPair, boolean closeGently) throws SQLException {
        removeReplicaHost(hostPortPair, closeGently);
    }

    /**
     * Promotes a replica host to source.
     * 
     * We can safely assume that if this host was removed from the replicas list or added to the sources list, then the same host promotion must happen in each
     * one of the replication connections from this group as well.
     * Unnecessary calls to {@link ReplicationConnection#promoteReplicaToSource(String)} could result in undesirable locking issues, assuming that this method
     * is
     * synchronized by nature.
     * 
     * This is a no-op if the group already has this host in a source role and not in replica role.
     * 
     * @param hostPortPair
     *            "host:port"
     * @throws SQLException
     *             if an error occurs
     */
    public void promoteReplicaToSource(String hostPortPair) throws SQLException {
        // remove host from replicas AND add host to sources, note that both operands need to be evaluated.
        if (this.replicaHostList.remove(hostPortPair) | this.sourceHostList.add(hostPortPair)) {
            this.replicasPromoted++;

            for (ReplicationConnection c : this.replicationConnections.values()) {
                c.promoteReplicaToSource(hostPortPair);
            }
        }
    }

    /**
     * Use {@link #promoteReplicaToSource(String)} instead.
     * 
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public void promoteSlaveToMaster(String hostPortPair) throws SQLException {
        promoteReplicaToSource(hostPortPair);
    }

    /**
     * Removes a host from the sources hosts list.
     * 
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     *             if an error occurs
     */
    public void removeSourceHost(String hostPortPair) throws SQLException {
        this.removeSourceHost(hostPortPair, true);
    }

    /**
     * Use {@link #removeSourceHost(String)} instead.
     * 
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public void removeMasterHost(String hostPortPair) throws SQLException {
        removeSourceHost(hostPortPair);
    }

    /**
     * Removes a host from the sources hosts list.
     * 
     * We can safely assume that if this host was removed from the sources list, then it must be removed from each one of the replication connections from this
     * group as well.
     * Unnecessary calls to {@link ReplicationConnection#removeSourceHost(String, boolean)} could result in undesirable locking issues, assuming that this
     * method is synchronized by nature.
     * 
     * This is a no-op if the group doesn't have this host in a source role.
     * 
     * @param hostPortPair
     *            "host:port"
     * @param closeGently
     *            remove host when it's not in use
     * @throws SQLException
     *             if an error occurs
     */
    public void removeSourceHost(String hostPortPair, boolean closeGently) throws SQLException {
        if (this.sourceHostList.remove(hostPortPair)) {
            // remove the source from all connections:
            for (ReplicationConnection c : this.replicationConnections.values()) {
                c.removeSourceHost(hostPortPair, closeGently);
            }
        }
    }

    /**
     * Use {@link #removeSourceHost(String, boolean)} instead.
     * 
     * @param hostPortPair
     *            host:port
     * @param closeGently
     *            option
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public void removeMasterHost(String hostPortPair, boolean closeGently) throws SQLException {
        removeSourceHost(hostPortPair, closeGently);
    }

    public int getConnectionCountWithHostAsReplica(String hostPortPair) {
        int matched = 0;

        for (ReplicationConnection c : this.replicationConnections.values()) {
            if (c.isHostReplica(hostPortPair)) {
                matched++;
            }
        }
        return matched;
    }

    /**
     * Use {@link #getConnectionCountWithHostAsReplica(String)} instead.
     * 
     * @param hostPortPair
     *            host:port
     * @return count
     * @deprecated
     */
    @Deprecated
    public int getConnectionCountWithHostAsSlave(String hostPortPair) {
        return getConnectionCountWithHostAsReplica(hostPortPair);
    }

    public int getConnectionCountWithHostAsSource(String hostPortPair) {
        int matched = 0;

        for (ReplicationConnection c : this.replicationConnections.values()) {
            if (c.isHostSource(hostPortPair)) {
                matched++;
            }
        }
        return matched;
    }

    /**
     * Use {@link #getConnectionCountWithHostAsSource(String)} instead.
     * 
     * @param hostPortPair
     *            host:port
     * @return count
     * @deprecated
     */
    @Deprecated
    public int getConnectionCountWithHostAsMaster(String hostPortPair) {
        return getConnectionCountWithHostAsSource(hostPortPair);
    }

    public long getNumberOfReplicasAdded() {
        return this.replicasAdded;
    }

    /**
     * Use {@link #getNumberOfReplicasAdded()} instead.
     * 
     * @return count
     * @deprecated
     */
    @Deprecated
    public long getNumberOfSlavesAdded() {
        return getNumberOfReplicasAdded();
    }

    public long getNumberOfReplicasRemoved() {
        return this.replicasRemoved;
    }

    /**
     * Use {@link #getNumberOfReplicasRemoved()} instead.
     * 
     * @return count
     * @deprecated
     */
    @Deprecated
    public long getNumberOfSlavesRemoved() {
        return getNumberOfReplicasRemoved();
    }

    public long getNumberOfReplicaPromotions() {
        return this.replicasPromoted;
    }

    /**
     * Use {@link #getNumberOfReplicaPromotions()} instead.
     * 
     * @return count
     * @deprecated
     */
    @Deprecated
    public long getNumberOfSlavePromotions() {
        return getNumberOfReplicaPromotions();
    }

    public long getTotalConnectionCount() {
        return this.connections;
    }

    public long getActiveConnectionCount() {
        return this.activeConnections;
    }

    @Override
    public String toString() {
        return "ReplicationConnectionGroup[groupName=" + this.groupName + ",sourceHostList=" + this.sourceHostList + ",replicaHostList=" + this.replicaHostList
                + "]";
    }
}
