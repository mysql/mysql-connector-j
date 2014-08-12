/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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
import java.util.List;
import java.util.Set;

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
    private HashMap<Long, ReplicationConnection> replicationConnections = new HashMap<Long, ReplicationConnection>();
    private Set<String> slaveHostList = new HashSet<String>();
    private boolean isInitialized = false;
    private Set<String> masterHostList = new HashSet<String>();

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

    public void addSlaveHost(String host) throws SQLException {
        // only do this if addition was successful:
        if (this.slaveHostList.add(host)) {
            this.slavesAdded++;
        }
        // add the slave to all connections:
        for (ReplicationConnection c : this.replicationConnections.values()) {
            c.addSlaveHost(host);
        }

    }

    public void handleCloseConnection(ReplicationConnection conn) {
        this.replicationConnections.remove(conn.getConnectionGroupId());
        this.activeConnections--;
    }

    public void removeSlaveHost(String host, boolean closeGently) throws SQLException {
        if (this.slaveHostList.remove(host)) {
            this.slavesRemoved++;
        }
        for (ReplicationConnection c : this.replicationConnections.values()) {
            c.removeSlave(host, closeGently);
        }
    }

    public void promoteSlaveToMaster(String host) throws SQLException {
        this.slaveHostList.remove(host);
        this.masterHostList.add(host);
        for (ReplicationConnection c : this.replicationConnections.values()) {
            c.promoteSlaveToMaster(host);
        }

        this.slavesPromoted++;
    }

    public void removeMasterHost(String host) throws SQLException {
        this.removeMasterHost(host, true);
    }

    public void removeMasterHost(String host, boolean closeGently) throws SQLException {
        if (this.masterHostList.remove(host)) {

        }
        for (ReplicationConnection c : this.replicationConnections.values()) {
            c.removeMasterHost(host, closeGently);
        }

    }

    public int getConnectionCountWithHostAsSlave(String host) {
        int matched = 0;

        for (ReplicationConnection c : this.replicationConnections.values()) {
            if (c.isHostSlave(host)) {
                matched++;
            }
        }
        return matched;
    }

    public int getConnectionCountWithHostAsMaster(String host) {
        int matched = 0;

        for (ReplicationConnection c : this.replicationConnections.values()) {
            if (c.isHostMaster(host)) {
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

}
