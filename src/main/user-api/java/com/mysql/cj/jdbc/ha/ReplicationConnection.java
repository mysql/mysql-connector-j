/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc.ha;

import java.sql.SQLException;

import com.mysql.cj.jdbc.JdbcConnection;

public interface ReplicationConnection extends JdbcConnection {

    public long getConnectionGroupId();

    public JdbcConnection getCurrentConnection();

    public JdbcConnection getSourceConnection();

    /**
     * Use {@link #getSourceConnection()} instead.
     *
     * @return {@link JdbcConnection}
     * @deprecated
     */
    @Deprecated
    default public JdbcConnection getMasterConnection() {
        return getSourceConnection();
    }

    public void promoteReplicaToSource(String host) throws SQLException;

    /**
     * Use {@link #promoteReplicaToSource(String)} instead.
     *
     * @param host
     *            host name
     * @throws SQLException
     *             in case of failure
     * @deprecated
     */
    @Deprecated
    default public void promoteSlaveToMaster(String host) throws SQLException {
        promoteReplicaToSource(host);
    }

    public void removeSourceHost(String host) throws SQLException;

    /**
     * Use {@link #removeSourceHost(String)} instead.
     *
     * @param host
     *            host name
     * @throws SQLException
     *             in case of failure
     * @deprecated
     */
    @Deprecated
    default public void removeMasterHost(String host) throws SQLException {
        removeSourceHost(host);
    }

    public void removeSourceHost(String host, boolean waitUntilNotInUse) throws SQLException;

    /**
     * Use {@link #removeSourceHost(String, boolean)} instead.
     *
     * @param host
     *            host name
     * @param waitUntilNotInUse
     *            remove immediately or wait for it's release
     * @throws SQLException
     *             in case of failure
     * @deprecated
     */
    @Deprecated
    default public void removeMasterHost(String host, boolean waitUntilNotInUse) throws SQLException {
        removeSourceHost(host, waitUntilNotInUse);
    }

    public boolean isHostSource(String host);

    /**
     * Use {@link #isHostSource(String)} instead.
     *
     * @param host
     *            host name
     * @return true if it's a source host
     * @deprecated
     */
    @Deprecated
    default public boolean isHostMaster(String host) {
        return isHostSource(host);
    }

    public JdbcConnection getReplicaConnection();

    /**
     * Use {@link #getReplicaConnection()} instead.
     *
     * @return {@link JdbcConnection}
     * @deprecated
     */
    @Deprecated
    default public JdbcConnection getSlavesConnection() {
        return getReplicaConnection();
    }

    public void addReplicaHost(String host) throws SQLException;

    /**
     * Use {@link #addReplicaHost(String)} instead.
     *
     * @param host
     *            host name
     * @throws SQLException
     *             in case of failure
     * @deprecated
     */
    @Deprecated
    default public void addSlaveHost(String host) throws SQLException {
        addReplicaHost(host);
    }

    public void removeReplica(String host) throws SQLException;

    /**
     * Use {@link #removeReplica(String)} instead.
     *
     * @param host
     *            host name
     * @throws SQLException
     *             in case of failure
     * @deprecated
     */
    @Deprecated
    default public void removeSlave(String host) throws SQLException {
        removeReplica(host);
    }

    public void removeReplica(String host, boolean closeGently) throws SQLException;

    /**
     * Use {@link #removeReplica(String, boolean)} instead.
     *
     * @param host
     *            host name
     * @param closeGently
     *            mode
     * @throws SQLException
     *             in case of failure
     * @deprecated
     */
    @Deprecated
    default public void removeSlave(String host, boolean closeGently) throws SQLException {
        removeReplica(host, closeGently);
    }

    public boolean isHostReplica(String host);

    /**
     * Use {@link #isHostReplica(String)} instead.
     *
     * @param host
     *            host name
     * @return true if it's a replica
     * @deprecated
     */
    @Deprecated
    default public boolean isHostSlave(String host) {
        return isHostReplica(host);
    }

}
