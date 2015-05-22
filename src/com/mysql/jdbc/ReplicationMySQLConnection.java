/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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
import java.util.Properties;
import java.util.concurrent.Executor;

public class ReplicationMySQLConnection extends MultiHostMySQLConnection implements ReplicationConnection {
    public ReplicationMySQLConnection(MultiHostConnectionProxy proxy) {
        super(proxy);
    }

    @Override
    protected ReplicationConnectionProxy getThisAsProxy() {
        return (ReplicationConnectionProxy) super.getThisAsProxy();
    }

    public synchronized Connection getCurrentConnection() {
        return getThisAsProxy().getCurrentConnection();
    }

    public long getConnectionGroupId() {
        return getThisAsProxy().getConnectionGroupId();
    }

    public synchronized Connection getMasterConnection() {
        return getThisAsProxy().getMasterConnection();
    }

    public void promoteSlaveToMaster(String host) throws SQLException {
        getThisAsProxy().promoteSlaveToMaster(host);
    }

    public void removeMasterHost(String host) throws SQLException {
        getThisAsProxy().removeMasterHost(host);
    }

    public void removeMasterHost(String host, boolean waitUntilNotInUse) throws SQLException {
        getThisAsProxy().removeMasterHost(host, waitUntilNotInUse);
    }

    public boolean isHostMaster(String host) {
        return getThisAsProxy().isHostMaster(host);
    }

    public synchronized Connection getSlavesConnection() {
        return getThisAsProxy().getSlavesConnection();
    }

    public void addSlaveHost(String host) throws SQLException {
        getThisAsProxy().addSlaveHost(host);
    }

    public void removeSlave(String host) throws SQLException {
        getThisAsProxy().removeSlave(host);
    }

    public void removeSlave(String host, boolean closeGently) throws SQLException {
        getThisAsProxy().removeSlave(host, closeGently);
    }

    public boolean isHostSlave(String host) {
        return getThisAsProxy().isHostSlave(host);
    }

    @Override
    public void setReadOnly(boolean readOnlyFlag) throws SQLException {
        getThisAsProxy().setReadOnly(readOnlyFlag);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return getThisAsProxy().isReadOnly();
    }

    @Override
    public synchronized void ping() throws SQLException {
        try {
            getThisAsProxy().masterConnection.ping();
        } catch (SQLException e) {
            if (isMasterConnection()) {
                throw e;
            }
        }
        try {
            getThisAsProxy().slavesConnection.ping();
        } catch (SQLException e) {
            if (!isMasterConnection()) {
                throw e;
            }
        }
    }

    @Override
    public synchronized void changeUser(String userName, String newPassword) throws SQLException {
        getThisAsProxy().masterConnection.changeUser(userName, newPassword);
        getThisAsProxy().slavesConnection.changeUser(userName, newPassword);
    }

    @Override
    public synchronized void setStatementComment(String comment) {
        getThisAsProxy().masterConnection.setStatementComment(comment);
        getThisAsProxy().slavesConnection.setStatementComment(comment);
    }

    @Override
    public boolean hasSameProperties(Connection c) {
        return getThisAsProxy().masterConnection.hasSameProperties(c) && getThisAsProxy().slavesConnection.hasSameProperties(c);
    }

    @Override
    public Properties getProperties() {
        Properties props = new Properties();
        props.putAll(getThisAsProxy().masterConnection.getProperties());
        props.putAll(getThisAsProxy().slavesConnection.getProperties());

        return props;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        getThisAsProxy().doAbort(executor);
    }

    @Override
    public void abortInternal() throws SQLException {
        getThisAsProxy().doAbortInternal();
    }

    @Override
    public boolean getAllowMasterDownConnections() {
        return getThisAsProxy().allowMasterDownConnections;
    }

    @Override
    public void setAllowMasterDownConnections(boolean connectIfMasterDown) {
        getThisAsProxy().allowMasterDownConnections = connectIfMasterDown;
    }

    @Override
    public boolean getReplicationEnableJMX() {
        return getThisAsProxy().enableJMX;
    }

    @Override
    public void setReplicationEnableJMX(boolean replicationEnableJMX) {
        getThisAsProxy().enableJMX = replicationEnableJMX;
    }

    @Override
    public void setProxy(MySQLConnection proxy) {
        getThisAsProxy().setProxy(proxy);
    }
}
