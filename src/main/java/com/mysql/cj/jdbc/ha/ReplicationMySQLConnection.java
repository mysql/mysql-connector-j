/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.ha;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.ha.ReplicationConnection;
import com.mysql.cj.core.Messages;
import com.mysql.cj.jdbc.exceptions.SQLError;

public class ReplicationMySQLConnection extends MultiHostMySQLConnection implements ReplicationConnection {
    public ReplicationMySQLConnection(MultiHostConnectionProxy proxy) {
        super(proxy);
    }

    @Override
    public ReplicationConnectionProxy getThisAsProxy() {
        return (ReplicationConnectionProxy) super.getThisAsProxy();
    }

    @Override
    protected JdbcConnection getActiveMySQLConnection() {
        return getCurrentConnection();
    }

    public synchronized JdbcConnection getCurrentConnection() {
        return getThisAsProxy().getCurrentConnection();
    }

    public long getConnectionGroupId() {
        return getThisAsProxy().getConnectionGroupId();
    }

    public synchronized JdbcConnection getMasterConnection() {
        return getThisAsProxy().getMasterConnection();
    }

    private JdbcConnection getValidatedMasterConnection() {
        JdbcConnection conn = getThisAsProxy().masterConnection;
        try {
            return conn == null || conn.isClosed() ? null : conn;
        } catch (SQLException e) {
            return null;
        }
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

    public synchronized JdbcConnection getSlavesConnection() {
        return getThisAsProxy().getSlavesConnection();
    }

    private JdbcConnection getValidatedSlavesConnection() {
        JdbcConnection conn = getThisAsProxy().slavesConnection;
        try {
            return conn == null || conn.isClosed() ? null : conn;
        } catch (SQLException e) {
            return null;
        }
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
        JdbcConnection conn;
        try {
            if ((conn = getValidatedMasterConnection()) != null) {
                conn.ping();
            }
        } catch (SQLException e) {
            if (isMasterConnection()) {
                throw e;
            }
        }
        try {
            if ((conn = getValidatedSlavesConnection()) != null) {
                conn.ping();
            }
        } catch (SQLException e) {
            if (!isMasterConnection()) {
                throw e;
            }
        }
    }

    @Override
    public synchronized void changeUser(String userName, String newPassword) throws SQLException {
        JdbcConnection conn;
        if ((conn = getValidatedMasterConnection()) != null) {
            conn.changeUser(userName, newPassword);
        }
        if ((conn = getValidatedSlavesConnection()) != null) {
            conn.changeUser(userName, newPassword);
        }
    }

    @Override
    public synchronized void setStatementComment(String comment) {
        JdbcConnection conn;
        if ((conn = getValidatedMasterConnection()) != null) {
            conn.setStatementComment(comment);
        }
        if ((conn = getValidatedSlavesConnection()) != null) {
            conn.setStatementComment(comment);
        }
    }

    @Override
    public boolean hasSameProperties(JdbcConnection c) {
        JdbcConnection connM = getValidatedMasterConnection();
        JdbcConnection connS = getValidatedSlavesConnection();
        if (connM == null && connS == null) {
            return false;
        }
        return (connM == null || connM.hasSameProperties(c)) && (connS == null || connS.hasSameProperties(c));
    }

    @Override
    public Properties getProperties() {
        Properties props = new Properties();
        JdbcConnection conn;
        if ((conn = getValidatedMasterConnection()) != null) {
            props.putAll(conn.getProperties());
        }
        if ((conn = getValidatedSlavesConnection()) != null) {
            props.putAll(conn.getProperties());
        }

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
    public void setProxy(JdbcConnection proxy) {
        getThisAsProxy().setProxy(proxy);
    }

    /*
     * public void setProxy(JdbcConnection proxy) {
     * this.proxy = proxy;
     * if (this.masterConnection != null) {
     * this.masterConnection.setProxy(proxy);
     * }
     * if (this.slavesConnection != null) {
     * this.slavesConnection.setProxy(proxy);
     * }
     * }
     */

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    @Deprecated
    @Override
    public synchronized void clearHasTriedMaster() {
        getThisAsProxy().masterConnection.clearHasTriedMaster();
        getThisAsProxy().slavesConnection.clearHasTriedMaster();
    }

}
