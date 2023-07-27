/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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
import java.util.Properties;
import java.util.concurrent.Executor;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.JdbcConnection;
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
    public JdbcConnection getActiveMySQLConnection() {
        return getCurrentConnection();
    }

    @Override
    public synchronized JdbcConnection getCurrentConnection() {
        return getThisAsProxy().getCurrentConnection();
    }

    @Override
    public long getConnectionGroupId() {
        return getThisAsProxy().getConnectionGroupId();
    }

    @Override
    public synchronized JdbcConnection getSourceConnection() {
        return getThisAsProxy().getSourceConnection();
    }

    private JdbcConnection getValidatedSourceConnection() {
        JdbcConnection conn = getThisAsProxy().sourceConnection;
        try {
            return conn == null || conn.isClosed() ? null : conn;
        } catch (SQLException e) {
            return null;
        }
    }

    @Override
    public void promoteReplicaToSource(String host) throws SQLException {
        getThisAsProxy().promoteReplicaToSource(host);
    }

    @Override
    public void removeSourceHost(String host) throws SQLException {
        getThisAsProxy().removeSourceHost(host);
    }

    @Override
    public void removeSourceHost(String host, boolean waitUntilNotInUse) throws SQLException {
        getThisAsProxy().removeSourceHost(host, waitUntilNotInUse);
    }

    @Override
    public boolean isHostSource(String host) {
        return getThisAsProxy().isHostSource(host);
    }

    @Override
    public synchronized JdbcConnection getReplicaConnection() {
        return getThisAsProxy().getReplicasConnection();
    }

    private JdbcConnection getValidatedReplicasConnection() {
        JdbcConnection conn = getThisAsProxy().replicasConnection;
        try {
            return conn == null || conn.isClosed() ? null : conn;
        } catch (SQLException e) {
            return null;
        }
    }

    @Override
    public void addReplicaHost(String host) throws SQLException {
        getThisAsProxy().addReplicaHost(host);
    }

    @Override
    public void removeReplica(String host) throws SQLException {
        getThisAsProxy().removeReplica(host);
    }

    @Override
    public void removeReplica(String host, boolean closeGently) throws SQLException {
        getThisAsProxy().removeReplica(host, closeGently);
    }

    @Override
    public boolean isHostReplica(String host) {
        return getThisAsProxy().isHostReplica(host);
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
            if ((conn = getValidatedSourceConnection()) != null) {
                conn.ping();
            }
        } catch (SQLException e) {
            if (isSourceConnection()) {
                throw e;
            }
        }
        try {
            if ((conn = getValidatedReplicasConnection()) != null) {
                conn.ping();
            }
        } catch (SQLException e) {
            if (!isSourceConnection()) {
                throw e;
            }
        }
    }

    @Override
    public synchronized void changeUser(String userName, String newPassword) throws SQLException {
        JdbcConnection conn;
        if ((conn = getValidatedSourceConnection()) != null) {
            conn.changeUser(userName, newPassword);
        }
        if ((conn = getValidatedReplicasConnection()) != null) {
            conn.changeUser(userName, newPassword);
        }
    }

    @Override
    public synchronized void setStatementComment(String comment) {
        JdbcConnection conn;
        if ((conn = getValidatedSourceConnection()) != null) {
            conn.setStatementComment(comment);
        }
        if ((conn = getValidatedReplicasConnection()) != null) {
            conn.setStatementComment(comment);
        }
    }

    @Override
    public boolean hasSameProperties(JdbcConnection c) {
        JdbcConnection connM = getValidatedSourceConnection();
        JdbcConnection connS = getValidatedReplicasConnection();
        if (connM == null && connS == null) {
            return false;
        }
        return (connM == null || connM.hasSameProperties(c)) && (connS == null || connS.hasSameProperties(c));
    }

    @Override
    public Properties getProperties() {
        Properties props = new Properties();
        JdbcConnection conn;
        if ((conn = getValidatedSourceConnection()) != null) {
            props.putAll(conn.getProperties());
        }
        if ((conn = getValidatedReplicasConnection()) != null) {
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
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    @Deprecated
    @Override
    public synchronized void clearHasTriedMaster() {
        getThisAsProxy().sourceConnection.clearHasTriedMaster();
        getThisAsProxy().replicasConnection.clearHasTriedMaster();
    }

}
