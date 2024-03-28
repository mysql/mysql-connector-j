/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.mysql.cj.conf.PropertyKey;

public class SuspendableXAConnection extends MysqlPooledConnection implements XAConnection, XAResource {

    private static final Lock LOCK = new ReentrantLock();

    private final Lock lock = new ReentrantLock();

    protected static SuspendableXAConnection getInstance(JdbcConnection mysqlConnection) throws SQLException {
        return new SuspendableXAConnection(mysqlConnection);
    }

    public SuspendableXAConnection(JdbcConnection connection) {
        super(connection);
        this.underlyingConnection = connection;
    }

    private static final Map<Xid, XAConnection> XIDS_TO_PHYSICAL_CONNECTIONS = new HashMap<>();

    private Xid currentXid;

    private XAConnection currentXAConnection;
    private XAResource currentXAResource;

    private JdbcConnection underlyingConnection;

    private static XAConnection findConnectionForXid(JdbcConnection connectionToWrap, Xid xid) throws SQLException {
        LOCK.lock();
        try {
            // TODO: check for same GTRID, but different BQUALs...MySQL doesn't allow this yet

            // Note, we don't need to check for XIDs here, because MySQL itself will complain with a XAER_NOTA if need be.

            XAConnection conn = XIDS_TO_PHYSICAL_CONNECTIONS.get(xid);

            if (conn == null) {
                conn = new MysqlXAConnection(connectionToWrap, connectionToWrap.getPropertySet().getBooleanProperty(PropertyKey.logXaCommands).getValue());
                XIDS_TO_PHYSICAL_CONNECTIONS.put(xid, conn);
            }

            return conn;
        } finally {
            LOCK.unlock();
        }
    }

    private static void removeXAConnectionMapping(Xid xid) {
        LOCK.lock();
        try {
            XIDS_TO_PHYSICAL_CONNECTIONS.remove(xid);
        } finally {
            LOCK.unlock();
        }
    }

    private void switchToXid(Xid xid) throws XAException {
        this.lock.lock();
        try {
            if (xid == null) {
                throw new XAException();
            }

            try {
                if (!xid.equals(this.currentXid)) {
                    XAConnection toSwitchTo = findConnectionForXid(this.underlyingConnection, xid);
                    this.currentXAConnection = toSwitchTo;
                    this.currentXid = xid;
                    this.currentXAResource = toSwitchTo.getXAResource();
                }
            } catch (SQLException sqlEx) {
                throw new XAException();
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public XAResource getXAResource() throws SQLException {
        return this;
    }

    @Override
    public void commit(Xid xid, boolean arg1) throws XAException {
        switchToXid(xid);
        this.currentXAResource.commit(xid, arg1);
        removeXAConnectionMapping(xid);
    }

    @Override
    public void end(Xid xid, int arg1) throws XAException {
        switchToXid(xid);
        this.currentXAResource.end(xid, arg1);
    }

    @Override
    public void forget(Xid xid) throws XAException {
        switchToXid(xid);
        this.currentXAResource.forget(xid);
        // remove?
        removeXAConnectionMapping(xid);
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return 0;
    }

    @Override
    public boolean isSameRM(XAResource xaRes) throws XAException {
        return xaRes == this;
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        switchToXid(xid);
        return this.currentXAResource.prepare(xid);
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        return MysqlXAConnection.recover(this.underlyingConnection, flag);
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        switchToXid(xid);
        this.currentXAResource.rollback(xid);
        removeXAConnectionMapping(xid);
    }

    @Override
    public boolean setTransactionTimeout(int arg0) throws XAException {
        return false;
    }

    @Override
    public void start(Xid xid, int arg1) throws XAException {
        switchToXid(xid);

        if (arg1 != XAResource.TMJOIN) {
            this.currentXAResource.start(xid, arg1);

            return;
        }

        //
        // Emulate join, by using resume on the same physical connection
        //

        this.currentXAResource.start(xid, XAResource.TMRESUME);
    }

    @Override
    public java.sql.Connection getConnection() throws SQLException {
        this.lock.lock();
        try {
            if (this.currentXAConnection == null) {
                return getConnection(false, true);
            }

            return this.currentXAConnection.getConnection();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void close() throws SQLException {
        if (this.currentXAConnection == null) {
            super.close();
        } else {
            removeXAConnectionMapping(this.currentXid);
            this.currentXAConnection.close();
        }
    }

}
