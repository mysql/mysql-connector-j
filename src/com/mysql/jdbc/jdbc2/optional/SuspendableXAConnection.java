/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc.jdbc2.optional;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Util;

public class SuspendableXAConnection extends MysqlPooledConnection implements XAConnection, XAResource {

    private static final Constructor<?> JDBC_4_XA_CONNECTION_WRAPPER_CTOR;

    static {
        if (Util.isJdbc4()) {
            try {
                JDBC_4_XA_CONNECTION_WRAPPER_CTOR = Class.forName("com.mysql.jdbc.jdbc2.optional.JDBC4SuspendableXAConnection").getConstructor(
                        new Class[] { Connection.class });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            JDBC_4_XA_CONNECTION_WRAPPER_CTOR = null;
        }
    }

    protected static SuspendableXAConnection getInstance(Connection mysqlConnection) throws SQLException {
        if (!Util.isJdbc4()) {
            return new SuspendableXAConnection(mysqlConnection);
        }

        return (SuspendableXAConnection) Util.handleNewInstance(JDBC_4_XA_CONNECTION_WRAPPER_CTOR, new Object[] { mysqlConnection },
                mysqlConnection.getExceptionInterceptor());
    }

    public SuspendableXAConnection(Connection connection) {
        super(connection);
        this.underlyingConnection = connection;
    }

    private static final Map<Xid, XAConnection> XIDS_TO_PHYSICAL_CONNECTIONS = new HashMap<Xid, XAConnection>();

    private Xid currentXid;

    private XAConnection currentXAConnection;
    private XAResource currentXAResource;

    private Connection underlyingConnection;

    private static synchronized XAConnection findConnectionForXid(Connection connectionToWrap, Xid xid) throws SQLException {
        // TODO: check for same GTRID, but different BQUALs...MySQL doesn't allow this yet

        // Note, we don't need to check for XIDs here, because MySQL itself will complain with a XAER_NOTA if need be.

        XAConnection conn = XIDS_TO_PHYSICAL_CONNECTIONS.get(xid);

        if (conn == null) {
            conn = new MysqlXAConnection(connectionToWrap, connectionToWrap.getLogXaCommands());
            XIDS_TO_PHYSICAL_CONNECTIONS.put(xid, conn);
        }

        return conn;
    }

    private static synchronized void removeXAConnectionMapping(Xid xid) {
        XIDS_TO_PHYSICAL_CONNECTIONS.remove(xid);
    }

    private synchronized void switchToXid(Xid xid) throws XAException {
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
    }

    public XAResource getXAResource() throws SQLException {
        return this;
    }

    public void commit(Xid xid, boolean arg1) throws XAException {
        switchToXid(xid);
        this.currentXAResource.commit(xid, arg1);
        removeXAConnectionMapping(xid);
    }

    public void end(Xid xid, int arg1) throws XAException {
        switchToXid(xid);
        this.currentXAResource.end(xid, arg1);
    }

    public void forget(Xid xid) throws XAException {
        switchToXid(xid);
        this.currentXAResource.forget(xid);
        // remove?
        removeXAConnectionMapping(xid);
    }

    public int getTransactionTimeout() throws XAException {
        return 0;
    }

    public boolean isSameRM(XAResource xaRes) throws XAException {
        return xaRes == this;
    }

    public int prepare(Xid xid) throws XAException {
        switchToXid(xid);
        return this.currentXAResource.prepare(xid);
    }

    public Xid[] recover(int flag) throws XAException {
        return MysqlXAConnection.recover(this.underlyingConnection, flag);
    }

    public void rollback(Xid xid) throws XAException {
        switchToXid(xid);
        this.currentXAResource.rollback(xid);
        removeXAConnectionMapping(xid);
    }

    public boolean setTransactionTimeout(int arg0) throws XAException {
        return false;
    }

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
    public synchronized java.sql.Connection getConnection() throws SQLException {
        if (this.currentXAConnection == null) {
            return getConnection(false, true);
        }

        return this.currentXAConnection.getConnection();
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
