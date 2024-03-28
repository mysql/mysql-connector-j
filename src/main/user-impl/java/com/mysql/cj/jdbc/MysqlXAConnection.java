/*
 * Copyright (c) 2005, 2024, Oracle and/or its affiliates.
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.mysql.cj.Messages;
import com.mysql.cj.log.Log;
import com.mysql.cj.util.StringUtils;

public class MysqlXAConnection extends MysqlPooledConnection implements XAConnection, XAResource {

    private static final int MAX_COMMAND_LENGTH = 300;

    private com.mysql.cj.jdbc.JdbcConnection underlyingConnection;

    private final static Map<Integer, Integer> MYSQL_ERROR_CODES_TO_XA_ERROR_CODES;

    private Log log;

    protected boolean logXaCommands;

    static {
        HashMap<Integer, Integer> temp = new HashMap<>();

        temp.put(1397, XAException.XAER_NOTA);
        temp.put(1398, XAException.XAER_INVAL);
        temp.put(1399, XAException.XAER_RMFAIL);
        temp.put(1400, XAException.XAER_OUTSIDE);
        temp.put(1401, XAException.XAER_RMERR);
        temp.put(1402, XAException.XA_RBROLLBACK);
        temp.put(1440, XAException.XAER_DUPID);
        temp.put(1613, XAException.XA_RBTIMEOUT);
        temp.put(1614, XAException.XA_RBDEADLOCK);

        MYSQL_ERROR_CODES_TO_XA_ERROR_CODES = Collections.unmodifiableMap(temp);
    }

    protected static MysqlXAConnection getInstance(JdbcConnection mysqlConnection, boolean logXaCommands) throws SQLException {
        return new MysqlXAConnection(mysqlConnection, logXaCommands);
    }

    public MysqlXAConnection(JdbcConnection connection, boolean logXaCommands) {
        super(connection);
        this.underlyingConnection = connection;
        this.log = connection.getSession().getLog();
        this.logXaCommands = logXaCommands;
    }

    @Override
    public XAResource getXAResource() throws SQLException {
        return this;
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return 0;
    }

    @Override
    public boolean setTransactionTimeout(int arg0) throws XAException {
        return false;
    }

    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
        if (xares instanceof MysqlXAConnection) {
            return this.underlyingConnection.isSameResource(((MysqlXAConnection) xares).underlyingConnection);
        }

        return false;
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        return recover(this.underlyingConnection, flag);
    }

    protected static Xid[] recover(Connection c, int flag) throws XAException {
        /*
         * The XA RECOVER statement returns information for those XA transactions on the MySQL server that are in the PREPARED state. (See Section 13.4.7.2, "XA
         * Transaction States".) The output includes a row for each such XA transaction on the server, regardless of which client started it.
         *
         * XA RECOVER output rows look like this (for an example xid value consisting of the parts 'abc', 'def', and 7):
         *
         * mysql> XA RECOVER;
         * +----------+--------------+--------------+--------+
         * | formatID | gtrid_length | bqual_length | data |
         * +----------+--------------+--------------+--------+
         * | 7 | 3 | 3 | abcdef |
         * +----------+--------------+--------------+--------+
         *
         * The output columns have the following meanings:
         *
         * formatID is the formatID part of the transaction xid
         * gtrid_length is the length in bytes of the gtrid part of the xid
         * bqual_length is the length in bytes of the bqual part of the xid
         * data is the concatenation of the gtrid and bqual parts of the xid
         */

        boolean startRscan = (flag & TMSTARTRSCAN) > 0;
        boolean endRscan = (flag & TMENDRSCAN) > 0;

        if (!startRscan && !endRscan && flag != TMNOFLAGS) {
            throw new MysqlXAException(XAException.XAER_INVAL, Messages.getString("MysqlXAConnection.001"), null);
        }

        //
        // We return all recovered XIDs at once, so if not  TMSTARTRSCAN, return no new XIDs
        //
        // We don't attempt to maintain state to check for TMNOFLAGS "outside" of a scan
        //

        if (!startRscan) {
            return new Xid[0];
        }

        ResultSet rs = null;
        Statement stmt = null;

        List<MysqlXid> recoveredXidList = new ArrayList<>();

        try {
            // TODO: Cache this for lifetime of XAConnection
            stmt = c.createStatement();

            rs = stmt.executeQuery("XA RECOVER");

            while (rs.next()) {
                final int formatId = rs.getInt(1);
                int gtridLength = rs.getInt(2);
                int bqualLength = rs.getInt(3);
                byte[] gtridAndBqual = rs.getBytes(4);

                final byte[] gtrid = new byte[gtridLength];
                final byte[] bqual = new byte[bqualLength];

                if (gtridAndBqual.length != gtridLength + bqualLength) {
                    throw new MysqlXAException(XAException.XA_RBPROTO, Messages.getString("MysqlXAConnection.002"), null);
                }

                System.arraycopy(gtridAndBqual, 0, gtrid, 0, gtridLength);
                System.arraycopy(gtridAndBqual, gtridLength, bqual, 0, bqualLength);

                recoveredXidList.add(new MysqlXid(gtrid, bqual, formatId));
            }
        } catch (SQLException sqlEx) {
            throw mapXAExceptionFromSQLException(sqlEx);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                    throw mapXAExceptionFromSQLException(sqlEx);
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                    throw mapXAExceptionFromSQLException(sqlEx);
                }
            }
        }

        int numXids = recoveredXidList.size();

        Xid[] asXids = new Xid[numXids];
        Object[] asObjects = recoveredXidList.toArray();

        for (int i = 0; i < numXids; i++) {
            asXids[i] = (Xid) asObjects[i];
        }

        return asXids;
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
        commandBuf.append("XA PREPARE ");
        appendXid(commandBuf, xid);

        dispatchCommand(commandBuf.toString());

        return XA_OK; // TODO: Check for read-only
    }

    @Override
    public void forget(Xid xid) throws XAException {
        // mysql doesn't support this
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
        commandBuf.append("XA ROLLBACK ");
        appendXid(commandBuf, xid);

        try {
            dispatchCommand(commandBuf.toString());
        } finally {
            this.underlyingConnection.setInGlobalTx(false);
        }
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
        StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
        commandBuf.append("XA END ");
        appendXid(commandBuf, xid);

        switch (flags) {
            case TMSUCCESS:
                break; // no-op
            case TMSUSPEND:
                commandBuf.append(" SUSPEND");
                break;
            case TMFAIL:
                break; // no-op
            default:
                throw new XAException(XAException.XAER_INVAL);
        }

        dispatchCommand(commandBuf.toString());
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
        StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
        commandBuf.append("XA START ");
        appendXid(commandBuf, xid);

        switch (flags) {
            case TMJOIN:
                commandBuf.append(" JOIN");
                break;
            case TMRESUME:
                commandBuf.append(" RESUME");
                break;
            case TMNOFLAGS:
                // no-op
                break;
            default:
                throw new XAException(XAException.XAER_INVAL);
        }

        dispatchCommand(commandBuf.toString());

        this.underlyingConnection.setInGlobalTx(true);
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
        commandBuf.append("XA COMMIT ");
        appendXid(commandBuf, xid);

        if (onePhase) {
            commandBuf.append(" ONE PHASE");
        }

        try {
            dispatchCommand(commandBuf.toString());
        } finally {
            this.underlyingConnection.setInGlobalTx(false);
        }
    }

    private ResultSet dispatchCommand(String command) throws XAException {
        Statement stmt = null;

        try {
            if (this.logXaCommands) {
                this.log.logDebug("Executing XA statement: " + command);
            }

            // TODO: Cache this for lifetime of XAConnection
            stmt = this.underlyingConnection.createStatement();

            stmt.execute(command);

            ResultSet rs = stmt.getResultSet();

            return rs;
        } catch (SQLException sqlEx) {
            throw mapXAExceptionFromSQLException(sqlEx);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                }
            }
        }
    }

    protected static XAException mapXAExceptionFromSQLException(SQLException sqlEx) {
        Integer xaCode = MYSQL_ERROR_CODES_TO_XA_ERROR_CODES.get(sqlEx.getErrorCode());

        if (xaCode != null) {
            return (XAException) new MysqlXAException(xaCode.intValue(), sqlEx.getMessage(), null).initCause(sqlEx);
        }

        return (XAException) new MysqlXAException(XAException.XAER_RMFAIL, Messages.getString("MysqlXAConnection.003"), null).initCause(sqlEx);
    }

    private static void appendXid(StringBuilder builder, Xid xid) {
        byte[] gtrid = xid.getGlobalTransactionId();
        byte[] btrid = xid.getBranchQualifier();

        if (gtrid != null) {
            StringUtils.appendAsHex(builder, gtrid);
        }

        builder.append(',');
        if (btrid != null) {
            StringUtils.appendAsHex(builder, btrid);
        }

        builder.append(',');
        StringUtils.appendAsHex(builder, xid.getFormatId());
    }

    @Override
    public Connection getConnection() throws SQLException {
        this.lock.lock();
        try {
            Connection connToWrap = getConnection(false, true);

            return connToWrap;
        } finally {
            this.lock.unlock();
        }
    }

}
