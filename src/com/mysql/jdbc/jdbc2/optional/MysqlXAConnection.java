/*
  Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.

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

import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.Util;
import com.mysql.jdbc.log.Log;

/*
 * XA BEGIN <xid> [JOIN | RESUME] XA START TRANSACTION <xid> [JOIN | RESUME] XA
 * COMMIT <xid> [ONE PHASE] XA END <xid> [SUSPEND [FOR MIGRATE]] XA PREPARE
 * <xid> XA RECOVER XA ROLLBACK <xid>
 */

/**
 * An object that provides support for distributed transactions. An
 * <code>XAConnection</code> object may be enlisted in a distributed
 * transaction by means of an <code>XAResource</code> object. A transaction
 * manager, usually part of a middle tier server, manages an
 * <code>XAConnection</code> object through the <code>XAResource</code>
 * object.
 * <P>
 * An application programmer does not use this interface directly; rather, it is
 * used by a transaction manager working in the middle tier server.
 * 
 * @since 1.4
 */
public class MysqlXAConnection extends MysqlPooledConnection implements
		XAConnection, XAResource {

	private static final int MAX_COMMAND_LENGTH = 300;
	
	private com.mysql.jdbc.ConnectionImpl underlyingConnection;

	private final static Map<Integer, Integer> MYSQL_ERROR_CODES_TO_XA_ERROR_CODES;

	private Log log;

	protected boolean logXaCommands;
	
	static {
		HashMap<Integer, Integer> temp = new HashMap<Integer, Integer>();

		temp.put(Integer.valueOf(1397), Integer.valueOf(XAException.XAER_NOTA));
		temp.put(Integer.valueOf(1398), Integer.valueOf(XAException.XAER_INVAL));
		temp.put(Integer.valueOf(1399), Integer.valueOf(XAException.XAER_RMFAIL));
		temp.put(Integer.valueOf(1400), Integer.valueOf(XAException.XAER_OUTSIDE));
		temp.put(Integer.valueOf(1401), Integer.valueOf(XAException.XAER_RMERR));
		temp.put(Integer.valueOf(1402), Integer.valueOf(XAException.XA_RBROLLBACK));
		temp.put(Integer.valueOf(1440), Integer.valueOf(XAException.XAER_DUPID));

		MYSQL_ERROR_CODES_TO_XA_ERROR_CODES = Collections.unmodifiableMap(temp);
	}
	
	private static final Constructor<?> JDBC_4_XA_CONNECTION_WRAPPER_CTOR;

	static {
		if (Util.isJdbc4()) {
			try {
				JDBC_4_XA_CONNECTION_WRAPPER_CTOR = Class.forName(
						"com.mysql.jdbc.jdbc2.optional.JDBC4MysqlXAConnection")
						.getConstructor(
								new Class[] { ConnectionImpl.class, Boolean.TYPE });
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

	protected static MysqlXAConnection getInstance(ConnectionImpl mysqlConnection, 
			boolean logXaCommands) throws SQLException {
		if (!Util.isJdbc4()) {
			return new MysqlXAConnection(mysqlConnection, logXaCommands);
		}

		return (MysqlXAConnection) Util.handleNewInstance(
				JDBC_4_XA_CONNECTION_WRAPPER_CTOR, new Object[] {
						mysqlConnection,
						Boolean.valueOf(logXaCommands) }, mysqlConnection.getExceptionInterceptor());
	}

	/**
	 * @param connection
	 */
	public MysqlXAConnection(ConnectionImpl connection, boolean logXaCommands)
			throws SQLException {
		super(connection);
		this.underlyingConnection = connection;
		this.log = connection.getLog();
		this.logXaCommands = logXaCommands;
	}

	/**
	 * Retrieves an <code>XAResource</code> object that the transaction
	 * manager will use to manage this <code>XAConnection</code> object's
	 * participation in a distributed transaction.
	 * 
	 * @return the <code>XAResource</code> object
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public XAResource getXAResource() throws SQLException {
		return this;
	}

	/**
	 * Obtains the current transaction timeout value set for this XAResource
	 * instance. If XAResource.setTransactionTimeout was not used prior to
	 * invoking this method, the return value is the default timeout set for the
	 * resource manager; otherwise, the value used in the previous
	 * setTransactionTimeout call is returned.
	 * 
	 * @return the transaction timeout value in seconds.
	 * 
	 * @throws XAException
	 *             An error has occurred. Possible exception values are
	 *             XAER_RMERR and XAER_RMFAIL.
	 */
	public int getTransactionTimeout() throws XAException {
		return 0;
	}

	/**
	 * Sets the current transaction timeout value for this XAResource instance.
	 * Once set, this timeout value is effective until setTransactionTimeout is
	 * invoked again with a different value.
	 * 
	 * To reset the timeout value to the default value used by the resource
	 * manager, set the value to zero. If the timeout operation is performed
	 * successfully, the method returns true; otherwise false.
	 * 
	 * If a resource manager does not support explicitly setting the transaction
	 * timeout value, this method returns false.
	 * 
	 * @parameter seconds The transaction timeout value in seconds.
	 * 
	 * @return true if the transaction timeout value is set successfully;
	 *         otherwise false.
	 * 
	 * @throws XAException
	 *             An error has occurred. Possible exception values are
	 *             XAER_RMERR, XAER_RMFAIL, or XAER_INVAL.
	 */
	public boolean setTransactionTimeout(int arg0) throws XAException {
		return false;
	}

	/**
	 * This method is called to determine if the resource manager instance
	 * represented by the target object is the same as the resouce manager
	 * instance represented by the parameter xares.
	 * 
	 * @parameter xares An XAResource object whose resource manager instance is
	 *            to be compared with the resource manager instance of the
	 *            target object.
	 * 
	 * @return true if it's the same RM instance; otherwise false.
	 * 
	 * @throws XAException
	 *             An error has occurred. Possible exception values are
	 *             XAER_RMERR and XAER_RMFAIL.
	 */
	public boolean isSameRM(XAResource xares) throws XAException {

		if (xares instanceof MysqlXAConnection) {
			return this.underlyingConnection
					.isSameResource(((MysqlXAConnection) xares).underlyingConnection);
		}

		return false;
	}

	/**
	 * This method is called to obtain a list of prepared transaction branches
	 * from a resource manager. The transaction manager calls this method during
	 * recovery to obtain the list of transaction branches that are currently in
	 * prepared or heuristically completed states. 
	 * 
	 * The flag parameter indicates where the recover scan should start or end, 
	 * or start and end. This method may be invoked one or more times during a 
	 * recovery scan. The resource manager maintains a cursor which marks the 
	 * current position of the prepared or heuristically completed transaction list. 
	 * Each invocation of the recover method moves the cursor passed the set of Xids 
	 * that are returned. 
	 * 
	 * Two consecutive invocation of this method that starts from the
	 * beginning of the list must return the same list of transaction branches
	 * unless one of the following takes place: 
	 * 
	 * - the transaction manager invokes the commit, forget, prepare, or rollback method for that resource
	 * manager, between the two consecutive invocation of the recovery scan. 
	 * 
	 * - the resource manager heuristically completes some transaction branches
	 * between the two invocation of the recovery scan.
	 * 
	 * @param flag
	 *            One of TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS. TMNOFLAGS must be
	 *            used when no other flags are set in the parameter.
	 * 
	 * @returns The resource manager returns zero or more XIDs of the
	 *          transaction branches that are currently in a prepared or
	 *          heuristically completed state. If an error occurs during the
	 *          operation, the resource manager should throw the appropriate
	 *          XAException.
	 * 
	 * @throws XAException
	 *             An error has occurred. Possible values are XAER_RMERR,
	 *             XAER_RMFAIL, XAER_INVAL, and XAER_PROTO.
	 */
	public Xid[] recover(int flag) throws XAException {
		return recover(this.underlyingConnection, flag);
	}
	
	protected static Xid[] recover(Connection c, int flag) throws XAException {
		/*
		    The XA RECOVER statement returns information for those XA transactions on the MySQL server that are in the PREPARED state. (See Section 13.4.7.2, �XA Transaction States�.) The output includes a row for each such XA transaction on the server, regardless of which client started it.

			XA RECOVER output rows look like this (for an example xid value consisting of the parts 'abc', 'def', and 7):

			mysql> XA RECOVER;
			+----------+--------------+--------------+--------+
			| formatID | gtrid_length | bqual_length | data   |
			+----------+--------------+--------------+--------+
			|        7 |            3 |            3 | abcdef |
			+----------+--------------+--------------+--------+

			The output columns have the following meanings:

      			formatID is the formatID part of the transaction xid
			    gtrid_length is the length in bytes of the gtrid part of the xid
			    bqual_length is the length in bytes of the bqual part of the xid
 			    data is the concatenation of the gtrid and bqual parts of the xid
		 */
		
		boolean startRscan = ((flag & TMSTARTRSCAN) > 0);
		boolean endRscan = ((flag & TMENDRSCAN) > 0);
		
		if (!startRscan && !endRscan && flag != TMNOFLAGS) {
			throw new MysqlXAException(XAException.XAER_INVAL, 
					"Invalid flag, must use TMNOFLAGS, or any combination of TMSTARTRSCAN and TMENDRSCAN",
					null);
		}

		//
		// We return all recovered XIDs at once, so if not 
		// TMSTARTRSCAN, return no new XIDs
		//
		// We don't attempt to maintain state to check for TMNOFLAGS
		// "outside" of a scan
		//
		
		if (!startRscan) {
			return new Xid[0];
		}
		
		ResultSet rs = null;
		Statement stmt = null;

		List<MysqlXid> recoveredXidList = new ArrayList<MysqlXid>();

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

				if (gtridAndBqual.length != (gtridLength + bqualLength)) {
					throw new MysqlXAException(XAException.XA_RBPROTO,
							"Error while recovering XIDs from RM. GTRID and BQUAL are wrong sizes", 
							null);
				}

				System.arraycopy(gtridAndBqual, 0, gtrid, 0,
						gtridLength);
				System.arraycopy(gtridAndBqual, gtridLength, bqual, 0,
						bqualLength);

				recoveredXidList.add(new MysqlXid(gtrid, bqual, 
						formatId));
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

	/**
	 * Asks the resource manager to prepare for a transaction commit of the
	 * transaction specified in xid.
	 * 
	 * @parameter xid A global transaction identifier.
	 * 
	 * @returns A value indicating the resource manager's vote on the outcome of
	 *          the transaction.
	 * 
	 * The possible values are: XA_RDONLY or XA_OK. If the resource manager
	 * wants to roll back the transaction, it should do so by raising an
	 * appropriate XAException in the prepare method.
	 * 
	 * @throws XAException
	 *             An error has occurred. Possible exception values are: XA_RB*,
	 *             XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or
	 *             XAER_PROTO.
	 */
	public int prepare(Xid xid) throws XAException {
		StringBuilder commandBuf = new StringBuilder(MAX_COMMAND_LENGTH);
		commandBuf.append("XA PREPARE ");
		appendXid(commandBuf, xid);

		dispatchCommand(commandBuf.toString());

		return XA_OK; // TODO: Check for read-only
	}

	/**
	 * Tells the resource manager to forget about a heuristically completed
	 * transaction branch.
	 * 
	 * @parameter xid A global transaction identifier.
	 * 
	 * @throws XAException
	 *             An error has occurred. Possible exception values are
	 *             XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or
	 *             XAER_PROTO.
	 */
	public void forget(Xid xid) throws XAException {
		// mysql doesn't support this
	}

	/**
	 * Informs the resource manager to roll back work done on behalf of a
	 * transaction branch.
	 * 
	 * @parameter xid A global transaction identifier.
	 * 
	 * @throws XAException
	 *             An error has occurred. Possible XAExceptions are XA_HEURHAZ,
	 *             XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR, XAER_RMFAIL,
	 *             XAER_NOTA, XAER_INVAL, or XAER_PROTO.
	 * 
	 * If the transaction branch is already marked rollback-only the resource
	 * manager may throw one of the XA_RB* exceptions.
	 * 
	 * Upon return, the resource manager has rolled back the branch's work and
	 * has released all held resources.
	 */
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

	/**
	 * Ends the work performed on behalf of a transaction branch.
	 * 
	 * The resource manager disassociates the XA resource from the transaction
	 * branch specified and lets the transaction complete.
	 * 
	 * If TMSUSPEND is specified in the flags, the transaction branch is
	 * temporarily suspended in an incomplete state. The transaction context is
	 * in a suspended state and must be resumed via the start method with
	 * TMRESUME specified.
	 * 
	 * If TMFAIL is specified, the portion of work has failed. The resource
	 * manager may mark the transaction as rollback-only
	 * 
	 * If TMSUCCESS is specified, the portion of work has completed
	 * successfully.
	 * 
	 * @parameter xid A global transaction identifier that is the same as the
	 *            identifier used previously in the start method.
	 * 
	 * @parameter flags One of TMSUCCESS, TMFAIL, or TMSUSPEND.
	 * 
	 * @throws XAException -
	 *             An error has occurred. Possible XAException values are
	 *             XAER_RMERR, XAER_RMFAIL, XAER_NOTA, XAER_INVAL, XAER_PROTO,
	 *             or XA_RB*.
	 */
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

	/**
	 * Starts work on behalf of a transaction branch specified in xid.
	 * 
	 * If TMJOIN is specified, the start applies to joining a transaction
	 * previously seen by the resource manager.
	 * 
	 * If TMRESUME is specified, the start applies to resuming a suspended
	 * transaction specified in the parameter xid.
	 * 
	 * If neither TMJOIN nor TMRESUME is specified and the transaction specified
	 * by xid has previously been seen by the resource manager, the resource
	 * manager throws the XAException exception with XAER_DUPID error code.
	 * 
	 * @parameter xid A global transaction identifier to be associated with the
	 *            resource.
	 * 
	 * @parameter flags One of TMNOFLAGS, TMJOIN, or TMRESUME.
	 * 
	 * @throws XAException
	 *             An error has occurred. Possible exceptions are XA_RB*,
	 *             XAER_RMERR, XAER_RMFAIL, XAER_DUPID, XAER_OUTSIDE, XAER_NOTA,
	 *             XAER_INVAL, or XAER_PROTO.
	 */
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

	/**
	 * Commits the global transaction specified by xid.
	 * 
	 * @parameter xid A global transaction identifier
	 * @parameter onePhase - If true, the resource manager should use a
	 *            one-phase commit protocol to commit the work done on behalf of
	 *            xid.
	 * 
	 * @throws XAException
	 *             An error has occurred. Possible XAExceptions are XA_HEURHAZ,
	 *             XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR, XAER_RMFAIL,
	 *             XAER_NOTA, XAER_INVAL, or XAER_PROTO.
	 * 
	 * If the resource manager did not commit the transaction and the parameter
	 * onePhase is set to true, the resource manager may throw one of the XA_RB*
	 * exceptions.
	 * 
	 * Upon return, the resource manager has rolled back the branch's work and
	 * has released all held resources.
	 */

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

		Integer xaCode = MYSQL_ERROR_CODES_TO_XA_ERROR_CODES
				.get(Integer.valueOf(sqlEx.getErrorCode()));

		if (xaCode != null) {
			return new MysqlXAException(xaCode.intValue(), sqlEx.getMessage(), null);
		}

		// Punt? We don't know what the error code is here
		return new MysqlXAException(sqlEx.getMessage(), null);
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see javax.sql.PooledConnection#getConnection()
	 */
	public synchronized Connection getConnection() throws SQLException {
		Connection connToWrap = getConnection(false, true);
		
		return connToWrap;
	}
}
