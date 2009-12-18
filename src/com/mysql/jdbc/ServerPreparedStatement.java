/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package com.mysql.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.mysql.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.jdbc.profiler.ProfilerEvent;
import com.mysql.jdbc.profiler.ProfilerEventHandlerFactory;

/**
 * JDBC Interface for MySQL-4.1 and newer server-side PreparedStatements.
 * 
 * @author Mark Matthews
 * @version $Id: ServerPreparedStatement.java,v 1.1.2.2 2005/05/17 14:58:56
 *          mmatthews Exp $
 */
public class ServerPreparedStatement extends PreparedStatement {
	private static final Constructor JDBC_4_SPS_CTOR;
	
	static {
		if (Util.isJdbc4()) {
			try {
				JDBC_4_SPS_CTOR = Class.forName("com.mysql.jdbc.JDBC4ServerPreparedStatement")
				.getConstructor(
				new Class[] { ConnectionImpl.class, String.class, String.class,
						Integer.TYPE, Integer.TYPE});
			} catch (SecurityException e) {
				throw new RuntimeException(e);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			} 
		} else {
			JDBC_4_SPS_CTOR = null;
		}
	}
	
	protected static final int BLOB_STREAM_READ_BUF_SIZE = 8192;

	static class BatchedBindValues {
		BindValue[] batchedParameterValues;

		BatchedBindValues(BindValue[] paramVals) {
			int numParams = paramVals.length;

			this.batchedParameterValues = new BindValue[numParams];

			for (int i = 0; i < numParams; i++) {
				this.batchedParameterValues[i] = new BindValue(paramVals[i]);
			}
		}
	}

	public static class BindValue {

		long boundBeforeExecutionNum = 0;
		
		public long bindLength; /* Default length of data */

		int bufferType; /* buffer type */

		byte byteBinding;

		double doubleBinding;

		float floatBinding;

		int intBinding;

		public boolean isLongData; /* long data indicator */

		public boolean isNull; /* NULL indicator */

		boolean isSet = false; /* has this parameter been set? */

		long longBinding;

		short shortBinding;

		public Object value; /* The value to store */

		BindValue() {
		}

		BindValue(BindValue copyMe) {
			this.value = copyMe.value;
			this.isSet = copyMe.isSet;
			this.isLongData = copyMe.isLongData;
			this.isNull = copyMe.isNull;
			this.bufferType = copyMe.bufferType;
			this.bindLength = copyMe.bindLength;
			this.byteBinding = copyMe.byteBinding;
			this.shortBinding = copyMe.shortBinding;
			this.intBinding = copyMe.intBinding;
			this.longBinding = copyMe.longBinding;
			this.floatBinding = copyMe.floatBinding;
			this.doubleBinding = copyMe.doubleBinding;
		}

		void reset() {
			this.isSet = false;
			this.value = null;
			this.isLongData = false;

			this.byteBinding = 0;
			this.shortBinding = 0;
			this.intBinding = 0;
			this.longBinding = 0L;
			this.floatBinding = 0;
			this.doubleBinding = 0D;
		}

		public String toString() {
			return toString(false);
		}

		public String toString(boolean quoteIfNeeded) {
			if (this.isLongData) {
				return "' STREAM DATA '";
			}

			switch (this.bufferType) {
			case MysqlDefs.FIELD_TYPE_TINY:
				return String.valueOf(byteBinding);
			case MysqlDefs.FIELD_TYPE_SHORT:
				return String.valueOf(shortBinding);
			case MysqlDefs.FIELD_TYPE_LONG:
				return String.valueOf(intBinding);
			case MysqlDefs.FIELD_TYPE_LONGLONG:
				return String.valueOf(longBinding);
			case MysqlDefs.FIELD_TYPE_FLOAT:
				return String.valueOf(floatBinding);
			case MysqlDefs.FIELD_TYPE_DOUBLE:
				return String.valueOf(doubleBinding);
			case MysqlDefs.FIELD_TYPE_TIME:
			case MysqlDefs.FIELD_TYPE_DATE:
			case MysqlDefs.FIELD_TYPE_DATETIME:
			case MysqlDefs.FIELD_TYPE_TIMESTAMP:
			case MysqlDefs.FIELD_TYPE_VAR_STRING:
			case MysqlDefs.FIELD_TYPE_STRING:
			case MysqlDefs.FIELD_TYPE_VARCHAR:
				if (quoteIfNeeded) {
					return "'" + String.valueOf(value) + "'";
				} else {
					return String.valueOf(value);
				}
			default:
				if (value instanceof byte[]) {
					return "byte data";

				} else {
					if (quoteIfNeeded) {
						return "'" + String.valueOf(value) + "'";
					} else {
						return String.valueOf(value);
					}
				}
			}
		}
		
		long getBoundLength() {
			if (isNull) {
				return 0;
			}
			
			if (isLongData) {
				return bindLength;
			}

			switch (bufferType) {

			case MysqlDefs.FIELD_TYPE_TINY:
				return 1;
			case MysqlDefs.FIELD_TYPE_SHORT:
				return 2;
			case MysqlDefs.FIELD_TYPE_LONG:
				return 4;
			case MysqlDefs.FIELD_TYPE_LONGLONG:
				return 8;
			case MysqlDefs.FIELD_TYPE_FLOAT:
				return 4;
			case MysqlDefs.FIELD_TYPE_DOUBLE:
				return 8;
			case MysqlDefs.FIELD_TYPE_TIME:
				return 9;
			case MysqlDefs.FIELD_TYPE_DATE:
				return 7;
			case MysqlDefs.FIELD_TYPE_DATETIME:
			case MysqlDefs.FIELD_TYPE_TIMESTAMP:
				return 11;
			case MysqlDefs.FIELD_TYPE_VAR_STRING:
			case MysqlDefs.FIELD_TYPE_STRING:
			case MysqlDefs.FIELD_TYPE_VARCHAR:
			case MysqlDefs.FIELD_TYPE_DECIMAL:
			case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
				if (value instanceof byte[]) {
					return ((byte[]) value).length;
				} else {
					return ((String) value).length();
				}
			default: 
				return 0;
			}
		}
	}

	/* 1 (length) + 2 (year) + 1 (month) + 1 (day) */
	private static final byte MAX_DATE_REP_LENGTH = (byte) 5;

	/*
	 * 1 (length) + 2 (year) + 1 (month) + 1 (day) + 1 (hour) + 1 (minute) + 1
	 * (second) + 4 (microseconds)
	 */
	private static final byte MAX_DATETIME_REP_LENGTH = 12;

	/*
	 * 1 (length) + 1 (is negative) + 4 (day count) + 1 (hour) + 1 (minute) + 1
	 * (seconds) + 4 (microseconds)
	 */
	private static final byte MAX_TIME_REP_LENGTH = 13;
	
	private boolean hasOnDuplicateKeyUpdate = false;

	private void storeTime(Buffer intoBuf, Time tm) throws SQLException {
		
		intoBuf.ensureCapacity(9);
		intoBuf.writeByte((byte) 8); // length
		intoBuf.writeByte((byte) 0); // neg flag
		intoBuf.writeLong(0); // tm->day, not used

		Calendar sessionCalendar = getCalendarInstanceForSessionOrNew();
		
		synchronized (sessionCalendar) {
			java.util.Date oldTime = sessionCalendar.getTime();
			try {
				sessionCalendar.setTime(tm);
				intoBuf.writeByte((byte) sessionCalendar.get(Calendar.HOUR_OF_DAY));
				intoBuf.writeByte((byte) sessionCalendar.get(Calendar.MINUTE));
				intoBuf.writeByte((byte) sessionCalendar.get(Calendar.SECOND));

				// intoBuf.writeLongInt(0); // tm-second_part
			} finally {
				sessionCalendar.setTime(oldTime);
			}
		}
	}

	/**
	 * Flag indicating whether or not the long parameters have been 'switched'
	 * back to normal parameters. We can not execute() if clearParameters()
	 * hasn't been called in this case.
	 */
	private boolean detectedLongParameterSwitch = false;

	/**
	 * The number of fields in the result set (if any) for this
	 * PreparedStatement.
	 */
	private int fieldCount;

	/** Has this prepared statement been marked invalid? */
	private boolean invalid = false;

	/** If this statement has been marked invalid, what was the reason? */
	private SQLException invalidationException;

	/** Does this query modify data? */
	private boolean isSelectQuery;

	private Buffer outByteBuffer;

	/** Bind values for individual fields */
	private BindValue[] parameterBindings;

	/** Field-level metadata for parameters */
	private Field[] parameterFields;

	/** Field-level metadata for result sets. */
	private Field[] resultFields;

	/** Do we need to send/resend types to the server? */
	private boolean sendTypesToServer = false;

	/** The ID that the server uses to identify this PreparedStatement */
	private long serverStatementId;

	/** The type used for string bindings, changes from version-to-version */
	private int stringTypeCode = MysqlDefs.FIELD_TYPE_STRING;

	private boolean serverNeedsResetBeforeEachExecution;

	/**
	 * Creates a prepared statement instance -- We need to provide factory-style
	 * methods so we can support both JDBC3 (and older) and JDBC4 runtimes,
	 * otherwise the class verifier complains when it tries to load JDBC4-only
	 * interface classes that are present in JDBC4 method signatures.
	 */

	protected static ServerPreparedStatement getInstance(ConnectionImpl conn,
			String sql, String catalog, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		if (!Util.isJdbc4()) {
			return new ServerPreparedStatement(conn, sql, catalog,
					resultSetType, resultSetConcurrency);
		}

		try {
			return (ServerPreparedStatement) JDBC_4_SPS_CTOR.newInstance(new Object[] { conn,
					sql, catalog, Constants.integerValueOf(resultSetType),
					Constants.integerValueOf(resultSetConcurrency) });
		} catch (IllegalArgumentException e) {
			throw new SQLException(e.toString(), SQLError.SQL_STATE_GENERAL_ERROR);
		} catch (InstantiationException e) {
			throw new SQLException(e.toString(), SQLError.SQL_STATE_GENERAL_ERROR);
		} catch (IllegalAccessException e) {
			throw new SQLException(e.toString(), SQLError.SQL_STATE_GENERAL_ERROR);
		} catch (InvocationTargetException e) {
			Throwable target = e.getTargetException(); 
			
			if (target instanceof SQLException) {
				throw (SQLException)target;
			}
			
			throw new SQLException(target.toString(), SQLError.SQL_STATE_GENERAL_ERROR);
		}
	}

	/**
	 * Creates a new ServerPreparedStatement object.
	 * 
	 * @param conn
	 *            the connection creating us.
	 * @param sql
	 *            the SQL containing the statement to prepare.
	 * @param catalog
	 *            the catalog in use when we were created.
	 * 
	 * @throws SQLException
	 *             If an error occurs
	 */
	protected ServerPreparedStatement(ConnectionImpl conn, String sql, String catalog,
			int resultSetType, int resultSetConcurrency)
			throws SQLException {
		super(conn, catalog);

		checkNullOrEmptyQuery(sql);

		this.hasOnDuplicateKeyUpdate = containsOnDuplicateKeyInString(sql);
		
		int startOfStatement = findStartOfStatement(sql);
		
		this.firstCharOfStmt = StringUtils.firstAlphaCharUc(sql, startOfStatement);
		
		this.isSelectQuery = 'S' == this.firstCharOfStmt;
		
		if (this.connection.versionMeetsMinimum(5, 0, 0)) {
			this.serverNeedsResetBeforeEachExecution = 
				!this.connection.versionMeetsMinimum(5, 0, 3);
		} else {
			this.serverNeedsResetBeforeEachExecution =
				!this.connection.versionMeetsMinimum(4, 1, 10);
		}
		
		this.useAutoSlowLog = this.connection.getAutoSlowLog();
		this.useTrueBoolean = this.connection.versionMeetsMinimum(3, 21, 23);
		this.hasLimitClause = (StringUtils.indexOfIgnoreCase(sql, "LIMIT") != -1); //$NON-NLS-1$
		
		String statementComment = this.connection.getStatementComment();

		this.originalSql = (statementComment == null) ? sql : "/* "
				+ statementComment + " */ " + sql;

		if (this.connection.versionMeetsMinimum(4, 1, 2)) {
			this.stringTypeCode = MysqlDefs.FIELD_TYPE_VAR_STRING;
		} else {
			this.stringTypeCode = MysqlDefs.FIELD_TYPE_STRING;
		}

		try {
			serverPrepare(sql);
		} catch (SQLException sqlEx) {
			realClose(false, true);
			// don't wrap SQLExceptions
			throw sqlEx;
		} catch (Exception ex) {
			realClose(false, true);

			SQLException sqlEx = SQLError.createSQLException(ex.toString(),
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
			sqlEx.initCause(ex);
			
			throw sqlEx;
		}
		
		setResultSetType(resultSetType);
		setResultSetConcurrency(resultSetConcurrency);
		
		this.parameterTypes = new int[this.parameterCount];
	}

	/**
	 * JDBC 2.0 Add a set of parameters to the batch.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs.
	 * 
	 * @see StatementImpl#addBatch
	 */
	public synchronized void addBatch() throws SQLException {
		checkClosed();

		if (this.batchedArgs == null) {
			this.batchedArgs = new ArrayList();
		}

		this.batchedArgs.add(new BatchedBindValues(this.parameterBindings));
	}

	protected String asSql(boolean quoteStreamsAndUnknowns) throws SQLException {

		if (this.isClosed) {
			return "statement has been closed, no further internal information available";
		}
		
		PreparedStatement pStmtForSub = null;

		try {
			pStmtForSub = PreparedStatement.getInstance(this.connection,
					this.originalSql, this.currentCatalog);

			int numParameters = pStmtForSub.parameterCount;
			int ourNumParameters = this.parameterCount;

			for (int i = 0; (i < numParameters) && (i < ourNumParameters); i++) {
				if (this.parameterBindings[i] != null) {
					if (this.parameterBindings[i].isNull) {
						pStmtForSub.setNull(i + 1, Types.NULL);
					} else {
						BindValue bindValue = this.parameterBindings[i];

						//
						// Handle primitives first
						//
						switch (bindValue.bufferType) {

						case MysqlDefs.FIELD_TYPE_TINY:
							pStmtForSub.setByte(i + 1, bindValue.byteBinding);
							break;
						case MysqlDefs.FIELD_TYPE_SHORT:
							pStmtForSub.setShort(i + 1, bindValue.shortBinding);
							break;
						case MysqlDefs.FIELD_TYPE_LONG:
							pStmtForSub.setInt(i + 1, bindValue.intBinding);
							break;
						case MysqlDefs.FIELD_TYPE_LONGLONG:
							pStmtForSub.setLong(i + 1, bindValue.longBinding);
							break;
						case MysqlDefs.FIELD_TYPE_FLOAT:
							pStmtForSub.setFloat(i + 1, bindValue.floatBinding);
							break;
						case MysqlDefs.FIELD_TYPE_DOUBLE:
							pStmtForSub.setDouble(i + 1,
									bindValue.doubleBinding);
							break;
						default:
							pStmtForSub.setObject(i + 1,
									this.parameterBindings[i].value);
							break;
						}
					}
				}
			}

			return pStmtForSub.asSql(quoteStreamsAndUnknowns);
		} finally {
			if (pStmtForSub != null) {
				try {
					pStmtForSub.close();
				} catch (SQLException sqlEx) {
					; // ignore
				}
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.Statement#checkClosed()
	 */
	protected void checkClosed() throws SQLException {
		if (this.invalid) {
			throw this.invalidationException;
		}

		super.checkClosed();
	}

	/**
	 * @see java.sql.PreparedStatement#clearParameters()
	 */
	public void clearParameters() throws SQLException {
		checkClosed();
		clearParametersInternal(true);
	}

	private void clearParametersInternal(boolean clearServerParameters)
			throws SQLException {
		boolean hadLongData = false;

		if (this.parameterBindings != null) {
			for (int i = 0; i < this.parameterCount; i++) {
				if ((this.parameterBindings[i] != null)
						&& this.parameterBindings[i].isLongData) {
					hadLongData = true;
				}

				this.parameterBindings[i].reset();
			}
		}

		if (clearServerParameters && hadLongData) {
			serverResetStatement();

			this.detectedLongParameterSwitch = false;
		}
	}

	protected boolean isCached = false;

	private boolean useAutoSlowLog;

	private Calendar serverTzCalendar;

	private Calendar defaultTzCalendar;

	protected void setClosed(boolean flag) {
		this.isClosed = flag;
	}
	
	/**
	 * @see java.sql.Statement#close()
	 */
	public synchronized void close() throws SQLException {
		if (this.isCached && !this.isClosed) {
			clearParameters();
			
			this.isClosed = true;
			
			this.connection.recachePreparedStatement(this);
			return;
		}
		
		realClose(true, true);
	}

	private void dumpCloseForTestcase() {
		StringBuffer buf = new StringBuffer();
		this.connection.generateConnectionCommentBlock(buf);
		buf.append("DEALLOCATE PREPARE debug_stmt_");
		buf.append(this.statementId);
		buf.append(";\n");

		this.connection.dumpTestcaseQuery(buf.toString());
	}

	private void dumpExecuteForTestcase() throws SQLException {
		StringBuffer buf = new StringBuffer();

		for (int i = 0; i < this.parameterCount; i++) {
			this.connection.generateConnectionCommentBlock(buf);

			buf.append("SET @debug_stmt_param");
			buf.append(this.statementId);
			buf.append("_");
			buf.append(i);
			buf.append("=");

			if (this.parameterBindings[i].isNull) {
				buf.append("NULL");
			} else {
				buf.append(this.parameterBindings[i].toString(true));
			}

			buf.append(";\n");
		}

		this.connection.generateConnectionCommentBlock(buf);

		buf.append("EXECUTE debug_stmt_");
		buf.append(this.statementId);

		if (this.parameterCount > 0) {
			buf.append(" USING ");
			for (int i = 0; i < this.parameterCount; i++) {
				if (i > 0) {
					buf.append(", ");
				}

				buf.append("@debug_stmt_param");
				buf.append(this.statementId);
				buf.append("_");
				buf.append(i);

			}
		}

		buf.append(";\n");

		this.connection.dumpTestcaseQuery(buf.toString());
	}

	private void dumpPrepareForTestcase() throws SQLException {

		StringBuffer buf = new StringBuffer(this.originalSql.length() + 64);

		this.connection.generateConnectionCommentBlock(buf);

		buf.append("PREPARE debug_stmt_");
		buf.append(this.statementId);
		buf.append(" FROM \"");
		buf.append(this.originalSql);
		buf.append("\";\n");

		this.connection.dumpTestcaseQuery(buf.toString());
	}

	protected int[] executeBatchSerially(int batchTimeout) throws SQLException {
		ConnectionImpl locallyScopedConn = this.connection;
		
		if (locallyScopedConn == null) {
			checkClosed();
		}
		
		if (locallyScopedConn.isReadOnly()) {
			throw SQLError.createSQLException(Messages
					.getString("ServerPreparedStatement.2") //$NON-NLS-1$
					+ Messages.getString("ServerPreparedStatement.3"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
		}

		checkClosed();

		synchronized (locallyScopedConn.getMutex()) {
			clearWarnings();

			// Store this for later, we're going to 'swap' them out
			// as we execute each batched statement...
			BindValue[] oldBindValues = this.parameterBindings;

			try {
				int[] updateCounts = null;

				if (this.batchedArgs != null) {
					int nbrCommands = this.batchedArgs.size();
					updateCounts = new int[nbrCommands];

					if (this.retrieveGeneratedKeys) {
						this.batchedGeneratedKeys = new ArrayList(nbrCommands);
					}

					for (int i = 0; i < nbrCommands; i++) {
						updateCounts[i] = -3;
					}

					SQLException sqlEx = null;

					int commandIndex = 0;

					BindValue[] previousBindValuesForBatch = null;

					CancelTask timeoutTask = null;
					
					try {
						if (locallyScopedConn.getEnableQueryTimeouts() &&
								batchTimeout != 0
								&& locallyScopedConn.versionMeetsMinimum(5, 0, 0)) {
							timeoutTask = new CancelTask(this);
							locallyScopedConn.getCancelTimer().schedule(timeoutTask,
									batchTimeout);
						}
						
						for (commandIndex = 0; commandIndex < nbrCommands; commandIndex++) {
							Object arg = this.batchedArgs.get(commandIndex);
	
							if (arg instanceof String) {
								updateCounts[commandIndex] = executeUpdate((String) arg);
							} else {
								this.parameterBindings = ((BatchedBindValues) arg).batchedParameterValues;
	
								try {
									// We need to check types each time, as
									// the user might have bound different
									// types in each addBatch()
	
									if (previousBindValuesForBatch != null) {
										for (int j = 0; j < this.parameterBindings.length; j++) {
											if (this.parameterBindings[j].bufferType != previousBindValuesForBatch[j].bufferType) {
												this.sendTypesToServer = true;
	
												break;
											}
										}
									}
	
									try {
										updateCounts[commandIndex] = executeUpdate(false, true);
									} finally {
										previousBindValuesForBatch = this.parameterBindings;
									}
	
									if (this.retrieveGeneratedKeys) {
										java.sql.ResultSet rs = null;
	
										try {
											// we don't want to use our version,
											// because we've altered the behavior of
											// ours to support batch updates
											// (catch-22)
											// Ideally, what we need here is
											// super.super.getGeneratedKeys()
											// but that construct doesn't exist in
											// Java, so that's why there's
											// this kludge.
											rs = getGeneratedKeysInternal();
	
											while (rs.next()) {
												this.batchedGeneratedKeys
														.add(new ByteArrayRow(new byte[][] { rs
																.getBytes(1) }, getExceptionInterceptor()));
											}
										} finally {
											if (rs != null) {
												rs.close();
											}
										}
									}
								} catch (SQLException ex) {
									updateCounts[commandIndex] = EXECUTE_FAILED;
									
									if (this.continueBatchOnError && 
											!(ex instanceof MySQLTimeoutException) && 
											!(ex instanceof MySQLStatementCancelledException)
											&& !hasDeadlockOrTimeoutRolledBackTx(ex)) {
										sqlEx = ex;
									} else {
										int[] newUpdateCounts = new int[commandIndex];
										System.arraycopy(updateCounts, 0,
												newUpdateCounts, 0, commandIndex);
			
										throw new java.sql.BatchUpdateException(ex
												.getMessage(), ex.getSQLState(), ex
												.getErrorCode(), newUpdateCounts);
									}
								}
							}
						}
					} finally {
						if (timeoutTask != null) {
							timeoutTask.cancel();
						}
						
						resetCancelledState();
					}

					if (sqlEx != null) {
						throw new java.sql.BatchUpdateException(sqlEx
								.getMessage(), sqlEx.getSQLState(), sqlEx
								.getErrorCode(), updateCounts);
					}
				}

				return (updateCounts != null) ? updateCounts : new int[0];
			} finally {
				this.parameterBindings = oldBindValues;
				this.sendTypesToServer = true;

				clearBatch();
			}
		}
	}

	/**
	 * @see com.mysql.jdbc.PreparedStatement#executeInternal(int,
	 *      com.mysql.jdbc.Buffer, boolean, boolean)
	 */
	protected com.mysql.jdbc.ResultSetInternalMethods executeInternal(int maxRowsToRetrieve,
			Buffer sendPacket, boolean createStreamingResultSet,
			boolean queryIsSelectOnly, Field[] metadataFromCache,
			boolean isBatch)
			throws SQLException {
		this.numberOfExecutions++;

		// We defer to server-side execution
		try {
			return serverExecute(maxRowsToRetrieve, createStreamingResultSet, 
					metadataFromCache);
		} catch (SQLException sqlEx) {
			// don't wrap SQLExceptions
			if (this.connection.getEnablePacketDebug()) {
				this.connection.getIO().dumpPacketRingBuffer();
			}

			if (this.connection.getDumpQueriesOnException()) {
				String extractedSql = toString();
				StringBuffer messageBuf = new StringBuffer(extractedSql
						.length() + 32);
				messageBuf
						.append("\n\nQuery being executed when exception was thrown:\n\n");
				messageBuf.append(extractedSql);

				sqlEx = ConnectionImpl.appendMessageToException(sqlEx, messageBuf
						.toString(), getExceptionInterceptor());
			}

			throw sqlEx;
		} catch (Exception ex) {
			if (this.connection.getEnablePacketDebug()) {
				this.connection.getIO().dumpPacketRingBuffer();
			}

			SQLException sqlEx = SQLError.createSQLException(ex.toString(),
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());

			if (this.connection.getDumpQueriesOnException()) {
				String extractedSql = toString();
				StringBuffer messageBuf = new StringBuffer(extractedSql
						.length() + 32);
				messageBuf
						.append("\n\nQuery being executed when exception was thrown:\n\n");
				messageBuf.append(extractedSql);

				sqlEx = ConnectionImpl.appendMessageToException(sqlEx, messageBuf
						.toString(), getExceptionInterceptor());
			}

			sqlEx.initCause(ex);
			
			throw sqlEx;
		}
	}

	/**
	 * @see com.mysql.jdbc.PreparedStatement#fillSendPacket()
	 */
	protected Buffer fillSendPacket() throws SQLException {
		return null; // we don't use this type of packet
	}

	/**
	 * @see com.mysql.jdbc.PreparedStatement#fillSendPacket(byte,
	 *      java.io.InputStream, boolean, int)
	 */
	protected Buffer fillSendPacket(byte[][] batchedParameterStrings,
			InputStream[] batchedParameterStreams, boolean[] batchedIsStream,
			int[] batchedStreamLengths) throws SQLException {
		return null; // we don't use this type of packet
	}

	/**
	 * Returns the structure representing the value that (can be)/(is)
	 * bound at the given parameter index.
	 * 
	 * @param parameterIndex 1-based
	 * @param forLongData is this for a stream?
	 * @return
	 * @throws SQLException
	 */
	protected BindValue getBinding(int parameterIndex, boolean forLongData)
			throws SQLException {
		checkClosed();
		
		if (this.parameterBindings.length == 0) {
			throw SQLError.createSQLException(Messages
					.getString("ServerPreparedStatement.8"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
		}

		parameterIndex--;

		if ((parameterIndex < 0)
				|| (parameterIndex >= this.parameterBindings.length)) {
			throw SQLError.createSQLException(Messages
					.getString("ServerPreparedStatement.9") //$NON-NLS-1$
					+ (parameterIndex + 1)
					+ Messages.getString("ServerPreparedStatement.10") //$NON-NLS-1$
					+ this.parameterBindings.length,
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
		}

		if (this.parameterBindings[parameterIndex] == null) {
			this.parameterBindings[parameterIndex] = new BindValue();
		} else {
			if (this.parameterBindings[parameterIndex].isLongData
					&& !forLongData) {
				this.detectedLongParameterSwitch = true;
			}
		}

		this.parameterBindings[parameterIndex].isSet = true;
		this.parameterBindings[parameterIndex].boundBeforeExecutionNum = this.numberOfExecutions;

		return this.parameterBindings[parameterIndex];
	}

	/**
	 * @see com.mysql.jdbc.PreparedStatement#getBytes(int)
	 */
	byte[] getBytes(int parameterIndex) throws SQLException {
		BindValue bindValue = getBinding(parameterIndex, false);

		if (bindValue.isNull) {
			return null;
		} else if (bindValue.isLongData) {
			throw SQLError.notImplemented();
		} else {
			if (this.outByteBuffer == null) {
				this.outByteBuffer = new Buffer(this.connection
						.getNetBufferLength());
			}

			this.outByteBuffer.clear();

			int originalPosition = this.outByteBuffer.getPosition();

			storeBinding(this.outByteBuffer, bindValue, this.connection.getIO());

			int newPosition = this.outByteBuffer.getPosition();

			int length = newPosition - originalPosition;

			byte[] valueAsBytes = new byte[length];

			System.arraycopy(this.outByteBuffer.getByteBuffer(),
					originalPosition, valueAsBytes, 0, length);

			return valueAsBytes;
		}
	}

	/**
	 * @see java.sql.PreparedStatement#getMetaData()
	 */
	public java.sql.ResultSetMetaData getMetaData() throws SQLException {
		checkClosed();

		if (this.resultFields == null) {
			return null;
		}

		return new ResultSetMetaData(this.resultFields, 
				this.connection.getUseOldAliasMetadataBehavior(), getExceptionInterceptor());
	}

	/**
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException {
		checkClosed();
		
		if (this.parameterMetaData == null) {
			this.parameterMetaData = new MysqlParameterMetadata(
					this.parameterFields, this.parameterCount, getExceptionInterceptor());
		}
		
		return this.parameterMetaData;
	}

	/**
	 * @see com.mysql.jdbc.PreparedStatement#isNull(int)
	 */
	boolean isNull(int paramIndex) {
		throw new IllegalArgumentException(Messages
				.getString("ServerPreparedStatement.7")); //$NON-NLS-1$
	}

	/**
	 * Closes this connection and frees all resources.
	 * 
	 * @param calledExplicitly
	 *            was this called from close()?
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	protected void realClose(boolean calledExplicitly, 
			boolean closeOpenResults) throws SQLException {
		if (this.isClosed) {
			return;
		}

		if (this.connection != null) {
			if (this.connection.getAutoGenerateTestcaseScript()) {
				dumpCloseForTestcase();
			}

			//
			// Don't communicate with the server if we're being
			// called from the finalizer...
			// 
			// This will leak server resources, but if we don't do this,
			// we'll deadlock (potentially, because there's no guarantee
			// when, what order, and what concurrency finalizers will be
			// called with). Well-behaved programs won't rely on finalizers
			// to clean up their statements.
			//

			SQLException exceptionDuringClose = null;

			if (calledExplicitly && !this.connection.isClosed()) {
				synchronized (this.connection.getMutex()) {
					try {

						MysqlIO mysql = this.connection.getIO();

						Buffer packet = mysql.getSharedSendPacket();

						packet.writeByte((byte) MysqlDefs.COM_CLOSE_STATEMENT);
						packet.writeLong(this.serverStatementId);

						mysql.sendCommand(MysqlDefs.COM_CLOSE_STATEMENT, null,
								packet, true, null, 0);
					} catch (SQLException sqlEx) {
						exceptionDuringClose = sqlEx;
					}
				}
			}

			super.realClose(calledExplicitly, closeOpenResults);

			clearParametersInternal(false);
			this.parameterBindings = null;

			this.parameterFields = null;
			this.resultFields = null;

			if (exceptionDuringClose != null) {
				throw exceptionDuringClose;
			}
		}
	}

	/**
	 * Used by Connection when auto-reconnecting to retrieve 'lost' prepared
	 * statements.
	 * 
	 * @throws SQLException
	 *             if an error occurs.
	 */
	protected void rePrepare() throws SQLException {
		this.invalidationException = null;

		try {
			serverPrepare(this.originalSql);
		} catch (SQLException sqlEx) {
			// don't wrap SQLExceptions
			this.invalidationException = sqlEx;
		} catch (Exception ex) {
			this.invalidationException = SQLError.createSQLException(ex.toString(),
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
			this.invalidationException.initCause(ex);
		}

		if (this.invalidationException != null) {
			this.invalid = true;

			this.parameterBindings = null;

			this.parameterFields = null;
			this.resultFields = null;

			if (this.results != null) {
				try {
					this.results.close();
				} catch (Exception ex) {
					;
				}
			}

			if (this.connection != null) {
				if (this.maxRowsChanged) {
					this.connection.unsetMaxRows(this);
				}

				if (!this.connection.getDontTrackOpenResources()) {
					this.connection.unregisterStatement(this);
				}
			}
		}
	}

	/**
	 * Tells the server to execute this prepared statement with the current
	 * parameter bindings.
	 * 
	 * <pre>
	 * 
	 * 
	 *    -   Server gets the command 'COM_EXECUTE' to execute the
	 *        previously         prepared query. If there is any param markers;
	 *  then client will send the data in the following format:
	 * 
	 *  [COM_EXECUTE:1]
	 *  [STMT_ID:4]
	 *  [NULL_BITS:(param_count+7)/8)]
	 *  [TYPES_SUPPLIED_BY_CLIENT(0/1):1]
	 *  [[length]data]
	 *  [[length]data] .. [[length]data].
	 * 
	 *  (Note: Except for string/binary types; all other types will not be
	 *  supplied with length field)
	 * 
	 *  
	 * </pre>
	 * 
	 * @param maxRowsToRetrieve
	 *            DOCUMENT ME!
	 * @param createStreamingResultSet
	 *            DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 * 
	 * @throws SQLException
	 */
	private com.mysql.jdbc.ResultSetInternalMethods serverExecute(int maxRowsToRetrieve,
			boolean createStreamingResultSet, 
			Field[] metadataFromCache) throws SQLException {
		synchronized (this.connection.getMutex()) {
			MysqlIO mysql = this.connection.getIO();

			if (mysql.shouldIntercept()) {
				ResultSetInternalMethods interceptedResults =
	    			mysql.invokeStatementInterceptorsPre(this.originalSql, this, true);
	
	    		if (interceptedResults != null) {
	    			return interceptedResults;
	    		}
			}
    		
			if (this.detectedLongParameterSwitch) {
				// Check when values were bound
				boolean firstFound = false;
				long boundTimeToCheck = 0;
				
				for (int i = 0; i < this.parameterCount - 1; i++) {
					if (this.parameterBindings[i].isLongData) {
						if (firstFound && boundTimeToCheck != 
							this.parameterBindings[i].boundBeforeExecutionNum) { 					
							throw SQLError.createSQLException(Messages
									.getString("ServerPreparedStatement.11") //$NON-NLS-1$
									+ Messages.getString("ServerPreparedStatement.12"), //$NON-NLS-1$
									SQLError.SQL_STATE_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
						} else {
							firstFound = true;
							boundTimeToCheck = this.parameterBindings[i].boundBeforeExecutionNum;
						}
					}
				}
				
				// Okay, we've got all "newly"-bound streams, so reset 
				// server-side state to clear out previous bindings
				
				serverResetStatement();
			}


			// Check bindings
			for (int i = 0; i < this.parameterCount; i++) {
				if (!this.parameterBindings[i].isSet) {
					throw SQLError.createSQLException(Messages
							.getString("ServerPreparedStatement.13") + (i + 1) //$NON-NLS-1$
							+ Messages.getString("ServerPreparedStatement.14"),
							SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor()); //$NON-NLS-1$
				}
			}

			//
			// Send all long data
			//
			for (int i = 0; i < this.parameterCount; i++) {
				if (this.parameterBindings[i].isLongData) {
					serverLongData(i, this.parameterBindings[i]);
				}
			}

			if (this.connection.getAutoGenerateTestcaseScript()) {
				dumpExecuteForTestcase();
			}

			//
			// store the parameter values
			//

			Buffer packet = mysql.getSharedSendPacket();

			packet.clear();
			packet.writeByte((byte) MysqlDefs.COM_EXECUTE);
			packet.writeLong(this.serverStatementId);

			boolean usingCursor = false;

			if (this.connection.versionMeetsMinimum(4, 1, 2)) {
				// we only create cursor-backed result sets if
				// a) The query is a SELECT
				// b) The server supports it
				// c) We know it is forward-only (note this doesn't
				// preclude updatable result sets)
				// d) The user has set a fetch size
				if (this.resultFields != null &&
						this.connection.isCursorFetchEnabled()
						&& getResultSetType() == ResultSet.TYPE_FORWARD_ONLY
						&& getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY
						&& getFetchSize() > 0) {
					packet.writeByte(MysqlDefs.OPEN_CURSOR_FLAG);
					usingCursor = true;
				} else {
					packet.writeByte((byte) 0); // placeholder for flags
				}

				packet.writeLong(1); // placeholder for parameter
				                     // iterations
			}

			/* Reserve place for null-marker bytes */
			int nullCount = (this.parameterCount + 7) / 8;

			// if (mysql.versionMeetsMinimum(4, 1, 2)) {
			// nullCount = (this.parameterCount + 9) / 8;
			// }
			int nullBitsPosition = packet.getPosition();

			for (int i = 0; i < nullCount; i++) {
				packet.writeByte((byte) 0);
			}

			byte[] nullBitsBuffer = new byte[nullCount];

			/* In case if buffers (type) altered, indicate to server */
			packet.writeByte(this.sendTypesToServer ? (byte) 1 : (byte) 0);

			if (this.sendTypesToServer) {
				/*
				 * Store types of parameters in first in first package that is
				 * sent to the server.
				 */
				for (int i = 0; i < this.parameterCount; i++) {
					packet.writeInt(this.parameterBindings[i].bufferType);
				}
			}

			//
			// store the parameter values
			//
			for (int i = 0; i < this.parameterCount; i++) {
				if (!this.parameterBindings[i].isLongData) {
					if (!this.parameterBindings[i].isNull) {
						storeBinding(packet, this.parameterBindings[i], mysql);
					} else {
						nullBitsBuffer[i / 8] |= (1 << (i & 7));
					}
				}
			}

			//
			// Go back and write the NULL flags
			// to the beginning of the packet
			//
			int endPosition = packet.getPosition();
			packet.setPosition(nullBitsPosition);
			packet.writeBytesNoNull(nullBitsBuffer);
			packet.setPosition(endPosition);

			long begin = 0;

			boolean logSlowQueries = this.connection.getLogSlowQueries();
			boolean gatherPerformanceMetrics = this.connection
					.getGatherPerformanceMetrics();

			if (this.profileSQL || logSlowQueries || gatherPerformanceMetrics) {
				begin = mysql.getCurrentTimeNanosOrMillis();
			}

			resetCancelledState();
			
			CancelTask timeoutTask = null;

			try {
				if (this.connection.getEnableQueryTimeouts() &&
						this.timeoutInMillis != 0
						&& this.connection.versionMeetsMinimum(5, 0, 0)) {
					timeoutTask = new CancelTask(this);
					this.connection.getCancelTimer().schedule(timeoutTask, 
							this.timeoutInMillis);
				}
				
				Buffer resultPacket = mysql.sendCommand(MysqlDefs.COM_EXECUTE,
					null, packet, false, null, 0);
				
				long queryEndTime = 0L;
				
				if (logSlowQueries || gatherPerformanceMetrics || this.profileSQL) {
					queryEndTime = mysql.getCurrentTimeNanosOrMillis();
				}
				
				if (timeoutTask != null) {
					timeoutTask.cancel();
					
					if (timeoutTask.caughtWhileCancelling != null) {
						throw timeoutTask.caughtWhileCancelling;
					}
					
					timeoutTask = null;
				}
				
				synchronized (this.cancelTimeoutMutex) {
					if (this.wasCancelled) {
						SQLException cause = null;
						
						if (this.wasCancelledByTimeout) {
							cause = new MySQLTimeoutException();
						} else {
							cause = new MySQLStatementCancelledException();
						}
						
						resetCancelledState();
						
						throw cause;
					}
				}

				boolean queryWasSlow = false;
				
				if (logSlowQueries || gatherPerformanceMetrics) {
					long elapsedTime = queryEndTime - begin;

					if (logSlowQueries) {
		    			if (this.useAutoSlowLog) {
		    				queryWasSlow = elapsedTime > this.connection.getSlowQueryThresholdMillis();
		    			} else {
		    				queryWasSlow = this.connection.isAbonormallyLongQuery(elapsedTime);
		    				
		    				this.connection.reportQueryTime(elapsedTime);
		    			}
					}

					if (queryWasSlow) {
						
						StringBuffer mesgBuf = new StringBuffer(
								48 + this.originalSql.length());
						mesgBuf.append(Messages
								.getString("ServerPreparedStatement.15")); //$NON-NLS-1$
						mesgBuf.append(mysql.getSlowQueryThreshold());
						mesgBuf.append(Messages
								.getString("ServerPreparedStatement.15a")); //$NON-NLS-1$
						mesgBuf.append(elapsedTime);
						mesgBuf.append(Messages
								.getString("ServerPreparedStatement.16")); //$NON-NLS-1$

						mesgBuf.append("as prepared: ");
						mesgBuf.append(this.originalSql);
						mesgBuf.append("\n\n with parameters bound:\n\n");
						mesgBuf.append(asSql(true));

						this.eventSink
								.consumeEvent(new ProfilerEvent(
										ProfilerEvent.TYPE_SLOW_QUERY,
										"", this.currentCatalog, this.connection.getId(), //$NON-NLS-1$
										getId(), 0, System.currentTimeMillis(),
										elapsedTime, mysql
												.getQueryTimingUnits(), null,
										new Throwable(), mesgBuf.toString()));
					}

					if (gatherPerformanceMetrics) {
						this.connection.registerQueryExecutionTime(elapsedTime);
					}
				}

				this.connection.incrementNumberOfPreparedExecutes();

				if (this.profileSQL) {
					this.eventSink = ProfilerEventHandlerFactory
							.getInstance(this.connection);

					this.eventSink.consumeEvent(new ProfilerEvent(
							ProfilerEvent.TYPE_EXECUTE,
							"", this.currentCatalog, //$NON-NLS-1$
							this.connectionId, this.statementId, -1, System
									.currentTimeMillis(), (int) (mysql
									.getCurrentTimeNanosOrMillis() - begin),
							mysql.getQueryTimingUnits(), null, new Throwable(),
							truncateQueryToLog(asSql(true))));
				}
	
				com.mysql.jdbc.ResultSetInternalMethods rs = mysql.readAllResults(this,
						maxRowsToRetrieve, this.resultSetType,
						this.resultSetConcurrency, createStreamingResultSet,
						this.currentCatalog, resultPacket, true, this.fieldCount,
						metadataFromCache);
				
				if (mysql.shouldIntercept()) {
					ResultSetInternalMethods interceptedResults =
		    			mysql.invokeStatementInterceptorsPost(this.originalSql, this, rs, true, null);
		
		    		if (interceptedResults != null) {
		    			rs = interceptedResults;
		    		}
				}
				
				if (this.profileSQL) {
					long fetchEndTime = mysql.getCurrentTimeNanosOrMillis();

					this.eventSink.consumeEvent(new ProfilerEvent(
							ProfilerEvent.TYPE_FETCH,
							"", this.currentCatalog, this.connection.getId(), //$NON-NLS-1$
							getId(), 0 /* FIXME rs.resultId */, System.currentTimeMillis(),
							(fetchEndTime - queryEndTime), mysql
									.getQueryTimingUnits(), null,
							new Throwable(), null));
				}
	
				if (queryWasSlow && this.connection.getExplainSlowQueries()) {
					String queryAsString = asSql(true);

					mysql.explainSlowQuery(queryAsString.getBytes(),
							queryAsString);
				}
				
				if (!createStreamingResultSet && 
						this.serverNeedsResetBeforeEachExecution) {
					serverResetStatement(); // clear any long data...
				}
	
				
				this.sendTypesToServer = false;
				this.results = rs;
	
				if (mysql.hadWarnings()) {
		            mysql.scanForAndThrowDataTruncation();
		        }
				
				return rs;
			} catch (SQLException sqlEx) {
				if (mysql.shouldIntercept()) {
					mysql.invokeStatementInterceptorsPost(this.originalSql, this, null, true, sqlEx);
				}
				
				throw sqlEx;
			} finally {
				if (timeoutTask != null) {
					timeoutTask.cancel();
				}
			}
		}
	}

	/**
	 * Sends stream-type data parameters to the server.
	 * 
	 * <pre>
	 * 
	 *  Long data handling:
	 * 
	 *  - Server gets the long data in pieces with command type 'COM_LONG_DATA'.
	 *  - The packet recieved will have the format as:
	 *    [COM_LONG_DATA:     1][STMT_ID:4][parameter_number:2][type:2][data]
	 *  - Checks if the type is specified by client, and if yes reads the type,
	 *    and  stores the data in that format.
	 *  - It's up to the client to check for read data ended. The server doesn't
	 *    care;  and also server doesn't notify to the client that it got the
	 *    data  or not; if there is any error; then during execute; the error
	 *    will  be returned
	 *  
	 * </pre>
	 * 
	 * @param parameterIndex
	 *            DOCUMENT ME!
	 * @param longData
	 *            DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             if an error occurs.
	 */
	private void serverLongData(int parameterIndex, BindValue longData)
			throws SQLException {
		synchronized (this.connection.getMutex()) {
			MysqlIO mysql = this.connection.getIO();

			Buffer packet = mysql.getSharedSendPacket();

			Object value = longData.value;

			if (value instanceof byte[]) {
				packet.clear();
				packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
				packet.writeLong(this.serverStatementId);
				packet.writeInt((parameterIndex));

				packet.writeBytesNoNull((byte[]) longData.value);

				mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true,
						null, 0);
			} else if (value instanceof InputStream) {
				storeStream(mysql, parameterIndex, packet, (InputStream) value);
			} else if (value instanceof java.sql.Blob) {
				storeStream(mysql, parameterIndex, packet,
						((java.sql.Blob) value).getBinaryStream());
			} else if (value instanceof Reader) {
				storeReader(mysql, parameterIndex, packet, (Reader) value);
			} else {
				throw SQLError.createSQLException(Messages
						.getString("ServerPreparedStatement.18") //$NON-NLS-1$
						+ value.getClass().getName() + "'", //$NON-NLS-1$
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
			}
		}
	}

	private void serverPrepare(String sql) throws SQLException {
		synchronized (this.connection.getMutex()) {
			MysqlIO mysql = this.connection.getIO();

			if (this.connection.getAutoGenerateTestcaseScript()) {
				dumpPrepareForTestcase();
			}

			try {
				long begin = 0;

				if (StringUtils.startsWithIgnoreCaseAndWs(sql, "LOAD DATA")) { //$NON-NLS-1$
					this.isLoadDataQuery = true;
				} else {
					this.isLoadDataQuery = false;
				}

				if (this.connection.getProfileSql()) {
					begin = System.currentTimeMillis();
				}

				String characterEncoding = null;
				String connectionEncoding = this.connection.getEncoding();

				if (!this.isLoadDataQuery && this.connection.getUseUnicode()
						&& (connectionEncoding != null)) {
					characterEncoding = connectionEncoding;
				}

				Buffer prepareResultPacket = mysql.sendCommand(
						MysqlDefs.COM_PREPARE, sql, null, false,
						characterEncoding, 0);

				if (this.connection.versionMeetsMinimum(4, 1, 1)) {
					// 4.1.1 and newer use the first byte
					// as an 'ok' or 'error' flag, so move
					// the buffer pointer past it to
					// start reading the statement id.
					prepareResultPacket.setPosition(1);
				} else {
					// 4.1.0 doesn't use the first byte as an
					// 'ok' or 'error' flag
					prepareResultPacket.setPosition(0);
				}

				this.serverStatementId = prepareResultPacket.readLong();
				this.fieldCount = prepareResultPacket.readInt();
				this.parameterCount = prepareResultPacket.readInt();
				this.parameterBindings = new BindValue[this.parameterCount];

				for (int i = 0; i < this.parameterCount; i++) {
					this.parameterBindings[i] = new BindValue();
				}

				this.connection.incrementNumberOfPrepares();

				if (this.profileSQL) {
					this.eventSink.consumeEvent(new ProfilerEvent(
							ProfilerEvent.TYPE_PREPARE,
							"", this.currentCatalog, //$NON-NLS-1$
							this.connectionId, this.statementId, -1,
							System.currentTimeMillis(), 
							mysql.getCurrentTimeNanosOrMillis() - begin, 
							mysql.getQueryTimingUnits(), null,
							new Throwable(), truncateQueryToLog(sql)));
				}

				if (this.parameterCount > 0) {
					if (this.connection.versionMeetsMinimum(4, 1, 2)
							&& !mysql.isVersion(5, 0, 0)) {
						this.parameterFields = new Field[this.parameterCount];

						Buffer metaDataPacket = mysql.readPacket();

						int i = 0;

						while (!metaDataPacket.isLastDataPacket()
								&& (i < this.parameterCount)) {
							this.parameterFields[i++] = mysql.unpackField(
									metaDataPacket, false);
							metaDataPacket = mysql.readPacket();
						}
					}
				}

				if (this.fieldCount > 0) {
					this.resultFields = new Field[this.fieldCount];

					Buffer fieldPacket = mysql.readPacket();

					int i = 0;

					// Read in the result set column information
					while (!fieldPacket.isLastDataPacket()
							&& (i < this.fieldCount)) {
						this.resultFields[i++] = mysql.unpackField(fieldPacket,
								false);
						fieldPacket = mysql.readPacket();
					}
				}
			} catch (SQLException sqlEx) {
				if (this.connection.getDumpQueriesOnException()) {
					StringBuffer messageBuf = new StringBuffer(this.originalSql
							.length() + 32);
					messageBuf
							.append("\n\nQuery being prepared when exception was thrown:\n\n");
					messageBuf.append(this.originalSql);

					sqlEx = ConnectionImpl.appendMessageToException(sqlEx,
							messageBuf.toString(), getExceptionInterceptor());
				}

				throw sqlEx;
			} finally {
				// Leave the I/O channel in a known state...there might be
				// packets out there
				// that we're not interested in
				this.connection.getIO().clearInputStream();
			}
		}
	}

	private String truncateQueryToLog(String sql) {
		String query = null;
		
		if (sql.length() > this.connection.getMaxQuerySizeToLog()) {
			StringBuffer queryBuf = new StringBuffer(
					this.connection.getMaxQuerySizeToLog() + 12);
			queryBuf.append(sql.substring(0, this.connection.getMaxQuerySizeToLog()));
			queryBuf.append(Messages.getString("MysqlIO.25"));
			
			query = queryBuf.toString();
		} else {
			query = sql;
		}
		
		return query;
	}

	private void serverResetStatement() throws SQLException {
		synchronized (this.connection.getMutex()) {

			MysqlIO mysql = this.connection.getIO();

			Buffer packet = mysql.getSharedSendPacket();

			packet.clear();
			packet.writeByte((byte) MysqlDefs.COM_RESET_STMT);
			packet.writeLong(this.serverStatementId);

			try {
				mysql.sendCommand(MysqlDefs.COM_RESET_STMT, null, packet,
						!this.connection.versionMeetsMinimum(4, 1, 2), null, 0);
			} catch (SQLException sqlEx) {
				throw sqlEx;
			} catch (Exception ex) {
				SQLException sqlEx = SQLError.createSQLException(ex.toString(),
						SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
				sqlEx.initCause(ex);
				
				throw sqlEx;
			} finally {
				mysql.clearInputStream();
			}
		}
	}

	/**
	 * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
	 */
	public void setArray(int i, Array x) throws SQLException {
		throw SQLError.notImplemented();
	}

	/**
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream,
	 *      int)
	 */
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		checkClosed();

		if (x == null) {
			setNull(parameterIndex, java.sql.Types.BINARY);
		} else {
			BindValue binding = getBinding(parameterIndex, true);
			setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

			binding.value = x;
			binding.isNull = false;
			binding.isLongData = true;

			if (this.connection.getUseStreamLengthsInPrepStmts()) {
				binding.bindLength = length;
			} else {
				binding.bindLength = -1;
			}
		}
	}

	/**
	 * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
	 */
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		checkClosed();

		if (x == null) {
			setNull(parameterIndex, java.sql.Types.DECIMAL);
		} else {
		
			BindValue binding = getBinding(parameterIndex, false);

			if (this.connection.versionMeetsMinimum(5, 0, 3)) {
				setType(binding, MysqlDefs.FIELD_TYPE_NEW_DECIMAL);
			} else {
				setType(binding, this.stringTypeCode);
			}

			binding.value = StringUtils
				.fixDecimalExponent(StringUtils.consistentToString(x));
			binding.isNull = false;
			binding.isLongData = false;
		}
	}

	/**
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream,
	 *      int)
	 */
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		checkClosed();

		if (x == null) {
			setNull(parameterIndex, java.sql.Types.BINARY);
		} else {
			BindValue binding = getBinding(parameterIndex, true);
			setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

			binding.value = x;
			binding.isNull = false;
			binding.isLongData = true;

			if (this.connection.getUseStreamLengthsInPrepStmts()) {
				binding.bindLength = length;
			} else {
				binding.bindLength = -1;
			}
		}
	}

	/**
	 * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
	 */
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		checkClosed();

		if (x == null) {
			setNull(parameterIndex, java.sql.Types.BINARY);
		} else {
			BindValue binding = getBinding(parameterIndex, true);
			setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

			binding.value = x;
			binding.isNull = false;
			binding.isLongData = true;

			if (this.connection.getUseStreamLengthsInPrepStmts()) {
				binding.bindLength = x.length();
			} else {
				binding.bindLength = -1;
			}
		}
	}

	/**
	 * @see java.sql.PreparedStatement#setBoolean(int, boolean)
	 */
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		setByte(parameterIndex, (x ? (byte) 1 : (byte) 0));
	}

	/**
	 * @see java.sql.PreparedStatement#setByte(int, byte)
	 */
	public void setByte(int parameterIndex, byte x) throws SQLException {
		checkClosed();

		BindValue binding = getBinding(parameterIndex, false);
		setType(binding, MysqlDefs.FIELD_TYPE_TINY);

		binding.value = null;
		binding.byteBinding = x;
		binding.isNull = false;
		binding.isLongData = false;
	}

	/**
	 * @see java.sql.PreparedStatement#setBytes(int, byte)
	 */
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		checkClosed();

		if (x == null) {
			setNull(parameterIndex, java.sql.Types.BINARY);
		} else {
			BindValue binding = getBinding(parameterIndex, false);
			setType(binding, MysqlDefs.FIELD_TYPE_VAR_STRING);

			binding.value = x;
			binding.isNull = false;
			binding.isLongData = false;
		}
	}

	/**
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader,
	 *      int)
	 */
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		checkClosed();

		if (reader == null) {
			setNull(parameterIndex, java.sql.Types.BINARY);
		} else {
			BindValue binding = getBinding(parameterIndex, true);
			setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

			binding.value = reader;
			binding.isNull = false;
			binding.isLongData = true;

			if (this.connection.getUseStreamLengthsInPrepStmts()) {
				binding.bindLength = length;
			} else {
				binding.bindLength = -1;
			}
		}
	}

	/**
	 * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
	 */
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		checkClosed();

		if (x == null) {
			setNull(parameterIndex, java.sql.Types.BINARY);
		} else {
			BindValue binding = getBinding(parameterIndex, true);
			setType(binding, MysqlDefs.FIELD_TYPE_BLOB);

			binding.value = x.getCharacterStream();
			binding.isNull = false;
			binding.isLongData = true;

			if (this.connection.getUseStreamLengthsInPrepStmts()) {
				binding.bindLength = x.length();
			} else {
				binding.bindLength = -1;
			}
		}
	}

	/**
	 * Set a parameter to a java.sql.Date value. The driver converts this to a
	 * SQL DATE value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public void setDate(int parameterIndex, Date x) throws SQLException {
		setDate(parameterIndex, x, null);
	}

	/**
	 * Set a parameter to a java.sql.Date value. The driver converts this to a
	 * SQL DATE value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            the parameter value
	 * @param cal
	 *            the calendar to interpret the date with
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.DATE);
		} else {
			BindValue binding = getBinding(parameterIndex, false);
			setType(binding, MysqlDefs.FIELD_TYPE_DATE);

			binding.value = x;
			binding.isNull = false;
			binding.isLongData = false;
		}
	}

	/**
	 * @see java.sql.PreparedStatement#setDouble(int, double)
	 */
	public void setDouble(int parameterIndex, double x) throws SQLException {
		checkClosed();

		if (!this.connection.getAllowNanAndInf()
				&& (x == Double.POSITIVE_INFINITY
						|| x == Double.NEGATIVE_INFINITY || Double.isNaN(x))) {
			throw SQLError.createSQLException("'" + x
					+ "' is not a valid numeric or approximate numeric value",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());

		}

		BindValue binding = getBinding(parameterIndex, false);
		setType(binding, MysqlDefs.FIELD_TYPE_DOUBLE);

		binding.value = null;
		binding.doubleBinding = x;
		binding.isNull = false;
		binding.isLongData = false;
	}

	/**
	 * @see java.sql.PreparedStatement#setFloat(int, float)
	 */
	public void setFloat(int parameterIndex, float x) throws SQLException {
		checkClosed();

		BindValue binding = getBinding(parameterIndex, false);
		setType(binding, MysqlDefs.FIELD_TYPE_FLOAT);

		binding.value = null;
		binding.floatBinding = x;
		binding.isNull = false;
		binding.isLongData = false;
	}

	/**
	 * @see java.sql.PreparedStatement#setInt(int, int)
	 */
	public void setInt(int parameterIndex, int x) throws SQLException {
		checkClosed();

		BindValue binding = getBinding(parameterIndex, false);
		setType(binding, MysqlDefs.FIELD_TYPE_LONG);

		binding.value = null;
		binding.intBinding = x;
		binding.isNull = false;
		binding.isLongData = false;
	}

	/**
	 * @see java.sql.PreparedStatement#setLong(int, long)
	 */
	public void setLong(int parameterIndex, long x) throws SQLException {
		checkClosed();

		BindValue binding = getBinding(parameterIndex, false);
		setType(binding, MysqlDefs.FIELD_TYPE_LONGLONG);

		binding.value = null;
		binding.longBinding = x;
		binding.isNull = false;
		binding.isLongData = false;
	}

	/**
	 * @see java.sql.PreparedStatement#setNull(int, int)
	 */
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		checkClosed();

		BindValue binding = getBinding(parameterIndex, false);

		//
		// Don't re-set types, but use something if this
		// parameter was never specified
		//
		if (binding.bufferType == 0) {
			setType(binding, MysqlDefs.FIELD_TYPE_NULL);
		}

		binding.value = null;
		binding.isNull = true;
		binding.isLongData = false;
	}

	/**
	 * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
	 */
	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		checkClosed();

		BindValue binding = getBinding(parameterIndex, false);

		//
		// Don't re-set types, but use something if this
		// parameter was never specified
		//
		if (binding.bufferType == 0) {
			setType(binding, MysqlDefs.FIELD_TYPE_NULL);
		}

		binding.value = null;
		binding.isNull = true;
		binding.isLongData = false;
	}

	/**
	 * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
	 */
	public void setRef(int i, Ref x) throws SQLException {
		throw SQLError.notImplemented();
	}

	/**
	 * @see java.sql.PreparedStatement#setShort(int, short)
	 */
	public void setShort(int parameterIndex, short x) throws SQLException {
		checkClosed();

		BindValue binding = getBinding(parameterIndex, false);
		setType(binding, MysqlDefs.FIELD_TYPE_SHORT);

		binding.value = null;
		binding.shortBinding = x;
		binding.isNull = false;
		binding.isLongData = false;
	}

	/**
	 * @see java.sql.PreparedStatement#setString(int, java.lang.String)
	 */
	public void setString(int parameterIndex, String x) throws SQLException {
		checkClosed();

		if (x == null) {
			setNull(parameterIndex, java.sql.Types.CHAR);
		} else {
			BindValue binding = getBinding(parameterIndex, false);

			setType(binding, this.stringTypeCode);

			binding.value = x;
			binding.isNull = false;
			binding.isLongData = false;
		}
	}

	/**
	 * Set a parameter to a java.sql.Time value.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...));
	 * @param x
	 *            the parameter value
	 * 
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void setTime(int parameterIndex, java.sql.Time x)
			throws SQLException {
		setTimeInternal(parameterIndex, x, null, this.connection.getDefaultTimeZone(), false);
	}

	/**
	 * Set a parameter to a java.sql.Time value. The driver converts this to a
	 * SQL TIME value when it sends it to the database, using the given
	 * timezone.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...));
	 * @param x
	 *            the parameter value
	 * @param cal
	 *            the timezone to use
	 * 
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void setTime(int parameterIndex, java.sql.Time x, Calendar cal)
			throws SQLException {
		setTimeInternal(parameterIndex, x, cal, cal.getTimeZone(), true);
	}

	/**
	 * Set a parameter to a java.sql.Time value. The driver converts this to a
	 * SQL TIME value when it sends it to the database, using the given
	 * timezone.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...));
	 * @param x
	 *            the parameter value
	 * @param tz
	 *            the timezone to use
	 * 
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void setTimeInternal(int parameterIndex, java.sql.Time x,
			Calendar targetCalendar,
			TimeZone tz, boolean rollForward) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.TIME);
		} else {
			BindValue binding = getBinding(parameterIndex, false);
			setType(binding, MysqlDefs.FIELD_TYPE_TIME);

			if (!this.useLegacyDatetimeCode) {
				binding.value = x;
			} else {
				Calendar sessionCalendar = getCalendarInstanceForSessionOrNew();
				
				synchronized (sessionCalendar) {
					binding.value = TimeUtil.changeTimezone(this.connection, 
							sessionCalendar,
							targetCalendar,
							x, tz,
							this.connection.getServerTimezoneTZ(), 
							rollForward);
				}
			}
			
			binding.isNull = false;
			binding.isLongData = false;
		}
	}

	/**
	 * Set a parameter to a java.sql.Timestamp value. The driver converts this
	 * to a SQL TIMESTAMP value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            the parameter value
	 * 
	 * @throws SQLException
	 *             if a database-access error occurs.
	 */
	public void setTimestamp(int parameterIndex, java.sql.Timestamp x)
			throws SQLException {
		setTimestampInternal(parameterIndex, x, null, this.connection.getDefaultTimeZone(), false);
	}

	/**
	 * Set a parameter to a java.sql.Timestamp value. The driver converts this
	 * to a SQL TIMESTAMP value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            the parameter value
	 * @param cal
	 *            the timezone to use
	 * 
	 * @throws SQLException
	 *             if a database-access error occurs.
	 */
	public void setTimestamp(int parameterIndex, java.sql.Timestamp x,
			Calendar cal) throws SQLException {
		setTimestampInternal(parameterIndex, x, cal, cal.getTimeZone(), true);
	}

	protected void setTimestampInternal(int parameterIndex,
			java.sql.Timestamp x, Calendar targetCalendar,
			TimeZone tz, boolean rollForward)
			throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.TIMESTAMP);
		} else {
			BindValue binding = getBinding(parameterIndex, false);
			setType(binding, MysqlDefs.FIELD_TYPE_DATETIME);

			if (!this.useLegacyDatetimeCode) {
				binding.value = x;
			} else {
				Calendar sessionCalendar = this.connection.getUseJDBCCompliantTimezoneShift() ?
						this.connection.getUtcCalendar() : 
							getCalendarInstanceForSessionOrNew();
				
				synchronized (sessionCalendar) {
					binding.value = TimeUtil.changeTimezone(this.connection, 
							sessionCalendar,
							targetCalendar,
							x, tz,
							this.connection.getServerTimezoneTZ(), 
							rollForward);
				}
				
				binding.isNull = false;
				binding.isLongData = false;
			}
		}
	}

	protected void setType(BindValue oldValue, int bufferType) {
		if (oldValue.bufferType != bufferType) {
			this.sendTypesToServer = true;
		}

		oldValue.bufferType = bufferType;
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param parameterIndex
	 *            DOCUMENT ME!
	 * @param x
	 *            DOCUMENT ME!
	 * @param length
	 *            DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 * @throws NotImplemented
	 *             DOCUMENT ME!
	 * 
	 * @see java.sql.PreparedStatement#setUnicodeStream(int,
	 *      java.io.InputStream, int)
	 * @deprecated
	 */
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		checkClosed();

		throw SQLError.notImplemented();
	}

	/**
	 * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
	 */
	public void setURL(int parameterIndex, URL x) throws SQLException {
		checkClosed();

		setString(parameterIndex, x.toString());
	}

	/**
	 * Method storeBinding.
	 * 
	 * @param packet
	 * @param bindValue
	 * @param mysql
	 *            DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	private void storeBinding(Buffer packet, BindValue bindValue, MysqlIO mysql)
			throws SQLException {
		try {
			Object value = bindValue.value;

			//
			// Handle primitives first
			//
			switch (bindValue.bufferType) {

			case MysqlDefs.FIELD_TYPE_TINY:
				packet.writeByte(bindValue.byteBinding);
				return;
			case MysqlDefs.FIELD_TYPE_SHORT:
				packet.ensureCapacity(2);
				packet.writeInt(bindValue.shortBinding);
				return;
			case MysqlDefs.FIELD_TYPE_LONG:
				packet.ensureCapacity(4);
				packet.writeLong(bindValue.intBinding);
				return;
			case MysqlDefs.FIELD_TYPE_LONGLONG:
				packet.ensureCapacity(8);
				packet.writeLongLong(bindValue.longBinding);
				return;
			case MysqlDefs.FIELD_TYPE_FLOAT:
				packet.ensureCapacity(4);
				packet.writeFloat(bindValue.floatBinding);
				return;
			case MysqlDefs.FIELD_TYPE_DOUBLE:
				packet.ensureCapacity(8);
				packet.writeDouble(bindValue.doubleBinding);
				return;
			case MysqlDefs.FIELD_TYPE_TIME:
				storeTime(packet, (Time) value);
				return;
			case MysqlDefs.FIELD_TYPE_DATE:
			case MysqlDefs.FIELD_TYPE_DATETIME:
			case MysqlDefs.FIELD_TYPE_TIMESTAMP:
				storeDateTime(packet, (java.util.Date) value, mysql, bindValue.bufferType);
				return;
			case MysqlDefs.FIELD_TYPE_VAR_STRING:
			case MysqlDefs.FIELD_TYPE_STRING:
			case MysqlDefs.FIELD_TYPE_VARCHAR:
			case MysqlDefs.FIELD_TYPE_DECIMAL:
			case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
				if (value instanceof byte[]) {
					packet.writeLenBytes((byte[]) value);
				} else if (!this.isLoadDataQuery) {
					packet.writeLenString((String) value, this.charEncoding,
							this.connection.getServerCharacterEncoding(),
							this.charConverter, this.connection
									.parserKnowsUnicode(),
									this.connection);
				} else {
					packet.writeLenBytes(((String) value).getBytes());
				}

				return;
			}

			
		} catch (UnsupportedEncodingException uEE) {
			throw SQLError.createSQLException(Messages
					.getString("ServerPreparedStatement.22") //$NON-NLS-1$
					+ this.connection.getEncoding() + "'", //$NON-NLS-1$
					SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
		}
	}

	private void storeDateTime412AndOlder(Buffer intoBuf, java.util.Date dt, int bufferType)
			throws SQLException {
		
		Calendar sessionCalendar = null;
		
		if (!this.useLegacyDatetimeCode) {
			if (bufferType == MysqlDefs.FIELD_TYPE_DATE) {
				sessionCalendar = getDefaultTzCalendar();
			} else {
				sessionCalendar = getServerTzCalendar();
			}
		} else {
			sessionCalendar = (dt instanceof Timestamp && 
				this.connection.getUseJDBCCompliantTimezoneShift()) ? 
				this.connection.getUtcCalendar() : getCalendarInstanceForSessionOrNew();
		}
		
		synchronized (sessionCalendar) {
			java.util.Date oldTime = sessionCalendar.getTime();
			
			try {
				intoBuf.ensureCapacity(8);
				intoBuf.writeByte((byte) 7); // length
	
				sessionCalendar.setTime(dt);
				
				int year = sessionCalendar.get(Calendar.YEAR);
				int month = sessionCalendar.get(Calendar.MONTH) + 1;
				int date = sessionCalendar.get(Calendar.DATE);

				intoBuf.writeInt(year);
				intoBuf.writeByte((byte) month);
				intoBuf.writeByte((byte) date);
		
				if (dt instanceof java.sql.Date) {
					intoBuf.writeByte((byte) 0);
					intoBuf.writeByte((byte) 0);
					intoBuf.writeByte((byte) 0);
				} else {
					intoBuf.writeByte((byte) sessionCalendar
							.get(Calendar.HOUR_OF_DAY));
					intoBuf.writeByte((byte) sessionCalendar
							.get(Calendar.MINUTE));
					intoBuf.writeByte((byte) sessionCalendar
							.get(Calendar.SECOND));
				}
			} finally {
				sessionCalendar.setTime(oldTime);
			}
		}
	}

	private void storeDateTime(Buffer intoBuf, java.util.Date dt, MysqlIO mysql, int bufferType)
			throws SQLException {
		if (this.connection.versionMeetsMinimum(4, 1, 3)) {
			storeDateTime413AndNewer(intoBuf, dt, bufferType);
		} else {
			storeDateTime412AndOlder(intoBuf, dt, bufferType);
		}
	}

	private void storeDateTime413AndNewer(Buffer intoBuf, java.util.Date dt, int bufferType)
			throws SQLException {
		Calendar sessionCalendar = null;
		
		if (!this.useLegacyDatetimeCode) {
			if (bufferType == MysqlDefs.FIELD_TYPE_DATE) {
				sessionCalendar = getDefaultTzCalendar();
			} else {
				sessionCalendar = getServerTzCalendar();
			}
		} else {
			sessionCalendar = (dt instanceof Timestamp && 
				this.connection.getUseJDBCCompliantTimezoneShift()) ? 
				this.connection.getUtcCalendar() : getCalendarInstanceForSessionOrNew();
		}
		
		synchronized (sessionCalendar) {
			java.util.Date oldTime = sessionCalendar.getTime();

			try {
				sessionCalendar.setTime(dt);
				
				if (dt instanceof java.sql.Date) {
					sessionCalendar.set(Calendar.HOUR_OF_DAY, 0);
					sessionCalendar.set(Calendar.MINUTE, 0);
					sessionCalendar.set(Calendar.SECOND, 0);
				}

				byte length = (byte) 7;

				if (dt instanceof java.sql.Timestamp) {
					length = (byte) 11;
				}

				intoBuf.ensureCapacity(length);
				
				intoBuf.writeByte(length); // length

				int year = sessionCalendar.get(Calendar.YEAR);
				int month = sessionCalendar.get(Calendar.MONTH) + 1;
				int date = sessionCalendar.get(Calendar.DAY_OF_MONTH);
				
				intoBuf.writeInt(year);
				intoBuf.writeByte((byte) month);
				intoBuf.writeByte((byte) date);

				if (dt instanceof java.sql.Date) {
					intoBuf.writeByte((byte) 0);
					intoBuf.writeByte((byte) 0);
					intoBuf.writeByte((byte) 0);
				} else {
					intoBuf.writeByte((byte) sessionCalendar
							.get(Calendar.HOUR_OF_DAY));
					intoBuf.writeByte((byte) sessionCalendar
							.get(Calendar.MINUTE));
					intoBuf.writeByte((byte) sessionCalendar
							.get(Calendar.SECOND));
				}

				if (length == 11) {
					//	MySQL expects microseconds, not nanos
					intoBuf.writeLong(((java.sql.Timestamp) dt).getNanos() / 1000);
				}
			
			} finally {
				sessionCalendar.setTime(oldTime);
			}
		}
	}

	private Calendar getServerTzCalendar() {
		synchronized (this) {
			if (serverTzCalendar == null) {
				serverTzCalendar = new GregorianCalendar(this.connection.getServerTimezoneTZ());
			}
			
			return this.serverTzCalendar;
		}
	}
	
	private Calendar getDefaultTzCalendar() {
		synchronized (this) {
			if (defaultTzCalendar == null) {
				defaultTzCalendar = new GregorianCalendar(TimeZone.getDefault());
			}
			
			return this.defaultTzCalendar;
		}
	}

	//
	// TO DO: Investigate using NIO to do this faster
	//
	private void storeReader(MysqlIO mysql, int parameterIndex, Buffer packet,
			Reader inStream) throws SQLException {
		String forcedEncoding = this.connection.getClobCharacterEncoding();
		
		String clobEncoding = 
			(forcedEncoding == null ? this.connection.getEncoding() : forcedEncoding);
		
		int maxBytesChar = 2;
			
		if (clobEncoding != null) {
			if (!clobEncoding.equals("UTF-16")) {
				maxBytesChar = this.connection.getMaxBytesPerChar(clobEncoding);
				
				if (maxBytesChar == 1) {
					maxBytesChar = 2; // for safety
				}
			} else {
				maxBytesChar = 4;
			}
		}
			
		char[] buf = new char[BLOB_STREAM_READ_BUF_SIZE / maxBytesChar];
		
		int numRead = 0;

		int bytesInPacket = 0;
		int totalBytesRead = 0;
		int bytesReadAtLastSend = 0;
		int packetIsFullAt = this.connection.getBlobSendChunkSize();
		
		
		
		try {
			packet.clear();
			packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
			packet.writeLong(this.serverStatementId);
			packet.writeInt((parameterIndex));

			boolean readAny = false;
			
			while ((numRead = inStream.read(buf)) != -1) {
				readAny = true;
			
				byte[] valueAsBytes = StringUtils.getBytes(buf, null,
						clobEncoding, this.connection
								.getServerCharacterEncoding(), 0, numRead,
						this.connection.parserKnowsUnicode(), getExceptionInterceptor());

				packet.writeBytesNoNull(valueAsBytes, 0, valueAsBytes.length);

				bytesInPacket += valueAsBytes.length;
				totalBytesRead += valueAsBytes.length;

				if (bytesInPacket >= packetIsFullAt) {
					bytesReadAtLastSend = totalBytesRead;

					mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet,
							true, null, 0);

					bytesInPacket = 0;
					packet.clear();
					packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
					packet.writeLong(this.serverStatementId);
					packet.writeInt((parameterIndex));
				}
			}

			if (totalBytesRead != bytesReadAtLastSend) {
				mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true,
						null, 0);
			}
			
			if (!readAny) {
				mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true,
						null, 0);
			}
		} catch (IOException ioEx) {
			SQLException sqlEx = SQLError.createSQLException(Messages
					.getString("ServerPreparedStatement.24") //$NON-NLS-1$
					+ ioEx.toString(), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
			sqlEx.initCause(ioEx);
			
			throw sqlEx;
		} finally {
			if (this.connection.getAutoClosePStmtStreams()) {
				if (inStream != null) {
					try {
						inStream.close();
					} catch (IOException ioEx) {
						; // ignore
					}
				}
			}
		}
	}

	private void storeStream(MysqlIO mysql, int parameterIndex, Buffer packet,
			InputStream inStream) throws SQLException {
		byte[] buf = new byte[BLOB_STREAM_READ_BUF_SIZE];

		int numRead = 0;
		
		try {
			int bytesInPacket = 0;
			int totalBytesRead = 0;
			int bytesReadAtLastSend = 0;
			int packetIsFullAt = this.connection.getBlobSendChunkSize();

			packet.clear();
			packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
			packet.writeLong(this.serverStatementId);
			packet.writeInt((parameterIndex));

			boolean readAny = false;
			
			while ((numRead = inStream.read(buf)) != -1) {

				readAny = true;
				
				packet.writeBytesNoNull(buf, 0, numRead);
				bytesInPacket += numRead;
				totalBytesRead += numRead;

				if (bytesInPacket >= packetIsFullAt) {
					bytesReadAtLastSend = totalBytesRead;

					mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet,
							true, null, 0);

					bytesInPacket = 0;
					packet.clear();
					packet.writeByte((byte) MysqlDefs.COM_LONG_DATA);
					packet.writeLong(this.serverStatementId);
					packet.writeInt((parameterIndex));
				}
			}

			if (totalBytesRead != bytesReadAtLastSend) {
				mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true,
						null, 0);
			}
			
			if (!readAny) {
				mysql.sendCommand(MysqlDefs.COM_LONG_DATA, null, packet, true,
						null, 0);
			}
		} catch (IOException ioEx) {
			SQLException sqlEx = SQLError.createSQLException(Messages
					.getString("ServerPreparedStatement.25") //$NON-NLS-1$
					+ ioEx.toString(), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
			sqlEx.initCause(ioEx);
			
			throw sqlEx;
		} finally {
			if (this.connection.getAutoClosePStmtStreams()) {
				if (inStream != null) {
					try {
						inStream.close();
					} catch (IOException ioEx) {
						; // ignore
					}
				}
			}
		}
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		StringBuffer toStringBuf = new StringBuffer();

		toStringBuf.append("com.mysql.jdbc.ServerPreparedStatement["); //$NON-NLS-1$
		toStringBuf.append(this.serverStatementId);
		toStringBuf.append("] - "); //$NON-NLS-1$

		try {
			toStringBuf.append(asSql());
		} catch (SQLException sqlEx) {
			toStringBuf.append(Messages.getString("ServerPreparedStatement.6")); //$NON-NLS-1$
			toStringBuf.append(sqlEx);
		}

		return toStringBuf.toString();
	}

	protected long getServerStatementId() {
		return serverStatementId;
	}

	private boolean hasCheckedRewrite = false;
	private boolean canRewrite = false;
	
	public synchronized boolean canRewriteAsMultiValueInsertAtSqlLevel() throws SQLException {
		if (!hasCheckedRewrite) {
			this.hasCheckedRewrite = true;
			this.canRewrite = canRewrite(this.originalSql, isOnDuplicateKeyUpdate(), getLocationOfOnDuplicateKeyUpdate(), 0);
			// We need to client-side parse this to get the VALUES clause, etc.
			this.parseInfo = new ParseInfo(this.originalSql, this.connection, this.connection.getMetaData(), this.charEncoding, this.charConverter);
		}
		
		return this.canRewrite;
	}

	
	public synchronized boolean canRewriteAsMultivalueInsertStatement() throws SQLException {
		if (!canRewriteAsMultiValueInsertAtSqlLevel()) {
			return false;
		}
	
		BindValue[] currentBindValues = null;
		BindValue[] previousBindValues = null;
	
		int nbrCommands = this.batchedArgs.size();
	
		// Can't have type changes between sets of bindings for this to work...
	
		for (int commandIndex = 0; commandIndex < nbrCommands; commandIndex++) {
			Object arg = this.batchedArgs.get(commandIndex);
	
			if (!(arg instanceof String)) {
	
				currentBindValues = ((BatchedBindValues) arg).batchedParameterValues;
	
				// We need to check types each time, as
				// the user might have bound different
				// types in each addBatch()
	
				if (previousBindValues != null) {
					for (int j = 0; j < this.parameterBindings.length; j++) {
						if (currentBindValues[j].bufferType != previousBindValues[j].bufferType) {
							return false;
						}
					}
				}
			}
		}
		
		
		
		return true;
	}
	
	private int locationOfOnDuplicateKeyUpdate = -2;
	
	protected synchronized int getLocationOfOnDuplicateKeyUpdate() {
		if (this.locationOfOnDuplicateKeyUpdate == -2) {
			this.locationOfOnDuplicateKeyUpdate = getOnDuplicateKeyLocation(this.originalSql);
		}
		
		return this.locationOfOnDuplicateKeyUpdate;
	}
	
	protected synchronized boolean isOnDuplicateKeyUpdate() {
		return getLocationOfOnDuplicateKeyUpdate() != -1;
	}

	
	

	/** 
	 *  Computes the maximum parameter set size, and entire batch size given 
	 *  the number of arguments in the batch.
	 */
	protected long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs) {
		long sizeOfEntireBatch = 1 + /* com_execute */ + 4 /* stmt id */ + 1 /* flags */ + 4 /* batch count padding */; 
		long maxSizeOfParameterSet = 0;
		
		for (int i = 0; i < numBatchedArgs; i++) {
			BindValue[] paramArg = ((BatchedBindValues) this.batchedArgs.get(i)).batchedParameterValues;
	
			long sizeOfParameterSet = 0;
			
			sizeOfParameterSet += (this.parameterCount + 7) / 8; // for isNull
			
			sizeOfParameterSet += this.parameterCount * 2; // have to send types
			
			for (int j = 0; j < this.parameterBindings.length; j++) {
				if (!paramArg[j].isNull) {
	
					long size = paramArg[j].getBoundLength();
					
					if (paramArg[j].isLongData) {
						if (size != -1) {
							sizeOfParameterSet += size;
						}
					} else {
						sizeOfParameterSet += size;
					}
				}
			}
			
			sizeOfEntireBatch += sizeOfParameterSet;
			
			if (sizeOfParameterSet > maxSizeOfParameterSet) {
				maxSizeOfParameterSet = sizeOfParameterSet;
			}
		}	
		
		return new long[] {maxSizeOfParameterSet, sizeOfEntireBatch};
	}

	protected int setOneBatchedParameterSet(
			java.sql.PreparedStatement batchedStatement, int batchedParamIndex,
			Object paramSet) throws SQLException {
		BindValue[] paramArg = ((BatchedBindValues) paramSet).batchedParameterValues;
	
		for (int j = 0; j < paramArg.length; j++) {
			if (paramArg[j].isNull) {
				batchedStatement.setNull(batchedParamIndex++, Types.NULL);
			} else {
				if (paramArg[j].isLongData) {
					Object value = paramArg[j].value;
	
					if (value instanceof InputStream) {
						batchedStatement.setBinaryStream(batchedParamIndex++,
								(InputStream) value,
								(int) paramArg[j].bindLength);
					} else {
						batchedStatement.setCharacterStream(
								batchedParamIndex++, (Reader) value,
								(int) paramArg[j].bindLength);
					}
				} else {
	
					switch (paramArg[j].bufferType) {
	
					case MysqlDefs.FIELD_TYPE_TINY:
						batchedStatement.setByte(batchedParamIndex++,
								paramArg[j].byteBinding);
						break;
					case MysqlDefs.FIELD_TYPE_SHORT:
						batchedStatement.setShort(batchedParamIndex++,
								paramArg[j].shortBinding);
						break;
					case MysqlDefs.FIELD_TYPE_LONG:
						batchedStatement.setInt(batchedParamIndex++,
								paramArg[j].intBinding);
						break;
					case MysqlDefs.FIELD_TYPE_LONGLONG:
						batchedStatement.setLong(batchedParamIndex++,
								paramArg[j].longBinding);
						break;
					case MysqlDefs.FIELD_TYPE_FLOAT:
						batchedStatement.setFloat(batchedParamIndex++,
								paramArg[j].floatBinding);
						break;
					case MysqlDefs.FIELD_TYPE_DOUBLE:
						batchedStatement.setDouble(batchedParamIndex++,
								paramArg[j].doubleBinding);
						break;
					case MysqlDefs.FIELD_TYPE_TIME:
						batchedStatement.setTime(batchedParamIndex++,
								(Time) paramArg[j].value);
						break;
					case MysqlDefs.FIELD_TYPE_DATE:
						batchedStatement.setDate(batchedParamIndex++,
								(Date) paramArg[j].value);
						break;
					case MysqlDefs.FIELD_TYPE_DATETIME:
					case MysqlDefs.FIELD_TYPE_TIMESTAMP:
						batchedStatement.setTimestamp(batchedParamIndex++,
								(Timestamp) paramArg[j].value);
						break;
					case MysqlDefs.FIELD_TYPE_VAR_STRING:
					case MysqlDefs.FIELD_TYPE_STRING:
					case MysqlDefs.FIELD_TYPE_VARCHAR:
					case MysqlDefs.FIELD_TYPE_DECIMAL:
					case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
						Object value = paramArg[j].value;
	
						if (value instanceof byte[]) {
							batchedStatement.setBytes(batchedParamIndex,
									(byte[]) value);
						} else {
							batchedStatement.setString(batchedParamIndex,
									(String) value);
						}
	
						// If we ended up here as a multi-statement, we're not working with a server prepared statement
						
						if (batchedStatement instanceof ServerPreparedStatement) {
							BindValue asBound = ((ServerPreparedStatement) batchedStatement)
									.getBinding(
											batchedParamIndex,
											false);
							asBound.bufferType = paramArg[j].bufferType;
						}
	
						batchedParamIndex++;
	
						break;
					default:
						throw new IllegalArgumentException(
								"Unknown type when re-binding parameter into batched statement for parameter index "
										+ batchedParamIndex);
					}
				}
			}
		}

		return batchedParamIndex;
	}

	protected boolean containsOnDuplicateKeyUpdateInSQL() {
		return this.hasOnDuplicateKeyUpdate;
	}
	
	protected PreparedStatement prepareBatchedInsertSQL(ConnectionImpl localConn, int numBatches) throws SQLException {
		try {
			PreparedStatement pstmt = new ServerPreparedStatement(localConn, this.parseInfo.getSqlForBatch(numBatches), this.currentCatalog, this.resultSetConcurrency, this.resultSetType);
			pstmt.setRetrieveGeneratedKeys(this.retrieveGeneratedKeys);
			
			return pstmt;
		} catch (UnsupportedEncodingException e) {
			SQLException sqlEx = SQLError.createSQLException("Unable to prepare batch statement", SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
			sqlEx.initCause(e);
			
			throw sqlEx;
		}
		
	}
}
