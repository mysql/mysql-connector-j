/*
 Copyright (C) 2002-2004 MySQL AB

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import com.mysql.jdbc.exceptions.JDBC40NotYetImplementedException;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.jdbc.profiler.ProfilerEvent;

/**
 * A SQL Statement is pre-compiled and stored in a PreparedStatement object.
 * This object can then be used to efficiently execute this statement multiple
 * times.
 * 
 * <p>
 * <B>Note:</B> The setXXX methods for setting IN parameter values must specify
 * types that are compatible with the defined SQL type of the input parameter.
 * For instance, if the IN parameter has SQL type Integer, then setInt should be
 * used.
 * </p>
 * 
 * <p>
 * If arbitrary parameter type conversions are required, then the setObject
 * method should be used with a target SQL type.
 * </p>
 * 
 * @author Mark Matthews
 * @version $Id: PreparedStatement.java,v 1.1.2.1 2005/05/13 18:58:38 mmatthews
 *          Exp $
 * 
 * @see java.sql.ResultSet
 * @see java.sql.PreparedStatement
 */
public class PreparedStatement extends com.mysql.jdbc.Statement implements
		java.sql.PreparedStatement {
	class BatchParams {
		boolean[] isNull = null;

		boolean[] isStream = null;

		InputStream[] parameterStreams = null;

		byte[][] parameterStrings = null;

		int[] streamLengths = null;

		BatchParams(byte[][] strings, InputStream[] streams,
				boolean[] isStreamFlags, int[] lengths, boolean[] isNullFlags) {
			//
			// Make copies
			//
			this.parameterStrings = new byte[strings.length][];
			this.parameterStreams = new InputStream[streams.length];
			this.isStream = new boolean[isStreamFlags.length];
			this.streamLengths = new int[lengths.length];
			this.isNull = new boolean[isNullFlags.length];
			System.arraycopy(strings, 0, this.parameterStrings, 0,
					strings.length);
			System.arraycopy(streams, 0, this.parameterStreams, 0,
					streams.length);
			System.arraycopy(isStreamFlags, 0, this.isStream, 0,
					isStreamFlags.length);
			System.arraycopy(lengths, 0, this.streamLengths, 0, lengths.length);
			System
					.arraycopy(isNullFlags, 0, this.isNull, 0,
							isNullFlags.length);
		}
	}

	class EndPoint {
		int begin;

		int end;

		EndPoint(int b, int e) {
			this.begin = b;
			this.end = e;
		}
	}

	class ParseInfo {
		char firstStmtChar = 0;

		boolean foundLimitClause = false;

		boolean foundLoadData = false;

		long lastUsed = 0;

		int statementLength = 0;

		byte[][] staticSql = null;

		/**
		 * 
		 */
		public ParseInfo(String sql, Connection conn,
				java.sql.DatabaseMetaData dbmd, String encoding,
				SingleByteCharsetConverter converter) throws SQLException {
			if (sql == null) {
				throw SQLError.createSQLException(Messages
						.getString("PreparedStatement.61"), //$NON-NLS-1$
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
			}

			this.lastUsed = System.currentTimeMillis();

			String quotedIdentifierString = dbmd.getIdentifierQuoteString();

			char quotedIdentifierChar = 0;

			if ((quotedIdentifierString != null)
					&& !quotedIdentifierString.equals(" ") //$NON-NLS-1$
					&& (quotedIdentifierString.length() > 0)) {
				quotedIdentifierChar = quotedIdentifierString.charAt(0);
			}

			this.statementLength = sql.length();

			ArrayList endpointList = new ArrayList();
			boolean inQuotes = false;
			char quoteChar = 0;
			boolean inQuotedId = false;
			int lastParmEnd = 0;
			int i;

			int stopLookingForLimitClause = this.statementLength - 5;

			this.foundLimitClause = false;
			
			boolean noBackslashEscapes = connection.isNoBackslashEscapesSet();
			
			for (i = 0; i < this.statementLength; ++i) {
				char c = sql.charAt(i);

				if ((this.firstStmtChar == 0) && !Character.isWhitespace(c)) {
					// Determine what kind of statement we're doing (_S_elect,
					// _I_nsert, etc.)
					this.firstStmtChar = Character.toUpperCase(c);
				}

				if (!noBackslashEscapes &&
						c == '\\' && i < (this.statementLength - 1)) {
					i++;
					continue; // next character is escaped
				}
				
				// are we in a quoted identifier?
				// (only valid when the id is not inside a 'string')
				if (!inQuotes && (quotedIdentifierChar != 0)
						&& (c == quotedIdentifierChar)) {
					inQuotedId = !inQuotedId;
				} else if (!inQuotedId) {
					//	only respect quotes when not in a quoted identifier
					
					if (inQuotes) {
						if (((c == '\'') || (c == '"')) && c == quoteChar) {
							if (i < (this.statementLength - 1) && sql.charAt(i + 1) == quoteChar) {
								i++; 
								continue; // inline quote escape
							}
							
							inQuotes = !inQuotes;
							quoteChar = 0;
						} else if (((c == '\'') || (c == '"')) && c == quoteChar) {
							inQuotes = !inQuotes;
							quoteChar = 0;
						}
					} else {
						if ((c == '\'') || (c == '"')) {
							inQuotes = true;
							quoteChar = c;
						} else if ((c == '\'') || (c == '"')) {
							inQuotes = true;
							quoteChar = c;
						}
					}
				}

				if ((c == '?') && !inQuotes && !inQuotedId) {
					endpointList.add(new int[] { lastParmEnd, i });
					lastParmEnd = i + 1;
				}

				if (!inQuotes && (i < stopLookingForLimitClause)) {
					if ((c == 'L') || (c == 'l')) {
						char posI1 = sql.charAt(i + 1);

						if ((posI1 == 'I') || (posI1 == 'i')) {
							char posM = sql.charAt(i + 2);

							if ((posM == 'M') || (posM == 'm')) {
								char posI2 = sql.charAt(i + 3);

								if ((posI2 == 'I') || (posI2 == 'i')) {
									char posT = sql.charAt(i + 4);

									if ((posT == 'T') || (posT == 't')) {
										foundLimitClause = true;
									}
								}
							}
						}
					}
				}
			}

			if (this.firstStmtChar == 'L') {
				if (StringUtils.startsWithIgnoreCaseAndWs(sql, "LOAD DATA")) { //$NON-NLS-1$
					this.foundLoadData = true;
				} else {
					this.foundLoadData = false;
				}
			} else {
				this.foundLoadData = false;
			}

			endpointList.add(new int[] { lastParmEnd, this.statementLength });
			this.staticSql = new byte[endpointList.size()][];
			char[] asCharArray = sql.toCharArray();

			for (i = 0; i < this.staticSql.length; i++) {
				int[] ep = (int[]) endpointList.get(i);
				int end = ep[1];
				int begin = ep[0];
				int len = end - begin;

				if (this.foundLoadData) {
					String temp = new String(asCharArray, begin, len);
					this.staticSql[i] = temp.getBytes();
				} else if (encoding == null) {
					byte[] buf = new byte[len];

					for (int j = 0; j < len; j++) {
						buf[j] = (byte) sql.charAt(begin + j);
					}

					this.staticSql[i] = buf;
				} else {
					if (converter != null) {
						this.staticSql[i] = StringUtils.getBytes(sql,
								converter, encoding, connection
										.getServerCharacterEncoding(), begin,
								len, connection.parserKnowsUnicode());
					} else {
						String temp = new String(asCharArray, begin, len);

						this.staticSql[i] = StringUtils.getBytes(temp,
								encoding, connection
										.getServerCharacterEncoding(),
								connection.parserKnowsUnicode(), conn);
					}
				}
			}
		}
	}

	private final static byte[] HEX_DIGITS = new byte[] { (byte) '0',
			(byte) '1', (byte) '2', (byte) '3', (byte) '4', (byte) '5',
			(byte) '6', (byte) '7', (byte) '8', (byte) '9', (byte) 'A',
			(byte) 'B', (byte) 'C', (byte) 'D', (byte) 'E', (byte) 'F' };

	/**
	 * Reads length bytes from reader into buf. Blocks until enough input is
	 * available
	 * 
	 * @param reader
	 *            DOCUMENT ME!
	 * @param buf
	 *            DOCUMENT ME!
	 * @param length
	 *            DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 * 
	 * @throws IOException
	 *             DOCUMENT ME!
	 */
	protected static int readFully(Reader reader, char[] buf, int length)
			throws IOException {
		int numCharsRead = 0;

		while (numCharsRead < length) {
			int count = reader.read(buf, numCharsRead, length - numCharsRead);

			if (count < 0) {
				break;
			}

			numCharsRead += count;
		}

		return numCharsRead;
	}

	/**
	 * Does the batch (if any) contain "plain" statements added by
	 * Statement.addBatch(String)?
	 * 
	 * If so, we can't re-write it to use multi-value or multi-queries.
	 */
	protected boolean batchHasPlainStatements = false;

	private java.sql.DatabaseMetaData dbmd = null;

	/**
	 * What is the first character of the prepared statement (used to check for
	 * SELECT vs. INSERT/UPDATE/DELETE)
	 */
	protected char firstCharOfStmt = 0;

	/** Does the SQL for this statement contain a 'limit' clause? */
	protected boolean hasLimitClause = false;
	
	/** Is this query a LOAD DATA query? */
	protected boolean isLoadDataQuery = false;

	private boolean[] isNull = null;

	private boolean[] isStream = null;

	protected int numberOfExecutions = 0;

	/** The SQL that was passed in to 'prepare' */
	protected String originalSql = null;

	/** The number of parameters in this PreparedStatement */
	protected int parameterCount;

	protected MysqlParameterMetadata parameterMetaData;

	private InputStream[] parameterStreams = null;

	private byte[][] parameterValues = null;

	private ParseInfo parseInfo;

	private java.sql.ResultSetMetaData pstmtResultMetaData;

	private byte[][] staticSqlStrings = null;

	private byte[] streamConvertBuf = new byte[4096];

	private int[] streamLengths = null;

	private SimpleDateFormat tsdf = null;

	/**
	 * Are we using a version of MySQL where we can use 'true' boolean values?
	 */
	protected boolean useTrueBoolean = false;

	private boolean usingAnsiMode;

	private String batchedValuesClause;

	/**
	 * Constructor used by server-side prepared statements
	 * 
	 * @param conn
	 *            the connection that created us
	 * @param catalog
	 *            the catalog in use when we were created
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	protected PreparedStatement(Connection conn, String catalog)
			throws SQLException {
		super(conn, catalog);
	}

	/**
	 * Constructor for the PreparedStatement class.
	 * 
	 * @param conn
	 *            the connection creating this statement
	 * @param sql
	 *            the SQL for this statement
	 * @param catalog
	 *            the catalog/database this statement should be issued against
	 * 
	 * @throws SQLException
	 *             if a database error occurs.
	 */
	public PreparedStatement(Connection conn, String sql, String catalog)
			throws SQLException {
		super(conn, catalog);

		if (sql == null) {
			throw SQLError.createSQLException(Messages.getString("PreparedStatement.0"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}

		this.originalSql = sql;

		this.dbmd = this.connection.getMetaData();

		this.useTrueBoolean = this.connection.versionMeetsMinimum(3, 21, 23);

		this.parseInfo = new ParseInfo(sql, this.connection, this.dbmd,
				this.charEncoding, this.charConverter);

		initializeFromParseInfo();
	}

	/**
	 * Creates a new PreparedStatement object.
	 * 
	 * @param conn
	 *            the connection creating this statement
	 * @param sql
	 *            the SQL for this statement
	 * @param catalog
	 *            the catalog/database this statement should be issued against
	 * @param cachedParseInfo
	 *            already created parseInfo.
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public PreparedStatement(Connection conn, String sql, String catalog,
			ParseInfo cachedParseInfo) throws SQLException {
		super(conn, catalog);

		if (sql == null) {
			throw SQLError.createSQLException(Messages.getString("PreparedStatement.1"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}

		this.originalSql = sql;

		this.dbmd = this.connection.getMetaData();

		this.useTrueBoolean = this.connection.versionMeetsMinimum(3, 21, 23);

		this.parseInfo = cachedParseInfo;

		this.usingAnsiMode = !this.connection.useAnsiQuotedIdentifiers();

		initializeFromParseInfo();
	}

	/**
	 * JDBC 2.0 Add a set of parameters to the batch.
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs.
	 * 
	 * @see Statement#addBatch
	 */
	public void addBatch() throws SQLException {
		if (this.batchedArgs == null) {
			this.batchedArgs = new ArrayList();
		}

		this.batchedArgs.add(new BatchParams(this.parameterValues,
				this.parameterStreams, this.isStream, this.streamLengths,
				this.isNull));
	}

	public synchronized void addBatch(String sql) throws SQLException {
		this.batchHasPlainStatements = true;

		super.addBatch(sql);
	}

	protected String asSql() throws SQLException {
		return asSql(false);
	}

	protected String asSql(boolean quoteStreamsAndUnknowns) throws SQLException {
		if (this.isClosed) {
			return "statement has been closed, no further internal information available";
		}
		
		StringBuffer buf = new StringBuffer();

		try {
			for (int i = 0; i < this.parameterCount; ++i) {
				if (this.charEncoding != null) {
					buf.append(new String(this.staticSqlStrings[i],
							this.charEncoding));
				} else {
					buf.append(new String(this.staticSqlStrings[i]));
				}

				if ((this.parameterValues[i] == null) && !this.isStream[i]) {
					if (quoteStreamsAndUnknowns) {
						buf.append("'");
					}

					buf.append("** NOT SPECIFIED **"); //$NON-NLS-1$

					if (quoteStreamsAndUnknowns) {
						buf.append("'");
					}
				} else if (this.isStream[i]) {
					if (quoteStreamsAndUnknowns) {
						buf.append("'");
					}

					buf.append("** STREAM DATA **"); //$NON-NLS-1$

					if (quoteStreamsAndUnknowns) {
						buf.append("'");
					}
				} else {
					if (this.charConverter != null) {
						buf.append(this.charConverter
								.toString(this.parameterValues[i]));
					} else {
						if (this.charEncoding != null) {
							buf.append(new String(this.parameterValues[i],
									this.charEncoding));
						} else {
							buf.append(StringUtils
									.toAsciiString(this.parameterValues[i]));
						}
					}
				}
			}

			if (this.charEncoding != null) {
				buf.append(new String(
						this.staticSqlStrings[this.parameterCount],
						this.charEncoding));
			} else {
				buf
						.append(StringUtils
								.toAsciiString(this.staticSqlStrings[this.parameterCount]));
			}
		} catch (UnsupportedEncodingException uue) {
			throw new RuntimeException(Messages
					.getString("PreparedStatement.32") //$NON-NLS-1$
					+ this.charEncoding
					+ Messages.getString("PreparedStatement.33")); //$NON-NLS-1$
		}

		return buf.toString();
	}

	public synchronized void clearBatch() throws SQLException {
		this.batchHasPlainStatements = false;

		super.clearBatch();
	}

	/**
	 * In general, parameter values remain in force for repeated used of a
	 * Statement. Setting a parameter value automatically clears its previous
	 * value. However, in some cases, it is useful to immediately release the
	 * resources used by the current parameter values; this can be done by
	 * calling clearParameters
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public synchronized void clearParameters() throws SQLException {
		checkClosed();
		
		for (int i = 0; i < this.parameterValues.length; i++) {
			this.parameterValues[i] = null;
			this.parameterStreams[i] = null;
			this.isStream[i] = false;
			this.isNull[i] = false;
		}
	}

	/**
	 * Closes this prepared statement and releases all resources.
	 * 
	 * @throws SQLException
	 *             if database error occurs.
	 */
	public synchronized void close() throws SQLException {
		realClose(true, true);
	}

	private final void escapeblockFast(byte[] buf, Buffer packet, int size)
			throws SQLException {
		int lastwritten = 0;

		for (int i = 0; i < size; i++) {
			byte b = buf[i];

			if (b == '\0') {
				// write stuff not yet written
				if (i > lastwritten) {
					packet.writeBytesNoNull(buf, lastwritten, i - lastwritten);
				}

				// write escape
				packet.writeByte((byte) '\\');
				packet.writeByte((byte) '0');
				lastwritten = i + 1;
			} else {
				if ((b == '\\') || (b == '\'')
						|| (!this.usingAnsiMode && b == '"')) {
					// write stuff not yet written
					if (i > lastwritten) {
						packet.writeBytesNoNull(buf, lastwritten, i
								- lastwritten);
					}

					// write escape
					packet.writeByte((byte) '\\');
					lastwritten = i; // not i+1 as b wasn't written.
				}
			}
		}

		// write out remaining stuff from buffer
		if (lastwritten < size) {
			packet.writeBytesNoNull(buf, lastwritten, size - lastwritten);
		}
	}

	private final void escapeblockFast(byte[] buf,
			ByteArrayOutputStream bytesOut, int size) {
		int lastwritten = 0;

		for (int i = 0; i < size; i++) {
			byte b = buf[i];

			if (b == '\0') {
				// write stuff not yet written
				if (i > lastwritten) {
					bytesOut.write(buf, lastwritten, i - lastwritten);
				}

				// write escape
				bytesOut.write('\\');
				bytesOut.write('0');
				lastwritten = i + 1;
			} else {
				if ((b == '\\') || (b == '\'')
						|| (!this.usingAnsiMode && b == '"')) {
					// write stuff not yet written
					if (i > lastwritten) {
						bytesOut.write(buf, lastwritten, i - lastwritten);
					}

					// write escape
					bytesOut.write('\\');
					lastwritten = i; // not i+1 as b wasn't written.
				}
			}
		}

		// write out remaining stuff from buffer
		if (lastwritten < size) {
			bytesOut.write(buf, lastwritten, size - lastwritten);
		}
	}

	/**
	 * Some prepared statements return multiple results; the execute method
	 * handles these complex statements as well as the simpler form of
	 * statements handled by executeQuery and executeUpdate
	 * 
	 * @return true if the next result is a ResultSet; false if it is an update
	 *         count or there are no more results
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public boolean execute() throws SQLException {
		if (this.connection.isReadOnly() && (this.firstCharOfStmt != 'S')) {
			throw SQLError.createSQLException(Messages.getString("PreparedStatement.20") //$NON-NLS-1$
					+ Messages.getString("PreparedStatement.21"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}

		checkClosed();

		ResultSet rs = null;

		synchronized (this.connection.getMutex()) {
			clearWarnings();

			this.batchedGeneratedKeys = null;

			Buffer sendPacket = fillSendPacket();

			String oldCatalog = null;

			if (!this.connection.getCatalog().equals(this.currentCatalog)) {
				oldCatalog = this.connection.getCatalog();
				this.connection.setCatalog(this.currentCatalog);
			}

			boolean oldInfoMsgState = false;

			if (this.retrieveGeneratedKeys) {
				oldInfoMsgState = this.connection.isReadInfoMsgEnabled();
				this.connection.setReadInfoMsgEnabled(true);
			}

			// If there isn't a limit clause in the SQL
			// then limit the number of rows to return in
			// an efficient manner. Only do this if
			// setMaxRows() hasn't been used on any Statements
			// generated from the current Connection (saves
			// a query, and network traffic).
			//
			// Only apply max_rows to selects
			//
			if (this.connection.useMaxRows()) {
				int rowLimit = -1;

				if (this.firstCharOfStmt == 'S') {
					if (this.hasLimitClause) {
						rowLimit = this.maxRows;
					} else {
						if (this.maxRows <= 0) {
							this.connection.execSQL(this,
									"SET OPTION SQL_SELECT_LIMIT=DEFAULT", -1, //$NON-NLS-1$
									null, java.sql.ResultSet.TYPE_FORWARD_ONLY,
									java.sql.ResultSet.CONCUR_READ_ONLY, false,
									this.currentCatalog, true);
						} else {
							this.connection
									.execSQL(
											this,
											"SET OPTION SQL_SELECT_LIMIT=" + this.maxRows, -1, //$NON-NLS-1$
											null,
											java.sql.ResultSet.TYPE_FORWARD_ONLY,
											java.sql.ResultSet.CONCUR_READ_ONLY,
											false, this.currentCatalog,
											true);
						}
					}
				} else {
					this.connection.execSQL(this,
							"SET OPTION SQL_SELECT_LIMIT=DEFAULT", -1, null, //$NON-NLS-1$
							java.sql.ResultSet.TYPE_FORWARD_ONLY,
							java.sql.ResultSet.CONCUR_READ_ONLY, false,
							this.currentCatalog, true);
				}

				// Finally, execute the query
				rs = executeInternal(rowLimit, sendPacket,
						createStreamingResultSet(),
						(this.firstCharOfStmt == 'S'), true, false);
			} else {
				rs = executeInternal(-1, sendPacket,
						createStreamingResultSet(),
						(this.firstCharOfStmt == 'S'), true, false);
			}

			if (this.retrieveGeneratedKeys) {
				this.connection.setReadInfoMsgEnabled(oldInfoMsgState);
				rs.setFirstCharOfQuery('R');
			}

			if (oldCatalog != null) {
				this.connection.setCatalog(oldCatalog);
			}

			this.lastInsertId = rs.getUpdateID();

			if (rs != null) {
				this.results = rs;
			}
		}

		return ((rs != null) && rs.reallyResult());
	}

	/**
	 * JDBC 2.0 Submit a batch of commands to the database for execution. This
	 * method is optional.
	 * 
	 * @return an array of update counts containing one element for each command
	 *         in the batch. The array is ordered according to the order in
	 *         which commands were inserted into the batch
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs, or the driver does not
	 *                support batch statements
	 * @throws java.sql.BatchUpdateException
	 *             DOCUMENT ME!
	 */
	public int[] executeBatch() throws SQLException {
		checkClosed();
		
		if (this.connection.isReadOnly()) {
			throw new SQLException(Messages.getString("PreparedStatement.25") //$NON-NLS-1$
					+ Messages.getString("PreparedStatement.26"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}

		synchronized (this.connection.getMutex()) {
			try {
				clearWarnings();

				if (!this.batchHasPlainStatements
						&& this.connection.getRewriteBatchedStatements()) {
					if (StringUtils.startsWithIgnoreCaseAndWs(this.originalSql,
							"INSERT")) {
						return executeBatchedInserts();
					}
				}

				return executeBatchSerially();
			} finally {
				clearBatch();
			}
		}
	}

	/**
	 * Rewrites the already prepared statement into a multi-value insert
	 * statement of 'statementsPerBatch' values and executes the entire batch
	 * using this new statement.
	 * 
	 * @return update counts in the same fashion as executeBatch()
	 * 
	 * @throws SQLException
	 */
	private int[] executeBatchedInserts() throws SQLException {
		String valuesClause = extractValuesClause();

		Connection locallyScopedConn = this.connection;
		
		if (valuesClause == null) {
			return executeBatchSerially();
		}

		int numBatchedArgs = this.batchedArgs.size();
		
		if (this.retrieveGeneratedKeys) {
			this.batchedGeneratedKeys = new ArrayList(numBatchedArgs);
		}

		int numValuesPerBatch = computeBatchSize(numBatchedArgs);

		if (numBatchedArgs < numValuesPerBatch) {
			numValuesPerBatch = numBatchedArgs;
		}

		java.sql.PreparedStatement batchedStatement = null;

		if (this.retrieveGeneratedKeys) {
			batchedStatement = locallyScopedConn.prepareStatement(
					generateBatchedInsertSQL(valuesClause, numValuesPerBatch),
					RETURN_GENERATED_KEYS);
		} else {
			batchedStatement = locallyScopedConn
					.prepareStatement(generateBatchedInsertSQL(valuesClause,
							numValuesPerBatch));
		}

		int batchedParamIndex = 1;
		int updateCountRunningTotal = 0;
		int numberToExecuteAsMultiValue = 0;
		int batchCounter = 0;

		if (numBatchedArgs < numValuesPerBatch) {
			numberToExecuteAsMultiValue = numBatchedArgs;
		} else {
			numberToExecuteAsMultiValue = numBatchedArgs / numValuesPerBatch;
		}

		int numberArgsToExecute = numberToExecuteAsMultiValue * numValuesPerBatch;

		for (int i = 0; i < numberArgsToExecute; i++) {
			if (i != 0 && i % numValuesPerBatch == 0) {
				updateCountRunningTotal += batchedStatement.executeUpdate();

				getBatchedGeneratedKeys(batchedStatement);
				batchedStatement.clearParameters();
				batchedParamIndex = 1;

			}

			BatchParams paramArg = (BatchParams) this.batchedArgs
					.get(batchCounter++);

			batchedParamIndex = setOneBatchedParameterSet(batchedStatement,
					batchedParamIndex, paramArg);
		}

		updateCountRunningTotal += batchedStatement.executeUpdate();
		getBatchedGeneratedKeys(batchedStatement);

		numValuesPerBatch = numBatchedArgs - batchCounter;

		if (numValuesPerBatch > 0) {

			batchedStatement = locallyScopedConn.prepareStatement(
					generateBatchedInsertSQL(valuesClause, numValuesPerBatch),
					RETURN_GENERATED_KEYS);
			batchedParamIndex = 1;

			while (batchCounter < numBatchedArgs) {

				BatchParams paramArg = (BatchParams) this.batchedArgs
						.get(batchCounter++);
				batchedParamIndex = setOneBatchedParameterSet(batchedStatement,
						batchedParamIndex, paramArg);
			}

			updateCountRunningTotal += batchedStatement.executeUpdate();
			getBatchedGeneratedKeys(batchedStatement);
		}

		int[] updateCounts = new int[this.batchedArgs.size()];

		for (int i = 0; i < this.batchedArgs.size(); i++) {
			updateCounts[i] = 1;
		}

		return updateCounts;
	}

	protected int computeBatchSize(int numBatchedArgs) {
		long sizeOfEntireBatch = 0;
		long maxSizeOfParameterSet = 0;
		
		for (int i = 0; i < numBatchedArgs; i++) {
			BatchParams paramArg = (BatchParams) this.batchedArgs
			.get(i);

			boolean[] isNullBatch = paramArg.isNull;
			boolean[] isStreamBatch = paramArg.isStream;

			long sizeOfParameterSet = 0;
			
			for (int j = 0; j < isNullBatch.length; j++) {
				if (!isNullBatch[j]) {

					if (isStreamBatch[j]) {
						int streamLength = paramArg.streamLengths[j];
						
						if (streamLength != -1) {
							sizeOfParameterSet += streamLength * 2; // for safety in escaping
						} else {
							int paramLength = paramArg.parameterStrings[j].length;
							sizeOfParameterSet += paramLength;
						}
					} else {
						sizeOfParameterSet += 4; // for NULL literal in SQL 
					}
				}
			}
			
			//
			// Account for static part of values clause
			// This is a little naiive, because the ?s will be replaced
			// but it gives us some padding, and is less housekeeping
			// to ignore them. We're looking for a "fuzzy" value here
			// anyway
			//
			
			sizeOfParameterSet += this.batchedValuesClause.length() + 1; 
			sizeOfEntireBatch += sizeOfParameterSet;
			
			if (sizeOfParameterSet > maxSizeOfParameterSet) {
				maxSizeOfParameterSet = sizeOfParameterSet;
			}
		}
		
		int maxAllowedPacket = this.connection.getMaxAllowedPacket();
		
		if (sizeOfEntireBatch < maxAllowedPacket - this.originalSql.length()) {
			return numBatchedArgs;
		}
		
		return (int)Math.max(1, (maxAllowedPacket - this.originalSql.length()) / maxSizeOfParameterSet);
	}

	/**
	 * Executes the current batch of statements by executing them one-by-one.
	 * 
	 * @return a list of update counts
	 * @throws SQLException
	 *             if an error occurs
	 */
	protected int[] executeBatchSerially() throws SQLException {
		
		Connection locallyScopedConn = this.connection;
		
		if (locallyScopedConn == null) {
			checkClosed();
		}

		int[] updateCounts = null;

		if (this.batchedArgs != null) {
			int nbrCommands = this.batchedArgs.size();
			updateCounts = new int[nbrCommands];

			for (int i = 0; i < nbrCommands; i++) {
				updateCounts[i] = -3;
			}

			SQLException sqlEx = null;

			int commandIndex = 0;

			if (this.retrieveGeneratedKeys) {
				this.batchedGeneratedKeys = new ArrayList(nbrCommands);
			}

			for (commandIndex = 0; commandIndex < nbrCommands; commandIndex++) {
				Object arg = this.batchedArgs.get(commandIndex);

				if (arg instanceof String) {
					updateCounts[commandIndex] = executeUpdate((String) arg);
				} else {
					BatchParams paramArg = (BatchParams) arg;

					try {
						updateCounts[commandIndex] = executeUpdate(
								paramArg.parameterStrings,
								paramArg.parameterStreams, paramArg.isStream,
								paramArg.streamLengths, paramArg.isNull, true);

						if (this.retrieveGeneratedKeys) {
							java.sql.ResultSet rs = null;

							try {
								rs = getGeneratedKeysInternal();

								while (rs.next()) {
									this.batchedGeneratedKeys
											.add(new byte[][] { rs.getBytes(1) });
								}
							} finally {
								if (rs != null) {
									rs.close();
								}
							}
						}
					} catch (SQLException ex) {
						updateCounts[commandIndex] = EXECUTE_FAILED;

						if (locallyScopedConn.getContinueBatchOnError()) {
							sqlEx = ex;
						} else {
							int[] newUpdateCounts = new int[commandIndex];
							System.arraycopy(updateCounts, 0, newUpdateCounts,
									0, commandIndex);

							throw new java.sql.BatchUpdateException(ex
									.getMessage(), ex.getSQLState(), ex
									.getErrorCode(), newUpdateCounts);
						}
					}
				}
			}

			if (sqlEx != null) {
				throw new java.sql.BatchUpdateException(sqlEx.getMessage(),
						sqlEx.getSQLState(), sqlEx.getErrorCode(), updateCounts);
			}
		}

		return (updateCounts != null) ? updateCounts : new int[0];
	}

	/**
	 * Actually execute the prepared statement. This is here so server-side
	 * PreparedStatements can re-use most of the code from this class.
	 * 
	 * @param maxRowsToRetrieve
	 *            the max number of rows to return
	 * @param sendPacket
	 *            the packet to send
	 * @param createStreamingResultSet
	 *            should a 'streaming' result set be created?
	 * @param queryIsSelectOnly
	 *            is this query doing a SELECT?
	 * @param unpackFields
	 *            DOCUMENT ME!
	 * 
	 * @return the results as a ResultSet
	 * 
	 * @throws SQLException
	 *             if an error occurs.
	 */
	protected ResultSet executeInternal(int maxRowsToRetrieve,
			Buffer sendPacket, boolean createStreamingResultSet,
			boolean queryIsSelectOnly, boolean unpackFields, boolean isBatch)
			throws SQLException {
		this.wasCancelled = false;
		
		Connection locallyScopedConnection= this.connection;
		
		this.numberOfExecutions++;

		ResultSet rs;
		
		CancelTask timeoutTask = null;

		try {
			if (this.timeoutInMillis != 0
					&& locallyScopedConnection.versionMeetsMinimum(5, 0, 0)) {
				timeoutTask = new CancelTask();
				Connection.getCancelTimer().schedule(timeoutTask, 
						this.timeoutInMillis);
			}
			
			rs = locallyScopedConnection.execSQL(this, null, maxRowsToRetrieve, sendPacket,
				this.resultSetType, this.resultSetConcurrency,
				createStreamingResultSet, this.currentCatalog,
				unpackFields, isBatch);
			
			if (timeoutTask != null) {
				timeoutTask.cancel();
				timeoutTask = null;
			}
		
			if (this.wasCancelled) {
				this.wasCancelled = false;
				throw new MySQLTimeoutException();
			}
		} finally {
			if (timeoutTask != null) {
				timeoutTask.cancel();
			}
		}

		return rs;
	}

	/**
	 * A Prepared SQL query is executed and its ResultSet is returned
	 * 
	 * @return a ResultSet that contains the data produced by the query - never
	 *         null
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public java.sql.ResultSet executeQuery() throws SQLException {
		checkClosed();
		
		Connection locallyScopedConn = this.connection;
		
		checkForDml(this.originalSql, this.firstCharOfStmt);

		CachedResultSetMetaData cachedMetadata = null;

		// We need to execute this all together
		// So synchronize on the Connection's mutex (because
		// even queries going through there synchronize
		// on the same mutex.
		synchronized (locallyScopedConn.getMutex()) {
			clearWarnings();

			this.batchedGeneratedKeys = null;

			Buffer sendPacket = fillSendPacket();

			if (this.results != null) {
				if (!this.connection.getHoldResultsOpenOverStatementClose()) {
					if (!this.holdResultsOpenOverClose) {
						this.results.realClose(false);
					}
				}
			}

			String oldCatalog = null;

			if (!locallyScopedConn.getCatalog().equals(this.currentCatalog)) {
				oldCatalog = locallyScopedConn.getCatalog();
				locallyScopedConn.setCatalog(this.currentCatalog);
			}

			//
			// Check if we have cached metadata for this query...
			//
			if (locallyScopedConn.getCacheResultSetMetadata()) {
				cachedMetadata = getCachedMetaData(this.originalSql);
			}

			if (locallyScopedConn.useMaxRows()) {
				// If there isn't a limit clause in the SQL
				// then limit the number of rows to return in
				// an efficient manner. Only do this if
				// setMaxRows() hasn't been used on any Statements
				// generated from the current Connection (saves
				// a query, and network traffic).
				if (this.hasLimitClause) {
					this.results = executeInternal(this.maxRows, sendPacket,
							createStreamingResultSet(), true,
							(cachedMetadata == null), false);
				} else {
					if (this.maxRows <= 0) {
						locallyScopedConn
								.execSQL(
										this,
										"SET OPTION SQL_SELECT_LIMIT=DEFAULT", -1, null, //$NON-NLS-1$
										java.sql.ResultSet.TYPE_FORWARD_ONLY,
										java.sql.ResultSet.CONCUR_READ_ONLY,
										false, this.currentCatalog, true);
					} else {
						locallyScopedConn
								.execSQL(
										this,
										"SET OPTION SQL_SELECT_LIMIT=" + this.maxRows, -1, null, //$NON-NLS-1$
										java.sql.ResultSet.TYPE_FORWARD_ONLY,
										java.sql.ResultSet.CONCUR_READ_ONLY,
										false, this.currentCatalog, true);
					}

					this.results = executeInternal(-1, sendPacket,
							createStreamingResultSet(), true,
							(cachedMetadata == null), false);

					if (oldCatalog != null) {
						this.connection.setCatalog(oldCatalog);
					}
				}
			} else {
				this.results = executeInternal(-1, sendPacket,
						createStreamingResultSet(), true,
						(cachedMetadata == null), false);
			}

			if (oldCatalog != null) {
				locallyScopedConn.setCatalog(oldCatalog);
			}
		}

		this.lastInsertId = this.results.getUpdateID();

		if (cachedMetadata != null) {
			initializeResultsMetadataFromCache(this.originalSql,
					cachedMetadata, this.results);
		} else {
			if (this.connection.getCacheResultSetMetadata()) {
				initializeResultsMetadataFromCache(this.originalSql,
						null /* will be created */, this.results);
			}
		}

		return this.results;
	}

	/**
	 * Execute a SQL INSERT, UPDATE or DELETE statement. In addition, SQL
	 * statements that return nothing such as SQL DDL statements can be
	 * executed.
	 * 
	 * @return either the row count for INSERT, UPDATE or DELETE; or 0 for SQL
	 *         statements that return nothing.
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public int executeUpdate() throws SQLException {
		return executeUpdate(true, false);
	}

	/*
	 * We need this variant, because ServerPreparedStatement calls this for
	 * batched updates, which will end up clobbering the warnings and generated
	 * keys we need to gather for the batch.
	 */
	protected int executeUpdate(
			boolean clearBatchedGeneratedKeysAndWarnings, boolean isBatch) throws SQLException {
		if (clearBatchedGeneratedKeysAndWarnings) {
			clearWarnings();
			this.batchedGeneratedKeys = null;
		}

		return executeUpdate(this.parameterValues, this.parameterStreams,
				this.isStream, this.streamLengths, this.isNull, isBatch);
	}

	/**
	 * Added to allow batch-updates
	 * 
	 * @param batchedParameterStrings
	 *            string values used in single statement
	 * @param batchedParameterStreams
	 *            stream values used in single statement
	 * @param batchedIsStream
	 *            flags for streams used in single statement
	 * @param batchedStreamLengths
	 *            lengths of streams to be read.
	 * @param batchedIsNull
	 *            flags for parameters that are null
	 * 
	 * @return the update count
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	protected int executeUpdate(byte[][] batchedParameterStrings,
			InputStream[] batchedParameterStreams, boolean[] batchedIsStream,
			int[] batchedStreamLengths, boolean[] batchedIsNull, boolean isReallyBatch)
			throws SQLException {

		checkClosed();

		Connection locallyScopedConn = this.connection;
		
		if (locallyScopedConn.isReadOnly()) {
			throw SQLError.createSQLException(Messages.getString("PreparedStatement.34") //$NON-NLS-1$
					+ Messages.getString("PreparedStatement.35"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}

		if ((this.firstCharOfStmt == 'S')
				&& StringUtils.startsWithIgnoreCaseAndWs(this.originalSql,
						"SELECT")) { //$NON-NLS-1$
			throw SQLError.createSQLException(Messages.getString("PreparedStatement.37"), //$NON-NLS-1$
					"01S03"); //$NON-NLS-1$
		}

		if (this.results != null) {
			if (!locallyScopedConn.getHoldResultsOpenOverStatementClose()) {
				this.results.realClose(false);
			}
		}

		ResultSet rs = null;

		// The checking and changing of catalogs
		// must happen in sequence, so synchronize
		// on the same mutex that _conn is using
		synchronized (locallyScopedConn.getMutex()) {
			Buffer sendPacket = fillSendPacket(batchedParameterStrings,
					batchedParameterStreams, batchedIsStream,
					batchedStreamLengths);

			String oldCatalog = null;

			if (!locallyScopedConn.getCatalog().equals(this.currentCatalog)) {
				oldCatalog = locallyScopedConn.getCatalog();
				locallyScopedConn.setCatalog(this.currentCatalog);
			}

			//
			// Only apply max_rows to selects
			//
			if (locallyScopedConn.useMaxRows()) {
				locallyScopedConn.execSQL(this,
						"SET OPTION SQL_SELECT_LIMIT=DEFAULT", -1, null, //$NON-NLS-1$
						java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY, false,
						this.currentCatalog, true);
			}

			boolean oldInfoMsgState = false;

			if (this.retrieveGeneratedKeys) {
				oldInfoMsgState = locallyScopedConn.isReadInfoMsgEnabled();
				locallyScopedConn.setReadInfoMsgEnabled(true);
			}

			rs = executeInternal(-1, sendPacket, false, false, true, isReallyBatch);

			if (this.retrieveGeneratedKeys) {
				locallyScopedConn.setReadInfoMsgEnabled(oldInfoMsgState);
				rs.setFirstCharOfQuery(this.firstCharOfStmt);
			}

			if (oldCatalog != null) {
				locallyScopedConn.setCatalog(oldCatalog);
			}
		}

		this.results = rs;

		this.updateCount = rs.getUpdateCount();

		int truncatedUpdateCount = 0;

		if (this.updateCount > Integer.MAX_VALUE) {
			truncatedUpdateCount = Integer.MAX_VALUE;
		} else {
			truncatedUpdateCount = (int) this.updateCount;
		}

		this.lastInsertId = rs.getUpdateID();

		return truncatedUpdateCount;
	}

	private String extractValuesClause() throws SQLException {
		if (this.batchedValuesClause == null) {
			String quoteCharStr = this.connection.getMetaData()
					.getIdentifierQuoteString();
	
			int indexOfValues = -1;
	
			if (quoteCharStr.length() > 0) {
				indexOfValues = StringUtils.indexOfIgnoreCaseRespectQuotes(0,
						this.originalSql, "VALUES ", quoteCharStr.charAt(0), false);
			} else {
				indexOfValues = StringUtils.indexOfIgnoreCase(0, this.originalSql,
						"VALUES ");
			}
	
			if (indexOfValues == -1) {
				return null;
			}
	
			int indexOfFirstParen = this.originalSql
					.indexOf('(', indexOfValues + 7);
	
			if (indexOfFirstParen == -1) {
				return null;
			}
	
			int indexOfLastParen = this.originalSql.lastIndexOf(')');
	
			if (indexOfLastParen == -1) {
				return null;
			}
	
			this.batchedValuesClause = this.originalSql.substring(indexOfFirstParen,
					indexOfLastParen + 1);
		}
			
		return this.batchedValuesClause;
	}

	/**
	 * Creates the packet that contains the query to be sent to the server.
	 * 
	 * @return A Buffer filled with the query representing the
	 *         PreparedStatement.
	 * 
	 * @throws SQLException
	 *             if an error occurs.
	 */
	protected Buffer fillSendPacket() throws SQLException {
		return fillSendPacket(this.parameterValues, this.parameterStreams,
				this.isStream, this.streamLengths);
	}

	/**
	 * Creates the packet that contains the query to be sent to the server.
	 * 
	 * @param batchedParameterStrings
	 *            the parameters as strings
	 * @param batchedParameterStreams
	 *            the parameters as streams
	 * @param batchedIsStream
	 *            is the given parameter a stream?
	 * @param batchedStreamLengths
	 *            the lengths of the streams (if appropriate)
	 * 
	 * @return a Buffer filled with the query that represents this statement
	 * 
	 * @throws SQLException
	 *             if an error occurs.
	 */
	protected Buffer fillSendPacket(byte[][] batchedParameterStrings,
			InputStream[] batchedParameterStreams, boolean[] batchedIsStream,
			int[] batchedStreamLengths) throws SQLException {
		Buffer sendPacket = this.connection.getIO().getSharedSendPacket();

		sendPacket.clear();

		sendPacket.writeByte((byte) MysqlDefs.QUERY);

		boolean useStreamLengths = this.connection
				.getUseStreamLengthsInPrepStmts();

		//
		// Try and get this allocation as close as possible
		// for BLOBs
		//
		int ensurePacketSize = 0;

		for (int i = 0; i < batchedParameterStrings.length; i++) {
			if (batchedIsStream[i] && useStreamLengths) {
				ensurePacketSize += batchedStreamLengths[i];
			}
		}

		if (ensurePacketSize != 0) {
			sendPacket.ensureCapacity(ensurePacketSize);
		}

		for (int i = 0; i < batchedParameterStrings.length; i++) {
			if ((batchedParameterStrings[i] == null)
					&& (batchedParameterStreams[i] == null)) {
				throw SQLError.createSQLException(Messages
						.getString("PreparedStatement.40") //$NON-NLS-1$
						+ (i + 1), SQLError.SQL_STATE_WRONG_NO_OF_PARAMETERS);
			}

			sendPacket.writeBytesNoNull(this.staticSqlStrings[i]);

			if (batchedIsStream[i]) {
				streamToBytes(sendPacket, batchedParameterStreams[i], true,
						batchedStreamLengths[i], useStreamLengths);
			} else {
				sendPacket.writeBytesNoNull(batchedParameterStrings[i]);
			}
		}

		sendPacket
				.writeBytesNoNull(this.staticSqlStrings[batchedParameterStrings.length]);

		return sendPacket;
	}

	private String generateBatchedInsertSQL(String valuesClause, int numBatches) {
		StringBuffer newStatementSql = new StringBuffer(this.originalSql
				.length()
				+ (numBatches * (valuesClause.length() + 1)));

		newStatementSql.append(this.originalSql);

		for (int i = 0; i < numBatches - 1; i++) {
			newStatementSql.append(',');
			newStatementSql.append(valuesClause);
		}

		return newStatementSql.toString();
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param parameterIndex
	 *            DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public byte[] getBytesRepresentation(int parameterIndex)
			throws SQLException {
		if (this.isStream[parameterIndex]) {
			return streamToBytes(this.parameterStreams[parameterIndex], false,
					this.streamLengths[parameterIndex], this.connection
							.getUseStreamLengthsInPrepStmts());
		}

		byte[] parameterVal = this.parameterValues[parameterIndex];

		if (parameterVal == null) {
			return null;
		}

		if ((parameterVal[0] == '\'')
				&& (parameterVal[parameterVal.length - 1] == '\'')) {
			byte[] valNoQuotes = new byte[parameterVal.length - 2];
			System.arraycopy(parameterVal, 1, valNoQuotes, 0,
					parameterVal.length - 2);

			return valNoQuotes;
		}

		return parameterVal;
	}

	// --------------------------JDBC 2.0-----------------------------

	private final String getDateTimePattern(String dt, boolean toTime)
			throws Exception {
		//
		// Special case
		//
		int dtLength = (dt != null) ? dt.length() : 0;

		if ((dtLength >= 8) && (dtLength <= 10)) {
			int dashCount = 0;
			boolean isDateOnly = true;

			for (int i = 0; i < dtLength; i++) {
				char c = dt.charAt(i);

				if (!Character.isDigit(c) && (c != '-')) {
					isDateOnly = false;

					break;
				}

				if (c == '-') {
					dashCount++;
				}
			}

			if (isDateOnly && (dashCount == 2)) {
				return "yyyy-MM-dd"; //$NON-NLS-1$
			}
		}

		//
		// Special case - time-only
		//
		boolean colonsOnly = true;

		for (int i = 0; i < dtLength; i++) {
			char c = dt.charAt(i);

			if (!Character.isDigit(c) && (c != ':')) {
				colonsOnly = false;

				break;
			}
		}

		if (colonsOnly) {
			return "HH:mm:ss"; //$NON-NLS-1$
		}

		int n;
		int z;
		int count;
		int maxvecs;
		char c;
		char separator;
		StringReader reader = new StringReader(dt + " "); //$NON-NLS-1$
		ArrayList vec = new ArrayList();
		ArrayList vecRemovelist = new ArrayList();
		Object[] nv = new Object[3];
		Object[] v;
		nv[0] = new Character('y');
		nv[1] = new StringBuffer();
		nv[2] = new Integer(0);
		vec.add(nv);

		if (toTime) {
			nv = new Object[3];
			nv[0] = new Character('h');
			nv[1] = new StringBuffer();
			nv[2] = new Integer(0);
			vec.add(nv);
		}

		while ((z = reader.read()) != -1) {
			separator = (char) z;
			maxvecs = vec.size();

			for (count = 0; count < maxvecs; count++) {
				v = (Object[]) vec.get(count);
				n = ((Integer) v[2]).intValue();
				c = getSuccessor(((Character) v[0]).charValue(), n);

				if (!Character.isLetterOrDigit(separator)) {
					if ((c == ((Character) v[0]).charValue()) && (c != 'S')) {
						vecRemovelist.add(v);
					} else {
						((StringBuffer) v[1]).append(separator);

						if ((c == 'X') || (c == 'Y')) {
							v[2] = new Integer(4);
						}
					}
				} else {
					if (c == 'X') {
						c = 'y';
						nv = new Object[3];
						nv[1] = (new StringBuffer(((StringBuffer) v[1])
								.toString())).append('M');
						nv[0] = new Character('M');
						nv[2] = new Integer(1);
						vec.add(nv);
					} else if (c == 'Y') {
						c = 'M';
						nv = new Object[3];
						nv[1] = (new StringBuffer(((StringBuffer) v[1])
								.toString())).append('d');
						nv[0] = new Character('d');
						nv[2] = new Integer(1);
						vec.add(nv);
					}

					((StringBuffer) v[1]).append(c);

					if (c == ((Character) v[0]).charValue()) {
						v[2] = new Integer(n + 1);
					} else {
						v[0] = new Character(c);
						v[2] = new Integer(1);
					}
				}
			}

			int size = vecRemovelist.size();

			for (int i = 0; i < size; i++) {
				v = (Object[]) vecRemovelist.get(i);
				vec.remove(v);
			}

			vecRemovelist.clear();
		}

		int size = vec.size();

		for (int i = 0; i < size; i++) {
			v = (Object[]) vec.get(i);
			c = ((Character) v[0]).charValue();
			n = ((Integer) v[2]).intValue();

			boolean bk = getSuccessor(c, n) != c;
			boolean atEnd = (((c == 's') || (c == 'm') || ((c == 'h') && toTime)) && bk);
			boolean finishesAtDate = (bk && (c == 'd') && !toTime);
			boolean containsEnd = (((StringBuffer) v[1]).toString()
					.indexOf('W') != -1);

			if ((!atEnd && !finishesAtDate) || (containsEnd)) {
				vecRemovelist.add(v);
			}
		}

		size = vecRemovelist.size();

		for (int i = 0; i < size; i++) {
			vec.remove(vecRemovelist.get(i));
		}

		vecRemovelist.clear();
		v = (Object[]) vec.get(0); // might throw exception

		StringBuffer format = (StringBuffer) v[1];
		format.setLength(format.length() - 1);

		return format.toString();
	}

	/**
	 * The number, types and properties of a ResultSet's columns are provided by
	 * the getMetaData method.
	 * 
	 * @return the description of a ResultSet's columns
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public java.sql.ResultSetMetaData getMetaData()
			throws SQLException {

		if (!StringUtils.startsWithIgnoreCaseAndNonAlphaNumeric(
				this.originalSql, "SELECT")) {
			return null;
		}

		PreparedStatement mdStmt = null;
		java.sql.ResultSet mdRs = null;

		if (this.pstmtResultMetaData == null) {
			try {
				mdStmt = new PreparedStatement(this.connection,
						this.originalSql, this.currentCatalog, this.parseInfo);

				mdStmt.setMaxRows(0);

				int paramCount = this.parameterValues.length;

				for (int i = 1; i <= paramCount; i++) {
					mdStmt.setString(i, ""); //$NON-NLS-1$
				}

				boolean hadResults = mdStmt.execute();

				if (hadResults) {
					mdRs = mdStmt.getResultSet();

					this.pstmtResultMetaData = mdRs.getMetaData();
				} else {
					this.pstmtResultMetaData = new ResultSetMetaData(
							new Field[0], 
							this.connection.getUseOldAliasMetadataBehavior());
				}
			} finally {
				SQLException sqlExRethrow = null;

				if (mdRs != null) {
					try {
						mdRs.close();
					} catch (SQLException sqlEx) {
						sqlExRethrow = sqlEx;
					}

					mdRs = null;
				}

				if (mdStmt != null) {
					try {
						mdStmt.close();
					} catch (SQLException sqlEx) {
						sqlExRethrow = sqlEx;
					}

					mdStmt = null;
				}

				if (sqlExRethrow != null) {
					throw sqlExRethrow;
				}
			}
		}

		return this.pstmtResultMetaData;
	}

	/**
	 * @see PreparedStatement#getParameterMetaData()
	 */
	public ParameterMetaData getParameterMetaData() 
		throws SQLException {
		if (this.parameterMetaData == null) {
			this.parameterMetaData = new MysqlParameterMetadata(
					null, this.parameterCount);
		}
		
		return this.parameterMetaData;
	}

	ParseInfo getParseInfo() {
		return this.parseInfo;
	}

	private final char getSuccessor(char c, int n) {
		return ((c == 'y') && (n == 2)) ? 'X'
				: (((c == 'y') && (n < 4)) ? 'y'
						: ((c == 'y') ? 'M'
								: (((c == 'M') && (n == 2)) ? 'Y'
										: (((c == 'M') && (n < 3)) ? 'M'
												: ((c == 'M') ? 'd'
														: (((c == 'd') && (n < 2)) ? 'd'
																: ((c == 'd') ? 'H'
																		: (((c == 'H') && (n < 2)) ? 'H'
																				: ((c == 'H') ? 'm'
																						: (((c == 'm') && (n < 2)) ? 'm'
																								: ((c == 'm') ? 's'
																										: (((c == 's') && (n < 2)) ? 's'
																												: 'W'))))))))))));
	}

	/**
	 * Used to escape binary data with hex for mb charsets
	 * 
	 * @param buf
	 * @param packet
	 * @param size
	 * @throws SQLException
	 */
	private final void hexEscapeBlock(byte[] buf, Buffer packet, int size)
			throws SQLException {
		for (int i = 0; i < size; i++) {
			byte b = buf[i];
			int lowBits = (b & 0xff) / 16;
			int highBits = (b & 0xff) % 16;

			packet.writeByte(HEX_DIGITS[lowBits]);
			packet.writeByte(HEX_DIGITS[highBits]);
		}
	}

	private void initializeFromParseInfo() throws SQLException {
		this.staticSqlStrings = this.parseInfo.staticSql;
		this.hasLimitClause = this.parseInfo.foundLimitClause;
		this.isLoadDataQuery = this.parseInfo.foundLoadData;
		this.firstCharOfStmt = this.parseInfo.firstStmtChar;

		this.parameterCount = this.staticSqlStrings.length - 1;

		this.parameterValues = new byte[this.parameterCount][];
		this.parameterStreams = new InputStream[this.parameterCount];
		this.isStream = new boolean[this.parameterCount];
		this.streamLengths = new int[this.parameterCount];
		this.isNull = new boolean[this.parameterCount];

		clearParameters();

		for (int j = 0; j < this.parameterCount; j++) {
			this.isStream[j] = false;
		}
	}

	boolean isNull(int paramIndex) {
		return this.isNull[paramIndex];
	}

	public boolean isWrapperFor(Class arg0) throws SQLException {
		throw new JDBC40NotYetImplementedException();
	}

	private final int readblock(InputStream i, byte[] b) throws SQLException {
		try {
			return i.read(b);
		} catch (Throwable E) {
			throw SQLError.createSQLException(Messages.getString("PreparedStatement.56") //$NON-NLS-1$
					+ E.getClass().getName(), SQLError.SQL_STATE_GENERAL_ERROR);
		}
	}

	private final int readblock(InputStream i, byte[] b, int length)
			throws SQLException {
		try {
			int lengthToRead = length;

			if (lengthToRead > b.length) {
				lengthToRead = b.length;
			}

			return i.read(b, 0, lengthToRead);
		} catch (Throwable E) {
			throw SQLError.createSQLException(Messages.getString("PreparedStatement.55") //$NON-NLS-1$
					+ E.getClass().getName(), SQLError.SQL_STATE_GENERAL_ERROR);
		}
	}

	/**
	 * Closes this statement, releasing all resources
	 * 
	 * @param calledExplicitly
	 *            was this called by close()?
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	protected void realClose(boolean calledExplicitly, 
			boolean closeOpenResults) throws SQLException {
		if (this.useUsageAdvisor) {
			if (this.numberOfExecutions <= 1) {
				String message = Messages.getString("PreparedStatement.43"); //$NON-NLS-1$

				this.eventSink.consumeEvent(new ProfilerEvent(
						ProfilerEvent.TYPE_WARN, "", this.currentCatalog, //$NON-NLS-1$
						this.connectionId, this.getId(), -1, System
								.currentTimeMillis(), 0, null,
						this.pointOfOrigin, message));
			}
		}
		
		super.realClose(calledExplicitly, closeOpenResults);

		this.dbmd = null;
		this.originalSql = null;
		this.staticSqlStrings = null;
		this.parameterValues = null;
		this.parameterStreams = null;
		this.isStream = null;
		this.streamLengths = null;
		this.isNull = null;
		this.streamConvertBuf = null;
	}

	/**
	 * JDBC 2.0 Set an Array parameter.
	 * 
	 * @param i
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            an object representing an SQL array
	 * 
	 * @throws SQLException
	 *             because this method is not implemented.
	 * @throws NotImplemented
	 *             DOCUMENT ME!
	 */
	public void setArray(int i, Array x) throws SQLException {
		throw new NotImplemented();
	}

	/**
	 * When a very large ASCII value is input to a LONGVARCHAR parameter, it may
	 * be more practical to send it via a java.io.InputStream. JDBC will read
	 * the data from the stream as needed, until it reaches end-of-file. The
	 * JDBC driver will do any necessary conversion from ASCII to the database
	 * char format.
	 * 
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream
	 * object or your own subclass that implements the standard interface.
	 * </p>
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * @param length
	 *            the number of bytes in the stream
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setAsciiStream(int parameterIndex, InputStream x,
			int length) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.VARCHAR);
		} else {
			setBinaryStream(parameterIndex, x, length);
		}
	}

	/**
	 * Set a parameter to a java.math.BigDecimal value. The driver converts this
	 * to a SQL NUMERIC value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.DECIMAL);
		} else {
			setInternal(parameterIndex, StringUtils
					.fixDecimalExponent(StringUtils.consistentToString(x)));
		}
	}

	/**
	 * When a very large binary value is input to a LONGVARBINARY parameter, it
	 * may be more practical to send it via a java.io.InputStream. JDBC will
	 * read the data from the stream as needed, until it reaches end-of-file.
	 * 
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream
	 * object or your own subclass that implements the standard interface.
	 * </p>
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * @param length
	 *            the number of bytes to read from the stream (ignored)
	 * 
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.BINARY);
		} else {
			if ((parameterIndex < 1)
					|| (parameterIndex > this.staticSqlStrings.length)) {
				throw SQLError.createSQLException(
						Messages.getString("PreparedStatement.2") //$NON-NLS-1$
								+ parameterIndex
								+ Messages.getString("PreparedStatement.3") + this.staticSqlStrings.length + Messages.getString("PreparedStatement.4"), //$NON-NLS-1$ //$NON-NLS-2$
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
			}

			this.parameterStreams[parameterIndex - 1] = x;
			this.isStream[parameterIndex - 1] = true;
			this.streamLengths[parameterIndex - 1] = length;
			this.isNull[parameterIndex - 1] = false;
		}
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		setBinaryStream(parameterIndex, inputStream, (int)length);
	}

	/**
	 * JDBC 2.0 Set a BLOB parameter.
	 * 
	 * @param i
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            an object representing a BLOB
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public void setBlob(int i, java.sql.Blob x) throws SQLException {
		if (x == null) {
			setNull(i, Types.BLOB);
		} else {
			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

			bytesOut.write('\'');
			escapeblockFast(x.getBytes(1, (int) x.length()), bytesOut, (int) x
					.length());
			bytesOut.write('\'');

			setInternal(i, bytesOut.toByteArray());
		}
	}

	/**
	 * Set a parameter to a Java boolean value. The driver converts this to a
	 * SQL BIT value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		if (this.useTrueBoolean) {
			setInternal(parameterIndex, x ? "1" : "0"); //$NON-NLS-1$ //$NON-NLS-2$
		} else {
			setInternal(parameterIndex, x ? "'t'" : "'f'"); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}

	/**
	 * Set a parameter to a Java byte value. The driver converts this to a SQL
	 * TINYINT value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setByte(int parameterIndex, byte x) throws SQLException {
		setInternal(parameterIndex, String.valueOf(x));
	}

	/**
	 * Set a parameter to a Java array of bytes. The driver converts this to a
	 * SQL VARBINARY or LONGVARBINARY (depending on the argument's size relative
	 * to the driver's limits on VARBINARYs) when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		setBytes(parameterIndex, x, true, true);
	}

	protected void setBytes(int parameterIndex, byte[] x,
			boolean checkForIntroducer, boolean escapeForMBChars)
			throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.BINARY);
		} else {
			String connectionEncoding = this.connection.getEncoding();

			if (this.connection.isNoBackslashEscapesSet()
					|| (escapeForMBChars 
							&& this.connection.getUseUnicode()
							&& connectionEncoding != null
							&& CharsetMapping.isMultibyteCharset(connectionEncoding))) {

				// Send as hex

				ByteArrayOutputStream bOut = new ByteArrayOutputStream(
						(x.length * 2) + 3);
				bOut.write('x');
				bOut.write('\'');

				for (int i = 0; i < x.length; i++) {
					int lowBits = (x[i] & 0xff) / 16;
					int highBits = (x[i] & 0xff) % 16;

					bOut.write(HEX_DIGITS[lowBits]);
					bOut.write(HEX_DIGITS[highBits]);
				}

				bOut.write('\'');

				setInternal(parameterIndex, bOut.toByteArray());

				return;
			}

			// escape them
			int numBytes = x.length;

			int pad = 2;

			boolean needsIntroducer = checkForIntroducer
					&& this.connection.versionMeetsMinimum(4, 1, 0);

			if (needsIntroducer) {
				pad += 7;
			}

			ByteArrayOutputStream bOut = new ByteArrayOutputStream(numBytes
					+ pad);

			if (needsIntroducer) {
				bOut.write('_');
				bOut.write('b');
				bOut.write('i');
				bOut.write('n');
				bOut.write('a');
				bOut.write('r');
				bOut.write('y');
			}
			bOut.write('\'');

			for (int i = 0; i < numBytes; ++i) {
				byte b = x[i];

				switch (b) {
				case 0: /* Must be escaped for 'mysql' */
					bOut.write('\\');
					bOut.write('0');

					break;

				case '\n': /* Must be escaped for logs */
					bOut.write('\\');
					bOut.write('n');

					break;

				case '\r':
					bOut.write('\\');
					bOut.write('r');

					break;

				case '\\':
					bOut.write('\\');
					bOut.write('\\');

					break;

				case '\'':
					bOut.write('\\');
					bOut.write('\'');

					break;

				case '"': /* Better safe than sorry */
					bOut.write('\\');
					bOut.write('"');

					break;

				case '\032': /* This gives problems on Win32 */
					bOut.write('\\');
					bOut.write('Z');

					break;

				default:
					bOut.write(b);
				}
			}

			bOut.write('\'');

			setInternal(parameterIndex, bOut.toByteArray());
		}
	}

	/**
	 * Used by updatable result sets for refreshRow() because the parameter has
	 * already been escaped for updater or inserter prepared statements.
	 * 
	 * @param parameterIndex
	 *            the parameter to set.
	 * @param parameterAsBytes
	 *            the parameter as a string.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	protected void setBytesNoEscape(int parameterIndex, byte[] parameterAsBytes)
			throws SQLException {
		byte[] parameterWithQuotes = new byte[parameterAsBytes.length + 2];
		parameterWithQuotes[0] = '\'';
		System.arraycopy(parameterAsBytes, 0, parameterWithQuotes, 1,
				parameterAsBytes.length);
		parameterWithQuotes[parameterAsBytes.length + 1] = '\'';

		setInternal(parameterIndex, parameterWithQuotes);
	}

	protected void setBytesNoEscapeNoQuotes(int parameterIndex,
			byte[] parameterAsBytes) throws SQLException {
		setInternal(parameterIndex, parameterAsBytes);
	}

	/**
	 * JDBC 2.0 When a very large UNICODE value is input to a LONGVARCHAR
	 * parameter, it may be more practical to send it via a java.io.Reader. JDBC
	 * will read the data from the stream as needed, until it reaches
	 * end-of-file. The JDBC driver will do any necessary conversion from
	 * UNICODE to the database char format.
	 * 
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream
	 * object or your own subclass that implements the standard interface.
	 * </p>
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param reader
	 *            the java reader which contains the UNICODE data
	 * @param length
	 *            the number of characters in the stream
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public void setCharacterStream(int parameterIndex, java.io.Reader reader,
			int length) throws SQLException {
		try {
			if (reader == null) {
				setNull(parameterIndex, Types.LONGVARCHAR);
			} else {
				char[] c = null;
				int len = 0;

				boolean useLength = this.connection
						.getUseStreamLengthsInPrepStmts();

				String forcedEncoding = this.connection.getClobCharacterEncoding();
				
				if (useLength && (length != -1)) {
					c = new char[length];

					int numCharsRead = readFully(reader, c, length); // blocks
					// until
					// all
					// read

					if (forcedEncoding == null) {
						setString(parameterIndex, new String(c, 0, numCharsRead));
					} else {
						try {
							setBytes(parameterIndex, new String(c, 
									0, 
									numCharsRead).getBytes(forcedEncoding));
						} catch (UnsupportedEncodingException uee) {
							throw SQLError.createSQLException("Unsupported character encoding " + 
									forcedEncoding, SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
						}
					}
				} else {
					c = new char[4096];

					StringBuffer buf = new StringBuffer();

					while ((len = reader.read(c)) != -1) {
						buf.append(c, 0, len);
					}

					if (forcedEncoding == null) {
						setString(parameterIndex, buf.toString());
					} else {
						try {
							setBytes(parameterIndex, 
									buf.toString().getBytes(forcedEncoding));
						} catch (UnsupportedEncodingException uee) {
							throw SQLError.createSQLException("Unsupported character encoding " + 
									forcedEncoding, SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
						}
					}
				}
			}
		} catch (java.io.IOException ioEx) {
			throw SQLError.createSQLException(ioEx.toString(),
					SQLError.SQL_STATE_GENERAL_ERROR);
		}
	}

	/**
	 * JDBC 2.0 Set a CLOB parameter.
	 * 
	 * @param i
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            an object representing a CLOB
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public void setClob(int i, Clob x) throws SQLException {
		if (x == null) {
			setNull(i, Types.CLOB);

			return;
		}

		String forcedEncoding = this.connection.getClobCharacterEncoding();
		
		if (forcedEncoding == null) {
			setString(i, x.getSubString(1L, (int) x.length()));
		} else {
			try {
				setBytes(i, x.getSubString(1L, 
						(int)x.length()).getBytes(forcedEncoding));
			} catch (UnsupportedEncodingException uee) {
				throw SQLError.createSQLException("Unsupported character encoding " + 
						forcedEncoding, SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
			}
		}
	}

	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		setCharacterStream(parameterIndex, reader, length);
	}

	/**
	 * Set a parameter to a java.sql.Date value. The driver converts this to a
	 * SQL DATE value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception java.sql.SQLException
	 *                if a database access error occurs
	 */
	public void setDate(int parameterIndex, java.sql.Date x)
			throws java.sql.SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.DATE);
		} else {
			// FIXME: Have instance version of this, problem as it's
			// not thread-safe :(
			SimpleDateFormat dateFormatter = new SimpleDateFormat(
					"''yyyy-MM-dd''", Locale.US); //$NON-NLS-1$
			setInternal(parameterIndex, dateFormatter.format(x));
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
	 * @param cal
	 *            the calendar to interpret the date with
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public void setDate(int parameterIndex, java.sql.Date x, Calendar cal)
			throws SQLException {
		setDate(parameterIndex, x);
	}

	/**
	 * Set a parameter to a Java double value. The driver converts this to a SQL
	 * DOUBLE value when it sends it to the database
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setDouble(int parameterIndex, double x) throws SQLException {

		if (!this.connection.getAllowNanAndInf()
				&& (x == Double.POSITIVE_INFINITY
						|| x == Double.NEGATIVE_INFINITY || Double.isNaN(x))) {
			throw SQLError.createSQLException("'" + x
					+ "' is not a valid numeric or approximate numeric value",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);

		}

		setInternal(parameterIndex, StringUtils.fixDecimalExponent(String
				.valueOf(x)));
	}

	/**
	 * Set a parameter to a Java float value. The driver converts this to a SQL
	 * FLOAT value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setFloat(int parameterIndex, float x) throws SQLException {
		setInternal(parameterIndex, StringUtils.fixDecimalExponent(String
				.valueOf(x)));
	}

	/**
	 * Set a parameter to a Java int value. The driver converts this to a SQL
	 * INTEGER value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setInt(int parameterIndex, int x) throws SQLException {
		setInternal(parameterIndex, String.valueOf(x));
	}

	private final void setInternal(int paramIndex, byte[] val)
			throws SQLException {
		if (this.isClosed) {
			throw SQLError.createSQLException(Messages.getString("PreparedStatement.48"), //$NON-NLS-1$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}

		if ((paramIndex < 1)) {
			throw SQLError.createSQLException(
					Messages.getString("PreparedStatement.49") //$NON-NLS-1$
							+ paramIndex
							+ Messages.getString("PreparedStatement.50"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT); //$NON-NLS-1$
		} else if (paramIndex > this.parameterCount) {
			throw SQLError.createSQLException(
					Messages.getString("PreparedStatement.51") //$NON-NLS-1$
							+ paramIndex
							+ Messages.getString("PreparedStatement.52") + (this.parameterValues.length) + Messages.getString("PreparedStatement.53"), //$NON-NLS-1$ //$NON-NLS-2$
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}

		this.isStream[paramIndex - 1] = false;
		this.isNull[paramIndex - 1] = false;
		this.parameterStreams[paramIndex - 1] = null;
		this.parameterValues[paramIndex - 1] = val;
	}

	private final void setInternal(int paramIndex, String val)
			throws SQLException {
		checkClosed();
		
		byte[] parameterAsBytes = null;

		if (this.charConverter != null) {
			parameterAsBytes = this.charConverter.toBytes(val);
		} else {
			parameterAsBytes = StringUtils.getBytes(val, this.charConverter,
					this.charEncoding, this.connection
							.getServerCharacterEncoding(), this.connection
							.parserKnowsUnicode());
		}

		setInternal(paramIndex, parameterAsBytes);
	}

	/**
	 * Set a parameter to a Java long value. The driver converts this to a SQL
	 * BIGINT value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setLong(int parameterIndex, long x) throws SQLException {
		setInternal(parameterIndex, String.valueOf(x));
	}

	/**
     * JDBC 2.0 When a very large UNICODE value is input to a LONGVARCHAR
     * parameter, it may be more practical to send it via a java.io.Reader. JDBC
     * will read the data from the stream as needed, until it reaches
     * end-of-file. The JDBC driver will do any necessary conversion from
     * UNICODE to the database char format.
     * 
     * <P>
     * <B>Note:</B> This stream object can either be a standard Java stream
     * object or your own subclass that implements the standard interface.
     * </p>
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param reader
     *            the java reader which contains the UNICODE data
     * @param length
     *            the number of characters in the stream
     * 
     * @exception SQLException
     *                if a database-access error occurs.
     */
	public void setNCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
        try {
            if (reader == null) {
                setNull(parameterIndex, java.sql.Types.LONGNVARCHAR);
                
            } else {
                char[] c = null;
                int len = 0;

                boolean useLength = this.connection
                        .getUseStreamLengthsInPrepStmts();
                
                // Ignore "clobCharacterEncoding" because utf8 should be used this time.

                if (useLength && (length != -1)) {
                    c = new char[(int) length];  // can't take more than Integer.MAX_VALUE

                    int numCharsRead = readFully(reader, c, (int) length); // blocks
                    // until
                    // all
                    // read
                    setNString(parameterIndex, new String(c, 0, numCharsRead));

                } else {
                    c = new char[4096];

                    StringBuffer buf = new StringBuffer();

                    while ((len = reader.read(c)) != -1) {
                        buf.append(c, 0, len);
                    }

                    setNString(parameterIndex, buf.toString());
                }
            }
        } catch (java.io.IOException ioEx) {
            throw SQLError.createSQLException(ioEx.toString(),
                    SQLError.SQL_STATE_GENERAL_ERROR);
        }
    }
    
    /**
     * JDBC 4.0 Set a NCLOB parameter.
     * 
     * @param i
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            an object representing a NCLOB
     * 
     * @throws SQLException
     *             if a database error occurs
     */
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		if (value == null) {
            setNull(parameterIndex, java.sql.Types.NCLOB);
        } else {
            setNCharacterStream(parameterIndex, value.getCharacterStream(), value.length());
        }
	}

    /**
     * Set a parameter to a Java String value. The driver converts this to a SQL
     * VARCHAR or LONGVARCHAR value with introducer _utf8 (depending on the 
     * arguments size relative to the driver's limits on VARCHARs) when it sends 
     * it to the database. If charset is set as utf8, this method just call setString.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param x
     *            the parameter value
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setNString(int parameterIndex, String x) throws SQLException {
        if (this.charEncoding.equalsIgnoreCase("UTF-8")
                || this.charEncoding.equalsIgnoreCase("utf8")) {
            setString(parameterIndex, x);
            return;
        }

        // if the passed string is null, then set this column to null
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.NCHAR); 
        } else {
            int stringLength = x.length();
            // Ignore sql_mode=NO_BACKSLASH_ESCAPES in current implementation.

            // Add introducer _utf8 for NATIONAL CHARACTER
            StringBuffer buf = new StringBuffer((int) (x.length() * 1.1 + 4));
            buf.append("_utf8");
            buf.append('\'');

            //
            // Note: buf.append(char) is _faster_ than
            // appending in blocks, because the block
            // append requires a System.arraycopy()....
            // go figure...
            //

            for (int i = 0; i < stringLength; ++i) {
                char c = x.charAt(i);

                switch (c) {
                case 0: /* Must be escaped for 'mysql' */
                    buf.append('\\');
                    buf.append('0');

                    break;

                case '\n': /* Must be escaped for logs */
                    buf.append('\\');
                    buf.append('n');

                    break;

                case '\r':
                    buf.append('\\');
                    buf.append('r');

                    break;

                case '\\':
                    buf.append('\\');
                    buf.append('\\');

                    break;

                case '\'':
                    buf.append('\\');
                    buf.append('\'');

                    break;

                case '"': /* Better safe than sorry */
                    if (this.usingAnsiMode) {
                        buf.append('\\');
                    }

                    buf.append('"');

                    break;

                case '\032': /* This gives problems on Win32 */
                    buf.append('\\');
                    buf.append('Z');

                    break;

                default:
                    buf.append(c);
                }
            }

            buf.append('\'');

            String parameterAsString = buf.toString();

            byte[] parameterAsBytes = null;

            if (!this.isLoadDataQuery) {
                parameterAsBytes = StringUtils.getBytes(parameterAsString,
                        this.connection.getCharsetConverter("UTF-8"), "UTF-8", 
                                this.connection.getServerCharacterEncoding(),
                                this.connection.parserKnowsUnicode());
            } else {
                // Send with platform character encoding
                parameterAsBytes = parameterAsString.getBytes();
            }

            setInternal(parameterIndex, parameterAsBytes);
        }
    }

	/**
	 * Set a parameter to SQL NULL
	 * 
	 * <p>
	 * <B>Note:</B> You must specify the parameters SQL type (although MySQL
	 * ignores it)
	 * </p>
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, etc...
	 * @param sqlType
	 *            the SQL type code defined in java.sql.Types
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		setInternal(parameterIndex, "null"); //$NON-NLS-1$
		this.isNull[parameterIndex - 1] = true;
	}

	/**
	 * Set a parameter to SQL NULL.
	 * 
	 * <P>
	 * <B>Note:</B> You must specify the parameter's SQL type.
	 * </p>
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param sqlType
	 *            SQL type code defined by java.sql.Types
	 * @param arg
	 *            argument parameters for null
	 * 
	 * @exception SQLException
	 *                if a database-access error occurs.
	 */
	public void setNull(int parameterIndex, int sqlType, String arg)
			throws SQLException {
		setNull(parameterIndex, sqlType);
	}

	private void setNumericObject(int parameterIndex, Object parameterObj, int targetSqlType, int scale) throws SQLException {
		Number parameterAsNum;

		if (parameterObj instanceof Boolean) {
			parameterAsNum = ((Boolean) parameterObj)
					.booleanValue() ? new Integer(1) : new Integer(
					0);
		} else if (parameterObj instanceof String) {
			switch (targetSqlType) {
			case Types.BIT:
				boolean parameterAsBoolean = "true"
						.equalsIgnoreCase((String) parameterObj);

				parameterAsNum = parameterAsBoolean ? new Integer(1)
						: new Integer(0);

				break;

			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
				parameterAsNum = Integer
						.valueOf((String) parameterObj);

				break;

			case Types.BIGINT:
				parameterAsNum = Long
						.valueOf((String) parameterObj);

				break;

			case Types.REAL:
				parameterAsNum = Float
						.valueOf((String) parameterObj);

				break;

			case Types.FLOAT:
			case Types.DOUBLE:
				parameterAsNum = Double
						.valueOf((String) parameterObj);

				break;

			case Types.DECIMAL:
			case Types.NUMERIC:
			default:
				parameterAsNum = new java.math.BigDecimal(
						(String) parameterObj);
			}
		} else {
			parameterAsNum = (Number) parameterObj;
		}

		switch (targetSqlType) {
		case Types.BIT:
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			setInt(parameterIndex, parameterAsNum.intValue());

			break;

		case Types.BIGINT:
			setLong(parameterIndex, parameterAsNum.longValue());

			break;

		case Types.REAL:
			setFloat(parameterIndex, parameterAsNum.floatValue());

			break;

		case Types.FLOAT:
		case Types.DOUBLE:
			setDouble(parameterIndex, parameterAsNum.doubleValue());

			break;

		case Types.DECIMAL:
		case Types.NUMERIC:

			if (parameterAsNum instanceof java.math.BigDecimal) {
				BigDecimal scaledBigDecimal = null;

				try {
					scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum)
							.setScale(scale);
				} catch (ArithmeticException ex) {
					try {
						scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum)
								.setScale(scale,
										BigDecimal.ROUND_HALF_UP);
					} catch (ArithmeticException arEx) {
						throw SQLError.createSQLException(
								"Can't set scale of '"
										+ scale
										+ "' for DECIMAL argument '"
										+ parameterAsNum + "'",
								SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
					}
				}

				setBigDecimal(parameterIndex, scaledBigDecimal);
			} else if (parameterAsNum instanceof java.math.BigInteger) {
				setBigDecimal(
						parameterIndex,
						new java.math.BigDecimal(
								(java.math.BigInteger) parameterAsNum,
								scale));
			} else {
				setBigDecimal(parameterIndex,
						new java.math.BigDecimal(parameterAsNum
								.doubleValue()));
			}

			break;
		}
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param parameterIndex
	 *            DOCUMENT ME!
	 * @param parameterObj
	 *            DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void setObject(int parameterIndex, Object parameterObj)
			throws SQLException {
		if (parameterObj == null) {
			setNull(parameterIndex, java.sql.Types.OTHER);
		} else {
			if (parameterObj instanceof Byte) {
				setInt(parameterIndex, ((Byte) parameterObj).intValue());
			} else if (parameterObj instanceof String) {
				setString(parameterIndex, (String) parameterObj);
			} else if (parameterObj instanceof BigDecimal) {
				setBigDecimal(parameterIndex, (BigDecimal) parameterObj);
			} else if (parameterObj instanceof Short) {
				setShort(parameterIndex, ((Short) parameterObj).shortValue());
			} else if (parameterObj instanceof Integer) {
				setInt(parameterIndex, ((Integer) parameterObj).intValue());
			} else if (parameterObj instanceof Long) {
				setLong(parameterIndex, ((Long) parameterObj).longValue());
			} else if (parameterObj instanceof Float) {
				setFloat(parameterIndex, ((Float) parameterObj).floatValue());
			} else if (parameterObj instanceof Double) {
				setDouble(parameterIndex, ((Double) parameterObj).doubleValue());
			} else if (parameterObj instanceof byte[]) {
				setBytes(parameterIndex, (byte[]) parameterObj);
			} else if (parameterObj instanceof java.sql.Date) {
				setDate(parameterIndex, (java.sql.Date) parameterObj);
			} else if (parameterObj instanceof Time) {
				setTime(parameterIndex, (Time) parameterObj);
			} else if (parameterObj instanceof Timestamp) {
				setTimestamp(parameterIndex, (Timestamp) parameterObj);
			} else if (parameterObj instanceof Boolean) {
				setBoolean(parameterIndex, ((Boolean) parameterObj)
						.booleanValue());
			} else if (parameterObj instanceof InputStream) {
				setBinaryStream(parameterIndex, (InputStream) parameterObj, -1);
			} else if (parameterObj instanceof java.sql.Blob) {
				setBlob(parameterIndex, (java.sql.Blob) parameterObj);
			} else if (parameterObj instanceof java.sql.Clob) {
				setClob(parameterIndex, (java.sql.Clob) parameterObj);
			} else if (parameterObj instanceof java.util.Date) {
				setTimestamp(parameterIndex, new Timestamp(
						((java.util.Date) parameterObj).getTime()));
			} else if (parameterObj instanceof BigInteger) {
				setString(parameterIndex, parameterObj.toString());
			} else {
				setSerializableObject(parameterIndex, parameterObj);
			}
		}
	}
	

	/**
	 * DOCUMENT ME!
	 * 
	 * @param parameterIndex
	 *            DOCUMENT ME!
	 * @param parameterObj
	 *            DOCUMENT ME!
	 * @param targetSqlType
	 *            DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void setObject(int parameterIndex, Object parameterObj,
			int targetSqlType) throws SQLException {
		if (!(parameterObj instanceof BigDecimal)) {
			setObject(parameterIndex, parameterObj, targetSqlType, 0);
		} else {
			setObject(parameterIndex, parameterObj, targetSqlType,
					((BigDecimal)parameterObj).scale());
		}
	}

	/**
	 * Set the value of a parameter using an object; use the java.lang
	 * equivalent objects for integral values.
	 * 
	 * <P>
	 * The given Java object will be converted to the targetSqlType before being
	 * sent to the database.
	 * </p>
	 * 
	 * <P>
	 * note that this method may be used to pass database-specific abstract data
	 * types. This is done by using a Driver-specific Java type and using a
	 * targetSqlType of java.sql.Types.OTHER
	 * </p>
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param parameterObj
	 *            the object containing the input parameter value
	 * @param targetSqlType
	 *            The SQL type to be send to the database
	 * @param scale
	 *            For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types
	 *            this is the number of digits after the decimal. For all other
	 *            types this value will be ignored.
	 * 
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void setObject(int parameterIndex, Object parameterObj,
			int targetSqlType, int scale) throws SQLException {
		if (parameterObj == null) {
			setNull(parameterIndex, java.sql.Types.OTHER);
		} else {
			try {
				switch (targetSqlType) {
				case Types.BOOLEAN:
					/*
					 From Table-B5 in the JDBC-3.0 Spec
				
					        T S I B R F D D N B B C V L
					        I M N I E L O E U I O H A O
					        N A T G A O U C M T O A R N
					        Y L E I L A B I E   L R C G
					        I L G N   T L M R   E   H V
					        N I E T     E A I   A   A A
					        T N R         L C   N   R R
					          T                       C
                                                      H
				                                      A
					                                  R
					-----------------------------------
					Boolean x x x x x x x x x x x x x x
					*/
					
					if (parameterObj instanceof Boolean) {
						setBoolean(parameterIndex, ((Boolean)parameterObj).booleanValue());
						
						break;
					} else if (parameterObj instanceof String) {
						setBoolean(parameterIndex, "true".equalsIgnoreCase((String)parameterObj) ||
								!"0".equalsIgnoreCase((String)parameterObj));
						
						break;
					} else if (parameterObj instanceof Number) {
						int intValue = ((Number)parameterObj).intValue();
						
						setBoolean(parameterIndex, intValue != 0);
						
						break;
					} else {
						throw SQLError.createSQLException("No conversion from " + parameterObj.getClass().getName() + 
								" to Types.BOOLEAN possible.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
					}
					
					
				case Types.BIT:
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
				case Types.BIGINT:
				case Types.REAL:
				case Types.FLOAT:
				case Types.DOUBLE:
				case Types.DECIMAL:
				case Types.NUMERIC:

					setNumericObject(parameterIndex, parameterObj, targetSqlType, scale);

					break;

				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
					if (parameterObj instanceof BigDecimal) {
						setString(
								parameterIndex,
								(StringUtils
										.fixDecimalExponent(StringUtils
												.consistentToString((BigDecimal) parameterObj))));
					} else {
						setString(parameterIndex, parameterObj.toString());
					}

					break;

				case Types.CLOB:

					if (parameterObj instanceof java.sql.Clob) {
						setClob(parameterIndex, (java.sql.Clob) parameterObj);
					} else {
						setString(parameterIndex, parameterObj.toString());
					}

					break;

				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
				case Types.BLOB:

					if (parameterObj instanceof byte[]) {
						setBytes(parameterIndex, (byte[]) parameterObj);
					} else if (parameterObj instanceof java.sql.Blob) {
						setBlob(parameterIndex, (java.sql.Blob) parameterObj);
					} else {
						setBytes(parameterIndex, StringUtils.getBytes(
								parameterObj.toString(), this.charConverter,
								this.charEncoding, this.connection
										.getServerCharacterEncoding(),
								this.connection.parserKnowsUnicode()));
					}

					break;

				case Types.DATE:
				case Types.TIMESTAMP:

					java.util.Date parameterAsDate;

					if (parameterObj instanceof String) {
						ParsePosition pp = new ParsePosition(0);
						java.text.DateFormat sdf = new java.text.SimpleDateFormat(
								getDateTimePattern((String) parameterObj, false), Locale.US);
						parameterAsDate = sdf.parse((String) parameterObj, pp);
					} else {
						parameterAsDate = (java.util.Date) parameterObj;
					}

					switch (targetSqlType) {
					case Types.DATE:

						if (parameterAsDate instanceof java.sql.Date) {
							setDate(parameterIndex,
									(java.sql.Date) parameterAsDate);
						} else {
							setDate(parameterIndex, new java.sql.Date(
									parameterAsDate.getTime()));
						}

						break;

					case Types.TIMESTAMP:

						if (parameterAsDate instanceof java.sql.Timestamp) {
							setTimestamp(parameterIndex,
									(java.sql.Timestamp) parameterAsDate);
						} else {
							setTimestamp(parameterIndex,
									new java.sql.Timestamp(parameterAsDate
											.getTime()));
						}

						break;
					}

					break;

				case Types.TIME:

					if (parameterObj instanceof String) {
						java.text.DateFormat sdf = new java.text.SimpleDateFormat(
								getDateTimePattern((String) parameterObj, true), Locale.US);
						setTime(parameterIndex, new java.sql.Time(sdf.parse(
								(String) parameterObj).getTime()));
					} else if (parameterObj instanceof Timestamp) {
						Timestamp xT = (Timestamp) parameterObj;
						setTime(parameterIndex, new java.sql.Time(xT.getTime()));
					} else {
						setTime(parameterIndex, (java.sql.Time) parameterObj);
					}

					break;

				case Types.OTHER:
					setSerializableObject(parameterIndex, parameterObj);

					break;

				default:
					throw SQLError.createSQLException(Messages
							.getString("PreparedStatement.16"), //$NON-NLS-1$
							SQLError.SQL_STATE_GENERAL_ERROR);
				}
			} catch (Exception ex) {
				if (ex instanceof SQLException) {
					throw (SQLException) ex;
				}

				throw SQLError.createSQLException(
						Messages.getString("PreparedStatement.17") //$NON-NLS-1$
								+ parameterObj.getClass().toString()
								+ Messages.getString("PreparedStatement.18") //$NON-NLS-1$
								+ ex.getClass().getName()
								+ Messages.getString("PreparedStatement.19") + ex.getMessage(), //$NON-NLS-1$
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		}
	}

	private int setOneBatchedParameterSet(
			java.sql.PreparedStatement batchedStatement, int batchedParamIndex,
			BatchParams paramArg) throws SQLException {
		boolean[] isNullBatch = paramArg.isNull;
		boolean[] isStreamBatch = paramArg.isStream;

		for (int j = 0; j < isNullBatch.length; j++) {
			if (isNullBatch[j]) {
				batchedStatement.setNull(batchedParamIndex++, Types.NULL);
			} else {
				if (isStreamBatch[j]) {
					batchedStatement.setBinaryStream(batchedParamIndex++,
							paramArg.parameterStreams[j],
							paramArg.streamLengths[j]);
				} else {
					((com.mysql.jdbc.PreparedStatement) batchedStatement)
							.setBytesNoEscapeNoQuotes(batchedParamIndex++,
									paramArg.parameterStrings[j]);
				}
			}
		}

		return batchedParamIndex;
	}

	/**
	 * JDBC 2.0 Set a REF(&lt;structured-type&gt;) parameter.
	 * 
	 * @param i
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            an object representing data of an SQL REF Type
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 * @throws NotImplemented
	 *             DOCUMENT ME!
	 */
	public void setRef(int i, Ref x) throws SQLException {
		throw new NotImplemented();
	}

	/**
	 * Sets the concurrency for result sets generated by this statement
	 * 
	 * @param concurrencyFlag
	 *            DOCUMENT ME!
	 */
	void setResultSetConcurrency(int concurrencyFlag) {
		this.resultSetConcurrency = concurrencyFlag;
	}

	/**
	 * Sets the result set type for result sets generated by this statement
	 * 
	 * @param typeFlag
	 *            DOCUMENT ME!
	 */
	void setResultSetType(int typeFlag) {
		this.resultSetType = typeFlag;
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param retrieveGeneratedKeys
	 */
	protected void setRetrieveGeneratedKeys(boolean retrieveGeneratedKeys) {
		this.retrieveGeneratedKeys = retrieveGeneratedKeys;
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		throw new JDBC40NotYetImplementedException();
	}

	/**
	 * Sets the value for the placeholder as a serialized Java object (used by
	 * various forms of setObject()
	 * 
	 * @param parameterIndex
	 *            DOCUMENT ME!
	 * @param parameterObj
	 *            DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	private final void setSerializableObject(int parameterIndex,
			Object parameterObj) throws SQLException {
		try {
			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
			objectOut.writeObject(parameterObj);
			objectOut.flush();
			objectOut.close();
			bytesOut.flush();
			bytesOut.close();

			byte[] buf = bytesOut.toByteArray();
			ByteArrayInputStream bytesIn = new ByteArrayInputStream(buf);
			setBinaryStream(parameterIndex, bytesIn, buf.length);
		} catch (Exception ex) {
			throw SQLError.createSQLException(Messages.getString("PreparedStatement.54") //$NON-NLS-1$
					+ ex.getClass().getName(),
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
		}
	}

	/**
	 * Set a parameter to a Java short value. The driver converts this to a SQL
	 * SMALLINT value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setShort(int parameterIndex, short x) throws SQLException {
		setInternal(parameterIndex, String.valueOf(x));
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		if (xmlObject == null) {
			setNull(parameterIndex, Types.SQLXML);
		} else {
			if (xmlObject instanceof MysqlSQLXML) {
				setString(parameterIndex, ((MysqlSQLXML)xmlObject).serializeAsString());
			} else {
				setString(parameterIndex, xmlObject.getString());
			}
		}
	}

	/**
	 * Set a parameter to a Java String value. The driver converts this to a SQL
	 * VARCHAR or LONGVARCHAR value (depending on the arguments size relative to
	 * the driver's limits on VARCHARs) when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @exception SQLException
	 *                if a database access error occurs
	 */
	public void setString(int parameterIndex, String x) throws SQLException {
		// if the passed string is null, then set this column to null
		if (x == null) {
			setNull(parameterIndex, Types.CHAR);
		} else {
			checkClosed();
			
			int stringLength = x.length();

			if (this.connection.isNoBackslashEscapesSet()) {
				// Scan for any nasty chars

				boolean needsHexEscape = false;

				for (int i = 0; i < stringLength; ++i) {
					char c = x.charAt(i);

					switch (c) {
					case 0: /* Must be escaped for 'mysql' */

						needsHexEscape = true;
						break;

					case '\n': /* Must be escaped for logs */
						needsHexEscape = true;

						break;

					case '\r':
						needsHexEscape = true;
						break;

					case '\\':
						needsHexEscape = true;

						break;

					case '\'':
						needsHexEscape = true;

						break;

					case '"': /* Better safe than sorry */
						needsHexEscape = true;

						break;

					case '\032': /* This gives problems on Win32 */
						needsHexEscape = true;
						break;
					}

					if (needsHexEscape) {
						break; // no need to scan more
					}
				}

				

				if (!needsHexEscape) {
					byte[] parameterAsBytes = null;

					StringBuffer quotedString = new StringBuffer(x.length() + 2);
					quotedString.append('\'');
					quotedString.append(x);
					quotedString.append('\'');
					
					if (!this.isLoadDataQuery) {
						parameterAsBytes = StringUtils.getBytes(quotedString.toString(),
								this.charConverter, this.charEncoding,
								this.connection.getServerCharacterEncoding(),
								this.connection.parserKnowsUnicode());
					} else {
						// Send with platform character encoding
						parameterAsBytes = quotedString.toString().getBytes();
					}
					
					setInternal(parameterIndex, parameterAsBytes);
				} else {
					byte[] parameterAsBytes = null;

					if (!this.isLoadDataQuery) {
						parameterAsBytes = StringUtils.getBytes(x,
								this.charConverter, this.charEncoding,
								this.connection.getServerCharacterEncoding(),
								this.connection.parserKnowsUnicode());
					} else {
						// Send with platform character encoding
						parameterAsBytes = x.getBytes();
					}
					
					setBytes(parameterIndex, parameterAsBytes);
				}

				return;
			}

			StringBuffer buf = new StringBuffer((int) (x.length() * 1.1));
			buf.append('\'');

			//
			// Note: buf.append(char) is _faster_ than
			// appending in blocks, because the block
			// append requires a System.arraycopy()....
			// go figure...
			//

			for (int i = 0; i < stringLength; ++i) {
				char c = x.charAt(i);

				switch (c) {
				case 0: /* Must be escaped for 'mysql' */
					buf.append('\\');
					buf.append('0');

					break;

				case '\n': /* Must be escaped for logs */
					buf.append('\\');
					buf.append('n');

					break;

				case '\r':
					buf.append('\\');
					buf.append('r');

					break;

				case '\\':
					buf.append('\\');
					buf.append('\\');

					break;

				case '\'':
					buf.append('\\');
					buf.append('\'');

					break;

				case '"': /* Better safe than sorry */
					if (this.usingAnsiMode) {
						buf.append('\\');
					}

					buf.append('"');

					break;

				case '\032': /* This gives problems on Win32 */
					buf.append('\\');
					buf.append('Z');

					break;

				default:
					buf.append(c);
				}
			}

			buf.append('\'');

			String parameterAsString = buf.toString();

			byte[] parameterAsBytes = null;

			if (!this.isLoadDataQuery) {
				parameterAsBytes = StringUtils.getBytes(parameterAsString,
						this.charConverter, this.charEncoding, this.connection
								.getServerCharacterEncoding(), this.connection
								.parserKnowsUnicode());
			} else {
				// Send with platform character encoding
				parameterAsBytes = parameterAsString.getBytes();
			}

			setInternal(parameterIndex, parameterAsBytes);
		}
	}

	/**
	 * Set a parameter to a java.sql.Time value. The driver converts this to a
	 * SQL TIME value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            the parameter value
	 * @param cal
	 *            the cal specifying the timezone
	 * 
	 * @throws SQLException
	 *             if a database-access error occurs.
	 */
	public void setTime(int parameterIndex, java.sql.Time x, Calendar cal)
			throws SQLException {
		setTimeInternal(parameterIndex, x, cal, cal.getTimeZone(), true);
	}

	/**
	 * Set a parameter to a java.sql.Time value. The driver converts this to a
	 * SQL TIME value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...));
	 * @param x
	 *            the parameter value
	 * 
	 * @throws java.sql.SQLException
	 *             if a database access error occurs
	 */
	public void setTime(int parameterIndex, Time x)
			throws java.sql.SQLException {
		setTimeInternal(parameterIndex, x, null, TimeZone.getDefault(), false);
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
	 * @throws java.sql.SQLException
	 *             if a database access error occurs
	 */
	private void setTimeInternal(int parameterIndex, Time x, Calendar targetCalendar,
			TimeZone tz,
			boolean rollForward) throws java.sql.SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.TIME);
		} else {
			checkClosed();
			
			Calendar sessionCalendar = getCalendarInstanceForSessionOrNew();
			
			synchronized (sessionCalendar) {
				x = TimeUtil.changeTimezone(this.connection, 
						sessionCalendar,
						targetCalendar,
						 x, tz, this.connection
						.getServerTimezoneTZ(), rollForward);
			}
			
			setInternal(parameterIndex, "'" + x.toString() + "'"); //$NON-NLS-1$ //$NON-NLS-2$
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
	 * @param cal
	 *            the calendar specifying the timezone to use
	 * 
	 * @throws SQLException
	 *             if a database-access error occurs.
	 */
	public void setTimestamp(int parameterIndex, java.sql.Timestamp x,
			Calendar cal) throws SQLException {
		setTimestampInternal(parameterIndex, x, cal, cal.getTimeZone(), true);
	}

	/**
	 * Set a parameter to a java.sql.Timestamp value. The driver converts this
	 * to a SQL TIMESTAMP value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * 
	 * @throws java.sql.SQLException
	 *             if a database access error occurs
	 */
	public void setTimestamp(int parameterIndex, Timestamp x)
			throws java.sql.SQLException {
		setTimestampInternal(parameterIndex, x, null, TimeZone.getDefault(), false);
	}

	/**
	 * Set a parameter to a java.sql.Timestamp value. The driver converts this
	 * to a SQL TIMESTAMP value when it sends it to the database.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            the parameter value
	 * @param tz
	 *            the timezone to use
	 * 
	 * @throws SQLException
	 *             if a database-access error occurs.
	 */
	private void setTimestampInternal(int parameterIndex,
			Timestamp x, Calendar targetCalendar,
			TimeZone tz, boolean rollForward) throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.TIMESTAMP);
		} else {
			checkClosed();
			
			String timestampString = null;
			
			Calendar sessionCalendar = this.connection.getUseJDBCCompliantTimezoneShift() ?
					this.connection.getUtcCalendar() : 
						getCalendarInstanceForSessionOrNew();
					
			synchronized (sessionCalendar) {
				x = TimeUtil.changeTimezone(this.connection, 
						sessionCalendar,
						targetCalendar,
						x, tz, this.connection
					.getServerTimezoneTZ(), rollForward);
			}

			if (this.tsdf == null) {
				this.tsdf = new SimpleDateFormat("''yyyy-MM-dd HH:mm:ss''", Locale.US); //$NON-NLS-1$
			}

			timestampString = this.tsdf.format(x);

			setInternal(parameterIndex, timestampString); // SimpleDateFormat
			// is not
			// thread-safe
		}
	}

	/**
	 * When a very large Unicode value is input to a LONGVARCHAR parameter, it
	 * may be more practical to send it via a java.io.InputStream. JDBC will
	 * read the data from the stream as needed, until it reaches end-of-file.
	 * The JDBC driver will do any necessary conversion from UNICODE to the
	 * database char format.
	 * 
	 * <P>
	 * <B>Note:</B> This stream object can either be a standard Java stream
	 * object or your own subclass that implements the standard interface.
	 * </p>
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1...
	 * @param x
	 *            the parameter value
	 * @param length
	 *            the number of bytes to read from the stream
	 * 
	 * @throws SQLException
	 *             if a database access error occurs
	 * 
	 * @deprecated
	 */
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		if (x == null) {
			setNull(parameterIndex, java.sql.Types.VARCHAR);
		} else {
			setBinaryStream(parameterIndex, x, length);
		}
	}

	/**
	 * @see PreparedStatement#setURL(int, URL)
	 */
	public void setURL(int parameterIndex, URL arg) throws SQLException {
		if (arg != null) {
			setString(parameterIndex, arg.toString());
		} else {
			setNull(parameterIndex, Types.CHAR);
		}
	}

	private final void streamToBytes(Buffer packet, InputStream in,
			boolean escape, int streamLength, boolean useLength)
			throws SQLException {
		try {
			String connectionEncoding = this.connection.getEncoding();

			boolean hexEscape = false;

			if (this.connection.isNoBackslashEscapesSet()
					|| (this.connection.getUseUnicode() 
							&& connectionEncoding != null
							&& CharsetMapping.isMultibyteCharset(connectionEncoding)
							&& !this.connection.parserKnowsUnicode())) {
				hexEscape = true;
			}

			if (streamLength == -1) {
				useLength = false;
			}

			int bc = -1;

			if (useLength) {
				bc = readblock(in, streamConvertBuf, streamLength);
			} else {
				bc = readblock(in, streamConvertBuf);
			}

			int lengthLeftToRead = streamLength - bc;

			if (hexEscape) {
				packet.writeStringNoNull("x");
			} else if (this.connection.getIO().versionMeetsMinimum(4, 1, 0)) {
				packet.writeStringNoNull("_binary");
			}

			if (escape) {
				packet.writeByte((byte) '\'');
			}

			while (bc > 0) {
				if (hexEscape) {
					hexEscapeBlock(streamConvertBuf, packet, bc);
				} else if (escape) {
					escapeblockFast(streamConvertBuf, packet, bc);
				} else {
					packet.writeBytesNoNull(streamConvertBuf, 0, bc);
				}

				if (useLength) {
					bc = readblock(in, streamConvertBuf, lengthLeftToRead);

					if (bc > 0) {
						lengthLeftToRead -= bc;
					}
				} else {
					bc = readblock(in, streamConvertBuf);
				}
			}

			if (escape) {
				packet.writeByte((byte) '\'');
			}
		} finally {
			if (this.connection.getAutoClosePStmtStreams()) {
				try {
					in.close();
				} catch (IOException ioEx) {
					;
				}

				in = null;
			}
		}
	}

	private final byte[] streamToBytes(InputStream in, boolean escape,
			int streamLength, boolean useLength) throws SQLException {
		try {
			if (streamLength == -1) {
				useLength = false;
			}

			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

			int bc = -1;

			if (useLength) {
				bc = readblock(in, this.streamConvertBuf, streamLength);
			} else {
				bc = readblock(in, this.streamConvertBuf);
			}

			int lengthLeftToRead = streamLength - bc;

			if (this.connection.versionMeetsMinimum(4, 1, 0)) {
				bytesOut.write('_');
				bytesOut.write('b');
				bytesOut.write('i');
				bytesOut.write('n');
				bytesOut.write('a');
				bytesOut.write('r');
				bytesOut.write('y');
			}

			if (escape) {
				bytesOut.write('\'');
			}

			while (bc > 0) {
				if (escape) {
					escapeblockFast(this.streamConvertBuf, bytesOut, bc);
				} else {
					bytesOut.write(this.streamConvertBuf, 0, bc);
				}

				if (useLength) {
					bc = readblock(in, this.streamConvertBuf, lengthLeftToRead);

					if (bc > 0) {
						lengthLeftToRead -= bc;
					}
				} else {
					bc = readblock(in, this.streamConvertBuf);
				}
			}

			if (escape) {
				bytesOut.write('\'');
			}

			return bytesOut.toByteArray();
		} finally {
			if (this.connection.getAutoClosePStmtStreams()) {
				try {
					in.close();
				} catch (IOException ioEx) {
					;
				}

				in = null;
			}
		}
	}

	/**
	 * Returns this PreparedStatement represented as a string.
	 * 
	 * @return this PreparedStatement represented as a string.
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append(super.toString());
		buf.append(": "); //$NON-NLS-1$

		try {
			buf.append(asSql());
		} catch (SQLException sqlEx) {
			buf.append("EXCEPTION: " + sqlEx.toString());
		}

		return buf.toString();
	}

	public Object unwrap(Class arg0) throws SQLException {
		throw new JDBC40NotYetImplementedException();
	}

	public void setPoolable(boolean poolable) throws SQLException {
		throw new JDBC40NotYetImplementedException();
	}

	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
		setAsciiStream(parameterIndex, x, -1);
		
	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		setAsciiStream(parameterIndex, x, (int)length);
		
	}

	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
		setBinaryStream(parameterIndex, x, -1);
		
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		setBinaryStream(parameterIndex, x, (int)length);
		
	}

	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		setBinaryStream(parameterIndex, inputStream);
		
	}

	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		setCharacterStream(parameterIndex, reader, -1);
		
	}

	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		setCharacterStream(parameterIndex, reader, (int)length);
		
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		setCharacterStream(parameterIndex, reader);
		
	}

	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		setNCharacterStream(parameterIndex, value, -1);
		
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		setNCharacterStream(parameterIndex, reader);
		
	}

	public synchronized boolean isClosed() throws SQLException {
		return this.isClosed;
	}

	/**
	 * JDBC 4.0 Set a NCLOB parameter.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param reader
	 *            the java reader which contains the UNICODE data
	 * @param length
	 *            the number of characters in the stream
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
	    if (reader == null) {
	        setNull(parameterIndex, java.sql.Types.NCLOB);
	    } else {
	        setNCharacterStream(parameterIndex, reader, length);
	    }
	}
}
