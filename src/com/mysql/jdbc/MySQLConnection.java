/*
Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems
All rights reserved. Use is subject to license terms.

The MySQL Connector/J is licensed under the terms of the GPL,
like most MySQL Connectors. There are special exceptions to the
terms and conditions of the GPL as it is applied to this software,
see the FLOSS License Exception available on mysql.com.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as
published by the Free Software Foundation; version 2 of the
License.

This program is distributed in the hope that it will be useful,  
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
02110-1301 USA



 */
package com.mysql.jdbc;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.util.Properties;
import java.util.TimeZone;

import com.mysql.jdbc.log.Log;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;

public interface MySQLConnection extends Connection, ConnectionProperties {

	void abortInternal() throws SQLException;

	public void changeUser(String userName, String newPassword)
			throws SQLException;

	void checkClosed() throws SQLException;

	public void clearHasTriedMaster();

	public void clearWarnings() throws SQLException;

	public java.sql.PreparedStatement clientPrepareStatement(String sql)
			throws SQLException;

	public java.sql.PreparedStatement clientPrepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException;

	public java.sql.PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException;

	java.sql.PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			boolean processEscapeCodesIfNeeded) throws SQLException;

	public java.sql.PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException;

	public java.sql.PreparedStatement clientPrepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException;

	public java.sql.PreparedStatement clientPrepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException;

	public void close() throws SQLException;

	public void commit() throws SQLException;

	void createNewIO(boolean isForReconnect) throws SQLException;

	public java.sql.Statement createStatement() throws SQLException;

	public java.sql.Statement createStatement(int resultSetType,
			int resultSetConcurrency) throws SQLException;

	public java.sql.Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException;

	void dumpTestcaseQuery(String query);

	Connection duplicate() throws SQLException;

	ResultSetInternalMethods execSQL(StatementImpl callingStatement,
			String sql, int maxRows, Buffer packet, int resultSetType,
			int resultSetConcurrency, boolean streamResults, String catalog,
			Field[] cachedMetadata) throws SQLException;

	ResultSetInternalMethods execSQL(StatementImpl callingStatement,
			String sql, int maxRows, Buffer packet, int resultSetType,
			int resultSetConcurrency, boolean streamResults, String catalog,
			Field[] cachedMetadata, boolean isBatch) throws SQLException;

	String extractSqlFromPacket(String possibleSqlQuery, Buffer queryPacket,
			int endOfQueryPacketPosition) throws SQLException;

	StringBuffer generateConnectionCommentBlock(StringBuffer buf);

	public int getActiveStatementCount();

	public boolean getAutoCommit() throws SQLException;

	public int getAutoIncrementIncrement();

	CachedResultSetMetaData getCachedMetaData(String sql);

	Calendar getCalendarInstanceForSessionOrNew();

	Timer getCancelTimer();

	public String getCatalog() throws SQLException;

	String getCharacterSetMetadata();

	SingleByteCharsetConverter getCharsetConverter(String javaEncodingName)
			throws SQLException;

	String getCharsetNameForIndex(int charsetIndex) throws SQLException;

	public String getDateTime(String pattern);

	TimeZone getDefaultTimeZone();

	String getErrorMessageEncoding();

	public ExceptionInterceptor getExceptionInterceptor();

	public int getHoldability() throws SQLException;

	public String getHost();

	public long getId();

	public long getIdleFor();

	MysqlIO getIO() throws SQLException;

	public Log getLog() throws SQLException;

	int getMaxBytesPerChar(String javaCharsetName) throws SQLException;

	public java.sql.DatabaseMetaData getMetaData() throws SQLException;

	java.sql.Statement getMetadataSafeStatement() throws SQLException;

	Object getMutex() throws SQLException;

	int getNetBufferLength();

	public Properties getProperties();

	public boolean getRequiresEscapingEncoder();

	public String getServerCharacterEncoding();

	int getServerMajorVersion();

	int getServerMinorVersion();

	int getServerSubMinorVersion();

	public TimeZone getServerTimezoneTZ();

	String getServerVariable(String variableName);

	String getServerVersion();

	Calendar getSessionLockedCalendar();

	public String getStatementComment();

	List getStatementInterceptorsInstances();

	public int getTransactionIsolation() throws SQLException;

	public java.util.Map getTypeMap() throws SQLException;

	String getURL();

	String getUser();

	Calendar getUtcCalendar();

	public SQLWarning getWarnings() throws SQLException;

	public boolean hasSameProperties(Connection c);

	public boolean hasTriedMaster();

	void incrementNumberOfPreparedExecutes();

	void incrementNumberOfPrepares();

	void incrementNumberOfResultSetsCreated();

	public void initializeExtension(Extension ex) throws SQLException;

	void initializeResultsMetadataFromCache(String sql,
			CachedResultSetMetaData cachedMetaData,
			ResultSetInternalMethods resultSet) throws SQLException;

	void initializeSafeStatementInterceptors() throws SQLException;

	public boolean isAbonormallyLongQuery(long millisOrNanos);

	boolean isClientTzUTC();

	public boolean isClosed();

	boolean isCursorFetchEnabled() throws SQLException;

	public boolean isInGlobalTx();

	public boolean isMasterConnection();

	public boolean isNoBackslashEscapesSet();

	boolean isReadInfoMsgEnabled();

	public boolean isReadOnly() throws SQLException;

	boolean isRunningOnJDK13();

	public boolean isSameResource(Connection otherConnection);

	boolean isServerTzUTC();

	public boolean lowerCaseTableNames();

	void maxRowsChanged(Statement stmt);

	public String nativeSQL(String sql) throws SQLException;

	public boolean parserKnowsUnicode();

	public void ping() throws SQLException;

	void pingInternal(boolean checkForClosedConnection, int timeoutMillis)
			throws SQLException;

	public java.sql.CallableStatement prepareCall(String sql)
			throws SQLException;

	public java.sql.CallableStatement prepareCall(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException;

	public java.sql.CallableStatement prepareCall(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException;

	public java.sql.PreparedStatement prepareStatement(String sql)
			throws SQLException;

	public java.sql.PreparedStatement prepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException;

	public java.sql.PreparedStatement prepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException;

	public java.sql.PreparedStatement prepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException;

	public java.sql.PreparedStatement prepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException;

	public java.sql.PreparedStatement prepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException;

	void realClose(boolean calledExplicitly, boolean issueRollback,
			boolean skipLocalTeardown, Throwable reason) throws SQLException;

	void recachePreparedStatement(ServerPreparedStatement pstmt)
			throws SQLException;

	void registerQueryExecutionTime(long queryTimeMs);

	void registerStatement(Statement stmt);

	public void releaseSavepoint(Savepoint arg0) throws SQLException;

	void reportNumberOfTablesAccessed(int numTablesAccessed);

	public void reportQueryTime(long millisOrNanos);

	public void resetServerState() throws SQLException;

	public void rollback() throws SQLException;

	public void rollback(final Savepoint savepoint) throws SQLException;

	public java.sql.PreparedStatement serverPrepareStatement(String sql)
			throws SQLException;

	public java.sql.PreparedStatement serverPrepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException;

	public java.sql.PreparedStatement serverPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException;

	public java.sql.PreparedStatement serverPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException;

	public java.sql.PreparedStatement serverPrepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException;

	public java.sql.PreparedStatement serverPrepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException;

	boolean serverSupportsConvertFn() throws SQLException;

	public void setAutoCommit(final boolean autoCommitFlag) throws SQLException;

	public void setCatalog(final String catalog) throws SQLException;

	public void setFailedOver(boolean flag);

	public void setHoldability(int arg0) throws SQLException;

	public void setInGlobalTx(boolean flag);

	public void setPreferSlaveDuringFailover(boolean flag);

	public void setProxy(MySQLConnection proxy);

	void setReadInfoMsgEnabled(boolean flag);

	public void setReadOnly(boolean readOnlyFlag) throws SQLException;

	void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException;

	public java.sql.Savepoint setSavepoint() throws SQLException;

	public java.sql.Savepoint setSavepoint(String name) throws SQLException;

	public void setStatementComment(String comment);

	public void setTransactionIsolation(int level) throws SQLException;

	public void setTypeMap(java.util.Map map) throws SQLException;

	public void shutdownServer() throws SQLException;

	public boolean storesLowerCaseTableName();

	public boolean supportsIsolationLevel();

	public boolean supportsQuotedIdentifiers();

	public boolean supportsTransactions();

	public void throwConnectionClosedException() throws SQLException;

	void transactionBegun() throws SQLException;

	void transactionCompleted() throws SQLException;

	void unregisterStatement(Statement stmt);

	void unSafeStatementInterceptors() throws SQLException;

	void unsetMaxRows(Statement stmt) throws SQLException;

	boolean useAnsiQuotedIdentifiers();

	boolean useMaxRows();

	public boolean versionMeetsMinimum(int major, int minor, int subminor)
			throws SQLException;
}
