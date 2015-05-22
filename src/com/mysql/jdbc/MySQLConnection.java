/*
  Copyright (c) 2010, 2015, Oracle and/or its affiliates. All rights reserved.

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
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Timer;

import com.mysql.jdbc.log.Log;
import com.mysql.jdbc.profiler.ProfilerEventHandler;

public interface MySQLConnection extends Connection, ConnectionProperties {

    public boolean isProxySet();

    void createNewIO(boolean isForReconnect) throws SQLException;

    void dumpTestcaseQuery(String query);

    Connection duplicate() throws SQLException;

    ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType, int resultSetConcurrency,
            boolean streamResults, String catalog, Field[] cachedMetadata) throws SQLException;

    ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType, int resultSetConcurrency,
            boolean streamResults, String catalog, Field[] cachedMetadata, boolean isBatch) throws SQLException;

    String extractSqlFromPacket(String possibleSqlQuery, Buffer queryPacket, int endOfQueryPacketPosition) throws SQLException;

    StringBuilder generateConnectionCommentBlock(StringBuilder buf);

    int getActiveStatementCount();

    int getAutoIncrementIncrement();

    CachedResultSetMetaData getCachedMetaData(String sql);

    Calendar getCalendarInstanceForSessionOrNew();

    Timer getCancelTimer();

    String getCharacterSetMetadata();

    SingleByteCharsetConverter getCharsetConverter(String javaEncodingName) throws SQLException;

    /**
     * @deprecated replaced by <code>getEncodingForIndex(int collationIndex)</code>
     */
    @Deprecated
    String getCharsetNameForIndex(int charsetIndex) throws SQLException;

    String getEncodingForIndex(int collationIndex) throws SQLException;

    TimeZone getDefaultTimeZone();

    String getErrorMessageEncoding();

    ExceptionInterceptor getExceptionInterceptor();

    String getHost();

    long getId();

    long getIdleFor();

    MysqlIO getIO() throws SQLException;

    Log getLog() throws SQLException;

    int getMaxBytesPerChar(String javaCharsetName) throws SQLException;

    int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) throws SQLException;

    java.sql.Statement getMetadataSafeStatement() throws SQLException;

    int getNetBufferLength();

    Properties getProperties();

    boolean getRequiresEscapingEncoder();

    String getServerCharset();

    int getServerMajorVersion();

    int getServerMinorVersion();

    int getServerSubMinorVersion();

    TimeZone getServerTimezoneTZ();

    String getServerVariable(String variableName);

    String getServerVersion();

    Calendar getSessionLockedCalendar();

    String getStatementComment();

    List<StatementInterceptorV2> getStatementInterceptorsInstances();

    String getURL();

    String getUser();

    Calendar getUtcCalendar();

    void incrementNumberOfPreparedExecutes();

    void incrementNumberOfPrepares();

    void incrementNumberOfResultSetsCreated();

    void initializeResultsMetadataFromCache(String sql, CachedResultSetMetaData cachedMetaData, ResultSetInternalMethods resultSet) throws SQLException;

    void initializeSafeStatementInterceptors() throws SQLException;

    boolean isAbonormallyLongQuery(long millisOrNanos);

    boolean isClientTzUTC();

    boolean isCursorFetchEnabled() throws SQLException;

    boolean isReadInfoMsgEnabled();

    public boolean isReadOnly() throws SQLException;

    public boolean isReadOnly(boolean useSessionStatus) throws SQLException;

    boolean isRunningOnJDK13();

    boolean isServerTzUTC();

    boolean lowerCaseTableNames();

    void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException;

    void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException;

    void recachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException;

    void decachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException;

    void registerQueryExecutionTime(long queryTimeMs);

    void registerStatement(Statement stmt);

    void reportNumberOfTablesAccessed(int numTablesAccessed);

    boolean serverSupportsConvertFn() throws SQLException;

    void setProxy(MySQLConnection proxy);

    void setReadInfoMsgEnabled(boolean flag);

    void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException;

    void shutdownServer() throws SQLException;

    boolean storesLowerCaseTableName();

    void throwConnectionClosedException() throws SQLException;

    void transactionBegun() throws SQLException;

    void transactionCompleted() throws SQLException;

    void unregisterStatement(Statement stmt);

    void unSafeStatementInterceptors() throws SQLException;

    boolean useAnsiQuotedIdentifiers();

    String getConnectionAttributes() throws SQLException;

    /**
     * @deprecated replaced by <code>getMultiHostSafeProxy()</code>
     */
    @Deprecated
    MySQLConnection getLoadBalanceSafeProxy();

    MySQLConnection getMultiHostSafeProxy();

    ProfilerEventHandler getProfilerEventHandlerInstance();

    void setProfilerEventHandlerInstance(ProfilerEventHandler h);
}
