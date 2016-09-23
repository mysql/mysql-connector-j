/*
2  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.SocketAddress;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.conf.ModifiableProperty;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.io.SocketFactory;
import com.mysql.cj.api.io.SocketMetadata;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.Statement;
import com.mysql.cj.api.jdbc.interceptors.StatementInterceptor;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.core.AbstractSession;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.HostInfo;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.NetworkResources;
import com.mysql.cj.core.log.LogFactory;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.util.TimeUtil;
import com.mysql.cj.mysqla.io.MysqlaProtocol;
import com.mysql.cj.mysqla.io.MysqlaServerSession;
import com.mysql.cj.mysqla.io.MysqlaSocketConnection;

public class MysqlaSession extends AbstractSession implements Session, Serializable {

    private static final long serialVersionUID = 5323638898749073419L;

    protected transient MysqlaProtocol protocol;

    /** The timezone of the server */
    private TimeZone serverTimezoneTZ = null;

    /** c.f. getDefaultTimeZone(). this value may be overridden during connection initialization */
    private TimeZone defaultTimeZone = TimeZone.getDefault();

    /** The max-rows setting for current session */
    private int sessionMaxRows = -1;

    private HostInfo hostInfo = null;

    protected ModifiableProperty<Integer> socketTimeout;

    private boolean serverHasFracSecsSupport = true;

    public MysqlaSession(HostInfo hostInfo, PropertySet propSet) {
        this.propertySet = propSet;

        this.socketTimeout = getPropertySet().getModifiableProperty(PropertyDefinitions.PNAME_socketTimeout);

        this.hostInfo = hostInfo;

        //
        // Normally, this code would be in initializeDriverProperties, but we need to do this as early as possible, so we can start logging to the 'correct'
        // place as early as possible...this.log points to 'NullLogger' for every connection at startup to avoid NPEs and the overhead of checking for NULL at
        // every logging call.
        //
        // We will reset this to the configured logger during properties initialization.
        //
        this.log = LogFactory.getLogger(getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_logger).getStringValue(), Log.LOGGER_INSTANCE_NAME,
                getExceptionInterceptor());
    }

    public void connect(MysqlConnection conn, HostInfo hi, Properties mergedProps, String user, String password, String database, int loginTimeout)
            throws IOException {

        this.hostInfo = hi;

        // reset max-rows to default value
        this.setSessionMaxRows(-1);

        // TODO do we need different types of physical connections?
        SocketConnection socketConnection = new MysqlaSocketConnection();
        socketConnection.connect(this.hostInfo.getHost(), this.hostInfo.getPort(), mergedProps, getPropertySet(), getExceptionInterceptor(), this.log,
                loginTimeout);

        // we use physical connection to create a -> protocol
        // this configuration places no knowledge of protocol or session on physical connection.
        // physical connection is responsible *only* for I/O streams
        this.protocol = MysqlaProtocol.getInstance(conn, socketConnection, this.propertySet, this.log);

        // use protocol to create a -> session
        // protocol is responsible for building a session and authenticating (using AuthenticationProvider) internally
        this.protocol.connect(user, password, database);

        this.serverHasFracSecsSupport = this.protocol.versionMeetsMinimum(5, 6, 4);

        // error messages are returned according to character_set_results which, at this point, is set from the response packet
        setErrorMessageEncoding(this.protocol.getAuthenticationProvider().getEncodingForHandshake());
    }

    // TODO: this method should be removed after implementation of MYSQLCONNJ-478 "Bind Extension interface to Session instead of Connection"
    public MysqlaProtocol getProtocol() {
        return this.protocol;
    }

    @Override
    public void changeUser(String userName, String password, String database) {
        // reset maxRows to default value
        this.sessionMaxRows = -1;

        this.protocol.changeUser(userName, password, database);
    }

    @Override
    public boolean characterSetNamesMatches(String mysqlEncodingName) {
        return this.protocol.getServerSession().characterSetNamesMatches(mysqlEncodingName);
    }

    @Override
    public String getServerVariable(String name) {
        return this.protocol.getServerSession().getServerVariable(name);
    }

    @Override
    public int getServerVariable(String variableName, int fallbackValue) {

        try {
            return Integer.valueOf(getServerVariable(variableName));
        } catch (NumberFormatException nfe) {
            getLog().logWarn(
                    Messages.getString("Connection.BadValueInServerVariables", new Object[] { variableName, getServerVariable(variableName), fallbackValue }));

        }
        return fallbackValue;
    }

    @Override
    public boolean inTransactionOnServer() {
        return this.protocol.getServerSession().inTransactionOnServer();
    }

    @Override
    public int getServerDefaultCollationIndex() {
        return this.protocol.getServerSession().getServerDefaultCollationIndex();
    }

    @Override
    public void setServerDefaultCollationIndex(int serverDefaultCollationIndex) {
        this.protocol.getServerSession().setServerDefaultCollationIndex(serverDefaultCollationIndex);
    }

    @Override
    public Map<String, String> getServerVariables() {
        return this.protocol.getServerSession().getServerVariables();
    }

    @Override
    public void setServerVariables(Map<String, String> serverVariables) {
        this.protocol.getServerSession().setServerVariables(serverVariables);
    }

    // TODO: we should examine the call flow here, we shouldn't have to know about the socket connection but this should be address in a wider scope.
    @Override
    public void abortInternal() {
        if (this.protocol != null) {
            // checking this.protocol != null isn't enough if connection is used concurrently (the usual situation
            // with application servers which have additional thread management), this.protocol can become null
            // at any moment after this check, causing a race condition and NPEs on next calls;
            // but we may ignore them because at this stage null this.protocol means that we successfully closed all resources by other thread.
            try {
                this.protocol.getSocketConnection().forceClose();
                this.protocol.releaseResources();
            } catch (Throwable t) {
                // can't do anything about it, and we're forcibly aborting
            }
            //this.protocol = null; // TODO actually we shouldn't remove protocol instance because some it's methods can be called after closing socket
        }
        //this.isClosed = true;
    }

    @Override
    public void quit() {
        if (this.protocol != null) {
            try {
                this.protocol.quit();
            } catch (Exception e) {
            }

        }
    }

    @Override
    public void forceClose() {
        abortInternal();
    }

    @Override
    public ServerVersion getServerVersion() {
        return this.protocol.getServerSession().getServerVersion();
    }

    @Override
    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        return this.protocol.versionMeetsMinimum(major, minor, subminor);
    }

    public void enableMultiQueries() {
        PacketPayload buf = this.protocol.getSharedSendPacket();

        buf.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_SET_OPTION);
        buf.writeInteger(IntegerDataType.INT2, 0);
        sendCommand(MysqlaConstants.COM_SET_OPTION, null, buf, false, null, 0);
    }

    public void disableMultiQueries() {
        PacketPayload buf = this.protocol.getSharedSendPacket();

        buf.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_SET_OPTION);
        buf.writeInteger(IntegerDataType.INT2, 1);
        sendCommand(MysqlaConstants.COM_SET_OPTION, null, buf, false, null, 0);
    }

    @Override
    public long getThreadId() {
        return this.protocol.getServerSession().getCapabilities().getThreadId();
    }

    @Override
    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        return this.protocol.getServerSession().isSetNeededForAutoCommitMode(autoCommitFlag,
                getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_elideSetAutoCommits).getValue());
    }

    /**
     * Configures the client's timezone if required.
     * 
     * @throws CJException
     *             if the timezone the server is configured to use can't be
     *             mapped to a Java timezone.
     */
    public void configureTimezone() {
        String configuredTimeZoneOnServer = getServerVariable("time_zone");

        if ("SYSTEM".equalsIgnoreCase(configuredTimeZoneOnServer)) {
            configuredTimeZoneOnServer = getServerVariable("system_time_zone");
        }

        String canonicalTimezone = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_serverTimezone).getValue();

        if (configuredTimeZoneOnServer != null) {
            // user can override this with driver properties, so don't detect if that's the case
            if (canonicalTimezone == null || StringUtils.isEmptyOrWhitespaceOnly(canonicalTimezone)) {
                try {
                    canonicalTimezone = TimeUtil.getCanonicalTimezone(configuredTimeZoneOnServer, getExceptionInterceptor());
                } catch (IllegalArgumentException iae) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, iae.getMessage(), getExceptionInterceptor());
                }
            }
        }

        if (canonicalTimezone != null && canonicalTimezone.length() > 0) {
            this.serverTimezoneTZ = TimeZone.getTimeZone(canonicalTimezone);

            //
            // The Calendar class has the behavior of mapping unknown timezones to 'GMT' instead of throwing an exception, so we must check for this...
            //
            if (!canonicalTimezone.equalsIgnoreCase("GMT") && this.serverTimezoneTZ.getID().equals("GMT")) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Connection.9", new Object[] { canonicalTimezone }),
                        getExceptionInterceptor());
            }
        }

        this.defaultTimeZone = this.serverTimezoneTZ;
    }

    public TimeZone getDefaultTimeZone() {
        return this.defaultTimeZone;
    }

    @Override
    public String getErrorMessageEncoding() {
        return this.protocol.getServerSession().getErrorMessageEncoding();
    }

    @Override
    public void setErrorMessageEncoding(String errorMessageEncoding) {
        this.protocol.getServerSession().setErrorMessageEncoding(errorMessageEncoding);
    }

    /**
     * Returns the server's character set
     * 
     * @return the server's character set.
     */
    public String getServerCharset() {
        return this.protocol.getServerSession().getServerDefaultCharset();
    }

    public int getMaxBytesPerChar(String javaCharsetName) {
        return this.protocol.getServerSession().getMaxBytesPerChar(javaCharsetName);
    }

    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) {
        return this.protocol.getServerSession().getMaxBytesPerChar(charsetIndex, javaCharsetName);
    }

    public String getEncodingForIndex(int charsetIndex) {
        return this.protocol.getServerSession().getEncodingForIndex(charsetIndex);
    }

    public int getSessionMaxRows() {
        return this.sessionMaxRows;
    }

    public void setSessionMaxRows(int sessionMaxRows) {
        this.sessionMaxRows = sessionMaxRows;
    }

    public HostInfo getHostInfo() {
        return this.hostInfo;
    }

    public void setCharsetMaps(Map<Integer, String> indexToCharset, Map<Integer, String> customCharset, Map<String, Integer> customMblen) {
        this.protocol.getServerSession().indexToMysqlCharset = Collections.unmodifiableMap(indexToCharset);
        if (customCharset != null) {
            this.protocol.getServerSession().indexToCustomMysqlCharset = Collections.unmodifiableMap(customCharset);
        }
        if (customMblen != null) {
            this.protocol.getServerSession().mysqlCharsetToCustomMblen = Collections.unmodifiableMap(customMblen);
        }
    }

    public void setStatementInterceptors(List<StatementInterceptor> statementInterceptors) {
        this.protocol.setStatementInterceptors(statementInterceptors);
    }

    public boolean isServerLocal(JdbcConnection conn) {
        synchronized (conn.getConnectionMutex()) {
            SocketFactory factory = this.protocol.getSocketConnection().getSocketFactory();

            //if (factory instanceof SocketMetadata) {
            return ((SocketMetadata) factory).isLocallyConnected(conn);
            //}
            //this.log.logWarn(Messages.getString("Connection.NoMetadataOnSocketFactory"));
            //return false;
        }
    }

    /**
     * Used by MiniAdmin to shutdown a MySQL server
     * 
     */
    public void shutdownServer() throws SQLException {
        sendCommand(MysqlaConstants.COM_SHUTDOWN, null, null, false, null, 0);
    }

    public void setSocketTimeout(Executor executor, final int milliseconds) {
        executor.execute(new Runnable() {

            public void run() {
                MysqlaSession.this.socketTimeout.setValue(milliseconds); // for re-connects
                MysqlaSession.this.protocol.setSocketTimeout(milliseconds);
            }
        });
    }

    public int getSocketTimeout() {
        return this.socketTimeout.getValue();
    }

    /**
     * Send a query stored in a packet directly to the server.
     * 
     * @param callingStatement
     * @param resultSetConcurrency
     * @param characterEncoding
     * @param queryPacket
     * @param maxRows
     * @param conn
     * @param resultSetType
     * @param resultSetConcurrency
     * @param streamResults
     * @param catalog
     * @param unpackFieldInfo
     *            should we read MYSQL_FIELD info (if available)?
     * @throws IOException
     * 
     */
    public final <T extends Resultset> T sqlQueryDirect(StatementImpl callingStatement, String query, String characterEncoding, PacketPayload queryPacket,
            int maxRows, boolean streamResults, String catalog, ColumnDefinition cachedMetadata, ProtocolEntityFactory<T> resultSetFactory) throws IOException {

        return this.protocol.sqlQueryDirect(callingStatement, query, characterEncoding, queryPacket, maxRows, streamResults, catalog, cachedMetadata,
                this::getProfilerEventHandlerInstanceFunction, resultSetFactory);
    }

    /**
     * Determines if the database charset is the same as the platform charset
     */
    public void checkForCharsetMismatch() {
        this.protocol.checkForCharsetMismatch();
    }

    /**
     * Returns the packet used for sending data (used by PreparedStatement) with position set to 0.
     * Guarded by external synchronization on a mutex.
     * 
     * @return A packet to send data with
     */
    public PacketPayload getSharedSendPacket() {
        return this.protocol.getSharedSendPacket();
    }

    public void dumpPacketRingBuffer() {
        this.protocol.dumpPacketRingBuffer();
    }

    public <T extends Resultset> T invokeStatementInterceptorsPre(String sql, Statement interceptedStatement, boolean forceExecute) {
        return this.protocol.invokeStatementInterceptorsPre(sql, interceptedStatement, forceExecute);
    }

    public <T extends Resultset> T invokeStatementInterceptorsPost(String sql, Statement interceptedStatement, T originalResultSet, boolean forceExecute,
            Exception statementException) {
        return this.protocol.invokeStatementInterceptorsPost(sql, interceptedStatement, originalResultSet, forceExecute, statementException);
    }

    public boolean shouldIntercept() {
        return this.protocol.getStatementInterceptors() != null;
    }

    public long getCurrentTimeNanosOrMillis() {
        return this.protocol.getCurrentTimeNanosOrMillis();
    }

    public final PacketPayload sendCommand(int command, String extraData, PacketPayload queryPacket, boolean skipCheck, String extraDataCharEncoding,
            int timeoutMillis) {
        return this.protocol.sendCommand(command, extraData, queryPacket, skipCheck, extraDataCharEncoding, timeoutMillis);
    }

    public long getSlowQueryThreshold() {
        return this.protocol.getSlowQueryThreshold();
    }

    public String getQueryTimingUnits() {
        return this.protocol.getQueryTimingUnits();
    }

    /**
     * Runs an 'EXPLAIN' on the given query and dumps the results to the log
     * 
     * @param querySQL
     * @param truncatedQuery
     * 
     */
    public void explainSlowQuery(byte[] querySQL, String truncatedQuery) {
        this.protocol.explainSlowQuery(querySQL, truncatedQuery);
    }

    public boolean hadWarnings() {
        return this.protocol.hadWarnings();
    }

    public void clearInputStream() {
        this.protocol.clearInputStream();
    }

    public final PacketPayload readPacket() {
        return this.protocol.readPacket(null);
    }

    public NetworkResources getNetworkResources() {
        return this.protocol.getSocketConnection().getNetworkResources();
    }

    public MysqlaServerSession getServerSession() {
        return this.protocol.getServerSession();
    }

    @Override
    public boolean isSSLEstablished() {
        return this.protocol.getSocketConnection().isSSLEstablished();
    }

    public int getCommandCount() {
        return this.protocol.getCommandCount();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        return this.protocol.getSocketConnection().getMysqlSocket().getRemoteSocketAddress();
    }

    @Override
    public boolean serverSupportsFracSecs() {
        return this.serverHasFracSecsSupport;
    }

    public ProfilerEventHandler getProfilerEventHandlerInstanceFunction() {
        return ProfilerEventHandlerFactory.getInstance(this);
    }

    public InputStream getLocalInfileInputStream() {
        return this.protocol.getLocalInfileInputStream();
    }

    public void setLocalInfileInputStream(InputStream stream) {
        this.protocol.setLocalInfileInputStream(stream);
    }

}
