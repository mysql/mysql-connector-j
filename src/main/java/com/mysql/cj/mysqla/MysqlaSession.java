/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import com.mysql.cj.api.CacheAdapter;
import com.mysql.cj.api.CacheAdapterFactory;
import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.Query;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.TransactionManager;
import com.mysql.cj.api.conf.ModifiableProperty;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.interceptors.QueryInterceptor;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.io.SocketFactory;
import com.mysql.cj.api.io.SocketMetadata;
import com.mysql.cj.api.io.ValueFactory;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.ProtocolEntityFactory;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.api.mysqla.result.Resultset;
import com.mysql.cj.api.mysqla.result.Resultset.Type;
import com.mysql.cj.api.result.Row;
import com.mysql.cj.core.AbstractSession;
import com.mysql.cj.core.CharsetMapping;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.HostInfo;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.ExceptionInterceptorChain;
import com.mysql.cj.core.exceptions.PasswordExpiredException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.IntegerValueFactory;
import com.mysql.cj.core.io.LongValueFactory;
import com.mysql.cj.core.io.NetworkResources;
import com.mysql.cj.core.io.StringValueFactory;
import com.mysql.cj.core.log.LogFactory;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.result.Field;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.TimeUtil;
import com.mysql.cj.mysqla.io.MysqlaProtocol;
import com.mysql.cj.mysqla.io.MysqlaServerSession;
import com.mysql.cj.mysqla.io.MysqlaSocketConnection;
import com.mysql.cj.mysqla.io.ResultsetFactory;

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
    private ReadableProperty<Boolean> gatherPerfMetrics;
    private ModifiableProperty<String> characterEncoding;
    private ReadableProperty<Boolean> useOldUTF8Behavior;
    private ReadableProperty<Boolean> disconnectOnExpiredPasswords;
    private ReadableProperty<Boolean> cacheServerConfiguration;

    private boolean serverHasFracSecsSupport = true;

    private CacheAdapter<String, Map<String, String>> serverConfigCache;

    /**
     * Actual collation index to mysql charset name map of user defined charsets for given server URLs.
     */
    private static final Map<String, Map<Integer, String>> customIndexToCharsetMapByUrl = new HashMap<String, Map<Integer, String>>();

    /**
     * Actual mysql charset name to mblen map of user defined charsets for given server URLs.
     */
    private static final Map<String, Map<String, Integer>> customCharsetToMblenMapByUrl = new HashMap<String, Map<String, Integer>>();

    /**
     * If a CharsetEncoder is required for escaping. Needed for SJIS and related
     * problems with \u00A5.
     */
    private boolean requiresEscapingEncoder;

    public MysqlaSession(HostInfo hostInfo, PropertySet propSet) {
        this.propertySet = propSet;

        this.socketTimeout = getPropertySet().getModifiableProperty(PropertyDefinitions.PNAME_socketTimeout);
        this.gatherPerfMetrics = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_gatherPerfMetrics);
        this.characterEncoding = getPropertySet().getModifiableProperty(PropertyDefinitions.PNAME_characterEncoding);
        this.useOldUTF8Behavior = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldUTF8Behavior);
        this.disconnectOnExpiredPasswords = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords);
        this.cacheServerConfiguration = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheServerConfiguration);

        this.hostInfo = hostInfo;

        //
        // Normally, this code would be in initializeDriverProperties, but we need to do this as early as possible, so we can start logging to the 'correct'
        // place as early as possible...this.log points to 'NullLogger' for every connection at startup to avoid NPEs and the overhead of checking for NULL at
        // every logging call.
        //
        // We will reset this to the configured logger during properties initialization.
        //
        this.log = LogFactory.getLogger(getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_logger).getStringValue(),
                Log.LOGGER_INSTANCE_NAME);
    }

    public void connect(MysqlConnection conn, HostInfo hi, Properties mergedProps, String user, String password, String database, int loginTimeout,
            TransactionManager transactionManager) throws IOException {

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
        if (this.protocol == null) {
            this.protocol = MysqlaProtocol.getInstance(conn, socketConnection, this.propertySet, this.log, transactionManager);
        } else {
            this.protocol.init(conn, socketConnection, this.propertySet, transactionManager);
        }

        // use protocol to create a -> session
        // protocol is responsible for building a session and authenticating (using AuthenticationProvider) internally
        this.protocol.connect(user, password, database);

        this.serverHasFracSecsSupport = this.protocol.versionMeetsMinimum(5, 6, 4);

        // error messages are returned according to character_set_results which, at this point, is set from the response packet
        this.protocol.getServerSession().setErrorMessageEncoding(this.protocol.getAuthenticationProvider().getEncodingForHandshake());
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

    // TODO remove ?
    @Override
    public String getServerVariable(String name) {
        return this.protocol.getServerSession().getServerVariable(name);
    }

    // TODO remove ?
    @Override
    public int getServerVariable(String variableName, int fallbackValue) {

        try {
            return Integer.valueOf(this.protocol.getServerSession().getServerVariable(variableName));
        } catch (NumberFormatException nfe) {
            getLog().logWarn(Messages.getString("Connection.BadValueInServerVariables",
                    new Object[] { variableName, this.protocol.getServerSession().getServerVariable(variableName), fallbackValue }));

        }
        return fallbackValue;
    }

    @Override
    public boolean inTransactionOnServer() {
        return this.protocol.getServerSession().inTransactionOnServer();
    }

    // TODO remove ?
    @Override
    public Map<String, String> getServerVariables() {
        return this.protocol.getServerSession().getServerVariables();
    }

    // TODO remove?
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
        sendCommand(MysqlaConstants.COM_SET_OPTION, buf, false, 0);
    }

    public void disableMultiQueries() {
        PacketPayload buf = this.protocol.getSharedSendPacket();

        buf.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_SET_OPTION);
        buf.writeInteger(IntegerDataType.INT2, 1);
        sendCommand(MysqlaConstants.COM_SET_OPTION, buf, false, 0);
    }

    @Override
    public long getThreadId() {
        return this.protocol.getServerSession().getCapabilities().getThreadId();
    }

    @Override
    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        // Server Bug#66884 (SERVER_STATUS is always initiated with SERVER_STATUS_AUTOCOMMIT=1) invalidates "elideSetAutoCommits" feature.
        // TODO Turn this feature back on as soon as the server bug is fixed. Consider making it version specific.
        //return this.protocol.getServerSession().isSetNeededForAutoCommitMode(autoCommitFlag,
        //        getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_elideSetAutoCommits).getValue());
        return this.protocol.getServerSession().isSetNeededForAutoCommitMode(autoCommitFlag, false);
    }

    /**
     * Configures the client's timezone if required.
     * 
     * @throws CJException
     *             if the timezone the server is configured to use can't be
     *             mapped to a Java timezone.
     */
    public void configureTimezone() {
        String configuredTimeZoneOnServer = this.protocol.getServerSession().getServerVariable("time_zone");

        if ("SYSTEM".equalsIgnoreCase(configuredTimeZoneOnServer)) {
            configuredTimeZoneOnServer = this.protocol.getServerSession().getServerVariable("system_time_zone");
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

    // TODO remove ?
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

    public void setQueryInterceptors(List<QueryInterceptor> queryInterceptors) {
        this.protocol.setQueryInterceptors(queryInterceptors);
    }

    public boolean isServerLocal(MysqlConnection conn) {
        synchronized (conn.getConnectionMutex()) {
            SocketFactory factory = this.protocol.getSocketConnection().getSocketFactory();
            return ((SocketMetadata) factory).isLocallyConnected(conn.getSession());
        }
    }

    /**
     * Used by MiniAdmin to shutdown a MySQL server
     * 
     */
    public void shutdownServer() {
        PacketPayload packet = getSharedSendPacket();
        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_SHUTDOWN);
        sendCommand(MysqlaConstants.COM_SHUTDOWN, packet, false, 0);
    }

    public void setSocketTimeout(int milliseconds) {
        this.socketTimeout.setValue(milliseconds); // for re-connects
        this.protocol.setSocketTimeout(milliseconds);
    }

    public int getSocketTimeout() {
        return this.socketTimeout.getValue();
    }

    /**
     * Build a query packet from the given string and send it to the server.
     * 
     * @param callingQuery
     * @param query
     * @param charEncoding
     * @param maxRows
     * @param streamResults
     * @param catalog
     * @param cachedMetadata
     * @param resultSetFactory
     * @return
     * @throws IOException
     */
    public final <T extends Resultset> T sendQueryString(Query callingQuery, String query, String charEncoding, int maxRows, boolean streamResults,
            String catalog, ColumnDefinition cachedMetadata, ProtocolEntityFactory<T> resultSetFactory) throws IOException {

        return this.protocol.sendQueryString(callingQuery, query, charEncoding, maxRows, streamResults, catalog, cachedMetadata,
                this::getProfilerEventHandlerInstanceFunction, resultSetFactory);
    }

    /**
     * Send a query stored in a packet to the server.
     * 
     * @param callingQuery
     * @param queryPacket
     * @param maxRows
     * @param streamResults
     * @param catalog
     * @param cachedMetadata
     * @param resultSetFactory
     * @return
     * @throws IOException
     */
    public final <T extends Resultset> T sendQueryPacket(Query callingQuery, PacketPayload queryPacket, int maxRows, boolean streamResults, String catalog,
            ColumnDefinition cachedMetadata, ProtocolEntityFactory<T> resultSetFactory) throws IOException {

        return this.protocol.sendQueryPacket(callingQuery, queryPacket, maxRows, streamResults, catalog, cachedMetadata,
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

    public <T extends Resultset> T invokeQueryInterceptorsPre(String sql, Query interceptedQuery, boolean forceExecute) {
        return this.protocol.invokeQueryInterceptorsPre(sql, interceptedQuery, forceExecute);
    }

    public <T extends Resultset> T invokeQueryInterceptorsPost(String sql, Query interceptedQuery, T originalResultSet, boolean forceExecute) {
        return this.protocol.invokeQueryInterceptorsPost(sql, interceptedQuery, originalResultSet, forceExecute);
    }

    public boolean shouldIntercept() {
        return this.protocol.getQueryInterceptors() != null;
    }

    public long getCurrentTimeNanosOrMillis() {
        return this.protocol.getCurrentTimeNanosOrMillis();
    }

    public final PacketPayload sendCommand(int command, PacketPayload queryPacket, boolean skipCheck, int timeoutMillis) {
        return this.protocol.sendCommand(command, queryPacket, skipCheck, timeoutMillis);
    }

    public long getSlowQueryThreshold() {
        return this.protocol.getSlowQueryThreshold();
    }

    public String getQueryTimingUnits() {
        return this.protocol.getQueryTimingUnits();
    }

    public boolean hadWarnings() {
        return this.protocol.hadWarnings();
    }

    public void clearInputStream() {
        this.protocol.clearInputStream();
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

    public void registerQueryExecutionTime(long queryTimeMs) {
        this.protocol.getMetricsHolder().registerQueryExecutionTime(queryTimeMs);
    }

    public void reportNumberOfTablesAccessed(int numTablesAccessed) {
        this.protocol.getMetricsHolder().reportNumberOfTablesAccessed(numTablesAccessed);
    }

    public void incrementNumberOfPreparedExecutes() {
        if (this.gatherPerfMetrics.getValue()) {
            this.protocol.getMetricsHolder().incrementNumberOfPreparedExecutes();
        }
    }

    public void incrementNumberOfPrepares() {
        if (this.gatherPerfMetrics.getValue()) {
            this.protocol.getMetricsHolder().incrementNumberOfPrepares();
        }
    }

    public void incrementNumberOfResultSetsCreated() {
        if (this.gatherPerfMetrics.getValue()) {
            this.protocol.getMetricsHolder().incrementNumberOfResultSetsCreated();
        }
    }

    public void reportMetrics() {
        if (this.gatherPerfMetrics.getValue()) {

        }
    }

    /**
     * Configures client-side properties for character set information.
     */
    private void configureCharsetProperties() {
        if (this.characterEncoding.getValue() != null) {
            // Attempt to use the encoding, and bail out if it can't be used
            try {
                String testString = "abc";
                StringUtils.getBytes(testString, this.characterEncoding.getValue());
            } catch (WrongArgumentException waEx) {
                // Try the MySQL character encoding, then....
                String oldEncoding = this.characterEncoding.getValue();

                this.characterEncoding.setValue(CharsetMapping.getJavaEncodingForMysqlCharset(oldEncoding));

                if (this.characterEncoding.getValue() == null) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Connection.5", new Object[] { oldEncoding }),
                            getExceptionInterceptor());
                }

                String testString = "abc";
                StringUtils.getBytes(testString, this.characterEncoding.getValue());
            }
        }
    }

    /**
     * Sets up client character set. This must be done before any further communication with the server!
     * 
     * @return true if this routine actually configured the client character
     *         set, or false if the driver needs to use 'older' methods to
     *         detect the character set, as it is connected to a MySQL server
     *         older than 4.1.0
     * @throws CJException
     *             if an exception happens while sending 'SET NAMES' to the
     *             server, or the server sends character set information that
     *             the client doesn't know about.
     */
    public boolean configureClientCharacterSet(boolean dontCheckServerMatch) {
        String realJavaEncoding = this.characterEncoding.getValue();
        ModifiableProperty<String> characterSetResults = getPropertySet().getModifiableProperty(PropertyDefinitions.PNAME_characterSetResults);
        boolean characterSetAlreadyConfigured = false;

        try {
            characterSetAlreadyConfigured = true;

            configureCharsetProperties();
            realJavaEncoding = this.characterEncoding.getValue(); // we need to do this again to grab this for versions > 4.1.0

            try {

                // Fault injection for testing server character set indices
                /*
                 * if (this.props != null && this.props.getProperty(PropertyDefinitions.PNAME_testsuite_faultInjection_serverCharsetIndex) != null) {
                 * this.session.setServerDefaultCollationIndex(
                 * Integer.parseInt(this.props.getProperty(PropertyDefinitions.PNAME_testsuite_faultInjection_serverCharsetIndex)));
                 * }
                 */

                String serverEncodingToSet = CharsetMapping.getJavaEncodingForCollationIndex(this.protocol.getServerSession().getServerDefaultCollationIndex());

                if (serverEncodingToSet == null || serverEncodingToSet.length() == 0) {
                    if (realJavaEncoding != null) {
                        // user knows best, try it
                        this.characterEncoding.setValue(realJavaEncoding);
                    } else {
                        throw ExceptionFactory.createException(
                                Messages.getString("Connection.6", new Object[] { this.protocol.getServerSession().getServerDefaultCollationIndex() }),
                                getExceptionInterceptor());
                    }
                }

                // "latin1" on MySQL-4.1.0+ is actually CP1252, not ISO8859_1
                if ("ISO8859_1".equalsIgnoreCase(serverEncodingToSet)) {
                    serverEncodingToSet = "Cp1252";
                }
                if ("UnicodeBig".equalsIgnoreCase(serverEncodingToSet) || "UTF-16".equalsIgnoreCase(serverEncodingToSet)
                        || "UTF-16LE".equalsIgnoreCase(serverEncodingToSet) || "UTF-32".equalsIgnoreCase(serverEncodingToSet)) {
                    serverEncodingToSet = "UTF-8";
                }

                this.characterEncoding.setValue(serverEncodingToSet);

            } catch (ArrayIndexOutOfBoundsException outOfBoundsEx) {
                if (realJavaEncoding != null) {
                    // user knows best, try it
                    this.characterEncoding.setValue(realJavaEncoding);
                } else {
                    throw ExceptionFactory.createException(
                            Messages.getString("Connection.6", new Object[] { this.protocol.getServerSession().getServerDefaultCollationIndex() }),
                            getExceptionInterceptor());
                }
            }

            if (this.characterEncoding.getValue() == null) {
                // punt?
                this.characterEncoding.setValue("ISO8859_1");
            }

            if (realJavaEncoding != null) {

                //
                // Now, inform the server what character set we will be using from now-on...
                //
                if (realJavaEncoding.equalsIgnoreCase("UTF-8") || realJavaEncoding.equalsIgnoreCase("UTF8")) {
                    // charset names are case-sensitive

                    boolean useutf8mb4 = CharsetMapping.UTF8MB4_INDEXES.contains(this.protocol.getServerSession().getServerDefaultCollationIndex());

                    if (!this.useOldUTF8Behavior.getValue()) {
                        if (dontCheckServerMatch || !this.protocol.getServerSession().characterSetNamesMatches("utf8")
                                || (!this.protocol.getServerSession().characterSetNamesMatches("utf8mb4"))) {

                            PacketPayload packet = getSharedSendPacket();
                            packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                            packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SET NAMES " + (useutf8mb4 ? "utf8mb4" : "utf8")));
                            sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);

                            this.protocol.getServerSession().getServerVariables().put("character_set_client", useutf8mb4 ? "utf8mb4" : "utf8");
                            this.protocol.getServerSession().getServerVariables().put("character_set_connection", useutf8mb4 ? "utf8mb4" : "utf8");
                        }
                    } else {
                        PacketPayload packet = getSharedSendPacket();
                        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                        packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SET NAMES latin1"));
                        sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);

                        this.protocol.getServerSession().getServerVariables().put("character_set_client", "latin1");
                        this.protocol.getServerSession().getServerVariables().put("character_set_connection", "latin1");
                    }

                    this.characterEncoding.setValue(realJavaEncoding);
                } /* not utf-8 */else {
                    String mysqlCharsetName = CharsetMapping.getMysqlCharsetForJavaEncoding(realJavaEncoding.toUpperCase(Locale.ENGLISH), getServerVersion());

                    if (mysqlCharsetName != null) {

                        if (dontCheckServerMatch || !this.protocol.getServerSession().characterSetNamesMatches(mysqlCharsetName)) {
                            PacketPayload packet = getSharedSendPacket();
                            packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                            packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SET NAMES " + mysqlCharsetName));
                            sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);

                            this.protocol.getServerSession().getServerVariables().put("character_set_client", mysqlCharsetName);
                            this.protocol.getServerSession().getServerVariables().put("character_set_connection", mysqlCharsetName);
                        }
                    }

                    // Switch driver's encoding now, since the server knows what we're sending...
                    //
                    this.characterEncoding.setValue(realJavaEncoding);
                }
            } else if (this.characterEncoding.getValue() != null) {
                // Tell the server we'll use the server default charset to send our queries from now on....
                String mysqlCharsetName = getServerCharset();

                if (this.useOldUTF8Behavior.getValue()) {
                    mysqlCharsetName = "latin1";
                }

                boolean ucs2 = false;
                if ("ucs2".equalsIgnoreCase(mysqlCharsetName) || "utf16".equalsIgnoreCase(mysqlCharsetName) || "utf16le".equalsIgnoreCase(mysqlCharsetName)
                        || "utf32".equalsIgnoreCase(mysqlCharsetName)) {
                    mysqlCharsetName = "utf8";
                    ucs2 = true;
                    if (characterSetResults.getValue() == null) {
                        characterSetResults.setValue("UTF-8");
                    }
                }

                if (dontCheckServerMatch || !this.protocol.getServerSession().characterSetNamesMatches(mysqlCharsetName) || ucs2) {
                    try {
                        PacketPayload packet = getSharedSendPacket();
                        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                        packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SET NAMES " + mysqlCharsetName));
                        sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);

                        this.protocol.getServerSession().getServerVariables().put("character_set_client", mysqlCharsetName);
                        this.protocol.getServerSession().getServerVariables().put("character_set_connection", mysqlCharsetName);
                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }
                }

                realJavaEncoding = this.characterEncoding.getValue();
            }

            //
            // We know how to deal with any charset coming back from the database, so tell the server not to do conversion if the user hasn't 'forced' a
            // result-set character set
            //

            String onServer = null;
            boolean isNullOnServer = false;

            if (this.protocol.getServerSession().getServerVariables() != null) {
                onServer = this.protocol.getServerSession().getServerVariable("character_set_results");

                isNullOnServer = onServer == null || "NULL".equalsIgnoreCase(onServer) || onServer.length() == 0;
            }

            if (characterSetResults.getValue() == null) {

                //
                // Only send if needed, if we're caching server variables we -have- to send, because we don't know what it was before we cached them.
                //
                if (!isNullOnServer) {
                    try {
                        PacketPayload packet = getSharedSendPacket();
                        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                        packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SET character_set_results = NULL"));
                        sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);

                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }
                    this.protocol.getServerSession().getServerVariables().put(ServerSession.JDBC_LOCAL_CHARACTER_SET_RESULTS, null);
                } else {
                    this.protocol.getServerSession().getServerVariables().put(ServerSession.JDBC_LOCAL_CHARACTER_SET_RESULTS, onServer);
                }
            } else {

                if (this.useOldUTF8Behavior.getValue()) {
                    try {
                        PacketPayload packet = getSharedSendPacket();
                        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                        packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SET NAMES latin1"));
                        sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);

                        this.protocol.getServerSession().getServerVariables().put("character_set_client", "latin1");
                        this.protocol.getServerSession().getServerVariables().put("character_set_connection", "latin1");
                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }
                }
                String charsetResults = characterSetResults.getValue();
                String mysqlEncodingName = null;

                if ("UTF-8".equalsIgnoreCase(charsetResults) || "UTF8".equalsIgnoreCase(charsetResults)) {
                    mysqlEncodingName = "utf8";
                } else if ("null".equalsIgnoreCase(charsetResults)) {
                    mysqlEncodingName = "NULL";
                } else {
                    mysqlEncodingName = CharsetMapping.getMysqlCharsetForJavaEncoding(charsetResults.toUpperCase(Locale.ENGLISH), getServerVersion());
                }

                //
                // Only change the value if needed
                //

                if (mysqlEncodingName == null) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Connection.7", new Object[] { charsetResults }),
                            getExceptionInterceptor());
                }

                if (!mysqlEncodingName.equalsIgnoreCase(this.protocol.getServerSession().getServerVariable("character_set_results"))) {
                    StringBuilder setBuf = new StringBuilder("SET character_set_results = ".length() + mysqlEncodingName.length());
                    setBuf.append("SET character_set_results = ").append(mysqlEncodingName);

                    try {
                        PacketPayload packet = getSharedSendPacket();
                        packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                        packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(setBuf.toString()));
                        sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);

                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }

                    this.protocol.getServerSession().getServerVariables().put(ServerSession.JDBC_LOCAL_CHARACTER_SET_RESULTS, mysqlEncodingName);

                    // We have to set errorMessageEncoding according to new value of charsetResults for server version 5.5 and higher
                    this.protocol.getServerSession().setErrorMessageEncoding(charsetResults);

                } else {
                    this.protocol.getServerSession().getServerVariables().put(ServerSession.JDBC_LOCAL_CHARACTER_SET_RESULTS, onServer);
                }
            }

            String connectionCollation = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionCollation).getStringValue();
            if (connectionCollation != null) {
                StringBuilder setBuf = new StringBuilder("SET collation_connection = ".length() + connectionCollation.length());
                setBuf.append("SET collation_connection = ").append(connectionCollation);

                try {
                    PacketPayload packet = getSharedSendPacket();
                    packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                    packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(setBuf.toString()));
                    sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);

                } catch (PasswordExpiredException ex) {
                    if (this.disconnectOnExpiredPasswords.getValue()) {
                        throw ex;
                    }
                }
            }
        } finally {
            // Failsafe, make sure that the driver's notion of character encoding matches what the user has specified.
            //
            this.characterEncoding.setValue(realJavaEncoding);
        }

        /**
         * Check if we need a CharsetEncoder for escaping codepoints that are
         * transformed to backslash (0x5c) in the connection encoding.
         */
        try {
            CharsetEncoder enc = Charset.forName(this.characterEncoding.getValue()).newEncoder();
            CharBuffer cbuf = CharBuffer.allocate(1);
            ByteBuffer bbuf = ByteBuffer.allocate(1);

            cbuf.put("\u00a5");
            cbuf.position(0);
            enc.encode(cbuf, bbuf, true);
            if (bbuf.get(0) == '\\') {
                this.requiresEscapingEncoder = true;
            } else {
                cbuf.clear();
                bbuf.clear();

                cbuf.put("\u20a9");
                cbuf.position(0);
                enc.encode(cbuf, bbuf, true);
                if (bbuf.get(0) == '\\') {
                    this.requiresEscapingEncoder = true;
                }
            }
        } catch (java.nio.charset.UnsupportedCharsetException ucex) {
            // fallback to String API
            byte bbuf[] = StringUtils.getBytes("\u00a5", this.characterEncoding.getValue());
            if (bbuf[0] == '\\') {
                this.requiresEscapingEncoder = true;
            } else {
                bbuf = StringUtils.getBytes("\u20a9", this.characterEncoding.getValue());
                if (bbuf[0] == '\\') {
                    this.requiresEscapingEncoder = true;
                }
            }
        }

        return characterSetAlreadyConfigured;
    }

    public boolean getRequiresEscapingEncoder() {
        return this.requiresEscapingEncoder;
    }

    private void createConfigCacheIfNeeded(MysqlConnection conn) {
        synchronized (conn.getConnectionMutex()) {
            if (this.serverConfigCache != null) {
                return;
            }

            try {
                Class<?> factoryClass;

                factoryClass = Class.forName(getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_serverConfigCacheFactory).getStringValue());

                @SuppressWarnings("unchecked")
                CacheAdapterFactory<String, Map<String, String>> cacheFactory = ((CacheAdapterFactory<String, Map<String, String>>) factoryClass.newInstance());

                this.serverConfigCache = cacheFactory.getInstance(conn, this.hostInfo.getDatabaseUrl(), Integer.MAX_VALUE, Integer.MAX_VALUE);

                ExceptionInterceptor evictOnCommsError = new ExceptionInterceptor() {

                    public ExceptionInterceptor init(Properties config, Log log1) {
                        return this;
                    }

                    public void destroy() {
                    }

                    @SuppressWarnings("synthetic-access")
                    public Exception interceptException(Exception sqlEx) {
                        if (sqlEx instanceof SQLException && ((SQLException) sqlEx).getSQLState() != null
                                && ((SQLException) sqlEx).getSQLState().startsWith("08")) {
                            MysqlaSession.this.serverConfigCache.invalidate(MysqlaSession.this.hostInfo.getDatabaseUrl());
                        }
                        return null;
                    }
                };

                if (this.exceptionInterceptor == null) {
                    this.exceptionInterceptor = evictOnCommsError;
                } else {
                    ((ExceptionInterceptorChain) this.exceptionInterceptor).addRingZero(evictOnCommsError);
                }
            } catch (ClassNotFoundException e) {
                throw ExceptionFactory.createException(Messages.getString("Connection.CantFindCacheFactory",
                        new Object[] { getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).getValue(),
                                PropertyDefinitions.PNAME_parseInfoCacheFactory }),
                        e, getExceptionInterceptor());
            } catch (InstantiationException | IllegalAccessException | CJException e) {
                throw ExceptionFactory.createException(Messages.getString("Connection.CantLoadCacheFactory",
                        new Object[] { getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).getValue(),
                                PropertyDefinitions.PNAME_parseInfoCacheFactory }),
                        e, getExceptionInterceptor());
            }
        }
    }

    // TODO what's the purpose of this variable?
    private final static String SERVER_VERSION_STRING_VAR_NAME = "server_version_string";

    /**
     * Loads the result of 'SHOW VARIABLES' into the serverVariables field so
     * that the driver can configure itself.
     */
    public void loadServerVariables(MysqlConnection conn, String version) {

        if (this.cacheServerConfiguration.getValue()) {
            createConfigCacheIfNeeded(conn);

            Map<String, String> cachedVariableMap = this.serverConfigCache.get(this.hostInfo.getDatabaseUrl());

            if (cachedVariableMap != null) {
                String cachedServerVersion = cachedVariableMap.get(SERVER_VERSION_STRING_VAR_NAME);

                if (cachedServerVersion != null && getServerVersion() != null && cachedServerVersion.equals(getServerVersion().toString())) {
                    this.protocol.getServerSession().setServerVariables(cachedVariableMap);

                    return;
                }

                this.serverConfigCache.invalidate(this.hostInfo.getDatabaseUrl());
            }
        }

        try {
            if (version != null && version.indexOf('*') != -1) {
                StringBuilder buf = new StringBuilder(version.length() + 10);
                for (int i = 0; i < version.length(); i++) {
                    char c = version.charAt(i);
                    buf.append(c == '*' ? "[star]" : c);
                }
                version = buf.toString();
            }

            String versionComment = (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_paranoid).getValue() || version == null) ? ""
                    : "/* " + version + " */";

            this.protocol.getServerSession().setServerVariables(new HashMap<String, String>());

            if (versionMeetsMinimum(5, 1, 0)) {
                StringBuilder queryBuf = new StringBuilder(versionComment).append("SELECT");
                queryBuf.append("  @@session.auto_increment_increment AS auto_increment_increment");
                queryBuf.append(", @@character_set_client AS character_set_client");
                queryBuf.append(", @@character_set_connection AS character_set_connection");
                queryBuf.append(", @@character_set_results AS character_set_results");
                queryBuf.append(", @@character_set_server AS character_set_server");
                queryBuf.append(", @@init_connect AS init_connect");
                queryBuf.append(", @@interactive_timeout AS interactive_timeout");
                if (!versionMeetsMinimum(5, 5, 0)) {
                    queryBuf.append(", @@language AS language");
                }
                queryBuf.append(", @@license AS license");
                queryBuf.append(", @@lower_case_table_names AS lower_case_table_names");
                queryBuf.append(", @@max_allowed_packet AS max_allowed_packet");
                queryBuf.append(", @@net_buffer_length AS net_buffer_length");
                queryBuf.append(", @@net_write_timeout AS net_write_timeout");
                queryBuf.append(", @@query_cache_size AS query_cache_size");
                queryBuf.append(", @@query_cache_type AS query_cache_type");
                queryBuf.append(", @@sql_mode AS sql_mode");
                queryBuf.append(", @@system_time_zone AS system_time_zone");
                queryBuf.append(", @@time_zone AS time_zone");
                queryBuf.append(", @@tx_isolation AS tx_isolation");
                queryBuf.append(", @@wait_timeout AS wait_timeout");

                PacketPayload packet = getSharedSendPacket();
                packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(queryBuf.toString()));
                PacketPayload resultPacket = sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);
                Resultset rs = this.protocol.readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));
                Field[] f = rs.getColumnDefinition().getFields();
                if (f.length > 0) {
                    ValueFactory<String> vf = new StringValueFactory(f[0].getEncoding());
                    Row r;
                    if ((r = rs.getRows().next()) != null) {
                        for (int i = 0; i < f.length; i++) {
                            this.protocol.getServerSession().getServerVariables().put(f[i].getColumnLabel(), r.getValue(i, vf));
                        }
                    }
                }
            } else {
                PacketPayload packet = getSharedSendPacket();
                packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(versionComment + "SHOW VARIABLES"));
                PacketPayload resultPacket = sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);
                Resultset rs = this.protocol.readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));
                ValueFactory<String> vf = new StringValueFactory(rs.getColumnDefinition().getFields()[0].getEncoding());
                Row r;
                while ((r = rs.getRows().next()) != null) {
                    this.protocol.getServerSession().getServerVariables().put(r.getValue(0, vf), r.getValue(1, vf));
                }
            }
        } catch (PasswordExpiredException ex) {
            if (this.disconnectOnExpiredPasswords.getValue()) {
                throw ex;
            }
        } catch (IOException e) {
            throw ExceptionFactory.createException(e.getMessage(), e);
        }

        if (this.cacheServerConfiguration.getValue()) {
            this.protocol.getServerSession().getServerVariables().put(SERVER_VERSION_STRING_VAR_NAME, getServerVersion().toString());
            this.serverConfigCache.put(this.hostInfo.getDatabaseUrl(), this.protocol.getServerSession().getServerVariables());
        }
    }

    public void setSessionVariables() {
        String sessionVariables = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_sessionVariables).getValue();
        if (sessionVariables != null) {
            List<String> variablesToSet = StringUtils.split(sessionVariables, ",", "\"'", "\"'", false);

            int numVariablesToSet = variablesToSet.size();

            for (int i = 0; i < numVariablesToSet; i++) {
                String variableValuePair = variablesToSet.get(i);

                PacketPayload packet = getSharedSendPacket();
                packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                packet.writeBytes(StringLengthDataType.STRING_FIXED,
                        StringUtils.getBytes(variableValuePair.startsWith("@") ? "SET " + variableValuePair : "SET SESSION " + variableValuePair));
                sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);
            }
        }

    }

    /**
     * Builds the map needed for 4.1.0 and newer servers that maps field-level
     * charset/collation info to a java character encoding name.
     */
    public void buildCollationMapping() {

        Map<Integer, String> customCharset = null;
        Map<String, Integer> customMblen = null;

        String databaseURL = this.hostInfo.getDatabaseUrl();

        if (this.cacheServerConfiguration.getValue()) {
            synchronized (customIndexToCharsetMapByUrl) {
                customCharset = customIndexToCharsetMapByUrl.get(databaseURL);
                customMblen = customCharsetToMblenMapByUrl.get(databaseURL);
            }
        }

        if (customCharset == null && getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_detectCustomCollations).getValue()) {
            customCharset = new HashMap<Integer, String>();
            customMblen = new HashMap<String, Integer>();

            ValueFactory<Integer> ivf = new IntegerValueFactory();

            try {
                PacketPayload packet = getSharedSendPacket();
                packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SHOW COLLATION"));
                PacketPayload resultPacket = sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);
                Resultset rs = this.protocol.readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));
                ValueFactory<String> svf = new StringValueFactory(rs.getColumnDefinition().getFields()[1].getEncoding());
                Row r;
                while ((r = rs.getRows().next()) != null) {
                    int collationIndex = ((Number) r.getValue(2, ivf)).intValue();
                    String charsetName = r.getValue(1, svf);

                    // if no static map for charsetIndex or server has a different mapping then our static map, adding it to custom map 
                    if (collationIndex >= CharsetMapping.MAP_SIZE || !charsetName.equals(CharsetMapping.getMysqlCharsetNameForCollationIndex(collationIndex))) {
                        customCharset.put(collationIndex, charsetName);
                    }

                    // if no static map for charsetName adding to custom map
                    if (!CharsetMapping.CHARSET_NAME_TO_CHARSET.containsKey(charsetName)) {
                        customMblen.put(charsetName, null);
                    }
                }
            } catch (PasswordExpiredException ex) {
                if (this.disconnectOnExpiredPasswords.getValue()) {
                    throw ex;
                }
            } catch (IOException e) {
                throw ExceptionFactory.createException(e.getMessage(), e, this.exceptionInterceptor);
            }

            // if there is a number of custom charsets we should execute SHOW CHARACTER SET to know theirs mblen
            if (customMblen.size() > 0) {
                try {
                    PacketPayload packet = getSharedSendPacket();
                    packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                    packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SHOW CHARACTER SET"));
                    PacketPayload resultPacket = sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);
                    Resultset rs = this.protocol.readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));

                    int charsetColumn = rs.getColumnDefinition().getColumnNameToIndex().get("Charset");
                    int maxlenColumn = rs.getColumnDefinition().getColumnNameToIndex().get("Maxlen");

                    ValueFactory<String> svf = new StringValueFactory(rs.getColumnDefinition().getFields()[1].getEncoding());
                    Row r;
                    while ((r = rs.getRows().next()) != null) {
                        String charsetName = r.getValue(charsetColumn, svf);
                        if (customMblen.containsKey(charsetName)) {
                            customMblen.put(charsetName, r.getValue(maxlenColumn, ivf));
                        }
                    }
                } catch (PasswordExpiredException ex) {
                    if (this.disconnectOnExpiredPasswords.getValue()) {
                        throw ex;
                    }
                } catch (IOException e) {
                    throw ExceptionFactory.createException(e.getMessage(), e, this.exceptionInterceptor);
                }
            }

            if (this.cacheServerConfiguration.getValue()) {
                synchronized (customIndexToCharsetMapByUrl) {
                    customIndexToCharsetMapByUrl.put(databaseURL, customCharset);
                    customCharsetToMblenMapByUrl.put(databaseURL, customMblen);
                }
            }
        }

        // set charset maps
        if (customCharset != null) {
            this.protocol.getServerSession().indexToCustomMysqlCharset = Collections.unmodifiableMap(customCharset);
        }
        if (customMblen != null) {
            this.protocol.getServerSession().mysqlCharsetToCustomMblen = Collections.unmodifiableMap(customMblen);
        }
    }

    public String getProcessHost() {
        try {
            long threadId = getThreadId();
            String processHost = findProcessHost(threadId);

            if (processHost == null) {
                // http://bugs.mysql.com/bug.php?id=44167 - connection ids on the wire wrap at 4 bytes even though they're 64-bit numbers
                this.log.logWarn(String.format(
                        "Connection id %d not found in \"SHOW PROCESSLIST\", assuming 32-bit overflow, using SELECT CONNECTION_ID() instead", threadId));

                PacketPayload packet = getSharedSendPacket();
                packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
                packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SELECT CONNECTION_ID()"));
                PacketPayload resultPacket = sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);
                Resultset rs = this.protocol.readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));

                ValueFactory<Long> lvf = new LongValueFactory();
                Row r;
                if ((r = rs.getRows().next()) != null) {
                    threadId = r.getValue(0, lvf);
                    processHost = findProcessHost(threadId);
                } else {
                    this.log.logError("No rows returned for statement \"SELECT CONNECTION_ID()\", local connection check will most likely be incorrect");
                }
            }

            if (processHost == null) {
                this.log.logWarn(String.format(
                        "Cannot find process listing for connection %d in SHOW PROCESSLIST output, unable to determine if locally connected", threadId));
            }
            return processHost;
        } catch (IOException e) {
            throw ExceptionFactory.createException(e.getMessage(), e);
        }
    }

    private String findProcessHost(long threadId) {
        try {
            String processHost = null;

            PacketPayload packet = getSharedSendPacket();
            packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
            packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SHOW PROCESSLIST"));
            PacketPayload resultPacket = sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);
            Resultset rs = this.protocol.readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));

            ValueFactory<Long> lvf = new LongValueFactory();
            ValueFactory<String> svf = new StringValueFactory(rs.getColumnDefinition().getFields()[2].getEncoding());
            Row r;
            while ((r = rs.getRows().next()) != null) {
                long id = r.getValue(0, lvf);
                if (threadId == id) {
                    processHost = r.getValue(2, svf);
                    break;
                }
            }

            return processHost;

        } catch (IOException e) {
            throw ExceptionFactory.createException(e.getMessage(), e);
        }
    }

    /**
     * Get the variable value from server.
     * 
     * @param varName
     * @return
     */
    public String queryServerVariable(String varName) {
        try {

            PacketPayload packet = getSharedSendPacket();
            packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_QUERY);
            packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes("SELECT " + varName));
            PacketPayload resultPacket = sendCommand(MysqlaConstants.COM_QUERY, packet, false, 0);
            Resultset rs = this.protocol.readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));

            ValueFactory<String> svf = new StringValueFactory(rs.getColumnDefinition().getFields()[0].getEncoding());
            Row r;
            if ((r = rs.getRows().next()) != null) {
                String s = r.getValue(0, svf);
                if (s != null) {
                    return s;
                }
            }

            return null;

        } catch (IOException e) {
            throw ExceptionFactory.createException(e.getMessage(), e);
        }
    }
}
