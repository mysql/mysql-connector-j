/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ConnectionIsClosedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.ExceptionInterceptorChain;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.OperationCancelledException;
import com.mysql.cj.exceptions.PasswordExpiredException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.ProfilerEventHandler;
import com.mysql.cj.log.ProfilerEventHandlerFactory;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.NetworkResources;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Type;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.SocketFactory;
import com.mysql.cj.protocol.a.NativeMessageBuilder;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.a.NativeProtocol;
import com.mysql.cj.protocol.a.NativeServerSession;
import com.mysql.cj.protocol.a.NativeSocketConnection;
import com.mysql.cj.protocol.a.ResultsetFactory;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.IntegerValueFactory;
import com.mysql.cj.result.LongValueFactory;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.util.StringUtils;

public class NativeSession extends CoreSession implements Serializable {

    private static final long serialVersionUID = 5323638898749073419L;

    private CacheAdapter<String, Map<String, String>> serverConfigCache;

    /**
     * Actual collation index to mysql charset name map of user defined charsets for given server URLs.
     */
    private static final Map<String, Map<Integer, String>> customIndexToCharsetMapByUrl = new HashMap<>();

    /**
     * Actual mysql charset name to mblen map of user defined charsets for given server URLs.
     */
    private static final Map<String, Map<String, Integer>> customCharsetToMblenMapByUrl = new HashMap<>();

    /**
     * If a CharsetEncoder is required for escaping. Needed for SJIS and related
     * problems with \u00A5.
     */
    private boolean requiresEscapingEncoder;

    /** When did the last query finish? */
    private long lastQueryFinishedTime = 0;

    /** Does this connection need to be tested? */
    private boolean needsPing = false;

    private NativeMessageBuilder commandBuilder = new NativeMessageBuilder(); // TODO use shared builder

    /** Has this session been closed? */
    private boolean isClosed = true;

    /** Why was this session implicitly closed, if known? (for diagnostics) */
    private Throwable forceClosedReason;

    private CopyOnWriteArrayList<WeakReference<SessionEventListener>> listeners = new CopyOnWriteArrayList<>();

    private transient Timer cancelTimer;

    public NativeSession(HostInfo hostInfo, PropertySet propSet) {
        super(hostInfo, propSet);
    }

    public void connect(HostInfo hi, String user, String password, String database, int loginTimeout, TransactionEventHandler transactionManager)
            throws IOException {

        this.hostInfo = hi;

        // reset max-rows to default value
        this.setSessionMaxRows(-1);

        // TODO do we need different types of physical connections?
        SocketConnection socketConnection = new NativeSocketConnection();
        socketConnection.connect(this.hostInfo.getHost(), this.hostInfo.getPort(), this.propertySet, getExceptionInterceptor(), this.log, loginTimeout);

        // we use physical connection to create a -> protocol
        // this configuration places no knowledge of protocol or session on physical connection.
        // physical connection is responsible *only* for I/O streams
        if (this.protocol == null) {
            this.protocol = NativeProtocol.getInstance(this, socketConnection, this.propertySet, this.log, transactionManager);
        } else {
            this.protocol.init(this, socketConnection, this.propertySet, transactionManager);
        }

        // use protocol to create a -> session
        // protocol is responsible for building a session and authenticating (using AuthenticationProvider) internally
        this.protocol.connect(user, password, database);

        // error messages are returned according to character_set_results which, at this point, is set from the response packet
        this.protocol.getServerSession().setErrorMessageEncoding(this.protocol.getAuthenticationProvider().getEncodingForHandshake());

        this.isClosed = false;
    }

    // TODO: this method should not be used in user-level APIs
    public NativeProtocol getProtocol() {
        return (NativeProtocol) this.protocol;
    }

    @Override
    public void quit() {
        if (this.protocol != null) {
            try {
                ((NativeProtocol) this.protocol).quit();
            } catch (Exception e) {
            }

        }
        synchronized (this) {
            if (this.cancelTimer != null) {
                this.cancelTimer.cancel();
                this.cancelTimer = null;
            }
        }
        this.isClosed = true;
    }

    // TODO: we should examine the call flow here, we shouldn't have to know about the socket connection but this should be address in a wider scope.
    @Override
    public void forceClose() {
        if (this.protocol != null) {
            // checking this.protocol != null isn't enough if connection is used concurrently (the usual situation
            // with application servers which have additional thread management), this.protocol can become null
            // at any moment after this check, causing a race condition and NPEs on next calls;
            // but we may ignore them because at this stage null this.protocol means that we successfully closed all resources by other thread.
            try {
                this.protocol.getSocketConnection().forceClose();
                ((NativeProtocol) this.protocol).releaseResources();
            } catch (Throwable t) {
                // can't do anything about it, and we're forcibly aborting
            }
            //this.protocol = null; // TODO actually we shouldn't remove protocol instance because some it's methods can be called after closing socket
        }
        synchronized (this) {
            if (this.cancelTimer != null) {
                this.cancelTimer.cancel();
                this.cancelTimer = null;
            }
        }
        this.isClosed = true;
    }

    public void enableMultiQueries() {
        sendCommand(this.commandBuilder.buildComSetOption(((NativeProtocol) this.protocol).getSharedSendPacket(), 0), false, 0);
        ((NativeServerSession) getServerSession()).preserveOldTransactionState();
    }

    public void disableMultiQueries() {
        sendCommand(this.commandBuilder.buildComSetOption(((NativeProtocol) this.protocol).getSharedSendPacket(), 1), false, 0);
        ((NativeServerSession) getServerSession()).preserveOldTransactionState();
    }

    @Override
    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        // Server Bug#66884 (SERVER_STATUS is always initiated with SERVER_STATUS_AUTOCOMMIT=1) invalidates "elideSetAutoCommits" feature.
        // TODO Turn this feature back on as soon as the server bug is fixed. Consider making it version specific.
        //return this.protocol.getServerSession().isSetNeededForAutoCommitMode(autoCommitFlag,
        //        getPropertySet().getBooleanReadableProperty(PropertyKey.elideSetAutoCommits).getValue());
        return ((NativeServerSession) this.protocol.getServerSession()).isSetNeededForAutoCommitMode(autoCommitFlag, false);
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
        ((NativeProtocol) this.protocol).setQueryInterceptors(queryInterceptors);
    }

    public boolean isServerLocal(Session sess) {
        SocketFactory factory = this.protocol.getSocketConnection().getSocketFactory();
        return factory.isLocallyConnected(sess);
    }

    /**
     * Used by MiniAdmin to shutdown a MySQL server
     * 
     */
    public void shutdownServer() {
        if (versionMeetsMinimum(5, 7, 9)) {
            sendCommand(this.commandBuilder.buildComQuery(getSharedSendPacket(), "SHUTDOWN"), false, 0);
        } else {
            sendCommand(this.commandBuilder.buildComShutdown(getSharedSendPacket()), false, 0);
        }
    }

    public void setSocketTimeout(int milliseconds) {
        getPropertySet().getProperty(PropertyKey.socketTimeout).setValue(milliseconds); // for re-connects
        ((NativeProtocol) this.protocol).setSocketTimeout(milliseconds);
    }

    public int getSocketTimeout() {
        RuntimeProperty<Integer> sto = getPropertySet().getProperty(PropertyKey.socketTimeout);
        return sto.getValue();
    }

    /**
     * Determines if the database charset is the same as the platform charset
     */
    public void checkForCharsetMismatch() {
        ((NativeProtocol) this.protocol).checkForCharsetMismatch();
    }

    /**
     * Returns the packet used for sending data (used by PreparedStatement) with position set to 0.
     * Guarded by external synchronization on a mutex.
     * 
     * @return A packet to send data with
     */
    public NativePacketPayload getSharedSendPacket() {
        return ((NativeProtocol) this.protocol).getSharedSendPacket();
    }

    public void dumpPacketRingBuffer() {
        ((NativeProtocol) this.protocol).dumpPacketRingBuffer();
    }

    public <T extends Resultset> T invokeQueryInterceptorsPre(Supplier<String> sql, Query interceptedQuery, boolean forceExecute) {
        return ((NativeProtocol) this.protocol).invokeQueryInterceptorsPre(sql, interceptedQuery, forceExecute);
    }

    public <T extends Resultset> T invokeQueryInterceptorsPost(Supplier<String> sql, Query interceptedQuery, T originalResultSet, boolean forceExecute) {
        return ((NativeProtocol) this.protocol).invokeQueryInterceptorsPost(sql, interceptedQuery, originalResultSet, forceExecute);
    }

    public boolean shouldIntercept() {
        return ((NativeProtocol) this.protocol).getQueryInterceptors() != null;
    }

    public long getCurrentTimeNanosOrMillis() {
        return ((NativeProtocol) this.protocol).getCurrentTimeNanosOrMillis();
    }

    public final NativePacketPayload sendCommand(NativePacketPayload queryPacket, boolean skipCheck, int timeoutMillis) {
        return (NativePacketPayload) this.protocol.sendCommand(queryPacket, skipCheck, timeoutMillis);
    }

    public long getSlowQueryThreshold() {
        return ((NativeProtocol) this.protocol).getSlowQueryThreshold();
    }

    public String getQueryTimingUnits() {
        return ((NativeProtocol) this.protocol).getQueryTimingUnits();
    }

    public boolean hadWarnings() {
        return ((NativeProtocol) this.protocol).hadWarnings();
    }

    public void clearInputStream() {
        ((NativeProtocol) this.protocol).clearInputStream();
    }

    public NetworkResources getNetworkResources() {
        return this.protocol.getSocketConnection().getNetworkResources();
    }

    @Override
    public boolean isSSLEstablished() {
        return this.protocol.getSocketConnection().isSSLEstablished();
    }

    public int getCommandCount() {
        return ((NativeProtocol) this.protocol).getCommandCount();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        try {
            return this.protocol.getSocketConnection().getMysqlSocket().getRemoteSocketAddress();
        } catch (IOException e) {
            throw new CJCommunicationsException(e);
        }
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
        ((NativeProtocol) this.protocol).getMetricsHolder().registerQueryExecutionTime(queryTimeMs);
    }

    public void reportNumberOfTablesAccessed(int numTablesAccessed) {
        ((NativeProtocol) this.protocol).getMetricsHolder().reportNumberOfTablesAccessed(numTablesAccessed);
    }

    public void incrementNumberOfPreparedExecutes() {
        if (this.gatherPerfMetrics.getValue()) {
            ((NativeProtocol) this.protocol).getMetricsHolder().incrementNumberOfPreparedExecutes();
        }
    }

    public void incrementNumberOfPrepares() {
        if (this.gatherPerfMetrics.getValue()) {
            ((NativeProtocol) this.protocol).getMetricsHolder().incrementNumberOfPrepares();
        }
    }

    public void incrementNumberOfResultSetsCreated() {
        if (this.gatherPerfMetrics.getValue()) {
            ((NativeProtocol) this.protocol).getMetricsHolder().incrementNumberOfResultSetsCreated();
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
     * @param dontCheckServerMatch
     *            if true then send the SET NAMES query even if server charset already matches the new value
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
        RuntimeProperty<String> characterSetResults = getPropertySet().getProperty(PropertyKey.characterSetResults);
        boolean characterSetAlreadyConfigured = false;

        try {
            characterSetAlreadyConfigured = true;

            configureCharsetProperties();
            realJavaEncoding = this.characterEncoding.getValue(); // we need to do this again to grab this for versions > 4.1.0

            String connectionCollationSuffix = "";
            String connectionCollationCharset = null;

            String connectionCollation = getPropertySet().getStringProperty(PropertyKey.connectionCollation).getStringValue();
            if (connectionCollation != null) {
                for (int i = 1; i < CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME.length; i++) {
                    if (CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[i].equals(connectionCollation)) {
                        connectionCollationSuffix = " COLLATE " + CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[i];
                        connectionCollationCharset = CharsetMapping.COLLATION_INDEX_TO_CHARSET[i].charsetName;
                        realJavaEncoding = CharsetMapping.getJavaEncodingForCollationIndex(i);
                    }
                }
            }

            try {
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
                    String utf8CharsetName = connectionCollationSuffix.length() > 0 ? connectionCollationCharset : "utf8mb4";

                    if (dontCheckServerMatch || !this.protocol.getServerSession().characterSetNamesMatches("utf8")
                            || (!this.protocol.getServerSession().characterSetNamesMatches("utf8mb4")) || (connectionCollationSuffix.length() > 0
                                    && !connectionCollation.equalsIgnoreCase(this.protocol.getServerSession().getServerVariable("collation_server")))) {

                        sendCommand(this.commandBuilder.buildComQuery(null, "SET NAMES " + utf8CharsetName + connectionCollationSuffix), false, 0);

                        this.protocol.getServerSession().getServerVariables().put("character_set_client", utf8CharsetName);
                        this.protocol.getServerSession().getServerVariables().put("character_set_connection", utf8CharsetName);
                    }

                    this.characterEncoding.setValue(realJavaEncoding);
                } /* not utf-8 */else {
                    String mysqlCharsetName = connectionCollationSuffix.length() > 0 ? connectionCollationCharset
                            : CharsetMapping.getMysqlCharsetForJavaEncoding(realJavaEncoding.toUpperCase(Locale.ENGLISH),
                                    getServerSession().getServerVersion());

                    if (mysqlCharsetName != null) {

                        if (dontCheckServerMatch || !this.protocol.getServerSession().characterSetNamesMatches(mysqlCharsetName)) {
                            sendCommand(this.commandBuilder.buildComQuery(null, "SET NAMES " + mysqlCharsetName + connectionCollationSuffix), false, 0);

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
                String mysqlCharsetName = connectionCollationSuffix.length() > 0 ? connectionCollationCharset : getServerSession().getServerDefaultCharset();

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
                        sendCommand(this.commandBuilder.buildComQuery(null, "SET NAMES " + mysqlCharsetName + connectionCollationSuffix), false, 0);

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

            String onServer = this.protocol.getServerSession().getServerVariable("character_set_results");
            if (characterSetResults.getValue() == null) {

                //
                // Only send if needed, if we're caching server variables we -have- to send, because we don't know what it was before we cached them.
                //
                if (onServer != null && onServer.length() > 0 && !"NULL".equalsIgnoreCase(onServer)) {
                    try {
                        sendCommand(this.commandBuilder.buildComQuery(null, "SET character_set_results = NULL"), false, 0);

                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }
                    this.protocol.getServerSession().getServerVariables().put(ServerSession.LOCAL_CHARACTER_SET_RESULTS, null);
                } else {
                    this.protocol.getServerSession().getServerVariables().put(ServerSession.LOCAL_CHARACTER_SET_RESULTS, onServer);
                }
            } else {

                String charsetResults = characterSetResults.getValue();
                String mysqlEncodingName = null;

                if ("UTF-8".equalsIgnoreCase(charsetResults) || "UTF8".equalsIgnoreCase(charsetResults)) {
                    mysqlEncodingName = "utf8";
                } else if ("null".equalsIgnoreCase(charsetResults)) {
                    mysqlEncodingName = "NULL";
                } else {
                    mysqlEncodingName = CharsetMapping.getMysqlCharsetForJavaEncoding(charsetResults.toUpperCase(Locale.ENGLISH),
                            getServerSession().getServerVersion());
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
                        sendCommand(this.commandBuilder.buildComQuery(null, setBuf.toString()), false, 0);

                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }

                    this.protocol.getServerSession().getServerVariables().put(ServerSession.LOCAL_CHARACTER_SET_RESULTS, mysqlEncodingName);

                    // We have to set errorMessageEncoding according to new value of charsetResults for server version 5.5 and higher
                    this.protocol.getServerSession().setErrorMessageEncoding(charsetResults);

                } else {
                    this.protocol.getServerSession().getServerVariables().put(ServerSession.LOCAL_CHARACTER_SET_RESULTS, onServer);
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

    private void createConfigCacheIfNeeded(Object syncMutex) {
        synchronized (syncMutex) {
            if (this.serverConfigCache != null) {
                return;
            }

            try {
                Class<?> factoryClass;

                factoryClass = Class.forName(getPropertySet().getStringProperty(PropertyKey.serverConfigCacheFactory).getStringValue());

                @SuppressWarnings("unchecked")
                CacheAdapterFactory<String, Map<String, String>> cacheFactory = ((CacheAdapterFactory<String, Map<String, String>>) factoryClass.newInstance());

                this.serverConfigCache = cacheFactory.getInstance(syncMutex, this.hostInfo.getDatabaseUrl(), Integer.MAX_VALUE, Integer.MAX_VALUE);

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
                            NativeSession.this.serverConfigCache.invalidate(NativeSession.this.hostInfo.getDatabaseUrl());
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
                        new Object[] { getPropertySet().getStringProperty(PropertyKey.parseInfoCacheFactory).getValue(), PropertyKey.parseInfoCacheFactory }),
                        e, getExceptionInterceptor());
            } catch (InstantiationException | IllegalAccessException | CJException e) {
                throw ExceptionFactory.createException(Messages.getString("Connection.CantLoadCacheFactory",
                        new Object[] { getPropertySet().getStringProperty(PropertyKey.parseInfoCacheFactory).getValue(), PropertyKey.parseInfoCacheFactory }),
                        e, getExceptionInterceptor());
            }
        }
    }

    // TODO what's the purpose of this variable?
    private final static String SERVER_VERSION_STRING_VAR_NAME = "server_version_string";

    /**
     * Loads the result of 'SHOW VARIABLES' into the serverVariables field so
     * that the driver can configure itself.
     * 
     * @param syncMutex
     *            synchronization mutex
     * @param version
     *            driver version string
     */
    public void loadServerVariables(Object syncMutex, String version) {

        if (this.cacheServerConfiguration.getValue()) {
            createConfigCacheIfNeeded(syncMutex);

            Map<String, String> cachedVariableMap = this.serverConfigCache.get(this.hostInfo.getDatabaseUrl());

            if (cachedVariableMap != null) {
                String cachedServerVersion = cachedVariableMap.get(SERVER_VERSION_STRING_VAR_NAME);

                if (cachedServerVersion != null && getServerSession().getServerVersion() != null
                        && cachedServerVersion.equals(getServerSession().getServerVersion().toString())) {
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

            String versionComment = (this.propertySet.getBooleanProperty(PropertyKey.paranoid).getValue() || version == null) ? "" : "/* " + version + " */";

            this.protocol.getServerSession().setServerVariables(new HashMap<String, String>());

            if (versionMeetsMinimum(5, 1, 0)) {
                StringBuilder queryBuf = new StringBuilder(versionComment).append("SELECT");
                queryBuf.append("  @@session.auto_increment_increment AS auto_increment_increment");
                queryBuf.append(", @@character_set_client AS character_set_client");
                queryBuf.append(", @@character_set_connection AS character_set_connection");
                queryBuf.append(", @@character_set_results AS character_set_results");
                queryBuf.append(", @@character_set_server AS character_set_server");
                queryBuf.append(", @@collation_server AS collation_server");
                queryBuf.append(", @@collation_connection AS collation_connection");
                queryBuf.append(", @@init_connect AS init_connect");
                queryBuf.append(", @@interactive_timeout AS interactive_timeout");
                if (!versionMeetsMinimum(5, 5, 0)) {
                    queryBuf.append(", @@language AS language");
                }
                queryBuf.append(", @@license AS license");
                queryBuf.append(", @@lower_case_table_names AS lower_case_table_names");
                queryBuf.append(", @@max_allowed_packet AS max_allowed_packet");
                queryBuf.append(", @@net_write_timeout AS net_write_timeout");
                queryBuf.append(", @@performance_schema AS performance_schema");
                if (!versionMeetsMinimum(8, 0, 3)) {
                    queryBuf.append(", @@query_cache_size AS query_cache_size");
                    queryBuf.append(", @@query_cache_type AS query_cache_type");
                }
                queryBuf.append(", @@sql_mode AS sql_mode");
                queryBuf.append(", @@system_time_zone AS system_time_zone");
                queryBuf.append(", @@time_zone AS time_zone");
                if (versionMeetsMinimum(8, 0, 3) || (versionMeetsMinimum(5, 7, 20) && !versionMeetsMinimum(8, 0, 0))) {
                    queryBuf.append(", @@transaction_isolation AS transaction_isolation");
                } else {
                    queryBuf.append(", @@tx_isolation AS transaction_isolation");
                }
                queryBuf.append(", @@wait_timeout AS wait_timeout");

                NativePacketPayload resultPacket = sendCommand(this.commandBuilder.buildComQuery(null, queryBuf.toString()), false, 0);
                Resultset rs = ((NativeProtocol) this.protocol).readAllResults(-1, false, resultPacket, false, null,
                        new ResultsetFactory(Type.FORWARD_ONLY, null));
                Field[] f = rs.getColumnDefinition().getFields();
                if (f.length > 0) {
                    ValueFactory<String> vf = new StringValueFactory(this.propertySet);
                    Row r;
                    if ((r = rs.getRows().next()) != null) {
                        for (int i = 0; i < f.length; i++) {
                            this.protocol.getServerSession().getServerVariables().put(f[i].getColumnLabel(), r.getValue(i, vf));
                        }
                    }
                }

            } else {
                NativePacketPayload resultPacket = sendCommand(this.commandBuilder.buildComQuery(null, versionComment + "SHOW VARIABLES"), false, 0);
                Resultset rs = ((NativeProtocol) this.protocol).readAllResults(-1, false, resultPacket, false, null,
                        new ResultsetFactory(Type.FORWARD_ONLY, null));
                ValueFactory<String> vf = new StringValueFactory(this.propertySet);
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
            this.protocol.getServerSession().getServerVariables().put(SERVER_VERSION_STRING_VAR_NAME, getServerSession().getServerVersion().toString());
            this.serverConfigCache.put(this.hostInfo.getDatabaseUrl(), this.protocol.getServerSession().getServerVariables());
        }
    }

    public void setSessionVariables() {
        String sessionVariables = getPropertySet().getStringProperty(PropertyKey.sessionVariables).getValue();
        if (sessionVariables != null) {
            List<String> variablesToSet = new ArrayList<>();
            for (String part : StringUtils.split(sessionVariables, ",", "\"'(", "\"')", "\"'", true)) {
                variablesToSet.addAll(StringUtils.split(part, ";", "\"'(", "\"')", "\"'", true));
            }

            if (!variablesToSet.isEmpty()) {
                StringBuilder query = new StringBuilder("SET ");
                String separator = "";
                for (String variableToSet : variablesToSet) {
                    if (variableToSet.length() > 0) {
                        query.append(separator);
                        if (!variableToSet.startsWith("@")) {
                            query.append("SESSION ");
                        }
                        query.append(variableToSet);
                        separator = ",";
                    }
                }
                sendCommand(this.commandBuilder.buildComQuery(null, query.toString()), false, 0);
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

        if (customCharset == null && getPropertySet().getBooleanProperty(PropertyKey.detectCustomCollations).getValue()) {
            customCharset = new HashMap<>();
            customMblen = new HashMap<>();

            ValueFactory<Integer> ivf = new IntegerValueFactory(getPropertySet());

            try {
                NativePacketPayload resultPacket = sendCommand(this.commandBuilder.buildComQuery(null, "SHOW COLLATION"), false, 0);
                Resultset rs = ((NativeProtocol) this.protocol).readAllResults(-1, false, resultPacket, false, null,
                        new ResultsetFactory(Type.FORWARD_ONLY, null));
                ValueFactory<String> svf = new StringValueFactory(this.propertySet);
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
                    NativePacketPayload resultPacket = sendCommand(this.commandBuilder.buildComQuery(null, "SHOW CHARACTER SET"), false, 0);
                    Resultset rs = ((NativeProtocol) this.protocol).readAllResults(-1, false, resultPacket, false, null,
                            new ResultsetFactory(Type.FORWARD_ONLY, null));

                    int charsetColumn = rs.getColumnDefinition().getColumnNameToIndex().get("Charset");
                    int maxlenColumn = rs.getColumnDefinition().getColumnNameToIndex().get("Maxlen");

                    ValueFactory<String> svf = new StringValueFactory(this.propertySet);
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
            ((NativeServerSession) this.protocol.getServerSession()).indexToCustomMysqlCharset = Collections.unmodifiableMap(customCharset);
        }
        if (customMblen != null) {
            ((NativeServerSession) this.protocol.getServerSession()).mysqlCharsetToCustomMblen = Collections.unmodifiableMap(customMblen);
        }

        // Trying to workaround server collations with index > 255. Such index doesn't fit into server greeting packet, 0 is sent instead.
        // Now we could set io.serverCharsetIndex according to "collation_server" value.
        if (this.protocol.getServerSession().getServerDefaultCollationIndex() == 0) {
            String collationServer = this.protocol.getServerSession().getServerVariable("collation_server");
            if (collationServer != null) {
                for (int i = 1; i < CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME.length; i++) {
                    if (CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[i].equals(collationServer)) {
                        this.protocol.getServerSession().setServerDefaultCollationIndex(i);
                        break;
                    }
                }
            } else {
                // We can't do more, just trying to use utf8mb4_general_ci because the most of collations in that range are utf8mb4.
                this.protocol.getServerSession().setServerDefaultCollationIndex(45);
            }
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

                NativePacketPayload resultPacket = sendCommand(this.commandBuilder.buildComQuery(null, "SELECT CONNECTION_ID()"), false, 0);
                Resultset rs = ((NativeProtocol) this.protocol).readAllResults(-1, false, resultPacket, false, null,
                        new ResultsetFactory(Type.FORWARD_ONLY, null));

                ValueFactory<Long> lvf = new LongValueFactory(getPropertySet());
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

            String ps = this.protocol.getServerSession().getServerVariable("performance_schema");

            NativePacketPayload resultPacket = versionMeetsMinimum(5, 6, 0) // performance_schema.threads in MySQL 5.5 does not contain PROCESSLIST_HOST column
                    && ps != null && ("1".contentEquals(ps) || "ON".contentEquals(ps))
                            ? sendCommand(this.commandBuilder.buildComQuery(null,
                                    "select PROCESSLIST_ID, PROCESSLIST_USER, PROCESSLIST_HOST from performance_schema.threads where PROCESSLIST_ID="
                                            + threadId),
                                    false, 0)
                            : sendCommand(this.commandBuilder.buildComQuery(null, "SHOW PROCESSLIST"), false, 0);

            Resultset rs = ((NativeProtocol) this.protocol).readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));

            ValueFactory<Long> lvf = new LongValueFactory(getPropertySet());
            ValueFactory<String> svf = new StringValueFactory(this.propertySet);
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
     *            server variable name
     * @return server variable value
     */
    public String queryServerVariable(String varName) {
        try {

            NativePacketPayload resultPacket = sendCommand(this.commandBuilder.buildComQuery(null, "SELECT " + varName), false, 0);
            Resultset rs = ((NativeProtocol) this.protocol).readAllResults(-1, false, resultPacket, false, null, new ResultsetFactory(Type.FORWARD_ONLY, null));

            ValueFactory<String> svf = new StringValueFactory(this.propertySet);
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

    /**
     * Send a query to the server. Returns one of the ResultSet objects.
     * To ensure that Statement's queries are serialized, calls to this method
     * should be enclosed in a connection mutex synchronized block.
     * 
     * @param <T>
     *            extends {@link Resultset}
     * @param callingQuery
     *            {@link Query} object
     * @param query
     *            the SQL statement to be executed
     * @param maxRows
     *            rows limit
     * @param packet
     *            {@link NativePacketPayload}
     * @param streamResults
     *            whether a stream result should be created
     * @param resultSetFactory
     *            {@link ProtocolEntityFactory}
     * @param catalog
     *            database name
     * @param cachedMetadata
     *            use this metadata instead of the one provided on wire
     * @param isBatch
     *            is it a batch query
     * 
     * @return a ResultSet holding the results
     */
    public <T extends Resultset> T execSQL(Query callingQuery, String query, int maxRows, NativePacketPayload packet, boolean streamResults,
            ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory, String catalog, ColumnDefinition cachedMetadata, boolean isBatch) {

        long queryStartTime = 0;
        int endOfQueryPacketPosition = 0;
        if (packet != null) {
            endOfQueryPacketPosition = packet.getPosition();
        }

        if (this.gatherPerfMetrics.getValue()) {
            queryStartTime = System.currentTimeMillis();
        }

        this.lastQueryFinishedTime = 0; // we're busy!

        if (this.autoReconnect.getValue() && (getServerSession().isAutoCommit() || this.autoReconnectForPools.getValue()) && this.needsPing && !isBatch) {
            try {
                ping(false, 0);
                this.needsPing = false;

            } catch (Exception Ex) {
                invokeReconnectListeners();
            }
        }

        try {
            if (packet == null) {
                String encoding = this.characterEncoding.getValue();
                return ((NativeProtocol) this.protocol).sendQueryString(callingQuery, query, encoding, maxRows, streamResults, catalog, cachedMetadata,
                        this::getProfilerEventHandlerInstanceFunction, resultSetFactory);
            }
            return ((NativeProtocol) this.protocol).sendQueryPacket(callingQuery, packet, maxRows, streamResults, catalog, cachedMetadata,
                    this::getProfilerEventHandlerInstanceFunction, resultSetFactory);

        } catch (CJException sqlE) {
            if (getPropertySet().getBooleanProperty(PropertyKey.dumpQueriesOnException).getValue()) {
                String extractedSql = NativePacketPayload.extractSqlFromPacket(query, packet, endOfQueryPacketPosition,
                        getPropertySet().getIntegerProperty(PropertyKey.maxQuerySizeToLog).getValue());
                StringBuilder messageBuf = new StringBuilder(extractedSql.length() + 32);
                messageBuf.append("\n\nQuery being executed when exception was thrown:\n");
                messageBuf.append(extractedSql);
                messageBuf.append("\n\n");
                sqlE.appendMessage(messageBuf.toString());
            }

            if ((this.autoReconnect.getValue())) {
                if (sqlE instanceof CJCommunicationsException) {
                    // IO may be dirty or damaged beyond repair, force close it.
                    this.protocol.getSocketConnection().forceClose();
                }
                this.needsPing = true;
            } else if (sqlE instanceof CJCommunicationsException) {
                invokeCleanupListeners(sqlE);
            }
            throw sqlE;

        } catch (Throwable ex) {
            if (this.autoReconnect.getValue()) {
                if (ex instanceof IOException) {
                    // IO may be dirty or damaged beyond repair, force close it.
                    this.protocol.getSocketConnection().forceClose();
                } else if (ex instanceof IOException) {
                    invokeCleanupListeners(ex);
                }
                this.needsPing = true;
            }
            throw ExceptionFactory.createException(ex.getMessage(), ex, this.exceptionInterceptor);

        } finally {
            if (this.maintainTimeStats.getValue()) {
                this.lastQueryFinishedTime = System.currentTimeMillis();
            }

            if (this.gatherPerfMetrics.getValue()) {
                long queryTime = System.currentTimeMillis() - queryStartTime;

                registerQueryExecutionTime(queryTime);
            }
        }

    }

    public long getIdleFor() {
        if (this.lastQueryFinishedTime == 0) {
            return 0;
        }

        long now = System.currentTimeMillis();
        long idleTime = now - this.lastQueryFinishedTime;

        return idleTime;
    }

    public boolean isNeedsPing() {
        return this.needsPing;
    }

    public void setNeedsPing(boolean needsPing) {
        this.needsPing = needsPing;
    }

    public void ping(boolean checkForClosedConnection, int timeoutMillis) {
        if (checkForClosedConnection) {
            checkClosed();
        }

        long pingMillisLifetime = getPropertySet().getIntegerProperty(PropertyKey.selfDestructOnPingSecondsLifetime).getValue();
        int pingMaxOperations = getPropertySet().getIntegerProperty(PropertyKey.selfDestructOnPingMaxOperations).getValue();

        if ((pingMillisLifetime > 0 && (System.currentTimeMillis() - this.connectionCreationTimeMillis) > pingMillisLifetime)
                || (pingMaxOperations > 0 && pingMaxOperations <= getCommandCount())) {

            invokeNormalCloseListeners();

            throw ExceptionFactory.createException(Messages.getString("Connection.exceededConnectionLifetime"),
                    MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE, 0, false, null, this.exceptionInterceptor);
        }
        sendCommand(this.commandBuilder.buildComPing(null), false, timeoutMillis); // it isn't safe to use a shared packet here 
    }

    public long getConnectionCreationTimeMillis() {
        return this.connectionCreationTimeMillis;
    }

    public void setConnectionCreationTimeMillis(long connectionCreationTimeMillis) {
        this.connectionCreationTimeMillis = connectionCreationTimeMillis;
    }

    public boolean isClosed() {
        return this.isClosed;
    }

    public void checkClosed() {
        if (this.isClosed) {
            if (this.forceClosedReason != null && this.forceClosedReason.getClass().equals(OperationCancelledException.class)) {
                throw (OperationCancelledException) this.forceClosedReason;
            }
            throw ExceptionFactory.createException(ConnectionIsClosedException.class, Messages.getString("Connection.2"), this.forceClosedReason,
                    getExceptionInterceptor());
        }
    }

    public Throwable getForceClosedReason() {
        return this.forceClosedReason;
    }

    public void setForceClosedReason(Throwable forceClosedReason) {
        this.forceClosedReason = forceClosedReason;
    }

    @Override
    public void addListener(SessionEventListener l) {
        this.listeners.addIfAbsent(new WeakReference<>(l));
    }

    @Override
    public void removeListener(SessionEventListener listener) {
        for (WeakReference<SessionEventListener> wr : this.listeners) {
            SessionEventListener l = wr.get();
            if (l == listener) {
                this.listeners.remove(wr);
                break;
            }
        }
    }

    protected void invokeNormalCloseListeners() {
        for (WeakReference<SessionEventListener> wr : this.listeners) {
            SessionEventListener l = wr.get();
            if (l != null) {
                l.handleNormalClose();
            } else {
                this.listeners.remove(wr);
            }
        }
    }

    protected void invokeReconnectListeners() {
        for (WeakReference<SessionEventListener> wr : this.listeners) {
            SessionEventListener l = wr.get();
            if (l != null) {
                l.handleReconnect();
            } else {
                this.listeners.remove(wr);
            }
        }
    }

    public void invokeCleanupListeners(Throwable whyCleanedUp) {
        for (WeakReference<SessionEventListener> wr : this.listeners) {
            SessionEventListener l = wr.get();
            if (l != null) {
                l.handleCleanup(whyCleanedUp);
            } else {
                this.listeners.remove(wr);
            }
        }
    }

    @Override
    public String getIdentifierQuoteString() {
        return this.protocol != null && this.protocol.getServerSession().useAnsiQuotedIdentifiers() ? "\"" : "`";
    }

    public synchronized Timer getCancelTimer() {
        if (this.cancelTimer == null) {
            this.cancelTimer = new Timer("MySQL Statement Cancellation Timer", Boolean.TRUE);
        }
        return this.cancelTimer;
    }

    @Override
    public <M extends Message, RES_T, R> RES_T query(M message, Predicate<Row> filterRow, Function<Row, R> mapRow, Collector<R, ?, RES_T> collector) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }
}
