/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.net.SocketAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions.OpenTelemetry;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ConnectionIsClosedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.ExceptionInterceptorChain;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.OperationCancelledException;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.otel.OpenTelemetryHandler;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.NetworkResources;
import com.mysql.cj.protocol.ProtocolEntityFactory;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.Resultset.Type;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.SocketFactory;
import com.mysql.cj.protocol.a.NativeMessageBuilder;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.a.NativeProtocol;
import com.mysql.cj.protocol.a.NativeServerSession;
import com.mysql.cj.protocol.a.NativeSocketConnection;
import com.mysql.cj.protocol.a.ResultsetFactory;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.LongValueFactory;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.StringValueFactory;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.telemetry.NoopTelemetryHandler;
import com.mysql.cj.telemetry.TelemetryAttribute;
import com.mysql.cj.telemetry.TelemetryScope;
import com.mysql.cj.telemetry.TelemetrySpan;
import com.mysql.cj.telemetry.TelemetrySpanName;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

public class NativeSession extends CoreSession implements Serializable {

    private static final long serialVersionUID = 5323638898749073419L;

    private CacheAdapter<String, Map<String, String>> serverConfigCache;

    /** When did the last query finish? */
    private long lastQueryFinishedTime = 0;

    /** The comment (if any) to prepend to all queries sent to the server (to show up in "SHOW PROCESSLIST") */
    private String queryComment = null;

    /** Does this connection need to be tested? */
    private boolean needsPing = false;

    private NativeMessageBuilder commandBuilder = null;

    /** Has this session been closed? */
    private boolean isClosed = true;

    /** Why was this session implicitly closed, if known? (for diagnostics) */
    private Throwable forceClosedReason = null;

    private CopyOnWriteArrayList<WeakReference<SessionEventListener>> listeners = new CopyOnWriteArrayList<>();

    private transient Timer cancelTimer;

    public NativeSession(HostInfo hostInfo, PropertySet propSet) {
        super(hostInfo, propSet);

        // Check Telemetry option.
        RuntimeProperty<OpenTelemetry> openTelemetry = this.propertySet.getEnumProperty(PropertyKey.openTelemetry);
        if (openTelemetry.getValue() == OpenTelemetry.PREFERRED || openTelemetry.getValue() == OpenTelemetry.REQUIRED) {
            if (!OpenTelemetryHandler.isOpenTelemetryApiAvailable()) {
                if (openTelemetry.getValue() == OpenTelemetry.REQUIRED) {
                    throw ExceptionFactory.createException(Messages.getString("Connection.OtelApiNotFound"));
                }
                if (openTelemetry.isExplicitlySet()) {
                    getLog().logInfo(Messages.getString("Connection.OtelApiNotFound"));
                }
                setTelemetryHandler(NoopTelemetryHandler.getInstance());
            } else {
                setTelemetryHandler(new OpenTelemetryHandler());
            }
        } else {
            setTelemetryHandler(NoopTelemetryHandler.getInstance());
        }
    }

    public void connect(HostInfo hi, String user, String password, String database, int loginTimeout, TransactionEventHandler transactionManager)
            throws IOException {
        this.hostInfo = hi;

        // reset max-rows to default value
        setSessionMaxRows(-1);

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

        this.isClosed = false;

        this.commandBuilder = new NativeMessageBuilder(getServerSession().supportsQueryAttributes());
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
        getSessionLock().lock();
        try {
            if (this.cancelTimer != null) {
                this.cancelTimer.cancel();
                this.cancelTimer = null;
            }
        } finally {
            getSessionLock().unlock();
        }
        this.isClosed = true;
        super.quit();
    }

    // TODO: we should examine the call flow here, we shouldn't have to know about the socket connection but this should be addressed in a wider scope.
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
            //this.protocol = null; // TODO actually we shouldn't remove protocol instance because some of its methods can be called after closing the socket
        }
        getSessionLock().lock();
        try {
            if (this.cancelTimer != null) {
                this.cancelTimer.cancel();
                this.cancelTimer = null;
            }
        } finally {
            getSessionLock().unlock();
        }
        this.isClosed = true;
        super.forceClose();
    }

    public void enableMultiQueries() {
        TelemetrySpan span = getTelemetryHandler().startSpan(TelemetrySpanName.SET_OPTION_MULTI_STATEMENTS, "on");
        try (TelemetryScope scope = span.makeCurrent()) {
            span.setAttribute(TelemetryAttribute.DB_NAME, this.hostInfo.getDatabase());
            span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_SET);
            span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_SET + TelemetryAttribute.STATEMENT_SUFFIX);
            span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
            span.setAttribute(TelemetryAttribute.DB_USER, this.hostInfo.getUser());
            span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
            span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

            this.protocol.sendCommand(this.commandBuilder.buildComSetOption(((NativeProtocol) this.protocol).getSharedSendPacket(), 0), false, 0);
            // OK_PACKET returned in previous sendCommand() was not processed so keep original transaction state.
            ((NativeServerSession) getServerSession()).preserveOldTransactionState();
        } catch (Throwable t) {
            span.setError(t);
            throw t;
        } finally {
            span.end();
        }
    }

    public void disableMultiQueries() {
        TelemetrySpan span = getTelemetryHandler().startSpan(TelemetrySpanName.SET_OPTION_MULTI_STATEMENTS, "off");
        try (TelemetryScope scope = span.makeCurrent()) {
            span.setAttribute(TelemetryAttribute.DB_NAME, this.hostInfo.getDatabase());
            span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_SET);
            span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_SET + TelemetryAttribute.STATEMENT_SUFFIX);
            span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
            span.setAttribute(TelemetryAttribute.DB_USER, this.hostInfo.getUser());
            span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
            span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

            this.protocol.sendCommand(this.commandBuilder.buildComSetOption(((NativeProtocol) this.protocol).getSharedSendPacket(), 1), false, 0);
            // OK_PACKET returned in previous sendCommand() was not processed so keep original transaction state.
            ((NativeServerSession) getServerSession()).preserveOldTransactionState();
        } catch (Throwable t) {
            span.setError(t);
            throw t;
        } finally {
            span.end();
        }
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
        TelemetrySpan span = getTelemetryHandler().startSpan(TelemetrySpanName.SHUTDOWN);
        try (TelemetryScope scope = span.makeCurrent()) {
            span.setAttribute(TelemetryAttribute.DB_NAME, this.hostInfo.getDatabase());
            span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_SHUTDOWN);
            span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_SHUTDOWN + TelemetryAttribute.STATEMENT_SUFFIX);
            span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
            span.setAttribute(TelemetryAttribute.DB_USER, this.hostInfo.getUser());
            span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
            span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

            if (versionMeetsMinimum(5, 7, 9)) {
                this.protocol.sendCommand(this.commandBuilder.buildComQuery(getSharedSendPacket(), this, "SHUTDOWN"), false, 0);
            } else {
                this.protocol.sendCommand(this.commandBuilder.buildComShutdown(getSharedSendPacket()), false, 0);
            }
        } catch (Throwable t) {
            span.setError(t);
            throw t;
        } finally {
            span.end();
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
     * Returns the packet used for sending data (used by PreparedStatement) with position set to 0.
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

    public long getSlowQueryThreshold() {
        return ((NativeProtocol) this.protocol).getSlowQueryThreshold();
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

    public InputStream getLocalInfileInputStream() {
        return this.protocol.getLocalInfileInputStream();
    }

    public void setLocalInfileInputStream(InputStream stream) {
        this.protocol.setLocalInfileInputStream(stream);
    }

    private void createConfigCacheIfNeeded(Lock lock) {
        lock.lock();
        try {
            if (this.serverConfigCache != null) {
                return;
            }

            String serverConfigCacheFactory = this.propertySet.getStringProperty(PropertyKey.serverConfigCacheFactory).getStringValue();

            @SuppressWarnings("unchecked")
            CacheAdapterFactory<String, Map<String, String>> cacheFactory = Util.getInstance(CacheAdapterFactory.class, serverConfigCacheFactory, null, null,
                    getExceptionInterceptor());
            this.serverConfigCache = cacheFactory.getInstance(lock, this.hostInfo.getDatabaseUrl(), Integer.MAX_VALUE, Integer.MAX_VALUE);

            ExceptionInterceptor evictOnCommsError = new ExceptionInterceptor() {

                @Override
                public ExceptionInterceptor init(Properties config, Log log1) {
                    return this;
                }

                @Override
                public void destroy() {
                }

                @Override
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
        } finally {
            lock.unlock();
        }
    }

    // TODO what's the purpose of this variable?
    private final static String SERVER_VERSION_STRING_VAR_NAME = "server_version_string";

    /**
     * Loads pertinent server variables so that the driver can configure itself.
     *
     * @param lock
     *            synchronization lock
     * @param version
     *            driver version string
     */
    public void loadServerVariables(Lock lock, String version) {
        if (this.cacheServerConfiguration.getValue()) {
            createConfigCacheIfNeeded(lock);

            Map<String, String> cachedVariableMap = this.serverConfigCache.get(this.hostInfo.getDatabaseUrl());

            if (cachedVariableMap != null) {
                String cachedServerVersion = cachedVariableMap.get(SERVER_VERSION_STRING_VAR_NAME);

                if (cachedServerVersion != null && getServerSession().getServerVersion() != null
                        && cachedServerVersion.equals(getServerSession().getServerVersion().toString())) {
                    Map<String, String> localVariableMap = this.protocol.getServerSession().getServerVariables();
                    Map<String, String> newLocalVariableMap = new HashMap<>();
                    newLocalVariableMap.putAll(cachedVariableMap);
                    newLocalVariableMap.putAll(localVariableMap); // preserving variables already configured on previous session initialization steps
                    this.protocol.getServerSession().setServerVariables(newLocalVariableMap);
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

            String versionComment = this.propertySet.getBooleanProperty(PropertyKey.paranoid).getValue() || version == null ? "" : "/* " + version + " */";

            this.protocol.getServerSession().setServerVariables(new HashMap<>());

            TelemetrySpan span = getTelemetryHandler().startSpan(TelemetrySpanName.LOAD_VARIABLES);
            try (TelemetryScope scope = span.makeCurrent()) {
                span.setAttribute(TelemetryAttribute.DB_SYSTEM, "MySQL");
                span.setAttribute(TelemetryAttribute.DB_NAME, this.hostInfo.getDatabase());
                span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_SELECT);
                span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_SELECT + TelemetryAttribute.STATEMENT_SUFFIX);
                span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
                span.setAttribute(TelemetryAttribute.DB_USER, this.hostInfo.getUser());
                span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
                span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

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
                    if (versionMeetsMinimum(8, 0, 3) || versionMeetsMinimum(5, 7, 20) && !versionMeetsMinimum(8, 0, 0)) {
                        queryBuf.append(", @@transaction_isolation AS transaction_isolation");
                    } else {
                        queryBuf.append(", @@tx_isolation AS transaction_isolation");
                    }
                    queryBuf.append(", @@wait_timeout AS wait_timeout");

                    NativePacketPayload resultPacket = (NativePacketPayload) this.protocol
                            .sendCommand(this.commandBuilder.buildComQuery(null, this, queryBuf.toString()), false, 0);
                    Resultset rs = ((NativeProtocol) this.protocol).readAllResults(-1, false, resultPacket, false, null,
                            new ResultsetFactory(Type.FORWARD_ONLY, null));
                    Field[] f = rs.getColumnDefinition().getFields();
                    if (f.length > 0) {
                        ValueFactory<String> vf = new StringValueFactory(this.propertySet);
                        Row r;
                        if ((r = rs.getRows().next()) != null) {
                            for (int i = 0; i < f.length; i++) {
                                String value = r.getValue(i, vf);
                                this.protocol.getServerSession().getServerVariables().put(f[i].getColumnLabel(), value);
                            }
                        }
                    }

                } else {
                    NativePacketPayload resultPacket = (NativePacketPayload) this.protocol
                            .sendCommand(this.commandBuilder.buildComQuery(null, this, versionComment + "SHOW VARIABLES"), false, 0);
                    Resultset rs = ((NativeProtocol) this.protocol).readAllResults(-1, false, resultPacket, false, null,
                            new ResultsetFactory(Type.FORWARD_ONLY, null));
                    ValueFactory<String> vf = new StringValueFactory(this.propertySet);
                    Row r;
                    while ((r = rs.getRows().next()) != null) {
                        this.protocol.getServerSession().getServerVariables().put(r.getValue(0, vf), r.getValue(1, vf));
                    }
                }
            } catch (Throwable t) {
                span.setError(t);
                throw t;
            } finally {
                span.end();
            }

        } catch (IOException e) {
            throw ExceptionFactory.createException(e.getMessage(), e);
        }

        if (this.cacheServerConfiguration.getValue()) {
            this.protocol.getServerSession().getServerVariables().put(SERVER_VERSION_STRING_VAR_NAME, getServerSession().getServerVersion().toString());
            Map<String, String> localVariableMap = new HashMap<>();
            localVariableMap.putAll(this.protocol.getServerSession().getServerVariables());
            this.serverConfigCache.put(this.hostInfo.getDatabaseUrl(), Collections.unmodifiableMap(localVariableMap));
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
                TelemetrySpan span = getTelemetryHandler().startSpan(TelemetrySpanName.SET_VARIABLES);
                try (TelemetryScope scope = span.makeCurrent()) {
                    span.setAttribute(TelemetryAttribute.DB_NAME, this.hostInfo.getDatabase());
                    span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_SET);
                    span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_SET + TelemetryAttribute.STATEMENT_SUFFIX);
                    span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
                    span.setAttribute(TelemetryAttribute.DB_USER, this.hostInfo.getUser());
                    span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
                    span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

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
                    this.protocol.sendCommand(this.commandBuilder.buildComQuery(null, this, query.toString()), false, 0);
                } catch (Throwable t) {
                    span.setError(t);
                    throw t;
                } finally {
                    span.end();
                }

            }
        }
    }

    @Override
    public String getQueryComment() {
        String comment = this.queryComment;
        if (getPropertySet().getBooleanProperty(PropertyKey.includeThreadNamesAsStatementComment).getValue()) {
            comment = Stream.of(comment, Messages.getString("NativeSession.ThreadNameComment", new String[] { Thread.currentThread().getName() }))
                    .filter(s -> !StringUtils.isNullOrEmpty(s)).collect(Collectors.joining(", "));
        }
        return comment;
    }

    @Override
    public void setQueryComment(String comment) {
        this.queryComment = comment;
    }

    @Override
    public String getProcessHost() {
        TelemetrySpan span = getTelemetryHandler().startSpan(TelemetrySpanName.GET_PROCESS_HOST);
        try (TelemetryScope scope = span.makeCurrent()) {
            String dbOperation = TelemetryAttribute.OPERATION_SELECT + "/" + TelemetryAttribute.OPERATION_SHOW;
            span.setAttribute(TelemetryAttribute.DB_NAME, this.hostInfo.getDatabase());
            span.setAttribute(TelemetryAttribute.DB_OPERATION, dbOperation);
            span.setAttribute(TelemetryAttribute.DB_STATEMENT, dbOperation + TelemetryAttribute.STATEMENT_SUFFIX);
            span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
            span.setAttribute(TelemetryAttribute.DB_USER, this.hostInfo.getUser());
            span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
            span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

            try {
                long threadId = getThreadId();
                String processHost = findProcessHost(threadId);

                if (processHost == null) {
                    // http://bugs.mysql.com/bug.php?id=44167 - connection ids on the wire wrap at 4 bytes even though they're 64-bit numbers
                    this.log.logWarn(String.format(
                            "Connection id %d not found in \"SHOW PROCESSLIST\", assuming 32-bit overflow, using SELECT CONNECTION_ID() instead", threadId));

                    NativePacketPayload resultPacket = (NativePacketPayload) this.protocol
                            .sendCommand(this.commandBuilder.buildComQuery(null, this, "SELECT CONNECTION_ID()"), false, 0);
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
        } catch (Throwable t) {
            span.setError(t);
            throw t;
        } finally {
            span.end();
        }
    }

    private String findProcessHost(long threadId) {
        try {
            String processHost = null;

            String ps = this.protocol.getServerSession().getServerVariable("performance_schema");

            NativePacketPayload resultPacket = ps != null && ("1".contentEquals(ps) || "ON".contentEquals(ps))
                    ? (NativePacketPayload) this.protocol.sendCommand(this.commandBuilder.buildComQuery(null, this,
                            "SELECT PROCESSLIST_ID, PROCESSLIST_USER, PROCESSLIST_HOST FROM performance_schema.threads WHERE PROCESSLIST_ID=" + threadId),
                            false, 0)
                    : (NativePacketPayload) this.protocol.sendCommand(this.commandBuilder.buildComQuery(null, this, "SHOW PROCESSLIST"), false, 0);

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
        TelemetrySpan span = getTelemetryHandler().startSpan(TelemetrySpanName.GET_VARIABLE, varName);
        try (TelemetryScope scope = span.makeCurrent()) {
            span.setAttribute(TelemetryAttribute.DB_NAME, this.hostInfo.getDatabase());
            span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_SELECT);
            span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_SELECT + TelemetryAttribute.STATEMENT_SUFFIX);
            span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
            span.setAttribute(TelemetryAttribute.DB_USER, this.hostInfo.getUser());
            span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
            span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

            try {
                NativePacketPayload resultPacket = (NativePacketPayload) this.protocol
                        .sendCommand(this.commandBuilder.buildComQuery(null, this, "SELECT " + varName), false, 0);
                Resultset rs = ((NativeProtocol) this.protocol).readAllResults(-1, false, resultPacket, false, null,
                        new ResultsetFactory(Type.FORWARD_ONLY, null));

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
        } catch (Throwable t) {
            span.setError(t);
            throw t;
        } finally {
            span.end();
        }
    }

    /**
     * Send a query to the server. Returns one of the ResultSet objects.
     * To ensure that Statement's queries are serialized, calls to this method should be enclosed in a connection locked block.
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
     * @param cachedMetadata
     *            use this metadata instead of the one provided on wire
     * @param isBatch
     *            is it a batch query
     *
     * @return a ResultSet holding the results
     */
    public <T extends Resultset> T execSQL(Query callingQuery, String query, int maxRows, NativePacketPayload packet, boolean streamResults,
            ProtocolEntityFactory<T, NativePacketPayload> resultSetFactory, ColumnDefinition cachedMetadata, boolean isBatch) {
        long queryStartTime = this.gatherPerfMetrics.getValue() ? System.currentTimeMillis() : 0;
        int endOfQueryPacketPosition = packet != null ? packet.getPosition() : 0;

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
                packet = this.commandBuilder.buildComQuery(null, this, query, callingQuery, this.characterEncoding.getValue());
            }
            return ((NativeProtocol) this.protocol).sendQueryPacket(callingQuery, packet, maxRows, streamResults, cachedMetadata, resultSetFactory);

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

            if (this.autoReconnect.getValue()) {
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
                }
                this.needsPing = true;
            } else if (ex instanceof IOException) {
                invokeCleanupListeners(ex);
            }
            throw ExceptionFactory.createException(ex.getMessage(), ex, this.exceptionInterceptor);

        } finally {
            if (this.maintainTimeStats.getValue()) {
                this.lastQueryFinishedTime = System.currentTimeMillis();
            }

            if (this.gatherPerfMetrics.getValue()) {
                ((NativeProtocol) this.protocol).getMetricsHolder().registerQueryExecutionTime(System.currentTimeMillis() - queryStartTime);
            }
        }
    }

    public long getIdleFor() {
        return this.lastQueryFinishedTime == 0 ? 0 : System.currentTimeMillis() - this.lastQueryFinishedTime;
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

        TelemetrySpan span = getTelemetryHandler().startSpan(TelemetrySpanName.PING);
        try (TelemetryScope scope = span.makeCurrent()) {
            span.setAttribute(TelemetryAttribute.DB_NAME, this.hostInfo.getDatabase());
            span.setAttribute(TelemetryAttribute.DB_OPERATION, TelemetryAttribute.OPERATION_PING);
            span.setAttribute(TelemetryAttribute.DB_STATEMENT, TelemetryAttribute.OPERATION_PING);
            span.setAttribute(TelemetryAttribute.DB_SYSTEM, TelemetryAttribute.DB_SYSTEM_DEFAULT);
            span.setAttribute(TelemetryAttribute.DB_USER, this.hostInfo.getUser());
            span.setAttribute(TelemetryAttribute.THREAD_ID, Thread.currentThread().getId());
            span.setAttribute(TelemetryAttribute.THREAD_NAME, Thread.currentThread().getName());

            long pingMillisLifetime = getPropertySet().getIntegerProperty(PropertyKey.selfDestructOnPingSecondsLifetime).getValue();
            int pingMaxOperations = getPropertySet().getIntegerProperty(PropertyKey.selfDestructOnPingMaxOperations).getValue();

            if (pingMillisLifetime > 0 && System.currentTimeMillis() - this.connectionCreationTimeMillis > pingMillisLifetime
                    || pingMaxOperations > 0 && pingMaxOperations <= getCommandCount()) {

                invokeNormalCloseListeners();

                throw ExceptionFactory.createException(Messages.getString("Connection.exceededConnectionLifetime"),
                        MysqlErrorNumbers.SQLSTATE_MYSQL_COMMUNICATION_LINK_FAILURE, 0, false, null, this.exceptionInterceptor);
            }
            this.protocol.sendCommand(this.commandBuilder.buildComPing(null), false, timeoutMillis); // it isn't safe to use a shared packet here
        } catch (Throwable t) {
            span.setError(t);
            throw t;
        } finally {
            span.end();
        }
    }

    public long getConnectionCreationTimeMillis() {
        return this.connectionCreationTimeMillis;
    }

    public void setConnectionCreationTimeMillis(long connectionCreationTimeMillis) {
        this.connectionCreationTimeMillis = connectionCreationTimeMillis;
    }

    @Override
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

    public Timer getCancelTimer() {
        getSessionLock().lock();
        try {
            if (this.cancelTimer == null) {
                this.cancelTimer = new Timer("MySQL Statement Cancellation Timer", Boolean.TRUE);
            }
            return this.cancelTimer;
        } finally {
            getSessionLock().unlock();
        }
    }

    public void resetSessionState() {
        checkClosed();
        NativePacketPayload message = this.commandBuilder.buildComResetConnection(((NativeProtocol) this.protocol).getSharedSendPacket());
        this.protocol.sendCommand(message, false, 0);
    }

}
