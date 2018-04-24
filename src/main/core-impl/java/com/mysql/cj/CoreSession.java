/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import com.mysql.cj.log.NullLogger;
import com.mysql.cj.log.ProfilerEventHandler;
import com.mysql.cj.log.ProfilerEventHandlerFactory;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.result.Row;

public abstract class CoreSession implements Session {

    protected PropertySet propertySet;
    protected ExceptionInterceptor exceptionInterceptor;

    /** The logger we're going to use */
    protected transient Log log;

    /** Null logger shared by all connections at startup */
    protected static final Log NULL_LOGGER = new NullLogger(Log.LOGGER_INSTANCE_NAME);

    protected transient Protocol<? extends Message> protocol;
    protected MessageBuilder<? extends Message> messageBuilder;

    /** The point in time when this connection was created */
    protected long connectionCreationTimeMillis = 0;
    protected HostInfo hostInfo = null;

    protected RuntimeProperty<Boolean> gatherPerfMetrics;
    protected RuntimeProperty<String> characterEncoding;
    protected RuntimeProperty<Boolean> useOldUTF8Behavior;
    protected RuntimeProperty<Boolean> disconnectOnExpiredPasswords;
    protected RuntimeProperty<Boolean> cacheServerConfiguration;
    protected RuntimeProperty<Boolean> autoReconnect;
    protected RuntimeProperty<Boolean> autoReconnectForPools;
    protected RuntimeProperty<Boolean> maintainTimeStats;

    /** The max-rows setting for current session */
    protected int sessionMaxRows = -1;

    /** The event sink to use for profiling */
    private ProfilerEventHandler eventSink;

    public CoreSession(HostInfo hostInfo, PropertySet propSet) {
        this.connectionCreationTimeMillis = System.currentTimeMillis();
        this.hostInfo = hostInfo;
        this.propertySet = propSet;

        this.gatherPerfMetrics = getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_gatherPerfMetrics);
        this.characterEncoding = getPropertySet().getStringProperty(PropertyDefinitions.PNAME_characterEncoding);
        this.useOldUTF8Behavior = getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_useOldUTF8Behavior);
        this.disconnectOnExpiredPasswords = getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords);
        this.cacheServerConfiguration = getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_cacheServerConfiguration);
        this.autoReconnect = getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_autoReconnect);
        this.autoReconnectForPools = getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_autoReconnectForPools);
        this.maintainTimeStats = getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_maintainTimeStats);

        this.log = LogFactory.getLogger(getPropertySet().getStringProperty(PropertyDefinitions.PNAME_logger).getStringValue(), Log.LOGGER_INSTANCE_NAME);
        if (getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_profileSQL).getValue()
                || getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_useUsageAdvisor).getValue()) {
            ProfilerEventHandlerFactory.getInstance(this);
        }
    }

    /**
     * Change user as given by parameters. This implementation only supports calling this during the initial handshake.
     * 
     * @param user
     *            user name
     * @param password
     *            password
     * @param database
     *            database name
     */
    public void changeUser(String user, String password, String database) {
        // reset maxRows to default value
        this.sessionMaxRows = -1;

        this.protocol.changeUser(user, password, database);
    }

    @Override
    public PropertySet getPropertySet() {
        return this.propertySet;
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    public void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor) {
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * Returns the log mechanism that should be used to log information from/for this Session.
     * 
     * @return the Log instance to use for logging messages.
     */
    public Log getLog() {
        return this.log;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M extends Message> MessageBuilder<M> getMessageBuilder() {
        return (MessageBuilder<M>) this.messageBuilder;
    }

    public <QR extends QueryResult> QR sendMessage(Message message) {
        this.protocol.send(message, 0);
        return this.protocol.readQueryResult();
    }

    public <QR extends QueryResult> CompletableFuture<QR> asyncSendMessage(Message message) {
        return this.protocol.sendAsync(message);
    }

    public <M extends Message, RES_T, R> RES_T query(M message, Predicate<Row> filterRow, Function<Row, R> mapRow, Collector<R, ?, RES_T> collector) {
        this.protocol.send(message, 0);
        ColumnDefinition metadata = this.protocol.readMetadata();
        Iterator<Row> ris = this.protocol.getRowInputStream(metadata);
        Stream<Row> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(ris, 0), false);
        if (filterRow != null) {
            stream = stream.filter(filterRow);
        }
        RES_T result = stream.map(mapRow).collect(collector);
        this.protocol.readQueryResult();
        return result;
    }

    @Override
    public ServerSession getServerSession() {
        return this.protocol.getServerSession();
    }

    @Override
    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        return this.protocol.versionMeetsMinimum(major, minor, subminor);
    }

    @Override
    public long getThreadId() {
        return this.protocol.getServerSession().getThreadId();
    }

    public void forceClose() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
        // TODO: REPLACE ME WITH close() unless there's different semantics here
    }

    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public ProfilerEventHandler getProfilerEventHandler() {
        return this.eventSink;
    }

    @Override
    public void setProfilerEventHandler(ProfilerEventHandler h) {
        this.eventSink = h;
    }

    @Override
    public boolean isSSLEstablished() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public void addListener(SessionEventListener l) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public void removeListener(SessionEventListener l) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public String getIdentifierQuoteString() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public DataStoreMetadata getDataStoreMetadata() {
        return new DataStoreMetadataImpl(this);
    }
}
