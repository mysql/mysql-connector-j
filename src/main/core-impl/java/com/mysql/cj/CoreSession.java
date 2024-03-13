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

import java.net.SocketAddress;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import com.mysql.cj.log.NullLogger;
import com.mysql.cj.log.ProfilerEventHandler;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.telemetry.TelemetryHandler;
import com.mysql.cj.util.Util;

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
    protected RuntimeProperty<Boolean> disconnectOnExpiredPasswords;
    protected RuntimeProperty<Boolean> cacheServerConfiguration;
    protected RuntimeProperty<Boolean> autoReconnect;
    protected RuntimeProperty<Boolean> autoReconnectForPools;
    protected RuntimeProperty<Boolean> maintainTimeStats;

    /** The max-rows setting for current session */
    protected int sessionMaxRows = -1;

    /** The event sink to use for profiling */
    private ProfilerEventHandler eventSink;

    /** The telemetry handler to process telemetry operations */
    private TelemetryHandler telemetryHandler = null;

    public CoreSession(HostInfo hostInfo, PropertySet propSet) {
        this.connectionCreationTimeMillis = System.currentTimeMillis();
        this.hostInfo = hostInfo;
        this.propertySet = propSet;

        this.gatherPerfMetrics = getPropertySet().getBooleanProperty(PropertyKey.gatherPerfMetrics);
        this.characterEncoding = getPropertySet().getStringProperty(PropertyKey.characterEncoding);
        this.disconnectOnExpiredPasswords = getPropertySet().getBooleanProperty(PropertyKey.disconnectOnExpiredPasswords);
        this.cacheServerConfiguration = getPropertySet().getBooleanProperty(PropertyKey.cacheServerConfiguration);
        this.autoReconnect = getPropertySet().getBooleanProperty(PropertyKey.autoReconnect);
        this.autoReconnectForPools = getPropertySet().getBooleanProperty(PropertyKey.autoReconnectForPools);
        this.maintainTimeStats = getPropertySet().getBooleanProperty(PropertyKey.maintainTimeStats);

        this.log = LogFactory.getLogger(getPropertySet().getStringProperty(PropertyKey.logger).getStringValue(), Log.LOGGER_INSTANCE_NAME);
    }

    @Override
    public void changeUser(String user, String password, String database) {
        // reset maxRows to default value
        this.sessionMaxRows = -1;

        this.protocol.changeUser(user, password, database);
    }

    @Override
    public PropertySet getPropertySet() {
        return this.propertySet;
    }

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    @Override
    public void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor) {
        this.exceptionInterceptor = exceptionInterceptor;
    }

    @Override
    public Log getLog() {
        return this.log;
    }

    @Override
    public HostInfo getHostInfo() {
        return this.hostInfo;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M extends Message> MessageBuilder<M> getMessageBuilder() {
        return (MessageBuilder<M>) this.messageBuilder;
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
        return this.protocol.getServerSession().getCapabilities().getThreadId();
    }

    @Override
    public void quit() {
        if (this.eventSink != null) {
            this.eventSink.destroy();
            this.eventSink = null;
        }
    }

    @Override
    public void forceClose() {
        if (this.eventSink != null) {
            this.eventSink.destroy();
            this.eventSink = null;
        }
    }

    @Override
    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public ProfilerEventHandler getProfilerEventHandler() {
        if (this.eventSink == null) {
            synchronized (this) {
                if (this.eventSink == null) { // check again to ensure that other thread didn't set it already
                    this.eventSink = Util.getInstance(ProfilerEventHandler.class,
                            this.propertySet.getStringProperty(PropertyKey.profilerEventHandler).getStringValue(), null, null, this.exceptionInterceptor);
                    this.eventSink.init(this.log);
                }
            }
        }
        return this.eventSink;
    }

    @Override
    public String getQueryComment() {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public void setQueryComment(String comment) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    @Override
    public void setTelemetryHandler(TelemetryHandler telemetryHandler) {
        this.telemetryHandler = telemetryHandler;
    }

    @Override
    public TelemetryHandler getTelemetryHandler() {
        return this.telemetryHandler;
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

    @Override
    public String getQueryTimingUnits() {
        return this.protocol.getQueryTimingUnits();
    }

}
