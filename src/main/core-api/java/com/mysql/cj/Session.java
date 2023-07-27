/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.ProfilerEventHandler;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.Protocol;
import com.mysql.cj.protocol.ResultBuilder;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.result.Row;

/**
 * {@link Session} exposes logical level which user API uses internally to call {@link Protocol} methods.
 * It's a higher-level abstraction than MySQL server session ({@link ServerSession}). {@link Protocol} and {@link ServerSession} methods
 * should never be used directly from user API.
 *
 */
public interface Session {

    PropertySet getPropertySet();

    <M extends Message> MessageBuilder<M> getMessageBuilder();

    /**
     * Re-authenticates as the given user and password
     *
     * @param userName
     *            DB user name
     * @param password
     *            DB user password
     * @param database
     *            database name
     *
     */
    void changeUser(String userName, String password, String database);

    ExceptionInterceptor getExceptionInterceptor();

    void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor);

    /**
     * Log-off of the MySQL server and close the socket.
     *
     */
    void quit();

    /**
     * Clobbers the physical network connection and marks this session as closed.
     */
    void forceClose();

    /**
     * Does the version of the MySQL server we are connected to meet the given
     * minimums?
     *
     * @param major
     *            major version number
     * @param minor
     *            minor version number
     * @param subminor
     *            sub-minor version number
     * @return true if current server version equal or higher than provided one
     */
    boolean versionMeetsMinimum(int major, int minor, int subminor);

    long getThreadId();

    boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag);

    /**
     * Returns the log mechanism that should be used to log information from/for this Session.
     *
     * @return the Log instance to use for logging messages.
     */
    Log getLog();

    /**
     * Returns the current ProfilerEventHandler or initializes a new one if none exists.
     *
     * @return the {@link ProfilerEventHandler} object.
     */
    ProfilerEventHandler getProfilerEventHandler();

    HostInfo getHostInfo();

    String getQueryTimingUnits();

    ServerSession getServerSession();

    boolean isSSLEstablished();

    SocketAddress getRemoteSocketAddress();

    String getProcessHost();

    /**
     * Add listener for this session status changes.
     *
     * @param l
     *            {@link SessionEventListener} instance.
     */
    void addListener(SessionEventListener l);

    /**
     * Remove session listener.
     *
     * @param l
     *            {@link SessionEventListener} instance.
     */
    void removeListener(SessionEventListener l);

    public static interface SessionEventListener {

        void handleNormalClose();

        void handleReconnect();

        void handleCleanup(Throwable whyCleanedUp);

    }

    boolean isClosed();

    String getIdentifierQuoteString();

    DataStoreMetadata getDataStoreMetadata();

    /**
     * Synchronously query database with applying rows filtering and mapping.
     *
     * @param message
     *            query message
     * @param rowFilter
     *            row filter function
     * @param rowMapper
     *            row map function
     * @param collector
     *            result collector
     * @param <M>
     *            Message type
     * @param <R>
     *            Row type
     * @param <RES>
     *            Result type
     * @return List of rows
     */
    default <M extends Message, R, RES> RES query(M message, Predicate<Row> rowFilter, Function<Row, R> rowMapper, Collector<R, ?, RES> collector) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Synchronously query database.
     *
     * @param message
     *            query message
     * @param resultBuilder
     *            ResultBuilder instance
     * @param <M>
     *            Message type
     * @param <R>
     *            Result type
     * @return {@link QueryResult} object
     */
    default <M extends Message, R extends QueryResult> R query(M message, ResultBuilder<R> resultBuilder) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    /**
     * Asynchronously query database.
     *
     * @param message
     *            query message
     * @param resultBuilder
     *            ResultBuilder instance
     * @param <M>
     *            Message type
     * @param <R>
     *            Result type
     * @return CompletableFuture providing a {@link QueryResult} object
     */
    default <M extends Message, R extends QueryResult> CompletableFuture<R> queryAsync(M message, ResultBuilder<R> resultBuilder) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

}
