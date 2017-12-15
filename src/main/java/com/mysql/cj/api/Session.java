/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.api;

import java.net.SocketAddress;
import java.util.Map;
import java.util.TimeZone;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.Protocol;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.ServerVersion;

/**
 * {@link Session} exposes logical level which user API uses internally to call {@link Protocol} methods.
 * It's a higher-level abstraction than MySQL server session ({@link ServerSession}). {@link Protocol} and {@link ServerSession} methods
 * should never be used directly from user API.
 * 
 */
public interface Session {

    PropertySet getPropertySet();

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

    boolean inTransactionOnServer();

    /**
     * Shortcut to {@link ServerSession#getServerVariable(String)}
     * 
     * @param name
     *            server variable name
     * @return server variable value
     */
    String getServerVariable(String name); // TODO it's a temporary method, should be removed after resolving direct usages of ServerSession from Connection

    int getServerVariable(String variableName, int fallbackValue);

    Map<String, String> getServerVariables(); // TODO it's a temporary method, should be removed after resolving direct usages of ServerSession from Connection

    /**
     * Clobbers the physical network connection and marks this session as closed.
     */
    void abortInternal();

    /**
     * Log-off of the MySQL server and close the socket.
     * 
     */
    void quit();

    void forceClose();

    /**
     * Get the version of the MySQL server we are talking to.
     * 
     * @return {@link ServerVersion}
     */
    ServerVersion getServerVersion();

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

    Log getLog();

    void configureTimezone();

    /**
     * The default time zone used to marshall date/time values to/from the server. This is used when getDate(), etc methods are called without a calendar
     * argument.
     *
     * @return The server time zone (which may be user overridden in a connection property)
     */
    TimeZone getDefaultTimeZone();

    String getErrorMessageEncoding();

    int getMaxBytesPerChar(String javaCharsetName);

    int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName);

    /**
     * Returns the Java character encoding name for the given MySQL server
     * collation index
     * 
     * @param collationIndex
     *            collation index
     * @return the Java character encoding name for the given MySQL server
     *         charset index
     */
    String getEncodingForIndex(int collationIndex);

    ProfilerEventHandler getProfilerEventHandler();

    void setProfilerEventHandler(ProfilerEventHandler h);

    ServerSession getServerSession();

    boolean isSSLEstablished();

    SocketAddress getRemoteSocketAddress();

    boolean serverSupportsFracSecs();

    String getProcessHost();

    /**
     * Add listener for this session status changes.
     * 
     * @param l
     *            {@link SessionEventListener} instance.
     */
    void addListener(SessionEventListener l);

    void removeListener(SessionEventListener l);

    public static interface SessionEventListener {
        void handleNormalClose();

        void handleReconnect();

        void handleCleanup(Throwable whyCleanedUp);
    }

}
