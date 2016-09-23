/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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
     * @param password
     * @param database
     * 
     */
    void changeUser(String userName, String password, String database);

    ExceptionInterceptor getExceptionInterceptor();

    void setExceptionInterceptor(ExceptionInterceptor exceptionInterceptor);

    boolean characterSetNamesMatches(String mysqlEncodingName); // TODO it's a temporary method, should be removed after resolving direct usages of ServerSession from Connection

    boolean inTransactionOnServer();

    /**
     * Shortcut to {@link ServerSession#getServerVariable(String)}
     * 
     * @param name
     * @return
     */
    String getServerVariable(String name); // TODO it's a temporary method, should be removed after resolving direct usages of ServerSession from Connection

    int getServerVariable(String variableName, int fallbackValue);

    Map<String, String> getServerVariables(); // TODO it's a temporary method, should be removed after resolving direct usages of ServerSession from Connection

    void setServerVariables(Map<String, String> serverVariables); // TODO it's a temporary method, should be removed after resolving direct usages of ServerSession from Connection

    /**
     * 
     * @return Collation index which server provided in handshake greeting packet
     */
    int getServerDefaultCollationIndex();

    /**
     * Stores collation index which server provided in handshake greeting packet.
     * 
     * @param serverDefaultCollationIndex
     */
    void setServerDefaultCollationIndex(int serverDefaultCollationIndex);

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
     */
    ServerVersion getServerVersion();

    /**
     * Does the version of the MySQL server we are connected to meet the given
     * minimums?
     * 
     * @param major
     * @param minor
     * @param subminor
     */
    boolean versionMeetsMinimum(int major, int minor, int subminor);

    long getThreadId();

    boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag);

    Log getLog();

    void setLog(Log log);

    void configureTimezone();

    /**
     * The default time zone used to marshall date/time values to/from the server. This is used when getDate(), etc methods are called without a calendar
     * argument.
     *
     * @return The server time zone (which may be user overridden in a connection property)
     */
    TimeZone getDefaultTimeZone();

    String getErrorMessageEncoding();

    void setErrorMessageEncoding(String errorMessageEncoding);

    int getMaxBytesPerChar(String javaCharsetName);

    int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName);

    /**
     * Returns the Java character encoding name for the given MySQL server
     * charset index
     * 
     * @param charsetIndex
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

}
