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

package com.mysql.cj.api.io;

import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.Session;
import com.mysql.cj.api.TransactionEventHandler;
import com.mysql.cj.api.authentication.AuthenticationProvider;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;

/**
 * A protocol provides the facilities to communicate with a MySQL server.
 */
public interface Protocol {

    /**
     * Init method takes the place of constructor.
     *
     * A constructor should be used unless the encapsulation of ProtocolFactory is necessary.
     * 
     * @param session
     *            {@link Session}
     * @param socketConnection
     *            {@link SocketConnection}
     * @param propertySet
     *            {@link PropertySet}
     * @param transactionManager
     *            {@link TransactionEventHandler}
     */
    void init(Session session, SocketConnection socketConnection, PropertySet propertySet, TransactionEventHandler transactionManager);

    PropertySet getPropertySet();

    void setPropertySet(PropertySet propertySet);

    /**
     * Retrieve ServerCapabilities from server.
     * 
     * @return {@link ServerCapabilities}
     */
    ServerCapabilities readServerCapabilities();

    ServerSession getServerSession();

    SocketConnection getSocketConnection();

    AuthenticationProvider getAuthenticationProvider();

    ExceptionInterceptor getExceptionInterceptor();

    PacketSentTimeHolder getPacketSentTimeHolder();

    void setPacketSentTimeHolder(PacketSentTimeHolder packetSentTimeHolder);

    PacketReceivedTimeHolder getPacketReceivedTimeHolder();

    void setPacketReceivedTimeHolder(PacketReceivedTimeHolder packetReceivedTimeHolder);

    /**
     * Create a new session. This generally happens once at the beginning of a connection.
     * 
     * @param user
     *            DB user name
     * @param password
     *            DB user password
     * @param database
     *            database name
     */
    void connect(String user, String password, String database);

    void negotiateSSLConnection(int packLength);

    void beforeHandshake();

    void afterHandshake();

    void changeDatabase(String database);

    /**
     * Re-authenticates as the given user and password
     * 
     * @param user
     *            DB user name
     * @param password
     *            DB user password
     * @param database
     *            database name
     * 
     */
    void changeUser(String user, String password, String database);

    String getPasswordCharacterEncoding();

    boolean versionMeetsMinimum(int major, int minor, int subminor);

    @FunctionalInterface
    public static interface GetProfilerEventHandlerInstanceFunction {
        ProfilerEventHandler apply();
    }
}
