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

package com.mysql.cj.api.io;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.ProfilerEventHandler;
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
     * @note A constructor should be used unless the encapsulation of ProtocolFactory is necessary.
     * @note prefer instead
     * 
     *       <pre>
     *       new MysqlaProtocol(conn, to, netConn);
     *       </pre>
     * 
     *       or
     * 
     *       <pre>
     *       MysqlaProtocol.getInstance(conn, to, netConn);
     *       </pre>
     * 
     * @note MysqlConnection dependency will be removed.
     */
    void init(MysqlConnection conn, int socketTimeout, SocketConnection socketConnection, PropertySet propertySet);

    PropertySet getPropertySet();

    void setPropertySet(PropertySet propertySet);

    /**
     * Retrieve ServerCapabilities from server.
     * 
     * @return
     */
    ServerCapabilities readServerCapabilities();

    ServerSession getServerSession();

    MysqlConnection getConnection();

    void setConnection(MysqlConnection connection);

    SocketConnection getSocketConnection();

    AuthenticationProvider getAuthenticationProvider();

    ExceptionInterceptor getExceptionInterceptor();

    PacketSentTimeHolder getPacketSentTimeHolder();

    void setPacketSentTimeHolder(PacketSentTimeHolder packetSentTimeHolder);

    PacketReceivedTimeHolder getPacketReceivedTimeHolder();

    void setPacketReceivedTimeHolder(PacketReceivedTimeHolder packetReceivedTimeHolder);

    /**
     * Create a new session. This generally happens once at the beginning of a connection.
     */
    void connect(String user, String password, String database);

    void negotiateSSLConnection(int packLength);

    void rejectConnection(String message);

    void beforeHandshake();

    void afterHandshake();

    void changeDatabase(String database);

    /**
     * Re-authenticates as the given user and password
     * 
     * @param user
     * @param password
     * @param database
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
