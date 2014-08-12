/*
  Copyright (c) 2012, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.List;

/**
 * Implementors of this interface can be installed via the "authenticationPlugins" configuration property.
 * 
 * The driver will create one instance of a given plugin per {@link MysqlIO} instance if it's reusable (see {@link #isReusable()}) or a new instance
 * in each {@link MysqlIO#proceedHandshakeWithPluggableAuthentication(String, String, String, Buffer)} call.
 */
public interface AuthenticationPlugin extends Extension {

    /**
     * Returns the name that the MySQL server uses on
     * the wire for this plugin
     */
    String getProtocolPluginName();

    /**
     * Does this plugin require the connection itself to be confidential
     * (i.e. tls/ssl)...Highly recommended to return "true" for plugins
     * that return the credentials in the clear.
     */
    boolean requiresConfidentiality();

    /**
     * @return true if plugin instance may be reused, false otherwise
     */
    boolean isReusable();

    /**
     * This method called from cJ before first nextAuthenticationStep
     * call. Values of user and password parameters are passed from
     * those in MysqlIO.changeUser(String userName, String password,
     * String database) or MysqlIO.doHandshake(String user, String
     * password, String database).
     * 
     * Plugin should use these values instead of values from connection
     * properties because parent method may be a changeUser call which
     * saves user and password into connection only after successful
     * handshake.
     * 
     * @param user
     * @param password
     */
    void setAuthenticationParameters(String user, String password);

    /**
     * Process authentication handshake data from server and optionally
     * produce data to be sent back to the server. The driver will keep
     * calling this method until either a SQLException is thrown
     * (authentication failure, please use appropriate SQLStates) or the
     * method returns false or driver receives an OK packet from the server
     * which indicates that the connection has been already approved.
     * 
     * If, on return from this method, toServer is a non-empty list of
     * buffers, then these buffers should be sent to the server in order and
     * without any reads in between them. If toServer is an empty list, no
     * data should be sent to server.
     * 
     * If method returns true, it means that this plugin does not need any
     * more data from the server to conclude the handshake and this method
     * should not be called again. (Note that server can send an Auth Method
     * Switch request and then another handshake will start, possibly using a
     * different plugin.)
     * 
     * If this method returns false, it means that plugin needs more data from
     * the server to conclude the handshake. In that case next handshake data
     * payload should be read from the server (after possibly writing data
     * from toServer as explained above). Then this method should be called
     * again with the new data in fromServer parameter.
     * 
     * In case of errors the method should throw SQLException with appropriate
     * SQLStates.
     * 
     * @param fromServer
     *            a buffer containing handshake data payload from
     *            server (can be empty).
     * @param toServer
     *            list of buffers with data to be sent to the server
     *            (the list can be empty, but buffers in the list
     *            should contain data).
     * 
     * @return False if more data should be read from the server and next call
     *         to this method made, true otherwise.
     */
    boolean nextAuthenticationStep(Buffer fromServer, List<Buffer> toServer) throws SQLException;

}
