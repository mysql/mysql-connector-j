/*
 * Copyright (c) 2012, 2020, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol;

import java.util.List;

/**
 * Implementors of this interface can be installed via the "authenticationPlugins" configuration property.
 * 
 * The driver will create one instance of a given plugin per MysqlIO instance if it's reusable (see {@link #isReusable()}) or a new instance
 * in each MysqlIO#proceedHandshakeWithPluggableAuthentication(String, String, String, Buffer) call.
 * 
 * @param <M>
 *            Message type
 */
public interface AuthenticationPlugin<M extends Message> {

    /**
     * We need direct Protocol reference because it isn't available from Connection before authentication complete.
     * 
     * @param protocol
     *            protocol instance
     */
    default void init(Protocol<M> protocol) {
    }

    /**
     * Resets the authentication steps sequence.
     */
    default void reset() {
    }

    /**
     * Called by the driver when this extension should release any resources
     * it is holding and cleanup internally before the connection is
     * closed.
     */
    default void destroy() {
    }

    /**
     * Returns the name that the MySQL server uses on
     * the wire for this plugin
     * 
     * @return plugin name
     */
    String getProtocolPluginName();

    /**
     * Does this plugin require the connection itself to be confidential
     * (i.e. tls/ssl)...Highly recommended to return "true" for plugins
     * that return the credentials in the clear.
     * 
     * @return true if secure connection is required
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
     *            user name
     * @param password
     *            user password
     */
    void setAuthenticationParameters(String user, String password);

    /**
     * Process authentication handshake data from server and optionally produce data to be sent back to the server.
     * The driver will keep calling this method on each new server packet arrival until either an Exception is thrown
     * (authentication failure, please use appropriate SQLStates) or the number of exchange iterations exceeded max
     * limit or an OK packet is sent by server indicating that the connection has been approved.
     * 
     * If, on return from this method, toServer is a non-empty list of buffers, then these buffers will be sent to
     * the server in the same order and without any reads in between them. If toServer is an empty list, no
     * data will be sent to server, driver immediately reads the next packet from server.
     * 
     * In case of errors the method should throw Exception.
     * 
     * @param fromServer
     *            a buffer containing handshake data payload from
     *            server (can be empty).
     * @param toServer
     *            list of buffers with data to be sent to the server
     *            (the list can be empty, but buffers in the list
     *            should contain data).
     * 
     * @return return value is ignored.
     */
    boolean nextAuthenticationStep(M fromServer, List<M> toServer);
}