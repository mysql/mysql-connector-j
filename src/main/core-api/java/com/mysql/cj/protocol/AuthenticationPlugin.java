/*
 * Copyright (c) 2012, 2022, Oracle and/or its affiliates.
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

import com.mysql.cj.callback.MysqlCallbackHandler;

/**
 * Implementors of this interface can be installed via the "authenticationPlugins" configuration property.
 * 
 * The driver will create one instance of a given plugin per AuthenticationProvider instance if it's reusable (see {@link #isReusable()}) or a new instance
 * in each NativeAuthenticationProvider#proceedHandshakeWithPluggableAuthentication(String, String, String, Buffer) call.
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
     * Initializes this plugin with a direct Protocol reference and a generic {@link MysqlCallbackHandler} that can be used to pass over information back to the
     * authentication provider.
     * For example an authentication plugin may accept <code>null</code> usernames and use that information to obtain them from some external source, such as
     * the system login.
     * 
     * @param protocol
     *            the protocol instance
     * @param callbackHandler
     *            a callback handler to provide additional information to the authentication provider
     */
    default void init(Protocol<M> protocol, MysqlCallbackHandler callbackHandler) {
        init(protocol);
    }

    /**
     * Resets the authentication steps sequence.
     */
    default void reset() {
    }

    /**
     * Called by the driver when this extension should release any resources it is holding and cleanup internally before the connection is closed.
     */
    default void destroy() {
    }

    /**
     * Returns the client-side name that the MySQL server uses on the wire for this plugin.
     * 
     * @return plugin name
     */
    String getProtocolPluginName();

    /**
     * Does this plugin require the connection itself to be confidential (i.e. tls/ssl)...Highly recommended to return "true" for plugins that return the
     * credentials in the clear.
     * 
     * @return true if secure connection is required
     */
    boolean requiresConfidentiality();

    /**
     * @return true if plugin instance may be reused, false otherwise
     */
    boolean isReusable();

    /**
     * This method called from Connector/J before first nextAuthenticationStep call. Values of user and password parameters are passed from those in
     * NativeAuthenticationProvider#changeUser() or NativeAuthenticationProvider#connect().
     * 
     * Plugin should use these values instead of values from connection properties because parent method may be a changeUser call which saves user and password
     * into connection only after successful handshake.
     * 
     * @param user
     *            user name
     * @param password
     *            user password
     */
    void setAuthenticationParameters(String user, String password);

    /**
     * Connector/J uses this method to identify the source of the authentication data, as an authentication plugin name, that will be available to the next
     * authentication step(s). The source of the authentication data in the first iteration will always be the sever-side default authentication plugin name.
     * In the following iterations this depends on the client-side default authentication plugin or on the successive Protocol::AuthSwitchRequest that may have
     * been received in the meantime.
     * 
     * Authentication plugin implementation can use this information to decide if the data coming from the server is useful to them or not.
     * 
     * @param sourceOfAuthData
     *            the authentication plugin that is source of the authentication data
     */
    default void setSourceOfAuthData(String sourceOfAuthData) {
        // Do nothing by default.
    }

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
