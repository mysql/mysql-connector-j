/*
 * Copyright (c) 2015, 2022, Oracle and/or its affiliates.
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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.SSLParamsException;
import com.mysql.cj.log.Log;

/**
 * Represents physical connection with endpoint
 */
public interface SocketConnection {

    /**
     * Connect to the MySQL server and setup a stream connection.
     * 
     * @param host
     *            the hostname to connect to
     * @param port
     *            the port number that the server is listening on
     * @param propertySet
     *            the PropertySet with required connection options
     * @param exceptionInterceptor
     *            exception interceptor
     * @param log
     *            logger
     * @param loginTimeout
     *            the driver login time limit in milliseconds
     */
    void connect(String host, int port, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor, Log log, int loginTimeout);

    void performTlsHandshake(ServerSession serverSession) throws SSLParamsException, FeatureNotAvailableException, IOException;

    /**
     * Start a TLS handshake
     * 
     * @param serverSession
     *            server session state object
     * @param log
     *            logger
     * @throws SSLParamsException
     *             in case of failure
     * @throws FeatureNotAvailableException
     *             in case of failure
     * @throws IOException
     *             in case of failure
     */
    default void performTlsHandshake(ServerSession serverSession, Log log) throws SSLParamsException, FeatureNotAvailableException, IOException {
        performTlsHandshake(serverSession);
    }

    void forceClose();

    NetworkResources getNetworkResources();

    /**
     * Returns the host this IO is connected to
     * 
     * @return host name
     */
    String getHost();

    int getPort();

    Socket getMysqlSocket() throws IOException;

    FullReadInputStream getMysqlInput() throws IOException;

    void setMysqlInput(FullReadInputStream mysqlInput);

    BufferedOutputStream getMysqlOutput() throws IOException;

    boolean isSSLEstablished();

    SocketFactory getSocketFactory();

    void setSocketFactory(SocketFactory socketFactory);

    ExceptionInterceptor getExceptionInterceptor();

    PropertySet getPropertySet();

}
