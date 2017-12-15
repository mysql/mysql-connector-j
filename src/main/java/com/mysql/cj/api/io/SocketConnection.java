/*
 * Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.
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

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.util.Properties;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.io.FullReadInputStream;
import com.mysql.cj.core.io.NetworkResources;

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
     * @param props
     *            the translated Properties from DriverManager.getConnection()
     * @param propertySet
     *            the PropertySet with required connection options
     * @param exceptionInterceptor
     * @param log
     * @param loginTimeout
     *            the driver login time limit in milliseconds
     */
    void connect(String host, int port, Properties props, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor, Log log, int loginTimeout);

    void forceClose();

    NetworkResources getNetworkResources();

    /**
     * Returns the host this IO is connected to
     */
    String getHost();

    int getPort();

    Socket getMysqlSocket();

    void setMysqlSocket(Socket mysqlSocket);

    FullReadInputStream getMysqlInput();

    void setMysqlInput(InputStream mysqlInput);

    BufferedOutputStream getMysqlOutput();

    void setMysqlOutput(BufferedOutputStream mysqlOutput);

    boolean isSSLEstablished();

    SocketFactory getSocketFactory();

    void setSocketFactory(SocketFactory socketFactory);

    ExceptionInterceptor getExceptionInterceptor();

    PropertySet getPropertySet();

}
