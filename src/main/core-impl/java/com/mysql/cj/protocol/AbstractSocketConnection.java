/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.UnableToConnectException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.Util;
import com.mysql.jdbc.SocketFactoryWrapper;

public abstract class AbstractSocketConnection implements SocketConnection {

    protected String host = null;
    protected int port = 3306;
    protected SocketFactory socketFactory = null;
    protected Socket mysqlSocket = null;
    protected FullReadInputStream mysqlInput = null;
    protected BufferedOutputStream mysqlOutput = null;

    protected ExceptionInterceptor exceptionInterceptor;
    protected PropertySet propertySet;

    @Override
    public String getHost() {
        return this.host;
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public Socket getMysqlSocket() {
        return this.mysqlSocket;
    }

    @Override
    public FullReadInputStream getMysqlInput() throws IOException {
        if (this.mysqlInput != null) {
            return this.mysqlInput;
        }
        throw new IOException(Messages.getString("SocketConnection.1"));
    }

    @Override
    public void setMysqlInput(FullReadInputStream mysqlInput) {
        this.mysqlInput = mysqlInput;
    }

    @Override
    public BufferedOutputStream getMysqlOutput() throws IOException {
        if (this.mysqlOutput != null) {
            return this.mysqlOutput;
        }
        throw new IOException(Messages.getString("SocketConnection.1"));
    }

    @Override
    public boolean isSSLEstablished() {
        return ExportControlled.enabled() && ExportControlled.isSSLEstablished(this.getMysqlSocket());
    }

    @Override
    public SocketFactory getSocketFactory() {
        return this.socketFactory;
    }

    @Override
    public void setSocketFactory(SocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    /**
     * Forcibly closes the underlying socket to MySQL.
     */
    @Override
    public void forceClose() {
        try {
            getNetworkResources().forceClose();
        } finally {
            this.mysqlSocket = null;
            this.mysqlInput = null;
            this.mysqlOutput = null;
        }
    }

    // We do this to break the chain between MysqlIO and Connection, so that we can have PhantomReferences on connections that let the driver clean up the
    // socket connection without having to use finalize() somewhere (which although more straightforward, is horribly inefficient).
    @Override
    public NetworkResources getNetworkResources() {
        return new NetworkResources(this.mysqlSocket, this.mysqlInput, this.mysqlOutput);
    }

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    @Override
    public PropertySet getPropertySet() {
        return this.propertySet;
    }

    @SuppressWarnings("deprecation")
    protected SocketFactory createSocketFactory(String socketFactoryClassName) {
        if (socketFactoryClassName == null) {
            throw ExceptionFactory.createException(UnableToConnectException.class, Messages.getString("SocketConnection.0"), getExceptionInterceptor());
        }

        try {
            return Util.getInstance(SocketFactory.class, socketFactoryClassName, null, null, getExceptionInterceptor());
        } catch (WrongArgumentException e1) {
            if (e1.getCause() == null) {
                // Wrap legacy socket factories.
                try {
                    return new SocketFactoryWrapper(
                            Util.getInstance(com.mysql.jdbc.SocketFactory.class, socketFactoryClassName, null, null, getExceptionInterceptor()));
                } catch (Exception e2) {
                    throw e1;
                }
            }
            throw e1;
        }
    }

}
