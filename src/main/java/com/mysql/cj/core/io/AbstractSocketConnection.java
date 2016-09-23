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

package com.mysql.cj.core.io;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.net.Socket;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.io.SocketFactory;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.UnableToConnectException;

public abstract class AbstractSocketConnection implements SocketConnection {

    protected String host = null;
    protected int port = 3306;
    protected SocketFactory socketFactory = null;
    protected Socket mysqlSocket = null;
    protected FullReadInputStream mysqlInput = null;
    protected BufferedOutputStream mysqlOutput = null;

    protected ExceptionInterceptor exceptionInterceptor;
    protected PropertySet propertySet;

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public Socket getMysqlSocket() {
        return this.mysqlSocket;
    }

    public void setMysqlSocket(Socket mysqlSocket) {
        this.mysqlSocket = mysqlSocket;
    }

    public FullReadInputStream getMysqlInput() {
        return this.mysqlInput;
    }

    public void setMysqlInput(InputStream mysqlInput) {
        // TODO: note: this is a temporary measure until MYSQLCONNJ-453 fixes the way SSL is supported
        this.mysqlInput = new FullReadInputStream(mysqlInput);
    }

    public BufferedOutputStream getMysqlOutput() {
        return this.mysqlOutput;
    }

    public void setMysqlOutput(BufferedOutputStream mysqlOutput) {
        this.mysqlOutput = mysqlOutput;
    }

    public boolean isSSLEstablished() {
        return ExportControlled.enabled() && ExportControlled.isSSLEstablished(this.getMysqlSocket());
    }

    public SocketFactory getSocketFactory() {
        return this.socketFactory;
    }

    public void setSocketFactory(SocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    /**
     * Forcibly closes the underlying socket to MySQL.
     */
    public final void forceClose() {
        try {
            getNetworkResources().forceClose();
        } finally {
            this.mysqlSocket = null;
            this.mysqlInput = null;
            this.mysqlOutput = null;
        }
    }

    // We do this to break the chain between MysqlIO and Connection, so that we can have PhantomReferences on connections that let the driver clean up the
    // socket connection without having to use finalize() somewhere (which although more straightforward, is horribly inefficent).
    public NetworkResources getNetworkResources() {
        return new NetworkResources(this.mysqlSocket, this.mysqlInput, this.mysqlOutput);
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    public PropertySet getPropertySet() {
        return this.propertySet;
    }

    protected SocketFactory createSocketFactory(String socketFactoryClassName) {
        try {
            if (socketFactoryClassName == null) {
                throw ExceptionFactory.createException(UnableToConnectException.class, Messages.getString("SocketConnection.0"), getExceptionInterceptor());
            }

            return (SocketFactory) (Class.forName(socketFactoryClassName).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | CJException ex) {
            throw ExceptionFactory.createException(UnableToConnectException.class,
                    Messages.getString("SocketConnection.1", new String[] { socketFactoryClassName }), getExceptionInterceptor());
        }
    }

}
