/*
 * Copyright (c) 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol.x;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.AbstractSocketConnection;
import com.mysql.cj.protocol.FullReadInputStream;
import com.mysql.cj.protocol.NetworkResources;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.SocketFactory;

/**
 * An async I/O connection. This enables use of async methods on {@link XProtocol}.
 *
 */
public class XAsyncSocketConnection extends AbstractSocketConnection implements SocketConnection {

    AsynchronousSocketChannel channel;

    @Override
    public void connect(String hostName, int portNumber, Properties props, PropertySet propSet, ExceptionInterceptor excInterceptor, Log log,
            int loginTimeout) {
        this.port = portNumber;
        this.host = hostName;
        this.propertySet = propSet;

        try {
            this.channel = AsynchronousSocketChannel.open();
            //channel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
            this.channel.setOption(java.net.StandardSocketOptions.SO_SNDBUF, 128 * 1024);
            this.channel.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 128 * 1024);

            Future<Void> connectPromise = this.channel.connect(new InetSocketAddress(this.host, this.port));
            connectPromise.get();

        } catch (CJCommunicationsException e) {
            throw e;
        } catch (IOException | InterruptedException | ExecutionException | RuntimeException ex) {
            throw new CJCommunicationsException(ex);
        }
    }

    public AsynchronousSocketChannel getAsynchronousSocketChannel() {
        return this.channel;
    }

    @Override
    public void setAsynchronousSocketChannel(AsynchronousSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public final void forceClose() {
        try {
            if (this.channel != null && this.channel.isOpen()) {
                this.channel.close();
            }
        } catch (IOException e) {
            // ignore
        } finally {
            this.channel = null;
        }
    }

    @Override
    public NetworkResources getNetworkResources() {
        // TODO not supported ?
        return null;
    }

    @Override
    public Socket getMysqlSocket() {
        // TODO not supported ?
        return null;
    }

    @Override
    public void setMysqlSocket(Socket mysqlSocket) {
        // TODO not supported ?
    }

    @Override
    public FullReadInputStream getMysqlInput() {
        // TODO not supported ?
        return null;
    }

    @Override
    public void setMysqlInput(InputStream mysqlInput) {
        // TODO not supported ?
    }

    @Override
    public BufferedOutputStream getMysqlOutput() {
        // TODO not supported ?
        return null;
    }

    @Override
    public void setMysqlOutput(BufferedOutputStream mysqlOutput) {
        // TODO not supported ?
    }

    @Override
    public boolean isSSLEstablished() {
        // TODO
        return false;
    }

    @Override
    public SocketFactory getSocketFactory() {
        // TODO
        return null;
    }

    @Override
    public void setSocketFactory(SocketFactory socketFactory) {
        // TODO
    }

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        // TODO not supported ?
        return null;
    }

    @Override
    public boolean isSynchronous() {
        return false;
    }
}
