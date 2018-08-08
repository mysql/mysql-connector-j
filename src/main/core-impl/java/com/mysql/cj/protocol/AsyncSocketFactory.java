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

package com.mysql.cj.protocol;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJCommunicationsException;

public class AsyncSocketFactory implements SocketFactory {

    AsynchronousSocketChannel channel;

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Closeable> T connect(String host, int port, PropertySet props, int loginTimeout) throws IOException {
        try {
            this.channel = AsynchronousSocketChannel.open();
            //channel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
            this.channel.setOption(java.net.StandardSocketOptions.SO_SNDBUF, 128 * 1024);
            this.channel.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 128 * 1024);

            Future<Void> connectPromise = this.channel.connect(new InetSocketAddress(host, port));
            connectPromise.get();

        } catch (CJCommunicationsException e) {
            throw e;
        } catch (IOException | InterruptedException | ExecutionException | RuntimeException ex) {
            throw new CJCommunicationsException(ex);
        }
        return (T) this.channel;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Closeable> T performTlsHandshake(SocketConnection socketConnection, ServerSession serverSession) throws IOException {
        this.channel = ExportControlled.startTlsOnAsynchronousChannel(this.channel, socketConnection);
        return (T) this.channel;
    }

}
