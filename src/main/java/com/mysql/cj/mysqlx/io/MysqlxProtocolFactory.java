/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqlx.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.mysqla.io.MysqlaSocketConnection;

/**
 * @todo
 */
public class MysqlxProtocolFactory {
    public static MysqlxProtocol getInstance(String host, int port, PropertySet propertySet) {
        if (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useAsyncProtocol).getValue()) {
            return getAsyncInstance(host, port, propertySet);
        }

        // TODO: we should share SocketConnection unless there comes a time where they need to diverge
        MysqlaSocketConnection socketConnection = new MysqlaSocketConnection();

        // TODO pass props?
        Properties socketFactoryProperties = new Properties();

        socketConnection.connect(host, port, socketFactoryProperties, propertySet, null, null, 0);

        MessageReader messageReader = new SyncMessageReader(socketConnection.getMysqlInput());
        MessageWriter messageWriter = new SyncMessageWriter(socketConnection.getMysqlOutput());

        return new MysqlxProtocol(messageReader, messageWriter, socketConnection.getMysqlSocket(), propertySet);

    }

    /**
     * @todo
     */
    public static MysqlxProtocol getAsyncInstance(String host, int port, PropertySet propertySet) {
        try {
            final AsynchronousSocketChannel sockChan = AsynchronousSocketChannel.open();
            //sockChan.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
            sockChan.setOption(java.net.StandardSocketOptions.SO_SNDBUF, 128*1024);
            sockChan.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 128*1024);

            Future<Void> connectPromise = sockChan.connect(new InetSocketAddress(host, port));
            connectPromise.get();

            AsyncMessageReader messageReader = new AsyncMessageReader(sockChan);
            messageReader.start();
            AsyncMessageWriter messageWriter = new AsyncMessageWriter(sockChan);

            return new MysqlxProtocol(messageReader, messageWriter, sockChan, propertySet);
        } catch (IOException | InterruptedException | ExecutionException ex) {
            throw new CJCommunicationsException(ex);
        }
    }
}
