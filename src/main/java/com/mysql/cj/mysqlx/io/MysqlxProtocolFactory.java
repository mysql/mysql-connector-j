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

package com.mysql.cj.mysqlx.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.io.ExportControlled;
import com.mysql.cj.mysqla.io.MysqlaSocketConnection;

/**
 * Create a connection to X Plugin.
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
     * Create an async I/O connection. This enables use of async methods on {@link MysqlxProtocol}.
     */
    public static MysqlxProtocol getAsyncInstance(String host, int port, PropertySet propertySet) {
        try {
            AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
            //channel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
            channel.setOption(java.net.StandardSocketOptions.SO_SNDBUF, 128 * 1024);
            channel.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 128 * 1024);

            Future<Void> connectPromise = channel.connect(new InetSocketAddress(host, port));
            connectPromise.get();

            AsyncMessageReader messageReader = new AsyncMessageReader(channel);
            messageReader.start();
            AsyncMessageWriter messageWriter = new AsyncMessageWriter(channel);

            MysqlxProtocol protocol = new MysqlxProtocol(messageReader, messageWriter, channel, propertySet);

            // switch to encrypted channel if requested
            if (propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useSSL).getValue()) {
                // TODO also need to support the MY-299 X DevAPI properties, e.g. propertySet.get("mysqlx.ssl_enable")

                // the message reader is async and is always "reading". we need to stop it to use the socket for the TLS handshake
                messageReader.stopAfterNextMessage();

                protocol.setCapability("tls", true);
                SSLContext sslContext = ExportControlled.getSSLContext(propertySet, null);
                SSLEngine sslEngine = sslContext.createSSLEngine();
                sslEngine.setUseClientMode(true);
                // TODO: setEnabledCipherSuites()
                // TODO: setEnabledProtocols()
                // TODO: how to differentiate servers that do and don't support TLSv1.2
                sslEngine.setEnabledProtocols(new String[] { "TLSv1.1", "TLSv1" });
                //sslEngine.setEnabledProtocols(new String[] {"TLSv1.2", "TLSv1.1", "TLSv1"});

                performTlsHandshake(sslEngine, channel);

                // setup encrypted streams
                messageReader.setChannel(new TlsDecryptingByteChannel(channel, sslEngine));
                messageWriter.setChannel(new TlsEncryptingByteChannel(channel, sslEngine));

                // resume message processing
                messageReader.start();
            }
            return protocol;
        } catch (IOException | InterruptedException | ExecutionException | RuntimeException ex) {
            throw new CJCommunicationsException(ex);
        }
    }

    /**
     * Perform the handshaking step of the TLS connection. We use the `sslEngine' along with the `channel' to exchange messages with the server to setup an
     * encrypted channel.
     */
    private static void performTlsHandshake(SSLEngine sslEngine, AsynchronousSocketChannel channel) throws SSLException {
        sslEngine.beginHandshake();

        ByteBuffer clear = ByteBuffer.allocate(16916);//sslEngine.getHandshakeSession().getApplicationBufferSize());
        ByteBuffer cipher = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
        SSLEngineResult res;

        while (handshakeStatus != HandshakeStatus.FINISHED && handshakeStatus != HandshakeStatus.NOT_HANDSHAKING) {
            if (handshakeStatus == HandshakeStatus.NEED_WRAP) {
                cipher.clear();
                res = sslEngine.wrap(clear, cipher);
                if (res.getStatus() != Status.OK) {
                    throw new CJCommunicationsException("Unacceptable SSLEngine result: " + res);
                }
                handshakeStatus = sslEngine.getHandshakeStatus();
                cipher.flip();
                write(channel, cipher);
            } else if (handshakeStatus == HandshakeStatus.NEED_UNWRAP) {
                cipher.clear();
                read(channel, cipher);
                cipher.flip();
                while (handshakeStatus == HandshakeStatus.NEED_UNWRAP) {
                    res = sslEngine.unwrap(cipher, clear);
                    if (res.getStatus() != Status.OK) {
                        throw new CJCommunicationsException("Unacceptable SSLEngine result: " + res);
                    }
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    if (handshakeStatus == HandshakeStatus.NEED_TASK) {
                        sslEngine.getDelegatedTask().run();
                        handshakeStatus = sslEngine.getHandshakeStatus();
                    }
                }
            }
        }
    }

    /**
     * Synchronously send data to the server. (Needed here for TLS handshake)
     */
    private static void write(AsynchronousSocketChannel channel, ByteBuffer data) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        int bytesToWrite = data.limit();
        CompletionHandler<Integer, Void> handler = new CompletionHandler<Integer, Void>() {
            public void completed(Integer bytesWritten, Void nothing) {
                if (bytesWritten < bytesToWrite) {
                    channel.write(data, null, this);
                } else {
                    f.complete(null);
                }
            }

            public void failed(Throwable exc, Void nothing) {
                f.completeExceptionally(exc);
            }
        };
        channel.write(data, null, handler);
        try {
            f.get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new CJCommunicationsException(ex);
        }
    }

    /**
     * Synchronously read data from the server. (Needed here for TLS handshake)
     */
    private static void read(AsynchronousSocketChannel channel, ByteBuffer data) {
        Future<Integer> f = channel.read(data);
        try {
            f.get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new CJCommunicationsException(ex);
        }
    }
}
