/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.x.io;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;

import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.x.io.MessageReader;
import com.mysql.cj.api.x.io.MessageWriter;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.core.io.ExportControlled;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.mysqla.io.MysqlaSocketConnection;

/**
 * Create a connection to X Plugin.
 */
public class XProtocolFactory {
    public static XProtocol getInstance(String host, int port, PropertySet propertySet) {
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

        return new XProtocol(messageReader, messageWriter, socketConnection.getMysqlSocket(), propertySet);
    }

    /**
     * Create an async I/O connection. This enables use of async methods on {@link XProtocol}.
     * 
     * @param host
     *            host name
     * @param port
     *            port number
     * @param propertySet
     *            {@link PropertySet}
     * @return {@link XProtocol}
     */
    public static XProtocol getAsyncInstance(String host, int port, PropertySet propertySet) {
        try {
            AsynchronousSocketChannel channel = AsynchronousSocketChannel.open();
            //channel.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
            channel.setOption(java.net.StandardSocketOptions.SO_SNDBUF, 128 * 1024);
            channel.setOption(java.net.StandardSocketOptions.SO_RCVBUF, 128 * 1024);

            Future<Void> connectPromise = channel.connect(new InetSocketAddress(host, port));
            connectPromise.get();

            AsyncMessageReader messageReader = new AsyncMessageReader(propertySet, channel);
            messageReader.start();
            AsyncMessageWriter messageWriter = new AsyncMessageWriter(channel);

            XProtocol protocol = new XProtocol(messageReader, messageWriter, channel, propertySet);

            SslMode sslMode = propertySet.<SslMode> getEnumReadableProperty(PropertyDefinitions.PNAME_sslMode).getValue();
            boolean verifyServerCert = sslMode == SslMode.VERIFY_CA || sslMode == SslMode.VERIFY_IDENTITY;
            String trustStoreUrl = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_sslTrustStoreUrl).getValue();

            if (!verifyServerCert && !StringUtils.isNullOrEmpty(trustStoreUrl)) {
                StringBuilder msg = new StringBuilder("Incompatible security settings. The property '");
                msg.append(PropertyDefinitions.PNAME_sslTrustStoreUrl).append("' requires '");
                msg.append(PropertyDefinitions.PNAME_sslMode).append("' as '");
                msg.append(PropertyDefinitions.SslMode.VERIFY_CA).append("' or '");
                msg.append(PropertyDefinitions.SslMode.VERIFY_IDENTITY).append("'.");
                throw new CJCommunicationsException(msg.toString());
            }

            if (sslMode == SslMode.DISABLED) {
                return protocol;
            } // Otherwise switch to encrypted channel.

            if (!protocol.hasCapability("tls")) {
                throw new CJCommunicationsException("A secure connection is required but the server is not configured with SSL.");
            }

            // the message reader is async and is always "reading". we need to stop it to use the socket for the TLS handshake
            messageReader.stopAfterNextMessage();

            protocol.setCapability("tls", true);

            String trustStoreType = null;
            String trustStorePassword = null;
            if (verifyServerCert) {
                trustStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_sslTrustStoreType).getValue();
                trustStorePassword = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_sslTrustStorePassword).getValue();

                if (StringUtils.isNullOrEmpty(trustStoreUrl)) {
                    trustStoreUrl = System.getProperty("javax.net.ssl.trustStore");
                    trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
                    trustStoreType = System.getProperty("javax.net.ssl.trustStoreType");
                    if (StringUtils.isNullOrEmpty(trustStoreType)) {
                        trustStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_sslTrustStoreType).getInitialValue();
                    }
                    // check URL
                    if (!StringUtils.isNullOrEmpty(trustStoreUrl)) {
                        try {
                            new URL(trustStoreUrl);
                        } catch (MalformedURLException e) {
                            trustStoreUrl = "file:" + trustStoreUrl;
                        }
                    }
                }

                if (StringUtils.isNullOrEmpty(trustStoreUrl)) {
                    throw new CJCommunicationsException("No truststore provided to verify the Server certificate.");
                }
            }

            // TODO WL#9925 will redefine other SSL connection properties for X Protocol
            String keyStoreUrl = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreUrl).getValue();
            String keyStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType).getValue();
            String keyStorePassword = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStorePassword).getValue();

            if (StringUtils.isNullOrEmpty(keyStoreUrl)) {
                keyStoreUrl = System.getProperty("javax.net.ssl.keyStore");
                keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
                keyStoreType = System.getProperty("javax.net.ssl.keyStoreType");
                if (StringUtils.isNullOrEmpty(keyStoreType)) {
                    keyStoreType = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientCertificateKeyStoreType).getInitialValue();
                }
                // check URL
                if (!StringUtils.isNullOrEmpty(keyStoreUrl)) {
                    try {
                        new URL(keyStoreUrl);
                    } catch (MalformedURLException e) {
                        keyStoreUrl = "file:" + keyStoreUrl;
                    }
                }
            }

            SSLContext sslContext = ExportControlled.getSSLContext(keyStoreUrl, keyStoreType, keyStorePassword, trustStoreUrl, trustStoreType,
                    trustStorePassword, false, verifyServerCert, sslMode == PropertyDefinitions.SslMode.VERIFY_IDENTITY ? host : null, null);
            SSLEngine sslEngine = sslContext.createSSLEngine();
            sslEngine.setUseClientMode(true);

            // check allowed cipher suites
            String enabledSSLCipherSuites = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_enabledSSLCipherSuites).getValue();
            boolean overrideCiphers = enabledSSLCipherSuites != null && enabledSSLCipherSuites.length() > 0;

            if (overrideCiphers) {
                // If "enabledSSLCipherSuites" is set we check that JVM allows provided values,
                List<String> allowedCiphers = new ArrayList<>();
                List<String> availableCiphers = Arrays.asList(sslEngine.getEnabledCipherSuites());
                for (String cipher : enabledSSLCipherSuites.split("\\s*,\\s*")) {
                    if (availableCiphers.contains(cipher)) {
                        allowedCiphers.add(cipher);
                    }
                }

                // if some ciphers were filtered into allowedCiphers 
                sslEngine.setEnabledCipherSuites(allowedCiphers.toArray(new String[] {}));
            }

            // If enabledTLSProtocols configuration option is set, overriding the default TLS version restrictions.
            // This allows enabling TLSv1.2 for self-compiled MySQL versions supporting it, as well as the ability
            // for users to restrict TLS connections to approved protocols (e.g., prohibiting TLSv1) on the client side.
            String enabledTLSProtocols = propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_enabledTLSProtocols).getValue();
            String[] tryProtocols = enabledTLSProtocols != null && enabledTLSProtocols.length() > 0 ? enabledTLSProtocols.split("\\s*,\\s*")
                    : new String[] { "TLSv1.1", "TLSv1" };
            List<String> configuredProtocols = new ArrayList<>(Arrays.asList(tryProtocols));
            List<String> jvmSupportedProtocols = Arrays.asList(sslEngine.getSupportedProtocols());

            List<String> allowedProtocols = new ArrayList<>();
            final String[] TLS_PROTOCOLS = new String[] { "TLSv1.2", "TLSv1.1", "TLSv1" };
            for (String p : TLS_PROTOCOLS) {
                if (jvmSupportedProtocols.contains(p) && configuredProtocols.contains(p)) {
                    allowedProtocols.add(p);
                }
            }
            sslEngine.setEnabledProtocols(allowedProtocols.toArray(new String[0]));

            performTlsHandshake(sslEngine, channel);

            // setup encrypted streams
            messageReader.setChannel(new TlsDecryptingByteChannel(channel, sslEngine));
            messageWriter.setChannel(new TlsEncryptingByteChannel(channel, sslEngine));

            // resume message processing
            messageReader.start();
            return protocol;
        } catch (CJCommunicationsException e) {
            throw e;
        } catch (IOException | InterruptedException | ExecutionException | RuntimeException ex) {
            throw new CJCommunicationsException(ex);
        }
    }

    /**
     * Perform the handshaking step of the TLS connection. We use the `sslEngine' along with the `channel' to exchange messages with the server to setup an
     * encrypted channel.
     * 
     * @param sslEngine
     *            {@link SSLEngine}
     * @param channel
     *            {@link AsynchronousSocketChannel}
     * @throws SSLException
     *             in case of handshake error
     */
    private static void performTlsHandshake(SSLEngine sslEngine, AsynchronousSocketChannel channel) throws SSLException {
        sslEngine.beginHandshake();
        HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();

        // Create byte buffers to use for holding application data
        int packetBufferSize = sslEngine.getSession().getPacketBufferSize();
        ByteBuffer myNetData = ByteBuffer.allocate(packetBufferSize);
        ByteBuffer peerNetData = ByteBuffer.allocate(packetBufferSize);
        int appBufferSize = sslEngine.getSession().getApplicationBufferSize();
        ByteBuffer myAppData = ByteBuffer.allocate(appBufferSize);
        ByteBuffer peerAppData = ByteBuffer.allocate(appBufferSize);

        SSLEngineResult res = null;

        while (handshakeStatus != HandshakeStatus.FINISHED && handshakeStatus != HandshakeStatus.NOT_HANDSHAKING) {
            switch (handshakeStatus) {
                case NEED_WRAP:
                    myNetData.clear();
                    res = sslEngine.wrap(myAppData, myNetData);
                    handshakeStatus = res.getHandshakeStatus();
                    switch (res.getStatus()) {
                        case OK:
                            myNetData.flip();
                            write(channel, myNetData);
                            break;
                        case BUFFER_OVERFLOW:
                        case BUFFER_UNDERFLOW:
                        case CLOSED:
                            throw new CJCommunicationsException("Unacceptable SSLEngine result: " + res);
                    }
                    break;
                case NEED_UNWRAP:
                    peerNetData.flip(); // Process incoming handshaking data
                    res = sslEngine.unwrap(peerNetData, peerAppData);
                    handshakeStatus = res.getHandshakeStatus();
                    switch (res.getStatus()) {
                        case OK:
                            peerNetData.compact();
                            break;
                        case BUFFER_OVERFLOW:
                            // Check if we need to enlarge the peer application data buffer.
                            final int newPeerAppDataSize = sslEngine.getSession().getApplicationBufferSize();
                            if (newPeerAppDataSize > peerAppData.capacity()) {
                                // enlarge the peer application data buffer
                                ByteBuffer newPeerAppData = ByteBuffer.allocate(newPeerAppDataSize);
                                newPeerAppData.put(peerAppData);
                                newPeerAppData.flip();
                                peerAppData = newPeerAppData;
                            } else {
                                peerAppData.compact();
                            }
                            break;
                        case BUFFER_UNDERFLOW:
                            // Check if we need to enlarge the peer network packet buffer
                            final int newPeerNetDataSize = sslEngine.getSession().getPacketBufferSize();
                            if (newPeerNetDataSize > peerNetData.capacity()) {
                                // enlarge the peer network packet buffer
                                ByteBuffer newPeerNetData = ByteBuffer.allocate(newPeerNetDataSize);
                                newPeerNetData.put(peerNetData);
                                newPeerNetData.flip();
                                peerNetData = newPeerNetData;
                            } else {
                                peerNetData.compact();
                            }
                            // obtain more inbound network data and then retry the operation
                            if (read(channel, peerNetData) < 0) {
                                throw new CJCommunicationsException("Server does not provide enough data to proceed with SSL handshake.");
                            }
                            break;
                        case CLOSED:
                            throw new CJCommunicationsException("Unacceptable SSLEngine result: " + res);
                    }
                    break;

                case NEED_TASK:
                    sslEngine.getDelegatedTask().run();
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;
                case FINISHED:
                case NOT_HANDSHAKING:
                    break;
            }
        }
    }

    /**
     * Synchronously send data to the server. (Needed here for TLS handshake)
     * 
     * @param channel
     *            {@link AsynchronousSocketChannel}
     * @param data
     *            {@link ByteBuffer}
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
     * 
     * @param channel
     *            {@link AsynchronousSocketChannel}
     * @param data
     *            {@link ByteBuffer}
     * @return the number of bytes read
     */
    private static Integer read(AsynchronousSocketChannel channel, ByteBuffer data) {
        Future<Integer> f = channel.read(data);
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new CJCommunicationsException(ex);
        }
    }
}
