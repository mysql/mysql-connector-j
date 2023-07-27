/*
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol.a;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJPacketTooBigException;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.SSLParamsException;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.FullReadInputStream;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.NetworkResources;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.SocketConnection;
import com.mysql.cj.protocol.SocketFactory;

/**
 * Tests for simple packet reader.
 */
public class SimplePacketReaderTest {

    // the basic operation: make sure header bytes are interpreted properly
    @Test
    public void basicHeaderRead() throws IOException {
        RuntimeProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getProperty(PropertyKey.maxAllowedPacket);
        maxAllowedPacket.setValue(100000);
        // mix up the bits so we know they're interpreted correctly
        SocketConnection connection = new FixedBufferSocketConnection(new byte[] { 3, 2, 1, 42 });
        MessageReader<NativePacketHeader, NativePacketPayload> reader = new SimplePacketReader(connection, maxAllowedPacket);
        assertEquals(-1, reader.getMessageSequence());
        NativePacketHeader hdr = reader.readHeader();
        assertEquals(65536 + 512 + 3, hdr.getMessageSize());
        assertEquals(42, hdr.getMessageSequence());
        assertEquals(42, reader.getMessageSequence());
        reader.resetMessageSequence();
        assertEquals(0, reader.getMessageSequence());
    }

    // test checking of maxAllowedPacket
    @Test
    public void exceedMaxAllowedPacketHeaderRead() throws IOException {
        RuntimeProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getProperty(PropertyKey.maxAllowedPacket);
        maxAllowedPacket.setValue(1024);
        // read header with packet size = maxAllowedPacket => SUCCESS
        MockSocketConnection connection = new FixedBufferSocketConnection(new byte[] { 0, 4, 0, 42 });
        MessageReader<NativePacketHeader, NativePacketPayload> reader = new SimplePacketReader(connection, maxAllowedPacket);
        NativePacketHeader hdr = reader.readHeader();
        assertEquals(1024, hdr.getMessageSize());
        // read header with packet size = maxAllowedPacket + 1 => ERROR
        connection = new FixedBufferSocketConnection(new byte[] { 1, 4, 0, 42 });
        reader = new SimplePacketReader(connection, maxAllowedPacket);
        try {
            reader.readHeader();
            fail("Should throw exception as packet size exceeds maxAllowedPacket");
        } catch (CJPacketTooBigException ex) {
            assertTrue(connection.forceClosed, "Connection should be force closed when maxAllowedPacket is exceeded");
        }
    }

    // we only supply 3 bytes when 4 are needed
    @Test
    public void truncatedPacketHeaderRead() throws IOException {
        RuntimeProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getProperty(PropertyKey.maxAllowedPacket);
        MockSocketConnection connection = new FixedBufferSocketConnection(new byte[] { 3, 2, 1 });
        MessageReader<NativePacketHeader, NativePacketPayload> reader = new SimplePacketReader(connection, maxAllowedPacket);
        try {
            reader.readHeader();
            fail("Should throw an exception when we can't read the full header");
        } catch (EOFException ex) {
            assertTrue(connection.forceClosed, "Connection should be force closed when header read fails");
        }
    }

    // trivial payload test
    @Test
    public void readBasicPayload() throws IOException {
        RuntimeProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getProperty(PropertyKey.maxAllowedPacket);
        SocketConnection connection = new FixedBufferSocketConnection(new byte[] { 3, 2, 1, 6, 5, 4 });
        MessageReader<NativePacketHeader, NativePacketPayload> reader = new SimplePacketReader(connection, maxAllowedPacket);
        NativePacketPayload b = reader.readMessage(Optional.empty(), new NativePacketHeader(new byte[] { 3, 0, 0, 0 }));
        assertEquals(3, b.getByteBuffer()[0]);
        assertEquals(2, b.getByteBuffer()[1]);
        assertEquals(1, b.getByteBuffer()[2]);

        // make sure the first only consumed the requested 3 bytes
        b = reader.readMessage(Optional.empty(), new NativePacketHeader(new byte[] { 3, 0, 0, 0 }));
        assertEquals(6, b.getByteBuffer()[0]);
        assertEquals(5, b.getByteBuffer()[1]);
        assertEquals(4, b.getByteBuffer()[2]);
    }

    // test error handling when reading payload
    @Test
    public void readPayloadErrors() throws IOException {
        RuntimeProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getProperty(PropertyKey.maxAllowedPacket);
        MockSocketConnection connection = new FixedBufferSocketConnection(new byte[] { 5, 4 });
        MessageReader<NativePacketHeader, NativePacketPayload> reader = new SimplePacketReader(connection, maxAllowedPacket);

        // can't read 3 bytes if the buffer only has 2
        try {
            reader.readMessage(Optional.empty(), new NativePacketHeader(new byte[] { 3, 0, 0, 0 }));
            fail("Shouldn't be able to read more than 2 bytes");
        } catch (EOFException ex) {
            assertTrue(connection.forceClosed, "Connection should be force closed when payload read fails");
        }

        // any IO errors during read should hang up the connection
        connection = new MockSocketConnection() {

            @Override
            public int readFully(byte[] b, int off, int len) throws IOException {
                throw new IOException("arbitrary failure");
            }

        };
        reader = new SimplePacketReader(connection, maxAllowedPacket);

        try {
            reader.readMessage(Optional.empty(), new NativePacketHeader(new byte[] { 3, 0, 0, 0 }));
            fail("IOException should be thrown");
        } catch (IOException ex) {
            assertTrue(connection.forceClosed, "Connection should be force closed when payload read fails");
        }
    }

    // generate some random packets for the reader
    @Test
    public void heuristicTestWithRandomPackets() throws IOException {
        int numPackets = 127; // can't exceed 127 without changing code
        int maxPacketSize = 127;

        // >>>>>>>> generate random test packets <<<<<<<<

        // the sizes are random. the sequence is the packet # (in the array). payload is repeated packet #
        Random rand = new Random();
        int packetLengths[] = new int[numPackets];
        int totalBufferSize = 0;
        for (int i = 0; i < numPackets; ++i) {
            packetLengths[i] = rand.nextInt(maxPacketSize);
            totalBufferSize += packetLengths[i] + NativeConstants.HEADER_LENGTH;
        }
        ByteBuffer buffer = ByteBuffer.allocate(totalBufferSize);
        for (int i = 0; i < numPackets; ++i) {
            // i = packet number (in array of random test packets)
            // header
            buffer.put((byte) packetLengths[i]);
            buffer.put((byte) 0);
            buffer.put((byte) 0);
            buffer.put((byte) i);
            // payload
            for (int j = 0; j < packetLengths[i]; ++j) {
                buffer.put((byte) i);
            }
        }
        buffer.clear();

        // >>>>>>>> read the packets <<<<<<<<

        RuntimeProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getProperty(PropertyKey.maxAllowedPacket);
        MockSocketConnection connection = new FixedBufferSocketConnection(buffer.array());
        MessageReader<NativePacketHeader, NativePacketPayload> reader = new SimplePacketReader(connection, maxAllowedPacket);
        NativePacketPayload readBuffer = new NativePacketPayload(new byte[maxPacketSize]);
        for (int i = 0; i < numPackets; ++i) {
            NativePacketHeader hdr = reader.readHeader();
            // check length against generated lengths
            assertEquals(packetLengths[i], hdr.getMessageSize());
            // each packet sequence is the packet # in the array
            assertEquals(i, hdr.getMessageSequence());
            assertEquals(i, reader.getMessageSequence());
            reader.readMessage(Optional.of(readBuffer), hdr);
            // check payload bytes also match packet #
            for (int j = 0; j < packetLengths[i]; ++j) {
                assertEquals(i, readBuffer.getByteBuffer()[j]);
            }
        }
    }

    // TODO any boundary conditions or large packet issues?

    public static class FixedBufferSocketConnection extends MockSocketConnection {

        FullReadInputStream is;

        public FixedBufferSocketConnection(byte[] buffer) {
            this.is = new FullReadInputStream(new ByteArrayInputStream(buffer));
        }

        @Override
        public FullReadInputStream getMysqlInput() {
            return this.is;
        }

    }

    public static class MockSocketConnection implements SocketConnection {

        public boolean forceClosed = false;

        @Override
        public void connect(String host, int port, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor, Log log, int loginTimeout) {
        }

        @Override
        public void performTlsHandshake(ServerSession serverSession) throws SSLParamsException, FeatureNotAvailableException, IOException {
        }

        @Override
        public void forceClose() {
            this.forceClosed = true;
        }

        @Override
        public NetworkResources getNetworkResources() {
            return null;
        }

        @Override
        public String getHost() {
            return null;
        }

        @Override
        public int getPort() {
            return 0;
        }

        @Override
        public Socket getMysqlSocket() {
            return null;
        }

        @Override
        public FullReadInputStream getMysqlInput() {
            return new FullReadInputStream(new ByteArrayInputStream(new byte[] {})) {

                @Override
                public int readFully(byte[] b, int off, int len) throws IOException {
                    return MockSocketConnection.this.readFully(b, off, len);
                }

            };
        }

        /**
         * Mock method to override getMysqlInput().readFully().
         *
         * @param b
         * @param off
         * @param len
         * @return an integer
         * @throws IOException
         */
        public int readFully(byte[] b, int off, int len) throws IOException {
            return 0;
        }

        @Override
        public void setMysqlInput(FullReadInputStream mysqlInput) {
        }

        @Override
        public BufferedOutputStream getMysqlOutput() {
            return null;
        }

        @Override
        public boolean isSSLEstablished() {
            return false;
        }

        @Override
        public SocketFactory getSocketFactory() {
            return null;
        }

        @Override
        public void setSocketFactory(SocketFactory socketFactory) {
        }

        @Override
        public ExceptionInterceptor getExceptionInterceptor() {
            return null;
        }

        @Override
        public PropertySet getPropertySet() {
            return null;
        }

    }

}
