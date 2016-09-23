/*
  Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.mysqla.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;

import com.mysql.cj.api.conf.ModifiableProperty;
import com.mysql.cj.api.conf.PropertySet;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.SocketConnection;
import com.mysql.cj.api.io.SocketFactory;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.io.PacketHeader;
import com.mysql.cj.api.mysqla.io.PacketReader;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJPacketTooBigException;
import com.mysql.cj.core.io.FullReadInputStream;
import com.mysql.cj.core.io.NetworkResources;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.mysqla.MysqlaConstants;

/**
 * Tests for simple packet reader.
 */
public class SimplePacketReaderTest {

    // the basic operation: make sure header bytes are interpreted properly
    @Test
    public void basicHeaderRead() throws IOException {
        ModifiableProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getModifiableProperty(PropertyDefinitions.PNAME_maxAllowedPacket);
        maxAllowedPacket.setValue(100000);
        // mix up the bits so we know they're interpreted correctly
        SocketConnection connection = new FixedBufferSocketConnection(new byte[] { 3, 2, 1, 42 });
        PacketReader reader = new SimplePacketReader(connection, maxAllowedPacket);
        assertEquals(-1, reader.getPacketSequence());
        PacketHeader hdr = reader.readHeader();
        assertEquals(65536 + 512 + 3, hdr.getPacketLength());
        assertEquals(42, hdr.getPacketSequence());
        assertEquals(42, reader.getPacketSequence());
        reader.resetPacketSequence();
        assertEquals(0, reader.getPacketSequence());
    }

    // test checking of maxAllowedPacket
    @Test
    public void exceedMaxAllowedPacketHeaderRead() throws IOException {
        ModifiableProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getModifiableProperty(PropertyDefinitions.PNAME_maxAllowedPacket);
        maxAllowedPacket.setValue(1024);
        // read header with packet size = maxAllowedPacket => SUCCESS
        MockSocketConnection connection = new FixedBufferSocketConnection(new byte[] { 0, 4, 0, 42 });
        PacketReader reader = new SimplePacketReader(connection, maxAllowedPacket);
        PacketHeader hdr = reader.readHeader();
        assertEquals(1024, hdr.getPacketLength());
        // read header with packet size = maxAllowedPacket + 1 => ERROR
        connection = new FixedBufferSocketConnection(new byte[] { 1, 4, 0, 42 });
        reader = new SimplePacketReader(connection, maxAllowedPacket);
        try {
            reader.readHeader();
            fail("Should throw exception as packet size exceeds maxAllowedPacket");
        } catch (CJPacketTooBigException ex) {
            assertTrue("Connection should be force closed when maxAllowedPacket is exceeded", connection.forceClosed);
        }
    }

    // we only supply 3 bytes when 4 are needed
    @Test
    public void truncatedPacketHeaderRead() throws IOException {
        ModifiableProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getModifiableProperty(PropertyDefinitions.PNAME_maxAllowedPacket);
        MockSocketConnection connection = new FixedBufferSocketConnection(new byte[] { 3, 2, 1 });
        PacketReader reader = new SimplePacketReader(connection, maxAllowedPacket);
        try {
            reader.readHeader();
            fail("Should throw an exception when we can't read the full header");
        } catch (EOFException ex) {
            assertTrue("Connection should be force closed when header read fails", connection.forceClosed);
        }
    }

    // trivial payload test
    @Test
    public void readBasicPayload() throws IOException {
        ModifiableProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getModifiableProperty(PropertyDefinitions.PNAME_maxAllowedPacket);
        SocketConnection connection = new FixedBufferSocketConnection(new byte[] { 3, 2, 1, 6, 5, 4 });
        PacketReader reader = new SimplePacketReader(connection, maxAllowedPacket);
        PacketPayload b = reader.readPayload(Optional.empty(), 3);
        assertEquals(3, b.getByteBuffer()[0]);
        assertEquals(2, b.getByteBuffer()[1]);
        assertEquals(1, b.getByteBuffer()[2]);

        // make sure the first only consumed the requested 3 bytes
        b = reader.readPayload(Optional.empty(), 3);
        assertEquals(6, b.getByteBuffer()[0]);
        assertEquals(5, b.getByteBuffer()[1]);
        assertEquals(4, b.getByteBuffer()[2]);
    }

    // test error handling when reading payload
    @Test
    public void readPayloadErrors() throws IOException {
        ModifiableProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getModifiableProperty(PropertyDefinitions.PNAME_maxAllowedPacket);
        MockSocketConnection connection = new FixedBufferSocketConnection(new byte[] { 5, 4 });
        PacketReader reader = new SimplePacketReader(connection, maxAllowedPacket);

        // can't read 3 bytes if the buffer only has 2
        try {
            reader.readPayload(Optional.empty(), 3);
            fail("Shouldn't be able to read more than 2 bytes");
        } catch (EOFException ex) {
            assertTrue("Connection should be force closed when payload read fails", connection.forceClosed);
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
            reader.readPayload(Optional.empty(), 3);
            fail("IOException should be thrown");
        } catch (IOException ex) {
            assertTrue("Connection should be force closed when payload read fails", connection.forceClosed);
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
            totalBufferSize += packetLengths[i] + MysqlaConstants.HEADER_LENGTH;
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

        ModifiableProperty<Integer> maxAllowedPacket = new JdbcPropertySetImpl().getModifiableProperty(PropertyDefinitions.PNAME_maxAllowedPacket);
        MockSocketConnection connection = new FixedBufferSocketConnection(buffer.array());
        PacketReader reader = new SimplePacketReader(connection, maxAllowedPacket);
        Buffer readBuffer = new Buffer(new byte[maxPacketSize]);
        for (int i = 0; i < numPackets; ++i) {
            PacketHeader hdr = reader.readHeader();
            // check length against generated lengths
            assertEquals(packetLengths[i], hdr.getPacketLength());
            // each packet sequence is the packet # in the array
            assertEquals(i, hdr.getPacketSequence());
            assertEquals(i, reader.getPacketSequence());
            reader.readPayload(Optional.of(readBuffer), packetLengths[i]);
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

        public void connect(String host, int port, Properties props, PropertySet propertySet, ExceptionInterceptor exceptionInterceptor, Log log,
                int loginTimeout) {
        }

        public void forceClose() {
            this.forceClosed = true;
        }

        public NetworkResources getNetworkResources() {
            return null;
        }

        public String getHost() {
            return null;
        }

        public int getPort() {
            return 0;
        }

        public Socket getMysqlSocket() {
            return null;
        }

        public void setMysqlSocket(Socket mysqlSocket) {
        }

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
         * @return
         * @throws IOException
         */
        public int readFully(byte[] b, int off, int len) throws IOException {
            return 0;
        }

        public void setMysqlInput(InputStream mysqlInput) {
        }

        public BufferedOutputStream getMysqlOutput() {
            return null;
        }

        public void setMysqlOutput(BufferedOutputStream mysqlOutput) {
        }

        public boolean isSSLEstablished() {
            return false;
        }

        public SocketFactory getSocketFactory() {
            return null;
        }

        public void setSocketFactory(SocketFactory socketFactory) {
        }

        public ExceptionInterceptor getExceptionInterceptor() {
            return null;
        }

        public PropertySet getPropertySet() {
            return null;
        }
    }
}
