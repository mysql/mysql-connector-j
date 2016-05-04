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

package com.mysql.cj.api.mysqla.io;

import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;

/**
 * PacketPayload is the content of a full single packet (independent from
 * on-wire splitting) communicated with the server. We can manipulate the
 * packet's underlying buffer when sending commands with writeInteger(),
 * writeBytes(), etc. We can check the packet type with isEOFPacket(), etc
 * predicates.
 * 
 * A position is maintained for reading/writing data. A payload length is
 * maintained allowing the PacketPayload to be decoupled from the size of
 * the underlying buffer.
 * 
 */
public interface PacketPayload {

    static final int NO_LENGTH_LIMIT = -1;
    static final long NULL_LENGTH = -1;

    /* Type ids of response packets. */
    public static final short TYPE_ID_ERROR = 0xFF;
    public static final short TYPE_ID_EOF = 0xFE;
    /** It has the same signature as EOF, but may be issued by server only during handshake phase **/
    public static final short TYPE_ID_AUTH_SWITCH = 0xFE;
    public static final short TYPE_ID_LOCAL_INFILE = 0xFB;
    public static final short TYPE_ID_OK = 0;

    int getCapacity();

    /**
     * Checks that underlying buffer has enough space to store additionalData bytes starting from current position.
     * If buffer size is smaller than required then it is re-allocated with bigger size.
     * 
     * @param additionalData
     */
    void ensureCapacity(int additionalData);

    /**
     * Returns the array of bytes this Buffer is using to read from.
     * 
     * @return byte array being read from
     */
    byte[] getByteBuffer();

    /**
     * Sets the array of bytes to use as a buffer to read from.
     * 
     * @param byteBuffer
     *            the array of bytes to use as a buffer
     */
    void setByteBuffer(byte[] byteBufferToSet);

    /**
     * Get the actual length of payload the buffer contains.
     * It can be smaller than underlying buffer size because it can be reused after a big packet.
     * 
     * @return
     */
    int getPayloadLength();

    /**
     * Set the actual length of payload written to buffer.
     * It can be smaller or equal to underlying buffer size.
     * 
     * @param bufLengthToSet
     */
    void setPayloadLength(int bufLengthToSet);

    /**
     * Returns the current position to write to/ read from
     * 
     * @return the current position to write to/ read from
     */
    int getPosition();

    /**
     * Set the current position to write to/ read from
     * 
     * @param position
     *            the position (0-based index)
     */
    void setPosition(int positionToSet);

    /**
     * Is it a EOF packet.
     * See http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
     * 
     * @return
     */
    boolean isEOFPacket();

    /**
     * Is it a Protocol::AuthSwitchRequest packet.
     * See http://dev.mysql.com/doc/internals/en/connection-phase-packets.html
     * 
     * @return
     */
    boolean isAuthMethodSwitchRequestPacket();

    /**
     * Is it an OK packet.
     * See http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
     * 
     * @return
     */
    boolean isOKPacket();

    /**
     * Is it an OK packet for ResultSet. Unlike usual 0x00 signature it has 0xfe signature.
     * See http://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
     * 
     * @return
     */
    boolean isResultSetOKPacket();

    /**
     * Is it a Protocol::AuthMoreData packet.
     * See http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthMoreData
     * 
     * @return
     */
    boolean isAuthMoreData();

    /**
     * Write data according to provided Integer type.
     * 
     * @param type
     * @param l
     */
    void writeInteger(IntegerDataType type, long l);

    /**
     * Read data according to provided Integer type.
     * 
     * @param type
     * @return
     */
    long readInteger(IntegerDataType type);

    /**
     * Write all bytes from given byte array into internal buffer starting with current buffer position.
     * 
     * @param type
     *            on-wire data type
     * @param b
     *            from byte array
     */
    void writeBytes(StringLengthDataType type, byte[] b);

    /**
     * Write all bytes from given byte array into internal buffer starting with current buffer position.
     * 
     * @param type
     *            on-wire data type
     * @param b
     *            from byte array
     */
    void writeBytes(StringSelfDataType type, byte[] b);

    /**
     * Write len bytes from given byte array into internal buffer.
     * Read starts from given offset, write starts with current buffer position.
     * 
     * @param type
     *            on-wire data type
     * @param b
     *            from byte array
     * @param offset
     *            starting index of b
     * @param len
     *            number of bytes to be written
     */
    void writeBytes(StringLengthDataType type, byte[] b, int offset, int len);

    /**
     * Write len bytes from given byte array into internal buffer.
     * Read starts from given offset, write starts with current buffer position.
     * 
     * @param type
     *            on-wire data type
     * @param b
     *            from byte array
     * @param offset
     *            starting index of b
     * @param len
     *            number of bytes to be written
     */
    void writeBytes(StringSelfDataType type, byte[] b, int offset, int len);

    /**
     * Read bytes from internal buffer starting from current position into the new byte array.
     * The length of data to read depends on {@link StringSelfDataType}.
     * 
     * @param type
     * @return
     */
    byte[] readBytes(StringSelfDataType type);

    /**
     * Set position to next value in internal buffer skipping the current value according to {@link StringSelfDataType}.
     * 
     * @param type
     * @return
     */
    void skipBytes(StringSelfDataType type);

    /**
     * Read len bytes from internal buffer starting from current position into the new byte array.
     * 
     * @param type
     * @param len
     * @return
     */
    byte[] readBytes(StringLengthDataType type, int len);

    /**
     * Read bytes from internal buffer starting from current position decoding them into String using the specified character encoding.
     * The length of data to read depends on {@link StringSelfDataType}.
     * 
     * @param type
     * @param encoding
     *            if null then platform default encoding is used
     * @return
     */
    String readString(StringSelfDataType type, String encoding);

    /**
     * Read len bytes from internal buffer starting from current position decoding them into String using the specified character encoding.
     * 
     * @param type
     * @param encoding
     *            if null then platform default encoding is used
     * @param len
     * @return
     */
    String readString(StringLengthDataType type, String encoding, int len);
}
