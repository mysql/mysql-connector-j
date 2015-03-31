/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import com.mysql.cj.api.CharsetConverter;
import com.mysql.cj.api.ExceptionInterceptor;
import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.io.PacketBuffer;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.WrongArgumentException;
import com.mysql.cj.core.util.StringUtils;

/**
 * Buffer contains code to read and write packets from/to the MySQL server.
 */
public class Buffer implements PacketBuffer {
    static final int MAX_BYTES_TO_DUMP = 512;

    static final int NO_LENGTH_LIMIT = -1;

    public static final long NULL_LENGTH = -1;

    private int bufLength = 0;

    private byte[] byteBuffer;

    private int position = 0;

    protected boolean wasMultiPacket = false;

    public Buffer(byte[] buf) {
        this.byteBuffer = buf;
        setBufLength(buf.length);
    }

    public Buffer(int size) {
        this.byteBuffer = new byte[size];
        setBufLength(this.byteBuffer.length);
        this.position = ProtocolConstants.HEADER_LENGTH;
    }

    public final void clear() {
        this.position = ProtocolConstants.HEADER_LENGTH;
    }

    final void dump() {
        dump(getBufLength());
    }

    public final String dump(int numBytes) {
        return StringUtils.dumpAsHex(getBytes(0, numBytes > getBufLength() ? getBufLength() : numBytes), numBytes > getBufLength() ? getBufLength() : numBytes);
    }

    final String dumpClampedBytes(int numBytes) {
        int numBytesToDump = numBytes < MAX_BYTES_TO_DUMP ? numBytes : MAX_BYTES_TO_DUMP;

        String dumped = StringUtils.dumpAsHex(getBytes(0, numBytesToDump > getBufLength() ? getBufLength() : numBytesToDump),
                numBytesToDump > getBufLength() ? getBufLength() : numBytesToDump);

        if (numBytesToDump < numBytes) {
            return dumped + " ....(packet exceeds max. dump length)";
        }

        return dumped;
    }

    public final void ensureCapacity(int additionalData) {
        if ((this.position + additionalData) > getBufLength()) {
            if ((this.position + additionalData) < this.byteBuffer.length) {
                // byteBuffer.length is != getBufLength() all of the time due to re-using of packets (we don't shrink them)
                //
                // If we can, don't re-alloc, just set buffer length to size of current buffer
                setBufLength(this.byteBuffer.length);
            } else {
                //
                // Otherwise, re-size, and pad so we can avoid allocing again in the near future
                //
                int newLength = (int) (this.byteBuffer.length * 1.25);

                if (newLength < (this.byteBuffer.length + additionalData)) {
                    newLength = this.byteBuffer.length + (int) (additionalData * 1.25);
                }

                if (newLength < this.byteBuffer.length) {
                    newLength = this.byteBuffer.length + additionalData;
                }

                byte[] newBytes = new byte[newLength];

                System.arraycopy(this.byteBuffer, 0, newBytes, 0, this.byteBuffer.length);
                this.byteBuffer = newBytes;
                setBufLength(this.byteBuffer.length);
            }
        }
    }

    /**
     * Skip over a length-encoded string
     * 
     * @return The position past the end of the string
     */
    public int fastSkipLenString() {
        long len = this.readFieldLength();

        this.position += len;

        return (int) len;
    }

    public void fastSkipLenByteArray() {
        long len = this.readFieldLength();

        if (len == NULL_LENGTH || len == 0) {
            return;
        }

        this.position += len;
    }

    protected final byte[] getBufferSource() {
        return this.byteBuffer;
    }

    public int getBufLength() {
        return this.bufLength;
    }

    public byte[] getByteBuffer() {
        return this.byteBuffer;
    }

    public final byte[] getBytes(int len) {
        byte[] b = new byte[len];
        System.arraycopy(this.byteBuffer, this.position, b, 0, len);
        this.position += len; // update cursor

        return b;
    }

    public byte[] getBytes(int offset, int len) {
        byte[] dest = new byte[len];
        System.arraycopy(this.byteBuffer, offset, dest, 0, len);

        return dest;
    }

    public int getCapacity() {
        return this.byteBuffer.length;
    }

    public ByteBuffer getNioBuffer() {
        throw new IllegalArgumentException(Messages.getString("ByteArrayBuffer.0"));
    }

    /**
     * Returns the current position to write to/ read from
     * 
     * @return the current position to write to/ read from
     */
    public int getPosition() {
        return this.position;
    }

    public final boolean isLastDataPacket() {
        return ((getBufLength() < 9) && ((this.byteBuffer[0] & 0xff) == 254));
    }

    public final boolean isAuthMethodSwitchRequestPacket() {
        return ((this.byteBuffer[0] & 0xff) == 254);
    }

    public final boolean isOKPacket() {
        return ((this.byteBuffer[0] & 0xff) == 0);
    }

    public final boolean isRawPacket() {
        return ((this.byteBuffer[0] & 0xff) == 1);
    }

    public final long readLength() {
        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {
            case 251:
                return 0;

            case 252:
                return readInt();

            case 253:
                return readLongInt();

            case 254:
                return readLongLong();

            default:
                return sw;
        }
    }

    public final byte readByte() {
        return this.byteBuffer[this.position++];
    }

    public final byte readByte(int readAt) {
        return this.byteBuffer[readAt];
    }

    public final long readFieldLength() {
        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {
            case 251:
                return NULL_LENGTH;

            case 252:
                return readInt();

            case 253:
                return readLongInt();

            case 254:
                return readLongLong();

            default:
                return sw;
        }
    }

    public final int readInt() {
        byte[] b = this.byteBuffer; // a little bit optimization

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8);
    }

    public final int readIntAsLong() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8) | ((b[this.position++] & 0xff) << 16) | ((b[this.position++] & 0xff) << 24);
    }

    public final byte[] readLenByteArray(int offset) {
        long len = this.readFieldLength();

        if (len == NULL_LENGTH) {
            return null;
        }

        if (len == 0) {
            return Constants.EMPTY_BYTE_ARRAY;
        }

        this.position += offset;

        return getBytes((int) len);
    }

    public final long readLong() {
        byte[] b = this.byteBuffer;

        return ((long) b[this.position++] & 0xff) | (((long) b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16)
                | ((long) (b[this.position++] & 0xff) << 24);
    }

    final int readLongInt() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8) | ((b[this.position++] & 0xff) << 16);
    }

    public final long readLongLong() {
        byte[] b = this.byteBuffer;

        return (b[this.position++] & 0xff) | ((long) (b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16)
                | ((long) (b[this.position++] & 0xff) << 24) | ((long) (b[this.position++] & 0xff) << 32) | ((long) (b[this.position++] & 0xff) << 40)
                | ((long) (b[this.position++] & 0xff) << 48) | ((long) (b[this.position++] & 0xff) << 56);
    }

    final int readnBytes() {
        int sw = this.byteBuffer[this.position++] & 0xff;

        switch (sw) {
            case 1:
                return this.byteBuffer[this.position++] & 0xff;

            case 2:
                return this.readInt();

            case 3:
                return this.readLongInt();

            case 4:
                return (int) this.readLong();

            default:
                return 255;
        }
    }

    /**
     * Read a null-terminated string
     * 
     * To avoid alloc'ing a new byte array, we do this by hand, rather than calling getNullTerminatedBytes()
     * 
     */
    public final String readString() {
        int i = this.position;
        int len = 0;
        int maxLen = getBufLength();

        while ((i < maxLen) && (this.byteBuffer[i] != 0)) {
            len++;
            i++;
        }

        String s = StringUtils.toString(this.byteBuffer, this.position, len);
        this.position += (len + 1); // update cursor

        return s;
    }

    /**
     * Read string[NUL]
     * 
     * @param encoding
     * @param exceptionInterceptor
     */
    public final String readString(String encoding, ExceptionInterceptor exceptionInterceptor) {
        int i = this.position;
        int len = 0;
        int maxLen = getBufLength();

        while ((i < maxLen) && (this.byteBuffer[i] != 0)) {
            len++;
            i++;
        }

        try {
            return StringUtils.toString(this.byteBuffer, this.position, len, encoding);
        } catch (UnsupportedEncodingException uEE) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ByteArrayBuffer.1", new Object[] { encoding }), uEE,
                    exceptionInterceptor);
        } finally {
            this.position += (len + 1); // update cursor
        }
    }

    /**
     * Read string[$len]
     */
    public final String readString(String encoding, ExceptionInterceptor exceptionInterceptor, int expectedLength) {
        if (this.position + expectedLength > getBufLength()) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ByteArrayBuffer.2"), exceptionInterceptor);
        }

        try {
            return StringUtils.toString(this.byteBuffer, this.position, expectedLength, encoding);
        } catch (UnsupportedEncodingException uEE) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ByteArrayBuffer.1", new Object[] { encoding }), uEE,
                    exceptionInterceptor);
        } finally {
            this.position += expectedLength; // update cursor
        }
    }

    public void setBufLength(int bufLengthToSet) {
        this.bufLength = bufLengthToSet;
    }

    /**
     * Sets the array of bytes to use as a buffer to read from.
     * 
     * @param byteBuffer
     *            the array of bytes to use as a buffer
     */
    public void setByteBuffer(byte[] byteBufferToSet) {
        this.byteBuffer = byteBufferToSet;
    }

    /**
     * Set the current position to write to/ read from
     * 
     * @param position
     *            the position (0-based index)
     */
    public void setPosition(int positionToSet) {
        this.position = positionToSet;
    }

    /**
     * Sets whether this packet was part of a multipacket
     * 
     * @param flag
     *            was this packet part of a multipacket?
     */
    public void setWasMultiPacket(boolean flag) {
        this.wasMultiPacket = flag;
    }

    @Override
    public String toString() {
        return dumpClampedBytes(getPosition());
    }

    public String toSuperString() {
        return super.toString();
    }

    /**
     * Was this packet part of a multipacket?
     * 
     * @return was this packet part of a multipacket?
     */
    public boolean wasMultiPacket() {
        return this.wasMultiPacket;
    }

    public final void writeByte(byte b) {
        ensureCapacity(1);

        this.byteBuffer[this.position++] = b;
    }

    // Write a byte array
    public final void writeBytesNoNull(byte[] bytes) {
        int len = bytes.length;
        ensureCapacity(len);
        System.arraycopy(bytes, 0, this.byteBuffer, this.position, len);
        this.position += len;
    }

    // Write a byte array with the given offset and length
    public final void writeBytesNoNull(byte[] bytes, int offset, int length) {
        ensureCapacity(length);
        System.arraycopy(bytes, offset, this.byteBuffer, this.position, length);
        this.position += length;
    }

    public final void writeDouble(double d) {
        long l = Double.doubleToLongBits(d);
        writeLongLong(l);
    }

    final void writeFieldLength(long length) {
        if (length < 251) {
            writeByte((byte) length);
        } else if (length < 65536L) {
            ensureCapacity(3);
            writeByte((byte) 252);
            writeInt((int) length);
        } else if (length < 16777216L) {
            ensureCapacity(4);
            writeByte((byte) 253);
            writeLongInt((int) length);
        } else {
            ensureCapacity(9);
            writeByte((byte) 254);
            writeLongLong(length);
        }
    }

    public final void writeFloat(float f) {
        ensureCapacity(4);

        int i = Float.floatToIntBits(f);
        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
    }

    public final void writeInt(int i) {
        ensureCapacity(2);

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
    }

    // Write a String using the specified character encoding
    public final void writeLenBytes(byte[] b) {
        int len = b.length;
        ensureCapacity(len + 9);
        writeFieldLength(len);
        System.arraycopy(b, 0, this.byteBuffer, this.position, len);
        this.position += len;
    }

    // Write a String using the specified character encoding
    public final void writeLenString(String s, String encoding, CharsetConverter converter, MysqlConnection conn) {
        byte[] b = null;

        if (converter != null) {
            b = converter.toBytes(s);
        } else {
            b = StringUtils.getBytes(s, conn.getCharsetConverter(encoding), encoding, conn.getExceptionInterceptor());
        }

        int len = b.length;
        ensureCapacity(len + 9);
        writeFieldLength(len);
        System.arraycopy(b, 0, this.byteBuffer, this.position, len);
        this.position += len;
    }

    public final void writeLong(long i) {
        ensureCapacity(4);

        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
    }

    public final void writeLongInt(int i) {
        ensureCapacity(3);
        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
    }

    public final void writeLongLong(long i) {
        ensureCapacity(8);
        byte[] b = this.byteBuffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
        b[this.position++] = (byte) (i >>> 32);
        b[this.position++] = (byte) (i >>> 40);
        b[this.position++] = (byte) (i >>> 48);
        b[this.position++] = (byte) (i >>> 56);
    }

    // Write null-terminated string
    final void writeString(String s) {
        ensureCapacity((s.length() * 3) + 1);
        writeStringNoNull(s);
        this.byteBuffer[this.position++] = 0;
    }

    //	 Write null-terminated string in the given encoding
    public final void writeString(String s, String encoding, MysqlConnection conn) {
        ensureCapacity((s.length() * 3) + 1);
        writeStringNoNull(s, encoding, conn);
        this.byteBuffer[this.position++] = 0;
    }

    // Write string, with no termination
    public final void writeStringNoNull(String s) {
        int len = s.length();
        ensureCapacity(len * 3);
        System.arraycopy(StringUtils.getBytes(s), 0, this.byteBuffer, this.position, len);
        this.position += len;
    }

    // Write a String using the specified character encoding
    public final void writeStringNoNull(String s, String encoding, MysqlConnection conn) {
        byte[] b = StringUtils.getBytes(s, conn.getCharsetConverter(encoding), encoding, conn.getExceptionInterceptor());

        int len = b.length;
        ensureCapacity(len);
        System.arraycopy(b, 0, this.byteBuffer, this.position, len);
        this.position += len;
    }

}
