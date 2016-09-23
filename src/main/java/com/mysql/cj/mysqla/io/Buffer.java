/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.util.StringUtils;

/**
 * Buffer contains code to read and write packets from/to the MySQL server.
 */
public class Buffer implements PacketPayload {

    private int payloadLength = 0;

    private byte[] byteBuffer;

    private int position = 0;

    static final int MAX_BYTES_TO_DUMP = 512;

    @Override
    public String toString() {
        int numBytes = this.position <= this.payloadLength ? this.position : this.payloadLength;
        int numBytesToDump = numBytes < MAX_BYTES_TO_DUMP ? numBytes : MAX_BYTES_TO_DUMP;

        this.position = 0;
        String dumped = StringUtils.dumpAsHex(readBytes(StringLengthDataType.STRING_FIXED, numBytesToDump), numBytesToDump);

        if (numBytesToDump < numBytes) {
            return dumped + " ....(packet exceeds max. dump length)";
        }

        return dumped;
    }

    public String toSuperString() {
        return super.toString();
    }

    public Buffer(byte[] buf) {
        this.byteBuffer = buf;
        this.payloadLength = buf.length;
    }

    public Buffer(int size) {
        this.byteBuffer = new byte[size];
        this.payloadLength = size;
    }

    @Override
    public int getCapacity() {
        return this.byteBuffer.length;
    }

    @Override
    public final void ensureCapacity(int additionalData) {
        if ((this.position + additionalData) > this.byteBuffer.length) {
            //
            // Resize, and pad so we can avoid allocing again in the near future
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
        }
    }

    @Override
    public byte[] getByteBuffer() {
        return this.byteBuffer;
    }

    @Override
    public void setByteBuffer(byte[] byteBufferToSet) {
        this.byteBuffer = byteBufferToSet;
    }

    @Override
    public int getPayloadLength() {
        return this.payloadLength;
    }

    @Override
    public void setPayloadLength(int bufLengthToSet) {
        if (bufLengthToSet > this.byteBuffer.length) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Buffer.0"));
        }
        this.payloadLength = bufLengthToSet;
    }

    /**
     * To be called after write operations to ensure that payloadLength contains
     * the real size of written data.
     */
    private void adjustPayloadLength() {
        if (this.position > this.payloadLength) {
            this.payloadLength = this.position;
        }
    }

    @Override
    public int getPosition() {
        return this.position;
    }

    @Override
    public void setPosition(int positionToSet) {
        this.position = positionToSet;
    }

    @Override
    public final boolean isEOFPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_EOF && (getPayloadLength() <= 5);
    }

    @Override
    public final boolean isAuthMethodSwitchRequestPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_AUTH_SWITCH;
    }

    @Override
    public final boolean isOKPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_OK;
    }

    @Override
    public final boolean isResultSetOKPacket() {
        return (this.byteBuffer[0] & 0xff) == TYPE_ID_EOF && (getPayloadLength() < 16777215);
    }

    @Override
    public final boolean isAuthMoreData() {
        return ((this.byteBuffer[0] & 0xff) == 1);
    }

    @Override
    public void writeInteger(IntegerDataType type, long l) {
        byte[] b;
        switch (type) {
            case INT1:
                ensureCapacity(1);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                break;

            case INT2:
                ensureCapacity(2);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                b[this.position++] = (byte) (l >>> 8);
                break;

            case INT3:
                ensureCapacity(3);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                b[this.position++] = (byte) (l >>> 8);
                b[this.position++] = (byte) (l >>> 16);
                break;

            case INT4:
                ensureCapacity(4);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                b[this.position++] = (byte) (l >>> 8);
                b[this.position++] = (byte) (l >>> 16);
                b[this.position++] = (byte) (l >>> 24);
                break;

            case INT6:
                ensureCapacity(6);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                b[this.position++] = (byte) (l >>> 8);
                b[this.position++] = (byte) (l >>> 16);
                b[this.position++] = (byte) (l >>> 24);
                b[this.position++] = (byte) (l >>> 32);
                b[this.position++] = (byte) (l >>> 40);
                break;

            case INT8:
                ensureCapacity(8);
                b = this.byteBuffer;
                b[this.position++] = (byte) (l & 0xff);
                b[this.position++] = (byte) (l >>> 8);
                b[this.position++] = (byte) (l >>> 16);
                b[this.position++] = (byte) (l >>> 24);
                b[this.position++] = (byte) (l >>> 32);
                b[this.position++] = (byte) (l >>> 40);
                b[this.position++] = (byte) (l >>> 48);
                b[this.position++] = (byte) (l >>> 56);
                break;

            case INT_LENENC:
                if (l < 251) {
                    ensureCapacity(1);
                    writeInteger(IntegerDataType.INT1, l);

                } else if (l < 65536L) {
                    ensureCapacity(3);
                    writeInteger(IntegerDataType.INT1, 252);
                    writeInteger(IntegerDataType.INT2, l);

                } else if (l < 16777216L) {
                    ensureCapacity(4);
                    writeInteger(IntegerDataType.INT1, 253);
                    writeInteger(IntegerDataType.INT3, l);

                } else {
                    ensureCapacity(9);
                    writeInteger(IntegerDataType.INT1, 254);
                    writeInteger(IntegerDataType.INT8, l);
                }
        }

        adjustPayloadLength();
    }

    @Override
    public final long readInteger(IntegerDataType type) {
        byte[] b = this.byteBuffer;
        switch (type) {
            case INT1:
                return (b[this.position++] & 0xff);

            case INT2:
                return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8);

            case INT3:
                return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8) | ((b[this.position++] & 0xff) << 16);

            case INT4:
                return ((long) b[this.position++] & 0xff) | (((long) b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16)
                        | ((long) (b[this.position++] & 0xff) << 24);

            case INT6:
                return (b[this.position++] & 0xff) | ((long) (b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16)
                        | ((long) (b[this.position++] & 0xff) << 24) | ((long) (b[this.position++] & 0xff) << 32) | ((long) (b[this.position++] & 0xff) << 40);

            case INT8:
                return (b[this.position++] & 0xff) | ((long) (b[this.position++] & 0xff) << 8) | ((long) (b[this.position++] & 0xff) << 16)
                        | ((long) (b[this.position++] & 0xff) << 24) | ((long) (b[this.position++] & 0xff) << 32) | ((long) (b[this.position++] & 0xff) << 40)
                        | ((long) (b[this.position++] & 0xff) << 48) | ((long) (b[this.position++] & 0xff) << 56);

            case INT_LENENC:
                int sw = b[this.position++] & 0xff;
                switch (sw) {
                    case 251:
                        return NULL_LENGTH; // represents a NULL in a ProtocolText::ResultsetRow
                    case 252:
                        return readInteger(IntegerDataType.INT2);
                    case 253:
                        return readInteger(IntegerDataType.INT3);
                    case 254:
                        return readInteger(IntegerDataType.INT8);
                    default:
                        return sw;
                }

            default:
                return (b[this.position++] & 0xff);
        }
    }

    @Override
    public final void writeBytes(StringSelfDataType type, byte[] b) {
        writeBytes(type, b, 0, b.length);
    }

    @Override
    public final void writeBytes(StringLengthDataType type, byte[] b) {
        writeBytes(type, b, 0, b.length);
    }

    @Override
    public void writeBytes(StringSelfDataType type, byte[] b, int offset, int len) {
        switch (type) {
            case STRING_EOF:
                writeBytes(StringLengthDataType.STRING_FIXED, b, offset, len);
                break;

            case STRING_TERM:
                ensureCapacity(len + 1);
                writeBytes(StringLengthDataType.STRING_FIXED, b, offset, len);
                this.byteBuffer[this.position++] = 0;
                break;

            case STRING_LENENC:
                ensureCapacity(len + 9);
                writeInteger(IntegerDataType.INT_LENENC, len);
                writeBytes(StringLengthDataType.STRING_FIXED, b, offset, len);
                break;
        }

        adjustPayloadLength();
    }

    @Override
    public void writeBytes(StringLengthDataType type, byte[] b, int offset, int len) {
        switch (type) {
            case STRING_FIXED:
            case STRING_VAR:
                ensureCapacity(len);
                System.arraycopy(b, offset, this.byteBuffer, this.position, len);
                this.position += len;
                break;
        }

        adjustPayloadLength();
    }

    @Override
    public byte[] readBytes(StringSelfDataType type) {
        byte[] b;
        switch (type) {
            case STRING_TERM:
                int i = this.position;
                while ((i < this.payloadLength) && (this.byteBuffer[i] != 0)) {
                    i++;
                }
                b = readBytes(StringLengthDataType.STRING_FIXED, i - this.position);
                this.position++; // skip terminating byte
                return b;

            case STRING_LENENC:
                long l = readInteger(IntegerDataType.INT_LENENC);
                return l == NULL_LENGTH ? null : (l == 0 ? Constants.EMPTY_BYTE_ARRAY : readBytes(StringLengthDataType.STRING_FIXED, (int) l));

            case STRING_EOF:
                return readBytes(StringLengthDataType.STRING_FIXED, this.payloadLength - this.position);
        }
        return null;
    }

    @Override
    public void skipBytes(StringSelfDataType type) {
        switch (type) {
            case STRING_TERM:
                while ((this.position < this.payloadLength) && (this.byteBuffer[this.position] != 0)) {
                    this.position++;
                }
                this.position++; // skip terminating byte
                break;

            case STRING_LENENC:
                long len = readInteger(IntegerDataType.INT_LENENC);
                if (len != PacketPayload.NULL_LENGTH && len != 0) {
                    this.position += (int) len;
                }
                break;

            case STRING_EOF:
                this.position = this.payloadLength;
                break;
        }
    }

    @Override
    public byte[] readBytes(StringLengthDataType type, int len) {
        byte[] b;
        switch (type) {
            case STRING_FIXED:
            case STRING_VAR:
                b = new byte[len];
                System.arraycopy(this.byteBuffer, this.position, b, 0, len);
                this.position += len;
                return b;
        }
        return null;
    }

    @Override
    public String readString(StringSelfDataType type, String encoding) {
        String res = null;
        switch (type) {
            case STRING_TERM:
                int i = this.position;
                while ((i < this.payloadLength) && (this.byteBuffer[i] != 0)) {
                    i++;
                }
                res = readString(StringLengthDataType.STRING_FIXED, encoding, i - this.position);
                this.position++; // skip terminating byte
                break;

            case STRING_LENENC:
                long l = readInteger(IntegerDataType.INT_LENENC);
                return l == NULL_LENGTH ? null : (l == 0 ? "" : readString(StringLengthDataType.STRING_FIXED, encoding, (int) l));

            case STRING_EOF:
                return readString(StringLengthDataType.STRING_FIXED, encoding, this.payloadLength - this.position);

        }
        return res;
    }

    @Override
    public String readString(StringLengthDataType type, String encoding, int len) {
        String res = null;
        switch (type) {
            case STRING_FIXED:
            case STRING_VAR:
                if ((this.position + len) > this.payloadLength) {
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Buffer.1"));
                }

                res = StringUtils.toString(this.byteBuffer, this.position, len, encoding);
                this.position += len;
                break;

        }
        return res;
    }

}
