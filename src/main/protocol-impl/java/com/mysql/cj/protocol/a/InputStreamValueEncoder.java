/*
 * Copyright (c) 2022, 2023, Oracle and/or its affiliates.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.mysql.cj.BindValue;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJOperationNotSupportedException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

public class InputStreamValueEncoder extends AbstractValueEncoder {

    private byte[] streamConvertBuf = null;

    @Override
    public byte[] getBytes(BindValue binding) {
        return streamToBytes((InputStream) binding.getValue(), binding.getScaleOrLength(), null);
    }

    @Override
    public String getString(BindValue binding) {
        return "'** STREAM DATA **'";
    }

    @Override
    public void encodeAsText(Message msg, BindValue binding) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
        streamToBytes((InputStream) binding.getValue(), binding.getScaleOrLength(), intoPacket);
    }

    @Override
    public void encodeAsBinary(Message msg, BindValue binding) {
        throw ExceptionFactory.createException(CJOperationNotSupportedException.class, "Not supported");
    }

    protected byte[] streamToBytes(InputStream in, long length, NativePacketPayload packet) {
        boolean useLength = length == -1 ? false : this.propertySet.getBooleanProperty(PropertyKey.useStreamLengthsInPrepStmts).getValue();
        in.mark(Integer.MAX_VALUE); // we may need to read this same stream several times, so we need to reset it at the end.
        try {
            if (this.streamConvertBuf == null) {
                this.streamConvertBuf = new byte[4096];
            }
            int bcnt = useLength ? Util.readBlock(in, this.streamConvertBuf, (int) length, this.exceptionInterceptor)
                    : Util.readBlock(in, this.streamConvertBuf, this.exceptionInterceptor);
            int lengthLeftToRead = (int) (length - bcnt);

            ByteArrayOutputStream bytesOut = null;
            boolean hexEscape = false;
            if (packet == null) {
                bytesOut = new ByteArrayOutputStream();
            } else {
                hexEscape = this.serverSession.isNoBackslashEscapesSet();
                packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(hexEscape ? "x" : "_binary"));
                packet.writeInteger(IntegerDataType.INT1, (byte) '\'');
            }

            while (bcnt > 0) {
                if (packet == null) {
                    bytesOut.write(this.streamConvertBuf, 0, bcnt);
                } else {
                    if (hexEscape) {
                        StringUtils.hexEscapeBlock(this.streamConvertBuf, bcnt, (lowBits, highBits) -> {
                            packet.writeInteger(IntegerDataType.INT1, lowBits);
                            packet.writeInteger(IntegerDataType.INT1, highBits);
                        });
                    } else {
                        escapeblockFast(this.streamConvertBuf, packet, bcnt);
                    }
                }

                if (useLength) {
                    bcnt = Util.readBlock(in, this.streamConvertBuf, lengthLeftToRead, this.exceptionInterceptor);
                    if (bcnt > 0) {
                        lengthLeftToRead -= bcnt;
                    }
                } else {
                    bcnt = Util.readBlock(in, this.streamConvertBuf, this.exceptionInterceptor);
                }
            }

            if (packet == null) {
                return bytesOut.toByteArray();
            }

            packet.writeInteger(IntegerDataType.INT1, (byte) '\'');
            return null;

        } finally {
            try {
                in.reset();
            } catch (IOException e) {
            }
            if (this.propertySet.getBooleanProperty(PropertyKey.autoClosePStmtStreams).getValue()) {
                try {
                    in.close();
                } catch (IOException ioEx) {
                }

                in = null;
            }
        }
    }

    private final void escapeblockFast(byte[] buf, NativePacketPayload packet, int size) {
        int lastwritten = 0;

        for (int i = 0; i < size; i++) {
            byte b = buf[i];

            if (b == '\0') {
                // write stuff not yet written
                if (i > lastwritten) {
                    packet.writeBytes(StringLengthDataType.STRING_FIXED, buf, lastwritten, i - lastwritten);
                }

                // write escape
                packet.writeInteger(IntegerDataType.INT1, (byte) '\\');
                packet.writeInteger(IntegerDataType.INT1, (byte) '0');
                lastwritten = i + 1;
            } else {
                if (b == '\\' || b == '\'') {
                    // write stuff not yet written
                    if (i > lastwritten) {
                        packet.writeBytes(StringLengthDataType.STRING_FIXED, buf, lastwritten, i - lastwritten);
                    }

                    // write escape
                    packet.writeInteger(IntegerDataType.INT1, b);
                    lastwritten = i; // not i+1 as b wasn't written.
                }
            }
        }

        // write out remaining stuff from buffer
        if (lastwritten < size) {
            packet.writeBytes(StringLengthDataType.STRING_FIXED, buf, lastwritten, size - lastwritten);
        }
    }

}
