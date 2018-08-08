/*
 * Copyright (c) 2017, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.a.NativeConstants;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;

// TODO should not be protocol-specific
public abstract class AbstractPreparedQuery<T extends QueryBindings<?>> extends AbstractQuery implements PreparedQuery<T> {

    protected ParseInfo parseInfo;

    protected T queryBindings = null;

    /** The SQL that was passed in to 'prepare' */
    protected String originalSql = null;

    /** The number of parameters in this PreparedStatement */
    protected int parameterCount;

    protected RuntimeProperty<Boolean> autoClosePStmtStreams;

    /** Command index of currently executing batch command. */
    protected int batchCommandIndex = -1;

    protected RuntimeProperty<Boolean> useStreamLengthsInPrepStmts;

    private byte[] streamConvertBuf = null;

    private boolean usingAnsiMode;

    public AbstractPreparedQuery(NativeSession sess) {
        super(sess);

        this.autoClosePStmtStreams = this.session.getPropertySet().getBooleanProperty(PropertyKey.autoClosePStmtStreams);
        this.useStreamLengthsInPrepStmts = this.session.getPropertySet().getBooleanProperty(PropertyKey.useStreamLengthsInPrepStmts);
        this.usingAnsiMode = !this.session.getServerSession().useAnsiQuotedIdentifiers();
    }

    @Override
    public void closeQuery() {
        this.streamConvertBuf = null;
        super.closeQuery();
    }

    public ParseInfo getParseInfo() {
        return this.parseInfo;
    }

    public void setParseInfo(ParseInfo parseInfo) {
        this.parseInfo = parseInfo;
    }

    public String getOriginalSql() {
        return this.originalSql;
    }

    public void setOriginalSql(String originalSql) {
        this.originalSql = originalSql;
    }

    public int getParameterCount() {
        return this.parameterCount;
    }

    public void setParameterCount(int parameterCount) {
        this.parameterCount = parameterCount;
    }

    @Override
    public T getQueryBindings() {
        return this.queryBindings;
    }

    @Override
    public void setQueryBindings(T queryBindings) {
        this.queryBindings = queryBindings;
    }

    public int getBatchCommandIndex() {
        return this.batchCommandIndex;
    }

    public void setBatchCommandIndex(int batchCommandIndex) {
        this.batchCommandIndex = batchCommandIndex;
    }

    /**
     * Computes the optimum number of batched parameter lists to send
     * without overflowing max_allowed_packet.
     * 
     * @param numBatchedArgs
     *            original batch size
     * @return computed batch size
     */
    public int computeBatchSize(int numBatchedArgs) {
        long[] combinedValues = computeMaxParameterSetSizeAndBatchSize(numBatchedArgs);

        long maxSizeOfParameterSet = combinedValues[0];
        long sizeOfEntireBatch = combinedValues[1];

        if (sizeOfEntireBatch < this.maxAllowedPacket.getValue() - this.originalSql.length()) {
            return numBatchedArgs;
        }

        return (int) Math.max(1, (this.maxAllowedPacket.getValue() - this.originalSql.length()) / maxSizeOfParameterSet);
    }

    /**
     * Method checkNullOrEmptyQuery.
     * 
     * @param sql
     *            the SQL to check
     * 
     * @throws WrongArgumentException
     *             if query is null or empty.
     */
    public void checkNullOrEmptyQuery(String sql) {
        if (sql == null) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedQuery.0"), this.session.getExceptionInterceptor());
        }

        if (sql.length() == 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedQuery.1"), this.session.getExceptionInterceptor());
        }
    }

    public String asSql() {
        return asSql(false);
    }

    public String asSql(boolean quoteStreamsAndUnknowns) {
        StringBuilder buf = new StringBuilder();

        Object batchArg = null;
        if (this.batchCommandIndex != -1) {
            batchArg = this.batchedArgs.get(this.batchCommandIndex);
        }

        byte[][] staticSqlStrings = this.parseInfo.getStaticSql();
        for (int i = 0; i < this.parameterCount; ++i) {
            buf.append(this.charEncoding != null ? StringUtils.toString(staticSqlStrings[i], this.charEncoding) : StringUtils.toString(staticSqlStrings[i]));

            byte val[] = null;
            if (batchArg != null && batchArg instanceof String) {
                buf.append((String) batchArg);
                continue;
            }
            val = this.batchCommandIndex == -1 ? (this.queryBindings == null ? null : this.queryBindings.getBindValues()[i].getByteValue())
                    : ((QueryBindings<?>) batchArg).getBindValues()[i].getByteValue();

            boolean isStreamParam = this.batchCommandIndex == -1 ? (this.queryBindings == null ? false : this.queryBindings.getBindValues()[i].isStream())
                    : ((QueryBindings<?>) batchArg).getBindValues()[i].isStream();

            if ((val == null) && !isStreamParam) {
                buf.append(quoteStreamsAndUnknowns ? "'** NOT SPECIFIED **'" : "** NOT SPECIFIED **");
            } else if (isStreamParam) {
                buf.append(quoteStreamsAndUnknowns ? "'** STREAM DATA **'" : "** STREAM DATA **");
            } else {
                buf.append(StringUtils.toString(val, this.charEncoding));
            }
        }

        buf.append(this.charEncoding != null ? StringUtils.toString(staticSqlStrings[this.parameterCount], this.charEncoding)
                : StringUtils.toAsciiString(staticSqlStrings[this.parameterCount]));

        return buf.toString();
    }

    protected abstract long[] computeMaxParameterSetSizeAndBatchSize(int numBatchedArgs);

    /**
     * Creates the packet that contains the query to be sent to the server.
     * 
     * @return A Buffer filled with the query representing the
     *         PreparedStatement.
     */
    @Override
    public <M extends Message> M fillSendPacket() {
        synchronized (this) {
            return fillSendPacket(this.queryBindings);
        }
    }

    /**
     * Creates the packet that contains the query to be sent to the server.
     * 
     * @param bindings
     *            values
     * 
     * @return a Buffer filled with the query that represents this statement
     */
    @SuppressWarnings("unchecked")
    @Override
    public <M extends Message> M fillSendPacket(QueryBindings<?> bindings) {
        synchronized (this) {
            BindValue[] bindValues = bindings.getBindValues();

            NativePacketPayload sendPacket = this.session.getSharedSendPacket();

            sendPacket.writeInteger(IntegerDataType.INT1, NativeConstants.COM_QUERY);

            boolean useStreamLengths = this.useStreamLengthsInPrepStmts.getValue();

            //
            // Try and get this allocation as close as possible for BLOBs
            //
            int ensurePacketSize = 0;

            String statementComment = this.session.getProtocol().getQueryComment();

            byte[] commentAsBytes = null;

            if (statementComment != null) {
                commentAsBytes = StringUtils.getBytes(statementComment, this.charEncoding);

                ensurePacketSize += commentAsBytes.length;
                ensurePacketSize += 6; // for /*[space] [space]*/
            }

            for (int i = 0; i < bindValues.length; i++) {
                if (bindValues[i].isStream() && useStreamLengths) {
                    ensurePacketSize += bindValues[i].getStreamLength();
                }
            }

            if (ensurePacketSize != 0) {
                sendPacket.ensureCapacity(ensurePacketSize);
            }

            if (commentAsBytes != null) {
                sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, Constants.SLASH_STAR_SPACE_AS_BYTES);
                sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, commentAsBytes);
                sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, Constants.SPACE_STAR_SLASH_SPACE_AS_BYTES);
            }

            byte[][] staticSqlStrings = this.parseInfo.getStaticSql();
            for (int i = 0; i < bindValues.length; i++) {
                bindings.checkParameterSet(i);

                sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, staticSqlStrings[i]);

                if (bindValues[i].isStream()) {
                    streamToBytes(sendPacket, bindValues[i].getStreamValue(), true, bindValues[i].getStreamLength(), useStreamLengths);
                } else {
                    sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, bindValues[i].getByteValue());
                }
            }

            sendPacket.writeBytes(StringLengthDataType.STRING_FIXED, staticSqlStrings[bindValues.length]);

            return (M) sendPacket;
        }
    }

    private final void streamToBytes(NativePacketPayload packet, InputStream in, boolean escape, int streamLength, boolean useLength) {
        try {
            if (this.streamConvertBuf == null) {
                this.streamConvertBuf = new byte[4096];
            }

            boolean hexEscape = this.session.getServerSession().isNoBackslashEscapesSet();

            if (streamLength == -1) {
                useLength = false;
            }

            int bc = useLength ? readblock(in, this.streamConvertBuf, streamLength) : readblock(in, this.streamConvertBuf);

            int lengthLeftToRead = streamLength - bc;

            packet.writeBytes(StringLengthDataType.STRING_FIXED, StringUtils.getBytes(hexEscape ? "x" : "_binary"));

            if (escape) {
                packet.writeInteger(IntegerDataType.INT1, (byte) '\'');
            }

            while (bc > 0) {
                if (hexEscape) {
                    ((AbstractQueryBindings<?>) this.queryBindings).hexEscapeBlock(this.streamConvertBuf, packet, bc);
                } else if (escape) {
                    escapeblockFast(this.streamConvertBuf, packet, bc);
                } else {
                    packet.writeBytes(StringLengthDataType.STRING_FIXED, this.streamConvertBuf, 0, bc);
                }

                if (useLength) {
                    bc = readblock(in, this.streamConvertBuf, lengthLeftToRead);

                    if (bc > 0) {
                        lengthLeftToRead -= bc;
                    }
                } else {
                    bc = readblock(in, this.streamConvertBuf);
                }
            }

            if (escape) {
                packet.writeInteger(IntegerDataType.INT1, (byte) '\'');
            }
        } finally {
            if (this.autoClosePStmtStreams.getValue()) {
                try {
                    in.close();
                } catch (IOException ioEx) {
                }

                in = null;
            }
        }
    }

    protected final byte[] streamToBytes(InputStream in, boolean escape, int streamLength, boolean useLength) {
        in.mark(Integer.MAX_VALUE); // we may need to read this same stream several times, so we need to reset it at the end.
        try {
            if (this.streamConvertBuf == null) {
                this.streamConvertBuf = new byte[4096];
            }
            if (streamLength == -1) {
                useLength = false;
            }

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

            int bc = useLength ? readblock(in, this.streamConvertBuf, streamLength) : readblock(in, this.streamConvertBuf);

            int lengthLeftToRead = streamLength - bc;

            if (escape) {
                bytesOut.write('_');
                bytesOut.write('b');
                bytesOut.write('i');
                bytesOut.write('n');
                bytesOut.write('a');
                bytesOut.write('r');
                bytesOut.write('y');
                bytesOut.write('\'');
            }

            while (bc > 0) {
                if (escape) {
                    StringUtils.escapeblockFast(this.streamConvertBuf, bytesOut, bc, this.usingAnsiMode);
                } else {
                    bytesOut.write(this.streamConvertBuf, 0, bc);
                }

                if (useLength) {
                    bc = readblock(in, this.streamConvertBuf, lengthLeftToRead);

                    if (bc > 0) {
                        lengthLeftToRead -= bc;
                    }
                } else {
                    bc = readblock(in, this.streamConvertBuf);
                }
            }

            if (escape) {
                bytesOut.write('\'');
            }

            return bytesOut.toByteArray();
        } finally {
            try {
                in.reset();
            } catch (IOException e) {
            }
            if (this.autoClosePStmtStreams.getValue()) {
                try {
                    in.close();
                } catch (IOException ioEx) {
                }

                in = null;
            }
        }
    }

    private final int readblock(InputStream i, byte[] b) {
        try {
            return i.read(b);
        } catch (Throwable ex) {
            throw ExceptionFactory.createException(Messages.getString("PreparedStatement.56") + ex.getClass().getName(),
                    this.session.getExceptionInterceptor());
        }
    }

    private final int readblock(InputStream i, byte[] b, int length) {
        try {
            int lengthToRead = length;

            if (lengthToRead > b.length) {
                lengthToRead = b.length;
            }

            return i.read(b, 0, lengthToRead);
        } catch (Throwable ex) {
            throw ExceptionFactory.createException(Messages.getString("PreparedStatement.56") + ex.getClass().getName(),
                    this.session.getExceptionInterceptor());
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
                if ((b == '\\') || (b == '\'') || (!this.usingAnsiMode && b == '"')) {
                    // write stuff not yet written
                    if (i > lastwritten) {
                        packet.writeBytes(StringLengthDataType.STRING_FIXED, buf, lastwritten, i - lastwritten);
                    }

                    // write escape
                    packet.writeInteger(IntegerDataType.INT1, (byte) '\\');
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
