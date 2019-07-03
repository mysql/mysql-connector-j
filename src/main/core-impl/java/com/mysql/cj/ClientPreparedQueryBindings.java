/*
 * Copyright (c) 2017, 2019, Oracle and/or its affiliates. All rights reserved.
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
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;
import com.mysql.cj.util.Util;

public class ClientPreparedQueryBindings extends AbstractQueryBindings<ClientPreparedQueryBindValue> {

    /** Charset encoder used to escape if needed, such as Yen sign in SJIS */
    private CharsetEncoder charsetEncoder;

    private SimpleDateFormat ddf;

    private SimpleDateFormat tdf;

    private SimpleDateFormat tsdf = null;

    public ClientPreparedQueryBindings(int parameterCount, Session sess) {
        super(parameterCount, sess);
        if (((NativeSession) this.session).getRequiresEscapingEncoder()) {
            this.charsetEncoder = Charset.forName(this.charEncoding).newEncoder();
        }
    }

    @Override
    protected void initBindValues(int parameterCount) {
        this.bindValues = new ClientPreparedQueryBindValue[parameterCount];
        for (int i = 0; i < parameterCount; i++) {
            this.bindValues[i] = new ClientPreparedQueryBindValue();
        }
    }

    @Override
    public ClientPreparedQueryBindings clone() {
        ClientPreparedQueryBindings newBindings = new ClientPreparedQueryBindings(this.bindValues.length, this.session);
        ClientPreparedQueryBindValue[] bvs = new ClientPreparedQueryBindValue[this.bindValues.length];
        for (int i = 0; i < this.bindValues.length; i++) {
            bvs[i] = this.bindValues[i].clone();
        }
        newBindings.setBindValues(bvs);
        newBindings.isLoadDataQuery = this.isLoadDataQuery;
        return newBindings;
    }

    @Override
    public void checkParameterSet(int columnIndex) {
        if (!this.bindValues[columnIndex].isSet()) {
            throw ExceptionFactory.createException(Messages.getString("PreparedStatement.40") + (columnIndex + 1),
                    MysqlErrorNumbers.SQL_STATE_WRONG_NO_OF_PARAMETERS, 0, true, null, this.session.getExceptionInterceptor());
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) {
        setAsciiStream(parameterIndex, x, -1);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            setBinaryStream(parameterIndex, x, length);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) {
        setAsciiStream(parameterIndex, x, (int) length);
        this.bindValues[parameterIndex].setMysqlType(MysqlType.TEXT); // TODO was Types.CLOB, check; use length to find right TEXT type
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            setValue(parameterIndex, StringUtils.fixDecimalExponent(x.toPlainString()), MysqlType.DECIMAL);
        }
    }

    @Override
    public void setBigInteger(int parameterIndex, BigInteger x) {
        setValue(parameterIndex, x.toString(), MysqlType.BIGINT_UNSIGNED);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) {
        setBinaryStream(parameterIndex, x, -1);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            this.bindValues[parameterIndex].setNull(false);
            this.bindValues[parameterIndex].setIsStream(true);
            this.bindValues[parameterIndex].setMysqlType(MysqlType.BLOB); // TODO use length to find the right BLOB type
            this.bindValues[parameterIndex].setStreamValue(x, length);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) {
        setBinaryStream(parameterIndex, x, (int) length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) {
        setBinaryStream(parameterIndex, inputStream);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) {
        setBinaryStream(parameterIndex, inputStream, (int) length);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            try {
                setBinaryStream(parameterIndex, x.getBinaryStream());
            } catch (Throwable t) {
                throw ExceptionFactory.createException(t.getMessage(), t, this.session.getExceptionInterceptor());
            }
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        setValue(parameterIndex, x ? "1" : "0", MysqlType.BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        setValue(parameterIndex, String.valueOf(x), MysqlType.TINYINT);
    }

    public void setBytes(int parameterIndex, byte[] x) {
        setBytes(parameterIndex, x, true, true);

        if (x != null) {
            this.bindValues[parameterIndex].setMysqlType(MysqlType.BINARY); // TODO VARBINARY ?
        }
    }

    public synchronized void setBytes(int parameterIndex, byte[] x, boolean checkForIntroducer, boolean escapeForMBChars) {
        if (x == null) {
            setNull(parameterIndex); // setNull(parameterIndex, MysqlType.BINARY);
        } else {
            if (this.session.getServerSession().isNoBackslashEscapesSet() || (escapeForMBChars && CharsetMapping.isMultibyteCharset(this.charEncoding))) {

                // Send as hex

                ByteArrayOutputStream bOut = new ByteArrayOutputStream((x.length * 2) + 3);
                bOut.write('x');
                bOut.write('\'');

                // TODO unify with AbstractQueryBindings.hexEscapeBlock()
                for (int i = 0; i < x.length; i++) {
                    int lowBits = (x[i] & 0xff) / 16;
                    int highBits = (x[i] & 0xff) % 16;

                    bOut.write(HEX_DIGITS[lowBits]);
                    bOut.write(HEX_DIGITS[highBits]);
                }

                bOut.write('\'');

                setValue(parameterIndex, bOut.toByteArray(), MysqlType.BINARY); // TODO VARBINARY ?

                // save original value for re-usage by refresher SSPS in updatable result set
                setOrigValue(parameterIndex, x);

                return;
            }

            // escape them
            int numBytes = x.length;

            int pad = 2;

            if (checkForIntroducer) {
                pad += 7;
            }

            ByteArrayOutputStream bOut = new ByteArrayOutputStream(numBytes + pad);

            if (checkForIntroducer) {
                bOut.write('_');
                bOut.write('b');
                bOut.write('i');
                bOut.write('n');
                bOut.write('a');
                bOut.write('r');
                bOut.write('y');
            }
            bOut.write('\'');

            for (int i = 0; i < numBytes; ++i) {
                byte b = x[i];

                switch (b) {
                    case 0: /* Must be escaped for 'mysql' */
                        bOut.write('\\');
                        bOut.write('0');
                        break;
                    case '\n': /* Must be escaped for logs */
                        bOut.write('\\');
                        bOut.write('n');
                        break;
                    case '\r':
                        bOut.write('\\');
                        bOut.write('r');
                        break;
                    case '\\':
                        bOut.write('\\');
                        bOut.write('\\');
                        break;
                    case '\'':
                        bOut.write('\\');
                        bOut.write('\'');
                        break;
                    case '"': /* Better safe than sorry */
                        bOut.write('\\');
                        bOut.write('"');
                        break;
                    case '\032': /* This gives problems on Win32 */
                        bOut.write('\\');
                        bOut.write('Z');
                        break;
                    default:
                        bOut.write(b);
                }
            }

            bOut.write('\'');

            setValue(parameterIndex, bOut.toByteArray(), MysqlType.BINARY); // TODO VARBINARY ?
        }
    }

    @Override
    public void setBytesNoEscape(int parameterIndex, byte[] parameterAsBytes) {
        byte[] parameterWithQuotes = StringUtils.quoteBytes(parameterAsBytes);

        setValue(parameterIndex, parameterWithQuotes, MysqlType.BINARY); // TODO VARBINARY ?
    }

    @Override
    public void setBytesNoEscapeNoQuotes(int parameterIndex, byte[] parameterAsBytes) {
        setValue(parameterIndex, parameterAsBytes, MysqlType.BINARY); // TODO VARBINARY ?
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) {
        setCharacterStream(parameterIndex, reader, -1);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) {
        try {
            if (reader == null) {
                setNull(parameterIndex);
            } else {
                char[] c = null;
                int len = 0;

                boolean useLength = this.useStreamLengthsInPrepStmts.getValue();

                String forcedEncoding = this.session.getPropertySet().getStringProperty(PropertyKey.clobCharacterEncoding).getStringValue();

                if (useLength && (length != -1)) {
                    c = new char[length];

                    int numCharsRead = Util.readFully(reader, c, length); // blocks until all read

                    if (forcedEncoding == null) {
                        setString(parameterIndex, new String(c, 0, numCharsRead));
                    } else {
                        setBytes(parameterIndex, StringUtils.getBytes(new String(c, 0, numCharsRead), forcedEncoding));
                    }
                } else {
                    c = new char[4096];

                    StringBuilder buf = new StringBuilder();

                    while ((len = reader.read(c)) != -1) {
                        buf.append(c, 0, len);
                    }

                    if (forcedEncoding == null) {
                        setString(parameterIndex, buf.toString());
                    } else {
                        setBytes(parameterIndex, StringUtils.getBytes(buf.toString(), forcedEncoding));
                    }
                }

                this.bindValues[parameterIndex].setMysqlType(MysqlType.TEXT); // TODO was Types.CLOB
            }
        } catch (UnsupportedEncodingException uec) {
            throw ExceptionFactory.createException(WrongArgumentException.class, uec.toString(), uec, this.session.getExceptionInterceptor());
        } catch (IOException ioEx) {
            throw ExceptionFactory.createException(ioEx.toString(), ioEx, this.session.getExceptionInterceptor());
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) {
        setCharacterStream(parameterIndex, reader, (int) length);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) {
        setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) {
        setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setClob(int i, Clob x) {
        if (x == null) {
            setNull(i);
        } else {
            try {
                String forcedEncoding = this.session.getPropertySet().getStringProperty(PropertyKey.clobCharacterEncoding).getStringValue();

                if (forcedEncoding == null) {
                    setString(i, x.getSubString(1L, (int) x.length()));
                } else {
                    setBytes(i, StringUtils.getBytes(x.getSubString(1L, (int) x.length()), forcedEncoding));
                }

                this.bindValues[i].setMysqlType(MysqlType.TEXT); // TODO was Types.CLOB
            } catch (Throwable t) {
                throw ExceptionFactory.createException(t.getMessage(), t);
            }
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) {
        setDate(parameterIndex, x, null);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            this.ddf = TimeUtil.getSimpleDateFormat(this.ddf, "''yyyy-MM-dd''", cal, cal != null ? null : this.session.getServerSession().getDefaultTimeZone());
            setValue(parameterIndex, this.ddf.format(x), MysqlType.DATE);
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        if (!this.session.getPropertySet().getBooleanProperty(PropertyKey.allowNanAndInf).getValue()
                && (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY || Double.isNaN(x))) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedStatement.64", new Object[] { x }),
                    this.session.getExceptionInterceptor());
        }
        setValue(parameterIndex, StringUtils.fixDecimalExponent(String.valueOf(x)), MysqlType.DOUBLE);
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        setValue(parameterIndex, StringUtils.fixDecimalExponent(String.valueOf(x)), MysqlType.FLOAT); // TODO check; was Types.FLOAT but should be Types.REAL to map to SQL FLOAT
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        setValue(parameterIndex, String.valueOf(x), MysqlType.INT);
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        setValue(parameterIndex, String.valueOf(x), MysqlType.BIGINT);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) {
        setNCharacterStream(parameterIndex, value, -1);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader, long length) {
        if (reader == null) {
            setNull(parameterIndex);
        } else {
            try {
                char[] c = null;
                int len = 0;

                boolean useLength = this.useStreamLengthsInPrepStmts.getValue();

                // Ignore "clobCharacterEncoding" because utf8 should be used this time.

                if (useLength && (length != -1)) {
                    c = new char[(int) length];  // can't take more than Integer.MAX_VALUE

                    int numCharsRead = Util.readFully(reader, c, (int) length); // blocks until all read
                    setNString(parameterIndex, new String(c, 0, numCharsRead));

                } else {
                    c = new char[4096];

                    StringBuilder buf = new StringBuilder();

                    while ((len = reader.read(c)) != -1) {
                        buf.append(c, 0, len);
                    }

                    setNString(parameterIndex, buf.toString());
                }

                this.bindValues[parameterIndex].setMysqlType(MysqlType.TEXT); // TODO was Types.NCLOB; use length to find right TEXT type
            } catch (Throwable t) {
                throw ExceptionFactory.createException(t.getMessage(), t, this.session.getExceptionInterceptor());
            }
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) {
        setNCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) {
        if (reader == null) {
            setNull(parameterIndex);
        } else {
            setNCharacterStream(parameterIndex, reader, length);
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) {
        if (value == null) {
            setNull(parameterIndex);
        } else {
            try {
                setNCharacterStream(parameterIndex, value.getCharacterStream(), value.length());
            } catch (Throwable t) {
                throw ExceptionFactory.createException(t.getMessage(), t, this.session.getExceptionInterceptor());
            }
        }
    }

    @Override
    public void setNString(int parameterIndex, String x) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            if (this.charEncoding.equalsIgnoreCase("UTF-8") || this.charEncoding.equalsIgnoreCase("utf8")) {
                setString(parameterIndex, x);
                return;
            }

            int stringLength = x.length();
            // Ignore sql_mode=NO_BACKSLASH_ESCAPES in current implementation.

            // Add introducer _utf8 for NATIONAL CHARACTER
            StringBuilder buf = new StringBuilder((int) (x.length() * 1.1 + 4));
            buf.append("_utf8");
            buf.append('\'');

            //
            // Note: buf.append(char) is _faster_ than appending in blocks, because the block append requires a System.arraycopy().... go figure...
            //

            for (int i = 0; i < stringLength; ++i) {
                char c = x.charAt(i);

                switch (c) {
                    case 0: /* Must be escaped for 'mysql' */
                        buf.append('\\');
                        buf.append('0');
                        break;
                    case '\n': /* Must be escaped for logs */
                        buf.append('\\');
                        buf.append('n');
                        break;
                    case '\r':
                        buf.append('\\');
                        buf.append('r');
                        break;
                    case '\\':
                        buf.append('\\');
                        buf.append('\\');
                        break;
                    case '\'':
                        buf.append('\\');
                        buf.append('\'');
                        break;
                    case '"': /* Better safe than sorry */
                        if (this.session.getServerSession().useAnsiQuotedIdentifiers()) {
                            buf.append('\\');
                        }
                        buf.append('"');
                        break;
                    case '\032': /* This gives problems on Win32 */
                        buf.append('\\');
                        buf.append('Z');
                        break;
                    default:
                        buf.append(c);
                }
            }

            buf.append('\'');

            byte[] parameterAsBytes = this.isLoadDataQuery ? StringUtils.getBytes(buf.toString()) : StringUtils.getBytes(buf.toString(), "UTF-8");

            setValue(parameterIndex, parameterAsBytes, MysqlType.VARCHAR); // TODO was Types.NVARCHAR
        }
    }

    @Override
    public synchronized void setNull(int parameterIndex) {
        setValue(parameterIndex, "null", MysqlType.NULL);
        this.bindValues[parameterIndex].setNull(true);
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        setValue(parameterIndex, String.valueOf(x), MysqlType.SMALLINT);
    }

    @Override
    public void setString(int parameterIndex, String x) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            int stringLength = x.length();

            if (this.session.getServerSession().isNoBackslashEscapesSet()) {
                // Scan for any nasty chars

                boolean needsHexEscape = isEscapeNeededForString(x, stringLength);

                if (!needsHexEscape) {
                    StringBuilder quotedString = new StringBuilder(x.length() + 2);
                    quotedString.append('\'');
                    quotedString.append(x);
                    quotedString.append('\'');

                    byte[] parameterAsBytes = this.isLoadDataQuery ? StringUtils.getBytes(quotedString.toString())
                            : StringUtils.getBytes(quotedString.toString(), this.charEncoding);
                    setValue(parameterIndex, parameterAsBytes, MysqlType.VARCHAR);

                } else {
                    byte[] parameterAsBytes = this.isLoadDataQuery ? StringUtils.getBytes(x) : StringUtils.getBytes(x, this.charEncoding);
                    setBytes(parameterIndex, parameterAsBytes);
                }

                return;
            }

            String parameterAsString = x;
            boolean needsQuoted = true;

            if (this.isLoadDataQuery || isEscapeNeededForString(x, stringLength)) {
                needsQuoted = false; // saves an allocation later

                StringBuilder buf = new StringBuilder((int) (x.length() * 1.1));

                buf.append('\'');

                //
                // Note: buf.append(char) is _faster_ than appending in blocks, because the block append requires a System.arraycopy().... go figure...
                //

                for (int i = 0; i < stringLength; ++i) {
                    char c = x.charAt(i);

                    switch (c) {
                        case 0: /* Must be escaped for 'mysql' */
                            buf.append('\\');
                            buf.append('0');
                            break;
                        case '\n': /* Must be escaped for logs */
                            buf.append('\\');
                            buf.append('n');
                            break;
                        case '\r':
                            buf.append('\\');
                            buf.append('r');
                            break;
                        case '\\':
                            buf.append('\\');
                            buf.append('\\');
                            break;
                        case '\'':
                            buf.append('\'');
                            buf.append('\'');
                            break;
                        case '"': /* Better safe than sorry */
                            if (this.session.getServerSession().useAnsiQuotedIdentifiers()) {
                                buf.append('\\');
                            }
                            buf.append('"');
                            break;
                        case '\032': /* This gives problems on Win32 */
                            buf.append('\\');
                            buf.append('Z');
                            break;
                        case '\u00a5':
                        case '\u20a9':
                            // escape characters interpreted as backslash by mysql
                            if (this.charsetEncoder != null) {
                                CharBuffer cbuf = CharBuffer.allocate(1);
                                ByteBuffer bbuf = ByteBuffer.allocate(1);
                                cbuf.put(c);
                                cbuf.position(0);
                                this.charsetEncoder.encode(cbuf, bbuf, true);
                                if (bbuf.get(0) == '\\') {
                                    buf.append('\\');
                                }
                            }
                            buf.append(c);
                            break;

                        default:
                            buf.append(c);
                    }
                }

                buf.append('\'');

                parameterAsString = buf.toString();
            }

            byte[] parameterAsBytes = this.isLoadDataQuery ? StringUtils.getBytes(parameterAsString)
                    : (needsQuoted ? StringUtils.getBytesWrapped(parameterAsString, '\'', '\'', this.charEncoding)
                            : StringUtils.getBytes(parameterAsString, this.charEncoding));

            setValue(parameterIndex, parameterAsBytes, MysqlType.VARCHAR);
        }
    }

    private boolean isEscapeNeededForString(String x, int stringLength) {
        boolean needsHexEscape = false;

        for (int i = 0; i < stringLength; ++i) {
            char c = x.charAt(i);

            switch (c) {
                case 0: /* Must be escaped for 'mysql' */
                case '\n': /* Must be escaped for logs */
                case '\r':
                case '\\':
                case '\'':
                case '"': /* Better safe than sorry */
                case '\032': /* This gives problems on Win32 */
                    needsHexEscape = true;
                    break;
            }

            if (needsHexEscape) {
                break; // no need to scan more
            }
        }
        return needsHexEscape;
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            this.tdf = TimeUtil.getSimpleDateFormat(this.tdf, "''HH:mm:ss''", cal, cal != null ? null : this.session.getServerSession().getDefaultTimeZone());
            setValue(parameterIndex, this.tdf.format(x), MysqlType.TIME);
        }
    }

    public void setTime(int parameterIndex, Time x) {
        setTime(parameterIndex, x, null);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) {
        int fractLen = -1;
        if (!this.sendFractionalSeconds.getValue() || !this.session.getServerSession().getCapabilities().serverSupportsFracSecs()) {
            fractLen = 0;
        } else if (this.columnDefinition != null && parameterIndex <= this.columnDefinition.getFields().length && parameterIndex >= 0) {
            fractLen = this.columnDefinition.getFields()[parameterIndex].getDecimals();
        }

        setTimestamp(parameterIndex, x, null, fractLen);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {
        int fractLen = -1;
        if (!this.sendFractionalSeconds.getValue() || !this.session.getServerSession().getCapabilities().serverSupportsFracSecs()) {
            fractLen = 0;
        } else if (this.columnDefinition != null && parameterIndex <= this.columnDefinition.getFields().length && parameterIndex >= 0
                && this.columnDefinition.getFields()[parameterIndex].getDecimals() > 0) {
            fractLen = this.columnDefinition.getFields()[parameterIndex].getDecimals();
        }

        setTimestamp(parameterIndex, x, cal, fractLen);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar targetCalendar, int fractionalLength) {
        if (x == null) {
            setNull(parameterIndex);
        } else {

            x = (Timestamp) x.clone();

            if (!this.session.getServerSession().getCapabilities().serverSupportsFracSecs()
                    || !this.sendFractionalSeconds.getValue() && fractionalLength == 0) {
                x = TimeUtil.truncateFractionalSeconds(x);
            }

            if (fractionalLength < 0) {
                // default to 6 fractional positions
                fractionalLength = 6;
            }

            x = TimeUtil.adjustTimestampNanosPrecision(x, fractionalLength, !this.session.getServerSession().isServerTruncatesFracSecs());

            this.tsdf = TimeUtil.getSimpleDateFormat(this.tsdf, "''yyyy-MM-dd HH:mm:ss", targetCalendar,
                    targetCalendar != null ? null : this.session.getServerSession().getDefaultTimeZone());

            StringBuffer buf = new StringBuffer();
            buf.append(this.tsdf.format(x));
            if (this.session.getServerSession().getCapabilities().serverSupportsFracSecs()) {
                buf.append('.');
                buf.append(TimeUtil.formatNanos(x.getNanos(), 6));
            }
            buf.append('\'');

            setValue(parameterIndex, buf.toString(), MysqlType.TIMESTAMP);
        }
    }

}
