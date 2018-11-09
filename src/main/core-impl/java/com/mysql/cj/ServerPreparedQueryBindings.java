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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

//TODO should not be protocol-specific

public class ServerPreparedQueryBindings extends AbstractQueryBindings<ServerPreparedQueryBindValue> {

    /** Do we need to send/resend types to the server? */
    private AtomicBoolean sendTypesToServer = new AtomicBoolean(false);

    /**
     * Flag indicating whether or not the long parameters have been 'switched' back to normal parameters.
     * We cannot execute() if clearParameters() has not been called in this case.
     */
    private boolean longParameterSwitchDetected = false;

    public ServerPreparedQueryBindings(int parameterCount, Session sess) {
        super(parameterCount, sess);
    }

    @Override
    protected void initBindValues(int parameterCount) {
        this.bindValues = new ServerPreparedQueryBindValue[parameterCount];
        for (int i = 0; i < parameterCount; i++) {
            this.bindValues[i] = new ServerPreparedQueryBindValue(this.session.getServerSession().getDefaultTimeZone());
        }
    }

    @Override
    public ServerPreparedQueryBindings clone() {
        ServerPreparedQueryBindings newBindings = new ServerPreparedQueryBindings(this.bindValues.length, this.session);
        ServerPreparedQueryBindValue[] bvs = new ServerPreparedQueryBindValue[this.bindValues.length];
        for (int i = 0; i < this.bindValues.length; i++) {
            bvs[i] = this.bindValues[i].clone();
        }
        newBindings.bindValues = bvs;
        newBindings.sendTypesToServer = this.sendTypesToServer;
        newBindings.longParameterSwitchDetected = this.longParameterSwitchDetected;
        newBindings.isLoadDataQuery = this.isLoadDataQuery;
        return newBindings;
    }

    /**
     * Returns the structure representing the value that (can be)/(is)
     * bound at the given parameter index.
     * 
     * @param parameterIndex
     *            0-based
     * @param forLongData
     *            is this for a stream?
     * @return ServerPreparedQueryBindValue
     */
    public ServerPreparedQueryBindValue getBinding(int parameterIndex, boolean forLongData) {

        if (this.bindValues[parameterIndex] == null) {
            //            this.bindValues[parameterIndex] = new ServerPreparedQueryBindValue();
        } else {
            if (this.bindValues[parameterIndex].isStream && !forLongData) {
                this.longParameterSwitchDetected = true;
            }
        }

        return this.bindValues[parameterIndex];
    }

    @Override
    public void checkParameterSet(int columnIndex) {
        if (!this.bindValues[columnIndex].isSet()) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("ServerPreparedStatement.13") + (columnIndex + 1) + Messages.getString("ServerPreparedStatement.14"));
        }
    }

    public AtomicBoolean getSendTypesToServer() {
        return this.sendTypesToServer;
    }

    public boolean isLongParameterSwitchDetected() {
        return this.longParameterSwitchDetected;
    }

    public void setLongParameterSwitchDetected(boolean longParameterSwitchDetected) {
        this.longParameterSwitchDetected = longParameterSwitchDetected;
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
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_BLOB, this.numberOfExecutions));
            binding.value = x;
            binding.isStream = true;
            binding.streamLength = this.useStreamLengthsInPrepStmts.getValue() ? length : -1;
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
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_NEWDECIMAL, this.numberOfExecutions));
            binding.value = StringUtils.fixDecimalExponent(x.toPlainString());
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
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_BLOB, this.numberOfExecutions));
            binding.value = x;
            binding.isStream = true;
            binding.streamLength = this.useStreamLengthsInPrepStmts.getValue() ? length : -1;
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
                ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
                this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_BLOB, this.numberOfExecutions));
                binding.value = x;
                binding.isStream = true;
                binding.streamLength = this.useStreamLengthsInPrepStmts.getValue() ? x.length() : -1;
            } catch (Throwable t) {
                throw ExceptionFactory.createException(t.getMessage(), t);
            }
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        setByte(parameterIndex, (x ? (byte) 1 : (byte) 0));
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_TINY, this.numberOfExecutions));
        binding.value = Long.valueOf(x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_VAR_STRING, this.numberOfExecutions));
            binding.value = x;
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x, boolean checkForIntroducer, boolean escapeForMBChars) {
        setBytes(parameterIndex, x);
    }

    @Override
    public void setBytesNoEscape(int parameterIndex, byte[] parameterAsBytes) {
        setBytes(parameterIndex, parameterAsBytes);
    }

    @Override
    public void setBytesNoEscapeNoQuotes(int parameterIndex, byte[] parameterAsBytes) {
        setBytes(parameterIndex, parameterAsBytes);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) {
        setCharacterStream(parameterIndex, reader, -1);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) {
        if (reader == null) {
            setNull(parameterIndex);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_BLOB, this.numberOfExecutions));
            binding.value = reader;
            binding.isStream = true;
            binding.streamLength = this.useStreamLengthsInPrepStmts.getValue() ? length : -1;
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
    public void setClob(int parameterIndex, Clob x) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            try {
                ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
                this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_BLOB, this.numberOfExecutions));
                binding.value = x.getCharacterStream();
                binding.isStream = true;
                binding.streamLength = this.useStreamLengthsInPrepStmts.getValue() ? x.length() : -1;
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
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_DATE, this.numberOfExecutions));
            binding.value = x;
            binding.calendar = cal == null ? null : (Calendar) cal.clone();
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        if (!this.session.getPropertySet().getBooleanProperty(PropertyKey.allowNanAndInf).getValue()
                && (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY || Double.isNaN(x))) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedStatement.64", new Object[] { x }),
                    this.session.getExceptionInterceptor());
        }
        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_DOUBLE, this.numberOfExecutions));
        binding.value = Double.valueOf(x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_FLOAT, this.numberOfExecutions));
        binding.value = Float.valueOf(x);
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_LONG, this.numberOfExecutions));
        binding.value = Long.valueOf(x);
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_LONGLONG, this.numberOfExecutions));
        binding.value = Long.valueOf(x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) {
        setNCharacterStream(parameterIndex, value, -1);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader, long length) {
        if (!this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
            throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.28"), this.session.getExceptionInterceptor());
        }

        if (reader == null) {
            setNull(parameterIndex);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_BLOB, this.numberOfExecutions));
            binding.value = reader;
            binding.isStream = true;
            binding.streamLength = this.useStreamLengthsInPrepStmts.getValue() ? length : -1;
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) {
        setNCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) {
        if (!this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
            throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.29"), this.session.getExceptionInterceptor());
        }
        setNCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) {
        try {
            setNClob(parameterIndex, value.getCharacterStream(), this.useStreamLengthsInPrepStmts.getValue() ? value.length() : -1);
        } catch (Throwable t) {
            throw ExceptionFactory.createException(t.getMessage(), t, this.session.getExceptionInterceptor());
        }
    }

    @Override
    public void setNString(int parameterIndex, String x) {
        if (this.charEncoding.equalsIgnoreCase("UTF-8") || this.charEncoding.equalsIgnoreCase("utf8")) {
            setString(parameterIndex, x);
        } else {
            throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.30"), this.session.getExceptionInterceptor());
        }
    }

    @Override
    public void setNull(int parameterIndex) {
        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_NULL, this.numberOfExecutions));
        binding.setNull(true);
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_SHORT, this.numberOfExecutions));
        binding.value = Long.valueOf(x);
    }

    @Override
    public void setString(int parameterIndex, String x) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_VAR_STRING, this.numberOfExecutions));
            binding.value = x;
        }
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) {
        if (x == null) {
            setNull(parameterIndex);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_TIME, this.numberOfExecutions));
            binding.value = x;
            binding.calendar = cal == null ? null : (Calendar) cal.clone();
        }
    }

    public void setTime(int parameterIndex, Time x) {
        setTime(parameterIndex, x, null); // this.session.getServerSession().getDefaultTimeZone()
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
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlType.FIELD_TYPE_DATETIME, this.numberOfExecutions));

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

            binding.value = x;
            binding.calendar = targetCalendar == null ? null : (Calendar) targetCalendar.clone();
        }
    }
}
