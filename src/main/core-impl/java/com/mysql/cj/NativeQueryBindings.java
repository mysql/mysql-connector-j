/*
 * Copyright (c) 2017, 2022, Oracle and/or its affiliates.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.result.Field;
import com.mysql.cj.util.StringUtils;

public class NativeQueryBindings implements QueryBindings {

    private Session session;

    /** Bind values for individual fields */
    private BindValue[] bindValues;

    private int numberOfExecutions = 0;

    /** Is this query a LOAD DATA query? */
    private boolean isLoadDataQuery = false;

    private ColumnDefinition columnDefinition;

    /** Do we need to send/resend types to the server? */
    private AtomicBoolean sendTypesToServer = new AtomicBoolean(false); // specific to ServerPreparedQuery

    private Function<Session, BindValue> bindValueConstructor;
    /**
     * Flag indicating whether or not the long parameters have been 'switched' back to normal parameters.
     * We cannot execute() if clearParameters() has not been called in this case.
     */
    private boolean longParameterSwitchDetected = false; // specific to ServerPreparedQuery

    public NativeQueryBindings(int parameterCount, Session sess, Function<Session, BindValue> bindValueConstructor) {
        this.session = sess;
        this.bindValueConstructor = bindValueConstructor;
        this.bindValues = new BindValue[parameterCount];
        for (int i = 0; i < parameterCount; i++) {
            this.bindValues[i] = bindValueConstructor.apply(this.session);
        }
    }

    @Override
    public QueryBindings clone() {
        NativeQueryBindings newBindings = new NativeQueryBindings(this.bindValues.length, this.session, this.bindValueConstructor);
        BindValue[] bvs = new BindValue[this.bindValues.length];
        for (int i = 0; i < this.bindValues.length; i++) {
            bvs[i] = this.bindValues[i].clone();
        }
        newBindings.setBindValues(bvs);
        newBindings.isLoadDataQuery = this.isLoadDataQuery;
        newBindings.sendTypesToServer.set(this.sendTypesToServer.get());
        newBindings.setLongParameterSwitchDetected(this.isLongParameterSwitchDetected());
        return newBindings;
    }

    @Override
    public void setColumnDefinition(ColumnDefinition colDef) {
        this.columnDefinition = colDef;
    }

    @Override
    public BindValue[] getBindValues() {
        return this.bindValues;
    }

    @Override
    public void setBindValues(BindValue[] bindValues) {
        this.bindValues = bindValues;
    }

    @Override
    public boolean clearBindValues() {
        boolean hadLongData = false;

        if (this.bindValues != null) {
            for (int i = 0; i < this.bindValues.length; i++) {
                if ((this.bindValues[i] != null) && this.bindValues[i].isStream()) {
                    hadLongData = true;
                }
                this.bindValues[i].reset();
            }
        }

        return hadLongData;
    }

    @Override
    public void checkParameterSet(int columnIndex) {
        if (!this.bindValues[columnIndex].isSet()) {
            throw ExceptionFactory.createException(Messages.getString("PreparedStatement.40") + (columnIndex + 1),
                    MysqlErrorNumbers.SQL_STATE_WRONG_NO_OF_PARAMETERS, 0, true, null, this.session.getExceptionInterceptor());
        }
    }

    public void checkAllParametersSet() {
        for (int i = 0; i < this.bindValues.length; i++) {
            checkParameterSet(i);
        }
    }

    public int getNumberOfExecutions() {
        return this.numberOfExecutions;
    }

    public void setNumberOfExecutions(int numberOfExecutions) {
        this.numberOfExecutions = numberOfExecutions;
    }

    public boolean isLongParameterSwitchDetected() {
        return this.longParameterSwitchDetected;
    }

    public void setLongParameterSwitchDetected(boolean longParameterSwitchDetected) {
        this.longParameterSwitchDetected = longParameterSwitchDetected;
    }

    public AtomicBoolean getSendTypesToServer() {
        return this.sendTypesToServer;
    }

    /**
     * Returns the structure representing the value that (can be)/(is)
     * bound at the given parameter index.
     * 
     * @param parameterIndex
     *            0-based
     * @param forLongData
     *            is this for a stream?
     * @return BindValue
     */
    public BindValue getBinding(int parameterIndex, boolean forLongData) {
        if (this.bindValues[parameterIndex] != null && this.bindValues[parameterIndex].isStream() && !forLongData) {
            this.longParameterSwitchDetected = true;
        }
        return this.bindValues[parameterIndex];
    }

    public void setFromBindValue(int parameterIndex, BindValue bv) {
        BindValue binding = getBinding(parameterIndex, false);
        binding.setBinding(bv.getValue(), bv.getMysqlType(), this.numberOfExecutions, this.sendTypesToServer);
        binding.setKeepOrigNanos(bv.keepOrigNanos());
        binding.setCalendar(bv.getCalendar());
        binding.setEscapeBytesIfNeeded(bv.escapeBytesIfNeeded());
        binding.setIsNational(bv.isNational());
        binding.setField(bv.getField());
        binding.setScaleOrLength(bv.getScaleOrLength());
    }

    static Map<Class<?>, MysqlType> DEFAULT_MYSQL_TYPES = new HashMap<>();
    static {
        DEFAULT_MYSQL_TYPES.put(BigDecimal.class, MysqlType.DECIMAL);
        DEFAULT_MYSQL_TYPES.put(BigInteger.class, MysqlType.BIGINT);
        DEFAULT_MYSQL_TYPES.put(Blob.class, MysqlType.BLOB);
        DEFAULT_MYSQL_TYPES.put(Boolean.class, MysqlType.BOOLEAN);
        DEFAULT_MYSQL_TYPES.put(Byte.class, MysqlType.TINYINT);
        DEFAULT_MYSQL_TYPES.put(byte[].class, MysqlType.BINARY);
        DEFAULT_MYSQL_TYPES.put(Calendar.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(Clob.class, MysqlType.TEXT);
        DEFAULT_MYSQL_TYPES.put(Date.class, MysqlType.DATE);
        DEFAULT_MYSQL_TYPES.put(java.util.Date.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(Double.class, MysqlType.DOUBLE);
        DEFAULT_MYSQL_TYPES.put(Duration.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(Float.class, MysqlType.FLOAT);
        DEFAULT_MYSQL_TYPES.put(InputStream.class, MysqlType.BLOB);
        DEFAULT_MYSQL_TYPES.put(Instant.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(Integer.class, MysqlType.INT);
        DEFAULT_MYSQL_TYPES.put(LocalDate.class, MysqlType.DATE);
        DEFAULT_MYSQL_TYPES.put(LocalDateTime.class, MysqlType.DATETIME); // default JDBC mapping is TIMESTAMP, see B-4
        DEFAULT_MYSQL_TYPES.put(LocalTime.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(Long.class, MysqlType.BIGINT);
        DEFAULT_MYSQL_TYPES.put(OffsetDateTime.class, MysqlType.TIMESTAMP); // default JDBC mapping is TIMESTAMP_WITH_TIMEZONE, see B-4
        DEFAULT_MYSQL_TYPES.put(OffsetTime.class, MysqlType.TIME); // default JDBC mapping is TIME_WITH_TIMEZONE, see B-4
        DEFAULT_MYSQL_TYPES.put(Reader.class, MysqlType.TEXT);
        DEFAULT_MYSQL_TYPES.put(Short.class, MysqlType.SMALLINT);
        DEFAULT_MYSQL_TYPES.put(String.class, MysqlType.VARCHAR);
        DEFAULT_MYSQL_TYPES.put(Time.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(Timestamp.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(ZonedDateTime.class, MysqlType.TIMESTAMP); // no JDBC mapping is defined
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        BindValue binding = getBinding(parameterIndex, true);
        binding.setBinding(x, MysqlType.TEXT, this.numberOfExecutions, this.sendTypesToServer); // TODO use length to find right TEXT type
        binding.setScaleOrLength(length);

    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        getBinding(parameterIndex, false).setBinding(x, MysqlType.DECIMAL, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public void setBigInteger(int parameterIndex, BigInteger x) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        getBinding(parameterIndex, false).setBinding(x, MysqlType.BIGINT_UNSIGNED, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        BindValue binding = getBinding(parameterIndex, true);
        binding.setBinding(x, MysqlType.BLOB, this.numberOfExecutions, this.sendTypesToServer); // TODO use length to find the right BLOB type
        binding.setScaleOrLength(length);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        BindValue binding = getBinding(parameterIndex, false);
        binding.setBinding(x, MysqlType.BLOB, this.numberOfExecutions, this.sendTypesToServer);
        binding.setScaleOrLength(-1);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        getBinding(parameterIndex, false).setBinding(x, MysqlType.BOOLEAN, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        getBinding(parameterIndex, false).setBinding(x, MysqlType.TINYINT, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x, boolean escapeIfNeeded) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        BindValue binding = getBinding(parameterIndex, false);
        binding.setBinding(x, MysqlType.BINARY, this.numberOfExecutions, this.sendTypesToServer); // TODO VARBINARY ?
        binding.setEscapeBytesIfNeeded(escapeIfNeeded);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) {
        if (reader == null) {
            setNull(parameterIndex);
            return;
        }
        BindValue binding = getBinding(parameterIndex, true);
        binding.setBinding(reader, MysqlType.TEXT, this.numberOfExecutions, this.sendTypesToServer);
        binding.setScaleOrLength(length);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        BindValue binding = getBinding(parameterIndex, false);
        binding.setBinding(x, MysqlType.TEXT, this.numberOfExecutions, this.sendTypesToServer);
        binding.setScaleOrLength(-1);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        BindValue binding = getBinding(parameterIndex, false);
        binding.setBinding(x, MysqlType.DATE, this.numberOfExecutions, this.sendTypesToServer);
        binding.setCalendar(cal == null ? null : (Calendar) cal.clone());
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        if (!this.session.getPropertySet().getBooleanProperty(PropertyKey.allowNanAndInf).getValue()
                && (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY || Double.isNaN(x))) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedStatement.64", new Object[] { x }),
                    this.session.getExceptionInterceptor());
        }
        getBinding(parameterIndex, false).setBinding(x, MysqlType.DOUBLE, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        getBinding(parameterIndex, false).setBinding(x, MysqlType.FLOAT, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        getBinding(parameterIndex, false).setBinding(x, MysqlType.INT, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        getBinding(parameterIndex, false).setBinding(x, MysqlType.BIGINT, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader, long length) {
        if (reader == null) {
            setNull(parameterIndex);
            return;
        }
        BindValue binding = getBinding(parameterIndex, true);
        binding.setBinding(reader, MysqlType.TEXT, this.numberOfExecutions, this.sendTypesToServer);
        binding.setScaleOrLength(length);
        binding.setIsNational(true);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) {
        if (value == null) {
            setNull(parameterIndex);
            return;
        }
        try {
            setNCharacterStream(parameterIndex, value.getCharacterStream(), value.length());
        } catch (Throwable t) {
            throw ExceptionFactory.createException(t.getMessage(), t, this.session.getExceptionInterceptor());
        }
    }

    @Override
    public void setNString(int parameterIndex, String x) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        BindValue binding = getBinding(parameterIndex, false);
        binding.setBinding(x, MysqlType.VARCHAR, this.numberOfExecutions, this.sendTypesToServer);
        binding.setIsNational(true);
    }

    @Override
    public synchronized void setNull(int parameterIndex) {
        BindValue binding = getBinding(parameterIndex, false);
        binding.setBinding(null, MysqlType.NULL, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public boolean isNull(int parameterIndex) {
        return this.bindValues[parameterIndex].isNull();
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        getBinding(parameterIndex, false).setBinding(x, MysqlType.SMALLINT, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public void setString(int parameterIndex, String x) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        getBinding(parameterIndex, false).setBinding(x, MysqlType.VARCHAR, this.numberOfExecutions, this.sendTypesToServer);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) {
        if (x == null) {
            setNull(parameterIndex);
            return;
        }
        BindValue binding = getBinding(parameterIndex, false);
        binding.setBinding(x, MysqlType.TIME, this.numberOfExecutions, this.sendTypesToServer);
        binding.setCalendar(cal == null ? null : (Calendar) cal.clone());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar targetCalendar, Field field, MysqlType targetMysqlType) {

        if (x == null) {
            setNull(parameterIndex);
            return;
        }

        if (field == null) {
            if (this.columnDefinition != null && parameterIndex <= this.columnDefinition.getFields().length && parameterIndex >= 0
                    && this.columnDefinition.getFields()[parameterIndex].getDecimals() > 0) {
                field = this.columnDefinition.getFields()[parameterIndex];
            }
        }

        BindValue binding = getBinding(parameterIndex, false);
        if (field == null) {
            binding.setField(field);
        }
        binding.setBinding(x, targetMysqlType, this.numberOfExecutions, this.sendTypesToServer);
        binding.setCalendar(targetCalendar == null ? null : (Calendar) targetCalendar.clone());
    }

    @Override
    public void setObject(int parameterIndex, Object parameterObj) {
        if (parameterObj == null) {
            setNull(parameterIndex);
            return;
        }
        MysqlType defaultMysqlType = DEFAULT_MYSQL_TYPES.get(parameterObj.getClass());
        if (defaultMysqlType == null) {
            Optional<MysqlType> mysqlType = DEFAULT_MYSQL_TYPES.entrySet().stream().filter(m -> m.getKey().isAssignableFrom(parameterObj.getClass()))
                    .map(m -> m.getValue()).findFirst();
            if (mysqlType.isPresent()) {
                defaultMysqlType = mysqlType.get();
            }
        }
        setObject(parameterIndex, parameterObj, defaultMysqlType, -1);
    }

    /**
     * Set the value of a parameter using an object; use the java.lang equivalent objects for integral values.
     * 
     * <P>
     * The given Java object will be converted to the targetMysqlType before being sent to the database.
     * 
     * @param parameterIndex
     *            the first parameter is 1...
     * @param parameterObj
     *            the object containing the input parameter value
     * @param targetMysqlType
     *            The MysqlType to be send to the database
     * @param scaleOrLength
     *            For Types.DECIMAL or Types.NUMERIC types
     *            this is the number of digits after the decimal. For all other
     *            types this value will be ignored.
     */
    public void setObject(int parameterIndex, Object parameterObj, MysqlType targetMysqlType, int scaleOrLength) {
        if (parameterObj == null) {
            setNull(parameterIndex);
            return;
        }

        try {
            if (targetMysqlType == null || targetMysqlType == MysqlType.UNKNOWN || parameterObj instanceof java.util.Date
                    && !this.session.getPropertySet().getBooleanProperty(PropertyKey.treatUtilDateAsTimestamp).getValue()) {
                setSerializableObject(parameterIndex, parameterObj);
                return;
            }

            BindValue binding = getBinding(parameterIndex, false);
            if (this.columnDefinition != null && parameterIndex <= this.columnDefinition.getFields().length && parameterIndex >= 0) {
                // use the column definition if available
                binding.setField(this.columnDefinition.getFields()[parameterIndex]);
            }
            binding.setBinding(parameterObj, targetMysqlType, this.numberOfExecutions, this.sendTypesToServer);
            binding.setScaleOrLength(scaleOrLength);

        } catch (Exception ex) {
            throw ExceptionFactory.createException(
                    Messages.getString("PreparedStatement.17") + parameterObj.getClass().toString() + Messages.getString("PreparedStatement.18")
                            + ex.getClass().getName() + Messages.getString("PreparedStatement.19") + ex.getMessage(),
                    ex, this.session.getExceptionInterceptor());
        }
    }

    /**
     * Sets the value for the placeholder as a serialized Java object (used by various forms of setObject()
     * 
     * @param parameterIndex
     *            parameter index
     * @param parameterObj
     *            value
     */
    protected final void setSerializableObject(int parameterIndex, Object parameterObj) {
        try {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
            objectOut.writeObject(parameterObj);
            objectOut.flush();
            objectOut.close();
            bytesOut.flush();
            bytesOut.close();

            byte[] buf = bytesOut.toByteArray();
            ByteArrayInputStream bytesIn = new ByteArrayInputStream(buf);
            setBinaryStream(parameterIndex, bytesIn, buf.length);
            this.bindValues[parameterIndex].setMysqlType(MysqlType.BINARY);
        } catch (Exception ex) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedStatement.54") + ex.getClass().getName(), ex,
                    this.session.getExceptionInterceptor());
        }
    }

    public byte[] getBytesRepresentation(int parameterIndex) {
        byte[] parameterVal = this.bindValues[parameterIndex].getByteValue();
        return parameterVal == null ? null : this.bindValues[parameterIndex].isStream() ? parameterVal : StringUtils.unquoteBytes(parameterVal);
    }
}
