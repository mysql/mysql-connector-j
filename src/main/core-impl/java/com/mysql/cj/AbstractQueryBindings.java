/*
 * Copyright (c) 2017, 2020, Oracle and/or its affiliates.
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
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParsePosition;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;
import com.mysql.cj.util.Util;

//TODO should not be protocol-specific
public abstract class AbstractQueryBindings<T extends BindValue> implements QueryBindings<T> {

    protected final static byte[] HEX_DIGITS = new byte[] { (byte) '0', (byte) '1', (byte) '2', (byte) '3', (byte) '4', (byte) '5', (byte) '6', (byte) '7',
            (byte) '8', (byte) '9', (byte) 'A', (byte) 'B', (byte) 'C', (byte) 'D', (byte) 'E', (byte) 'F' };

    protected final static LocalDate DEFAULT_DATE = LocalDate.of(1970, 1, 1);
    protected final static LocalTime DEFAULT_TIME = LocalTime.of(0, 0);

    protected Session session;

    /** Bind values for individual fields */
    protected T[] bindValues;

    protected String charEncoding;

    protected int numberOfExecutions = 0;

    protected RuntimeProperty<Boolean> useStreamLengthsInPrepStmts;
    protected RuntimeProperty<Boolean> sendFractionalSeconds;
    private RuntimeProperty<Boolean> treatUtilDateAsTimestamp;

    /** Is this query a LOAD DATA query? */
    protected boolean isLoadDataQuery = false;

    protected ColumnDefinition columnDefinition;

    public AbstractQueryBindings(int parameterCount, Session sess) {
        this.session = sess;
        this.charEncoding = this.session.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue();
        this.sendFractionalSeconds = this.session.getPropertySet().getBooleanProperty(PropertyKey.sendFractionalSeconds);
        this.treatUtilDateAsTimestamp = this.session.getPropertySet().getBooleanProperty(PropertyKey.treatUtilDateAsTimestamp);
        this.useStreamLengthsInPrepStmts = this.session.getPropertySet().getBooleanProperty(PropertyKey.useStreamLengthsInPrepStmts);

        initBindValues(parameterCount);
    }

    protected abstract void initBindValues(int parameterCount);

    @Override
    public abstract AbstractQueryBindings<T> clone();

    @Override
    public void setColumnDefinition(ColumnDefinition colDef) {
        this.columnDefinition = colDef;
    }

    @Override
    public boolean isLoadDataQuery() {
        return this.isLoadDataQuery;
    }

    @Override
    public void setLoadDataQuery(boolean isLoadDataQuery) {
        this.isLoadDataQuery = isLoadDataQuery;
    }

    @Override
    public T[] getBindValues() {
        return this.bindValues;
    }

    @Override
    public void setBindValues(T[] bindValues) {
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

    public abstract void checkParameterSet(int columnIndex);

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

    public synchronized final void setValue(int paramIndex, byte[] val, MysqlType type) {
        this.bindValues[paramIndex].setByteValue(val);
        this.bindValues[paramIndex].setMysqlType(type);
    }

    public synchronized final void setOrigValue(int paramIndex, byte[] val) {
        this.bindValues[paramIndex].setOrigByteValue(val);
    }

    @Override
    public synchronized byte[] getOrigBytes(int parameterIndex) {
        return this.bindValues[parameterIndex].getOrigByteValue();
    }

    public synchronized final void setValue(int paramIndex, String val, MysqlType type) {
        byte[] parameterAsBytes = StringUtils.getBytes(val, this.charEncoding);
        setValue(paramIndex, parameterAsBytes, type);
    }

    /**
     * Used to escape binary data with hex for mb charsets
     * 
     * @param buf
     *            source bytes
     * @param packet
     *            write to this packet
     * @param size
     *            number of bytes to read
     */
    public final void hexEscapeBlock(byte[] buf, NativePacketPayload packet, int size) {
        for (int i = 0; i < size; i++) {
            byte b = buf[i];
            int lowBits = (b & 0xff) / 16;
            int highBits = (b & 0xff) % 16;

            packet.writeInteger(IntegerDataType.INT1, HEX_DIGITS[lowBits]);
            packet.writeInteger(IntegerDataType.INT1, HEX_DIGITS[highBits]);
        }
    }

    static Map<Class<?>, MysqlType> DEFAULT_MYSQL_TYPES = new HashMap<>();
    static {
        DEFAULT_MYSQL_TYPES.put(String.class, MysqlType.VARCHAR);
        DEFAULT_MYSQL_TYPES.put(java.sql.Date.class, MysqlType.DATE);
        DEFAULT_MYSQL_TYPES.put(java.sql.Time.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(java.sql.Timestamp.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(Byte.class, MysqlType.INT);
        DEFAULT_MYSQL_TYPES.put(BigDecimal.class, MysqlType.DECIMAL);
        DEFAULT_MYSQL_TYPES.put(Short.class, MysqlType.SMALLINT);
        DEFAULT_MYSQL_TYPES.put(Integer.class, MysqlType.INT);
        DEFAULT_MYSQL_TYPES.put(Long.class, MysqlType.BIGINT);
        DEFAULT_MYSQL_TYPES.put(Float.class, MysqlType.FLOAT); // TODO check; was Types.FLOAT but should be Types.REAL to map to SQL FLOAT
        DEFAULT_MYSQL_TYPES.put(Double.class, MysqlType.DOUBLE);
        DEFAULT_MYSQL_TYPES.put(byte[].class, MysqlType.BINARY);
        DEFAULT_MYSQL_TYPES.put(Boolean.class, MysqlType.BOOLEAN);
        DEFAULT_MYSQL_TYPES.put(Boolean.class, MysqlType.BOOLEAN);
        DEFAULT_MYSQL_TYPES.put(LocalDate.class, MysqlType.DATE);
        DEFAULT_MYSQL_TYPES.put(LocalTime.class, MysqlType.TIME);
        DEFAULT_MYSQL_TYPES.put(LocalDateTime.class, MysqlType.DATETIME); // TODO default JDBC mapping is TIMESTAMP, see B-4
        DEFAULT_MYSQL_TYPES.put(java.sql.Blob.class, MysqlType.BLOB);
        DEFAULT_MYSQL_TYPES.put(java.sql.Clob.class, MysqlType.TEXT);
        DEFAULT_MYSQL_TYPES.put(BigInteger.class, MysqlType.BIGINT);
        DEFAULT_MYSQL_TYPES.put(java.util.Date.class, MysqlType.TIMESTAMP);
        DEFAULT_MYSQL_TYPES.put(InputStream.class, MysqlType.BLOB);
    }

    @Override
    public void setObject(int parameterIndex, Object parameterObj) {
        if (parameterObj == null) {
            setNull(parameterIndex);
            return;
        }
        MysqlType defaultMysqlType = DEFAULT_MYSQL_TYPES.get(parameterObj.getClass());

        if (defaultMysqlType != null) {
            setObject(parameterIndex, parameterObj, defaultMysqlType);

        } else {
            setSerializableObject(parameterIndex, parameterObj); // TODO maybe default to error?
        }
    }

    @Override
    public void setObject(int parameterIndex, Object parameterObj, MysqlType targetMysqlType) {
        setObject(parameterIndex, parameterObj, targetMysqlType, parameterObj instanceof BigDecimal ? ((BigDecimal) parameterObj).scale() : 0);
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
        /*
         * According to Table-B5 in the JDBC Spec
         */
        try {
            if (parameterObj instanceof LocalDate) {
                switch (targetMysqlType) {
                    case DATE:
                        setLocalDate(parameterIndex, (LocalDate) parameterObj, targetMysqlType);
                        break;
                    case DATETIME: // non-JDBC
                    case TIMESTAMP: // non-JDBC
                        setLocalDateTime(parameterIndex, LocalDateTime.of((LocalDate) parameterObj, DEFAULT_TIME), targetMysqlType);
                        break;
                    case YEAR: // non-JDBC
                        setInt(parameterIndex, ((LocalDate) parameterObj).getYear());
                        break;
                    case CHAR:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                        setString(parameterIndex, parameterObj.toString());
                        break;
                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }

            } else if (parameterObj instanceof LocalTime) {
                switch (targetMysqlType) {
                    case TIME:
                        setLocalTime(parameterIndex, (LocalTime) parameterObj, targetMysqlType);
                        break;
                    case CHAR:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                        setString(parameterIndex, parameterObj.toString());
                        break;
                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }

            } else if (parameterObj instanceof LocalDateTime) {
                switch (targetMysqlType) {
                    case DATE:
                    case DATETIME:
                    case TIMESTAMP:
                    case TIME:
                        setLocalDateTime(parameterIndex, ((LocalDateTime) parameterObj), targetMysqlType);
                        break;
                    case YEAR:
                        setInt(parameterIndex, ((LocalDateTime) parameterObj).getYear());
                        break;
                    case CHAR:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                        setString(parameterIndex, parameterObj.toString().replace('T', ' '));
                        break;
                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }

            } else if (parameterObj instanceof java.sql.Date) {
                switch (targetMysqlType) {
                    case DATE:
                        setDate(parameterIndex, (java.sql.Date) parameterObj);
                        break;
                    case DATETIME:
                    case TIMESTAMP:
                        setTimestamp(parameterIndex, new java.sql.Timestamp(((java.util.Date) parameterObj).getTime()));
                        break;
                    case YEAR:
                        Calendar cal = Calendar.getInstance();
                        cal.setTime((java.util.Date) parameterObj);
                        setInt(parameterIndex, cal.get(Calendar.YEAR));
                        break;
                    case CHAR:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                        setString(parameterIndex, parameterObj.toString());
                        break;
                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }

            } else if (parameterObj instanceof java.sql.Timestamp) {
                switch (targetMysqlType) {
                    case DATE:
                        setDate(parameterIndex, new java.sql.Date(((java.util.Date) parameterObj).getTime()));
                        break;
                    case DATETIME:
                    case TIMESTAMP:
                        setTimestamp(parameterIndex, (java.sql.Timestamp) parameterObj);
                        break;
                    case YEAR:
                        Calendar cal = Calendar.getInstance();
                        cal.setTime((java.util.Date) parameterObj);
                        setInt(parameterIndex, cal.get(Calendar.YEAR));
                        break;
                    case TIME:
                        java.sql.Timestamp xT = (java.sql.Timestamp) parameterObj;
                        setTime(parameterIndex, new java.sql.Time(xT.getTime()));
                        break;
                    case CHAR:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                        setString(parameterIndex, parameterObj.toString());
                        break;
                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }

            } else if (parameterObj instanceof java.sql.Time) {
                switch (targetMysqlType) {
                    case DATE:
                        setDate(parameterIndex, new java.sql.Date(((java.util.Date) parameterObj).getTime()));
                        break;
                    case DATETIME:
                    case TIMESTAMP:
                        setTimestamp(parameterIndex, new java.sql.Timestamp(((java.util.Date) parameterObj).getTime()));
                        break;
                    case YEAR:
                        Calendar cal = Calendar.getInstance();
                        cal.setTime((java.util.Date) parameterObj);
                        setInt(parameterIndex, cal.get(Calendar.YEAR));
                        break;
                    case TIME:
                        setTime(parameterIndex, (java.sql.Time) parameterObj);
                        break;
                    case CHAR:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                        setString(parameterIndex, parameterObj.toString());
                        break;
                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }

            } else if (parameterObj instanceof java.util.Date) {
                if (!this.treatUtilDateAsTimestamp.getValue()) { // TODO is it needed at all?
                    setSerializableObject(parameterIndex, parameterObj);
                    return;
                }
                switch (targetMysqlType) {
                    case DATE:
                        setDate(parameterIndex, new java.sql.Date(((java.util.Date) parameterObj).getTime()));
                        break;
                    case DATETIME:
                    case TIMESTAMP:
                        setTimestamp(parameterIndex, new java.sql.Timestamp(((java.util.Date) parameterObj).getTime()));
                        break;
                    case YEAR:
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(((java.util.Date) parameterObj));
                        setInt(parameterIndex, cal.get(Calendar.YEAR));
                        break;
                    // TODO
                    //case TIME:
                    //    setTime(parameterIndex, (java.sql.Time) parameterObj);
                    //    break;
                    case CHAR:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                        setString(parameterIndex, parameterObj.toString());
                        break;
                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }

            } else if (parameterObj instanceof String) {
                switch (targetMysqlType) {
                    case BOOLEAN:
                        if ("true".equalsIgnoreCase((String) parameterObj) || "Y".equalsIgnoreCase((String) parameterObj)) {
                            setBoolean(parameterIndex, true);
                        } else if ("false".equalsIgnoreCase((String) parameterObj) || "N".equalsIgnoreCase((String) parameterObj)) {
                            setBoolean(parameterIndex, false);
                        } else if (((String) parameterObj).matches("-?\\d+\\.?\\d*")) {
                            setBoolean(parameterIndex, !((String) parameterObj).matches("-?[0]+[.]*[0]*"));
                        } else {
                            throw ExceptionFactory.createException(WrongArgumentException.class,
                                    Messages.getString("PreparedStatement.66", new Object[] { parameterObj }), this.session.getExceptionInterceptor());
                        }
                        break;
                    case BIT:
                        if ("1".equals(parameterObj) || "0".equals(parameterObj)) {
                            setInt(parameterIndex, Integer.valueOf((String) parameterObj).intValue());
                        } else {
                            boolean parameterAsBoolean = "true".equalsIgnoreCase((String) parameterObj);
                            setInt(parameterIndex, parameterAsBoolean ? 1 : 0);
                        }
                        break;

                    case TINYINT:
                    case TINYINT_UNSIGNED:
                    case SMALLINT:
                    case SMALLINT_UNSIGNED:
                    case MEDIUMINT:
                    case MEDIUMINT_UNSIGNED:
                    case INT:
                    case INT_UNSIGNED:
                        //case YEAR:
                        setInt(parameterIndex, Integer.valueOf((String) parameterObj).intValue());
                        break;
                    case BIGINT:
                        setLong(parameterIndex, Long.valueOf((String) parameterObj).longValue());
                        break;
                    case BIGINT_UNSIGNED:
                        setLong(parameterIndex, new BigInteger((String) parameterObj).longValue());
                        break;
                    case FLOAT:
                    case FLOAT_UNSIGNED:
                        setFloat(parameterIndex, Float.valueOf((String) parameterObj).floatValue());
                        break;
                    case DOUBLE:
                    case DOUBLE_UNSIGNED:
                        setDouble(parameterIndex, Double.valueOf((String) parameterObj).doubleValue());
                        break;
                    case DECIMAL:
                    case DECIMAL_UNSIGNED:
                        BigDecimal parameterAsNum = new BigDecimal((String) parameterObj);
                        BigDecimal scaledBigDecimal = null;

                        try {
                            scaledBigDecimal = parameterAsNum.setScale(scaleOrLength);
                        } catch (ArithmeticException ex) {
                            try {
                                scaledBigDecimal = parameterAsNum.setScale(scaleOrLength, BigDecimal.ROUND_HALF_UP);
                            } catch (ArithmeticException arEx) {
                                throw ExceptionFactory.createException(WrongArgumentException.class,
                                        Messages.getString("PreparedStatement.65", new Object[] { scaleOrLength, parameterAsNum }),
                                        this.session.getExceptionInterceptor());
                            }
                        }
                        setBigDecimal(parameterIndex, scaledBigDecimal);
                        break;

                    case CHAR:
                    case ENUM:
                    case SET:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                    case JSON:
                        setString(parameterIndex, parameterObj.toString());
                        break;
                    case BINARY:
                    case GEOMETRY:
                    case VARBINARY:
                    case TINYBLOB:
                    case BLOB:
                    case MEDIUMBLOB:
                    case LONGBLOB:
                        setBytes(parameterIndex, StringUtils.getBytes(parameterObj.toString(), this.charEncoding));
                        break;
                    case DATE:
                    case DATETIME:
                    case TIMESTAMP:
                    case YEAR:
                        ParsePosition pp = new ParsePosition(0);
                        java.text.DateFormat sdf = new java.text.SimpleDateFormat(TimeUtil.getDateTimePattern((String) parameterObj, false), Locale.US);
                        setObject(parameterIndex, sdf.parse((String) parameterObj, pp), targetMysqlType, scaleOrLength);
                        break;
                    case TIME:
                        sdf = new java.text.SimpleDateFormat(TimeUtil.getDateTimePattern((String) parameterObj, true), Locale.US);
                        setTime(parameterIndex, new java.sql.Time(sdf.parse((String) parameterObj).getTime()));
                        break;
                    case UNKNOWN:
                        setSerializableObject(parameterIndex, parameterObj);
                        break;
                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }

            } else if (parameterObj instanceof InputStream) {
                setBinaryStream(parameterIndex, (InputStream) parameterObj, -1);

            } else if (parameterObj instanceof Boolean) {
                switch (targetMysqlType) {
                    case BOOLEAN:
                        setBoolean(parameterIndex, ((Boolean) parameterObj).booleanValue());
                        break;
                    case BIT:
                    case TINYINT:
                    case TINYINT_UNSIGNED:
                    case SMALLINT:
                    case SMALLINT_UNSIGNED:
                    case MEDIUMINT:
                    case MEDIUMINT_UNSIGNED:
                    case INT:
                    case INT_UNSIGNED:
                    case YEAR:
                        setInt(parameterIndex, ((Boolean) parameterObj).booleanValue() ? 1 : 0);
                        break;
                    case BIGINT:
                    case BIGINT_UNSIGNED:
                        setLong(parameterIndex, ((Boolean) parameterObj).booleanValue() ? 1L : 0L);
                        break;
                    case FLOAT:
                    case FLOAT_UNSIGNED:
                        setFloat(parameterIndex, ((Boolean) parameterObj).booleanValue() ? 1f : 0f);
                        break;
                    case DOUBLE:
                    case DOUBLE_UNSIGNED:
                        setDouble(parameterIndex, ((Boolean) parameterObj).booleanValue() ? 1d : 0d);
                        break;
                    case DECIMAL:
                    case DECIMAL_UNSIGNED:
                        setBigDecimal(parameterIndex, new java.math.BigDecimal(((Boolean) parameterObj).booleanValue() ? 1d : 0d));
                        break;
                    case CHAR:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                        setString(parameterIndex, parameterObj.toString());
                        break;
                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }

            } else if (parameterObj instanceof Number) {
                Number parameterAsNum = (Number) parameterObj;
                switch (targetMysqlType) {
                    case BOOLEAN:
                        setBoolean(parameterIndex, parameterAsNum.intValue() != 0);
                        break;
                    case BIT:
                    case TINYINT:
                    case TINYINT_UNSIGNED:
                    case SMALLINT:
                    case SMALLINT_UNSIGNED:
                    case MEDIUMINT:
                    case MEDIUMINT_UNSIGNED:
                    case INT:
                    case INT_UNSIGNED:
                    case YEAR:
                        setInt(parameterIndex, parameterAsNum.intValue());
                        break;
                    case BIGINT:
                    case BIGINT_UNSIGNED:
                        setLong(parameterIndex, parameterAsNum.longValue());
                        break;
                    case FLOAT:
                    case FLOAT_UNSIGNED:
                        setFloat(parameterIndex, parameterAsNum.floatValue());
                        break;
                    case DOUBLE:
                    case DOUBLE_UNSIGNED:
                        setDouble(parameterIndex, parameterAsNum.doubleValue());
                        break;
                    case DECIMAL:
                    case DECIMAL_UNSIGNED:
                        if (parameterAsNum instanceof BigDecimal) {
                            BigDecimal scaledBigDecimal = null;

                            try {
                                scaledBigDecimal = ((BigDecimal) parameterAsNum).setScale(scaleOrLength);
                            } catch (ArithmeticException ex) {
                                try {
                                    scaledBigDecimal = ((BigDecimal) parameterAsNum).setScale(scaleOrLength, BigDecimal.ROUND_HALF_UP);
                                } catch (ArithmeticException arEx) {
                                    throw ExceptionFactory.createException(WrongArgumentException.class,
                                            Messages.getString("PreparedStatement.65", new Object[] { scaleOrLength, parameterAsNum }),
                                            this.session.getExceptionInterceptor());
                                }
                            }

                            setBigDecimal(parameterIndex, scaledBigDecimal);
                        } else if (parameterAsNum instanceof java.math.BigInteger) {
                            setBigDecimal(parameterIndex, new BigDecimal((java.math.BigInteger) parameterAsNum, scaleOrLength));
                        } else {
                            setBigDecimal(parameterIndex, new BigDecimal(parameterAsNum.doubleValue()));
                        }

                        break;
                    case CHAR:
                    case ENUM:
                    case SET:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                    case JSON:
                        if (parameterObj instanceof BigDecimal) {
                            setString(parameterIndex, (StringUtils.fixDecimalExponent(((BigDecimal) parameterObj).toPlainString())));
                        } else {
                            setString(parameterIndex, parameterObj.toString());
                        }
                        break;

                    case BINARY:
                    case GEOMETRY:
                    case VARBINARY:
                    case TINYBLOB:
                    case BLOB:
                    case MEDIUMBLOB:
                    case LONGBLOB:
                        setBytes(parameterIndex, StringUtils.getBytes(parameterObj.toString(), this.charEncoding));
                        break;
                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }

            } else {

                switch (targetMysqlType) {
                    case BOOLEAN:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.66", new Object[] { parameterObj.getClass().getName() }),
                                this.session.getExceptionInterceptor());
                    case CHAR:
                    case ENUM:
                    case SET:
                    case VARCHAR:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT:
                    case JSON:
                        if (parameterObj instanceof BigDecimal) {
                            setString(parameterIndex, (StringUtils.fixDecimalExponent(((BigDecimal) parameterObj).toPlainString())));
                        } else if (parameterObj instanceof java.sql.Clob) {
                            setClob(parameterIndex, (java.sql.Clob) parameterObj);
                        } else {
                            setString(parameterIndex, parameterObj.toString());
                        }
                        break;

                    case BINARY:
                    case GEOMETRY:
                    case VARBINARY:
                    case TINYBLOB:
                    case BLOB:
                    case MEDIUMBLOB:
                    case LONGBLOB:
                        if (parameterObj instanceof byte[]) {
                            setBytes(parameterIndex, (byte[]) parameterObj);
                        } else if (parameterObj instanceof java.sql.Blob) {
                            setBlob(parameterIndex, (java.sql.Blob) parameterObj);
                        } else {
                            setBytes(parameterIndex, StringUtils.getBytes(parameterObj.toString(), this.charEncoding));
                        }

                        break;
                    case UNKNOWN:
                        setSerializableObject(parameterIndex, parameterObj);
                        break;

                    default:
                        throw ExceptionFactory.createException(WrongArgumentException.class,
                                Messages.getString("PreparedStatement.67", new Object[] { parameterObj.getClass().getName(), targetMysqlType.toString() }),
                                this.session.getExceptionInterceptor());
                }
            }
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

    @Override
    public boolean isNull(int parameterIndex) {
        return this.bindValues[parameterIndex].isNull();
    }

    public byte[] getBytesRepresentation(int parameterIndex) {
        if (this.bindValues[parameterIndex].isStream()) {
            return streamToBytes(parameterIndex, this.session.getPropertySet().getBooleanProperty(PropertyKey.useStreamLengthsInPrepStmts).getValue());
        }

        byte[] parameterVal = this.bindValues[parameterIndex].getByteValue();

        if (parameterVal == null) {
            return null;
        }

        return StringUtils.unquoteBytes(parameterVal);
    }

    private byte[] streamConvertBuf = null;

    private final byte[] streamToBytes(int parameterIndex, boolean useLength) {
        InputStream in = this.bindValues[parameterIndex].getStreamValue();
        in.mark(Integer.MAX_VALUE); // we may need to read this same stream several times, so we need to reset it at the end.
        try {
            if (this.streamConvertBuf == null) {
                this.streamConvertBuf = new byte[4096];
            }
            if (this.bindValues[parameterIndex].getStreamLength() == -1) {
                useLength = false;
            }

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

            int bc = useLength ? Util.readBlock(in, this.streamConvertBuf, (int) this.bindValues[parameterIndex].getStreamLength(), null)
                    : Util.readBlock(in, this.streamConvertBuf, null);

            int lengthLeftToRead = (int) this.bindValues[parameterIndex].getStreamLength() - bc;

            while (bc > 0) {
                bytesOut.write(this.streamConvertBuf, 0, bc);

                if (useLength) {
                    bc = Util.readBlock(in, this.streamConvertBuf, lengthLeftToRead, null);

                    if (bc > 0) {
                        lengthLeftToRead -= bc;
                    }
                } else {
                    bc = Util.readBlock(in, this.streamConvertBuf, null);
                }
            }

            return bytesOut.toByteArray();
        } finally {
            try {
                in.reset();
            } catch (IOException e) {
            }
            if (this.session.getPropertySet().getBooleanProperty(PropertyKey.autoClosePStmtStreams).getValue()) {
                try {
                    in.close();
                } catch (IOException ioEx) {
                }

                in = null;
            }
        }
    }
}
