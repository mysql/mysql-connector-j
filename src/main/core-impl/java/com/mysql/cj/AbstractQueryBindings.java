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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Locale;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

//TODO should not be protocol-specific
public abstract class AbstractQueryBindings<T extends BindValue> implements QueryBindings<T> {

    protected final static byte[] HEX_DIGITS = new byte[] { (byte) '0', (byte) '1', (byte) '2', (byte) '3', (byte) '4', (byte) '5', (byte) '6', (byte) '7',
            (byte) '8', (byte) '9', (byte) 'A', (byte) 'B', (byte) 'C', (byte) 'D', (byte) 'E', (byte) 'F' };

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
                if ((this.bindValues[i] != null) && ((ServerPreparedQueryBindValue) this.bindValues[i]).isLongData) { // TODO ServerPreparedQueryBindValue should not be referred here
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

    public synchronized final void setValue(int paramIndex, byte[] val) {
        this.bindValues[paramIndex].setByteValue(val);
    }

    public synchronized final void setValue(int paramIndex, byte[] val, MysqlType type) {
        this.bindValues[paramIndex].setByteValue(val);
        this.bindValues[paramIndex].setMysqlType(type);
    }

    public synchronized final void setValue(int paramIndex, String val) {
        byte[] parameterAsBytes = StringUtils.getBytes(val, this.charEncoding);
        setValue(paramIndex, parameterAsBytes);
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

    @Override
    public void setObject(int parameterIndex, Object parameterObj) {
        if (parameterObj == null) {
            setNull(parameterIndex);
        } else {
            if (parameterObj instanceof Byte) {
                setInt(parameterIndex, ((Byte) parameterObj).intValue());

            } else if (parameterObj instanceof String) {
                setString(parameterIndex, (String) parameterObj);

            } else if (parameterObj instanceof BigDecimal) {
                setBigDecimal(parameterIndex, (BigDecimal) parameterObj);

            } else if (parameterObj instanceof Short) {
                setShort(parameterIndex, ((Short) parameterObj).shortValue());

            } else if (parameterObj instanceof Integer) {
                setInt(parameterIndex, ((Integer) parameterObj).intValue());

            } else if (parameterObj instanceof Long) {
                setLong(parameterIndex, ((Long) parameterObj).longValue());

            } else if (parameterObj instanceof Float) {
                setFloat(parameterIndex, ((Float) parameterObj).floatValue());

            } else if (parameterObj instanceof Double) {
                setDouble(parameterIndex, ((Double) parameterObj).doubleValue());

            } else if (parameterObj instanceof byte[]) {
                setBytes(parameterIndex, (byte[]) parameterObj);

            } else if (parameterObj instanceof java.sql.Date) {
                setDate(parameterIndex, (java.sql.Date) parameterObj);

            } else if (parameterObj instanceof Time) {
                setTime(parameterIndex, (Time) parameterObj);

            } else if (parameterObj instanceof Timestamp) {
                setTimestamp(parameterIndex, (Timestamp) parameterObj);

            } else if (parameterObj instanceof Boolean) {
                setBoolean(parameterIndex, ((Boolean) parameterObj).booleanValue());

            } else if (parameterObj instanceof InputStream) {
                setBinaryStream(parameterIndex, (InputStream) parameterObj, -1);

            } else if (parameterObj instanceof java.sql.Blob) {
                setBlob(parameterIndex, (java.sql.Blob) parameterObj);

            } else if (parameterObj instanceof java.sql.Clob) {
                setClob(parameterIndex, (java.sql.Clob) parameterObj);

            } else if (this.treatUtilDateAsTimestamp.getValue() && parameterObj instanceof java.util.Date) {
                setTimestamp(parameterIndex, new Timestamp(((java.util.Date) parameterObj).getTime()));

            } else if (parameterObj instanceof BigInteger) {
                setString(parameterIndex, parameterObj.toString());

            } else if (parameterObj instanceof LocalDate) {
                setDate(parameterIndex, Date.valueOf((LocalDate) parameterObj));

            } else if (parameterObj instanceof LocalDateTime) {
                setTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime) parameterObj));

            } else if (parameterObj instanceof LocalTime) {
                setTime(parameterIndex, Time.valueOf((LocalTime) parameterObj));

            } else {
                setSerializableObject(parameterIndex, parameterObj);
            }
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
        } else {
            if (parameterObj instanceof LocalDate) {
                parameterObj = Date.valueOf((LocalDate) parameterObj);
            } else if (parameterObj instanceof LocalDateTime) {
                parameterObj = Timestamp.valueOf((LocalDateTime) parameterObj);
            } else if (parameterObj instanceof LocalTime) {
                parameterObj = Time.valueOf((LocalTime) parameterObj);
            }

            try {
                /*
                 * From Table-B5 in the JDBC Spec
                 */
                switch (targetMysqlType) {
                    case BOOLEAN:
                        if (parameterObj instanceof Boolean) {
                            setBoolean(parameterIndex, ((Boolean) parameterObj).booleanValue());
                            break;

                        } else if (parameterObj instanceof String) {
                            setBoolean(parameterIndex, "true".equalsIgnoreCase((String) parameterObj) || !"0".equalsIgnoreCase((String) parameterObj));
                            break;

                        } else if (parameterObj instanceof Number) {
                            int intValue = ((Number) parameterObj).intValue();
                            setBoolean(parameterIndex, intValue != 0);
                            break;

                        } else {
                            throw ExceptionFactory.createException(WrongArgumentException.class,
                                    Messages.getString("PreparedStatement.66", new Object[] { parameterObj.getClass().getName() }),
                                    this.session.getExceptionInterceptor());
                        }

                    case BIT:
                    case TINYINT:
                    case TINYINT_UNSIGNED:
                    case SMALLINT:
                    case SMALLINT_UNSIGNED:
                    case INT:
                    case INT_UNSIGNED:
                    case MEDIUMINT:
                    case MEDIUMINT_UNSIGNED:
                    case BIGINT:
                    case BIGINT_UNSIGNED:
                    case FLOAT:
                    case FLOAT_UNSIGNED:
                    case DOUBLE:
                    case DOUBLE_UNSIGNED:
                    case DECIMAL:
                    case DECIMAL_UNSIGNED:
                        setNumericObject(parameterIndex, parameterObj, targetMysqlType, scaleOrLength);
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

                    case DATE:
                    case DATETIME:
                    case TIMESTAMP:
                    case YEAR:

                        java.util.Date parameterAsDate;

                        if (parameterObj instanceof String) {
                            ParsePosition pp = new ParsePosition(0);
                            java.text.DateFormat sdf = new java.text.SimpleDateFormat(TimeUtil.getDateTimePattern((String) parameterObj, false), Locale.US);
                            parameterAsDate = sdf.parse((String) parameterObj, pp);
                        } else {
                            parameterAsDate = (java.util.Date) parameterObj;
                        }

                        switch (targetMysqlType) {
                            case DATE:

                                if (parameterAsDate instanceof java.sql.Date) {
                                    setDate(parameterIndex, (java.sql.Date) parameterAsDate);
                                } else {
                                    setDate(parameterIndex, new java.sql.Date(parameterAsDate.getTime()));
                                }

                                break;

                            case DATETIME:
                            case TIMESTAMP:

                                if (parameterAsDate instanceof java.sql.Timestamp) {
                                    setTimestamp(parameterIndex, (java.sql.Timestamp) parameterAsDate);
                                } else {
                                    setTimestamp(parameterIndex, new java.sql.Timestamp(parameterAsDate.getTime()));
                                }

                                break;

                            case YEAR:
                                Calendar cal = Calendar.getInstance();
                                cal.setTime(parameterAsDate);
                                setNumericObject(parameterIndex, cal.get(Calendar.YEAR), targetMysqlType, scaleOrLength);
                                break;

                            default:
                                break;
                        }

                        break;

                    case TIME:
                        if (parameterObj instanceof String) {
                            java.text.DateFormat sdf = new java.text.SimpleDateFormat(TimeUtil.getDateTimePattern((String) parameterObj, true), Locale.US);
                            setTime(parameterIndex, new java.sql.Time(sdf.parse((String) parameterObj).getTime()));
                        } else if (parameterObj instanceof Timestamp) {
                            Timestamp xT = (Timestamp) parameterObj;
                            setTime(parameterIndex, new java.sql.Time(xT.getTime()));
                        } else {
                            setTime(parameterIndex, (java.sql.Time) parameterObj);
                        }

                        break;

                    case UNKNOWN:
                        setSerializableObject(parameterIndex, parameterObj);
                        break;

                    default:
                        throw ExceptionFactory.createException(Messages.getString("PreparedStatement.16"), this.session.getExceptionInterceptor());
                }
            } catch (Exception ex) {
                throw ExceptionFactory.createException(
                        Messages.getString("PreparedStatement.17") + parameterObj.getClass().toString() + Messages.getString("PreparedStatement.18")
                                + ex.getClass().getName() + Messages.getString("PreparedStatement.19") + ex.getMessage(),
                        ex, this.session.getExceptionInterceptor());
            }
        }
    }

    private void setNumericObject(int parameterIndex, Object parameterObj, MysqlType targetMysqlType, int scale) {
        Number parameterAsNum;

        if (parameterObj instanceof Boolean) {
            parameterAsNum = ((Boolean) parameterObj).booleanValue() ? Integer.valueOf(1) : Integer.valueOf(0);
        } else if (parameterObj instanceof String) {
            switch (targetMysqlType) {
                case BIT:
                    if ("1".equals(parameterObj) || "0".equals(parameterObj)) {
                        parameterAsNum = Integer.valueOf((String) parameterObj);
                    } else {
                        boolean parameterAsBoolean = "true".equalsIgnoreCase((String) parameterObj);

                        parameterAsNum = parameterAsBoolean ? Integer.valueOf(1) : Integer.valueOf(0);
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
                case YEAR:
                    parameterAsNum = Integer.valueOf((String) parameterObj);
                    break;

                case BIGINT:
                    parameterAsNum = Long.valueOf((String) parameterObj);
                    break;

                case BIGINT_UNSIGNED:
                    parameterAsNum = new BigInteger((String) parameterObj);
                    break;

                case FLOAT:
                case FLOAT_UNSIGNED:
                    parameterAsNum = Float.valueOf((String) parameterObj);

                    break;

                case DOUBLE:
                case DOUBLE_UNSIGNED:
                    parameterAsNum = Double.valueOf((String) parameterObj);
                    break;

                case DECIMAL:
                case DECIMAL_UNSIGNED:
                default:
                    parameterAsNum = new java.math.BigDecimal((String) parameterObj);
            }
        } else {
            parameterAsNum = (Number) parameterObj;
        }

        switch (targetMysqlType) {
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
                if (parameterAsNum instanceof java.math.BigDecimal) {
                    BigDecimal scaledBigDecimal = null;

                    try {
                        scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum).setScale(scale);
                    } catch (ArithmeticException ex) {
                        try {
                            scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum).setScale(scale, BigDecimal.ROUND_HALF_UP);
                        } catch (ArithmeticException arEx) {
                            throw ExceptionFactory.createException(WrongArgumentException.class,
                                    Messages.getString("PreparedStatement.65", new Object[] { scale, parameterAsNum }), this.session.getExceptionInterceptor());
                        }
                    }

                    setBigDecimal(parameterIndex, scaledBigDecimal);
                } else if (parameterAsNum instanceof java.math.BigInteger) {
                    setBigDecimal(parameterIndex, new java.math.BigDecimal((java.math.BigInteger) parameterAsNum, scale));
                } else {
                    setBigDecimal(parameterIndex, new java.math.BigDecimal(parameterAsNum.doubleValue()));
                }

                break;
            default:
                break;
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
}
