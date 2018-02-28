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

import java.util.Calendar;
import java.util.TimeZone;

import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;

//TODO should not be protocol-specific

public class ServerPreparedQueryBindValue extends ClientPreparedQueryBindValue implements BindValue {

    public long boundBeforeExecutionNum = 0;

    /** Default length of data */
    public long bindLength;

    public int bufferType;

    public double doubleBinding;

    public float floatBinding;

    public boolean isLongData;

    /** has this parameter been set? */
    private boolean isSet = false;

    /** all integral values are stored here */
    public long longBinding;

    /** The value to store */
    public Object value;

    /** The TimeZone for date/time types */
    public TimeZone tz;

    public ServerPreparedQueryBindValue() {
    }

    @Override
    public ServerPreparedQueryBindValue clone() {
        return new ServerPreparedQueryBindValue(this);
    }

    private ServerPreparedQueryBindValue(ServerPreparedQueryBindValue copyMe) {
        super(copyMe);

        this.value = copyMe.value;
        this.isSet = copyMe.isSet;
        this.isLongData = copyMe.isLongData;
        this.bufferType = copyMe.bufferType;
        this.bindLength = copyMe.bindLength;
        this.longBinding = copyMe.longBinding;
        this.floatBinding = copyMe.floatBinding;
        this.doubleBinding = copyMe.doubleBinding;
        this.tz = copyMe.tz;
    }

    @Override
    public void reset() {
        super.reset();

        this.isSet = false;
        this.value = null;
        this.isLongData = false;

        this.longBinding = 0L;
        this.floatBinding = 0;
        this.doubleBinding = 0D;
        this.tz = null;
    }

    /**
     * Reset a bind value to be used for a new value of the given type.
     * 
     * @param bufType
     * @param numberOfExecutions
     * @return true if we need to send/resend types to the server
     */
    public boolean resetToType(int bufType, long numberOfExecutions) {
        boolean sendTypesToServer = false;

        // clear any possible old value
        reset();

        if (bufType == MysqlType.FIELD_TYPE_NULL && this.bufferType != 0) {
            // preserve the previous type to (possibly) avoid sending types at execution time
        } else if (this.bufferType != bufType) {
            sendTypesToServer = true;
            this.bufferType = bufType;
        }

        // setup bind value for use
        this.isSet = true;
        this.boundBeforeExecutionNum = numberOfExecutions;
        return sendTypesToServer;
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean quoteIfNeeded) {
        if (this.isLongData) {
            return "' STREAM DATA '";
        }

        if (this.isNull) {
            return "NULL";
        }

        switch (this.bufferType) {
            case MysqlType.FIELD_TYPE_TINY:
            case MysqlType.FIELD_TYPE_SHORT:
            case MysqlType.FIELD_TYPE_LONG:
            case MysqlType.FIELD_TYPE_LONGLONG:
                return String.valueOf(this.longBinding);
            case MysqlType.FIELD_TYPE_FLOAT:
                return String.valueOf(this.floatBinding);
            case MysqlType.FIELD_TYPE_DOUBLE:
                return String.valueOf(this.doubleBinding);
            case MysqlType.FIELD_TYPE_TIME:
            case MysqlType.FIELD_TYPE_DATE:
            case MysqlType.FIELD_TYPE_DATETIME:
            case MysqlType.FIELD_TYPE_TIMESTAMP:
            case MysqlType.FIELD_TYPE_VAR_STRING:
            case MysqlType.FIELD_TYPE_STRING:
            case MysqlType.FIELD_TYPE_VARCHAR:
                if (quoteIfNeeded) {
                    return "'" + String.valueOf(this.value) + "'";
                }
                return String.valueOf(this.value);

            default:
                if (this.value instanceof byte[]) {
                    return "byte data";
                }
                if (quoteIfNeeded) {
                    return "'" + String.valueOf(this.value) + "'";
                }
                return String.valueOf(this.value);
        }
    }

    public long getBoundLength() {
        if (this.isNull) {
            return 0;
        }

        if (this.isLongData) {
            return this.bindLength;
        }

        switch (this.bufferType) {

            case MysqlType.FIELD_TYPE_TINY:
                return 1;
            case MysqlType.FIELD_TYPE_SHORT:
                return 2;
            case MysqlType.FIELD_TYPE_LONG:
                return 4;
            case MysqlType.FIELD_TYPE_LONGLONG:
                return 8;
            case MysqlType.FIELD_TYPE_FLOAT:
                return 4;
            case MysqlType.FIELD_TYPE_DOUBLE:
                return 8;
            case MysqlType.FIELD_TYPE_TIME:
                return 9;
            case MysqlType.FIELD_TYPE_DATE:
                return 7;
            case MysqlType.FIELD_TYPE_DATETIME:
            case MysqlType.FIELD_TYPE_TIMESTAMP:
                return 11;
            case MysqlType.FIELD_TYPE_VAR_STRING:
            case MysqlType.FIELD_TYPE_STRING:
            case MysqlType.FIELD_TYPE_VARCHAR:
            case MysqlType.FIELD_TYPE_DECIMAL:
            case MysqlType.FIELD_TYPE_NEWDECIMAL:
                if (this.value instanceof byte[]) {
                    return ((byte[]) this.value).length;
                }
                return ((String) this.value).length();

            default:
                return 0;
        }
    }

    @Override
    public boolean isSet() {
        return this.isSet;
    }

    public void storeBinding(NativePacketPayload intoPacket, boolean isLoadDataQuery, String characterEncoding, ExceptionInterceptor interceptor) {
        synchronized (this) {
            try {
                // Handle primitives first
                switch (this.bufferType) {

                    case MysqlType.FIELD_TYPE_TINY:
                        intoPacket.writeInteger(IntegerDataType.INT1, this.longBinding);
                        return;
                    case MysqlType.FIELD_TYPE_SHORT:
                        intoPacket.writeInteger(IntegerDataType.INT2, this.longBinding);
                        return;
                    case MysqlType.FIELD_TYPE_LONG:
                        intoPacket.writeInteger(IntegerDataType.INT4, this.longBinding);
                        return;
                    case MysqlType.FIELD_TYPE_LONGLONG:
                        intoPacket.writeInteger(IntegerDataType.INT8, this.longBinding);
                        return;
                    case MysqlType.FIELD_TYPE_FLOAT:
                        intoPacket.writeInteger(IntegerDataType.INT4, Float.floatToIntBits(this.floatBinding));
                        return;
                    case MysqlType.FIELD_TYPE_DOUBLE:
                        intoPacket.writeInteger(IntegerDataType.INT8, Double.doubleToLongBits(this.doubleBinding));
                        return;
                    case MysqlType.FIELD_TYPE_TIME:
                        storeTime(intoPacket);
                        return;
                    case MysqlType.FIELD_TYPE_DATE:
                    case MysqlType.FIELD_TYPE_DATETIME:
                    case MysqlType.FIELD_TYPE_TIMESTAMP:
                        storeDateTime(intoPacket);
                        return;
                    case MysqlType.FIELD_TYPE_VAR_STRING:
                    case MysqlType.FIELD_TYPE_STRING:
                    case MysqlType.FIELD_TYPE_VARCHAR:
                    case MysqlType.FIELD_TYPE_DECIMAL:
                    case MysqlType.FIELD_TYPE_NEWDECIMAL:
                        if (this.value instanceof byte[]) {
                            intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, (byte[]) this.value);
                        } else if (!isLoadDataQuery) {
                            intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes((String) this.value, characterEncoding));
                        } else {
                            intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes((String) this.value));
                        }

                        return;
                }

            } catch (CJException uEE) {
                throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.22") + characterEncoding + "'", uEE, interceptor);
            }
        }
    }

    private void storeTime(NativePacketPayload intoPacket) {

        intoPacket.ensureCapacity(9);
        intoPacket.writeInteger(IntegerDataType.INT1, 8); // length
        intoPacket.writeInteger(IntegerDataType.INT1, 0); // neg flag
        intoPacket.writeInteger(IntegerDataType.INT4, 0); // tm->day, not used

        Calendar cal = Calendar.getInstance(this.tz);

        cal.setTime((java.util.Date) this.value);
        intoPacket.writeInteger(IntegerDataType.INT1, cal.get(Calendar.HOUR_OF_DAY));
        intoPacket.writeInteger(IntegerDataType.INT1, cal.get(Calendar.MINUTE));
        intoPacket.writeInteger(IntegerDataType.INT1, cal.get(Calendar.SECOND));
    }

    /**
     * @param intoPacket
     * @param dt
     * @param mysql
     */
    private void storeDateTime(NativePacketPayload intoPacket) {
        synchronized (this) {
            Calendar cal = Calendar.getInstance(this.tz);

            cal.setTime((java.util.Date) this.value);

            if (this.value instanceof java.sql.Date) {
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
            }

            byte length = (byte) 7;

            if (this.value instanceof java.sql.Timestamp) {
                length = (byte) 11;
            }

            intoPacket.ensureCapacity(length);

            intoPacket.writeInteger(IntegerDataType.INT1, length); // length

            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONTH) + 1;
            int date = cal.get(Calendar.DAY_OF_MONTH);

            intoPacket.writeInteger(IntegerDataType.INT2, year);
            intoPacket.writeInteger(IntegerDataType.INT1, month);
            intoPacket.writeInteger(IntegerDataType.INT1, date);

            if (this.value instanceof java.sql.Date) {
                intoPacket.writeInteger(IntegerDataType.INT1, 0);
                intoPacket.writeInteger(IntegerDataType.INT1, 0);
                intoPacket.writeInteger(IntegerDataType.INT1, 0);
            } else {
                intoPacket.writeInteger(IntegerDataType.INT1, cal.get(Calendar.HOUR_OF_DAY));
                intoPacket.writeInteger(IntegerDataType.INT1, cal.get(Calendar.MINUTE));
                intoPacket.writeInteger(IntegerDataType.INT1, cal.get(Calendar.SECOND));
            }

            if (length == 11) {
                //  MySQL expects microseconds, not nanos
                intoPacket.writeInteger(IntegerDataType.INT4, ((java.sql.Timestamp) this.value).getNanos() / 1000);
            }
        }
    }
}
