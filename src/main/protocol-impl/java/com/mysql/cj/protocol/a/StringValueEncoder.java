/*
 * Copyright (c) 2022, Oracle and/or its affiliates.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.mysql.cj.BindValue;
import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

public class StringValueEncoder extends AbstractValueEncoder {

    /** Charset encoder used to escape if needed, such as Yen sign in SJIS */
    private CharsetEncoder charsetEncoder;

    @Override
    public void init(PropertySet pset, ServerSession serverSess, ExceptionInterceptor excInterceptor) {
        super.init(pset, serverSess, excInterceptor);
        if (this.serverSession.getCharsetSettings().getRequiresEscapingEncoder()) {
            this.charsetEncoder = Charset.forName(this.charEncoding.getValue()).newEncoder();
        }
    }

    @Override
    public byte[] getBytes(BindValue binding) {

        switch (binding.getMysqlType()) {
            case NULL:
                return StringUtils.getBytes("null");
            case CHAR:
            case ENUM:
            case SET:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case BINARY:
            case GEOMETRY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                String x = (String) binding.getValue();
                if (binding.isNational() && !this.charEncoding.getValue().equalsIgnoreCase("UTF-8") && !this.charEncoding.getValue().equalsIgnoreCase("utf8")) {
                    // Add introducer _utf8 for NATIONAL CHARACTER
                    StringBuilder buf = new StringBuilder((int) (x.length() * 1.1 + 4));
                    buf.append("_utf8");
                    StringUtils.escapeString(buf, x, this.serverSession.useAnsiQuotedIdentifiers(), null);
                    return StringUtils.getBytes(buf.toString(), "UTF-8");
                }

                int stringLength = x.length();

                if (this.serverSession.isNoBackslashEscapesSet()) {
                    // Scan for any nasty chars
                    if (!isEscapeNeededForString(x, stringLength)) {
                        StringBuilder quotedString = new StringBuilder(x.length() + 2);
                        quotedString.append('\'');
                        quotedString.append(x);
                        quotedString.append('\'');
                        StringUtils.getBytes(quotedString.toString(), this.charEncoding.getValue());
                    }
                    return escapeBytesIfNeeded(StringUtils.getBytes(x, this.charEncoding.getValue()));
                }

                if (isEscapeNeededForString(x, stringLength)) {
                    String escString = StringUtils
                            .escapeString(new StringBuilder((int) (x.length() * 1.1)), x, this.serverSession.useAnsiQuotedIdentifiers(), this.charsetEncoder)
                            .toString();
                    return StringUtils.getBytes(escString, this.charEncoding.getValue());
                }

                return StringUtils.getBytesWrapped(x, '\'', '\'', this.charEncoding.getValue());

            default:
                return StringUtils.getBytes(getString(binding), this.charEncoding.getValue());
        }

    }

    @Override
    public String getString(BindValue binding) {
        String x = (String) binding.getValue();
        switch (binding.getMysqlType()) {
            case NULL:
                return "null";
            case BOOLEAN:
            case BIT:
                Boolean b = null;
                if ("true".equalsIgnoreCase(x) || "Y".equalsIgnoreCase(x)) {
                    b = true;
                } else if ("false".equalsIgnoreCase(x) || "N".equalsIgnoreCase(x)) {
                    b = false;
                } else if (x.matches("-?\\d+\\.?\\d*")) {
                    b = !x.matches("-?[0]+[.]*[0]*");
                } else {
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedStatement.66", new Object[] { x }),
                            this.exceptionInterceptor);
                }
                return String.valueOf(b ? 1 : 0);
            case TINYINT:
            case TINYINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT:
            case INT_UNSIGNED:
                return String.valueOf(Integer.valueOf(x).intValue());
            case BIGINT:
                return String.valueOf(Long.valueOf(x).longValue());
            case BIGINT_UNSIGNED:
                return String.valueOf(new BigInteger(x).longValue());
            case FLOAT:
            case FLOAT_UNSIGNED:
                return StringUtils.fixDecimalExponent(Float.toString(Float.valueOf(x)));
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return StringUtils.fixDecimalExponent(Double.toString(Double.valueOf(x).doubleValue()));
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                return getScaled(new BigDecimal(x), binding.getScaleOrLength()).toPlainString();
            case CHAR:
            case ENUM:
            case SET:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case BINARY:
            case GEOMETRY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                StringBuilder sb = new StringBuilder("'");
                sb.append(x);
                sb.append("'");
                return sb.toString();
            case DATE:
                Object dt = TimeUtil.parseToDateTimeObject(x, binding.getMysqlType());
                if (dt instanceof LocalDate) {
                    sb = new StringBuilder("'");
                    sb.append(((LocalDate) dt).format(TimeUtil.DATE_FORMATTER));
                    sb.append("'");
                    return sb.toString();
                } else if (dt instanceof LocalDateTime) {
                    sb = new StringBuilder("'");
                    sb.append(((LocalDateTime) dt).format(TimeUtil.DATE_FORMATTER));
                    sb.append("'");
                    return sb.toString();
                }
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.67", new Object[] { dt.getClass().getName(), binding.getMysqlType().toString() }),
                        this.exceptionInterceptor);
            case DATETIME:
            case TIMESTAMP:
                dt = TimeUtil.parseToDateTimeObject(x, binding.getMysqlType());
                if (dt instanceof LocalDate) {
                    sb = new StringBuilder("'");
                    sb.append(LocalDateTime.of((LocalDate) dt, TimeUtil.DEFAULT_TIME).format(TimeUtil.DATETIME_FORMATTER_WITH_OPTIONAL_MICROS));
                    sb.append("'");
                    return sb.toString();
                } else if (dt instanceof LocalDateTime) {
                    sb = new StringBuilder("'");
                    sb.append(adjustLocalDateTime((LocalDateTime) dt, binding.getField()).format(TimeUtil.DATETIME_FORMATTER_WITH_OPTIONAL_MICROS));
                    sb.append("'");
                    return sb.toString();
                }
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.67", new Object[] { dt.getClass().getName(), binding.getMysqlType().toString() }),
                        this.exceptionInterceptor);
            case TIME:
                dt = TimeUtil.parseToDateTimeObject(x, binding.getMysqlType());
                if (dt instanceof LocalTime) {
                    sb = new StringBuilder("'");
                    sb.append(adjustLocalTime((LocalTime) dt, binding.getField()).format(TimeUtil.TIME_FORMATTER_WITH_OPTIONAL_MICROS));
                    sb.append("'");
                    return sb.toString();
                } else if (dt instanceof LocalDateTime) {
                    sb = new StringBuilder("'");
                    sb.append(adjustLocalTime(((LocalDateTime) dt).toLocalTime(), binding.getField()).format(TimeUtil.TIME_FORMATTER_WITH_OPTIONAL_MICROS));
                    sb.append("'");
                    return sb.toString();
                } else if (dt instanceof Duration) {
                    sb = new StringBuilder("'");
                    sb.append(TimeUtil.getDurationString(adjustDuration((Duration) dt, binding.getField())));
                    sb.append("'");
                    return sb.toString();
                }
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.67", new Object[] { dt.getClass().getName(), binding.getMysqlType().toString() }),
                        this.exceptionInterceptor);
            case YEAR:
                dt = TimeUtil.parseToDateTimeObject(x, binding.getMysqlType());
                if (dt instanceof LocalDate) {
                    return String.valueOf(((LocalDate) dt).getYear());
                } else if (dt instanceof LocalDateTime) {
                    return String.valueOf(((LocalDateTime) dt).getYear());
                }
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.67", new Object[] { dt.getClass().getName(), binding.getMysqlType().toString() }),
                        this.exceptionInterceptor);

            default:
                break;
        }
        throw ExceptionFactory.createException(WrongArgumentException.class,
                Messages.getString("PreparedStatement.67", new Object[] { binding.getValue().getClass().getName(), binding.getMysqlType().toString() }),
                this.exceptionInterceptor);

    }

    @Override
    public void encodeAsQueryAttribute(Message msg, BindValue binding) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
        String x = (String) binding.getValue();
        intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes(x, this.charEncoding.getValue()));
    }

    @Override
    public void encodeAsBinary(Message msg, BindValue binding) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;

        String x = (String) binding.getValue();
        switch (binding.getMysqlType()) {
            case BOOLEAN:
            case BIT:
                Boolean b = null;
                if ("true".equalsIgnoreCase(x) || "Y".equalsIgnoreCase(x)) {
                    b = true;
                } else if ("false".equalsIgnoreCase(x) || "N".equalsIgnoreCase(x)) {
                    b = false;
                } else if (x.matches("-?\\d+\\.?\\d*")) {
                    b = !x.matches("-?[0]+[.]*[0]*");
                } else {
                    throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("PreparedStatement.66", new Object[] { x }),
                            this.exceptionInterceptor);
                }
                intoPacket.writeInteger(IntegerDataType.INT1, Long.valueOf(b ? 1L : 0L));
                return;
            case TINYINT:
            case TINYINT_UNSIGNED:
                intoPacket.writeInteger(IntegerDataType.INT1, Long.valueOf(x));
                return;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                intoPacket.writeInteger(IntegerDataType.INT2, Long.valueOf(x));
                return;
            case INT:
            case INT_UNSIGNED:
                intoPacket.writeInteger(IntegerDataType.INT4, Long.valueOf(x));
                return;
            case BIGINT:
            case BIGINT_UNSIGNED:
                intoPacket.writeInteger(IntegerDataType.INT8, Long.valueOf(x));
                return;
            case FLOAT:
            case FLOAT_UNSIGNED:
                intoPacket.writeInteger(IntegerDataType.INT4, Float.floatToIntBits(Float.valueOf(x)));
                return;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                intoPacket.writeInteger(IntegerDataType.INT8, Double.doubleToLongBits(Double.valueOf(x)));
                return;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                BigDecimal bd = getScaled(new BigDecimal(x), binding.getScaleOrLength());
                intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes(bd.toPlainString(), this.charEncoding.getValue()));
                return;
            case CHAR:
            case ENUM:
            case SET:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case BINARY:
            case GEOMETRY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                if (binding.isNational() && !this.charEncoding.getValue().equalsIgnoreCase("UTF-8") && !this.charEncoding.getValue().equalsIgnoreCase("utf8")) {
                    throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.31"), this.exceptionInterceptor);
                }
                try {
                    intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes(x, this.charEncoding.getValue()));
                } catch (CJException uEE) {
                    throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.31") + this.charEncoding.getValue() + "'", uEE,
                            this.exceptionInterceptor);
                }
                return;
            case DATE:
                Object dt = TimeUtil.parseToDateTimeObject(x, binding.getMysqlType());
                if (dt instanceof LocalDate) {
                    writeDate(msg, InternalDate.from((LocalDate) dt));
                    return;
                } else if (dt instanceof LocalDateTime) {
                    writeDateTime(msg, InternalTimestamp.from((LocalDateTime) dt));
                    return;
                }
                break;
            case DATETIME:
            case TIMESTAMP:
                dt = TimeUtil.parseToDateTimeObject(x, binding.getMysqlType());
                if (dt instanceof LocalDate) {
                    writeDateTime(msg, InternalTimestamp.from((LocalDate) dt));
                    return;
                } else if (dt instanceof LocalDateTime) {
                    writeDateTime(msg, InternalTimestamp.from((LocalDateTime) dt));
                    return;
                }
                break;
            case TIME:
                dt = TimeUtil.parseToDateTimeObject(x, binding.getMysqlType());
                if (dt instanceof LocalTime) {
                    writeTime(msg, InternalTime.from((LocalTime) dt));
                    return;
                } else if (dt instanceof Duration) {
                    writeTime(msg, InternalTime.from(adjustDuration(Duration.ofNanos(((Duration) binding.getValue()).toNanos()), binding.getField())));
                    return;
                }
                break;
            case YEAR:
                dt = TimeUtil.parseToDateTimeObject(x, binding.getMysqlType());
                if (dt instanceof LocalDate) {
                    intoPacket.writeInteger(IntegerDataType.INT4, Long.valueOf(((LocalDate) dt).getYear()));
                    return;
                } else if (dt instanceof LocalDateTime) {
                    intoPacket.writeInteger(IntegerDataType.INT4, Long.valueOf(((LocalDateTime) dt).getYear()));
                    return;
                }
                break;

            default:
                break;
        }
        throw ExceptionFactory.createException(WrongArgumentException.class,
                Messages.getString("PreparedStatement.67", new Object[] { binding.getValue().getClass().getName(), binding.getMysqlType().toString() }),
                this.exceptionInterceptor);
    }

    private boolean isEscapeNeededForString(String x, int stringLength) {
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
                    return true; // no need to scan more
            }
        }
        return false;
    }
}
