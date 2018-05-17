/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.result;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.mysql.cj.util.StringUtils;

/**
 * A {@link com.mysql.cj.result.ValueFactory} implementation to create strings.
 */
public class StringValueFactory implements ValueFactory<String> {
    private String encoding;

    public StringValueFactory() {
    }

    public StringValueFactory(String encoding) {
        this.encoding = encoding;
    }

    /**
     * Create a string from date fields. The fields are formatted in a YYYY-mm-dd format. A point-in-time is not calculated.
     * 
     * @param year
     *            year
     * @param month
     *            month
     * @param day
     *            day
     * @return string
     */
    public String createFromDate(int year, int month, int day) {
        // essentially the same string we received from the server, no TZ interpretation
        return String.format("%04d-%02d-%02d", year, month, day);
    }

    /**
     * Create a string from time fields. The fields are formatted in a HH:MM:SS[.nnnnnnnnn] format. A point-in-time is not calculated.
     * 
     * @param hours
     *            hours
     * @param minutes
     *            minutes
     * @param seconds
     *            seconds
     * @param nanos
     *            nanoseconds
     * @return string
     */
    public String createFromTime(int hours, int minutes, int seconds, int nanos) {
        if (nanos > 0) {
            return String.format("%02d:%02d:%02d.%09d", hours, minutes, seconds, nanos);
        }
        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    /**
     * Create a string from time fields. The fields are formatted by concatening the result of {@link #createFromDate(int,int,int)} and {@link
     * #createFromTime(int,int,int,int)}. A point-in-time is not calculated.
     * 
     * @param year
     *            year
     * @param month
     *            month
     * @param day
     *            day
     * @param hours
     *            hours
     * @param minutes
     *            minutes
     * @param seconds
     *            seconds
     * @param nanos
     *            nanoseconds
     * @return string
     */
    public String createFromTimestamp(int year, int month, int day, int hours, int minutes, int seconds, int nanos) {
        return String.format("%s %s", createFromDate(year, month, day), createFromTime(hours, minutes, seconds, nanos));
    }

    public String createFromLong(long l) {
        return String.valueOf(l);
    }

    public String createFromBigInteger(BigInteger i) {
        return i.toString();
    }

    public String createFromDouble(double d) {
        return String.valueOf(d);
    }

    public String createFromBigDecimal(BigDecimal d) {
        return d.toString();
    }

    /**
     * Interpret the given byte array as a string. This value factory needs to know the encoding to interpret the string. The default (null) will interpet the
     * byte array using the platform encoding.
     * 
     * @param bytes
     *            byte array
     * @param offset
     *            offset
     * @param length
     *            data length in bytes
     * @return string
     */
    public String createFromBytes(byte[] bytes, int offset, int length) {
        return StringUtils.toString(bytes, offset, length, this.encoding);
    }

    @Override
    public String createFromBit(byte[] bytes, int offset, int length) {
        // BIT values are interpreted as numbers, not as character codes
        return new BigInteger(ByteBuffer.allocate(length + 1).put((byte) 0).put(bytes, offset, length).array()).toString();
    }

    public String createFromNull() {
        return null;
    }

    public String getTargetTypeName() {
        return String.class.getName();
    }
}
