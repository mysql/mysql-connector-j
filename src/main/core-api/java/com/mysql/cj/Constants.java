/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

import java.math.BigDecimal;
import java.math.BigInteger;

import com.mysql.cj.conf.PropertyDefinitions;

/**
 * Represents various constants used in the driver.
 */
public class Constants {

    /**
     * Avoids allocation of empty byte[] when representing 0-length strings.
     */
    public final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * I18N'd representation of the abbreviation for "ms"
     */
    public final static String MILLIS_I18N = Messages.getString("Milliseconds");

    public final static byte[] SLASH_STAR_SPACE_AS_BYTES = new byte[] { (byte) '/', (byte) '*', (byte) ' ' };

    public final static byte[] SPACE_STAR_SLASH_SPACE_AS_BYTES = new byte[] { (byte) ' ', (byte) '*', (byte) '/', (byte) ' ' };

    public static final String JVM_VENDOR = System.getProperty(PropertyDefinitions.SYSP_java_vendor);
    public static final String JVM_VERSION = System.getProperty(PropertyDefinitions.SYSP_java_version);

    public static final String OS_NAME = System.getProperty(PropertyDefinitions.SYSP_os_name);
    public static final String OS_ARCH = System.getProperty(PropertyDefinitions.SYSP_os_arch);
    public static final String OS_VERSION = System.getProperty(PropertyDefinitions.SYSP_os_version);

    public static final String CJ_NAME = "@MYSQL_CJ_DISPLAY_PROD_NAME@";
    public static final String CJ_FULL_NAME = "@MYSQL_CJ_FULL_PROD_NAME@";
    public static final String CJ_REVISION = "@MYSQL_CJ_REVISION@";
    public static final String CJ_VERSION = "@MYSQL_CJ_VERSION@";
    public static final String CJ_MAJOR_VERSION = "@MYSQL_CJ_MAJOR_VERSION@";
    public static final String CJ_MINOR_VERSION = "@MYSQL_CJ_MINOR_VERSION@";
    public static final String CJ_LICENSE = "@MYSQL_CJ_LICENSE_TYPE@";

    public static final BigInteger BIG_INTEGER_ZERO = BigInteger.valueOf(0);
    public static final BigInteger BIG_INTEGER_ONE = BigInteger.valueOf(1);
    public static final BigInteger BIG_INTEGER_NEGATIVE_ONE = BigInteger.valueOf(-1);
    public static final BigInteger BIG_INTEGER_MIN_BYTE_VALUE = BigInteger.valueOf(Byte.MIN_VALUE);
    public static final BigInteger BIG_INTEGER_MAX_BYTE_VALUE = BigInteger.valueOf(Byte.MAX_VALUE);
    public static final BigInteger BIG_INTEGER_MIN_SHORT_VALUE = BigInteger.valueOf(Short.MIN_VALUE);
    public static final BigInteger BIG_INTEGER_MAX_SHORT_VALUE = BigInteger.valueOf(Short.MAX_VALUE);
    public static final BigInteger BIG_INTEGER_MIN_INTEGER_VALUE = BigInteger.valueOf(Integer.MIN_VALUE);
    public static final BigInteger BIG_INTEGER_MAX_INTEGER_VALUE = BigInteger.valueOf(Integer.MAX_VALUE);
    public static final BigInteger BIG_INTEGER_MIN_LONG_VALUE = BigInteger.valueOf(Long.MIN_VALUE);
    public static final BigInteger BIG_INTEGER_MAX_LONG_VALUE = BigInteger.valueOf(Long.MAX_VALUE);

    public static final BigDecimal BIG_DECIMAL_ZERO = BigDecimal.valueOf(0);
    public static final BigDecimal BIG_DECIMAL_ONE = BigDecimal.valueOf(1);
    public static final BigDecimal BIG_DECIMAL_NEGATIVE_ONE = BigDecimal.valueOf(-1);
    public static final BigDecimal BIG_DECIMAL_MIN_BYTE_VALUE = BigDecimal.valueOf(Byte.MIN_VALUE);
    public static final BigDecimal BIG_DECIMAL_MAX_BYTE_VALUE = BigDecimal.valueOf(Byte.MAX_VALUE);
    public static final BigDecimal BIG_DECIMAL_MIN_SHORT_VALUE = BigDecimal.valueOf(Short.MIN_VALUE);
    public static final BigDecimal BIG_DECIMAL_MAX_SHORT_VALUE = BigDecimal.valueOf(Short.MAX_VALUE);
    public static final BigDecimal BIG_DECIMAL_MIN_INTEGER_VALUE = BigDecimal.valueOf(Integer.MIN_VALUE);
    public static final BigDecimal BIG_DECIMAL_MAX_INTEGER_VALUE = BigDecimal.valueOf(Integer.MAX_VALUE);
    public static final BigDecimal BIG_DECIMAL_MIN_LONG_VALUE = BigDecimal.valueOf(Long.MIN_VALUE);
    public static final BigDecimal BIG_DECIMAL_MAX_LONG_VALUE = BigDecimal.valueOf(Long.MAX_VALUE);
    public static final BigDecimal BIG_DECIMAL_MAX_DOUBLE_VALUE = BigDecimal.valueOf(Double.MAX_VALUE);
    public static final BigDecimal BIG_DECIMAL_MAX_NEGATIVE_DOUBLE_VALUE = BigDecimal.valueOf(-Double.MAX_VALUE);
    public static final BigDecimal BIG_DECIMAL_MAX_FLOAT_VALUE = BigDecimal.valueOf(Float.MAX_VALUE);
    public static final BigDecimal BIG_DECIMAL_MAX_NEGATIVE_FLOAT_VALUE = BigDecimal.valueOf(-Float.MAX_VALUE);

    public static final int UNSIGNED_BYTE_MAX_VALUE = 255;

    /**
     * Prevents instantiation
     */
    private Constants() {
    }

}
