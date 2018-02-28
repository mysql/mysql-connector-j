/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
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
    public static final String PLATFORM_ENCODING = System.getProperty(PropertyDefinitions.SYSP_file_encoding);

    public static final String CJ_NAME = "@MYSQL_CJ_DISPLAY_PROD_NAME@";
    public static final String CJ_FULL_NAME = "@MYSQL_CJ_FULL_PROD_NAME@";
    public static final String CJ_REVISION = "@MYSQL_CJ_REVISION@";
    public static final String CJ_VERSION = "@MYSQL_CJ_VERSION@";
    public static final String CJ_MAJOR_VERSION = "@MYSQL_CJ_MAJOR_VERSION@";
    public static final String CJ_MINOR_VERSION = "@MYSQL_CJ_MINOR_VERSION@";
    public static final String CJ_LICENSE = "@MYSQL_CJ_LICENSE_TYPE@";

    /**
     * Prevents instantiation
     */
    private Constants() {
    }
}
