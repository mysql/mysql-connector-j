/*
 * Copyright (c) 2021, Oracle and/or its affiliates.
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

/**
 * Exposes protected {@link CharsetMapping} methods for use in tests.
 */
public class CharsetMappingWrapper {

    public static boolean isStaticMultibyteCharset(String javaEncodingName) {
        return CharsetMapping.isStaticMultibyteCharset(javaEncodingName);
    }

    public static int getStaticCollationIndexForJavaEncoding(String javaEncoding, ServerVersion version) {
        return CharsetMapping.getStaticCollationIndexForJavaEncoding(javaEncoding, version);
    }

    public static String getStaticCollationNameForCollationIndex(Integer collationIndex) {
        return CharsetMapping.getStaticCollationNameForCollationIndex(collationIndex);
    }

    public static String getStaticJavaEncodingForCollationIndex(Integer collationIndex) {
        return CharsetMapping.getStaticJavaEncodingForCollationIndex(collationIndex);
    }

    public static String getStaticJavaEncodingForMysqlCharset(String mysqlCharsetName) {
        return CharsetMapping.getStaticJavaEncodingForMysqlCharset(mysqlCharsetName);
    }

    public static String getStaticMysqlCharsetByName(String mysqlCharsetName) {
        Object cs = CharsetMapping.getStaticMysqlCharsetByName(mysqlCharsetName);
        return cs == null ? null : cs.toString();
    }

    public static String getStaticMysqlCharsetForJavaEncoding(String javaEncoding, ServerVersion version) {
        return CharsetMapping.getStaticMysqlCharsetForJavaEncoding(javaEncoding, version);
    }

    public static String getStaticMysqlCharsetNameForCollationIndex(Integer collationIndex) {
        return CharsetMapping.getStaticMysqlCharsetNameForCollationIndex(collationIndex);
    }

}
