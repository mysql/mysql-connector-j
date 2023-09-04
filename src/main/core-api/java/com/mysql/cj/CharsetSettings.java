/*
 * Copyright (c) 2021, 2023, Oracle and/or its affiliates.
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

public interface CharsetSettings {

    public static final String CHARACTER_SET_CLIENT = "character_set_client";
    public static final String CHARACTER_SET_CONNECTION = "character_set_connection";
    public static final String CHARACTER_SET_RESULTS = "character_set_results";
    public static final String COLLATION_CONNECTION = "collation_connection";

    /**
     * <p>
     * Choose the MySQL collation index for the handshake packet and the corresponding Java encodings for the password and error messages.
     * </p>
     * <p>
     * This index will be sent with HandshakeResponse setting server variables 'character_set_connection', 'collation_connection', 'character_set_client'
     * and 'character_set_results' which will be used by the server for decoding passwords during the authentication phase and later on, if
     * no SET NAMES are issued by {@link #configurePostHandshake(boolean)}.
     * </p>
     * <p>
     * It also means that collation index should be set according to:
     * <ol>
     * <li>'passwordCharacterEncoding' if it's present, or
     * <li>'connectionCollation' if it's present, or
     * <li>'characterEncoding' if it's present
     * </ol>
     * otherwise it will be set to utf8mb4_general_ci or utf8mb4_0900_ai_ci depending on server version.
     * <p>
     * Since Protocol::HandshakeV10 and Protocol::HandshakeResponse41 use only one byte for the collation it's not possible to use indexes &gt; 255 during the
     * handshake.
     * Also, ucs2, utf16, utf16le and utf32 character sets are impermissible here. Connector/J will try to use utf8mb4 instead.
     * </p>
     *
     * @param reset
     *            reset the charsets configuration; needed for changeUser and resetServerState call.
     *
     * @return MySQL collation index to be used during the handshake.
     */
    int configurePreHandshake(boolean reset);

    /**
     * Sets up client character set. This must be done before any further communication with the server!
     *
     * The 'collation_connection', 'character_set_client', 'character_set_connection' and 'character_set_results' server variables are set
     * according to the collation index selected by {@link #configurePreHandshake(boolean)} and sent in the Protocol::HandshakeV10 packet.
     * Here Connector/J alters these server variables if needed.
     *
     * @param dontCheckServerMatch
     *            if true then send the SET NAMES query even if server charset already matches the new value; needed for changeUser call.
     */
    void configurePostHandshake(boolean dontCheckServerMatch);

    public boolean doesPlatformDbCharsetMatches();

    String getPasswordCharacterEncoding();

    String getErrorMessageEncoding();

    String getMetadataEncoding();

    int getMetadataCollationIndex();

    boolean getRequiresEscapingEncoder();

    String getJavaEncodingForCollationIndex(int collationIndex);

    int getMaxBytesPerChar(String javaCharsetName);

    int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName);

    Integer getCollationIndexForCollationName(String collationName);

    String getCollationNameForCollationIndex(Integer collationIndex);

    String getMysqlCharsetNameForCollationIndex(Integer collationIndex);

    int getCollationIndexForJavaEncoding(String javaEncoding, ServerVersion version);

    int getCollationIndexForMysqlCharsetName(String charsetName);

    String getJavaEncodingForMysqlCharset(String mysqlCharsetName);

    String getMysqlCharsetForJavaEncoding(String javaEncoding, ServerVersion version);

    boolean isMultibyteCharset(String javaEncodingName);

}
