/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.protocol;

import java.util.Map;
import java.util.TimeZone;

import com.mysql.cj.ServerVersion;

/**
 * Keeps the effective states of server/session variables,
 * contains methods for initial retrieving of these states and for their actualization.
 */
public interface ServerSession {

    /**
     * There was no change between old and current SERVER_STATUS_IN_TRANS state and it is 0.
     */
    public static int TRANSACTION_NOT_STARTED = 0;

    /**
     * There was no change between old and current SERVER_STATUS_IN_TRANS state and it is 1.
     */
    public static int TRANSACTION_IN_PROGRESS = 1;

    /**
     * Old SERVER_STATUS_IN_TRANS state was 0 and current one is 1.
     */
    public static int TRANSACTION_STARTED = 2;

    /**
     * Old SERVER_STATUS_IN_TRANS state was 1 and current one is 0.
     */
    public static int TRANSACTION_COMPLETED = 3;

    public static final String LOCAL_CHARACTER_SET_RESULTS = "local.character_set_results";

    ServerCapabilities getCapabilities();

    void setCapabilities(ServerCapabilities capabilities);

    int getStatusFlags();

    /**
     * Sets new server status (from response) without saving it's old state
     * 
     * @param statusFlags
     *            server status flags
     */
    void setStatusFlags(int statusFlags);

    /**
     * Sets new server status (from response)
     * 
     * @param statusFlags
     *            new server status flags
     * @param saveOldStatusFlags
     *            true if old server status flags should be preserved
     */
    void setStatusFlags(int statusFlags, boolean saveOldStatusFlags);

    int getOldStatusFlags();

    void setOldStatusFlags(int statusFlags);

    /**
     * 
     * @return Collation index which server provided in handshake greeting packet
     */
    int getServerDefaultCollationIndex();

    /**
     * Stores collation index which server provided in handshake greeting packet.
     * 
     * @param serverDefaultCollationIndex
     *            collation index
     */
    void setServerDefaultCollationIndex(int serverDefaultCollationIndex);

    /**
     * 
     * @return TRANSACTION_NOT_STARTED, TRANSACTION_IN_PROGRESS, TRANSACTION_STARTED or TRANSACTION_COMPLETED
     */
    int getTransactionState();

    boolean inTransactionOnServer();

    /**
     * Server will only open a cursor and set this flag if it can, otherwise it punts and goes back to mysql_store_results() behavior.
     * 
     * @return SERVER_STATUS_CURSOR_EXISTS <a href=http://dev.mysql.com/doc/internals/en/status-flags.html>status flag</a> value.
     */
    boolean cursorExists();

    boolean isAutocommit();

    boolean hasMoreResults();

    boolean isLastRowSent();

    boolean noGoodIndexUsed();

    boolean noIndexUsed();

    boolean queryWasSlow();

    long getClientParam();

    void setClientParam(long clientParam);

    boolean useMultiResults();

    boolean isEOFDeprecated();

    boolean hasLongColumnInfo();

    Map<String, String> getServerVariables();

    String getServerVariable(String name);

    int getServerVariable(String variableName, int fallbackValue);

    void setServerVariables(Map<String, String> serverVariables);

    boolean characterSetNamesMatches(String mysqlEncodingName);

    /**
     * Get the version of the MySQL server we are talking to.
     * 
     * @return {@link ServerVersion}
     */
    ServerVersion getServerVersion();

    /**
     * Is the version of the MySQL server we are connected to the given
     * version?
     * 
     * @param version
     *            the version to check for
     * 
     * @return true if the version of the MySQL server we are connected is the
     *         given version
     */
    boolean isVersion(ServerVersion version);

    /**
     * 
     * @return the server's default character set name according to collation index from server greeting,
     *         or value of 'character_set_server' variable if there is no mapping for that index
     */
    String getServerDefaultCharset();

    String getErrorMessageEncoding();

    void setErrorMessageEncoding(String errorMessageEncoding);

    int getMaxBytesPerChar(String javaCharsetName);

    int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName);

    /**
     * Returns the Java character encoding name for the given MySQL server
     * collation index
     * 
     * @param collationIndex
     *            collation index
     * @return the Java character encoding name for the given MySQL server
     *         collation index
     */
    String getEncodingForIndex(int collationIndex);

    void configureCharacterSets();

    String getCharacterSetMetadata();

    void setCharacterSetMetadata(String characterSetMetadata);

    int getMetadataCollationIndex();

    void setMetadataCollationIndex(int metadataCollationIndex);

    String getCharacterSetResultsOnServer();

    void setCharacterSetResultsOnServer(String characterSetResultsOnServer);

    /**
     * Is the server configured to use lower-case table names only?
     * 
     * @return true if lower_case_table_names is 'on'
     */
    boolean isLowerCaseTableNames();

    boolean storesLowerCaseTableNames();

    boolean isQueryCacheEnabled();

    boolean isNoBackslashEscapesSet();

    boolean useAnsiQuotedIdentifiers();

    public boolean isServerTruncatesFracSecs();

    long getThreadId();

    public void setThreadId(long threadId);

    boolean isAutoCommit();

    void setAutoCommit(boolean autoCommit);

    TimeZone getSessionTimeZone();

    void setSessionTimeZone(TimeZone sessionTimeZone);

    /**
     * The default time zone used to marshall date/time values to/from the server. This is used when getDate(), etc methods are called without a calendar
     * argument.
     *
     * @return The default JVM time zone
     */
    TimeZone getDefaultTimeZone();
}
