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

package com.mysql.cj.protocol.a;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.Messages;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.ServerCapabilities;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.util.StringUtils;

public class NativeServerSession implements ServerSession {

    public static final int SERVER_STATUS_IN_TRANS = 1;
    public static final int SERVER_STATUS_AUTOCOMMIT = 2; // Server in auto_commit mode
    public static final int SERVER_MORE_RESULTS_EXISTS = 8; // Multi query - next query exists
    public static final int SERVER_QUERY_NO_GOOD_INDEX_USED = 16;
    public static final int SERVER_QUERY_NO_INDEX_USED = 32;
    public static final int SERVER_STATUS_CURSOR_EXISTS = 64;
    public static final int SERVER_STATUS_LAST_ROW_SENT = 128; // The server status for 'last-row-sent'
    public static final int SERVER_QUERY_WAS_SLOW = 2048;

    public static final int CLIENT_LONG_PASSWORD = 0x00000001; /* new more secure passwords */
    public static final int CLIENT_FOUND_ROWS = 0x00000002;
    public static final int CLIENT_LONG_FLAG = 0x00000004; /* Get all column flags */
    public static final int CLIENT_CONNECT_WITH_DB = 0x00000008;
    public static final int CLIENT_COMPRESS = 0x00000020; /* Can use compression protcol */
    public static final int CLIENT_LOCAL_FILES = 0x00000080; /* Can use LOAD DATA LOCAL */
    public static final int CLIENT_PROTOCOL_41 = 0x00000200; // for > 4.1.1
    public static final int CLIENT_INTERACTIVE = 0x00000400;
    public static final int CLIENT_SSL = 0x00000800;
    public static final int CLIENT_TRANSACTIONS = 0x00002000; // Client knows about transactions
    public static final int CLIENT_RESERVED = 0x00004000; // for 4.1.0 only
    public static final int CLIENT_SECURE_CONNECTION = 0x00008000;
    public static final int CLIENT_MULTI_STATEMENTS = 0x00010000; // Enable/disable multiquery support
    public static final int CLIENT_MULTI_RESULTS = 0x00020000; // Enable/disable multi-results
    public static final int CLIENT_PS_MULTI_RESULTS = 0x00040000; // Enable/disable multi-results for server prepared statements
    public static final int CLIENT_PLUGIN_AUTH = 0x00080000;
    public static final int CLIENT_CONNECT_ATTRS = 0x00100000;
    public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
    public static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORD = 0x00400000;
    public static final int CLIENT_SESSION_TRACK = 0x00800000;
    public static final int CLIENT_DEPRECATE_EOF = 0x01000000;

    private PropertySet propertySet;
    private NativeCapabilities capabilities;
    private int oldStatusFlags = 0;
    private int statusFlags = 0;
    private int serverDefaultCollationIndex;
    private long clientParam = 0;
    private boolean hasLongColumnInfo = false;

    /** The map of server variables that we retrieve at connection init. */
    private Map<String, String> serverVariables = new HashMap<>();

    public Map<Integer, String> indexToCustomMysqlCharset = null;

    public Map<String, Integer> mysqlCharsetToCustomMblen = null;

    /**
     * What character set is the metadata returned in?
     */
    private String characterSetMetadata = null;
    private int metadataCollationIndex;

    /**
     * The character set we want results and result metadata returned in (null ==
     * results in any charset, metadata in UTF-8).
     */
    private String characterSetResultsOnServer = null;

    /**
     * The (Java) encoding used to interpret error messages received from the server.
     * We use character_set_results (since MySQL 5.5) if it is not null or UTF-8 otherwise.
     */
    private String errorMessageEncoding = "Cp1252"; // to begin with, changes after we talk to the server

    /** Are we in autoCommit mode? */
    private boolean autoCommit = true;

    /** The timezone of the server */
    private TimeZone serverTimeZone = null;

    /** c.f. getDefaultTimeZone(). this value may be overridden during connection initialization */
    private TimeZone defaultTimeZone = TimeZone.getDefault();

    public NativeServerSession(PropertySet propertySet) {
        this.propertySet = propertySet;

        // preconfigure some server variables which are consulted before their initialization from server
        this.serverVariables.put("character_set_server", "utf8");
    }

    @Override
    public NativeCapabilities getCapabilities() {
        return this.capabilities;
    }

    @Override
    public void setCapabilities(ServerCapabilities capabilities) {
        this.capabilities = (NativeCapabilities) capabilities;
    }

    @Override
    public int getStatusFlags() {
        return this.statusFlags;
    }

    @Override
    public void setStatusFlags(int statusFlags) {
        setStatusFlags(statusFlags, false);
    }

    @Override
    public void setStatusFlags(int statusFlags, boolean saveOldStatus) {
        if (saveOldStatus) {
            this.oldStatusFlags = this.statusFlags;
        }
        this.statusFlags = statusFlags;
    }

    @Override
    public int getOldStatusFlags() {
        return this.oldStatusFlags;
    }

    @Override
    public void setOldStatusFlags(int oldStatusFlags) {
        this.oldStatusFlags = oldStatusFlags;
    }

    @Override
    public int getTransactionState() {
        if ((this.oldStatusFlags & SERVER_STATUS_IN_TRANS) == 0) {
            if ((this.statusFlags & SERVER_STATUS_IN_TRANS) == 0) {
                return TRANSACTION_NOT_STARTED;
            }
            return TRANSACTION_STARTED;
        }
        if ((this.statusFlags & SERVER_STATUS_IN_TRANS) == 0) {
            return TRANSACTION_COMPLETED;
        }
        return TRANSACTION_IN_PROGRESS;
    }

    @Override
    public boolean inTransactionOnServer() {
        return (this.statusFlags & SERVER_STATUS_IN_TRANS) != 0;
    }

    @Override
    public boolean cursorExists() {
        return (this.statusFlags & SERVER_STATUS_CURSOR_EXISTS) != 0;
    }

    @Override
    public boolean isAutocommit() {
        return (this.statusFlags & SERVER_STATUS_AUTOCOMMIT) != 0;
    }

    @Override
    public boolean hasMoreResults() {
        return (this.statusFlags & SERVER_MORE_RESULTS_EXISTS) != 0;
    }

    @Override
    public boolean noGoodIndexUsed() {
        return (this.statusFlags & SERVER_QUERY_NO_GOOD_INDEX_USED) != 0;
    }

    @Override
    public boolean noIndexUsed() {
        return (this.statusFlags & SERVER_QUERY_NO_INDEX_USED) != 0;
    }

    @Override
    public boolean queryWasSlow() {
        return (this.statusFlags & SERVER_QUERY_WAS_SLOW) != 0;
    }

    @Override
    public boolean isLastRowSent() {
        return (this.statusFlags & SERVER_STATUS_LAST_ROW_SENT) != 0;
    }

    @Override
    public long getClientParam() {
        return this.clientParam;
    }

    @Override
    public void setClientParam(long clientParam) {
        this.clientParam = clientParam;
    }

    @Override
    public boolean useMultiResults() {
        return (this.clientParam & CLIENT_MULTI_RESULTS) != 0 || (this.clientParam & CLIENT_PS_MULTI_RESULTS) != 0;
    }

    public boolean isEOFDeprecated() {
        return (this.clientParam & CLIENT_DEPRECATE_EOF) != 0;
    }

    @Override
    public int getServerDefaultCollationIndex() {
        return this.serverDefaultCollationIndex;
    }

    @Override
    public void setServerDefaultCollationIndex(int serverDefaultCollationIndex) {
        this.serverDefaultCollationIndex = serverDefaultCollationIndex;
    }

    @Override
    public boolean hasLongColumnInfo() {
        return this.hasLongColumnInfo;
    }

    @Override
    public void setHasLongColumnInfo(boolean hasLongColumnInfo) {
        this.hasLongColumnInfo = hasLongColumnInfo;
    }

    @Override
    public Map<String, String> getServerVariables() {
        return this.serverVariables;
    }

    @Override
    public String getServerVariable(String name) {
        return this.serverVariables.get(name);
    }

    @Override
    public int getServerVariable(String variableName, int fallbackValue) {
        try {
            return Integer.valueOf(getServerVariable(variableName));
        } catch (NumberFormatException nfe) {
            //getLog().logWarn(
            //        Messages.getString("Connection.BadValueInServerVariables", new Object[] { variableName, getServerVariable(variableName), fallbackValue }));
        }
        return fallbackValue;
    }

    @Override
    public void setServerVariables(Map<String, String> serverVariables) {
        this.serverVariables = serverVariables;
    }

    public boolean characterSetNamesMatches(String mysqlEncodingName) {
        // set names is equivalent to character_set_client ..._results and ..._connection, but we set _results later, so don't check it here.
        return (mysqlEncodingName != null && mysqlEncodingName.equalsIgnoreCase(getServerVariable("character_set_client"))
                && mysqlEncodingName.equalsIgnoreCase(getServerVariable("character_set_connection")));
    }

    public final ServerVersion getServerVersion() {
        return this.capabilities.getServerVersion();
    }

    @Override
    public boolean isVersion(ServerVersion version) {
        return this.getServerVersion().equals(version);
    }

    /**
     * Should SET AUTOCOMMIT be sent to server if we are going to set autoCommitFlag in driver
     * 
     * @param autoCommitFlag
     *            autocommit status we are going to set in driver
     * @param elideSetAutoCommitsFlag
     *            'elideSetAutoCommits' property value
     * @return true if SET AUTOCOMMIT to be sent
     */
    public boolean isSetNeededForAutoCommitMode(boolean autoCommitFlag, boolean elideSetAutoCommitsFlag) {
        if (elideSetAutoCommitsFlag) {
            boolean autoCommitModeOnServer = isAutocommit();

            if (autoCommitModeOnServer && !autoCommitFlag) {
                // When trying to set autoCommit == false and SERVER_STATUS_AUTOCOMMIT = true we need to check
                // SERVER_STATUS_IN_TRANS flag, because SERVER_STATUS_AUTOCOMMIT isn't always correct.
                // If a transaction is in progress on the server, then we must be already in autoCommit == false
                // therefore return the opposite of transaction status
                return !inTransactionOnServer();
            }

            return autoCommitModeOnServer != autoCommitFlag;
        }

        return true;
    }

    @Override
    public String getErrorMessageEncoding() {
        return this.errorMessageEncoding;
    }

    @Override
    public void setErrorMessageEncoding(String errorMessageEncoding) {
        this.errorMessageEncoding = errorMessageEncoding;
    }

    public String getServerDefaultCharset() {
        String charset = null;
        if (this.indexToCustomMysqlCharset != null) {
            charset = this.indexToCustomMysqlCharset.get(getServerDefaultCollationIndex());
        }
        if (charset == null) {
            charset = CharsetMapping.getMysqlCharsetNameForCollationIndex(getServerDefaultCollationIndex());
        }
        return charset != null ? charset : getServerVariable("character_set_server");
    }

    public int getMaxBytesPerChar(String javaCharsetName) {
        return getMaxBytesPerChar(null, javaCharsetName);
    }

    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) {

        String charset = null;
        int res = 1;

        // if we can get it by charsetIndex just doing it

        // getting charset name from dynamic maps in connection; we do it before checking against static maps because custom charset on server can be mapped
        // to index from our static map key's diapason 
        if (this.indexToCustomMysqlCharset != null) {
            charset = this.indexToCustomMysqlCharset.get(charsetIndex);
        }
        // checking against static maps if no custom charset found
        if (charset == null) {
            charset = CharsetMapping.getMysqlCharsetNameForCollationIndex(charsetIndex);
        }

        // if we didn't find charset name by index
        if (charset == null) {
            charset = CharsetMapping.getMysqlCharsetForJavaEncoding(javaCharsetName, getServerVersion());
        }

        // checking against dynamic maps in connection
        Integer mblen = null;
        if (this.mysqlCharsetToCustomMblen != null) {
            mblen = this.mysqlCharsetToCustomMblen.get(charset);
        }

        // checking against static maps
        if (mblen == null) {
            mblen = CharsetMapping.getMblen(charset);
        }

        if (mblen != null) {
            res = mblen.intValue();
        }

        return res; // we don't know
    }

    public String getEncodingForIndex(int charsetIndex) {
        String javaEncoding = null;

        String characterEncoding = this.propertySet.getStringProperty(PropertyDefinitions.PNAME_characterEncoding).getValue();

        if (this.propertySet.getBooleanProperty(PropertyDefinitions.PNAME_useOldUTF8Behavior).getValue()) {
            return characterEncoding;
        }

        if (charsetIndex != NativeConstants.NO_CHARSET_INFO) {
            try {
                // getting charset name from dynamic maps in connection; we do it before checking against static maps because custom charset on server can be mapped
                // to index from our static map key's diapason 
                if (this.indexToCustomMysqlCharset != null) {
                    String cs = this.indexToCustomMysqlCharset.get(charsetIndex);
                    if (cs != null) {
                        javaEncoding = CharsetMapping.getJavaEncodingForMysqlCharset(cs, characterEncoding);
                    }
                }
                // checking against static maps if no custom charset found
                if (javaEncoding == null) {
                    javaEncoding = CharsetMapping.getJavaEncodingForCollationIndex(charsetIndex, characterEncoding);
                }

            } catch (ArrayIndexOutOfBoundsException outOfBoundsEx) {
                throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("Connection.11", new Object[] { charsetIndex }));
            }

            // Punt
            if (javaEncoding == null) {
                javaEncoding = characterEncoding;
            }
        } else {
            javaEncoding = characterEncoding;
        }

        return javaEncoding;
    }

    public void configureCharacterSets() {
        //
        // We need to figure out what character set metadata and error messages will be returned in, and then map them to Java encoding names
        //
        // We've already set it, and it might be different than what was originally on the server, which is why we use the "special" key to retrieve it
        String characterSetResultsOnServerMysql = getServerVariable(LOCAL_CHARACTER_SET_RESULTS);

        if (characterSetResultsOnServerMysql == null || StringUtils.startsWithIgnoreCaseAndWs(characterSetResultsOnServerMysql, "NULL")
                || characterSetResultsOnServerMysql.length() == 0) {
            String defaultMetadataCharsetMysql = getServerVariable("character_set_system");
            String defaultMetadataCharset = null;

            if (defaultMetadataCharsetMysql != null) {
                defaultMetadataCharset = CharsetMapping.getJavaEncodingForMysqlCharset(defaultMetadataCharsetMysql);
            } else {
                defaultMetadataCharset = "UTF-8";
            }

            this.characterSetMetadata = defaultMetadataCharset;
            setErrorMessageEncoding("UTF-8");
        } else {
            this.characterSetResultsOnServer = CharsetMapping.getJavaEncodingForMysqlCharset(characterSetResultsOnServerMysql);
            this.characterSetMetadata = this.characterSetResultsOnServer;
            setErrorMessageEncoding(this.characterSetResultsOnServer);
        }

        this.metadataCollationIndex = CharsetMapping.getCollationIndexForJavaEncoding(this.characterSetMetadata, getServerVersion());
    }

    public String getCharacterSetMetadata() {
        return this.characterSetMetadata;
    }

    public void setCharacterSetMetadata(String characterSetMetadata) {
        this.characterSetMetadata = characterSetMetadata;
    }

    public int getMetadataCollationIndex() {
        return this.metadataCollationIndex;
    }

    public void setMetadataCollationIndex(int metadataCollationIndex) {
        this.metadataCollationIndex = metadataCollationIndex;
    }

    public String getCharacterSetResultsOnServer() {
        return this.characterSetResultsOnServer;
    }

    public void setCharacterSetResultsOnServer(String characterSetResultsOnServer) {
        this.characterSetResultsOnServer = characterSetResultsOnServer;
    }

    public void preserveOldTransactionState() {
        this.statusFlags |= this.oldStatusFlags & SERVER_STATUS_IN_TRANS;
    }

    @Override
    public boolean isLowerCaseTableNames() {
        String lowerCaseTables = this.serverVariables.get("lower_case_table_names");
        return "on".equalsIgnoreCase(lowerCaseTables) || "1".equalsIgnoreCase(lowerCaseTables) || "2".equalsIgnoreCase(lowerCaseTables);
    }

    @Override
    public boolean storesLowerCaseTableNames() {
        String lowerCaseTables = this.serverVariables.get("lower_case_table_names");
        return "1".equalsIgnoreCase(lowerCaseTables) || "on".equalsIgnoreCase(lowerCaseTables);
    }

    public boolean isQueryCacheEnabled() {
        return "ON".equalsIgnoreCase(this.serverVariables.get("query_cache_type")) && !"0".equalsIgnoreCase(this.serverVariables.get("query_cache_size"));
    }

    /**
     * Is the server in a sql_mode that does not allow us to use \\ to escape things?
     * 
     * @return Returns the noBackslashEscapes.
     */
    public boolean isNoBackslashEscapesSet() {
        String sqlModeAsString = this.serverVariables.get("sql_mode");
        return sqlModeAsString != null && sqlModeAsString.indexOf("NO_BACKSLASH_ESCAPES") != -1;
    }

    public boolean useAnsiQuotedIdentifiers() {
        String sqlModeAsString = this.serverVariables.get("sql_mode");
        return sqlModeAsString != null && sqlModeAsString.indexOf("ANSI_QUOTES") != -1;
    }

    @Override
    public long getThreadId() {
        return this.capabilities.getThreadId();
    }

    @Override
    public void setThreadId(long threadId) {
        this.capabilities.setThreadId(threadId);
    }

    public boolean isAutoCommit() {
        return this.autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public TimeZone getServerTimeZone() {
        return this.serverTimeZone;
    }

    public void setServerTimeZone(TimeZone serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
    }

    public TimeZone getDefaultTimeZone() {
        return this.defaultTimeZone;
    }

    public void setDefaultTimeZone(TimeZone defaultTimeZone) {
        this.defaultTimeZone = defaultTimeZone;
    }
}
