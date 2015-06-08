/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.api;

import java.util.Map;

import com.mysql.cj.api.io.ServerCapabilities;
import com.mysql.cj.core.ServerVersion;

/**
 * Keeps the effective states of server/session variables,
 * contains methods for initial retrieving of these states and for their actualization.
 * 
 * @author say
 *
 */
public interface SessionState {

    static final int SERVER_STATUS_IN_TRANS = 1;
    static final int SERVER_STATUS_AUTOCOMMIT = 2; // Server in auto_commit mode
    static final int SERVER_MORE_RESULTS_EXISTS = 8; // Multi query - next query exists
    static final int SERVER_QUERY_NO_GOOD_INDEX_USED = 16;
    static final int SERVER_QUERY_NO_INDEX_USED = 32;
    static final int SERVER_STATUS_CURSOR_EXISTS = 64;
    static final int SERVER_STATUS_LAST_ROW_SENT = 128; // The server status for 'last-row-sent'
    static final int SERVER_QUERY_WAS_SLOW = 2048;

    static final int CLIENT_LONG_PASSWORD = 0x00000001; /* new more secure passwords */
    static final int CLIENT_FOUND_ROWS = 0x00000002;
    static final int CLIENT_LONG_FLAG = 0x00000004; /* Get all column flags */
    static final int CLIENT_CONNECT_WITH_DB = 0x00000008;
    static final int CLIENT_COMPRESS = 0x00000020; /* Can use compression protcol */
    static final int CLIENT_LOCAL_FILES = 0x00000080; /* Can use LOAD DATA LOCAL */
    static final int CLIENT_PROTOCOL_41 = 0x00000200; // for > 4.1.1
    static final int CLIENT_INTERACTIVE = 0x00000400;
    static final int CLIENT_SSL = 0x00000800;
    static final int CLIENT_TRANSACTIONS = 0x00002000; // Client knows about transactions
    static final int CLIENT_RESERVED = 0x00004000; // for 4.1.0 only
    static final int CLIENT_SECURE_CONNECTION = 0x00008000;
    static final int CLIENT_MULTI_STATEMENTS = 0x00010000; // Enable/disable multiquery support
    static final int CLIENT_MULTI_RESULTS = 0x00020000; // Enable/disable multi-results
    static final int CLIENT_PLUGIN_AUTH = 0x00080000;
    static final int CLIENT_CONNECT_ATTRS = 0x00100000;
    static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
    static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORD = 0x00400000;

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

    ServerVersion getServerVersion();

    void setServerVersion(ServerVersion serverVersion);

    ServerCapabilities getCapabilities();

    void setCapabilities(ServerCapabilities capabilities);

    int getServerStatus();

    /**
     * Sets new server status (from response) without saving it's old state
     * 
     * @param serverStatus
     */
    void setServerStatus(int serverStatus);

    /**
     * Sets new server status (from response)
     * 
     * @param serverStatus
     * @param saveOldStatus
     */
    void setServerStatus(int serverStatus, boolean saveOldStatus);

    int getOldServerStatus();

    void setOldServerStatus(int serverStatus);

    int getServerCharsetIndex();

    void setServerCharsetIndex(int serverCharsetIndex);

    /**
     * 
     * @return TRANSACTION_NOT_STARTED, TRANSACTION_IN_PROGRESS, TRANSACTION_STARTED or TRANSACTION_COMPLETED
     */
    int getTransactionState();

    boolean inTransactionOnServer();

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

    boolean hasLongColumnInfo();

    /**
     * Does the server send back extra column info?
     * 
     * @return true if so
     */
    void setHasLongColumnInfo(boolean hasLongColumnInfo);

    Map<String, String> getServerVariables();

    String getServerVariable(String name);

    void setServerVariables(Map<String, String> serverVariables);
}
