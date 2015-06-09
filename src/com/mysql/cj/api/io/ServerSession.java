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

package com.mysql.cj.api.io;

import java.util.Map;

/**
 * Keeps the effective states of server/session variables,
 * contains methods for initial retrieving of these states and for their actualization.
 * 
 * @author say
 *
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

    ServerCapabilities getCapabilities();

    void setCapabilities(ServerCapabilities capabilities);

    int getStatusFlags();

    /**
     * Sets new server status (from response) without saving it's old state
     * 
     * @param statusFlags
     */
    void setStatusFlags(int statusFlags);

    /**
     * Sets new server status (from response)
     * 
     * @param statusFlags
     * @param saveOldStatusFlags
     */
    void setStatusFlags(int statusFlags, boolean saveOldStatusFlags);

    int getOldStatusFlags();

    void setOldStatusFlags(int statusFlags);

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
