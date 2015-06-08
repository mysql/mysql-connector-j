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

package com.mysql.cj.core.io;

import java.util.Map;

import com.mysql.cj.api.SessionState;
import com.mysql.cj.api.io.ServerCapabilities;
import com.mysql.cj.core.ServerVersion;

public class MysqlSessionState implements SessionState {

    private ServerVersion serverVersion;
    private ServerCapabilities capabilities;
    private int oldServerStatus = 0;
    private int serverStatus = 0;
    private int serverCharsetIndex;
    private long clientParam = 0;
    private boolean hasLongColumnInfo = false;

    /** The map of server variables that we retrieve at connection init. */
    private Map<String, String> serverVariables = null;

    public MysqlSessionState() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public ServerVersion getServerVersion() {
        return this.serverVersion;
    }

    @Override
    public void setServerVersion(ServerVersion serverVersion) {
        this.serverVersion = serverVersion;
    }

    @Override
    public ServerCapabilities getCapabilities() {
        return this.capabilities;
    }

    @Override
    public void setCapabilities(ServerCapabilities capabilities) {
        this.capabilities = capabilities;
    }

    @Override
    public int getServerStatus() {
        return this.serverStatus;
    }

    @Override
    public void setServerStatus(int serverStatus) {
        setServerStatus(serverStatus, false);
    }

    @Override
    public void setServerStatus(int serverStatus, boolean saveOldStatus) {
        if (saveOldStatus) {
            this.oldServerStatus = this.serverStatus;
        }
        this.serverStatus = serverStatus;
    }

    @Override
    public int getOldServerStatus() {
        return this.oldServerStatus;
    }

    @Override
    public void setOldServerStatus(int oldServerStatus) {
        this.oldServerStatus = oldServerStatus;
    }

    @Override
    public int getTransactionState() {
        if ((this.oldServerStatus & SERVER_STATUS_IN_TRANS) == 0) {
            if ((this.serverStatus & SERVER_STATUS_IN_TRANS) == 0) {
                return TRANSACTION_NOT_STARTED;
            }
            return TRANSACTION_STARTED;
        }
        if ((this.serverStatus & SERVER_STATUS_IN_TRANS) == 0) {
            return TRANSACTION_COMPLETED;
        }
        return TRANSACTION_IN_PROGRESS;
    }

    @Override
    public boolean inTransactionOnServer() {
        return (this.serverStatus & SERVER_STATUS_IN_TRANS) != 0;
    }

    @Override
    public boolean cursorExists() {
        return (this.serverStatus & SERVER_STATUS_CURSOR_EXISTS) != 0;
    }

    @Override
    public boolean isAutocommit() {
        return (this.serverStatus & SERVER_STATUS_AUTOCOMMIT) != 0;
    }

    @Override
    public boolean hasMoreResults() {
        return (this.serverStatus & SERVER_MORE_RESULTS_EXISTS) != 0;
    }

    @Override
    public boolean noGoodIndexUsed() {
        return (this.serverStatus & SERVER_QUERY_NO_GOOD_INDEX_USED) != 0;
    }

    @Override
    public boolean noIndexUsed() {
        return (this.serverStatus & SERVER_QUERY_NO_INDEX_USED) != 0;
    }

    @Override
    public boolean queryWasSlow() {
        return (this.serverStatus & SERVER_QUERY_WAS_SLOW) != 0;
    }

    @Override
    public boolean isLastRowSent() {
        return (this.serverStatus & SERVER_STATUS_LAST_ROW_SENT) != 0;
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
        return (this.clientParam & CLIENT_MULTI_RESULTS) != 0;
    }

    @Override
    public int getServerCharsetIndex() {
        return this.serverCharsetIndex;
    }

    @Override
    public void setServerCharsetIndex(int serverCharsetIndex) {
        this.serverCharsetIndex = serverCharsetIndex;
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
    public void setServerVariables(Map<String, String> serverVariables) {
        this.serverVariables = serverVariables;
    }

}
