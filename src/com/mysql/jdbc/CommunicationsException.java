/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

package com.mysql.jdbc;

import java.sql.SQLException;

/**
 * An exception to represent communications errors with the database.
 * 
 * Attempts to provide 'friendler' error messages to end-users, including last time a packet was sent to the database, what the client-timeout is set to, and
 * whether the idle time has been exceeded.
 */
public class CommunicationsException extends SQLException implements StreamingNotifiable {
    static final long serialVersionUID = 3193864990663398317L;

    private String exceptionMessage = null;

    public CommunicationsException(MySQLConnection conn, long lastPacketSentTimeMs, long lastPacketReceivedTimeMs, Exception underlyingException) {
        this.exceptionMessage = SQLError.createLinkFailureMessageBasedOnHeuristics(conn, lastPacketSentTimeMs, lastPacketReceivedTimeMs, underlyingException);

        if (underlyingException != null) {
            initCause(underlyingException);
        }
    }

    /*
     * @see java.lang.Throwable#getMessage()
     */
    @Override
    public String getMessage() {
        return this.exceptionMessage;
    }

    /*
     * @see java.sql.SQLException#getSQLState()
     */
    @Override
    public String getSQLState() {
        return SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE;
    }

    /*
     * @see com.mysql.jdbc.StreamingNotifiable#setWasStreamingResults()
     */
    public void setWasStreamingResults() {
        // replace exception message
        this.exceptionMessage = Messages.getString("CommunicationsException.ClientWasStreaming");
    }
}
