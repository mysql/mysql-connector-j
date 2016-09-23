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

package com.mysql.cj.jdbc.exceptions;

import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.core.Messages;

/**
 * Thrown when a client requests a connection-level feature that isn't available for this particular distribution of Connector/J (currently only used by code
 * that is export-controlled).
 */
public class ConnectionFeatureNotAvailableException extends CommunicationsException {

    private static final long serialVersionUID = 8315412078945570018L;

    public ConnectionFeatureNotAvailableException(JdbcConnection conn, long lastPacketSentTimeMs, Exception underlyingException) {
        super(conn, lastPacketSentTimeMs, 0, underlyingException);
    }

    public ConnectionFeatureNotAvailableException(String message, Throwable underlyingException) {
        super(message, underlyingException);
    }

    @Override
    public String getMessage() {
        return Messages.getString("ConnectionFeatureNotAvailableException.0");
    }

    @Override
    public String getSQLState() {
        return SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE;
    }
}
