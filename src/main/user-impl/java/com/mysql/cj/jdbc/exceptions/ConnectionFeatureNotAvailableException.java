/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.jdbc.exceptions;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.protocol.PacketSentTimeHolder;

/**
 * Thrown when a client requests a connection-level feature that isn't available for this particular distribution of Connector/J (currently only used by code
 * that is export-controlled).
 */
public class ConnectionFeatureNotAvailableException extends CommunicationsException {

    private static final long serialVersionUID = 8315412078945570018L;

    public ConnectionFeatureNotAvailableException(JdbcConnection conn, PacketSentTimeHolder packetSentTimeHolder, Exception underlyingException) {
        super(conn, packetSentTimeHolder, null, underlyingException);
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
        return MysqlErrorNumbers.SQLSTATE_MYSQL_INVALID_CONNECTION_ATTRIBUTE;
    }

}
