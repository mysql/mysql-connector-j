/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc.exceptions;

import java.sql.SQLRecoverableException;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.StreamingNotifiable;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.protocol.PacketReceivedTimeHolder;
import com.mysql.cj.protocol.PacketSentTimeHolder;

/**
 * An exception to represent communications errors with the database.
 *
 * Attempts to provide 'friendlier' error messages to end-users, including the last time a packet was sent to the database,
 * what the client-timeout is set to, and whether the idle time has been exceeded.
 */
public class CommunicationsException extends SQLRecoverableException implements StreamingNotifiable {

    private static final long serialVersionUID = 4317904269000988676L;

    private String exceptionMessage;

    public CommunicationsException(JdbcConnection conn, PacketSentTimeHolder packetSentTimeHolder, PacketReceivedTimeHolder packetReceivedTimeHolder,
            Exception underlyingException) {
        this(ExceptionFactory.createLinkFailureMessageBasedOnHeuristics(conn.getPropertySet(), conn.getSession().getServerSession(), packetSentTimeHolder,
                packetReceivedTimeHolder, underlyingException), underlyingException);
    }

    public CommunicationsException(String message, Throwable underlyingException) {
        this.exceptionMessage = message;

        if (underlyingException != null) {
            initCause(underlyingException);
        }
    }

    @Override
    public String getMessage() {
        return this.exceptionMessage;
    }

    @Override
    public String getSQLState() {
        return MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE;
    }

    @Override
    public void setWasStreamingResults() {
        // replace exception message
        this.exceptionMessage = Messages.getString("CommunicationsException.ClientWasStreaming");
    }

}
