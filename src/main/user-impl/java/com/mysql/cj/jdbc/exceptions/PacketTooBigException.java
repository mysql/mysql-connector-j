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

import java.sql.SQLException;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.MysqlErrorNumbers;

/**
 * Thrown when a packet that is too big for the server is created.
 */
public class PacketTooBigException extends SQLException {

    static final long serialVersionUID = 7248633977685452174L;

    /**
     * Creates a new PacketTooBigException object.
     *
     * @param packetSize
     *            the size of the packet that was going to be sent
     * @param maximumPacketSize
     *            the maximum size the server will accept
     */
    public PacketTooBigException(long packetSize, long maximumPacketSize) {
        super(Messages.getString("PacketTooBigException.0", new Object[] { packetSize, maximumPacketSize }), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
    }

    public PacketTooBigException(String message) {
        super(message, MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR);
    }

}
