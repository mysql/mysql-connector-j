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

import java.sql.SQLException;

import com.mysql.cj.core.Messages;

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
        super(Messages.getString("PacketTooBigException.0", new Object[] { packetSize, maximumPacketSize }), SQLError.SQL_STATE_GENERAL_ERROR);
    }

    public PacketTooBigException(String message) {
        super(message, SQLError.SQL_STATE_GENERAL_ERROR);
    }
}
