/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA



 */
package com.mysql.jdbc;

import java.sql.SQLException;

/**
 * Thrown when a packet that is too big for the server is created.
 * 
 * @author Mark Matthews
 */
public class PacketTooBigException extends SQLException {
	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Creates a new PacketTooBigException object.
	 * 
	 * @param packetSize
	 *            the size of the packet that was going to be sent
	 * @param maximumPacketSize
	 *            the maximum size the server will accept
	 */
	public PacketTooBigException(long packetSize, long maximumPacketSize) {
		super(
				Messages.getString("PacketTooBigException.0") + packetSize + Messages.getString("PacketTooBigException.1") //$NON-NLS-1$ //$NON-NLS-2$
						+ maximumPacketSize
						+ Messages.getString("PacketTooBigException.2") //$NON-NLS-1$
						+ Messages.getString("PacketTooBigException.3") //$NON-NLS-1$
						+ Messages.getString("PacketTooBigException.4"), SQLError.SQL_STATE_GENERAL_ERROR); //$NON-NLS-1$
	}
}
