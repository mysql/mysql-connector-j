/*
 Copyright (C) 2002-2004 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



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
