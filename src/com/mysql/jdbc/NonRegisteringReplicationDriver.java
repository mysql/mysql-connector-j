/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as
 published by the Free Software Foundation.
 

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * Driver that opens two connections, one two a replication master, and another
 * to one or more slaves, and decides to use master when the connection is not
 * read-only, and use slave(s) when the connection is read-only.
 * 
 * @version $Id: NonRegisteringReplicationDriver.java,v 1.1.2.1 2005/05/13
 *          18:58:37 mmatthews Exp $
 */
public class NonRegisteringReplicationDriver extends NonRegisteringDriver {
	public NonRegisteringReplicationDriver() throws SQLException {
		super();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	public Connection connect(String url, Properties info) throws SQLException {
		return connectReplicationConnection(url, info);
	}
}
