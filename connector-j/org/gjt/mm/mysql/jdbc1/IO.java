/*
 * MM JDBC Drivers for MySQL
 *
 * $Id$
 *
 * Copyright (C) 1998 Mark Matthews <mmatthew@worldserver.com>
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 * 
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * See the COPYING file located in the top-level-directory of
 * the archive of this library for complete text of license.
 */

/**
 * This class is used by Connection for communicating with the
 * MySQL server.
 *
 * @see java.sql.Connection
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */

package org.gjt.mm.mysql.jdbc1;

import java.util.Vector;
import java.io.IOException;

public class IO extends org.gjt.mm.mysql.MysqlIO
{
  
    /**
     * Constructor:  Connect to the MySQL server and setup
     * a stream connection.
     *
     * @param host the hostname to connect to
     * @param port the port number that the server is listening on
     * @exception IOException if an IOException occurs during connect.
     */
    
    IO(String Host, int port, org.gjt.mm.mysql.Connection Conn) 
	throws IOException, java.sql.SQLException
    {
	super(Host, port, Conn);
    }

    protected org.gjt.mm.mysql.ResultSet buildResultSetWithRows(org.gjt.mm.mysql.Field[] Fields, Vector Rows, org.gjt.mm.mysql.Connection Conn)
    {
	return new org.gjt.mm.mysql.jdbc1.ResultSet(Fields, Rows, Conn);
    }

    protected org.gjt.mm.mysql.ResultSet buildResultSetWithUpdates(long updateCount, 
						  long updateID, org.gjt.mm.mysql.Connection Conn)
    {
	 return new org.gjt.mm.mysql.jdbc1.ResultSet(updateCount, updateID);
    }
};
