/*
 * MM JDBC Drivers for MySQL
 *
 * $Id: IO.java,v 1.2 2002/04/21 03:03:46 mark_matthews Exp $
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
 * @version $Id: IO.java,v 1.2 2002/04/21 03:03:46 mark_matthews Exp $
 */

package com.mysql.jdbc.jdbc2;

import java.util.Vector;
import java.io.IOException;

public class IO extends com.mysql.jdbc.MysqlIO
{
  
    /**
     * Constructor:  Connect to the MySQL server and setup
     * a stream connection.
     *
     * @param host the hostname to connect to
     * @param port the port number that the server is listening on
     * @exception IOException if an IOException occurs during connect.
     */
    
    IO(String Host, int port, com.mysql.jdbc.Connection Conn) 
	throws IOException, java.sql.SQLException
    {
	super(Host, port, Conn);
    }

    protected com.mysql.jdbc.ResultSet buildResultSetWithRows(com.mysql.jdbc.Field[] Fields, Vector Rows, com.mysql.jdbc.Connection Conn)
    {
	return new com.mysql.jdbc.jdbc2.ResultSet(Fields, Rows, Conn);
    }

    protected com.mysql.jdbc.ResultSet buildResultSetWithUpdates(long updateCount, 
						  long updateID, com.mysql.jdbc.Connection Conn)
    {
	return new com.mysql.jdbc.jdbc2.ResultSet(updateCount, updateID);
    }
}
