/*
 * MM JDBC Drivers for MySQL
 *
 * $Id: Connection.java,v 1.3 2002/04/21 03:03:46 mark_matthews Exp $
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
 *
 * Some portions:
 *
 * Copyright (c) 1996 Bradley McLean / Jeffrey Medeiros
 * Modifications Copyright (c) 1996/1997 Martin Rode
 * Copyright (c) 1997 Peter T Mount
 */

/**
 * A Connection represents a session with a specific database.  Within the
 * context of a Connection, SQL statements are executed and results are
 * returned.
 *
 * <P>A Connection's database is able to provide information describing
 * its tables, its supported SQL grammar, its stored procedures, the
 * capabilities of this connection, etc.  This information is obtained
 * with the getMetaData method.
 *
 * <p><B>Note:</B> MySQL does not support transactions, so all queries
 *                 are committed as they are executed.
 *
 * @see java.sql.Connection
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id: Connection.java,v 1.3 2002/04/21 03:03:46 mark_matthews Exp $
 */

package com.mysql.jdbc.jdbc1;

import java.io.UnsupportedEncodingException;

import java.sql.*;
import java.util.Properties;

public class Connection
	extends com.mysql.jdbc.Connection
	implements java.sql.Connection
{

	/**
	 * Connect to a MySQL Server.
	 *
	 * <p><b>Important Notice</b>
	 *
	 * <br>Although this will connect to the database, user code should open
	 * the connection via the DriverManager.getConnection() methods only.
	 *
	 * <br>This should only be called from the org.gjt.mm.mysql.Driver class.
	 *
	 * @param Host the hostname of the database server
	 * @param port the port number the server is listening on
	 * @param Info a Properties[] list holding the user and password
	 * @param Database the database to connect to
	 * @param Url the URL of the connection
	 * @param D the Driver instantation of the connection
	 * @return a valid connection profile
	 * @exception java.sql.SQLException if a database access error occurs
	 */

	public void connectionInit(
		String host,
		int port,
		Properties info,
		String database,
		String url,
		com.mysql.jdbc.Driver d)
		throws java.sql.SQLException
	{
		super.connectionInit(host, port, info, database, url, d);
	}

	/**
	 * SQL statements without parameters are normally executed using
	 * Statement objects.  If the same SQL statement is executed many
	 * times, it is more efficient to use a PreparedStatement
	 *
	 * @return a new Statement object
	 * @exception java.sql.SQLException passed through from the constructor
	 */

	public java.sql.Statement createStatement() throws java.sql.SQLException
	{
		return new com.mysql.jdbc.jdbc1.Statement(this, _database);
	}

	/**
	 * A SQL statement with or without IN parameters can be pre-compiled
	 * and stored in a PreparedStatement object.  This object can then
	 * be used to efficiently execute this statement multiple times.
	 * 
	 * <p>
	 * <B>Note:</B> This method is optimized for handling parametric
	 * SQL statements that benefit from precompilation if the driver
	 * supports precompilation. 
	 *
	 * In this case, the statement is not sent to the database until the
	 * PreparedStatement is executed.  This has no direct effect on users;
	 * however it does affect which method throws 
	 * certain java.sql.SQLExceptions
	 *
	 * <p>
	 * MySQL does not support precompilation of statements, so they
	 * are handled by the driver. 
	 *
	 * @param sql a SQL statement that may contain one or more '?' IN
	 *    parameter placeholders
	 * @return a new PreparedStatement object containing the pre-compiled
	 *    statement.
	 * @exception java.sql.SQLException if a database access error occurs.
	 */

	public java.sql.PreparedStatement prepareStatement(String sql)
		throws java.sql.SQLException
	{
		return new com.mysql.jdbc.jdbc1.PreparedStatement(this, sql, _database);
	}

	/**
	 * A connection's database is able to provide information describing
	 * its tables, its supported SQL grammar, its stored procedures, the
	 * capabilities of this connection, etc.  This information is made
	 * available through a DatabaseMetaData object.
	 *
	 * @return a DatabaseMetaData object for this connection
	 * @exception java.sql.SQLException if a database access error occurs
	 */

	public java.sql.DatabaseMetaData getMetaData() throws java.sql.SQLException
	{
		return new DatabaseMetaData(this, _database);
	}

	protected com.mysql.jdbc.MysqlIO createNewIO(String host, int port)
		throws Exception
	{
		return new IO(host, port, this);
	}
}