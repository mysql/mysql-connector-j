/*
 * MM JDBC Drivers for MySQL
 *
 * $Id: PreparedStatement.java,v 1.2 2002/04/21 03:03:46 mark_matthews Exp $
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
 * A SQL Statement is pre-compiled and stored in a PreparedStatement object.
 * This object can then be used to efficiently execute this statement multiple
 * times.
 *
 * <p><B>Note:</B> The setXXX methods for setting IN parameter values must
 * specify types that are compatible with the defined SQL type of the input
 * parameter.  For instance, if the IN parameter has SQL type Integer, then
 * setInt should be used.
 *
 * <p>If arbitrary parameter type conversions are required, then the setObject 
 * method should be used with a target SQL type.
 *
 * @see java.sql.ResultSet
 * @see java.sql.PreparedStatement
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id: PreparedStatement.java,v 1.2 2002/04/21 03:03:46 mark_matthews Exp $
 */

package com.mysql.jdbc.jdbc1;

public class PreparedStatement extends com.mysql.jdbc.PreparedStatement
    implements java.sql.PreparedStatement  
{
    /**
     * Constructor for the PreparedStatement class.
     * Split the SQL statement into segments - separated by the arguments.
     * When we rebuild the thing with the arguments, we can substitute the
     * args and join the whole thing together.
     *
     * @param conn the instanatiating connection
     * @param sql the SQL statement with ? for IN markers
     * @exception java.sql.SQLException if something bad occurs
     */

    public PreparedStatement(Connection Conn, String Sql, String Catalog) throws java.sql.SQLException
    {
	super(Conn, Sql, Catalog);
    }
};
