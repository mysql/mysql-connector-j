/*
 * MM JDBC Drivers for MySQL
 *
 * $Id: ResultSetMetaData.java,v 1.3 2002/04/25 01:08:36 mark_matthews Exp $
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
 * A ResultSetMetaData object can be used to find out about the types and
 * properties of the columns in a ResultSet
 *
 * @see java.sql.ResultSetMetaData
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id: ResultSetMetaData.java,v 1.3 2002/04/25 01:08:36 mark_matthews Exp $
 */

package com.mysql.jdbc.jdbc2;

import java.sql.*;
import java.util.*;

import com.mysql.jdbc.Field;

public class ResultSetMetaData extends com.mysql.jdbc.ResultSetMetaData
    implements java.sql.ResultSetMetaData
{
    /**
     * Initialize ResultSetMetaData for this ResultSet.
     *
     * @param rows the Vector of rows returned by the ResultSet
     * @param fields the array of field descriptors
     * 
     * @author Mark Matthews <mmatthew@worldserver.com>
     */
    
    ResultSetMetaData(Vector Rows, com.mysql.jdbc.Field[] Fields)
    {
	super(Rows, Fields);
    }

    //--------------------------JDBC 2.0-----------------------------------

    /**
     * JDBC 2.0
     *
     * <p>Return the fully qualified name of the Java class whose instances 
     * are manufactured if ResultSet.getObject() is called to retrieve a value 
     * from the column.  ResultSet.getObject() may return a subClass of the
     * class returned by this method.
     */

    public String getColumnClassName(int column) throws SQLException
    {
    	Field f = getField(column);
    	
    	switch (f.getSQLType())
		{
			case Types.BIT :
				return "java.lang.Boolean";
			case Types.TINYINT :
				if (f.isUnsigned())
				{
					return "java.lang.Integer";
				}
				else
				{
					return "java.lang.Byte";
				}
			case Types.SMALLINT :
				if (f.isUnsigned())
				{
					return "java.lang.Integer";
				}
				else
				{
					return "java.lang.Short";
				}
			case Types.INTEGER :
				if (f.isUnsigned())
				{
					return "java.lang.Long";
				}
				else
				{
					return "java.lang.Integer";
				}
			case Types.BIGINT :
				return "java.lang.Long";
			case Types.DECIMAL :
			case Types.NUMERIC :
				return "java.math.BigDecimal";
			case Types.REAL :
			case Types.FLOAT :
				return "java.lang.Float";
			case Types.DOUBLE :
				return "java.lang.Double";
			case Types.CHAR :
			case Types.VARCHAR :
			case Types.LONGVARCHAR :
				if (f.isBinary())
				{
					return "java.lang.Object";
				}
				else
				{
					return "java.lang.String";
				}
			case Types.BINARY :
			case Types.VARBINARY :
			case Types.LONGVARBINARY :
				if (!f.isBlob())
				{
					return "java.lang.String";
				}
				else if (!f.isBinary())
				{
					return "java.lang.String";
				}
				else
				{

					return "java.lang.Object";
				}

			case Types.DATE :
				return "java.sql.Date";
			case Types.TIME :
				return "java.sql.Time";
			case Types.TIMESTAMP :
				return "java.sql.Timestamp";
			default :
				return "java.lang.Object";
		}
	
    }
}
