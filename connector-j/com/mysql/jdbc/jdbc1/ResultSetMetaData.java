/*
 * MM JDBC Drivers for MySQL
 *
 * $Id: ResultSetMetaData.java,v 1.2 2002/04/21 03:03:46 mark_matthews Exp $
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
 * @version $Id: ResultSetMetaData.java,v 1.2 2002/04/21 03:03:46 mark_matthews Exp $
 */

package com.mysql.jdbc.jdbc1;

import java.sql.*;
import java.util.*;

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
};
