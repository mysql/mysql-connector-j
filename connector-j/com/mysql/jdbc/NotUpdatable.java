/*
   Copyright (C) 2002 MySQL AB
     This program is free software; you can redistribute it and/or modify
     it under the terms of the GNU General Public License as published by
     the Free Software Foundation; either version 2 of the License, or
     (at your option) any later version.
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


public class NotUpdatable
    extends SQLException
{

    //~ Instance/static variables .............................................

    public static final String NOT_UPDATEABLE_MESSAGE = 
        "Result Set not updateable. The " + 
         "query that generated this result set must select only one table, and must " + 
         "select all primary keys from that table. See the JDBC 2.1 API Specification, " + 
         "section 5.6 for more details.";

    //~ Constructors ..........................................................

    /**
     * Creates a new NotUpdatable object.
     */
    public NotUpdatable()
    {
        super(NOT_UPDATEABLE_MESSAGE, "S1000");
    }
}