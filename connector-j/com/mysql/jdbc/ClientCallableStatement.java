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

import java.util.Vector;
import java.util.StringTokenizer;

/**
 * Provides stored-procedure-like support to allow MM.MySQL to 
 * pass JDBC-compliance tests, even though MySQL does not have
 * stored procedures (yet).
 */

public class ClientCallableStatement 
{

	
	class ClientStoredProcedure
	{
		/** A list of arguments */
		
		Vector args;
		
		/** The SQL */
		
		String sql;
	}
	
	class StoredProcedureArg
	{
		String name;
		String type;
		int size;
		String direction;
		
		/**
		 * Creates a stored procedure argument based
		 * on a declaration of the form:
		 * 
		 * name type(size) input/output
		 * 
		 * where size is optional
		 */
		
		public StoredProcedureArg(String argDeclaration)
		{
			StringTokenizer st = new StringTokenizer(argDeclaration, " ", false);
			
			if (st.hasMoreTokens())
			{
				name = st.nextToken();
			}
			
			if (st.hasMoreTokens())
			{
				String typeAndSize = st.nextToken();
				
				int parenIdx = typeAndSize.indexOf("(");
				
				if (parenIdx != -1)
				{
					int endParenIdx = typeAndSize.indexOf(")", parenIdx);
					
					if (endParenIdx != -1)
					{
						String sizeAsString = typeAndSize.substring(parenIdx, endParenIdx);
						
						size = Integer.parseInt(sizeAsString);
					}
					else 
					{
						throw new IllegalArgumentException("Stored procedure argument type " +
							"size must be specified bewteen \"(\" and \")\"");
					}
				}
				else
				{
					type = typeAndSize;
				}
			}
			
			if (st.hasMoreTokens())
			{
				direction = st.nextToken();
			}
			else
			{
				direction = "input";
			}
			
			if (name == null)
			{
				throw new IllegalArgumentException("Stored procedure argument name must be given");
			}
			
			if (type == null)
			{
				throw new IllegalArgumentException("Stored procedure type must be given");
			}
			
			if (!direction.equalsIgnoreCase("input") && !direction.equalsIgnoreCase("output"))
			{
				throw new IllegalArgumentException("Stored procedure argument direction must be " +
					"either 'input' or 'output'");
			}		
			
		}
	}		
}

