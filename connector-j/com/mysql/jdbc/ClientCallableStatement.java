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

