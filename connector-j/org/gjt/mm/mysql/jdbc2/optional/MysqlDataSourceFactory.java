/*
 * MM JDBC Drivers for MySQL
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

package org.gjt.mm.mysql.jdbc2.optional;

import java.util.Hashtable;
import javax.naming.*;
import javax.naming.spi.ObjectFactory;

/**
 * Factory class for MysqlDataSource objects
 */

public class MysqlDataSourceFactory implements ObjectFactory
{
	/**
	 * The class name for a standard Mysql DataSource.
	 */

	protected final String dataSourceClassName =
		"org.gjt.mm.mysql.jdbc2.optional.MysqlDataSource";
	
	protected final String poolDataSourceName =
		"org.gjt.mm.mysql.jdbc2.optional.MysqlConnectionPoolDataSource";
		

	public Object getObjectInstance(
		Object refObj,
		Name nm,
		Context ctx,
		Hashtable env)
		throws Exception
	{
		Reference ref = (Reference) refObj;

		String className = ref.getClassName();
		
		if (className != null && 
		      (
				className.equals(dataSourceClassName) ||
			 	className.equals(poolDataSourceName)
			  )
			)
		{
			MysqlDataSource dataSource = null;
			
			try
			{
				dataSource = (MysqlDataSource)Class.forName(className).newInstance();
			}
			catch (Exception ex)
			{
				throw new RuntimeException("Unable to create DataSource of " +
					"class '" + className + "', reason: " + ex.toString());
			}
				
			int port_no = 1306;

			port_no = Integer.parseInt((String) ref.get("port").getContent());
			dataSource.setPort(port_no);

			dataSource.setUser((String) ref.get("user").getContent());
			dataSource.setPassword((String) ref.get("password").getContent());
			dataSource.setServerName((String) ref.get("serverName").getContent());
			dataSource.setDatabaseName((String) ref.get("databaseName").getContent());

			return dataSource;
		}
		else
		{ // We can't create an instance of the reference
			return null;
		}
	}
}