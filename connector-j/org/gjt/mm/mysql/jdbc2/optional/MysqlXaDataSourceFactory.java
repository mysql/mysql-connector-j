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

public class MysqlXaDataSourceFactory implements ObjectFactory
{
	/**
	 * The class name for a standard Mysql DataSource.
	 */

	protected final String DataSourceClassName =
		"org.gjt.mm.mysql.jdbc2.optional.MysqlXaDataSource";

	public Object getObjectInstance(
		Object RefObj,
		Name Nm,
		Context Ctx,
		Hashtable Env)
		throws Exception
	{
		Reference Ref = (Reference) RefObj;

		if (Ref.getClassName().equals(DataSourceClassName))
		{
			MysqlXaDataSource MDS = new MysqlXaDataSource();

			int port_no = 3306;

			port_no = Integer.parseInt((String) Ref.get("port").getContent());
			MDS.setPort(port_no);

			MDS.setUser((String) Ref.get("user").getContent());
			MDS.setPassword((String) Ref.get("password").getContent());
			MDS.setServerName((String) Ref.get("serverName").getContent());
			MDS.setDatabaseName((String) Ref.get("databaseName").getContent());

			return MDS;
		}
		else
		{ // We can't create an instance of the reference
			return null;
		}
	}
}