package org.gjt.mm.mysql;

import java.sql.SQLException;

/**
 * Here for backwards compatibility with MM.MySQL
 */

public class Driver extends com.mysql.jdbc.Driver 
{
	public Driver() throws SQLException
	{
		super();
	}

}
