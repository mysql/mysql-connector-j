/**
 * This class is used to obtain a physical connection and instantiate and return  
 * a MysqlPooledConnection.  J2EE application servers map client calls to 
 * dataSource.getConnection to this class based upon mapping set within deployment 
 * descriptor.  This class extends MysqlDataSource.
 *
 * @see javax.sql.PooledConnection
 * @see javax.sql.ConnectionPoolDataSource
 * @see org.gjt.mm.mysql.MysqlDataSource
 * @author Todd Wolff <todd.wolff@prodigy.net>
 */

package com.mysql.jdbc.jdbc2.optional;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.PooledConnection;
import javax.sql.ConnectionPoolDataSource;

public class MysqlConnectionPoolDataSource
	extends MysqlDataSource
	implements ConnectionPoolDataSource
{

	/**
	 * Returns a pooled connection.
	 *
	 * @exception java.sql.SQLException
	 */

	public synchronized PooledConnection getPooledConnection() throws SQLException
	{
		Connection connection = getConnection();
		MysqlPooledConnection mysqlPooledConnection =
			new MysqlPooledConnection(connection);
		return mysqlPooledConnection;
	}

	/**
	 * This method is invoked by the container.  Obtains physical connection using 
	 * mySql.Driver class and returns a mysqlPooledConnection object.
	 *
	 * @param s user name
	 * @param s1 password
	 * @exception java.sql.SQLException
	 */

	public synchronized PooledConnection getPooledConnection(String s, String s1)
		throws SQLException
	{

		Connection connection = getConnection(s, s1);
		MysqlPooledConnection mysqlPooledConnection =
			new MysqlPooledConnection(connection);
		return mysqlPooledConnection;
	}
}