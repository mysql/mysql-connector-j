package com.mysql.jdbc;

import java.sql.SQLException;

public interface LoadBalancedConnection extends MySQLConnection {
	
	public boolean addHost(String host) throws SQLException;
	public void removeHost(String host) throws SQLException;
	public void removeHostWhenNotInUse(String host)	throws SQLException;

}
