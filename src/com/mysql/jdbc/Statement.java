/*
 Copyright  2007 MySQL AB, 2008-2009 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL
 as it is applied to this software. View the full text of the
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/

package com.mysql.jdbc;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This interface contains methods that are considered the "vendor extension"
 * to the JDBC API for MySQL's implementation of java.sql.Statement.
 *
 * For those looking further into the driver implementation, it is not
 * an API that is used for plugability of implementations inside our driver
 * (which is why there are still references to StatementImpl throughout the
 * code).
 *
 * @version $Id: $
 *
 */
public interface Statement extends java.sql.Statement {

	/**
	 * Workaround for containers that 'check' for sane values of
	 * Statement.setFetchSize() so that applications can use
	 * the Java variant of libmysql's mysql_use_result() behavior.
	 *
	 * @throws SQLException
	 */
	public abstract void enableStreamingResults() throws SQLException;

	/**
	 * Resets this statements fetch size and result set type to the values
	 * they had before enableStreamingResults() was called.
	 *
	 * @throws SQLException
	 */
	public abstract void disableStreamingResults() throws SQLException;

	/**
	 * Sets an InputStream instance that will be used to send data
	 * to the MySQL server for a "LOAD DATA LOCAL INFILE" statement
	 * rather than a FileInputStream or URLInputStream that represents
	 * the path given as an argument to the statement.
	 *
	 * This stream will be read to completion upon execution of a
	 * "LOAD DATA LOCAL INFILE" statement, and will automatically
	 * be closed by the driver, so it needs to be reset
	 * before each call to execute*() that would cause the MySQL
	 * server to request data to fulfill the request for
	 * "LOAD DATA LOCAL INFILE".
	 *
	 * If this value is set to NULL, the driver will revert to using
	 * a FileInputStream or URLInputStream as required.
	 */
	public abstract void setLocalInfileInputStream(InputStream stream);

	/**
	 * Returns the InputStream instance that will be used to send
	 * data in response to a "LOAD DATA LOCAL INFILE" statement.
	 *
	 * This method returns NULL if no such stream has been set
	 * via setLocalInfileInputStream().
	 */
	public abstract InputStream getLocalInfileInputStream();

	public void setPingTarget(PingTarget pingTarget);

	public ExceptionInterceptor getExceptionInterceptor();
	
	/** 
	 * Callback for result set instances to remove them from the Set that 
	 * tracks them per-statement 
	 */
	 
	public abstract void removeOpenResultSet(ResultSet rs);
	
	/**
	 * Returns the number of open result sets for this statement.
	 * @return
	 */
	public abstract int getOpenResultSetCount();
}