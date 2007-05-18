/*
 Copyright (C) 2007 MySQL AB

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

import java.sql.SQLException;

/**
 * Implement this interface to be placed "inbetween" query execution, so that
 * you can influence it. (currently experimental).
 * 
 * @version $Id: $
 */

public interface StatementInterceptor {

	/**
	 * Called before the given statement is going to process the given SQL.
	 * 
	 * Interceptors are free to return a result set, and if so, the 
	 * server will not execute the query, and the given result set will be
	 * returned to the application instead.
	 */
	public abstract ResultSet preProcess(String sql, Statement interceptedStatement,
			Connection connection) throws SQLException;

	/**
	 * Called after the given statement has processed the given SQL.
	 * 
	 * Interceptors are free to inspect the "original" result set,
	 * and if a different result set is returned by the interceptor, 
	 * it is used in place of the "original" result set.
	 */
	public abstract ResultSet postProcess(String sql,
			Statement interceptedStatement, ResultSet originalResultSet,
			Connection connection) throws SQLException;

}
