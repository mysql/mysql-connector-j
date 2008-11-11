/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems

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

import java.rmi.server.UID;
import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * Represents SQL SAVEPOINTS in MySQL.
 * 
 * @author Mark Matthews
 * 
 * @version $Id$
 */
public class MysqlSavepoint implements Savepoint {
	private static String getUniqueId() {
		// no need to re-invent the wheel here...
		String uidStr = new UID().toString();

		int uidLength = uidStr.length();

		StringBuffer safeString = new StringBuffer(uidLength);

		for (int i = 0; i < uidLength; i++) {
			char c = uidStr.charAt(i);

			if (Character.isLetter(c) || Character.isDigit(c)) {
				safeString.append(c);
			} else {
				safeString.append('_');
			}
		}

		return safeString.toString();
	}

	private String savepointName;
	
	private ExceptionInterceptor exceptionInterceptor;

	/**
	 * Creates an unnamed savepoint.
	 * 
	 * @param conn
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	MysqlSavepoint(ExceptionInterceptor exceptionInterceptor) throws SQLException {
		this(getUniqueId(), exceptionInterceptor);
	}

	/**
	 * Creates a named savepoint
	 * 
	 * @param name
	 *            the name of the savepoint.
	 * 
	 * @throws SQLException
	 *             if name == null or is empty.
	 */
	MysqlSavepoint(String name, ExceptionInterceptor exceptionInterceptor) throws SQLException {
		if (name == null || name.length() == 0) {
			throw SQLError.createSQLException("Savepoint name can not be NULL or empty",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
		}

		this.savepointName = name;
		
		this.exceptionInterceptor = exceptionInterceptor;
	}

	/**
	 * @see java.sql.Savepoint#getSavepointId()
	 */
	public int getSavepointId() throws SQLException {
		throw SQLError.createSQLException("Only named savepoints are supported.",
				SQLError.SQL_STATE_DRIVER_NOT_CAPABLE, exceptionInterceptor);
	}

	/**
	 * @see java.sql.Savepoint#getSavepointName()
	 */
	public String getSavepointName() throws SQLException {
		return this.savepointName;
	}
}
