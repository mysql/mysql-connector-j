/*
 Copyright (C) 2002-2005 MySQL AB

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
package com.mysql.jdbc.integration.jboss;

import java.sql.SQLException;

import org.jboss.resource.adapter.jdbc.ExceptionSorter;
import org.jboss.resource.adapter.jdbc.vendor.MySQLExceptionSorter;

/**
 * Exception sorter used for JBoss to make recovery of downed/stale connections
 * work more consistently.
 * 
 * @version $Id: ExtendedMysqlExceptionSorter.java,v 1.1.2.1 2005/05/13 18:58:42
 *          mmatthews Exp $
 */
public final class ExtendedMysqlExceptionSorter extends MySQLExceptionSorter {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.resource.adapter.jdbc.ExceptionSorter#isExceptionFatal(java.sql.SQLException)
	 */
	public boolean isExceptionFatal(SQLException ex) {
		String sqlState = ex.getSQLState();

		if (sqlState != null && sqlState.startsWith("08")) {
			return true;
		}

		return super.isExceptionFatal(ex);
	}

}
