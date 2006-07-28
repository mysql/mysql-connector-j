/*
 Copyright (C) 2002-2004 MySQL AB

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

import java.util.Map;

/**
 * Used in commercially-licensed clients that require connections to
 * commercially-licensed servers as part of the licensing terms.
 * 
 * @author Mark Matthews
 * @version $Id: LicenseConfiguration.java,v 1.1.2.1 2005/05/13 18:58:38
 *          mmatthews Exp $
 */
class LicenseConfiguration {

	/**
	 * Used in commercially-licensed clients that require connections to
	 * commercially-licensed servers as part of the licensing terms.
	 * 
	 * @param serverVariables
	 *            a Map of the output of 'show variables' from the server we're
	 *            connecting to.
	 * 
	 * @throws SQLException
	 *             if commercial license is required, but not found
	 */
	static void checkLicenseType(Map serverVariables) throws SQLException {
		// This is a GPL build, so we don't check anything...
	}

	private LicenseConfiguration() {
		// this is a static utility class
	}
}
