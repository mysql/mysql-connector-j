/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA



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
