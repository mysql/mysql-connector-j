/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.jdbc.integration.jboss;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.jboss.resource.adapter.jdbc.ValidConnectionChecker;

/**
 * A more efficient connection checker for JBoss.
 * 
 * @version $Id: MysqlValidConnectionChecker.java,v 1.1.2.1 2005/05/13 18:58:42
 *          mmatthews Exp $
 */
public final class MysqlValidConnectionChecker implements
		ValidConnectionChecker, Serializable {

	private static final long serialVersionUID = 8909421133577519177L;

	public MysqlValidConnectionChecker() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jboss.resource.adapter.jdbc.ValidConnectionChecker#isValidConnection(java.sql.Connection)
	 */
	public SQLException isValidConnection(Connection conn) {

		// Use "/* ping */ SELECT 1" which will send
		// pings across multi-connections too in case the connection
		// was "wrapped" by Jboss in any way...

		Statement pingStatement = null;

		try {
			pingStatement = conn.createStatement();
			
			pingStatement.executeQuery("/* ping */ SELECT 1").close();

			return null;
		} catch (SQLException sqlEx) {
			return sqlEx;
		} finally {
			if (pingStatement != null) {
				try {
					pingStatement.close();
				} catch (SQLException sqlEx) {
					// can't do anything about it here
				}
			}
		}
	}
}
