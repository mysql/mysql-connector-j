/*
 Copyright  2007 MySQL AB, 2008 Sun Microsystems

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

package com.mysql.jdbc.interceptors;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.StatementInterceptor;
import com.mysql.jdbc.Util;

public class ServerStatusDiffInterceptor implements StatementInterceptor {

	private Map preExecuteValues = new HashMap();

	private Map postExecuteValues = new HashMap();

	public void init(Connection conn, Properties props) throws SQLException {

	}

	public ResultSetInternalMethods postProcess(String sql,
			Statement interceptedStatement,
			ResultSetInternalMethods originalResultSet, Connection connection)
			throws SQLException {

		if (connection.versionMeetsMinimum(5, 0, 2)) {
			populateMapWithSessionStatusValues(connection, this.postExecuteValues);
	
			connection.getLog().logInfo(
					"Server status change for statement:\n"
							+ Util.calculateDifferences(this.preExecuteValues,
									this.postExecuteValues));
		}

		return null; // we don't actually modify a result set

	}

	private void populateMapWithSessionStatusValues(Connection connection,
			Map toPopulate) throws SQLException {
		java.sql.Statement stmt = null;
		java.sql.ResultSet rs = null;

		try {
			toPopulate.clear();

			stmt = connection.createStatement();
			rs = stmt.executeQuery("SHOW SESSION STATUS");
			Util.resultSetToMap(toPopulate, rs);
		} finally {
			if (rs != null) {
				rs.close();
			}

			if (stmt != null) {
				stmt.close();
			}
		}
	}

	public ResultSetInternalMethods preProcess(String sql,
			Statement interceptedStatement, Connection connection)
			throws SQLException {

		if (connection.versionMeetsMinimum(5, 0, 2)) {
			populateMapWithSessionStatusValues(connection,
					this.preExecuteValues);
		}

		return null; // we don't actually modify a result set
	}

	public boolean executeTopLevelOnly() {
		return true;
	}

	public void destroy() {
		
	}
}
