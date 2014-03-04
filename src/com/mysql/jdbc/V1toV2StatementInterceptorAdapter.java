/*
  Copyright (c) 2009, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.Properties;

public class V1toV2StatementInterceptorAdapter implements StatementInterceptorV2 {
	private final StatementInterceptor toProxy;
	
	public V1toV2StatementInterceptorAdapter(StatementInterceptor toProxy) {
		this.toProxy = toProxy;
	}
	public ResultSetInternalMethods postProcess(String sql,
			Statement interceptedStatement,
			ResultSetInternalMethods originalResultSet, Connection connection,
			int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed, 
			SQLException statementException) throws SQLException {
		return toProxy.postProcess(sql, interceptedStatement, originalResultSet, connection);
	}

	public void destroy() {
		toProxy.destroy();
	}

	public boolean executeTopLevelOnly() {
		return toProxy.executeTopLevelOnly();
	}

	public void init(Connection conn, Properties props) throws SQLException {
		toProxy.init(conn, props);
	}

	public ResultSetInternalMethods preProcess(String sql,
			Statement interceptedStatement, Connection connection)
			throws SQLException {
		return toProxy.preProcess(sql, interceptedStatement, connection);
	}

}
