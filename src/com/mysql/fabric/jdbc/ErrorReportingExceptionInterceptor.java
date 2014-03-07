/*
  Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric.jdbc;

import java.sql.SQLException;
import java.util.Properties;

import com.mysql.fabric.FabricCommunicationException;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.ExceptionInterceptor;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.SQLError;

/**
 * Relay the exception to {@link FabricMySQLConnectionProxy} for error
 * reporting.
 */
public class ErrorReportingExceptionInterceptor implements ExceptionInterceptor {
	private String hostname;
	private String port;
	private String fabricHaGroup;

	public SQLException interceptException(SQLException sqlEx, Connection conn) {
		MySQLConnection mysqlConn = (MySQLConnection) conn;

		// don't intercept exceptions during initialization, before
		// the proxy has a chance to setProxy() on the physical
		// connection
		if (ConnectionImpl.class.isAssignableFrom(mysqlConn.getLoadBalanceSafeProxy().getClass())) {
			return null;
		}

		FabricMySQLConnectionProxy fabricProxy = (FabricMySQLConnectionProxy)
			mysqlConn.getLoadBalanceSafeProxy();
		try {
			return fabricProxy.interceptException(sqlEx, conn, this.fabricHaGroup,
												  this.hostname, this.port);
		} catch (FabricCommunicationException ex) {
			return SQLError.createSQLException("Failed to report error to Fabric.",
											   SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE,
											   ex, conn.getExceptionInterceptor(), conn);
		}
	}

	public void init(Connection conn, Properties props) throws SQLException {
		this.hostname = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
		this.port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
		String connectionAttributes = props.getProperty("connectionAttributes");
		this.fabricHaGroup = connectionAttributes.replaceAll("^.*\\bfabricHaGroup:(.+)\\b.*$", "$1");
	}

	public void destroy() {
	}
}
