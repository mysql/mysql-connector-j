/*
  Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

package com.mysql.cj.fabric.jdbc;

import java.util.Properties;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.fabric.exceptions.FabricCommunicationException;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.exceptions.SQLError;

/**
 * Relay the exception to {@link FabricMySQLConnectionProxy} for error reporting. This class exists solely because extensions cannot be provided with instances
 * but instead require the connection to instantiate the provided class.
 */
public class ErrorReportingExceptionInterceptor implements ExceptionInterceptor {
    private String hostname;
    private String port;
    private String fabricHaGroup;

    public Exception interceptException(Exception sqlEx, MysqlConnection conn) {
        JdbcConnection mysqlConn = (JdbcConnection) conn;

        // don't intercept exceptions during initialization, before the proxy has a chance to setProxy() on the physical connection
        if (ConnectionImpl.class.isAssignableFrom(mysqlConn.getMultiHostSafeProxy().getClass())) {
            return null;
        }

        FabricMySQLConnectionProxy fabricProxy = (FabricMySQLConnectionProxy) mysqlConn.getMultiHostSafeProxy();
        try {
            return fabricProxy.interceptException(sqlEx, conn, this.fabricHaGroup, this.hostname, this.port);
        } catch (FabricCommunicationException ex) {
            return SQLError.createSQLException("Failed to report error to Fabric.", SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE, ex, null);
        }
    }

    public void init(MysqlConnection conn, Properties props, Log log) {
        this.hostname = props.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY);
        this.port = props.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY);
        String connectionAttributes = props.getProperty(PropertyDefinitions.PNAME_connectionAttributes);
        this.fabricHaGroup = connectionAttributes.replaceAll("^.*\\bfabricHaGroup:(.+)\\b.*$", "$1");
    }

    public void destroy() {
    }
}
