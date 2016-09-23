/*
  Copyright (c) 2005, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;

import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.core.conf.PropertyDefinitions;

public class MysqlXADataSource extends MysqlDataSource implements javax.sql.XADataSource {

    static final long serialVersionUID = 7911390333152247455L;

    /**
     * Default no-arg constructor is required by specification.
     */
    public MysqlXADataSource() {
    }

    /**
     * @see javax.sql.XADataSource#getXAConnection()
     */
    public XAConnection getXAConnection() throws SQLException {

        Connection conn = getConnection();

        return wrapConnection(conn);
    }

    /**
     * @see javax.sql.XADataSource#getXAConnection(String, String)
     */
    public XAConnection getXAConnection(String u, String p) throws SQLException {

        Connection conn = getConnection(u, p);

        return wrapConnection(conn);
    }

    /**
     * Wraps a connection as a 'fake' XAConnection
     */

    private XAConnection wrapConnection(Connection conn) throws SQLException {
        if (getBooleanReadableProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection).getValue()
                || ((JdbcConnection) conn).getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection).getValue()) {
            return SuspendableXAConnection.getInstance((JdbcConnection) conn);
        }

        return MysqlXAConnection.getInstance((JdbcConnection) conn, getBooleanReadableProperty(PropertyDefinitions.PNAME_logXaCommands).getValue());
    }
}