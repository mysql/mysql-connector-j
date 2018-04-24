/*
 * Copyright (c) 2005, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;

import com.mysql.cj.conf.PropertyDefinitions;

public class MysqlXADataSource extends MysqlDataSource implements javax.sql.XADataSource {

    static final long serialVersionUID = 7911390333152247455L;

    /**
     * Default no-arg constructor is required by specification.
     */
    public MysqlXADataSource() {
    }

    @Override
    public XAConnection getXAConnection() throws SQLException {

        Connection conn = getConnection();

        return wrapConnection(conn);
    }

    @Override
    public XAConnection getXAConnection(String u, String p) throws SQLException {

        Connection conn = getConnection(u, p);

        return wrapConnection(conn);
    }

    /**
     * Wraps a connection as a 'fake' XAConnection
     * 
     * @param conn
     *            connection to wrap
     * @return {@link XAConnection}
     * @throws SQLException
     *             if an error occurs
     */
    private XAConnection wrapConnection(Connection conn) throws SQLException {
        if (getBooleanProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection).getValue()
                || ((JdbcConnection) conn).getPropertySet().getBooleanProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection).getValue()) {
            return SuspendableXAConnection.getInstance((JdbcConnection) conn);
        }

        return MysqlXAConnection.getInstance((JdbcConnection) conn, getBooleanProperty(PropertyDefinitions.PNAME_logXaCommands).getValue());
    }
}
