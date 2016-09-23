/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.integration.c3p0;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.QueryConnectionTester;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.core.exceptions.CJCommunicationsException;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;

/**
 * ConnectionTester for C3P0 connection pool that uses the more efficient COM_PING method of testing connection 'liveness' for MySQL, and 'sorts' exceptions
 * based on SQLState or class of 'CommunicationsException' for handling exceptions.
 */
public final class MysqlConnectionTester implements QueryConnectionTester {

    private static final long serialVersionUID = 3256444690067896368L;

    private static final Object[] NO_ARGS_ARRAY = new Object[0];

    private transient Method pingMethod;

    public MysqlConnectionTester() {
        try {
            this.pingMethod = JdbcConnection.class.getMethod("ping", (Class[]) null);
        } catch (Exception ex) {
            // punt, we have no way to recover, other than we now use 'SELECT 1' for handling the connection testing.
        }
    }

    public int activeCheckConnection(Connection con) {
        try {
            if (this.pingMethod != null) {
                if (con instanceof JdbcConnection) {
                    // We've been passed an instance of a MySQL connection -- no need for reflection
                    ((JdbcConnection) con).ping();
                } else {
                    // Assume the connection is a C3P0 proxy
                    C3P0ProxyConnection castCon = (C3P0ProxyConnection) con;
                    castCon.rawConnectionOperation(this.pingMethod, C3P0ProxyConnection.RAW_CONNECTION, NO_ARGS_ARRAY);
                }
            } else {
                Statement pingStatement = null;

                try {
                    pingStatement = con.createStatement();
                    pingStatement.executeQuery("SELECT 1").close();
                } finally {
                    if (pingStatement != null) {
                        pingStatement.close();
                    }
                }
            }

            return CONNECTION_IS_OKAY;
        } catch (Exception ex) {
            return CONNECTION_IS_INVALID;
        }
    }

    public int statusOnException(Connection arg0, Throwable throwable) {
        if (throwable instanceof CommunicationsException || throwable instanceof CJCommunicationsException) {
            return CONNECTION_IS_INVALID;
        }

        if (throwable instanceof SQLException) {
            String sqlState = ((SQLException) throwable).getSQLState();

            if (sqlState != null && sqlState.startsWith("08")) {
                return CONNECTION_IS_INVALID;
            }

            return CONNECTION_IS_OKAY;
        }

        // Runtime/Unchecked?

        return CONNECTION_IS_INVALID;
    }

    public int activeCheckConnection(Connection arg0, String arg1) {
        return CONNECTION_IS_OKAY;
    }
}
