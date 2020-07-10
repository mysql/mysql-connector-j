/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc.integration.c3p0;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.QueryConnectionTester;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.jdbc.JdbcConnection;
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

    @Override
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

    @Override
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

    @Override
    public int activeCheckConnection(Connection arg0, String arg1) {
        return CONNECTION_IS_OKAY;
    }
}
