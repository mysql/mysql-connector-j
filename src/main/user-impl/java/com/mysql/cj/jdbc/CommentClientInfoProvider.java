/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.jdbc;

import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * An implementation of ClientInfoProvider that exposes the client info as a comment prepended to all statements issued by the driver.
 *
 * Client information is <i>never</i> read from the server with this implementation, it is always cached locally.
 */

public class CommentClientInfoProvider implements ClientInfoProvider {

    private Properties clientInfo;

    @Override
    public synchronized void initialize(Connection conn, Properties configurationProps) throws SQLException {
        this.clientInfo = new Properties();
    }

    @Override
    public synchronized void destroy() throws SQLException {
        this.clientInfo = null;
    }

    @Override
    public synchronized Properties getClientInfo(Connection conn) throws SQLException {
        Properties clientInfoOut = new Properties();
        clientInfoOut.putAll(this.clientInfo);
        return clientInfoOut;
    }

    @Override
    public synchronized String getClientInfo(Connection conn, String name) throws SQLException {
        return this.clientInfo.getProperty(name);
    }

    @Override
    public synchronized void setClientInfo(Connection conn, Properties properties) throws SQLClientInfoException {
        this.clientInfo = new Properties();
        if (properties != null) {
            this.clientInfo.putAll(properties);
        }
        setComment(conn);
    }

    @Override
    public synchronized void setClientInfo(Connection conn, String name, String value) throws SQLClientInfoException {
        if (value == null) {
            this.clientInfo.remove(name);
        } else {
            this.clientInfo.setProperty(name, value);
        }
        setComment(conn);
    }

    private synchronized void setComment(Connection conn) throws SQLClientInfoException {
        String clientInfoComment = this.clientInfo.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(", "));
        try {
            conn.unwrap(JdbcConnection.class).setStatementComment(clientInfoComment);
        } catch (SQLException e) {
            SQLClientInfoException clientInfoEx = new SQLClientInfoException();
            clientInfoEx.initCause(e);
        }
    }

}
