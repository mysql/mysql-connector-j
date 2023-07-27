/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * An implementation of ClientInfoProvider that exposes the client info as a comment prepended to all statements issued by the driver.
 *
 * Client information is <i>never</i> read from the server with this implementation, it is always cached locally.
 */

public class CommentClientInfoProvider implements ClientInfoProvider {

    private Properties clientInfo;

    @Override
    public synchronized void initialize(java.sql.Connection conn, Properties configurationProps) throws SQLException {
        this.clientInfo = new Properties();
    }

    @Override
    public synchronized void destroy() throws SQLException {
        this.clientInfo = null;
    }

    @Override
    public synchronized Properties getClientInfo(java.sql.Connection conn) throws SQLException {
        return this.clientInfo;
    }

    @Override
    public synchronized String getClientInfo(java.sql.Connection conn, String name) throws SQLException {
        return this.clientInfo.getProperty(name);
    }

    @Override
    public synchronized void setClientInfo(java.sql.Connection conn, Properties properties) throws SQLClientInfoException {
        this.clientInfo = new Properties();

        Enumeration<?> propNames = properties.propertyNames();

        while (propNames.hasMoreElements()) {
            String name = (String) propNames.nextElement();

            this.clientInfo.put(name, properties.getProperty(name));
        }

        setComment(conn);
    }

    @Override
    public synchronized void setClientInfo(java.sql.Connection conn, String name, String value) throws SQLClientInfoException {
        this.clientInfo.setProperty(name, value);
        setComment(conn);
    }

    private synchronized void setComment(java.sql.Connection conn) {
        StringBuilder commentBuf = new StringBuilder();

        Enumeration<?> propNames = this.clientInfo.propertyNames();

        while (propNames.hasMoreElements()) {
            String name = (String) propNames.nextElement();

            if (commentBuf.length() > 0) {
                commentBuf.append(", ");
            }

            commentBuf.append("" + name);
            commentBuf.append("=");
            commentBuf.append("" + this.clientInfo.getProperty(name));
        }

        ((com.mysql.cj.jdbc.JdbcConnection) conn).setStatementComment(commentBuf.toString());
    }

}
