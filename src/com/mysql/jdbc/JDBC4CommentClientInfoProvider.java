/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLClientInfoException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * An implementation of JDBC4ClientInfoProvider that exposes the client info as a comment prepended to all statements issued by the driver.
 * 
 * Client information is <i>never</i> read from the server with this implementation, it is always cached locally.
 */

public class JDBC4CommentClientInfoProvider implements JDBC4ClientInfoProvider {
    private Properties clientInfo;

    public synchronized void initialize(java.sql.Connection conn, Properties configurationProps) throws SQLException {
        this.clientInfo = new Properties();
    }

    public synchronized void destroy() throws SQLException {
        this.clientInfo = null;
    }

    public synchronized Properties getClientInfo(java.sql.Connection conn) throws SQLException {
        return this.clientInfo;
    }

    public synchronized String getClientInfo(java.sql.Connection conn, String name) throws SQLException {
        return this.clientInfo.getProperty(name);
    }

    public synchronized void setClientInfo(java.sql.Connection conn, Properties properties) throws SQLClientInfoException {
        this.clientInfo = new Properties();

        Enumeration propNames = properties.propertyNames();

        while (propNames.hasMoreElements()) {
            String name = (String) propNames.nextElement();

            this.clientInfo.put(name, properties.getProperty(name));
        }

        setComment(conn);
    }

    public synchronized void setClientInfo(java.sql.Connection conn, String name, String value) throws SQLClientInfoException {
        this.clientInfo.setProperty(name, value);
        setComment(conn);
    }

    private synchronized void setComment(java.sql.Connection conn) {
        StringBuffer commentBuf = new StringBuffer();
        Iterator elements = this.clientInfo.entrySet().iterator();

        while (elements.hasNext()) {
            if (commentBuf.length() > 0) {
                commentBuf.append(", ");
            }

            Map.Entry entry = (Map.Entry) elements.next();
            commentBuf.append("" + entry.getKey());
            commentBuf.append("=");
            commentBuf.append("" + entry.getValue());
        }

        ((com.mysql.jdbc.Connection) conn).setStatementComment(commentBuf.toString());
    }
}