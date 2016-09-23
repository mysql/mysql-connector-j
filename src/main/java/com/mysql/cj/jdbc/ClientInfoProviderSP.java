/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

import com.mysql.cj.api.jdbc.ClientInfoProvider;
import com.mysql.cj.core.conf.PropertyDefinitions;

public class ClientInfoProviderSP implements ClientInfoProvider {
    PreparedStatement setClientInfoSp;

    PreparedStatement getClientInfoSp;

    PreparedStatement getClientInfoBulkSp;

    public synchronized void initialize(java.sql.Connection conn, Properties configurationProps) throws SQLException {
        String identifierQuote = conn.getMetaData().getIdentifierQuoteString();
        String setClientInfoSpName = configurationProps.getProperty(PropertyDefinitions.PNAME_clientInfoSetSPName, "setClientInfo");
        String getClientInfoSpName = configurationProps.getProperty(PropertyDefinitions.PNAME_clientInfoGetSPName, "getClientInfo");
        String getClientInfoBulkSpName = configurationProps.getProperty(PropertyDefinitions.PNAME_clientInfoGetBulkSPName, "getClientInfoBulk");
        String clientInfoCatalog = configurationProps.getProperty(PropertyDefinitions.PNAME_clientInfoCatalog, ""); // "" means use current from connection

        String catalog = "".equals(clientInfoCatalog) ? conn.getCatalog() : clientInfoCatalog;

        this.setClientInfoSp = ((com.mysql.cj.api.jdbc.JdbcConnection) conn).clientPrepareStatement(
                "CALL " + identifierQuote + catalog + identifierQuote + "." + identifierQuote + setClientInfoSpName + identifierQuote + "(?, ?)");

        this.getClientInfoSp = ((com.mysql.cj.api.jdbc.JdbcConnection) conn).clientPrepareStatement(
                "CALL" + identifierQuote + catalog + identifierQuote + "." + identifierQuote + getClientInfoSpName + identifierQuote + "(?)");

        this.getClientInfoBulkSp = ((com.mysql.cj.api.jdbc.JdbcConnection) conn).clientPrepareStatement(
                "CALL " + identifierQuote + catalog + identifierQuote + "." + identifierQuote + getClientInfoBulkSpName + identifierQuote + "()");
    }

    public synchronized void destroy() throws SQLException {
        if (this.setClientInfoSp != null) {
            this.setClientInfoSp.close();
            this.setClientInfoSp = null;
        }

        if (this.getClientInfoSp != null) {
            this.getClientInfoSp.close();
            this.getClientInfoSp = null;
        }

        if (this.getClientInfoBulkSp != null) {
            this.getClientInfoBulkSp.close();
            this.getClientInfoBulkSp = null;
        }
    }

    public synchronized Properties getClientInfo(java.sql.Connection conn) throws SQLException {
        ResultSet rs = null;

        Properties props = new Properties();

        try {
            this.getClientInfoBulkSp.execute();

            rs = this.getClientInfoBulkSp.getResultSet();

            while (rs.next()) {
                props.setProperty(rs.getString(1), rs.getString(2));
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }

        return props;
    }

    public synchronized String getClientInfo(java.sql.Connection conn, String name) throws SQLException {
        ResultSet rs = null;

        String clientInfo = null;

        try {
            this.getClientInfoSp.setString(1, name);
            this.getClientInfoSp.execute();

            rs = this.getClientInfoSp.getResultSet();

            if (rs.next()) {
                clientInfo = rs.getString(1);
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }

        return clientInfo;
    }

    public synchronized void setClientInfo(java.sql.Connection conn, Properties properties) throws SQLClientInfoException {
        try {
            Enumeration<?> propNames = properties.propertyNames();

            while (propNames.hasMoreElements()) {
                String name = (String) propNames.nextElement();
                String value = properties.getProperty(name);

                setClientInfo(conn, name, value);
            }
        } catch (SQLException sqlEx) {
            SQLClientInfoException clientInfoEx = new SQLClientInfoException();
            clientInfoEx.initCause(sqlEx);

            throw clientInfoEx;
        }
    }

    public synchronized void setClientInfo(java.sql.Connection conn, String name, String value) throws SQLClientInfoException {
        try {
            this.setClientInfoSp.setString(1, name);
            this.setClientInfoSp.setString(2, value);
            this.setClientInfoSp.execute();
        } catch (SQLException sqlEx) {
            SQLClientInfoException clientInfoEx = new SQLClientInfoException();
            clientInfoEx.initCause(sqlEx);

            throw clientInfoEx;
        }
    }
}