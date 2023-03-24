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

package com.mysql.cj.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

public class ClientInfoProviderSP implements ClientInfoProvider {

    public static final String PNAME_clientInfoSetSPName = "clientInfoSetSPName";
    public static final String PNAME_clientInfoGetSPName = "clientInfoGetSPName";
    public static final String PNAME_clientInfoGetBulkSPName = "clientInfoGetBulkSPName";
    public static final String PNAME_clientInfoDatabase = "clientInfoDatabase";

    PreparedStatement setClientInfoSp;

    PreparedStatement getClientInfoSp;

    PreparedStatement getClientInfoBulkSp;

    private final ReentrantLock objectLock = new ReentrantLock();

    @Override
    public void initialize(java.sql.Connection conn, Properties configurationProps) throws SQLException {
        objectLock.lock();
        try {
            String identifierQuote = ((JdbcConnection) conn).getSession().getIdentifierQuoteString();
            String setClientInfoSpName = configurationProps.getProperty(PNAME_clientInfoSetSPName, "setClientInfo");
            String getClientInfoSpName = configurationProps.getProperty(PNAME_clientInfoGetSPName, "getClientInfo");
            String getClientInfoBulkSpName = configurationProps.getProperty(PNAME_clientInfoGetBulkSPName, "getClientInfoBulk");
            String clientInfoDatabase = configurationProps.getProperty(PNAME_clientInfoDatabase, ""); // "" means use current from connection

            String db = "".equals(clientInfoDatabase) ? ((JdbcConnection) conn).getDatabase() : clientInfoDatabase;

            this.setClientInfoSp = ((JdbcConnection) conn).clientPrepareStatement(
                    "CALL " + identifierQuote + db + identifierQuote + "." + identifierQuote + setClientInfoSpName + identifierQuote + "(?, ?)");

            this.getClientInfoSp = ((JdbcConnection) conn).clientPrepareStatement(
                    "CALL" + identifierQuote + db + identifierQuote + "." + identifierQuote + getClientInfoSpName + identifierQuote + "(?)");

            this.getClientInfoBulkSp = ((JdbcConnection) conn).clientPrepareStatement(
                    "CALL " + identifierQuote + db + identifierQuote + "." + identifierQuote + getClientInfoBulkSpName + identifierQuote + "()");
        } finally {
            objectLock.unlock();
        }
    }

    @Override
    public void destroy() throws SQLException {
        objectLock.lock();
        try {
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
        } finally {
            objectLock.unlock();
        }
    }

    @Override
    public Properties getClientInfo(java.sql.Connection conn) throws SQLException {
        objectLock.lock();
        try {
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
        } finally {
            objectLock.unlock();
        }
    }

    @Override
    public String getClientInfo(java.sql.Connection conn, String name) throws SQLException {
        objectLock.lock();
        try {
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
        } finally {
            objectLock.unlock();
        }
    }

    @Override
    public void setClientInfo(java.sql.Connection conn, Properties properties) throws SQLClientInfoException {
        objectLock.lock();
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
        } finally {
            objectLock.unlock();
        }
    }

    @Override
    public void setClientInfo(java.sql.Connection conn, String name, String value) throws SQLClientInfoException {
        objectLock.lock();
        try {
            this.setClientInfoSp.setString(1, name);
            this.setClientInfoSp.setString(2, value);
            this.setClientInfoSp.execute();
        } catch (SQLException sqlEx) {
            SQLClientInfoException clientInfoEx = new SQLClientInfoException();
            clientInfoEx.initCause(sqlEx);

            throw clientInfoEx;
        } finally {
            objectLock.unlock();
        }
    }
}
