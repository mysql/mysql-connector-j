/*
  Copyright (c) 2013, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric.jdbc;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import com.mysql.jdbc.NonRegisteringDriver;

/**
 * JDBC driver for Fabric MySQL connections. This driver will create connections for URLs of the form:
 * <i>jdbc:mysql:fabric://host:port/?fabricShardTable=employees.employees&amp;fabricShardKey=4621</i>.
 */
public class FabricMySQLDriver extends NonRegisteringDriver implements Driver {
    // may be extended to support other protocols in the future
    public static final String FABRIC_URL_PREFIX = "jdbc:mysql:fabric://";

    // connection property keys
    public static final String FABRIC_SHARD_KEY_PROPERTY_KEY = "fabricShardKey";
    public static final String FABRIC_SHARD_TABLE_PROPERTY_KEY = "fabricShardTable";
    public static final String FABRIC_SERVER_GROUP_PROPERTY_KEY = "fabricServerGroup";
    public static final String FABRIC_PROTOCOL_PROPERTY_KEY = "fabricProtocol";
    public static final String FABRIC_USERNAME_PROPERTY_KEY = "fabricUsername";
    public static final String FABRIC_PASSWORD_PROPERTY_KEY = "fabricPassword";
    public static final String FABRIC_REPORT_ERRORS_PROPERTY_KEY = "fabricReportErrors";

    // Register ourselves with the DriverManager
    static {
        try {
            DriverManager.registerDriver(new FabricMySQLDriver());
        } catch (SQLException ex) {
            throw new RuntimeException("Can't register driver", ex);
        }
    }

    public FabricMySQLDriver() throws SQLException {
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Properties parsedProps = parseFabricURL(url, info);

        if (parsedProps == null) {
            return null;
        }

        parsedProps.setProperty(FABRIC_PROTOCOL_PROPERTY_KEY, "http");
        if (com.mysql.jdbc.Util.isJdbc4()) {
            try {
                Constructor<?> jdbc4proxy = Class.forName("com.mysql.fabric.jdbc.JDBC4FabricMySQLConnectionProxy")
                        .getConstructor(new Class[] { Properties.class });
                return (Connection) com.mysql.jdbc.Util.handleNewInstance(jdbc4proxy, new Object[] { parsedProps }, null);
            } catch (Exception e) {
                throw (SQLException) new SQLException(e.getMessage()).initCause(e);
            }
        }

        return new FabricMySQLConnectionProxy(parsedProps);
    }

    /**
     * Determine whether this is a valid Fabric MySQL URL. It should be of the form:
     * <i>jdbc:mysql:fabric://host:port/?options</i>.
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return parseFabricURL(url, null) != null;
    }

    /* static */Properties parseFabricURL(String url, Properties defaults) throws SQLException {
        if (!url.startsWith("jdbc:mysql:fabric://")) {
            return null;
        }
        // We have to fudge the URL here to get NonRegisteringDriver.parseURL() to parse it for us.
        // It actually checks the prefix and bails if it's not recognized.
        // jdbc:mysql:fabric:// => jdbc:mysql://
        return super.parseURL(url.replaceAll("fabric:", ""), defaults);
    }

    public Logger getParentLogger() throws SQLException {
        throw new SQLException("no logging");
    }
}
