/*
  Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.jdbc.NonRegisteringDriver;

/**
 * JDBC driver for Fabric MySQL connections. This driver will create connections for URLs of the form:
 * <i>jdbc:mysql:fabric://host:port/?fabricShardTable=employees.employees&amp;fabricShardKey=4621</i>.
 */
public class FabricMySQLDriver extends NonRegisteringDriver implements Driver {

    // TODO: this class should be merged with NonRegisteringDriver
    // TODO: local properties should be replaced with common one from PropertySet

    // may be extended to support other protocols in the future
    public static final String FABRIC_URL_PREFIX = "jdbc:mysql:fabric://";

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

        parsedProps.setProperty(PropertyDefinitions.PNAME_fabricProtocol, "http");

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

}
