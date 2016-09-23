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

package com.mysql.cj.fabric.jdbc;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.ConnectionUrl;
import com.mysql.cj.jdbc.MysqlDataSource;

/**
 * DataSource used to create connections to a MySQL fabric.
 */
public class FabricMySQLDataSource extends MysqlDataSource {

    private static final long serialVersionUID = 1L;

    /** Driver used to create connections. */
    private final static Driver driver;

    static {
        try {
            driver = new com.mysql.cj.jdbc.Driver();
        } catch (Exception ex) {
            throw new RuntimeException("Can create driver", ex);
        }
    }

    /**
     * Creates a connection using the specified properties.
     * copied directly from MysqlDataSource.getConnection().
     * No easy way to override the static `mysqlDriver' without
     * globally affecting the driver.
     * 
     * @param props
     *            the properties to connect with
     * 
     * @return a connection to the database
     * 
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    protected java.sql.Connection getConnection(Properties props) throws SQLException {
        String jdbcUrlToUse = null;

        if (!this.explicitUrl) {
            StringBuilder jdbcUrl = new StringBuilder(ConnectionUrl.Type.FABRIC_CONNECTION.getProtol());
            jdbcUrl.append("//").append(getServerName()).append(":").append(getPort()).append("/").append(getDatabaseName());
            jdbcUrlToUse = jdbcUrl.toString();
        } else {
            jdbcUrlToUse = this.url;
        }

        //
        // URL should take precedence over properties
        //
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(jdbcUrlToUse, null);
        Properties urlProps = connUrl.getConnectionArgumentsAsProperties();
        urlProps.remove(PropertyDefinitions.DBNAME_PROPERTY_KEY);
        urlProps.remove(PropertyDefinitions.HOST_PROPERTY_KEY);
        urlProps.remove(PropertyDefinitions.PORT_PROPERTY_KEY);
        urlProps.stringPropertyNames().stream().forEach(k -> props.setProperty(k, urlProps.getProperty(k)));

        if (this.fabricShardKey != null) {
            props.setProperty(PropertyDefinitions.PNAME_fabricShardKey, this.fabricShardKey);
        }
        if (this.fabricShardTable != null) {
            props.setProperty(PropertyDefinitions.PNAME_fabricShardTable, this.fabricShardTable);
        }
        if (this.fabricServerGroup != null) {
            props.setProperty(PropertyDefinitions.PNAME_fabricServerGroup, this.fabricServerGroup);
        }
        props.setProperty(PropertyDefinitions.PNAME_fabricProtocol, this.fabricProtocol);
        if (this.fabricUsername != null) {
            props.setProperty(PropertyDefinitions.PNAME_fabricUsername, this.fabricUsername);
        }
        if (this.fabricPassword != null) {
            props.setProperty(PropertyDefinitions.PNAME_fabricPassword, this.fabricPassword);
        }
        props.setProperty(PropertyDefinitions.PNAME_fabricReportErrors, Boolean.toString(this.fabricReportErrors));

        return driver.connect(jdbcUrlToUse, props);
    }

    private String fabricShardKey;
    private String fabricShardTable;
    private String fabricServerGroup;
    private String fabricProtocol = "http";
    private String fabricUsername;
    private String fabricPassword;
    private boolean fabricReportErrors = false;

    public void setFabricShardKey(String value) {
        this.fabricShardKey = value;
    }

    public String getFabricShardKey() {
        return this.fabricShardKey;
    }

    public void setFabricShardTable(String value) {
        this.fabricShardTable = value;
    }

    public String getFabricShardTable() {
        return this.fabricShardTable;
    }

    public void setFabricServerGroup(String value) {
        this.fabricServerGroup = value;
    }

    public String getFabricServerGroup() {
        return this.fabricServerGroup;
    }

    public void setFabricProtocol(String value) {
        this.fabricProtocol = value;
    }

    public String getFabricProtocol() {
        return this.fabricProtocol;
    }

    public void setFabricUsername(String value) {
        this.fabricUsername = value;
    }

    public String getFabricUsername() {
        return this.fabricUsername;
    }

    public void setFabricPassword(String value) {
        this.fabricPassword = value;
    }

    public String getFabricPassword() {
        return this.fabricPassword;
    }

    public void setFabricReportErrors(boolean value) {
        this.fabricReportErrors = value;
    }

    public boolean getFabricReportErrors() {
        return this.fabricReportErrors;
    }
}
