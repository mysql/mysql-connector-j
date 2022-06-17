/*
 * Copyright (c) 2022, Oracle and/or its affiliates.
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

package com.mysql.cj.osgi;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.XADataSource;

import org.osgi.service.jdbc.DataSourceFactory;

import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.MysqlXADataSource;
import com.mysql.cj.jdbc.NonRegisteringDriver;

public class ConnectorJDataSourceFactory implements DataSourceFactory {

    @Override
    public DataSource createDataSource(Properties props) throws SQLException {
        MysqlDataSource dataSource = new MysqlDataSource();
        setBasicProperties(dataSource, props);
        return dataSource;
    }

    private void setBasicProperties(MysqlDataSource dataSource, Properties props) throws SQLException {
        if (props == null || props.isEmpty()) {
            return;
        }
        setProperty(JDBC_DATABASE_NAME, dataSource::setDatabaseName, props);
        //Supported? setProperty(JDBC_DATASOURCE_NAME, dataSource::, props);
        setProperty(JDBC_DESCRIPTION, dataSource::setDescription, props);
        //Supported? setProperty(JDBC_NETWORK_PROTOCOL, dataSource::, props);
        setProperty(JDBC_PASSWORD, dataSource::setPassword, props);
        setProperty(JDBC_PORT_NUMBER, dataSource::setPortNumber, props);
        //Supported? setProperty(JDBC_ROLE_NAME, dataSource::, props);
        setProperty(JDBC_SERVER_NAME, dataSource::setServerName, props);
        setProperty(JDBC_USER, dataSource::setUser, props);
        setProperty(JDBC_URL, dataSource::setURL, props);
    }

    private static void setProperty(String key, Consumer<String> setter, Properties props) {
        String value = props.getProperty(key);
        if (value != null) {
            setter.accept(value);
        }
    }

    private static void setProperty(String key, IntConsumer setter, Properties props) throws SQLException {
        String value = props.getProperty(key);
        if (value != null) {
            try {
                setter.accept(Integer.parseInt(value));
            } catch (NumberFormatException e) {
                throw new SQLException("can't parse " + key + " as an int", e);
            }
        }
    }

    @Override
    public ConnectionPoolDataSource createConnectionPoolDataSource(Properties props) throws SQLException {
        MysqlConnectionPoolDataSource poolDataSource = new MysqlConnectionPoolDataSource();
        setBasicProperties(poolDataSource, props);
        return poolDataSource;
    }

    @Override
    public XADataSource createXADataSource(Properties props) throws SQLException {
        MysqlXADataSource xaDataSource = new MysqlXADataSource();
        setBasicProperties(xaDataSource, props);
        return xaDataSource;
    }

    @Override
    public Driver createDriver(Properties props) throws SQLException {
        //we use the non registering one here, as no need to register it
        return new NonRegisteringDriver();
    }

}
