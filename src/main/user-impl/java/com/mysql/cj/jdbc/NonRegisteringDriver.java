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

import static com.mysql.cj.util.StringUtils.isNullOrEmpty;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrl.Type;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.UnableToConnectException;
import com.mysql.cj.exceptions.UnsupportedConnectionStringException;
import com.mysql.cj.jdbc.ha.FailoverConnectionProxy;
import com.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;
import com.mysql.cj.jdbc.ha.ReplicationConnectionProxy;
import com.mysql.cj.util.StringUtils;

/**
 * The Java SQL framework allows for multiple database drivers. Each driver should supply a class that implements the Driver interface.
 * 
 * <p>
 * The DriverManager will try to load as many drivers as it can find and then for any given connection request, it will ask each driver in turn to try to
 * connect to the target URL.
 * </p>
 * 
 * <p>
 * It is strongly recommended that each Driver class should be small and standalone so that the Driver class can be loaded and queried without bringing in vast
 * quantities of supporting code.
 * </p>
 * 
 * <p>
 * When a Driver class is loaded, it should create an instance of itself and register it with the DriverManager. This means that a user can load and register a
 * driver by doing Class.forName("foo.bar.Driver").
 * </p>
 */
public class NonRegisteringDriver implements java.sql.Driver {
    static {
        try {
            Class.forName(AbandonedConnectionCleanupThread.class.getName());
        } catch (ClassNotFoundException e) {
        }
    }

    /**
     * Construct a new driver and register it with DriverManager.
     * 
     * @throws SQLException
     *             if a database error occurs.
     */
    public NonRegisteringDriver() throws SQLException {
        // Required for Class.forName().newInstance().
    }

    /**
     * Standardizes OS name information to align with other drivers/clients for MySQL connection attributes.
     * 
     * @return the transformed, standardized OS name
     */
    public static String getOSName() {
        return Constants.OS_NAME;
    }

    /**
     * Standardizes platform information to align with other drivers/clients for MySQL connection attributes.
     * 
     * @return the transformed, standardized platform details
     */
    public static String getPlatform() {
        return Constants.OS_ARCH;
    }

    /**
     * Gets the drivers major version number.
     * 
     * @return the drivers major version number
     */
    static int getMajorVersionInternal() {
        return StringUtils.safeIntParse(Constants.CJ_MAJOR_VERSION);
    }

    /**
     * Get the drivers minor version number.
     * 
     * @return the drivers minor version number
     */
    static int getMinorVersionInternal() {
        return StringUtils.safeIntParse(Constants.CJ_MINOR_VERSION);
    }

    /**
     * Typically, drivers will return true if they understand the subprotocol specified in the URL and false if they don't. This driver's protocols start with
     * jdbc:mysql:
     * 
     * @param url
     *            the URL of the driver.
     * 
     * @return true if this driver accepts the given URL.
     * 
     * @exception SQLException
     *                if a database access error occurs or the url is null.
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return (ConnectionUrl.acceptsUrl(url));
    }

    /**
     * Try to make a database connection to the given URL. The driver should return "null" if it realizes it is the wrong kind of driver to connect to the given
     * URL. This will be common, as when the JDBC driverManager is asked to connect to a given URL, it passes the URL to each loaded driver in turn.
     * 
     * <p>
     * The driver should raise an SQLException if the URL is null or if it is the right driver to connect to the given URL, but has trouble connecting to the
     * database.
     * </p>
     * 
     * <p>
     * The java.util.Properties argument can be used to pass arbitrary string tag/value pairs as connection arguments. These properties take precedence over any
     * properties sent in the URL.
     * </p>
     * 
     * <p>
     * MySQL protocol takes the form: jdbc:mysql://host:port/database.
     * </p>
     * 
     * @param url
     *            the URL of the database to connect to.
     * @param info
     *            a list of arbitrary tag/value pairs as connection arguments.
     * 
     * @return a connection to the URL or null if it isn't us.
     * 
     * @exception SQLException
     *                if a database access error occurs or the url is {@code null}.
     */
    @Override
    public java.sql.Connection connect(String url, Properties info) throws SQLException {
        try {
            if (!ConnectionUrl.acceptsUrl(url)) {
                /*
                 * According to JDBC spec:
                 * The driver should return "null" if it realizes it is the wrong kind of driver to connect to the given URL. This will be common, as when the
                 * JDBC driver manager is asked to connect to a given URL it passes the URL to each loaded driver in turn.
                 */
                return null;
            }

            ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, info);
            switch (conStr.getType()) {
                case SINGLE_CONNECTION:
                    return com.mysql.cj.jdbc.ConnectionImpl.getInstance(conStr.getMainHost());

                case FAILOVER_CONNECTION:
                case FAILOVER_DNS_SRV_CONNECTION:
                    return FailoverConnectionProxy.createProxyInstance(conStr);

                case LOADBALANCE_CONNECTION:
                case LOADBALANCE_DNS_SRV_CONNECTION:
                    return LoadBalancedConnectionProxy.createProxyInstance(conStr);

                case REPLICATION_CONNECTION:
                case REPLICATION_DNS_SRV_CONNECTION:
                    return ReplicationConnectionProxy.createProxyInstance(conStr);

                default:
                    return null;
            }

        } catch (UnsupportedConnectionStringException e) {
            // When Connector/J can't handle this connection string the Driver must return null.
            return null;

        } catch (CJException ex) {
            throw ExceptionFactory.createException(UnableToConnectException.class,
                    Messages.getString("NonRegisteringDriver.17", new Object[] { ex.toString() }), ex);
        }
    }

    @Override
    public int getMajorVersion() {
        return getMajorVersionInternal();
    }

    @Override
    public int getMinorVersion() {
        return getMinorVersionInternal();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        String host = "";
        String port = "";
        String database = "";
        String user = "";
        String password = "";

        if (!isNullOrEmpty(url)) {
            ConnectionUrl connStr = ConnectionUrl.getConnectionUrlInstance(url, info);
            if (connStr.getType() == Type.SINGLE_CONNECTION) {
                HostInfo hostInfo = connStr.getMainHost();
                info = hostInfo.exposeAsProperties();
            }
        }

        if (info != null) {
            host = info.getProperty(PropertyKey.HOST.getKeyName());
            port = info.getProperty(PropertyKey.PORT.getKeyName());
            database = info.getProperty(PropertyKey.DBNAME.getKeyName());
            user = info.getProperty(PropertyKey.USER.getKeyName());
            password = info.getProperty(PropertyKey.PASSWORD.getKeyName());
        }

        DriverPropertyInfo hostProp = new DriverPropertyInfo(PropertyKey.HOST.getKeyName(), host);
        hostProp.required = true;
        hostProp.description = Messages.getString("NonRegisteringDriver.3");

        DriverPropertyInfo portProp = new DriverPropertyInfo(PropertyKey.PORT.getKeyName(), port);
        portProp.required = false;
        portProp.description = Messages.getString("NonRegisteringDriver.7");

        DriverPropertyInfo dbProp = new DriverPropertyInfo(PropertyKey.DBNAME.getKeyName(), database);
        dbProp.required = false;
        dbProp.description = Messages.getString("NonRegisteringDriver.10");

        DriverPropertyInfo userProp = new DriverPropertyInfo(PropertyKey.USER.getKeyName(), user);
        userProp.required = true;
        userProp.description = Messages.getString("NonRegisteringDriver.13");

        DriverPropertyInfo passwordProp = new DriverPropertyInfo(PropertyKey.PASSWORD.getKeyName(), password);
        passwordProp.required = true;
        passwordProp.description = Messages.getString("NonRegisteringDriver.16");

        JdbcPropertySet propSet = new JdbcPropertySetImpl();
        propSet.initializeProperties(info);
        List<DriverPropertyInfo> driverPropInfo = propSet.exposeAsDriverPropertyInfo();

        DriverPropertyInfo[] dpi = new DriverPropertyInfo[5 + driverPropInfo.size()];
        dpi[0] = hostProp;
        dpi[1] = portProp;
        dpi[2] = dbProp;
        dpi[3] = userProp;
        dpi[4] = passwordProp;
        System.arraycopy(driverPropInfo.toArray(new DriverPropertyInfo[0]), 0, dpi, 5, driverPropInfo.size());

        return dpi;
    }

    @Override
    public boolean jdbcCompliant() {
        // NOTE: MySQL is not SQL92 compliant.
        // TODO Is it true? DatabaseMetaData.supportsANSI92EntryLevelSQL() returns true...
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
