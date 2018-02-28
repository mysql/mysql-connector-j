/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrl.Type;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.url.LoadbalanceConnectionUrl;
import com.mysql.cj.conf.url.ReplicationConnectionUrl;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.UnableToConnectException;
import com.mysql.cj.jdbc.ha.FailoverConnectionProxy;
import com.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;
import com.mysql.cj.jdbc.ha.ReplicationConnectionProxy;
import com.mysql.cj.protocol.NetworkResources;
import com.mysql.cj.util.StringUtils;

/**
 * The Java SQL framework allows for multiple database drivers. Each driver should supply a class that implements the Driver interface
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
 * driver by doing Class.forName("foo.bah.Driver")
 * </p>
 */
public class NonRegisteringDriver implements java.sql.Driver {

    protected static final ConcurrentHashMap<ConnectionPhantomReference, ConnectionPhantomReference> connectionPhantomRefs = new ConcurrentHashMap<>();

    protected static final ReferenceQueue<ConnectionImpl> refQueue = new ReferenceQueue<>();

    /*
     * Standardizes OS name information to align with other drivers/clients
     * for MySQL connection attributes
     * 
     * @return the transformed, standardized OS name
     */
    public static String getOSName() {
        return Constants.OS_NAME;
    }

    /*
     * Standardizes platform information to align with other drivers/clients
     * for MySQL connection attributes
     * 
     * @return the transformed, standardized platform details
     */
    public static String getPlatform() {
        return Constants.OS_ARCH;
    }

    static {
        try {
            Class.forName(AbandonedConnectionCleanupThread.class.getName());
        } catch (ClassNotFoundException e) {
            // ignore
        }
    }

    /**
     * Gets the drivers major version number
     * 
     * @return the drivers major version number
     */
    static int getMajorVersionInternal() {
        return StringUtils.safeIntParse(Constants.CJ_MAJOR_VERSION);
    }

    /**
     * Get the drivers minor version number
     * 
     * @return the drivers minor version number
     */
    static int getMinorVersionInternal() {
        return StringUtils.safeIntParse(Constants.CJ_MINOR_VERSION);
    }

    /**
     * Construct a new driver and register it with DriverManager
     * 
     * @throws SQLException
     *             if a database error occurs.
     */
    public NonRegisteringDriver() throws SQLException {
        // Required for Class.forName().newInstance()
    }

    /**
     * Typically, drivers will return true if they understand the subprotocol
     * specified in the URL and false if they don't. This driver's protocols
     * start with jdbc:mysql:
     * 
     * @param url
     *            the URL of the driver
     * 
     * @return true if this driver accepts the given URL
     * 
     * @exception SQLException
     *                if a database access error occurs or the url is null
     * 
     * @see java.sql.Driver#acceptsURL
     */
    public boolean acceptsURL(String url) throws SQLException {
        return (ConnectionUrl.acceptsUrl(url));
    }

    //
    // return the database name property
    //

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
     * MySQL protocol takes the form:
     * 
     * <PRE>
     * jdbc:mysql://host:port/database
     * </PRE>
     * 
     * </p>
     * 
     * @param url
     *            the URL of the database to connect to
     * @param info
     *            a list of arbitrary tag/value pairs as connection arguments
     * 
     * @return a connection to the URL or null if it isn't us
     * 
     * @exception SQLException
     *                if a database access error occurs or the url is {@code null}
     * 
     * @see java.sql.Driver#connect
     */
    public java.sql.Connection connect(String url, Properties info) throws SQLException {

        try {
            ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, info);
            if (conStr.getType() == null) {
                /*
                 * According to JDBC spec:
                 * The driver should return "null" if it realizes it is the wrong kind of driver to connect to the given URL. This will be common, as when the
                 * JDBC driver manager is asked to connect to a given URL it passes the URL to each loaded driver in turn.
                 */
                return null;
            }

            switch (conStr.getType()) {
                case LOADBALANCE_CONNECTION:
                    return LoadBalancedConnectionProxy.createProxyInstance((LoadbalanceConnectionUrl) conStr);

                case FAILOVER_CONNECTION:
                    return FailoverConnectionProxy.createProxyInstance(conStr);

                case REPLICATION_CONNECTION:
                    return ReplicationConnectionProxy.createProxyInstance((ReplicationConnectionUrl) conStr);

                case XDEVAPI_SESSION:
                    // TODO test it
                    //return new XJdbcConnection(conStr.getProperties());

                default:
                    return com.mysql.cj.jdbc.ConnectionImpl.getInstance(conStr.getMainHost());

            }

        } catch (CJException ex) {
            throw ExceptionFactory.createException(UnableToConnectException.class,
                    Messages.getString("NonRegisteringDriver.17", new Object[] { ex.toString() }), ex);
        }
    }

    protected static void trackConnection(JdbcConnection newConn) {

        ConnectionPhantomReference phantomRef = new ConnectionPhantomReference((ConnectionImpl) newConn, refQueue);
        connectionPhantomRefs.put(phantomRef, phantomRef);
    }

    /**
     * Gets the drivers major version number
     * 
     * @return the drivers major version number
     */
    public int getMajorVersion() {
        return getMajorVersionInternal();
    }

    /**
     * Get the drivers minor version number
     * 
     * @return the drivers minor version number
     */
    public int getMinorVersion() {
        return getMinorVersionInternal();
    }

    /**
     * The getPropertyInfo method is intended to allow a generic GUI tool to
     * discover what properties it should prompt a human for in order to get
     * enough information to connect to a database.
     * 
     * <p>
     * Note that depending on the values the human has supplied so far, additional values may become necessary, so it may be necessary to iterate through
     * several calls to getPropertyInfo
     * </p>
     * 
     * @param url
     *            the Url of the database to connect to
     * @param info
     *            a proposed list of tag/value pairs that will be sent on
     *            connect open.
     * 
     * @return An array of DriverPropertyInfo objects describing possible
     *         properties. This array may be an empty array if no properties are
     *         required
     * 
     * @exception SQLException
     *                if a database-access error occurs
     * 
     * @see java.sql.Driver#getPropertyInfo
     */
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
            host = info.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY);
            port = info.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY);
            database = info.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY);
            user = info.getProperty(PropertyDefinitions.PNAME_user);
            password = info.getProperty(PropertyDefinitions.PNAME_password);
        }

        DriverPropertyInfo hostProp = new DriverPropertyInfo(PropertyDefinitions.HOST_PROPERTY_KEY, host);
        hostProp.required = true;
        hostProp.description = Messages.getString("NonRegisteringDriver.3");

        DriverPropertyInfo portProp = new DriverPropertyInfo(PropertyDefinitions.PORT_PROPERTY_KEY, port);
        portProp.required = false;
        portProp.description = Messages.getString("NonRegisteringDriver.7");

        DriverPropertyInfo dbProp = new DriverPropertyInfo(PropertyDefinitions.DBNAME_PROPERTY_KEY, database);
        dbProp.required = false;
        dbProp.description = Messages.getString("NonRegisteringDriver.10");

        DriverPropertyInfo userProp = new DriverPropertyInfo(PropertyDefinitions.PNAME_user, user);
        userProp.required = true;
        userProp.description = Messages.getString("NonRegisteringDriver.13");

        DriverPropertyInfo passwordProp = new DriverPropertyInfo(PropertyDefinitions.PNAME_password, password);
        passwordProp.required = true;
        passwordProp.description = Messages.getString("NonRegisteringDriver.16");

        DriverPropertyInfo[] dpi;
        dpi = new JdbcPropertySetImpl().exposeAsDriverPropertyInfo(info, 5);

        dpi[0] = hostProp;
        dpi[1] = portProp;
        dpi[2] = dbProp;
        dpi[3] = userProp;
        dpi[4] = passwordProp;

        return dpi;
    }

    /**
     * Report whether the driver is a genuine JDBC compliant driver. A driver
     * may only report "true" here if it passes the JDBC compliance tests,
     * otherwise it is required to return false. JDBC compliance requires full
     * support for the JDBC API and full support for SQL 92 Entry Level.
     * 
     * <p>
     * MySQL is not SQL92 compliant
     * </p>
     * 
     * @return is this driver JDBC compliant?
     */
    public boolean jdbcCompliant() {
        return false;
    }

    static class ConnectionPhantomReference extends PhantomReference<ConnectionImpl> {
        private NetworkResources io;

        ConnectionPhantomReference(ConnectionImpl connectionImpl, ReferenceQueue<ConnectionImpl> q) {
            super(connectionImpl, q);

            this.io = connectionImpl.getSession().getNetworkResources();
        }

        void cleanup() {
            if (this.io != null) {
                try {
                    this.io.forceClose();
                } finally {
                    this.io = null;
                }
            }
        }
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
