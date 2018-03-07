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

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.AbstractRuntimeProperty;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.ReadableProperty;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.exceptions.SQLError;

/**
 * A JNDI DataSource for a Mysql JDBC connection
 */
public class MysqlDataSource extends JdbcPropertySetImpl implements DataSource, Referenceable, Serializable, JdbcPropertySet {

    static final long serialVersionUID = -5515846944416881264L;

    /** The driver to create connections with */
    protected final static NonRegisteringDriver mysqlDriver;

    static {
        try {
            mysqlDriver = new NonRegisteringDriver();
        } catch (Exception E) {
            throw new RuntimeException(Messages.getString("MysqlDataSource.0"));
        }
    }

    /** Log stream */
    protected transient PrintWriter logWriter = null;

    /** Database Name */
    protected String databaseName = null;

    /** Character Encoding */
    protected String encoding = null;

    /** Hostname */
    protected String hostName = null;

    /** Password */
    protected String password = null;

    /** The profileSQL property */
    protected String profileSQLString = "false";

    /** The JDBC URL */
    protected String url = null;

    /** User name */
    protected String user = null;

    /** Should we construct the URL, or has it been set explicitly */
    protected boolean explicitUrl = false;

    /** Port number */
    protected int port = 3306;

    protected String description = "MySQL Connector/J Data Source";

    /**
     * Default no-arg constructor for Serialization
     */
    public MysqlDataSource() {
    }

    /**
     * Creates a new connection using the already configured username and
     * password.
     * 
     * @return a connection to the database
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public java.sql.Connection getConnection() throws SQLException {
        return getConnection(this.user, this.password);
    }

    /**
     * Creates a new connection with the given username and password
     * 
     * @param userID
     *            the user id to connect with
     * @param password
     *            the password to connect with
     * 
     * @return a connection to the database
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public java.sql.Connection getConnection(String userID, String pass) throws SQLException {
        Properties props = exposeAsProperties();

        if (userID != null) {
            props.setProperty(PropertyDefinitions.PNAME_user, userID);
        }

        if (pass != null) {
            props.setProperty(PropertyDefinitions.PNAME_password, pass);
        }

        return getConnection(props);
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String value) {
        this.description = value;
    }

    /**
     * Sets the database name.
     * 
     * @param dbName
     *            the name of the database
     */
    public void setDatabaseName(String dbName) {
        this.databaseName = dbName;
    }

    /**
     * Gets the name of the database
     * 
     * @return the name of the database for this data source
     */
    public String getDatabaseName() {
        return (this.databaseName != null) ? this.databaseName : "";
    }

    /**
     * Sets the log writer for this data source.
     * 
     * @see javax.sql.DataSource#setLogWriter(PrintWriter)
     */
    public void setLogWriter(PrintWriter output) throws SQLException {
        this.logWriter = output;
    }

    /**
     * Returns the log writer for this data source
     * 
     * @return the log writer for this data source
     */
    public java.io.PrintWriter getLogWriter() {
        return this.logWriter;
    }

    /**
     * @param seconds
     * 
     * @throws SQLException
     */
    public void setLoginTimeout(int seconds) throws SQLException {
    }

    /**
     * Returns the login timeout
     * 
     * @return the login timeout
     */
    public int getLoginTimeout() {
        return 0;
    }

    /**
     * Sets the password
     * 
     * @param pass
     *            the password
     */
    public void setPassword(String pass) {
        this.password = pass;
    }

    /**
     * Sets the database port.
     * 
     * @param p
     *            the port
     */
    public void setPort(int p) {
        this.port = p;
    }

    /**
     * Returns the port number
     * 
     * @return the port number
     */
    public int getPort() {
        return this.port;
    }

    /**
     * Sets the port number
     * 
     * @param p
     *            the port
     * 
     * @see #setPort
     */
    public void setPortNumber(int p) {
        setPort(p);
    }

    /**
     * Returns the port number
     * 
     * @return the port number
     */
    public int getPortNumber() {
        return getPort();
    }

    /**
     * Initializes driver properties that come from a JNDI reference (in the
     * case of a javax.sql.DataSource bound into some name service that doesn't
     * handle Java objects directly).
     * 
     * @param ref
     *            The JNDI Reference that holds RefAddrs for all properties
     */
    public void setPropertiesViaRef(Reference ref) throws SQLException {
        for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
            ReadableProperty<?> propToSet = getReadableProperty(propName);

            if (ref != null) {
                propToSet.initializeFrom(ref, null);
            }
        }

        postInitialization();
    }

    /**
     * Required method to support this class as a <CODE>Referenceable</CODE>.
     * 
     * @return a Reference to this data source
     * 
     * @throws NamingException
     *             if a JNDI error occurs
     */
    public Reference getReference() throws NamingException {
        String factoryName = MysqlDataSourceFactory.class.getName();
        Reference ref = new Reference(getClass().getName(), factoryName, null);
        ref.add(new StringRefAddr(PropertyDefinitions.PNAME_user, getUser()));
        ref.add(new StringRefAddr(PropertyDefinitions.PNAME_password, this.password));
        ref.add(new StringRefAddr("serverName", getServerName()));
        ref.add(new StringRefAddr("port", "" + getPort()));
        ref.add(new StringRefAddr("databaseName", getDatabaseName()));
        ref.add(new StringRefAddr("url", getUrl()));
        ref.add(new StringRefAddr("explicitUrl", String.valueOf(this.explicitUrl)));

        //
        // Now store all of the 'non-standard' properties...
        //
        for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
            ReadableProperty<?> propToStore = getReadableProperty(propName);

            String val = propToStore.getStringValue();
            if (val != null) {
                ref.add(new StringRefAddr(propToStore.getPropertyDefinition().getName(), val));
            }

        }

        return ref;
    }

    /**
     * Sets the server name.
     * 
     * @param serverName
     *            the server name
     */
    public void setServerName(String serverName) {
        this.hostName = serverName;
    }

    /**
     * Returns the name of the database server
     * 
     * @return the name of the database server
     */
    public String getServerName() {
        return (this.hostName != null) ? this.hostName : "";
    }

    //
    // I've seen application servers use both formats, URL or url (doh)
    //

    /**
     * Sets the URL for this connection
     * 
     * @param url
     *            the URL for this connection
     */
    public void setURL(String url) {
        setUrl(url);
    }

    /**
     * Returns the URL for this connection
     * 
     * @return the URL for this connection
     */
    public String getURL() {
        return getUrl();
    }

    /**
     * This method is used by the app server to set the url string specified
     * within the datasource deployment descriptor. It is discovered using
     * introspection and matches if property name in descriptor is "url".
     * 
     * @param url
     *            url to be used within driver.connect
     */
    public void setUrl(String url) {
        this.url = url;
        this.explicitUrl = true;
    }

    /**
     * Returns the JDBC URL that will be used to create the database connection.
     * 
     * @return the URL for this connection
     */
    public String getUrl() {
        if (!this.explicitUrl) {
            StringBuilder sbUrl = new StringBuilder(ConnectionUrl.Type.SINGLE_CONNECTION.getProtocol());
            sbUrl.append("//").append(getServerName()).append(":").append(getPort()).append("/").append(getDatabaseName());
            return sbUrl.toString();
        }
        return this.url;
    }

    /**
     * Sets the user ID.
     * 
     * @param userID
     *            the User ID
     */
    public void setUser(String userID) {
        this.user = userID;
    }

    /**
     * Returns the configured user for this connection
     * 
     * @return the user for this connection
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Creates a connection using the specified properties.
     * 
     * @param props
     *            the properties to connect with
     * 
     * @return a connection to the database
     * 
     * @throws SQLException
     *             if an error occurs
     */
    protected java.sql.Connection getConnection(Properties props) throws SQLException {
        String jdbcUrlToUse = null;

        if (!this.explicitUrl) {
            jdbcUrlToUse = getUrl();
        } else {
            jdbcUrlToUse = this.url;
        }

        //
        // URL should take precedence over properties
        //
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(jdbcUrlToUse, null);
        if (connUrl.getType() == null) {
            throw SQLError.createSQLException(Messages.getString("MysqlDataSource.BadUrl", new Object[] { jdbcUrlToUse }),
                    MysqlErrorNumbers.SQL_STATE_CONNECTION_FAILURE, null);
        }
        Properties urlProps = connUrl.getConnectionArgumentsAsProperties();
        urlProps.remove(PropertyDefinitions.DBNAME_PROPERTY_KEY);
        urlProps.remove(PropertyDefinitions.HOST_PROPERTY_KEY);
        urlProps.remove(PropertyDefinitions.PORT_PROPERTY_KEY);
        urlProps.stringPropertyNames().stream().forEach(k -> props.setProperty(k, urlProps.getProperty(k)));

        return mysqlDriver.connect(jdbcUrlToUse, props);
    }

    //
    //	public boolean isWrapperFor(Class<?> iface) throws SQLException {
    //		throw SQLError.createSQLFeatureNotSupportedException();
    //	}
    //
    //	public <T> T unwrap(Class<T> iface) throws SQLException {
    //		throw SQLError.createSQLFeatureNotSupportedException();
    //	}

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name property name
     * @return
     * @throws SQLException
     */
    protected String getStringProperty(String name) throws SQLException {
        return getStringReadableProperty(name).getValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    protected void setStringProperty(String name, String value) throws SQLException {
        ReadableProperty<String> prop = getStringReadableProperty(name);
        if (prop.getPropertyDefinition().isRuntimeModifiable()) {
            getStringModifiableProperty(name).setValue(value);
        } else {
            ((AbstractRuntimeProperty<String>) prop).setFromString(value, null);
        }
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name
     * @return
     * @throws SQLException
     */
    protected boolean getBooleanProperty(String name) throws SQLException {
        return getBooleanReadableProperty(name).getValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    protected void setBooleanProperty(String name, boolean value) throws SQLException {
        ReadableProperty<Boolean> prop = getBooleanReadableProperty(name);
        if (prop.getPropertyDefinition().isRuntimeModifiable()) {
            getBooleanModifiableProperty(name).setValue(value);
        } else {
            ((AbstractRuntimeProperty<Boolean>) prop).setFromString("" + value, null);
        }
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name
     * @return
     * @throws SQLException
     */
    protected int getIntegerProperty(String name) throws SQLException {
        return getIntegerReadableProperty(name).getValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    protected void setIntegerProperty(String name, int value) throws SQLException {
        ReadableProperty<Integer> prop = getIntegerReadableProperty(name);
        if (prop.getPropertyDefinition().isRuntimeModifiable()) {
            getIntegerModifiableProperty(name).setValue(value);
        } else {
            ((AbstractRuntimeProperty<Integer>) prop).setFromString("" + value, null);
        }
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name
     * @return
     * @throws SQLException
     */
    protected long getLongProperty(String name) throws SQLException {
        return getLongReadableProperty(name).getValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    protected void setLongProperty(String name, long value) throws SQLException {
        ReadableProperty<Long> prop = getLongReadableProperty(name);
        if (prop.getPropertyDefinition().isRuntimeModifiable()) {
            getLongModifiableProperty(name).setValue(value);
        } else {
            ((AbstractRuntimeProperty<Long>) prop).setFromString("" + value, null);
        }
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name
     * @return
     * @throws SQLException
     */
    protected int getMemorySizeProperty(String name) throws SQLException {
        return getMemorySizeReadableProperty(name).getValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    protected void setMemorySizeProperty(String name, int value) throws SQLException {
        ReadableProperty<Integer> prop = getIntegerReadableProperty(name);
        if (prop.getPropertyDefinition().isRuntimeModifiable()) {
            getMemorySizeModifiableProperty(name).setValue(value);
        } else {
            ((AbstractRuntimeProperty<Integer>) prop).setFromString("" + value, null);
        }
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name
     * @return
     * @throws SQLException
     */
    protected String getEnumProperty(String name) throws SQLException {
        return getEnumReadableProperty(name).getStringValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     */
    protected void setEnumProperty(String name, String value) throws SQLException {
        ReadableProperty<?> prop = getEnumReadableProperty(name);
        if (prop.getPropertyDefinition().isRuntimeModifiable()) {
            getEnumModifiableProperty(name).setFromString(value, null);
        } else {
            ((AbstractRuntimeProperty<?>) prop).setFromString("" + value, null);
        }
    }

    @Override
    public Properties exposeAsProperties() {
        Properties props = new Properties();

        for (String propName : PropertyDefinitions.PROPERTY_NAME_TO_PROPERTY_DEFINITION.keySet()) {
            ReadableProperty<?> propToGet = getReadableProperty(propName);

            String propValue = propToGet.getStringValue();

            if (propValue != null && propToGet.isExplicitlySet()) {
                props.setProperty(propToGet.getPropertyDefinition().getName(), propValue);
            }
        }

        return props;
    }
}
