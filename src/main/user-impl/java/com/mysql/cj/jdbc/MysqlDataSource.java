/*
 * Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.
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
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;

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

    /** The JDBC URL */
    protected String url = null;

    /** Should we construct the URL, or has it been set explicitly? */
    protected boolean explicitUrl = false;

    /** Hostname */
    protected String hostName = null;

    /** Port number */
    protected int port = ConnectionUrl.DEFAULT_PORT;

    /** Was the port explicitly set? */
    protected boolean explicitPort = false;

    /** User name */
    protected String user = null;

    /** Password */
    protected String password = null;

    /** The profileSQL property */
    protected String profileSQLString = "false";

    protected String description = "MySQL Connector/J Data Source";

    /**
     * Default no-arg constructor for Serialization
     */
    public MysqlDataSource() {
    }

    @Override
    public java.sql.Connection getConnection() throws SQLException {
        return getConnection(this.user, this.password);
    }

    @Override
    public java.sql.Connection getConnection(String userID, String pass) throws SQLException {
        Properties props = exposeAsProperties();

        if (userID != null) {
            props.setProperty(PropertyKey.USER.getKeyName(), userID);
        }

        if (pass != null) {
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), pass);
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

    @Override
    public void setLogWriter(PrintWriter output) throws SQLException {
        this.logWriter = output;
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
        return this.logWriter;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        // TODO
    }

    @Override
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
     * Get the password.
     * 
     * @return password
     */
    public String getPassword() {
        return this.password;
    }

    /**
     * Sets the database port.
     * 
     * @param p
     *            the port
     */
    public void setPort(int p) {
        this.port = p;
        this.explicitPort = true;
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
     * @throws SQLException
     *             if error occurs
     */
    public void setPropertiesViaRef(Reference ref) throws SQLException {
        for (PropertyKey propKey : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.keySet()) {
            RuntimeProperty<?> propToSet = getProperty(propKey);

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
    @Override
    public Reference getReference() throws NamingException {
        String factoryName = MysqlDataSourceFactory.class.getName();
        Reference ref = new Reference(getClass().getName(), factoryName, null);
        ref.add(new StringRefAddr(PropertyKey.USER.getKeyName(), getUser()));
        ref.add(new StringRefAddr(PropertyKey.PASSWORD.getKeyName(), this.password));
        ref.add(new StringRefAddr("serverName", getServerName()));
        ref.add(new StringRefAddr("port", "" + getPort()));
        ref.add(new StringRefAddr("explicitPort", String.valueOf(this.explicitPort)));
        ref.add(new StringRefAddr("databaseName", getDatabaseName()));
        ref.add(new StringRefAddr("url", getUrl()));
        ref.add(new StringRefAddr("explicitUrl", String.valueOf(this.explicitUrl)));

        //
        // Now store all of the 'non-standard' properties...
        //
        for (PropertyKey propKey : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.keySet()) {
            RuntimeProperty<?> propToStore = getProperty(propKey);

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
            StringBuilder sbUrl = new StringBuilder(ConnectionUrl.Type.SINGLE_CONNECTION.getScheme());
            sbUrl.append("//").append(getServerName());
            try {
                if (this.explicitPort || !getBooleanRuntimeProperty(PropertyKey.dnsSrv.getKeyName())) {
                    sbUrl.append(":").append(getPort());
                }
            } catch (SQLException e) {
                // Should not happen, but if so, just add the port.
                sbUrl.append(":").append(getPort());
            }
            sbUrl.append("/").append(getDatabaseName());
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
        String jdbcUrlToUse = this.explicitUrl ? this.url : getUrl();

        //
        // URL should take precedence over properties
        //
        ConnectionUrl connUrl = ConnectionUrl.getConnectionUrlInstance(jdbcUrlToUse, null);
        Properties urlProps = connUrl.getConnectionArgumentsAsProperties();
        urlProps.remove(PropertyKey.HOST.getKeyName());
        urlProps.remove(PropertyKey.PORT.getKeyName());
        urlProps.remove(PropertyKey.DBNAME.getKeyName());
        urlProps.stringPropertyNames().stream().forEach(k -> props.setProperty(k, urlProps.getProperty(k)));

        return mysqlDriver.connect(jdbcUrlToUse, props);
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name property name
     * @return property value
     * @throws SQLException
     *             if error occurs
     */
    protected String getStringRuntimeProperty(String name) throws SQLException {
        return getStringProperty(name).getValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     *             if error occurs
     */
    protected void setStringRuntimeProperty(String name, String value) throws SQLException {
        ((AbstractRuntimeProperty<String>) getStringProperty(name)).setValueInternal(value, null, null);
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name
     * @return property value
     * @throws SQLException
     *             if error occurs
     */
    protected boolean getBooleanRuntimeProperty(String name) throws SQLException {
        return getBooleanProperty(name).getValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     *             if error occurs
     */
    protected void setBooleanRuntimeProperty(String name, boolean value) throws SQLException {
        ((AbstractRuntimeProperty<Boolean>) getBooleanProperty(name)).setValueInternal(value, null, null);
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name
     * @return property value
     * @throws SQLException
     *             if error occurs
     */
    protected int getIntegerRuntimeProperty(String name) throws SQLException {
        return getIntegerProperty(name).getValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     *             if error occurs
     */
    protected void setIntegerRuntimeProperty(String name, int value) throws SQLException {
        ((AbstractRuntimeProperty<Integer>) getIntegerProperty(name)).setValueInternal(value, null, null);
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name
     * @return property value
     * @throws SQLException
     *             if error occurs
     */
    protected long getLongRuntimeProperty(String name) throws SQLException {
        return getLongProperty(name).getValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     *             if error occurs
     */
    protected void setLongRuntimeProperty(String name, long value) throws SQLException {
        ((AbstractRuntimeProperty<Long>) getLongProperty(name)).setValueInternal(value, null, null);
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name
     * @return property value
     * @throws SQLException
     *             if error occurs
     */
    protected int getMemorySizeRuntimeProperty(String name) throws SQLException {
        return getMemorySizeProperty(name).getValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     *             if error occurs
     */
    protected void setMemorySizeRuntimeProperty(String name, int value) throws SQLException {
        ((AbstractRuntimeProperty<Integer>) getMemorySizeProperty(name)).setValueInternal(value, null, null);
    }

    /**
     * Used in properties getters added by instrumentation.
     * 
     * @param name
     *            property name
     * @return property value
     * @throws SQLException
     *             if error occurs
     */
    protected String getEnumRuntimeProperty(String name) throws SQLException {
        return getEnumProperty(name).getStringValue();
    }

    /**
     * Used in properties setters added by instrumentation.
     * 
     * @param name
     *            property name
     * @param value
     *            value
     * @throws SQLException
     *             if error occurs
     */
    protected void setEnumRuntimeProperty(String name, String value) throws SQLException {
        ((AbstractRuntimeProperty<?>) getEnumProperty(name)).setValueInternal(value, null);
    }

    @Override
    public Properties exposeAsProperties() {
        Properties props = new Properties();

        for (PropertyKey propKey : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.keySet()) {
            RuntimeProperty<?> propToGet = getProperty(propKey);

            String propValue = propToGet.getStringValue();

            if (propValue != null && propToGet.isExplicitlySet()) {
                props.setProperty(propToGet.getPropertyDefinition().getName(), propValue);
            }
        }

        return props;
    }
}
