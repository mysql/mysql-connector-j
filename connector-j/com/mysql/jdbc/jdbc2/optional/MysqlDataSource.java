/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
package com.mysql.jdbc.jdbc2.optional;

import java.io.PrintWriter;
import java.io.Serializable;

import java.sql.SQLException;

import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

import javax.sql.DataSource;


/**
 * A JNDI DataSource for a Mysql JDBC connection
 */
public class MysqlDataSource
    implements DataSource,
               Referenceable,
               Serializable {

    //~ Instance/static variables .............................................

    /**
     * The driver to create connections with
     */
    protected static com.mysql.jdbc.Driver mysqlDriver = null;

    /** Should we construct the URL, or has it been set explicitly */
    protected boolean explicitUrl = false;
    protected String url = null;

    /**
     * Hostname
     */
    protected String hostName = null;

    /**
     * Port number
     */
    protected int port = 3306;

    /**
     * Database Name
     */
    protected String databaseName = null;

    /**
     * Character Encoding
     */
    protected String encoding = null;

    /**
     * User name
     */
    protected String user = null;

    /**
     * Password
     */
    protected String password = null;

    /**
     * Log stream
     */
    protected PrintWriter logWriter = null;
    protected String profileSql = "false";

    //~ Initializers ..........................................................

    static {

        try {
            mysqlDriver = (com.mysql.jdbc.Driver) Class.forName(
                                                           "com.mysql.jdbc.Driver")
                 .newInstance();
        } catch (Exception E) {
            throw new RuntimeException("Can not load Driver class com.mysql.jdbc.Driver");
        }
    }

    //~ Constructors ..........................................................

    /**
     * Default no-arg constructor for Serialization
     */
    public MysqlDataSource() {
    }

    //~ Methods ...............................................................

    /**
     * Creates a new connection using the already configured
     * username and password.
     */
    public java.sql.Connection getConnection()
                                      throws SQLException {

        return getConnection(user, password);
    }

    /**
     * Creates a new connection with the given username and password
     */
    public java.sql.Connection getConnection(String userID, String password)
                                      throws SQLException {

        Properties props = new Properties();

        if (userID == null) {
            userID = "";
        }

        if (password == null) {
            password = "";
        }

        props.put("user", userID);
        props.put("password", password);
        props.put("profileSql", getProfileSql());

        return getConnection(props);
    }

    /**
     * Creates a connection using the specified properties.
     */
    protected java.sql.Connection getConnection(Properties props)
                                         throws SQLException {

        String jdbcUrlToUse = null;

        if (!explicitUrl) {

            StringBuffer jdbcUrl = new StringBuffer("jdbc:mysql://");

            if (hostName != null) {
                jdbcUrl.append(hostName);
            }

            jdbcUrl.append(":");
            jdbcUrl.append(port);
            jdbcUrl.append("/");

            if (databaseName != null) {
                jdbcUrl.append(databaseName);
            }

            jdbcUrlToUse = jdbcUrl.toString();
        } else {
            jdbcUrlToUse = this.url;
        }

        return mysqlDriver.connect(jdbcUrlToUse, props);
    }

    /**
     * Gets the name of the database
     */
    public String getDatabaseName() {

        return databaseName;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public java.io.PrintWriter getLogWriter() {

        return logWriter;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     * @throws SQLException DOCUMENT ME!
     */
    public int getLoginTimeout()
                        throws SQLException {

        return 0;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int getPortNumber() {

        return getPort();
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public int getPort() {

        return port;
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String getProfileSql() {

        return profileSql;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param flag DOCUMENT ME!
     */
    public void setProfileSql(String flag) {
        profileSql = flag;
    }

    /**
     * Required method to support this class as a <CODE>Referenceable</CODE>.
     */
    public Reference getReference()
                           throws NamingException {

        String factoryName = "com.mysql.jdbc.jdbc2.optional.MysqlDataSourceFactory";
        Reference ref = new Reference(getClass().getName(), factoryName, null);
        ref.add(new StringRefAddr("user", getUser()));
        ref.add(new StringRefAddr("password", password));
        ref.add(new StringRefAddr("serverName", getServerName()));
        ref.add(new StringRefAddr("port", "" + getPort()));
        ref.add(new StringRefAddr("databaseName", getDatabaseName()));
        ref.add(new StringRefAddr("profileSql", getProfileSql()));

        return ref;
    }

    /**
     * Gets the name of the database server
     */
    public String getServerName() {

        return hostName;
    }

    /**
      * This method is used by the app server to set the url string specified  
      * within the datasource deployment descriptor.  It is discovered using
      * introspection and matches if property name in descriptor is "url".
      *
      * @param url url to be used within driver.connect
      * @exception java.sql.SQLException
      */
    public void setUrl(String url) {
        this.url = url;
        explicitUrl = true;
    }

    /**
     * Gets the JDBC URL that will be used to create the
     * database connection.
     */
    public String getUrl() {

        if (!explicitUrl) {

            String builtUrl = "jdbc:mysql://";
            builtUrl = builtUrl + getServerName() + ":" + getPort() + "/"
                  + getDatabaseName();

            return builtUrl;
        } else {

            return this.url;
        }
    }

    //
    // I've seen application servers use both formats
    // URL or url (doh)
    //

    /**
     * DOCUMENT ME!
     * 
     * @param url DOCUMENT ME!
     */
    public void setURL(String url) {
        setUrl(url);
    }

    /**
     * DOCUMENT ME!
     * 
     * @return DOCUMENT ME! 
     */
    public String getURL() {

        return getUrl();
    }

    /**
     * Gets the configured user for this connection
     */
    public String getUser() {

        return user;
    }

    /**
     * Sets the database name.
     * @param nom the name of the database
     */
    public void setDatabaseName(String dbName) {
        databaseName = dbName;
    }

    /**
     * Sets the log writer for this data source.
     */
    public void setLogWriter(PrintWriter output)
                      throws SQLException {
        logWriter = output;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param seconds DOCUMENT ME!
     * @throws SQLException DOCUMENT ME!
     */
    public void setLoginTimeout(int seconds)
                         throws SQLException {
    }

    /**
     * Sets the password
     */
    public void setPassword(String pass) {
        password = pass;
    }

    /**
     * Sets the database port.
     */
    public void setPort(int p) {
        port = p;
    }

    /**
     * DOCUMENT ME!
     * 
     * @param p DOCUMENT ME!
     */
    public void setPortNumber(int p) {
        setPort(p);
    }

    /**
     * Sets the server name.
     */
    public void setServerName(String serverName) {
        hostName = serverName;
    }

    /**
     * Sets the user ID.
     */
    public void setUser(String userID) {
        user = userID;
    }
}