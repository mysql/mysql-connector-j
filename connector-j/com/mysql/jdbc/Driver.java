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

package com.mysql.jdbc;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;

import java.util.Properties;
import java.util.StringTokenizer;

/**
 * The Java SQL framework allows for multiple database drivers.  Each
 * driver should supply a class that implements the Driver interface
 *
 * <p>The DriverManager will try to load as many drivers as it can find and
 * then for any given connection request, it will ask each driver in turn
 * to try to connect to the target URL.
 *
 * <p>It is strongly recommended that each Driver class should be small and
 * standalone so that the Driver class can be loaded and queried without
 * bringing in vast quantities of supporting code.
 *
 * <p>When a Driver class is loaded, it should create an instance of itself
 * and register it with the DriverManager.  This means that a user can load
 * and register a driver by doing Class.forName("foo.bah.Driver")
 *
 * @see org.gjt.mm.mysql.Connection
 * @see java.sql.Driver
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */
public class Driver
    implements java.sql.Driver
{

    //~ Instance/static variables .............................................

    public static final boolean debug = false;
    public static final boolean trace = false;

    //
    // Version Info
    //
    static final int _MAJORVERSION = 2;
    static final int _MINORVERSION = 0;

    //~ Initializers ..........................................................

    //
    // Register ourselves with the DriverManager
    //
    static {

        try {
            java.sql.DriverManager.registerDriver(new Driver());
        } catch (java.sql.SQLException E) {
            throw new RuntimeException("Can't register driver!");
        }

        if (debug) {
            Debug.trace("ALL");
        }
    }

    //~ Constructors ..........................................................

    /**
     * Construct a new driver and register it with DriverManager
     *
     * @exception java.sql.SQLException
     */
    public Driver()
           throws java.sql.SQLException
    {

        // Required for Class.forName().newInstance()
    }

    //~ Methods ...............................................................

    /**
     * Gets the drivers major version number
     *
     * @return the drivers major version number
     */
    public int getMajorVersion()
    {

        return _MAJORVERSION;
    }

    /**
     * Get the drivers minor version number
     *
     * @return the drivers minor version number
     */
    public int getMinorVersion()
    {

        return _MINORVERSION;
    }

    /**
     * The getPropertyInfo method is intended to allow a generic GUI
     * tool to discover what properties it should prompt a human for
     * in order to get enough information to connect to a database.
     *
     * <p>Note that depending on the values the human has supplied so
     * far, additional values may become necessary, so it may be necessary
     * to iterate through several calls to getPropertyInfo
     *
     * @param url the Url of the database to connect to
     * @param info a proposed list of tag/value pairs that will be sent on
     *    connect open.
     * @return An array of DriverPropertyInfo objects describing
     *    possible properties.  This array may be an empty array if
     *    no properties are required
     * @exception java.sql.SQLException if a database-access error occurs
     * @see java.sql.Driver#getPropertyInfo
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
                                         throws java.sql.SQLException
    {
        info = parseURL(url, info);

        DriverPropertyInfo HostProp = new DriverPropertyInfo("HOST", 
                                                             info.getProperty(
                                                                     "HOST"));
        HostProp.required          = true;
        HostProp.description       = "Hostname of MySQL Server";

        DriverPropertyInfo PortProp = new DriverPropertyInfo("PORT", 
                                                             info.getProperty(
                                                                     "PORT", 
                                                                     "3306"));
        PortProp.required          = false;
        PortProp.description       = "Port number of MySQL Server";

        DriverPropertyInfo DBProp  = new DriverPropertyInfo("DBNAME", 
                                                            info.getProperty(
                                                                    "DBNAME"));
        DBProp.required            = false;
        DBProp.description         = "Database name";

        DriverPropertyInfo UserProp = new DriverPropertyInfo("user", 
                                                             info.getProperty(
                                                                     "user"));
        UserProp.required          = true;
        UserProp.description       = "Username to authenticate as";

        DriverPropertyInfo PasswordProp = new DriverPropertyInfo("password", 
                                                                 info.getProperty(
                                                                         "password"));
        PasswordProp.required      = true;
        PasswordProp.description   = "Password to use for authentication";

        DriverPropertyInfo AutoReconnect = new DriverPropertyInfo(
                                                   "autoReconnect", 
                                                   info.getProperty(
                                                           "autoReconnect", 
                                                           "false"));
        AutoReconnect.required     = false;
        AutoReconnect.choices      = new String[] { "true", "false" };
        AutoReconnect.description  = "Should the driver try to re-establish bad connections?";

        DriverPropertyInfo MaxReconnects = new DriverPropertyInfo(
                                                   "maxReconnects", 
                                                   info.getProperty(
                                                           "maxReconnects", 
                                                           "3"));
        MaxReconnects.required     = false;
        MaxReconnects.description  = "Maximum number of reconnects to attempt if autoReconnect is true";
        ;

        DriverPropertyInfo InitialTimeout = new DriverPropertyInfo(
                                                    "initialTimeout", 
                                                    info.getProperty(
                                                            "initialTimeout", 
                                                            "2"));
        InitialTimeout.required    = false;
        InitialTimeout.description = "Initial timeout (seconds) to wait between failed connections";

        DriverPropertyInfo profileSql = new DriverPropertyInfo("profileSql", 
                                                               info.getProperty(
                                                                       "profileSql", 
                                                                       "false"));
        profileSql.required    = false;
        profileSql.description = "Trace queries and their execution/fetch times on STDERR (true/false) defaults to false";
        ;

        DriverPropertyInfo[] Dpi = {
            HostProp, PortProp, DBProp, UserProp, PasswordProp, AutoReconnect, 
            MaxReconnects, InitialTimeout, profileSql
        };

        return Dpi;
    }

    /**
     * Typically, drivers will return true if they understand
     * the subprotocol specified in the URL and false if they don't.  This
     * driver's protocols start with jdbc:mysql:
     *
     * @see java.sql.Driver#acceptsURL
     * @param url the URL of the driver
     * @return true if this driver accepts the given URL
     * @exception java.sql.SQLException if a database-access error occurs
     */
    public synchronized boolean acceptsURL(String Url)
                                    throws java.sql.SQLException
    {

        if (parseURL(Url, null) == null) {

            return false;
        } else {

            return true;
        }
    }

    /**
     * Try to make a database connection to the given URL.  The driver
     * should return "null" if it realizes it is the wrong kind of
     * driver to connect to the given URL.  This will be common, as
     * when the JDBC driverManager is asked to connect to a given URL,
     * it passes the URL to each loaded driver in turn.
     *
     * <p>
     * The driver should raise an java.sql.SQLException if it is
     * the right driver
     * to connect to the given URL, but has trouble connecting to the
     * database.
     *
     * <p>
     * The java.util.Properties argument can be used to pass arbitrary
     * string tag/value pairs as connection arguments.
     *
     * <p>
     * My protocol takes the form:
     * <PRE>
     *    jdbc:mysql://host:port/database
     * </PRE>
     *
     * @param url the URL of the database to connect to
     * @param info a list of arbitrary tag/value pairs as connection
     *    arguments
     * @return a connection to the URL or null if it isnt us
     * @exception java.sql.SQLException if a database access error occurs
     * @see java.sql.Driver#connect
     */
    public synchronized java.sql.Connection connect(String url, 
                                                    Properties info)
                                             throws java.sql.SQLException
    {

        Properties props = null;

        if ((props = parseURL(url, info)) == null) {

            return null;
        } else {

            try {

                Connection newConn = new com.mysql.jdbc.Connection();
                newConn.connectionInit(host(props), port(props), props, 
                                       database(props), url, this);

                return (java.sql.Connection)newConn;
            } // Don't wrap SQLExceptions, throw 
            catch (SQLException sqlEx) {

                // them un-changed.
                throw sqlEx;
            }
             catch (Exception ex) {
                throw new SQLException("Cannot load connection class because of underlying exception: '" + 
                                       ex.toString() + "'.", "08001");
            }
        }
    }

    //
    // return the database name property
    //
    public String database(Properties props)
    {

        return props.getProperty("DBNAME");
    }

    /**
     * Returns the hostname property
     */
    public String host(Properties props)
    {

        return props.getProperty("HOST", "localhost");
    }

    /**
     * Report whether the driver is a genuine JDBC compliant driver.  A
     * driver may only report "true" here if it passes the JDBC compliance
     * tests, otherwise it is required to return false.  JDBC compliance
     * requires full support for the JDBC API and full support for SQL 92
     * Entry Level.
     *
     * <p>MySQL is not SQL92 compliant
     */
    public boolean jdbcCompliant()
    {

        return false;
    }

    /**
     * Returns the port number property
     */
    public int port(Properties props)
    {

        return Integer.parseInt(props.getProperty("PORT", "3306"));
    }

    //
    // return the value of any property this driver knows about
    //
    public String property(String Name, Properties props)
    {

        return props.getProperty(Name);
    }

    /**
     * Constructs a new DriverURL, splitting the specified URL into its
     * component parts
     * @param url JDBC URL to parse
     * @param defaults Default properties
     * @return Properties with elements added from the url
     * @exception java.sql.SQLException
     */

    //
    // This is a new URL-parser. This file no longer contains any
    // Postgresql code.
    //
    Properties parseURL(String Url, Properties Defaults)
                 throws java.sql.SQLException
    {

        Properties URLProps = new Properties(Defaults);

        if (Url == null) {

            return null;
        } else {

            /*
            * Parse parameters after the ? in the URL and remove
            * them from the original URL.
            */
            int index = Url.indexOf("?");

            if (index != -1) {

                String ParamString = Url.substring(index + 1, Url.length());
                Url = Url.substring(0, index);

                StringTokenizer QueryParams = new StringTokenizer(ParamString, 
                                                                  "&");

                while (QueryParams.hasMoreTokens()) {

                    StringTokenizer VP    = new StringTokenizer(QueryParams.nextToken(), 
                                                                "=");
                    String          Param = "";

                    if (VP.hasMoreTokens()) {
                        Param = VP.nextToken();
                    }

                    String Value = "";

                    if (VP.hasMoreTokens()) {
                        Value = VP.nextToken();
                    }

                    if (Value.length() > 0 && Param.length() > 0) {
                        URLProps.put(Param, Value);
                    }
                }
            }

            StringTokenizer ST = new StringTokenizer(Url, ":/", true);

            if (ST.hasMoreTokens()) {

                String Protocol = ST.nextToken();

                if (Protocol != null) {

                    if (!Protocol.toLowerCase().equals("jdbc")) {

                        return null;
                    }
                } else {

                    return null;
                }
            } else {

                return null;
            }

            // Look for the colon following 'jdbc'
            if (ST.hasMoreTokens()) {

                String Colon = ST.nextToken();

                if (Colon != null) {

                    if (!Colon.equals(":")) {

                        return null;
                    }
                } else {

                    return null;
                }
            } else {

                return null;
            }

            // Look for sub-protocol to be mysql
            if (ST.hasMoreTokens()) {

                String SubProto = ST.nextToken();

                if (SubProto != null) {

                    if (!SubProto.toLowerCase().equals("mysql")) {

                        return null;
                    }
                } else {

                    return null;
                }
            } else {

                return null;
            }

            // Look for the colon following 'mysql'
            if (ST.hasMoreTokens()) {

                String Colon = ST.nextToken();

                if (Colon != null) {

                    if (!Colon.equals(":")) {

                        return null;
                    }
                } else {

                    return null;
                }
            } else {

                return null;
            }

            // Look for the "//" of the URL
            if (ST.hasMoreTokens()) {

                String Slash  = ST.nextToken();
                String Slash2 = "";

                if (ST.hasMoreTokens()) {
                    Slash2 = ST.nextToken();
                }

                if (Slash != null && Slash2 != null) {

                    if (!Slash.equals("/") && !Slash2.equals("/")) {

                        return null;
                    }
                } else {

                    return null;
                }
            } else {

                return null;
            }

            // Okay the next one is a candidate for many things
            if (ST.hasMoreTokens()) {

                String Token = ST.nextToken();

                if (Token != null) {

                    if (!Token.equals(":") && !Token.equals("/")) {

                        // Must be hostname
                        URLProps.put("HOST", Token);

                        if (ST.hasMoreTokens()) {
                            Token = ST.nextToken();
                        } else {

                            return null;
                        }
                    }

                    // Check for Port spec
                    if (Token.equals(":")) {

                        if (ST.hasMoreTokens()) {
                            Token = ST.nextToken();
                            URLProps.put("PORT", Token);

                            if (ST.hasMoreTokens()) {
                                Token = ST.nextToken();
                            }
                        }
                    }

                    if (Token.equals("/")) {

                        if (ST.hasMoreTokens()) {
                            Token = ST.nextToken();
                            URLProps.put("DBNAME", Token);

                            // We're done
                            return URLProps;
                        } else {
                            URLProps.put("DBNAME", "");

                            return URLProps;
                        }
                    }
                } else {

                    return null;
                }
            } else {

                return null;
            }
        }

        return URLProps;
    }
}