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
 * @author Mark Matthews
 * @version $Id$
 */
public class Driver implements java.sql.Driver {

	//~ Instance/static variables .............................................

	public static final boolean debug = false;
	public static final boolean trace = false;

	//
	// Version Info
	//
	static final int MAJORVERSION = 3;
	static final int MINORVERSION = 0;

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
	public Driver() throws java.sql.SQLException {

		// Required for Class.forName().newInstance()
	}

	//~ Methods ...............................................................

	/**
	 * Gets the drivers major version number
	 *
	 * @return the drivers major version number
	 */
	public int getMajorVersion() {

		return MAJORVERSION;
	}

	/**
	 * Get the drivers minor version number
	 *
	 * @return the drivers minor version number
	 */
	public int getMinorVersion() {

		return MINORVERSION;
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
		throws java.sql.SQLException {
		info = parseURL(url, info);

		DriverPropertyInfo hostProp =
			new DriverPropertyInfo("HOST", info.getProperty("HOST"));
		hostProp.required = true;
		hostProp.description = "Hostname of MySQL Server";

		DriverPropertyInfo portProp =
			new DriverPropertyInfo("PORT", info.getProperty("PORT", "3306"));
		portProp.required = false;
		portProp.description = "Port number of MySQL Server";

		DriverPropertyInfo dbProp =
			new DriverPropertyInfo("DBNAME", info.getProperty("DBNAME"));
		dbProp.required = false;
		dbProp.description = "Database name";

		DriverPropertyInfo userProp =
			new DriverPropertyInfo("user", info.getProperty("user"));
		userProp.required = true;
		userProp.description = "Username to authenticate as";

		DriverPropertyInfo passwordProp =
			new DriverPropertyInfo("password", info.getProperty("password"));
		passwordProp.required = true;
		passwordProp.description = "Password to use for authentication";

		DriverPropertyInfo autoReconnect =
			new DriverPropertyInfo(
				"autoReconnect",
				info.getProperty("autoReconnect", "false"));
		autoReconnect.required = false;
		autoReconnect.choices = new String[] { "true", "false" };
		autoReconnect.description =
			"Should the driver try to re-establish bad connections?";

		DriverPropertyInfo maxReconnects =
			new DriverPropertyInfo(
				"maxReconnects",
				info.getProperty("maxReconnects", "3"));
		maxReconnects.required = false;
		maxReconnects.description =
			"Maximum number of reconnects to attempt if autoReconnect is true";
		;

		DriverPropertyInfo initialTimeout =
			new DriverPropertyInfo(
				"initialTimeout",
				info.getProperty("initialTimeout", "2"));
		initialTimeout.required = false;
		initialTimeout.description =
			"Initial timeout (seconds) to wait between failed connections";

		DriverPropertyInfo profileSql =
			new DriverPropertyInfo(
				"profileSql",
				info.getProperty("profileSql", "false"));
		profileSql.required = false;
		profileSql.description =
			"Trace queries and their execution/fetch times on STDERR (true/false) defaults to false";
		;

		DriverPropertyInfo socketTimeout =
			new DriverPropertyInfo(
				"socketTimeout",
				info.getProperty("socketTimeout", "0"));
		socketTimeout.required = false;
		socketTimeout.description =
			"Timeout on network socket operations (0 means no timeout)";
		;

		DriverPropertyInfo useSSL =
			new DriverPropertyInfo(
				"useSSL",
				info.getProperty("useSSL", "false"));
		useSSL.required = false;
		useSSL.description = "Use SSL when communicating with the server?";
		;
		
		DriverPropertyInfo paranoid =
			new DriverPropertyInfo(
				"paranoid",
				info.getProperty("paranoid", "false"));
		paranoid.required = false;
		paranoid.description = "Expose sensitive information in error messages and clear " +
		 "data structures holding sensitiven data when possible?";
		;

		DriverPropertyInfo[] dpi =
			{
				hostProp,
				portProp,
				dbProp,
				userProp,
				passwordProp,
				autoReconnect,
				maxReconnects,
				initialTimeout,
				profileSql,
				socketTimeout,
				useSSL,
				paranoid };

		return dpi;
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
	public synchronized boolean acceptsURL(String url)
		throws java.sql.SQLException {

		if (parseURL(url, null) == null) {

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
	public synchronized java.sql.Connection connect(
		String url,
		Properties info)
		throws java.sql.SQLException {

		Properties props = null;

		if ((props = parseURL(url, info)) == null) {

			return null;
		} else {

			try {

				Connection newConn = new com.mysql.jdbc.Connection();
				newConn.connectionInit(
					host(props),
					port(props),
					props,
					database(props),
					url,
					this);

				return (java.sql.Connection) newConn;
			} // Don't wrap SQLExceptions, throw
			catch (SQLException sqlEx) {

				// them un-changed.
				throw sqlEx;
			} catch (Exception ex) {
				throw new SQLException(
					"Cannot load connection class because of underlying exception: '"
						+ ex.toString()
						+ "'.",
					"08001");
			}
		}
	}

	//
	// return the database name property
	//

	/**
	 * DOCUMENT ME!
	 * 
	 * @param props DOCUMENT ME!
	 * @return DOCUMENT ME! 
	 */
	public String database(Properties props) {

		return props.getProperty("DBNAME");
	}

	/**
	 * Returns the hostname property
	 */
	public String host(Properties props) {

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
	public boolean jdbcCompliant() {

		return false;
	}

	/**
	 * Returns the port number property
	 */
	public int port(Properties props) {

		return Integer.parseInt(props.getProperty("PORT", "3306"));
	}

	//
	// return the value of any property this driver knows about
	//

	/**
	 * DOCUMENT ME!
	 * 
	 * @param Name DOCUMENT ME!
	 * @param props DOCUMENT ME!
	 * @return DOCUMENT ME! 
	 */
	public String property(String Name, Properties props) {

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
	Properties parseURL(String url, Properties defaults)
		throws java.sql.SQLException {

		Properties urlProps = new Properties(defaults);

		if (url == null) {

			return null;
		} else {

			/*
			* Parse parameters after the ? in the URL and remove
			* them from the original URL.
			*/
			int index = url.indexOf("?");

			if (index != -1) {

				String paramString = url.substring(index + 1, url.length());
				url = url.substring(0, index);

				StringTokenizer queryParams =
					new StringTokenizer(paramString, "&");

				while (queryParams.hasMoreTokens()) {

					StringTokenizer vp =
						new StringTokenizer(queryParams.nextToken(), "=");
					String param = "";

					if (vp.hasMoreTokens()) {
						param = vp.nextToken();
					}

					String value = "";

					if (vp.hasMoreTokens()) {
						value = vp.nextToken();
					}

					if (value.length() > 0 && param.length() > 0) {
						urlProps.put(param, value);
					}
				}
			}

			StringTokenizer st = new StringTokenizer(url, ":/", true);

			if (st.hasMoreTokens()) {

				String protocol = st.nextToken();

				if (protocol != null) {

					if (!protocol.toLowerCase().equals("jdbc")) {

						return null;
					}
				} else {

					return null;
				}
			} else {

				return null;
			}

			// Look for the colon following 'jdbc'
			if (st.hasMoreTokens()) {

				String colon = st.nextToken();

				if (colon != null) {

					if (!colon.equals(":")) {

						return null;
					}
				} else {

					return null;
				}
			} else {

				return null;
			}

			// Look for sub-protocol to be mysql
			if (st.hasMoreTokens()) {

				String subProto = st.nextToken();

				if (subProto != null) {

					if (!subProto.toLowerCase().equals("mysql")) {

						return null;
					}
				} else {

					return null;
				}
			} else {

				return null;
			}

			// Look for the colon following 'mysql'
			if (st.hasMoreTokens()) {

				String colon = st.nextToken();

				if (colon != null) {

					if (!colon.equals(":")) {

						return null;
					}
				} else {

					return null;
				}
			} else {

				return null;
			}

			// Look for the "//" of the URL
			if (st.hasMoreTokens()) {

				String slash = st.nextToken();
				String slash2 = "";

				if (st.hasMoreTokens()) {
					slash2 = st.nextToken();
				}

				if (slash != null && slash2 != null) {

					if (!slash.equals("/") && !slash2.equals("/")) {

						return null;
					}
				} else {

					return null;
				}
			} else {

				return null;
			}

			// Okay the next one is a candidate for many things
			if (st.hasMoreTokens()) {

				String token = st.nextToken();

				if (token != null) {

					if (!token.equals(":") && !token.equals("/")) {

						// Must be hostname
						urlProps.put("HOST", token);

						if (st.hasMoreTokens()) {
							token = st.nextToken();
						} else {

							return null;
						}
					}

					// Check for Port spec
					if (token.equals(":")) {

						if (st.hasMoreTokens()) {
							token = st.nextToken();
							urlProps.put("PORT", token);

							if (st.hasMoreTokens()) {
								token = st.nextToken();
							}
						}
					}

					if (token.equals("/")) {

						if (st.hasMoreTokens()) {
							token = st.nextToken();
							urlProps.put("DBNAME", token);

							// We're done
							return urlProps;
						} else {
							urlProps.put("DBNAME", "");

							return urlProps;
						}
					}
				} else {

					return null;
				}
			} else {

				return null;
			}
		}

		return urlProps;
	}
}