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

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

import java.math.BigDecimal;

import java.net.URL;

import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Time;
import java.sql.Timestamp;

import java.util.Calendar;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

/**
 * A Connection represents a session with a specific database.  Within the
 * context of a Connection, SQL statements are executed and results are
 * returned.
 *
 * <P>A Connection's database is able to provide information describing
 * its tables, its supported SQL grammar, its stored procedures, the
 * capabilities of this connection, etc.  This information is obtained
 * with the getMetaData method.
 *
 * @see java.sql.Connection
 * @author Mark Matthews mmatthew@worldserver.com
 * @version $Id$
 */
public class Connection
    implements java.sql.Connection
{

    //~ Instance/static variables .............................................

    // The command used to "ping" the database.
    // Newer versions of MySQL server have a ping() command,
    // but this works for everything.4
    private static final String _PING_COMMAND = "SELECT 1";

    /**
     * Map mysql transaction isolation level name to java.sql.Connection.TRANSACTION_XXX
     */
    static private Hashtable _mapTransIsolationName2Value = null;

    /**
     * The database we're currently using
     * (called Catalog in JDBC terms).
     */
    protected String _database = null;

    /** Should we return PreparedStatements for UltraDev's stupid bug? */
    protected boolean _useUltraDevWorkAround = false;

    /** 
     * The I/O abstraction interface (network conn to
     * MySQL server
     */
    MysqlIO _io = null;

    /**
     * Are we in autoCommit mode?
     */
    private boolean _autoCommit = true;

    /**
     * Should we capitalize mysql types
     */
    private boolean _capitalizeDBMDTypes = false;

    /**
     * Should we do unicode character conversions?
     */
    private boolean _doUnicode = false;

    /**
     * If we're doing unicode character conversions,
     * what encoding do we use?
     */
    private String _encoding = null;

    /** Does the server suuport isolation levels? */
    private boolean _hasIsolationLevels = false;

    /**
     * Does this version of MySQL support quoted identifiers?
     */
    private boolean _hasQuotedIdentifiers = false;

    //
    // This is for the high availability :) routines
    //
    private boolean _highAvailability = false;

    /**
     * The hostname we're connected to
     */
    private String _host           = null;
    private double _initialTimeout = 2.0D;

    /**
     * Has this connection been closed?
     */
    private boolean _isClosed = true;

    /**
     * When did the last query finish?
     */
    private long _lastQueryFinishedTime = 0;

    /**
     * The largest packet we can send (changed
     * once we know what the server supports, we
     * get this at connection init).
     */
    private int _maxAllowedPacket = 65536;
    private int _maxReconnects = 3;

    /**
     * The max rows that a result set can contain.
     * 
     * Defaults to -1, which according to the JDBC
     * spec means "all".
     */
    private int _maxRows = -1;

    /**
     * Has the max-rows setting been changed from
     * the default?
     */
    private boolean _maxRowsChanged = false;

    /**
     * Mutex
     */
    private final Object _mutex = new Object();

    /**
     * The driver instance that created us
     */
    private com.mysql.jdbc.Driver _myDriver;

    /**
     * The JDBC URL we're using
     */
    private String _myURL           = null;
    private int    _netBufferLength = 16384;

    /**
     * The password we used
     */
    private String _password = null;

    /**
     * The port number we're connected to
     * (defaults to 3306)
     */
    private int _port = 3306;

    /**
     * Are we in read-only mode?
     */
    private boolean _readOnly = false;

    /** Do we relax the autoCommit semantics? (For enhydra, for example) */
    private boolean _relaxAutoCommit = false;

    /**
     * The map of server variables that we retrieve
     * at connection init.
     */
    private Hashtable _serverVariables = null;

    /**
     * Do we need to correct endpoint rounding errors
     */
    private boolean _strictFloatingPoint = false;

    /** Are transactions supported by the MySQL server we are connected to? */
    private boolean _transactionsSupported = false;

    /** 
     * Has ANSI_QUOTES been enabled on the server?
     */
    private boolean _useAnsiQuotes = false;

    /**
     * Can we use the "ping" command rather than a 
     * query?
     */
    private boolean _useFastPing = false;

    /**
     * The user we're connected as
     */
    private String _user = null;
    
    /**
     * Should we use timezone information?
     */
    
    private boolean _useTimezone = false;
    
    /**
     * The timezone of the server
     */
    
    private TimeZone _serverTimezone = null;

    /**
     * You can call this method to try to change the transaction
     * isolation level using one of the TRANSACTION_* values.
     *
     * <B>Note:</B> setTransactionIsolation cannot be called while
     * in the middle of a transaction
     *
     * @param level one of the TRANSACTION_* isolation values with
     *    the exception of TRANSACTION_NONE; some databases may
     *    not support other values
     * @exception java.sql.SQLException if a database access error occurs
     * @see java.sql.DatabaseMetaData#supportsTransactionIsolationLevel
     */
    private int isolationLevel = java.sql.Connection.TRANSACTION_READ_COMMITTED;

    //~ Initializers ..........................................................

    static {
        _mapTransIsolationName2Value = new Hashtable(8);
        _mapTransIsolationName2Value.put("READ-UNCOMMITED", 
                                         new Integer(
                                                 java.sql.Connection.TRANSACTION_READ_UNCOMMITTED));
        _mapTransIsolationName2Value.put("READ-COMMITTED", 
                                         new Integer(
                                                 java.sql.Connection.TRANSACTION_READ_COMMITTED));
        _mapTransIsolationName2Value.put("REPEATABLE-READ", 
                                         new Integer(
                                                 java.sql.Connection.TRANSACTION_REPEATABLE_READ));
        _mapTransIsolationName2Value.put("SERIALIZABLE", 
                                         new Integer(
                                                 java.sql.Connection.TRANSACTION_SERIALIZABLE));
    }

    //~ Methods ...............................................................

    /**
     * If a connection is in auto-commit mode, than all its SQL
     * statements will be executed and committed as individual
     * transactions.  Otherwise, its SQL statements are grouped
     * into transactions that are terminated by either commit()
     * or rollback().  By default, new connections are in auto-
     * commit mode.  The commit occurs when the statement completes
     * or the next execute occurs, whichever comes first.  In the
     * case of statements returning a ResultSet, the statement
     * completes when the last row of the ResultSet has been retrieved
     * or the ResultSet has been closed.  In advanced cases, a single
     * statement may return multiple results as well as output parameter
     * values.  Here the commit occurs when all results and output param
     * values have been retrieved.
     *
     * <p><b>Note:</b> MySQL does not support transactions, so this
     *                 method is a no-op.
     *
     * @param autoCommit - true enables auto-commit; false disables it
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setAutoCommit(boolean autoCommit)
                       throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = { new Boolean(autoCommit) };
            Debug.methodCall(this, "setAutoCommit", args);
        }

        if (_transactionsSupported) {

            String sql = "SET autocommit=" + (autoCommit ? "1" : "0");
            execSQL(sql, -1);
            _autoCommit = autoCommit;
        } else {

            if (autoCommit == false && _relaxAutoCommit == false) {
                throw new SQLException("MySQL Versions Older than 3.23.15 do not support transactions", 
                                       "08003");
            } else {
                _autoCommit = autoCommit;
            }
        }

        return;
    }

    /**
     * gets the current auto-commit state
     *
     * @return Current state of the auto-commit mode
     * @exception java.sql.SQLException (why?)
     * @see setAutoCommit
     */
    public boolean getAutoCommit()
                          throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = new Object[0];
            Debug.methodCall(this, "getAutoCommit", args);
            Debug.returnValue(this, "getAutoCommit", new Boolean(_autoCommit));
        }

        return _autoCommit;
    }

    /**
     * A sub-space of this Connection's database may be selected by
     * setting a catalog name.  If the driver does not support catalogs,
     * it will silently ignore this request
     *
     * <p><b>Note:</b> MySQL's notion of catalogs are individual databases.
     *
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setCatalog(String catalog)
                    throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = { catalog };
            Debug.methodCall(this, "setCatalog", args);
        }

        execSQL("USE " + catalog, -1);
        _database = catalog;
    }

    /**
     * Return the connections current catalog name, or null if no
     * catalog name is set, or we dont support catalogs.
     *
     * <p><b>Note:</b> MySQL's notion of catalogs are individual databases.
     * @return the current catalog name or null
     * @exception java.sql.SQLException if a database access error occurs
     */
    public String getCatalog()
                      throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = new Object[0];
            Debug.methodCall(this, "getCatalog", args);
            Debug.returnValue(this, "getCatalog", _database);
        }

        return _database;
    }

    /**
     * Tests to see if a Connection is closed
     *
     * @return the status of the connection
     * @exception java.sql.SQLException (why?)
     */
    public boolean isClosed()
                     throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = new Object[0];
            Debug.methodCall(this, "isClosed", args);
            Debug.returnValue(this, "isClosed", new Boolean(_isClosed));
        }

        if (!_isClosed) {

            // Test the connection
            try {

                synchronized (_mutex) {
                    ping();
                }
            } catch (Exception E) {
                _isClosed = true;
            }
        }

        return _isClosed;
    }

    /** Returns the character encoding for this Connection */
    public String getEncoding()
    {

        return _encoding;
    }

    /**
     * @see Connection#setHoldability(int)
     */
    public void setHoldability(int arg0)
                        throws SQLException
    {

        // do nothing
    }

    /**
     * @see Connection#getHoldability()
     */
    public int getHoldability()
                       throws SQLException
    {

        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * NOT JDBC-Compliant, but clients can use this method
     * to determine how long this connection has been idle.
     * 
     * This time (reported in milliseconds) is updated once
     * a query has completed.
     * 
     * @return number of ms that this connection has
     * been idle, 0 if the driver is busy retrieving results.
     */
    public long getIdleFor()
    {

        if (_lastQueryFinishedTime == 0) {

            return 0;
        } else {

            long now      = System.currentTimeMillis();
            long idleTime = now - _lastQueryFinishedTime;

            return idleTime;
        }
    }

    /**
     * A connection's database is able to provide information describing
     * its tables, its supported SQL grammar, its stored procedures, the
     * capabilities of this connection, etc.  This information is made
     * available through a DatabaseMetaData object.
     *
     * @return a DatabaseMetaData object for this connection
     * @exception java.sql.SQLException if a database access error occurs
     */
    public java.sql.DatabaseMetaData getMetaData()
                                          throws java.sql.SQLException
    {

        return new DatabaseMetaData(this, _database);
    }

    /**
     * You can put a connection in read-only mode as a hint to enable
     * database optimizations
     *
     * <B>Note:</B> setReadOnly cannot be called while in the middle
     * of a transaction
     *
     * @param readOnly - true enables read-only mode; false disables it
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setReadOnly(boolean readOnly)
                     throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = { new Boolean(readOnly) };
            Debug.methodCall(this, "setReadOnly", args);
            Debug.returnValue(this, "setReadOnly", new Boolean(readOnly));
        }

        _readOnly = readOnly;
    }

    /**
     * Tests to see if the connection is in Read Only Mode.  Note that
     * we cannot really put the database in read only mode, but we pretend
     * we can by returning the value of the readOnly flag
     *
     * @return true if the connection is read only
     * @exception java.sql.SQLException if a database access error occurs
     */
    public boolean isReadOnly()
                       throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = new Object[0];
            Debug.methodCall(this, "isReadOnly", args);
            Debug.returnValue(this, "isReadOnly", new Boolean(_readOnly));
        }

        return _readOnly;
    }

    /**
     * @see Connection#setSavepoint()
     */
    public java.sql.Savepoint setSavepoint()
                                    throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see Connection#setSavepoint(String)
     */
    public java.sql.Savepoint setSavepoint(String arg0)
                                    throws SQLException
    {
        throw new NotImplemented();
    }

    public void setTransactionIsolation(int level)
                                 throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = { new Integer(level) };
            Debug.methodCall(this, "setTransactionIsolation", args);
        }

        if (_hasIsolationLevels) {

            StringBuffer sql = new StringBuffer(
                                       "SET SESSION TRANSACTION ISOLATION LEVEL ");

            switch (level) {

                case java.sql.Connection.TRANSACTION_NONE:
                    throw new SQLException("Transaction isolation level NONE not supported by MySQL");

                case java.sql.Connection.TRANSACTION_READ_COMMITTED:
                    sql.append("READ COMMITTED");

                    break;

                case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
                    sql.append("READ UNCOMMITTED");

                    break;

                case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
                    sql.append("REPEATABLE READ");

                    break;

                case java.sql.Connection.TRANSACTION_SERIALIZABLE:
                    sql.append("SERIALIZABLE");

                    break;

                default:
                    throw new SQLException("Unsupported transaction isolation level '" + 
                                           level + "'", "S1C00");
            }

            execSQL(sql.toString(), -1);
            isolationLevel = level;
        } else {
            throw new java.sql.SQLException("Transaction Isolation Levels are not supported on MySQL versions older than 3.23.36.", 
                                            "S1C00");
        }
    }

    /**
     * Get this Connection's current transaction isolation mode.
     *
     * @return the current TRANSACTION_* mode value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public int getTransactionIsolation()
                                throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = new Object[0];
            Debug.methodCall(this, "getTransactionIsolation", args);
            Debug.returnValue(this, "getTransactionIsolation", 
                              new Integer(
                                      java.sql.Connection.TRANSACTION_SERIALIZABLE));
        }

        return isolationLevel;
    }

    /**
     * JDBC 2.0
     *
     * Install a type-map object as the default type-map for
     * this connection
     */
    public void setTypeMap(java.util.Map map)
                    throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * JDBC 2.0
     *
     * Get the type-map object associated with this connection.
     * By default, the map returned is empty.
     */
    public java.util.Map getTypeMap()
                             throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * The first warning reported by calls on this Connection is
     * returned.
     *
     * <B>Note:</B> Sebsequent warnings will be changed to this
     * java.sql.SQLWarning
     *
     * @return the first java.sql.SQLWarning or null
     * @exception java.sql.SQLException if a database access error occurs
     */
    public java.sql.SQLWarning getWarnings()
                                    throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = new Object[0];
            Debug.methodCall(this, "getWarnings", args);
            Debug.returnValue(this, "getWarnings", null);
        }

        return null;
    }

    public boolean capitalizeDBMDTypes()
    {

        return _capitalizeDBMDTypes;
    }

    /**
     * After this call, getWarnings returns null until a new warning
     * is reported for this connection.
     *
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void clearWarnings()
                       throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = new Object[0];
            Debug.methodCall(this, "clearWarnings", args);
        }

        // firstWarning = null;
    }

    /**
     * In some cases, it is desirable to immediately release a Connection's
     * database and JDBC resources instead of waiting for them to be
     * automatically released (cant think why off the top of my head)
     *
     * <B>Note:</B> A Connection is automatically closed when it is
     * garbage collected.  Certain fatal errors also result in a closed
     * connection.
     *
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void close()
               throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = new Object[0];
            Debug.methodCall(this, "close", args);
        }

        if (_io != null) {

            try {
                _io.quit();
            } catch (Exception e) {
            }

            _io = null;
        }

        _isClosed = true;
    }

    /**
     * The method commit() makes all changes made since the previous
     * commit/rollback permanent and releases any database locks currently
     * held by the Connection.  This method should only be used when
     * auto-commit has been disabled.
     * 
     * <p><b>Note:</b> MySQL does not support transactions, so this
     *                 method is a no-op.
     *
     * @exception java.sql.SQLException if a database access error occurs
     * @see setAutoCommit
     */
    public void commit()
                throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = new Object[0];
            Debug.methodCall(this, "commit", args);
        }

        if (_isClosed) {
            throw new java.sql.SQLException("Commit attempt on closed connection.", 
                                            "08003");
        }

        // no-op if _relaxAutoCommit == true
        if (_autoCommit && !_relaxAutoCommit) {
            throw new SQLException("Can't call commit when autocommit=true");
        } else if (_transactionsSupported) {
            execSQL("commit", -1);
        }

        return;
    }

    /**
     * Connect to a MySQL Server.
     *
     * <p><b>Important Notice</b>
     *
     * <br>Although this will connect to the database, user code should open
     * the connection via the DriverManager.getConnection() methods only.
     *
     * <br>This should only be called from the org.gjt.mm.mysql.Driver class.
     *
     * @param Host the hostname of the database server
     * @param port the port number the server is listening on
     * @param Info a Properties[] list holding the user and password
     * @param Database the database to connect to
     * @param Url the URL of the connection
     * @param D the Driver instantation of the connection
     * @return a valid connection profile
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void connectionInit(String host, int port, Properties info, 
                               String database, String url, Driver d)
                        throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = { host, new Integer(port), info, database, url, d };
            Debug.methodCall(this, "constructor", args);
        }

        if (host == null) {
            _host = "localhost";
        } else {
            _host = host;
        }

        _port = port;

        if (database == null) {
            throw new SQLException("Malformed URL '" + url + "'.", "S1000");
        }

        _database = database;
        _myURL    = url;
        _myDriver = d;
        _user     = info.getProperty("user");
        _password = info.getProperty("password");

        if (_user == null || _user.equals("")) {
            _user = "nobody";
        }

        if (_password == null) {
            _password = "";
        }

        // Check for driver specific properties
        if (info.getProperty("relaxAutoCommit") != null) {
            _relaxAutoCommit = info.getProperty("relaxAutoCommit").toUpperCase()
                .equals("TRUE");
        }

        if (info.getProperty("autoReconnect") != null) {
            _highAvailability = info.getProperty("autoReconnect").toUpperCase()
                .equals("TRUE");
        }

        if (info.getProperty("capitalizeTypeNames") != null) {
            _capitalizeDBMDTypes = info.getProperty("capitalizeTypeNames").toUpperCase()
                .equals("TRUE");
        }

        if (info.getProperty("ultraDevHack") != null) {
            _useUltraDevWorkAround = info.getProperty("ultraDevHack").toUpperCase()
                .equals("TRUE");
        }

        if (info.getProperty("strictFloatingPoint") != null) {
            _strictFloatingPoint = info.getProperty("strictFloatingPoint").toUpperCase()
                .equals("TRUE");
        }

        if (_highAvailability) {

            if (info.getProperty("maxReconnects") != null) {

                try {

                    int n = Integer.parseInt(info.getProperty("maxReconnects"));
                    _maxReconnects = n;
                } catch (NumberFormatException NFE) {
                    throw new SQLException("Illegal parameter '" + 
                                           info.getProperty("maxReconnects") + 
                                           "' for maxReconnects", "0S100");
                }
            }

            if (info.getProperty("initialTimeout") != null) {

                try {

                    double n = Integer.parseInt(info.getProperty(
                                                        "initialTimeout"));
                    _initialTimeout = n;
                } catch (NumberFormatException NFE) {
                    throw new SQLException("Illegal parameter '" + 
                                           info.getProperty("initialTimeout") + 
                                           "' for initialTimeout", "0S100");
                }
            }
        }

        if (info.getProperty("maxRows") != null) {

            try {

                int n = Integer.parseInt(info.getProperty("maxRows"));

                if (n == 0) {
                    n = -1;
                } // adjust so that it will become MysqlDefs.MAX_ROWS

                // in execSQL()
                _maxRows = n;
            } catch (NumberFormatException NFE) {
                throw new SQLException("Illegal parameter '" + 
                                       info.getProperty("maxRows") + 
                                       "' for maxRows", "0S100");
            }
        }

        if (info.getProperty("useUnicode") != null) {

            String useUnicode = info.getProperty("useUnicode").toUpperCase();

            if (useUnicode.startsWith("TRUE")) {
                _doUnicode = true;
            }

            if (info.getProperty("characterEncoding") != null) {
                _encoding = info.getProperty("characterEncoding");

                // Attempt to use the encoding, and bail out if it
                // can't be used
                try {

                    String testString = "abc";
                    testString.getBytes(_encoding);
                } catch (UnsupportedEncodingException UE) {
                    throw new SQLException("Unsupported character encoding '" + 
                                           _encoding + "'.", "0S100");
                }
            }
        }

        if (Driver.debug) {
            System.out.println("Connect: " + _user + " to " + _database);
        }

        try {
            _io = createNewIO(host, port);
            _io.init(_user, _password);

            if (_database.length() != 0) {
                _io.sendCommand(MysqlDefs.INIT_DB, _database, null);
            }

            _isClosed             = false;
            _serverVariables      = new Hashtable();

            if (_io.versionMeetsMinimum(3, 22, 1)) {
                _useFastPing = true;
            }

            //
            // If version is greater than 3.21.22 get the server
            // variables.
            if (_io.versionMeetsMinimum(3, 21, 22)) {

                com.mysql.jdbc.Statement stmt    = null;
                com.mysql.jdbc.ResultSet results = null;

                try {
                    stmt    = (com.mysql.jdbc.Statement)createStatement();
                    results = (com.mysql.jdbc.ResultSet)stmt.executeQuery(
                                      "SHOW VARIABLES");

                    while (results.next()) {
                        _serverVariables.put(results.getString(1), 
                                             results.getString(2));
                    }
                } catch (java.sql.SQLException e) {
                    throw e;
                } finally {

                    if (results != null) {

                        try {
                            results.close();
                        } catch (java.sql.SQLException sqlE) {
                        }
                    }

                    if (stmt != null) {

                        try {
                            stmt.close();
                        } catch (java.sql.SQLException sqlE) {
                        }
                    }
                }

                if (_serverVariables.containsKey("max_allowed_packet")) {
                    _maxAllowedPacket = Integer.parseInt(
                                                (String)_serverVariables.get(
                                                        "max_allowed_packet"));
                }

                if (_serverVariables.containsKey("net_buffer_length")) {
                    _netBufferLength = Integer.parseInt(
                                               (String)_serverVariables.get(
                                                       "net_buffer_length"));
                }
               
               /*
                String serverTimezoneStr = (String)_serverVariables.get("timezone");
                
                if (serverTimezoneStr != null && serverTimezoneStr.trim().length() > 0)
                {
                	try
                	{
                		serverTimezoneStr = TimeUtil.getCanoncialTimezone(serverTimezoneStr);
                				
                		if (serverTimezoneStr != null)
                		{
                			_serverTimezone = TimeZone.getTimeZone(serverTimezoneStr);
                		
                			_useTimezone = true;
                		}
                	}
                	catch (Exception ex) { // Bail-out, we can't use the timezone 
                	}
                }
                */
                	

                checkTransactionIsolationLevel();
                checkServerEncoding();
            }

            if (_io.versionMeetsMinimum(3, 23, 15)) {
                _transactionsSupported = true;
            } else {
                _transactionsSupported = false;
            }

            if (_io.versionMeetsMinimum(3, 23, 36)) {
                _hasIsolationLevels = true;
            } else {
                _hasIsolationLevels = false;
            }

            // Start logging perf/profile data if the user has requested it.
            String profileSql = info.getProperty("profileSql");

            if (profileSql != null && 
                profileSql.trim().equalsIgnoreCase("true")) {
                _io.setProfileSql(true);
            } else {
                _io.setProfileSql(false);
            }

            _hasQuotedIdentifiers = _io.versionMeetsMinimum(3, 23, 6);

            if (_serverVariables.containsKey("sql_mode")) {

                int sqlMode = Integer.parseInt(
                                      (String)_serverVariables.get("sql_mode"));

                if ((sqlMode & 4) > 0) {
                    _useAnsiQuotes = true;
                } else {
                    _useAnsiQuotes = false;
                }
            }

            _io.resetMaxBuf();
        } catch (java.sql.SQLException ex) {

            // don't clobber SQL exceptions
            throw ex;
        }
         catch (Exception ex) {
            throw new java.sql.SQLException("Cannot connect to MySQL server on " + 
                                            _host + ":" + _port + 
                                            ". Is there a MySQL server running on the machine/port you are trying to connect to? (" + 
                                            ex.getClass().getName() + ")", 
                                            "08S01");
        }
    }

    //--------------------------JDBC 2.0-----------------------------

    /**
     * JDBC 2.0
     *
     * Same as createStatement() above, but allows the default result set
     * type and result set concurrency type to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new Statement object
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.Statement createStatement(int resultSetType, 
                                              int resultSetConcurrency)
                                       throws SQLException
    {

        Statement stmt = new com.mysql.jdbc.Statement(this, _database);
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);

        return stmt;
    }

    /**
     * SQL statements without parameters are normally executed using
     * Statement objects.  If the same SQL statement is executed many
     * times, it is more efficient to use a PreparedStatement
     *
     * @return a new Statement object
     * @exception java.sql.SQLException passed through from the constructor
     */
    public java.sql.Statement createStatement()
                                       throws SQLException
    {

        return createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, 
                               java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * @see Connection#createStatement(int, int, int)
     */
    public java.sql.Statement createStatement(int arg0, int arg1, int arg2)
                                       throws SQLException
    {
        throw new NotImplemented();
    }

    public void finalize()
                  throws Throwable
    {

        if (_io != null && !isClosed()) {
            close();
        } else if (_io != null) {
            _io.forceClose();
        }
    }

    /**
     * A driver may convert the JDBC sql grammar into its system's
     * native SQL grammar prior to sending it; nativeSQL returns the
     * native form of the statement that the driver would have sent.
     *
     * @param sql a SQL statement that may contain one or more '?'
     *    parameter placeholders
     * @return the native form of this statement
     * @exception java.sql.SQLException if a database access error occurs
     */
    public String nativeSQL(String sql)
                     throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = { sql };
            Debug.methodCall(this, "nativeSQL", args);
            Debug.returnValue(this, "nativeSQL", sql);
        }

        return sql;
    }

    public java.sql.CallableStatement prepareCall(String sql)
                                           throws java.sql.SQLException
    {

        if (_useUltraDevWorkAround) {

            return new UltraDevWorkAround(prepareStatement(sql));
        } else {
            throw new java.sql.SQLException("Callable statments not supported.", 
                                            "S1C00");
        }
    }

    /**
     * JDBC 2.0
     *
     * Same as prepareCall() above, but allows the default result set
     * type and result set concurrency type to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new CallableStatement object containing the
     * pre-compiled SQL statement
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.CallableStatement prepareCall(String sql, 
                                                  int resultSetType, 
                                                  int resultSetConcurrency)
                                           throws SQLException
    {

        return prepareCall(sql);
    }

    /**
     * @see Connection#prepareCall(String, int, int, int)
     */
    public java.sql.CallableStatement prepareCall(String arg0, int arg1, 
                                                  int arg2, int arg3)
                                           throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * A SQL statement with or without IN parameters can be pre-compiled
     * and stored in a PreparedStatement object.  This object can then
     * be used to efficiently execute this statement multiple times.
     * 
     * <p>
     * <B>Note:</B> This method is optimized for handling parametric
     * SQL statements that benefit from precompilation if the driver
     * supports precompilation. 
     *
     * In this case, the statement is not sent to the database until the
     * PreparedStatement is executed.  This has no direct effect on users;
     * however it does affect which method throws 
     * certain java.sql.SQLExceptions
     *
     * <p>
     * MySQL does not support precompilation of statements, so they
     * are handled by the driver. 
     *
     * @param sql a SQL statement that may contain one or more '?' IN
     *    parameter placeholders
     * @return a new PreparedStatement object containing the pre-compiled
     *    statement.
     * @exception java.sql.SQLException if a database access error occurs.
     */
    public java.sql.PreparedStatement prepareStatement(String sql)
                                                throws java.sql.SQLException
    {

        return prepareStatement(sql, 
                                java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * JDBC 2.0
     *
     * Same as prepareStatement() above, but allows the default result set
     * type and result set concurrency type to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new PreparedStatement object containing the
     * pre-compiled SQL statement
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.PreparedStatement prepareStatement(String sql, 
                                                       int resultSetType, 
                                                       int resultSetConcurrency)
                                                throws SQLException
    {

        //
        // FIXME: Create warnings if can't create results of the given
        //        type or concurrency
        //
        PreparedStatement pStmt = new com.mysql.jdbc.PreparedStatement(this, 
                                                                       sql, 
                                                                       _database);
        pStmt.setResultSetType(resultSetType);
        pStmt.setResultSetConcurrency(resultSetConcurrency);

        return pStmt;
    }

    /**
     * @see Connection#prepareStatement(String, int, int, int)
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, int arg1, 
                                                       int arg2, int arg3)
                                                throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see Connection#prepareStatement(String, int)
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, int arg1)
                                                throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see Connection#prepareStatement(String, int[])
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, int[] arg1)
                                                throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see Connection#prepareStatement(String, String[])
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, 
                                                       String[] arg1)
                                                throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * @see Connection#releaseSavepoint(Savepoint)
     */
    public void releaseSavepoint(Savepoint arg0)
                          throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * The method rollback() drops all changes made since the previous
     * commit/rollback and releases any database locks currently held by
     * the Connection.
     *
     * @exception java.sql.SQLException if a database access error occurs
     * @see commit
     */
    public void rollback()
                  throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = new Object[0];
            Debug.methodCall(this, "rollback", args);
        }

        if (_isClosed) {
            throw new java.sql.SQLException("Rollback attempt on closed connection.", 
                                            "08003");
        }

        // no-op if _relaxAutoCommit == true
        if (_autoCommit && !_relaxAutoCommit) {
            throw new SQLException("Can't call commit when autocommit=true", 
                                   "08003");
        } else if (_transactionsSupported) {
            execSQL("rollback", -1);
        }
    }

    /**
     * @see Connection#rollback(Savepoint)
     */
    public void rollback(Savepoint arg0)
                  throws SQLException
    {
        throw new NotImplemented();
    }

    public boolean supportsIsolationLevel()
    {

        return _hasIsolationLevels;
    }

    public boolean supportsQuotedIdentifiers()
    {

        return _hasQuotedIdentifiers;
    }

    public boolean supportsTransactions()
    {

        return _transactionsSupported;
    }

    public boolean useStrictFloatingPoint()
    {

        return _strictFloatingPoint;
    }

    /** Should unicode character mapping be used ? */
    public boolean useUnicode()
    {

        return _doUnicode;
    }

    protected MysqlIO getIO()
    {

        return _io;
    }

    protected com.mysql.jdbc.MysqlIO createNewIO(String host, int port)
                                          throws Exception
    {

        return new MysqlIO(host, port, this);
    }

    /** Returns the maximum packet size the MySQL server will accept */
    int getMaxAllowedPacket()
    {

        return _maxAllowedPacket;
    }

    /** Returns the Mutex all queries are locked against */
    Object getMutex()
             throws SQLException
    {

        if (_io == null) {
            throw new SQLException("Connection.close() has already been called. Invalid operation in this state.", 
                                   "08003");
        }

        return _mutex;
    }

    /** Returns the packet buffer size the MySQL server reported
     *  upon connection */
    int getNetBufferLength()
    {

        return _netBufferLength;
    }

    int getServerMajorVersion()
    {

        return _io.getServerMajorVersion();
    }

    int getServerMinorVersion()
    {

        return _io.getServerMinorVersion();
    }

    int getServerSubMinorVersion()
    {

        return _io.getServerSubMinorVersion();
    }

    String getServerVersion()
    {

        return _io.getServerVersion();
    }

    String getURL()
    {

        return _myURL;
    }

    String getUser()
    {

        return _user;
    }

    /**
     * Send a query to the server.  Returns one of the ResultSet
     * objects.
     *
     * This is synchronized, so Statement's queries
     * will be serialized.
     *
     * @param sql the SQL statement to be executed
     * @return a ResultSet holding the results
     * @exception java.sql.SQLException if a database error occurs
     */
    ResultSet execSQL(String sql, int max_rows)
               throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = { sql, new Integer(max_rows) };
            Debug.methodCall(this, "execSQL", args);
        }

        return execSQL(sql, max_rows, null, 
                       java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    ResultSet execSQL(String sql, int maxRows, int resultSetType)
               throws SQLException
    {

        return execSQL(sql, maxRows, null, resultSetType);
    }

    ResultSet execSQL(String sql, int maxRows, Buffer packet)
               throws java.sql.SQLException
    {

        return execSQL(sql, maxRows, packet, 
                       java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    ResultSet execSQL(String sql, int maxRows, Buffer packet, 
                      int resultSetType)
               throws java.sql.SQLException
    {

        if (Driver.trace) {

            Object[] args = { sql, new Integer(maxRows), packet };
            Debug.methodCall(this, "execSQL", args);
        }

        synchronized (_mutex) {
            _lastQueryFinishedTime = 0; // we're busy!

            if (_highAvailability) {

                try {
                    ping();
                } catch (Exception Ex) {

                    double  timeout        = _initialTimeout;
                    boolean connectionGood = false;

                    for (int i = 0; i < _maxReconnects; i++) {

                        try {

                            try {
                                _io.forceClose();
                            } catch (Exception ex) {

                                // do nothing
                            }

                            _io = createNewIO(_host, _port);
                            _io.init(_user, _password);

                            if (_database.length() != 0) {
                                _io.sendCommand(MysqlDefs.INIT_DB, _database, 
                                                null);
                            }

                            ping();
                            connectionGood = true;

                            break;
                        } catch (Exception EEE) {
                        }

                        try {
                            Thread.currentThread().sleep((long)timeout * 1000);
                            timeout = timeout * timeout;
                        } catch (InterruptedException IE) {
                        }
                    }

                    if (!connectionGood) {

                        // We've really failed!
                        throw new SQLException("Server connection failure during transaction. \nAttemtped reconnect " + 
                                               _maxReconnects + 
                                               " times. Giving up.", "08001");
                    }
                }
            }

            try {

                int realMaxRows = (maxRows == -1) ? MysqlDefs.MAX_ROWS : maxRows;

                if (packet == null) {

                    String encoding = null;

                    if (useUnicode()) {
                        encoding = getEncoding();
                    }

                    return _io.sqlQuery(sql, realMaxRows, encoding, this, 
                                        resultSetType);
                } else {

                    return _io.sqlQueryDirect(packet, realMaxRows, this, 
                                              resultSetType);
                }
            } catch (java.io.EOFException eofE) {
                throw new java.sql.SQLException("Lost connection to server during query", 
                                                "08007");
            }
             catch (java.sql.SQLException sqlE) {

                // don't clobber SQL exceptions
                throw sqlE;
            }
             catch (Exception ex) {

                String exceptionType    = ex.getClass().getName();
                String exceptionMessage = ex.getMessage();
                throw new java.sql.SQLException("Error during query: Unexpected Exception: " + 
                                                exceptionType + 
                                                " message given: " + 
                                                exceptionMessage, "S1000");
            } finally {
                _lastQueryFinishedTime = System.currentTimeMillis();
            }
        }
    }

    /** Has the maxRows value changed? */
    synchronized void maxRowsChanged()
    {
        _maxRowsChanged = true;
    }

    boolean useAnsiQuotedIdentifiers()
    {

        return _useAnsiQuotes;
    }

    /** Has maxRows() been set? */
    synchronized boolean useMaxRows()
    {

        return _maxRowsChanged;
    }

    /**
     * If useUnicode flag is set and explicit client character encoding isn't specified
     * then assign encoding from server if any.
     * Is called by connectionInit(...)
     */
    private void checkServerEncoding()
                              throws SQLException
    {

        if (useUnicode() && getEncoding() == null) {
            _encoding = (String)_serverVariables.get("character_set");

            if (_encoding != null) {

                // dirty hack to work around discrepancies in character encoding names, e.g.
                // mysql    java
                // latin1   Latin1
                // cp1251   Cp1251
                if (Character.isLowerCase(_encoding.charAt(0))) {

                    char[] ach = _encoding.toCharArray();
                    ach[0]    = Character.toUpperCase(_encoding.charAt(0));
                    _encoding = new String(ach);
                }

                // Attempt to use the encoding, and bail out if it
                // can't be used
                try {

                    String TestString = "abc";
                    TestString.getBytes(_encoding);
                } catch (UnsupportedEncodingException UE) {

                    //              throw new SQLException("Unsupported character encoding '" +
                    //                                     _encoding + "'.", "0S100");
                    _encoding = null;
                }
            }
        }
    }

    /**
     * Set transaction isolation level to the value received from server if any.
     * Is called by connectionInit(...)
     */
    private void checkTransactionIsolationLevel()
                                         throws SQLException
    {

        String s = (String)_serverVariables.get("transaction_isolation");

        if (s != null) {

            Integer intTI = (Integer)_mapTransIsolationName2Value.get(s);

            if (intTI != null) {
                isolationLevel = intTI.intValue();
            }
        }
    }

    // *********************************************************************
    //
    //                END OF PUBLIC INTERFACE
    //
    // *********************************************************************

    /**
     *  Detect if the connection is still good
     */
    private void ping()
               throws Exception
    {

        if (_useFastPing) {
            _io.sendCommand(MysqlDefs.PING, null, null);
        } else {
            _io.sqlQuery(_PING_COMMAND, MysqlDefs.MAX_ROWS, 
                         java.sql.ResultSet.CONCUR_READ_ONLY);
        }
    }

    //~ Inner classes .........................................................

    /**
     * Wrapper class for UltraDev CallableStatements that 
     * are really PreparedStatments.
     *
     * Nice going, macromedia!
     */
    class UltraDevWorkAround
        implements java.sql.CallableStatement
    {

        java.sql.PreparedStatement delegate = null;

        UltraDevWorkAround(java.sql.PreparedStatement pstmt)
        {
            delegate = pstmt;
        }

        public void setArray(int p1, final java.sql.Array p2)
                      throws java.sql.SQLException
        {
            delegate.setArray(p1, p2);
        }

        public java.sql.Array getArray(int p1)
                                throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getArray(String)
         */
        public java.sql.Array getArray(String arg0)
                                throws SQLException
        {
            throw new NotImplemented();
        }

        public void setAsciiStream(int p1, final java.io.InputStream p2, 
                                   int p3)
                            throws java.sql.SQLException
        {
            delegate.setAsciiStream(p1, p2, p3);
        }

        /**
         * @see CallableStatement#setAsciiStream(String, InputStream, int)
         */
        public void setAsciiStream(String arg0, InputStream arg1, int arg2)
                            throws SQLException
        {
            throw new NotImplemented();
        }

        public void setBigDecimal(int p1, final java.math.BigDecimal p2)
                           throws java.sql.SQLException
        {
            delegate.setBigDecimal(p1, p2);
        }

        /**
         * @see CallableStatement#setBigDecimal(String, BigDecimal)
         */
        public void setBigDecimal(String arg0, BigDecimal arg1)
                           throws SQLException
        {
            throw new NotImplemented();
        }

        public java.math.BigDecimal getBigDecimal(int p1)
                                           throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public java.math.BigDecimal getBigDecimal(int p1, int p2)
                                           throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getBigDecimal(String)
         */
        public BigDecimal getBigDecimal(String arg0)
                                 throws SQLException
        {

            return null;
        }

        public void setBinaryStream(int p1, final java.io.InputStream p2, 
                                    int p3)
                             throws java.sql.SQLException
        {
            delegate.setBinaryStream(p1, p2, p3);
        }

        /**
         * @see CallableStatement#setBinaryStream(String, InputStream, int)
         */
        public void setBinaryStream(String arg0, InputStream arg1, int arg2)
                             throws SQLException
        {
            throw new NotImplemented();
        }

        public void setBlob(int p1, final java.sql.Blob p2)
                     throws java.sql.SQLException
        {
            delegate.setBlob(p1, p2);
        }

        public java.sql.Blob getBlob(int p1)
                              throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getBlob(String)
         */
        public java.sql.Blob getBlob(String arg0)
                              throws SQLException
        {
            throw new NotImplemented();
        }

        public void setBoolean(int p1, boolean p2)
                        throws java.sql.SQLException
        {
            delegate.setBoolean(p1, p2);
        }

        /**
         * @see CallableStatement#setBoolean(String, boolean)
         */
        public void setBoolean(String arg0, boolean arg1)
                        throws SQLException
        {
            throw new NotImplemented();
        }

        public boolean getBoolean(int p1)
                           throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getBoolean(String)
         */
        public boolean getBoolean(String arg0)
                           throws SQLException
        {
            throw new NotImplemented();
        }

        public void setByte(int p1, byte p2)
                     throws java.sql.SQLException
        {
            delegate.setByte(p1, p2);
        }

        /**
         * @see CallableStatement#setByte(String, byte)
         */
        public void setByte(String arg0, byte arg1)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        public byte getByte(int p1)
                     throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getByte(String)
         */
        public byte getByte(String arg0)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        public void setBytes(int p1, byte[] p2)
                      throws java.sql.SQLException
        {
            delegate.setBytes(p1, p2);
        }

        /**
         * @see CallableStatement#setBytes(String, byte[])
         */
        public void setBytes(String arg0, byte[] arg1)
                      throws SQLException
        {
            throw new NotImplemented();
        }

        public byte[] getBytes(int p1)
                        throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getBytes(String)
         */
        public byte[] getBytes(String arg0)
                        throws SQLException
        {
            throw new NotImplemented();
        }

        public void setCharacterStream(int p1, final java.io.Reader p2, int p3)
                                throws java.sql.SQLException
        {
            delegate.setCharacterStream(p1, p2, p3);
        }

        /**
         * @see CallableStatement#setCharacterStream(String, Reader, int)
         */
        public void setCharacterStream(String arg0, Reader arg1, int arg2)
                                throws SQLException
        {
            throw new NotImplemented();
        }

        public void setClob(int p1, final java.sql.Clob p2)
                     throws java.sql.SQLException
        {
            delegate.setClob(p1, p2);
        }

        public java.sql.Clob getClob(int p1)
                              throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getClob(String)
         */
        public Clob getClob(String arg0)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        public java.sql.Connection getConnection()
                                          throws java.sql.SQLException
        {

            return delegate.getConnection();
        }

        public void setCursorName(java.lang.String p1)
                           throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public void setDate(int p1, final java.sql.Date p2)
                     throws java.sql.SQLException
        {
            delegate.setDate(p1, p2);
        }

        public void setDate(int p1, final java.sql.Date p2, 
                            final java.util.Calendar p3)
                     throws java.sql.SQLException
        {
            delegate.setDate(p1, p2, p3);
        }

        /**
         * @see CallableStatement#setDate(String, Date, Calendar)
         */
        public void setDate(String arg0, Date arg1, Calendar arg2)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#setDate(String, Date)
         */
        public void setDate(String arg0, Date arg1)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        public java.sql.Date getDate(int p1)
                              throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public java.sql.Date getDate(int p1, final java.util.Calendar p2)
                              throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getDate(String, Calendar)
         */
        public Date getDate(String arg0, Calendar arg1)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#getDate(String)
         */
        public Date getDate(String arg0)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        public void setDouble(int p1, double p2)
                       throws java.sql.SQLException
        {
            delegate.setDouble(p1, p2);
        }

        /**
         * @see CallableStatement#setDouble(String, double)
         */
        public void setDouble(String arg0, double arg1)
                       throws SQLException
        {
            throw new NotImplemented();
        }

        public double getDouble(int p1)
                         throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getDouble(String)
         */
        public double getDouble(String arg0)
                         throws SQLException
        {
            throw new NotImplemented();
        }

        public void setEscapeProcessing(boolean p1)
                                 throws java.sql.SQLException
        {
            delegate.setEscapeProcessing(p1);
        }

        public void setFetchDirection(int p1)
                               throws java.sql.SQLException
        {
            delegate.setFetchDirection(p1);
        }

        public int getFetchDirection()
                              throws java.sql.SQLException
        {

            return delegate.getFetchDirection();
        }

        public void setFetchSize(int p1)
                          throws java.sql.SQLException
        {
            delegate.setFetchSize(p1);
        }

        public int getFetchSize()
                         throws java.sql.SQLException
        {

            return delegate.getFetchSize();
        }

        public void setFloat(int p1, float p2)
                      throws java.sql.SQLException
        {
            delegate.setFloat(p1, p2);
        }

        /**
         * @see CallableStatement#setFloat(String, float)
         */
        public void setFloat(String arg0, float arg1)
                      throws SQLException
        {
            throw new NotImplemented();
        }

        public float getFloat(int p1)
                       throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getFloat(String)
         */
        public float getFloat(String arg0)
                       throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see Statement#getGeneratedKeys()
         */
        public java.sql.ResultSet getGeneratedKeys()
                                            throws SQLException
        {

            return delegate.getGeneratedKeys();
        }

        public void setInt(int p1, int p2)
                    throws java.sql.SQLException
        {
            delegate.setInt(p1, p2);
        }

        /**
         * @see CallableStatement#setInt(String, int)
         */
        public void setInt(String arg0, int arg1)
                    throws SQLException
        {
            throw new NotImplemented();
        }

        public int getInt(int p1)
                   throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getInt(String)
         */
        public int getInt(String arg0)
                   throws SQLException
        {
            throw new NotImplemented();
        }

        public void setLong(int p1, long p2)
                     throws java.sql.SQLException
        {
            delegate.setLong(p1, p2);
        }

        /**
         * @see CallableStatement#setLong(String, long)
         */
        public void setLong(String arg0, long arg1)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        public long getLong(int p1)
                     throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getLong(String)
         */
        public long getLong(String arg0)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        public void setMaxFieldSize(int p1)
                             throws java.sql.SQLException
        {
            delegate.setMaxFieldSize(p1);
        }

        public int getMaxFieldSize()
                            throws java.sql.SQLException
        {

            return delegate.getMaxFieldSize();
        }

        public void setMaxRows(int p1)
                        throws java.sql.SQLException
        {
            delegate.setMaxRows(p1);
        }

        public int getMaxRows()
                       throws java.sql.SQLException
        {

            return delegate.getMaxRows();
        }

        public java.sql.ResultSetMetaData getMetaData()
                                               throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public boolean getMoreResults()
                               throws java.sql.SQLException
        {

            return delegate.getMoreResults();
        }

        /**
         * @see Statement#getMoreResults(int)
         */
        public boolean getMoreResults(int arg0)
                               throws SQLException
        {

            return delegate.getMoreResults();
        }

        public void setNull(int p1, int p2)
                     throws java.sql.SQLException
        {
            delegate.setNull(p1, p2);
        }

        public void setNull(int p1, int p2, java.lang.String p3)
                     throws java.sql.SQLException
        {
            delegate.setNull(p1, p2, p3);
        }

        /**
         * @see CallableStatement#setNull(String, int, String)
         */
        public void setNull(String arg0, int arg1, String arg2)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#setNull(String, int)
         */
        public void setNull(String arg0, int arg1)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        public void setObject(int p1, final java.lang.Object p2)
                       throws java.sql.SQLException
        {
            delegate.setObject(p1, p2);
        }

        public void setObject(int p1, final java.lang.Object p2, int p3)
                       throws java.sql.SQLException
        {
            delegate.setObject(p1, p2, p3);
        }

        public void setObject(int p1, final java.lang.Object p2, int p3, 
                              int p4)
                       throws java.sql.SQLException
        {
            delegate.setObject(p1, p2, p3, p4);
        }

        /**
         * @see CallableStatement#setObject(String, Object, int, int)
         */
        public void setObject(String arg0, Object arg1, int arg2, int arg3)
                       throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#setObject(String, Object, int)
         */
        public void setObject(String arg0, Object arg1, int arg2)
                       throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#setObject(String, Object)
         */
        public void setObject(String arg0, Object arg1)
                       throws SQLException
        {
            throw new NotImplemented();
        }

        public java.lang.Object getObject(int p1)
                                   throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public java.lang.Object getObject(int p1, final java.util.Map p2)
                                   throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getObject(String, Map)
         */
        public Object getObject(String arg0, Map arg1)
                         throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#getObject(String)
         */
        public Object getObject(String arg0)
                         throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see PreparedStatement#getParameterMetaData()
         */
        public ParameterMetaData getParameterMetaData()
                                               throws SQLException
        {

            return delegate.getParameterMetaData();
        }

        public void setQueryTimeout(int p1)
                             throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public int getQueryTimeout()
                            throws java.sql.SQLException
        {

            return delegate.getQueryTimeout();
        }

        public void setRef(int p1, final java.sql.Ref p2)
                    throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public java.sql.Ref getRef(int p1)
                            throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getRef(String)
         */
        public Ref getRef(String arg0)
                   throws SQLException
        {
            throw new NotImplemented();
        }

        public java.sql.ResultSet getResultSet()
                                        throws java.sql.SQLException
        {

            return delegate.getResultSet();
        }

        public int getResultSetConcurrency()
                                    throws java.sql.SQLException
        {

            return delegate.getResultSetConcurrency();
        }

        /**
         * @see Statement#getResultSetHoldability()
         */
        public int getResultSetHoldability()
                                    throws SQLException
        {

            return delegate.getResultSetHoldability();
        }

        public int getResultSetType()
                             throws java.sql.SQLException
        {

            return delegate.getResultSetType();
        }

        public void setShort(int p1, short p2)
                      throws java.sql.SQLException
        {
            delegate.setShort(p1, p2);
        }

        /**
         * @see CallableStatement#setShort(String, short)
         */
        public void setShort(String arg0, short arg1)
                      throws SQLException
        {
            throw new NotImplemented();
        }

        public short getShort(int p1)
                       throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getShort(String)
         */
        public short getShort(String arg0)
                       throws SQLException
        {
            throw new NotImplemented();
        }

        public void setString(int p1, java.lang.String p2)
                       throws java.sql.SQLException
        {
            delegate.setString(p1, p2);
        }

        /**
         * @see CallableStatement#setString(String, String)
         */
        public void setString(String arg0, String arg1)
                       throws SQLException
        {
            throw new NotImplemented();
        }

        public java.lang.String getString(int p1)
                                   throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getString(String)
         */
        public String getString(String arg0)
                         throws SQLException
        {
            throw new NotImplemented();
        }

        public void setTime(int p1, final java.sql.Time p2)
                     throws java.sql.SQLException
        {
            delegate.setTime(p1, p2);
        }

        public void setTime(int p1, final java.sql.Time p2, 
                            final java.util.Calendar p3)
                     throws java.sql.SQLException
        {
            delegate.setTime(p1, p2, p3);
        }

        /**
         * @see CallableStatement#setTime(String, Time, Calendar)
         */
        public void setTime(String arg0, Time arg1, Calendar arg2)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#setTime(String, Time)
         */
        public void setTime(String arg0, Time arg1)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        public java.sql.Time getTime(int p1)
                              throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public java.sql.Time getTime(int p1, final java.util.Calendar p2)
                              throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getTime(String, Calendar)
         */
        public Time getTime(String arg0, Calendar arg1)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#getTime(String)
         */
        public Time getTime(String arg0)
                     throws SQLException
        {
            throw new NotImplemented();
        }

        public void setTimestamp(int p1, final java.sql.Timestamp p2)
                          throws java.sql.SQLException
        {
            delegate.setTimestamp(p1, p2);
        }

        public void setTimestamp(int p1, final java.sql.Timestamp p2, 
                                 final java.util.Calendar p3)
                          throws java.sql.SQLException
        {
            delegate.setTimestamp(p1, p2, p3);
        }

        /**
         * @see CallableStatement#setTimestamp(String, Timestamp, Calendar)
         */
        public void setTimestamp(String arg0, Timestamp arg1, Calendar arg2)
                          throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#setTimestamp(String, Timestamp)
         */
        public void setTimestamp(String arg0, Timestamp arg1)
                          throws SQLException
        {
            throw new NotImplemented();
        }

        public java.sql.Timestamp getTimestamp(int p1)
                                        throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public java.sql.Timestamp getTimestamp(int p1, 
                                               final java.util.Calendar p2)
                                        throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#getTimestamp(String, Calendar)
         */
        public Timestamp getTimestamp(String arg0, Calendar arg1)
                               throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#getTimestamp(String)
         */
        public Timestamp getTimestamp(String arg0)
                               throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#setURL(String, URL)
         */
        public void setURL(String arg0, URL arg1)
                    throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see PreparedStatement#setURL(int, URL)
         */
        public void setURL(int arg0, URL arg1)
                    throws SQLException
        {
            delegate.setURL(arg0, arg1);
        }

        /**
         * @see CallableStatement#getURL(int)
         */
        public URL getURL(int arg0)
                   throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#getURL(String)
         */
        public URL getURL(String arg0)
                   throws SQLException
        {
            throw new NotImplemented();
        }

        public void setUnicodeStream(int p1, final java.io.InputStream p2, 
                                     int p3)
                              throws java.sql.SQLException
        {
            delegate.setUnicodeStream(p1, p2, p3);
        }

        public int getUpdateCount()
                           throws java.sql.SQLException
        {

            return delegate.getUpdateCount();
        }

        public java.sql.SQLWarning getWarnings()
                                        throws java.sql.SQLException
        {

            return delegate.getWarnings();
        }

        public void addBatch()
                      throws java.sql.SQLException
        {
            delegate.addBatch();
        }

        public void addBatch(java.lang.String p1)
                      throws java.sql.SQLException
        {
            delegate.addBatch(p1);
        }

        public void cancel()
                    throws java.sql.SQLException
        {
            delegate.cancel();
        }

        public void clearBatch()
                        throws java.sql.SQLException
        {
            delegate.clearBatch();
        }

        public void clearParameters()
                             throws java.sql.SQLException
        {
            delegate.clearParameters();
        }

        public void clearWarnings()
                           throws java.sql.SQLException
        {
            delegate.clearWarnings();
        }

        public void close()
                   throws java.sql.SQLException
        {
            delegate.close();
        }

        public boolean execute()
                        throws java.sql.SQLException
        {

            return delegate.execute();
        }

        public boolean execute(java.lang.String p1)
                        throws java.sql.SQLException
        {

            return delegate.execute(p1);
        }

        /**
         * @see Statement#execute(String, int)
         */
        public boolean execute(String arg0, int arg1)
                        throws SQLException
        {

            return delegate.execute(arg0, arg1);
        }

        /**
         * @see Statement#execute(String, int[])
         */
        public boolean execute(String arg0, int[] arg1)
                        throws SQLException
        {

            return delegate.execute(arg0, arg1);
        }

        /**
         * @see Statement#execute(String, String[])
         */
        public boolean execute(String arg0, String[] arg1)
                        throws SQLException
        {

            return delegate.execute(arg0, arg1);
        }

        public int[] executeBatch()
                           throws java.sql.SQLException
        {

            return delegate.executeBatch();
        }

        public java.sql.ResultSet executeQuery()
                                        throws java.sql.SQLException
        {

            return delegate.executeQuery();
        }

        public java.sql.ResultSet executeQuery(java.lang.String p1)
                                        throws java.sql.SQLException
        {

            return delegate.executeQuery(p1);
        }

        public int executeUpdate()
                          throws java.sql.SQLException
        {

            return delegate.executeUpdate();
        }

        public int executeUpdate(java.lang.String p1)
                          throws java.sql.SQLException
        {

            return delegate.executeUpdate(p1);
        }

        /**
         * @see Statement#executeUpdate(String, int)
         */
        public int executeUpdate(String arg0, int arg1)
                          throws SQLException
        {

            return delegate.executeUpdate(arg0, arg1);
        }

        /**
         * @see Statement#executeUpdate(String, int[])
         */
        public int executeUpdate(String arg0, int[] arg1)
                          throws SQLException
        {

            return delegate.executeUpdate(arg0, arg1);
        }

        /**
         * @see Statement#executeUpdate(String, String[])
         */
        public int executeUpdate(String arg0, String[] arg1)
                          throws SQLException
        {

            return delegate.executeUpdate(arg0, arg1);
        }

        public void registerOutParameter(int p1, int p2)
                                  throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public void registerOutParameter(int p1, int p2, int p3)
                                  throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        public void registerOutParameter(int p1, int p2, java.lang.String p3)
                                  throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        }

        /**
         * @see CallableStatement#registerOutParameter(String, int, int)
         */
        public void registerOutParameter(String arg0, int arg1, int arg2)
                                  throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#registerOutParameter(String, int, String)
         */
        public void registerOutParameter(String arg0, int arg1, String arg2)
                                  throws SQLException
        {
            throw new NotImplemented();
        }

        /**
         * @see CallableStatement#registerOutParameter(String, int)
         */
        public void registerOutParameter(String arg0, int arg1)
                                  throws SQLException
        {
            throw new NotImplemented();
        }

        public boolean wasNull()
                        throws java.sql.SQLException
        {
            throw new SQLException("Not supported");
        } 
    }
    
    public boolean useTimezone()
    {
    	return _useTimezone;
    }
    
    public TimeZone getServerTimezone()
    {
    	return _serverTimezone;
    }
}