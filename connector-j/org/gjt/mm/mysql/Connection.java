/*
 * MM JDBC Drivers for MySQL
 *
 * $Id$
 *
 * Copyright (C) 1998 Mark Matthews <mmatthew@worldserver.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * See the COPYING file located in the top-level-directory of
 * the archive of this library for complete text of license.
 *
 * Some portions:
 *
 * Copyright (c) 1996 Bradley McLean / Jeffrey Medeiros
 * Modifications Copyright (c) 1996/1997 Martin Rode
 * Copyright (c) 1997 Peter T Mount
 */

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
 * <p><B>Note:</B> MySQL does not support transactions, so all queries
 *                 are committed as they are executed.
 *
 * @see java.sql.Connection
 * @author Mark Matthews mmatthew@worldserver.com
 * @version $Id$
 */
package org.gjt.mm.mysql;

import java.io.UnsupportedEncodingException;

import java.sql.*;

import java.util.Hashtable;
import java.util.Properties;


public abstract class Connection
{


    /** 
     * The I/O abstraction interface (network conn to
     * MySQL server
     */
    MysqlIO _io = null;

    /**
     * Has this connection been closed?
     */
    private boolean _isClosed = true;

    /**
     * When did the last query finish?
     */
    private long _lastQueryFinishedTime = 0;

    /**
     * The hostname we're connected to
     */
    private String _host = null;

    /**
     * The port number we're connected to
     * (defaults to 3306)
     */
    private int _port = 3306;

    /**
     * The user we're connected as
     */
    private String _user = null;

    /**
     * The password we used
     */
    private String _password = null;

    /**
     * The database we're currently using
     * (called Catalog in JDBC terms).
     */
    protected String _database = null;

    /**
     * Are we in autoCommit mode?
     */
    private boolean _autoCommit = true;

    /**
     * Are we in read-only mode?
     */
    private boolean _readOnly = false;

    /**
     * Should we do unicode character conversions?
     */
    private boolean _doUnicode = false;

    /**
     * If we're doing unicode character conversions,
     * what encoding do we use?
     */
    private String _encoding = null;

    /**
     * The JDBC URL we're using
     */
    private String _myURL = null;

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
     * The driver instance that created us
     */
    private org.gjt.mm.mysql.Driver _myDriver;

    /**
     * The map of server variables that we retrieve
     * at connection init.
     */
    private Hashtable _serverVariables = null;

    /**
     * The largest packet we can send (changed
     * once we know what the server supports, we
     * get this at connection init).
     */
    private int _maxAllowedPacket = 65536;
    private int _netBufferLength = 16384;

    /**
     * Can we use the "ping" command rather than a 
     * query?
     */
    private boolean _useFastPing = false;

    //
    // This is for the high availability :) routines
    //
    private boolean _highAvailability = false;
    private int _maxReconnects = 3;
    private double _initialTimeout = 2.0D;

    // The command used to "ping" the database.
    // Newer versions of MySQL server have a ping() command,
    // but this works for everything.4
    private static final String _PING_COMMAND = "SELECT 1";

    /** Are transactions supported by the MySQL server we are connected to? */
    private boolean _transactionsSupported = false;

    /** Do we relax the autoCommit semantics? (For enhydra, for example) */
    private boolean _relaxAutoCommit = false;

    /** Should we return PreparedStatements for UltraDev's stupid bug? */
    protected boolean _useUltraDevWorkAround = false;

    /** Does the server suuport isolation levels? */
    private boolean _hasIsolationLevels = false;

    /**
     * Should we capitalize mysql types
     */
    private boolean _capitalizeDBMDTypes = false;


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
        if (Driver.trace)
        {
            Object[] args = {host, new Integer(port), info, database, url, d};
            Debug.methodCall(this, "constructor", args);
        }

        if (host == null)
        {
            _host = "localhost";
        }
        else
        {
            _host = new String(host);
        }

        _port = port;

        if (database == null)
        {
            throw new SQLException("Malformed URL '" + url + 
                                   "'.", "S1000");
        }

        _database = new String(database);
        _myURL = new String(url);
        _myDriver = d;
        _user = info.getProperty("user");
        _password = info.getProperty("password");

        if (_user == null || _user.equals(""))
        {
            _user = "nobody";
        }

        if (_password == null)
        {
            _password = "";
        }

        // Check for driver specific properties
        if (info.getProperty("_relaxAutoCommit") != null)
        {
            _relaxAutoCommit = info.getProperty("_relaxAutoCommit").toUpperCase().equals(
                                       "TRUE");
        }

        if (info.getProperty("autoReconnect") != null)
        {
            _highAvailability = info.getProperty("autoReconnect").toUpperCase().equals(
                                        "TRUE");
        }

        if (info.getProperty("capitalizeTypeNames") != null)
        {
            _capitalizeDBMDTypes = info.getProperty("capitalizeTypeNames").toUpperCase().equals(
                                          "TRUE");
        }

        if (info.getProperty("ultraDevHack") != null)
        {
            _useUltraDevWorkAround = info.getProperty("ultraDevHack").toUpperCase().equals(
                                             "TRUE");
        }

        if (_highAvailability)
        {
            if (info.getProperty("maxReconnects") != null)
            {
                try
                {
                    int n = Integer.parseInt(info.getProperty("maxReconnects"));
                    _maxReconnects = n;
                }
                catch (NumberFormatException NFE)
                {
                    throw new SQLException("Illegal parameter '" + 
                                           info.getProperty("maxReconnects") + 
                                           "' for maxReconnects", "0S100");
                }
            }

            if (info.getProperty("initialTimeout") != null)
            {
                try
                {
                    double n = Integer.parseInt(info.getProperty(
                                                        "initialTimeout"));
                    _initialTimeout = n;
                }
                catch (NumberFormatException NFE)
                {
                    throw new SQLException("Illegal parameter '" + 
                                           info.getProperty("initialTimeout") + 
                                           "' for initialTimeout", "0S100");
                }
            }
        }

        if (info.getProperty("maxRows") != null)
        {
            try
            {
                int n = Integer.parseInt(info.getProperty("maxRows"));

                if (n == 0)
                {
                    n = -1;
                } // adjust so that it will become MysqlDefs.MAX_ROWS

                // in execSQL()
                _maxRows = n;
            }
            catch (NumberFormatException NFE)
            {
                throw new SQLException("Illegal parameter '" + 
                                       info.getProperty("maxRows") + 
                                       "' for maxRows", "0S100");
            }
        }

        if (info.getProperty("useUnicode") != null)
        {
            String useUnicode = info.getProperty("useUnicode").toUpperCase();

            if (useUnicode.startsWith("TRUE"))
            {
                _doUnicode = true;
            }

            if (info.getProperty("characterEncoding") != null)
            {
                _encoding = info.getProperty("characterEncoding");

                // Attempt to use the encoding, and bail out if it
                // can't be used
                try
                {
                    String testString = "abc";
                    testString.getBytes(_encoding);
                }
                catch (UnsupportedEncodingException UE)
                {
                    throw new SQLException("Unsupported character encoding '" + 
                                           _encoding + 
                                           "'.", "0S100");
                }
            }
        }

        if (Driver.debug)
            System.out.println("Connect: " + _user + 
                               " to " + _database);

        try
        {
            _io = createNewIO(host, port);
            _io.init(_user, _password);

            if (_database.length() != 0)
            {
                _io.sendCommand(MysqlDefs.INIT_DB, _database, null);
            }

            _isClosed = false;
            _serverVariables = new Hashtable();

            if (_io.versionMeetsMinimum(3, 22, 1))
            {
                _useFastPing = true;
            }

            //
            // If version is greater than 3.21.22 get the server
            // variables.
            if (_io.versionMeetsMinimum(3, 21, 22))
            {
                org.gjt.mm.mysql.Statement stmt = null;
                org.gjt.mm.mysql.ResultSet results = null;

                try
                {
                    stmt = (org.gjt.mm.mysql.Statement)createStatement();
                    results = (org.gjt.mm.mysql.ResultSet)stmt.executeQuery(
                                      "SHOW VARIABLES");

                    while (results.next())
                    {
                        _serverVariables.put(results.getString(1), 
                                             results.getString(2));
                    }
                }
                catch (java.sql.SQLException e)
                {
                    throw e;
                }
                finally
                {
                    if (results != null)
                    {
                        try
                        {
                            results.close();
                        }
                        catch (java.sql.SQLException sqlE)
                        {
                        }
                    }

                    if (stmt != null)
                    {
                        try
                        {
                            stmt.close();
                        }
                        catch (java.sql.SQLException sqlE)
                        {
                        }
                    }
                }

                if (_serverVariables.containsKey("max_allowed_packet"))
                {
                    _maxAllowedPacket = Integer.parseInt((String)_serverVariables.get(
                                                                 "max_allowed_packet"));
                }

                if (_serverVariables.containsKey("net_buffer_length"))
                {
                    _netBufferLength = Integer.parseInt((String)_serverVariables.get(
                                                                "net_buffer_length"));
                }

                checkTransactionIsolationLevel();
                checkServerEncoding();
            }

            if (_io.versionMeetsMinimum(3, 23, 15))
            {
                _transactionsSupported = true;
            }
            else
            {
                _transactionsSupported = false;
            }

            if (_io.versionMeetsMinimum(3, 23, 36))
            {
                _hasIsolationLevels = true;
            }
            else
            {
                _hasIsolationLevels = false;
            }

            _io.resetMaxBuf();
        }
        catch (java.sql.SQLException ex)
        {
             // don't clobber SQL exceptions
            throw ex;
        }
        catch (Exception ex)
        {
            throw new java.sql.SQLException(
                    "Cannot connect to MySQL server on " + 
                    _host + ":" + _port + 
                    ". Is there a MySQL server running on the machine/port you are trying to connect to? (" + 
                    ex.getClass().getName() + ")", "08S01");
        }
    }

    public boolean capitalizeDBMDTypes()
    {
        return _capitalizeDBMDTypes;
    }

    public boolean supportsTransactions()
    {
        return _transactionsSupported;
    }

    public boolean supportsIsolationLevel()
    {
        return _hasIsolationLevels;
    }


    /**
     * SQL statements without parameters are normally executed using
     * Statement objects.  If the same SQL statement is executed many
     * times, it is more efficient to use a PreparedStatement
     *
     * @return a new Statement object
     * @exception java.sql.SQLException passed through from the constructor
     */
    public abstract java.sql.Statement createStatement()
                                                throws java.sql.SQLException;


    /**
     * A SQL statement with or without IN parameters can be pre-compiled
     * and stored in a PreparedStatement object.  This object can then
     * be used to efficiently execute this statement multiple times.
     * 
     * <p>
     * <B>Note:</B> This method is optimized for handling parametric
     * SQL statements that benefit from precompilation if the driver
     * supports precompilation. 
     * In this case, the statement is not sent to the database until the
     * PreparedStatement is executed.  This has no direct effect on users;
     * however it does affect which method throws certain java.sql.SQLExceptions
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
    public abstract java.sql.PreparedStatement prepareStatement(String sql)
     throws java.sql.SQLException;


    /**
     * A SQL stored procedure call statement is handled by creating a
     * CallableStatement for it.  The CallableStatement provides methods
     * for setting up its IN and OUT parameters and methods for executing
     * it.
     *
     * <B>Note:</B> This method is optimised for handling stored procedure
     * call statements.  Some drivers may send the call statement to the
     * database when the prepareCall is done; others may wait until the
     * CallableStatement is executed.  This has no direct effect on users;
     * however, it does affect which method throws certain java.sql.SQLExceptions
     *
     * @param sql a SQL statement that may contain one or more '?' parameter
     *    placeholders.  Typically this statement is a JDBC function call
     *    escape string.
     * @return a new CallableStatement object containing the pre-compiled
     *    SQL statement
     * @exception java.sql.SQLException if a database access error occurs
     */
    public java.sql.CallableStatement prepareCall(String sql)
                                           throws java.sql.SQLException
    {
        throw new java.sql.SQLException("Callable statments not supported.", 
                                        "S1C00");
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
        if (Driver.trace)
        {
            Object[] args = {sql};
            Debug.methodCall(this, "nativeSQL", args);
            Debug.returnValue(this, "nativeSQL", sql);
        }

        return sql;
    }


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
        if (Driver.trace)
        {
            Object[] args = {new Boolean(autoCommit)};
            Debug.methodCall(this, "setAutoCommit", args);
        }

        if (_transactionsSupported)
        {
            String sql = "SET autocommit=" + 
                         (autoCommit ? "1" : "0");
            execSQL(sql, -1);
            _autoCommit = autoCommit;
        }
        else
        {
            if (autoCommit == false && 
                _relaxAutoCommit == false)
            {
                throw new SQLException("MySQL Versions Older than 3.23.15 do not support transactions", 
                                       "08003");
            }
            else
            {
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
        if (Driver.trace)
        {
            Object[] args = new Object[0];
            Debug.methodCall(this, "getAutoCommit", args);
            Debug.returnValue(this, "getAutoCommit", new Boolean(_autoCommit));
        }

        return _autoCommit;
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
        if (Driver.trace)
        {
            Object[] args = new Object[0];
            Debug.methodCall(this, "commit", args);
        }

        if (_isClosed)
        {
            throw new java.sql.SQLException("Commit attempt on closed connection.", 
                                            "08003");
        }

        // no-op if _relaxAutoCommit == true
        if (_autoCommit && !_relaxAutoCommit)
        {
            throw new SQLException("Can't call commit when autocommit=true");
        }
        else if (_transactionsSupported)
        {
            execSQL("commit", -1);
        }

        return;
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
        if (Driver.trace)
        {
            Object[] args = new Object[0];
            Debug.methodCall(this, "rollback", args);
        }

        if (_isClosed)
        {
            throw new java.sql.SQLException("Rollback attempt on closed connection.", 
                                            "08003");
        }

        // no-op if _relaxAutoCommit == true
        if (_autoCommit && !_relaxAutoCommit)
        {
            throw new SQLException("Can't call commit when autocommit=true", 
                                   "08003");
        }
        else if (_transactionsSupported)
        {
            execSQL("rollback", -1);
        }
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
        if (Driver.trace)
        {
            Object[] args = new Object[0];
            Debug.methodCall(this, "close", args);
        }

        if (_io != null)
        {
            try
            {
                _io.quit();
            }
            catch (Exception e)
            {
            }

            _io = null;
        }

        _isClosed = true;
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
        if (Driver.trace)
        {
            Object[] args = new Object[0];
            Debug.methodCall(this, "isClosed", args);
            Debug.returnValue(this, "isClosed", new Boolean(_isClosed));
        }

        if (!_isClosed)
        {

            // Test the connection
            try
            {
                synchronized (_io)
                {
                    ping();
                }
            }
            catch (Exception E)
            {
                _isClosed = true;
            }
        }

        return _isClosed;
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
    public abstract java.sql.DatabaseMetaData getMetaData()
                                                   throws java.sql.SQLException;


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
        if (Driver.trace)
        {
            Object[] args = {new Boolean(readOnly)};
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
        if (Driver.trace)
        {
            Object[] args = new Object[0];
            Debug.methodCall(this, "isReadOnly", args);
            Debug.returnValue(this, "isReadOnly", new Boolean(_readOnly));
        }

        return _readOnly;
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
        if (Driver.trace)
        {
            Object[] args = {catalog};
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
        if (Driver.trace)
        {
            Object[] args = new Object[0];
            Debug.methodCall(this, "getCatalog", args);
            Debug.returnValue(this, "getCatalog", _database);
        }

        return _database;
    }


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

    public void setTransactionIsolation(int level)
                                 throws java.sql.SQLException
    {
        if (Driver.trace)
        {
            Object[] args = {new Integer(level)};
            Debug.methodCall(this, "setTransactionIsolation", args);
        }

        if (_hasIsolationLevels)
        {
            StringBuffer sql = new StringBuffer("SET SESSION TRANSACTION ISOLATION LEVEL ");

            switch (level)
            {
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
                    throw new SQLException(
                            "Unsupported transaction isolation level '" + 
                            level + "'", "S1C00");
            }

            execSQL(sql.toString(), -1);
            isolationLevel = level;
        }
        else
        {
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
        if (Driver.trace)
        {
            Object[] args = new Object[0];
            Debug.methodCall(this, "getTransactionIsolation", args);
            Debug.returnValue(this, "getTransactionIsolation", 
                              new Integer(java.sql.Connection.TRANSACTION_SERIALIZABLE));
        }

        return isolationLevel;
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
        if (Driver.trace)
        {
            Object[] args = new Object[0];
            Debug.methodCall(this, "getWarnings", args);
            Debug.returnValue(this, "getWarnings", null);
        }

        return null;
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
        if (Driver.trace)
        {
            Object[] args = new Object[0];
            Debug.methodCall(this, "clearWarnings", args);
        }

        // firstWarning = null;
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
        if (_lastQueryFinishedTime == 0)
        {
            return 0;
        }
        else
        {
            long now = System.currentTimeMillis();
            long idleTime = now - _lastQueryFinishedTime;

            return idleTime;
        }
    }

    public void finalize()
                  throws Throwable
    {
        if (_io != null && !isClosed())
        {
            close();
        }
        else if (_io != null)
        {
            _io.forceClose();
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
        if (_useFastPing)
        {
            _io.sendCommand(MysqlDefs.PING, null, null);
        }
        else
        {
            _io.sqlQuery(_PING_COMMAND, MysqlDefs.MAX_ROWS);
        }
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
        if (Driver.trace)
        {
            Object[] args = {sql, new Integer(max_rows)};
            Debug.methodCall(this, "execSQL", args);
        }

        return execSQL(sql, max_rows, null);
    }

    ResultSet execSQL(String sql, int maxRows, Buffer packet)
               throws java.sql.SQLException
    {
        if (Driver.trace)
        {
            Object[] args = {sql, new Integer(maxRows), packet};
            Debug.methodCall(this, "execSQL", args);
        }

        synchronized (_io)
        {
            _lastQueryFinishedTime = 0; // we're busy!

            if (_highAvailability)
            {
                try
                {
                    ping();
                }
                catch (Exception Ex)
                {
                    double timeout = _initialTimeout;
                    boolean connectionGood = false;

                    for (int i = 0; i < _maxReconnects; i++)
                    {
                        try
                        {
                            try
                            {
                                _io.forceClose();
                            }
                            catch (Exception ex)
                            {

                                // do nothing
                            }

                            _io = createNewIO(_host, _port);
                            _io.init(_user, _password);

                            if (_database.length() != 0)
                            {
                                _io.sendCommand(MysqlDefs.INIT_DB, _database, 
                                                null);
                            }

                            if (_useFastPing)
                            {
                                _io.sendCommand(MysqlDefs.PING, null, null);
                            }
                            else
                            {
                                _io.sqlQuery(_PING_COMMAND, MysqlDefs.MAX_ROWS);
                            }

                            connectionGood = true;
                            break;
                        }
                        catch (Exception EEE)
                        {
                        }

                        try
                        {
                            Thread.currentThread().sleep((long)timeout * 1000);
                            timeout = timeout * timeout;
                        }
                        catch (InterruptedException IE)
                        {
                        }
                    }

                    if (!connectionGood)
                    {
                         // We've really failed!
                        throw new SQLException(
                                "Server connection failure during transaction. \nAttemtped reconnect " + 
                                _maxReconnects + 
                                " times. Giving up.", "08001");
                    }
                }
            }

            try
            {
                int realMaxRows = (maxRows == -1) ? MysqlDefs.MAX_ROWS : maxRows;

                if (packet == null)
                {
                    String encoding = null;

                    if (useUnicode())
                    {
                        encoding = getEncoding();
                    }

                    return _io.sqlQuery(sql, realMaxRows, encoding, this);
                }
                else
                {
                    return _io.sqlQueryDirect(packet, realMaxRows, this);
                }
            }
            catch (java.io.EOFException eofE)
            {
                throw new java.sql.SQLException("Lost connection to server during query", 
                                                "08007");
            }
            catch (java.sql.SQLException sqlE)
            {
                 // don't clobber SQL exceptions
                throw sqlE;
            }
            catch (Exception ex)
            {
                String exceptionType = ex.getClass().getName();
                String exceptionMessage = ex.getMessage();
                throw new java.sql.SQLException(
                        "Error during query: Unexpected Exception: " + 
                        exceptionType + " message given: " + 
                        exceptionMessage, "S1000");
            }
            finally
            {
                _lastQueryFinishedTime = System.currentTimeMillis();
            }
        }
    }


    /** JDBC1 and JDBC2 version of Connection create their own IO classes */
    protected abstract MysqlIO createNewIO(String host, int port)
                                    throws Exception;

    String getURL()
    {
        return _myURL;
    }

    String getUser()
    {
        return _user;
    }

    String getServerVersion()
    {
        return _io.getServerVersion();
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


    /** Has the maxRows value changed? */
    synchronized void maxRowsChanged()
    {
        _maxRowsChanged = true;
    }


    /** Has maxRows() been set? */
    synchronized boolean useMaxRows()
    {
        return _maxRowsChanged;
    }


    /** Should unicode character mapping be used ? */
    public boolean useUnicode()
    {
        return _doUnicode;
    }


    /** Returns the character encoding for this Connection */
    public String getEncoding()
    {
        return _encoding;
    }


    /** Returns the Mutex all queries are locked against */
    Object getMutex()
             throws SQLException
    {
        if (_io == null)
        {
            throw new SQLException("Connection.close() has already been called. Invalid operation in this state.", 
                                   "08003");
        }

        return _io;
    }


    /** Returns the maximum packet size the MySQL server will accept */
    int getMaxAllowedPacket()
    {
        return _maxAllowedPacket;
    }


    /** Returns the packet buffer size the MySQL server reported
     *  upon connection */
    int getNetBufferLength()
    {
        return _netBufferLength;
    }

    protected MysqlIO getIO()
    {
        return _io;
    }


    /**
     * If useUnicode flag is set and explicit client character encoding isn't specified
     * then assign encoding from server if any.
     * Is called by connectionInit(...)
     */
    private void checkServerEncoding()
                              throws SQLException
    {
        if (useUnicode() && getEncoding() == null)
        {
            _encoding = (String)_serverVariables.get("character_set");

            if (_encoding != null)
            {

                // dirty hack to work around discrepancies in character encoding names, e.g.
                // mysql    java
                // latin1   Latin1
                // cp1251   Cp1251
                if (Character.isLowerCase(_encoding.charAt(0)))
                {
                    char[] ach = _encoding.toCharArray();
                    ach[0] = Character.toUpperCase(_encoding.charAt(0));
                    _encoding = new String(ach);
                }

                // Attempt to use the encoding, and bail out if it
                // can't be used
                try
                {
                    String TestString = "abc";
                    TestString.getBytes(_encoding);
                }
                catch (UnsupportedEncodingException UE)
                {

                    //              throw new SQLException("Unsupported character encoding '" +
                    //                                     _encoding + "'.", "0S100");
                    _encoding = null;
                }
            }
        }
    }


    /**
     * Map mysql transaction isolation level name to java.sql.Connection.TRANSACTION_XXX
     */
    static private Hashtable _mapTransIsolationName2Value = null;

    static
    {
        _mapTransIsolationName2Value = new Hashtable(8);
        _mapTransIsolationName2Value.put("READ-UNCOMMITED", 
                                         new Integer(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED));
        _mapTransIsolationName2Value.put("READ-COMMITTED", 
                                         new Integer(java.sql.Connection.TRANSACTION_READ_COMMITTED));
        _mapTransIsolationName2Value.put("REPEATABLE-READ", 
                                         new Integer(java.sql.Connection.TRANSACTION_REPEATABLE_READ));
        _mapTransIsolationName2Value.put("SERIALIZABLE", 
                                         new Integer(java.sql.Connection.TRANSACTION_SERIALIZABLE));
    }

    /**
     * Set transaction isolation level to the value received from server if any.
     * Is called by connectionInit(...)
     */
    private void checkTransactionIsolationLevel()
                                         throws SQLException
    {
        String s = (String)_serverVariables.get("transaction_isolation");

        if (s != null)
        {
            Integer intTI = (Integer)_mapTransIsolationName2Value.get(s);

            if (intTI != null)
            {
                isolationLevel = intTI.intValue();
            }
        }
    }
}