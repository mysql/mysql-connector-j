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

/**
 * This class is used by Connection for communicating with the
 * MySQL server.
 *
 * @see java.sql.Connection
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */
package com.mysql.jdbc;

import java.io.*;

import java.net.*;

import java.sql.*;
import java.sql.SQLException;

import java.util.*;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


public class MysqlIO
{

    //~ Instance/static variables .............................................

    static final int   COMP_HEADER_LENGTH     = 3;
    static final int   HEADER_LENGTH          = 4;
    static int          MAXBUF                 = 65535;
    private static int CLIENT_COMPRESS        = 32; /* Can use compression 
    protcol */
    private static int CLIENT_CONNECT_WITH_DB = 8;
    private static int CLIENT_FOUND_ROWS      = 2;
    private static int CLIENT_IGNORE_SPACE    = 256; /* Ignore spaces 
    before '(' */
    private static int CLIENT_LOCAL_FILES     = 128; /* Can use LOAD DATA 
    LOCAL */

    /* Found instead of 
    affected rows */
    private static int CLIENT_LONG_FLAG     = 4; /* Get all column flags */
    private static int CLIENT_LONG_PASSWORD = 1; /* new more secure 
    passwords */

    /* One can specify db 
    on connect */
    private static int CLIENT_NO_SCHEMA = 16; /* Don't allow 
    db.table.column */
    private static int CLIENT_ODBC = 64; /* Odbc client */

    //
    // For SQL Warnings
    //
    java.sql.SQLWarning               _warningChain        = null;
    private boolean                   _colDecimalNeedsBump = false; // do we need to increment the colDecimal flag?
    private com.mysql.jdbc.Connection _connection;
    private Deflater                  _deflater            = null;
    private String                    _host                = null;
    private Inflater                  _inflater            = null;

    /** The connection to the server */
    private Socket _mysqlConnection = null;

    /** Buffered data from the server */

    //private BufferedInputStream  _Mysql_Buf_Input          = null;

    /** Buffered data to the server */

    //private BufferedOutputStream _Mysql_Buf_Output         = null;

    /** Data from the server */

    //private DataInputStream      _Mysql_Input              = null;
    private InputStream _mysqlInput = null;

    /** Data to the server */

    //private DataOutputStream     _Mysql_Output             = null;
    private BufferedOutputStream _mysqlOutput    = null;
    private byte                 _packetSequence = 0;

    /** Current open, streaming result set (if any) */
    private ResultSet _pendingResultSet = null;
    private int       _port            = 3306;
    private boolean   _profileSql      = false;
    private byte      _protocolVersion = 0;

    //
    // Use this when reading in rows to avoid thousands of new()
    // calls, because the byte arrays just get copied out of the
    // packet anyway
    //
    private Buffer  _reusablePacket        = null;
    private Buffer  _sendPacket            = null;
    private int     _serverMajorVersion    = 0;
    private int     _serverMinorVersion    = 0;
    private int     _serverSubMinorVersion = 0;
    private String  _serverVersion         = null;
    private boolean _useCompression        = false;
    private boolean _useNewUpdateCounts    = false; // should we use the new larger update counts?

    //~ Constructors ..........................................................

    /**
     * Constructor:  Connect to the MySQL server and setup
     * a stream connection.
     *
     * @param host the hostname to connect to
     * @param port the port number that the server is listening on
     * @exception IOException if an IOException occurs during connect.
     */
    public MysqlIO(String Host, int port, com.mysql.jdbc.Connection Conn)
            throws IOException, java.sql.SQLException
    {
        _connection      = Conn;
        _reusablePacket  = new Buffer(_connection.getNetBufferLength(), 
                                      _connection.getMaxAllowedPacket());
        _port            = port;
        _host            = Host;
        _mysqlConnection = new Socket(_host, _port);

        try {
            _mysqlConnection.setTcpNoDelay(true);
        } catch (Exception ex) {

            /* Ignore */
        }

        _mysqlInput  = new BufferedInputStream(_mysqlConnection.getInputStream(), 
                                               16384);
        _mysqlOutput = new BufferedOutputStream(_mysqlConnection.getOutputStream());

        //_Mysql_Input  = new DataInputStream(_Mysql_Buf_Input);
        //_Mysql_Output = new DataOutputStream(_Mysql_Buf_Output);
    }

    //~ Methods ...............................................................

    /**
     * Sets the _profileSql.
     * @param _profileSql The _profileSql to set
     */
    public void setProfileSql(boolean flag)
    {
        this._profileSql = flag;
    }

    /**
     * Build a result set. Delegates to buildResultSetWithRows() to
     * build a JDBC-version-specific ResultSet, given rows as byte
     * data, and field information.
     */
    protected ResultSet getResultSet(long columnCount, int max_rows, 
                                     int resultSetType)
                              throws Exception
    {

        Buffer  Packet; // The packet from the server
        Field[] Fields = new Field[(int)columnCount];

        // Read in the column information
        for (int i = 0; i < columnCount; i++) {
            Packet = readPacket();

            String TableName = Packet.readLenString();
            String ColName   = Packet.readLenString();
            int    colLength = Packet.readnBytes();
            int    colType   = Packet.readnBytes();
            Packet.readByte(); // We know it's currently 2

            short colFlag     = (short)(Packet.readByte() & 0xff);
            int   colDecimals = (Packet.readByte() & 0xff);

            if (_colDecimalNeedsBump) {
                colDecimals++;
            }

            Fields[i] = new Field(TableName, ColName, colLength, colType, 
                                  colFlag, colDecimals);
        }

        Packet = readPacket();

        ArrayList Rows = new ArrayList();

        // Now read the data
        byte[][] Row       = nextRow((int)columnCount);
        int      row_count = 0;

        if (Row != null) {
            Rows.add(Row);
            row_count = 1;
        }

        while (Row != null && row_count < max_rows) {
            Row = nextRow((int)columnCount);

            if (Row != null) {
                Rows.add(Row);
                row_count++;
            } else {

                if (Driver.trace) {
                    Debug.msg(this, "* NULL Row *");
                }
            }
        }

        if (Driver.trace) {
            Debug.msg(this, "* Fetched " + Rows.size() + 
                      " rows from server *");
        }

        return buildResultSetWithRows(Fields, Rows, null, resultSetType);
    }

    protected final void forceClose()
                             throws IOException
    {
        _mysqlConnection.close();
    }

    protected com.mysql.jdbc.ResultSet buildResultSetWithRows(com.mysql.jdbc.Field[]    Fields, 
                                                              ArrayList                 Rows, 
                                                              com.mysql.jdbc.Connection Conn, 
                                                              int                       resultSetType)
    {

        switch (resultSetType) {

            case java.sql.ResultSet.CONCUR_READ_ONLY:
                return new com.mysql.jdbc.ResultSet(Fields, Rows, Conn);

            case java.sql.ResultSet.CONCUR_UPDATABLE:
                return new com.mysql.jdbc.UpdatableResultSet(Fields, Rows, 
                                                             Conn);

            default:
                return new com.mysql.jdbc.ResultSet(Fields, Rows, Conn);
        }
    }

    protected com.mysql.jdbc.ResultSet buildResultSetWithUpdates(long                      updateCount, 
                                                                 long                      updateID, 
                                                                 com.mysql.jdbc.Connection Conn)
    {

        return new com.mysql.jdbc.ResultSet(updateCount, updateID);
    }

    static int getMaxBuf()
    {

        return MAXBUF;
    }

    /**
     * Get the major version of the MySQL server we are
     * talking to.
     */
    final int getServerMajorVersion()
    {

        return _serverMajorVersion;
    }

    /**
     * Get the minor version of the MySQL server we are
     * talking to.
     */
    final int getServerMinorVersion()
    {

        return _serverMinorVersion;
    }

    /**
     * Get the sub-minor version of the MySQL server we are
     * talking to.
     */
    final int getServerSubMinorVersion()
    {

        return _serverSubMinorVersion;
    }

    /**
     * Get the version string of the server we are talking to
     */
    String getServerVersion()
    {

        return _serverVersion;
    }

    /**
     * Initialize communications with the MySQL server.
     *
     * Handles logging on, and handling initial connection errors.
     */
    void init(String User, String Password)
       throws java.sql.SQLException
    {

        String Seed;

        try {

            // Read the first packet
            Buffer Buf = readPacket();

            // Get the protocol version
            _protocolVersion = Buf.readByte();

            if (_protocolVersion == -1) {

                try {
                    _mysqlConnection.close();
                } catch (Exception E) {
                }

                throw new SQLException("Server configuration denies access to data source", 
                                       "08001", 0);
            }

            _serverVersion = Buf.readString();

            // Parse the server version into major/minor/subminor
            int point = _serverVersion.indexOf(".");

            if (point != -1) {

                try {

                    int n = Integer.parseInt(_serverVersion.substring(0, point));
                    _serverMajorVersion = n;
                } catch (NumberFormatException NFE1) {
                }

                String Remaining = _serverVersion.substring(point + 1, 
                                                            _serverVersion.length());
                point = Remaining.indexOf(".");

                if (point != -1) {

                    try {

                        int n = Integer.parseInt(Remaining.substring(0, point));
                        _serverMinorVersion = n;
                    } catch (NumberFormatException NFE2) {
                    }

                    Remaining = Remaining.substring(point + 1, 
                                                    Remaining.length());

                    int pos = 0;

                    while (pos < Remaining.length()) {

                        if (Remaining.charAt(pos) < '0' || 
                            Remaining.charAt(pos) > '9') {

                            break;
                        }

                        pos++;
                    }

                    try {

                        int n = Integer.parseInt(Remaining.substring(0, pos));
                        _serverSubMinorVersion = n;
                    } catch (NumberFormatException NFE3) {
                    }
                }
            }

            _colDecimalNeedsBump = versionMeetsMinimum(3, 23, 0);
            _useNewUpdateCounts  = versionMeetsMinimum(3, 22, 5);

            long threadId        = Buf.readLong();
            Seed = Buf.readString();

            if (Driver.trace) {
                Debug.msg(this, "Protocol Version: " + 
                          (int)_protocolVersion);
                Debug.msg(this, "Server Version: " + _serverVersion);
                Debug.msg(this, "Thread ID: " + threadId);
                Debug.msg(this, "Crypt Seed: " + Seed);
            }

            // Client capabilities
            int clientParam = 0;

            if (Buf._pos < Buf._bufLength) {

                int serverCapabilities = Buf.readInt();

                // Should be settable by user
                if ((serverCapabilities & CLIENT_COMPRESS) != 0) {

                    // The following match with ZLIB's
                    // decompress() and compress()
                    //_Deflater = new Deflater();
                    //_Inflater = new Inflater();
                    //clientParam |= CLIENT_COMPRESS;
                }
            }

            // return FOUND rows
            clientParam |= CLIENT_FOUND_ROWS;

            // Authenticate
            if (_protocolVersion > 9) {
                clientParam |= CLIENT_LONG_PASSWORD; // for long passwords
            } else {
                clientParam &= ~CLIENT_LONG_PASSWORD;
            }

            int password_length = 16;
            int user_length = 0;

            if (User != null) {
                user_length = User.length();
            }

            int pack_length = (user_length + password_length) + 6 + 
                              HEADER_LENGTH;

            // Passwords can be 16 chars long
            Buffer Packet = new Buffer(pack_length);
            Packet.writeInt(clientParam);
            Packet.writeLongInt(pack_length);

            // User/Password data
            Packet.writeString(User);

            if (_protocolVersion > 9) {
                Packet.writeString(Util.newCrypt(Password, Seed));
            } else {
                Packet.writeString(Util.oldCrypt(Password, Seed));
            }

            send(Packet);

            // Check for errors
            Buffer B      = readPacket();
            byte   status = B.readByte();

            if (status == (byte)0xff) {

                String Message = "";
                int    errno = 2000;

                if (_protocolVersion > 9) {
                    errno   = B.readInt();
                    Message = B.readString();
                    clearReceive();

                    String XOpen = SQLError.mysqlToXOpen(errno);

                    if (XOpen.equals("S1000")) {
                        throw new java.sql.SQLException("Communication failure during handshake. Is there a server running on " + 
                                                        _host + ":" + _port + 
                                                        "?");
                    } else {
                        throw new java.sql.SQLException(SQLError.get(XOpen) + 
                                                        ": " + Message, XOpen, 
                                                        errno);
                    }
                } else {
                    Message = B.readString();
                    clearReceive();

                    if (Message.indexOf("Access denied") != -1) {
                        throw new java.sql.SQLException(SQLError.get("28000") + 
                                                        ": " + Message, 
                                                        "28000", errno);
                    } else {
                        throw new java.sql.SQLException(SQLError.get("08001") + 
                                                        ": " + Message, 
                                                        "08001", errno);
                    }
                }
            } else if (status == 0x00) {

                if (_serverMajorVersion >= 3 && _serverMinorVersion >= 22 && 
                    _serverSubMinorVersion >= 5) {
                    Packet.newReadLength();
                    Packet.newReadLength();
                } else {
                    Packet.readLength();
                    Packet.readLength();
                }
            } else {
                throw new java.sql.SQLException("Unknown Status code from server", 
                                                "08007", status);
            }

            //if ((clientParam & CLIENT_COMPRESS) != 0) {
            //use_compression = true;
            //}
        } catch (IOException E) {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": " + 
                                            E.getClass().getName(), "08S01", 0);
        }
    }

    /**
     * Log-off of the MySQL server and close the socket.
     */
    final void quit()
             throws IOException
    {

        Buffer Packet = new Buffer(6);
        _packetSequence = -1;
        Packet.writeByte((byte)MysqlDefs.QUIT);
        send(Packet);
        forceClose();
    }

    /**
     * Sets the buffer size to max-buf
     */
    void resetMaxBuf()
    {
        _reusablePacket._maxLength = _connection.getMaxAllowedPacket();
        _sendPacket._maxLength     = _connection.getMaxAllowedPacket();
    }

    /**
     * Send a command to the MySQL server
     * 
     * If data is to be sent with command, it should be put in ExtraData
     *
     * Raw packets can be sent by setting QueryPacket to something other
     * than null.
     */
    final Buffer sendCommand(int command, String ExtraData, Buffer QueryPacket)
                      throws Exception
    {

        Buffer Ret        = null; // results of our query
        byte   statusCode; // query status code

        try {

            //
            // PreparedStatements construct their own packets,
            // for efficiency's sake.
            //
            // If this is a generic query, we need to re-use
            // the sending packet.
            //
            if (QueryPacket == null) {

                int pack_length = HEADER_LENGTH + COMP_HEADER_LENGTH + 1 + 
                                  (ExtraData != null ? ExtraData.length() : 0) + 2;

                if (_sendPacket == null) {
                    _sendPacket = new Buffer(pack_length, 
                                             _connection.getMaxAllowedPacket());
                }

                _packetSequence = -1;
                _sendPacket.clear();

                // Offset different for compression
                if (_useCompression) {
                    _sendPacket._pos += COMP_HEADER_LENGTH;
                }

                _sendPacket.writeByte((byte)command);

                if (command == MysqlDefs.INIT_DB || 
                    command == MysqlDefs.CREATE_DB || 
                    command == MysqlDefs.DROP_DB || 
                    command == MysqlDefs.QUERY) {
                    _sendPacket.writeStringNoNull(ExtraData);
                } else if (command == MysqlDefs.PROCESS_KILL) {

                    long id = new Long(ExtraData).longValue();
                    _sendPacket.writeLong(id);
                } else if (command == MysqlDefs.RELOAD && 
                           _protocolVersion > 9) {
                    Debug.msg(this, "Reload");

                    //Packet.writeByte(reloadParam);
                }

                send(_sendPacket);
            } else {
                _packetSequence = -1;
                send(QueryPacket); // packet passed by PreparedStatement
            }
        } catch (Exception Ex) {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": " + 
                                            Ex.getClass().getName(), "08S01", 
                                            0);
        }

        try {

            // Check return value, if we get a java.io.EOFException,
            // the server has gone away. We'll pass it on up the
            // exception chain and let someone higher up decide
            // what to do (barf, reconnect, etc).
            //Ret = readPacket();
            Ret        = reuseAndReadPacket(_reusablePacket);
            statusCode = Ret.readByte();
        } catch (java.io.EOFException EOFE) {
            throw EOFE;
        }
         catch (Exception FallThru) {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": " + 
                                            FallThru.getClass().getName(), 
                                            "08S01", 0);
        }

        try {

            // Error handling
            if (statusCode == (byte)0xff) {

                String ErrorMessage;
                int    errno = 2000;

                if (_protocolVersion > 9) {
                    errno        = Ret.readInt();
                    ErrorMessage = Ret.readString();
                    clearReceive();

                    String XOpen = SQLError.mysqlToXOpen(errno);
                    throw new java.sql.SQLException(SQLError.get(XOpen) + 
                                                    ": " + ErrorMessage, XOpen, 
                                                    errno);
                } else {
                    ErrorMessage = Ret.readString();
                    clearReceive();

                    if (ErrorMessage.indexOf("Unknown column") != -1) {
                        throw new java.sql.SQLException(SQLError.get("S0022") + 
                                                        ": " + ErrorMessage, 
                                                        "S0022", -1);
                    } else {
                        throw new java.sql.SQLException(SQLError.get("S1000") + 
                                                        ": " + ErrorMessage, 
                                                        "S1000", -1);
                    }
                }
            } else if (statusCode == 0x00) {

                if (command == MysqlDefs.CREATE_DB || 
                    command == MysqlDefs.DROP_DB) {

                    java.sql.SQLWarning NW = new java.sql.SQLWarning(
                                                     "Command=" + command + 
                                                     ": ");

                    if (_warningChain != null) {
                        NW.setNextException(_warningChain);
                    }

                    _warningChain = NW;
                }
            } else if (Ret.isLastDataPacket()) {

                java.sql.SQLWarning NW = new java.sql.SQLWarning(
                                                 "Command=" + command + 
                                                 ": ");

                if (_warningChain != null) {
                    NW.setNextException(_warningChain);
                }

                _warningChain = NW;
            }

            return Ret;
        } catch (IOException E) {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": " + 
                                            E.getClass().getName(), "08S01", 0);
        }
    }

    /**
     * Send a query specified in the String "Query" to the MySQL server.
     *
     * This method uses the specified character encoding to get the
     * bytes from the query string.
     */
    final ResultSet sqlQuery(String Query, int max_rows, String Encoding, 
                             Connection Conn, int resultSetType)
                      throws Exception
    {

        // We don't know exactly how many bytes we're going to get
        // from the query. Since we're dealing with Unicode, the
        // max is 2, so pad it (2 * query) + space for headers
        int pack_length = HEADER_LENGTH + 1 + (Query.length() * 2) + 2;

        if (_sendPacket == null) {
            _sendPacket = new Buffer(pack_length, 
                                     _connection.getMaxAllowedPacket());
        } else {
            _sendPacket.clear();
        }

        _sendPacket.writeByte((byte)MysqlDefs.QUERY);

        if (Encoding != null) {
            _sendPacket.writeStringNoNull(Query, Encoding);
        } else {
            _sendPacket.writeStringNoNull(Query);
        }

        return sqlQueryDirect(_sendPacket, max_rows, Conn, resultSetType);
    }

    final ResultSet sqlQuery(String Query, int max_rows, String Encoding, 
                             int resultSetType)
                      throws Exception
    {

        return sqlQuery(Query, max_rows, Encoding, null, resultSetType);
    }

    final ResultSet sqlQuery(String Query, int max_rows, int resultSetType)
                      throws Exception
    {

        StringBuffer profileMsgBuf  = null; // used if profiling
        long         queryStartTime = 0;

        if (_profileSql) {
            profileMsgBuf  = new StringBuffer();
            queryStartTime = System.currentTimeMillis();
            profileMsgBuf.append("Query\t\"");
            profileMsgBuf.append(Query);
            profileMsgBuf.append("\"\texecution time:\t");
        }

        // Send query command and sql query string
        clearAllReceive();

        Buffer Packet = sendCommand(MysqlDefs.QUERY, Query, null); //, (byte)0);

        if (_profileSql) {

            long executionTime = System.currentTimeMillis() - 
                                 queryStartTime;
            profileMsgBuf.append(executionTime);
            profileMsgBuf.append("\t");
        }

        Packet._pos--;

        long columnCount = Packet.readLength();

        if (Driver.trace) {
            Debug.msg(this, "Column count: " + columnCount);
        }

        if (columnCount == 0) {

            long updateCount = -1;
            long updateID = -1;

            try {

                if (_useNewUpdateCounts) {
                    updateCount = Packet.newReadLength();
                    updateID    = Packet.newReadLength();
                } else {
                    updateCount = (long)Packet.readLength();
                    updateID    = (long)Packet.readLength();
                }
            } catch (Exception E) {
                throw new java.sql.SQLException(SQLError.get("S1000") + 
                                                ": " + 
                                                E.getClass().getName(), 
                                                "S1000", -1);
            }

            if (Driver.trace) {
                Debug.msg(this, "Update Count = " + updateCount);
            }

            if (_profileSql) {
                System.err.println(profileMsgBuf.toString());
            }

            return buildResultSetWithUpdates(updateCount, updateID, null);
        } else {

            long fetchStartTime = 0;

            if (_profileSql) {
                fetchStartTime = System.currentTimeMillis();
            }

            com.mysql.jdbc.ResultSet results = getResultSet(columnCount, 
                                                            max_rows, 
                                                            resultSetType);

            if (_profileSql) {

                long fetchElapsedTime = System.currentTimeMillis() - 
                                        fetchStartTime;
                profileMsgBuf.append("result set fetch time:\t");
                profileMsgBuf.append(fetchElapsedTime);
            }

            return results;
        }
    }

    /**
     * Send a query stored in a packet directly to the server.
     */
    final ResultSet sqlQueryDirect(Buffer QueryPacket, int max_rows, 
                                   Connection Conn, int resultSetType)
                            throws Exception
    {

        long         updateCount    = -1;
        long         updateID       = -1;
        StringBuffer profileMsgBuf  = null; // used if profiling
        long         queryStartTime = 0;

        if (_profileSql) {
            profileMsgBuf  = new StringBuffer();
            queryStartTime = System.currentTimeMillis();

            byte[] queryBuf = QueryPacket._buf;

            // Extract the actual query from the network packet
            String query = new String(queryBuf, 5, (QueryPacket._pos - 5));
            profileMsgBuf.append("Query\t\"");
            profileMsgBuf.append(query);
            profileMsgBuf.append("\"\texecution time:\t");
        }

        // Send query command and sql query string
        clearAllReceive();

        Buffer Packet = sendCommand(MysqlDefs.QUERY, null, QueryPacket);

        if (_profileSql) {

            long executionTime = System.currentTimeMillis() - 
                                 queryStartTime;
            profileMsgBuf.append(executionTime);
            profileMsgBuf.append("\t");
        }

        Packet._pos--;

        long columnCount = Packet.readLength();

        if (Driver.trace) {
            Debug.msg(this, "Column count: " + columnCount);
        }

        if (columnCount == 0) {

            try {

                if (_useNewUpdateCounts) {
                    updateCount = Packet.newReadLength();
                    updateID    = Packet.newReadLength();
                } else {
                    updateCount = (long)Packet.readLength();
                    updateID    = (long)Packet.readLength();
                }
            } catch (Exception E) {
                throw new java.sql.SQLException(SQLError.get("S1000") + 
                                                ": " + 
                                                E.getClass().getName(), 
                                                "S1000", -1);
            }

            if (Driver.trace) {
                Debug.msg(this, "Update Count = " + updateCount);
            }

            if (_profileSql) {
                System.err.println(profileMsgBuf.toString());
            }

            return buildResultSetWithUpdates(updateCount, updateID, Conn);
        } else {

            long fetchStartTime = 0;

            if (_profileSql) {
                fetchStartTime = System.currentTimeMillis();
            }

            com.mysql.jdbc.ResultSet results = getResultSet(columnCount, 
                                                            max_rows, 
                                                            resultSetType);

            if (_profileSql) {

                long fetchElapsedTime = System.currentTimeMillis() - 
                                        fetchStartTime;
                profileMsgBuf.append("result set fetch time:\t");
                profileMsgBuf.append(fetchElapsedTime);
            }

            return results;
        }
    }

    /**
     * Does the version of the MySQL server we are connected to
     * meet the given minimums?
     */
    boolean versionMeetsMinimum(int major, int minor, int subminor)
    {

        if (getServerMajorVersion() >= major) {

            if (getServerMajorVersion() == major) {

                if (getServerMinorVersion() >= minor) {

                    if (getServerMinorVersion() == minor) {

                        if (getServerSubMinorVersion() >= subminor) {

                            return true;
                        } else {

                            return false;
                        }
                    } else {

                        // newer than major.minor
                        return true;
                    }
                } else {

                    // older than major.minor
                    return false;
                }
            } else {

                // newer than major
                return true;
            }
        } else {

            return false;
        }
    }

    /**
     * Clear all data in the InputStream that is being
     * sent by the MySQL server.
     */
    private final void clearAllReceive()
                                throws java.sql.SQLException
    {

        try {

            int len = _mysqlInput.available();

            if (len > 0) {

                Buffer Packet = readPacket();

                if (Packet._buf[0] == (byte)0xff) {
                    clearReceive();

                    return;
                }

                while (!Packet.isLastDataPacket()) {

                    // larger than the socket buffer.
                    Packet = readPacket();

                    if (Packet._buf[0] == (byte)0xff) {

                        break;
                    }
                }
            }

            clearReceive();
        } catch (IOException E) {
            throw new SQLException("Communication link failure: " + 
                                   E.getClass().getName(), "08S01");
        }
    }

    /**
     * Clear waiting data in the InputStream
     */
    private final void clearReceive()
                             throws IOException
    {

        int len = _mysqlInput.available();

        if (len > 0) {

            // _Mysql_Input.skipBytes(len);
        }
    }

    /**
     * Retrieve one row from the MySQL server.
     *
     * Note: this method is not thread-safe, but it is only called
     * from methods that are guarded by synchronizing on this object.
     */
    private final byte[][] nextRow(int columnCount)
                            throws Exception
    {

        // Get the next incoming packet, re-using the packet because
        // all the data we need gets copied out of it.
        Buffer Packet = reuseAndReadPacket(_reusablePacket);

        // check for errors.
        if (Packet.readByte() == (byte)0xff) {

            String ErrorMessage;
            int    errno = 2000;

            if (_protocolVersion > 9) {
                errno        = Packet.readInt();
                ErrorMessage = Packet.readString();

                String XOpen = SQLError.mysqlToXOpen(errno);
                clearReceive();
                throw new java.sql.SQLException(SQLError.get(SQLError.get(
                                                                     XOpen)) + 
                                                ": " + ErrorMessage, XOpen, 
                                                errno);
            } else {
                ErrorMessage = Packet.readString();
                clearReceive();
                throw new java.sql.SQLException(ErrorMessage, 
                                                SQLError.mysqlToXOpen(errno), 
                                                errno);
            }
        }

        // Away we go....
        Packet._pos--;

        int[]    dataStart = new int[columnCount];
        byte[][] Row = new byte[columnCount][];

        if (!Packet.isLastDataPacket()) {

            for (int i = 0; i < columnCount; i++) {

                int p = Packet._pos;
                dataStart[i] = p;
                Packet._pos  = (int)Packet.readLength() + Packet._pos;
            }

            for (int i = 0; i < columnCount; i++) {
                Packet._pos = dataStart[i];
                Row[i]      = Packet.readLenByteArray();

                if (Driver.trace) {

                    if (Row[i] == null) {
                        Debug.msg(this, "Field value: NULL");
                    } else {
                        Debug.msg(this, "Field value: " + Row[i].toString());
                    }
                }
            }

            return Row;
        }

        return null;
    }

    private final void readFully(InputStream in, byte[] b, int off, int len)
                          throws IOException
    {

        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;

        while (n < len) {

            int count = in.read(b, off + n, len - n);

            if (count < 0) {
                throw new EOFException();
            }

            n += count;
        }
    }

    /**
     * Read one packet from the MySQL server
     */
    private final Buffer readPacket()
                             throws IOException
    {

        byte b0;
        byte b1;
        byte b2;
        b0 = (byte)_mysqlInput.read();
        b1 = (byte)_mysqlInput.read();
        b2 = (byte)_mysqlInput.read();

        // If a read failure is detected, close the socket and throw an IOException
        if ((int)b0 == -1 && (int)b1 == -1 && (int)b2 == -1) {
            forceClose();
            throw new IOException("Unexpected end of input stream");
        }

        int  packetLength = (int)((b0 & 0xff) + ((b1 & 0xff) << 8) + 
                            ((b2 & 0xff) << 16));
        byte packetSeq = (byte)_mysqlInput.read();

        // Read data
        byte[] buffer = new byte[packetLength + 1];
        readFully(_mysqlInput, buffer, 0, packetLength);
        buffer[packetLength] = 0;

        return new Buffer(buffer);
    }

    /**
     * Re-use a packet to read from the MySQL server
     */
    private final Buffer reuseAndReadPacket(Buffer Reuse)
                                     throws IOException
    {

        byte b0;
        byte b1;
        byte b2;
        b0 = (byte)_mysqlInput.read();
        b1 = (byte)_mysqlInput.read();
        b2 = (byte)_mysqlInput.read();

        // If a read failure is detected, close the socket and throw an IOException
        if ((int)b0 == -1 && (int)b1 == -1 && (int)b2 == -1) {
            forceClose();
            throw new IOException("Unexpected end of input stream");
        }

        //int packetLength = (int) (ub(b0) + (256 * ub(b1)) + (256 * 256 * ub(b2)));
        int  packetLength = (int)((b0 & 0xff) + ((b1 & 0xff) << 8) + 
                            ((b2 & 0xff) << 16));
        byte packetSeq = (byte)_mysqlInput.read();

        // Set the Buffer to it's original state
        Reuse._pos        = 0;
        Reuse._sendLength = 0;

        // Do we need to re-alloc the byte buffer?
        //
        // Note: We actually check the length of the buffer,
        // rather than buf_length, because buf_length is not
        // necesarily the actual length of the byte array
        // used as the buffer
        if (Reuse._buf.length <= packetLength) {
            Reuse._buf = new byte[packetLength + 1];
        }

        // Set the new length
        Reuse._bufLength = packetLength;

        // Read the data from the server
        readFully(_mysqlInput, Reuse._buf, 0, packetLength);
        Reuse._buf[packetLength] = 0; // Null-termination

        return Reuse;
    }

    /**
     * Send a packet to the MySQL server
     */
    private final void send(Buffer Packet)
                     throws IOException
    {

        int l = Packet._pos;
        _packetSequence++;
        Packet._pos = 0;
        Packet.writeLongInt(l - HEADER_LENGTH);
        Packet.writeByte(_packetSequence);
        _mysqlOutput.write(Packet._buf, 0, l);
        _mysqlOutput.flush();

        int total_header_length = HEADER_LENGTH;
    }
}