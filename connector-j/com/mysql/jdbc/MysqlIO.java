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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import java.net.Socket;

import java.sql.SQLException;

import java.util.ArrayList;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


public class MysqlIO
{

    //~ Instance/static variables .............................................

    static final int COMP_HEADER_LENGTH = 3;
    static final int HEADER_LENGTH = 4;
    static int MAXBUF = 65535;
    private static int CLIENT_COMPRESS = 32; /* Can use compression 
    protcol */
    private static int CLIENT_CONNECT_WITH_DB = 8;
    private static int CLIENT_FOUND_ROWS = 2;
    private static int CLIENT_IGNORE_SPACE = 256; /* Ignore spaces 
    before '(' */
    private static int CLIENT_LOCAL_FILES = 128; /* Can use LOAD DATA 
    LOCAL */

    /* Found instead of 
    affected rows */
    private static int CLIENT_LONG_FLAG = 4; /* Get all column flags */
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
    java.sql.SQLWarning warningChain = null;
    private boolean colDecimalNeedsBump = false; // do we need to increment the colDecimal flag?
    private com.mysql.jdbc.Connection connection;
    private Deflater deflater = null;
    private String host = null;
    private Inflater inflater = null;

    /** The connection to the server */
    private Socket mysqlConnection = null;

    /** Buffered data from the server */

    //private BufferedInputStream  _Mysql_Buf_Input          = null;

    /** Buffered data to the server */

    //private BufferedOutputStream _Mysql_Buf_Output         = null;

    /** Data from the server */

    //private DataInputStream      _Mysql_Input              = null;
    private InputStream mysqlInput = null;

    /** Data to the server */

    //private DataOutputStream     _Mysql_Output             = null;
    private BufferedOutputStream mysqlOutput = null;
    private byte packetSequence = 0;

    /** Current open, streaming result set (if any) */
    private ResultSet pendingResultSet = null;
    private int port = 3306;
    private boolean profileSql = false;
    private byte protocolVersion = 0;

    //
    // Use this when reading in rows to avoid thousands of new()
    // calls, because the byte arrays just get copied out of the
    // packet anyway
    //
    private Buffer reusablePacket = null;
    private Buffer sendPacket = null;
    private int serverMajorVersion = 0;
    private int serverMinorVersion = 0;
    private int serverSubMinorVersion = 0;
    private String serverVersion = null;
    private boolean useCompression = false;
    private boolean useNewUpdateCounts = false; // should we use the new larger update counts?
    private RowData streamingData = null;

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
        this.connection = Conn;
        this.reusablePacket = new Buffer(this.connection.getNetBufferLength(), 
                                     this.connection.getMaxAllowedPacket());
        this.port = port;
        this.host = Host;
        this.mysqlConnection = new Socket(this.host, this.port);

        try
        {
            this.mysqlConnection.setTcpNoDelay(true);
        }
        catch (Exception ex)
        {

            /* Ignore */
        }

        this.mysqlInput = new BufferedInputStream(this.mysqlConnection.getInputStream(), 
                                              16384);
        this.mysqlOutput = new BufferedOutputStream(this.mysqlConnection.getOutputStream());

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
        this.profileSql = flag;
    }

    /**
     * Build a result set. Delegates to buildResultSetWithRows() to
     * build a JDBC-version-specific ResultSet, given rows as byte
     * data, and field information.
     */
    protected ResultSet getResultSet(long columnCount, int max_rows, 
                                     int resultSetType, boolean streamResults)
                              throws Exception
    {

        Buffer Packet; // The packet from the server
        Field[] Fields = new Field[(int)columnCount];

        // Read in the column information
        for (int i = 0; i < columnCount; i++)
        {
            Packet = readPacket();

            String TableName = Packet.readLenString();
            String ColName = Packet.readLenString();
            int colLength = Packet.readnBytes();
            int colType = Packet.readnBytes();
            Packet.readByte(); // We know it's currently 2

            short colFlag = (short)(Packet.readByte() & 0xff);
            int colDecimals = (Packet.readByte() & 0xff);

            if (this.colDecimalNeedsBump)
            {
                colDecimals++;
            }

            Fields[i] = new Field(TableName, ColName, colLength, colType, 
                                  colFlag, colDecimals);
        }

        Packet = readPacket();

        RowData rowData = null;

        if (!streamResults)
        {

            ArrayList rows = new ArrayList();

            // Now read the data
            byte[][] rowBytes = nextRow((int)columnCount);
            int rowCount = 0;

            if (rowBytes != null)
            {
                rows.add(rowBytes);
                rowCount = 1;
            }

            while (rowBytes != null && rowCount < max_rows)
            {
                rowBytes = nextRow((int)columnCount);

                if (rowBytes != null)
                {
                    rows.add(rowBytes);
                    rowCount++;
                }
                else
                {

                    if (Driver.trace)
                    {
                        Debug.msg(this, "* NULL Row *");
                    }
                }
            }

            if (Driver.trace)
            {
                Debug.msg(this, 
                          "* Fetched " + rows.size() + 
                          " rows from server *");
            }

            rowData = new RowDataStatic(rows);
        }
        else
        {
            rowData = new RowDataDynamic(this, (int)columnCount);
            this.streamingData = rowData;
        }

        return buildResultSetWithRows(Fields, rowData, null, resultSetType);
    }

    protected final void forceClose()
                             throws IOException
    {
        this.mysqlConnection.close();
    }

    protected com.mysql.jdbc.ResultSet buildResultSetWithRows(com.mysql.jdbc.Field[] fields, 
                                                              RowData rows, 
                                                              com.mysql.jdbc.Connection conn, 
                                                              int resultSetType)
                                                       throws SQLException
    {

        switch (resultSetType)
        {

            case java.sql.ResultSet.CONCUR_READ_ONLY:
                return new com.mysql.jdbc.ResultSet(fields, rows, conn);

            case java.sql.ResultSet.CONCUR_UPDATABLE:
                return new com.mysql.jdbc.UpdatableResultSet(fields, rows, 
                                                             conn);

            default:
                return new com.mysql.jdbc.ResultSet(fields, rows, conn);
        }
    }

    protected com.mysql.jdbc.ResultSet buildResultSetWithUpdates(long updateCount, 
                                                                 long updateID, 
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

        return this.serverMajorVersion;
    }

    /**
     * Get the minor version of the MySQL server we are
     * talking to.
     */
    final int getServerMinorVersion()
    {

        return this.serverMinorVersion;
    }

    /**
     * Get the sub-minor version of the MySQL server we are
     * talking to.
     */
    final int getServerSubMinorVersion()
    {

        return this.serverSubMinorVersion;
    }

    /**
     * Get the version string of the server we are talking to
     */
    String getServerVersion()
    {

        return this.serverVersion;
    }

    /**
     * Initialize communications with the MySQL server.
     *
     * Handles logging on, and handling initial connection errors.
     */
    void init(String user, String password)
       throws java.sql.SQLException
    {

        String seed;

        try
        {

            // Read the first packet
            Buffer buf = readPacket();

            // Get the protocol version
            this.protocolVersion = buf.readByte();

            if (this.protocolVersion == -1)
            {

                try
                {
                    this.mysqlConnection.close();
                }
                catch (Exception e)
                {
                }

                throw new SQLException("Server configuration denies access to data source", 
                                       "08001", 0);
            }

            this.serverVersion = buf.readString();

            // Parse the server version into major/minor/subminor
            int point = this.serverVersion.indexOf(".");

            if (point != -1)
            {

                try
                {

                    int n = Integer.parseInt(this.serverVersion.substring(0, point));
                    this.serverMajorVersion = n;
                }
                catch (NumberFormatException NFE1)
                {
                }

                String remaining = this.serverVersion.substring(point + 1, 
                                                            this.serverVersion.length());
                point = remaining.indexOf(".");

                if (point != -1)
                {

                    try
                    {

                        int n = Integer.parseInt(remaining.substring(0, point));
                        this.serverMinorVersion = n;
                    }
                    catch (NumberFormatException nfe)
                    {
                    }

                    remaining = remaining.substring(point + 1, 
                                                    remaining.length());

                    int pos = 0;

                    while (pos < remaining.length())
                    {

                        if (remaining.charAt(pos) < '0' || 
                            remaining.charAt(pos) > '9')
                        {

                            break;
                        }

                        pos++;
                    }

                    try
                    {

                        int n = Integer.parseInt(remaining.substring(0, pos));
                        this.serverSubMinorVersion = n;
                    }
                    catch (NumberFormatException nfe)
                    {
                    }
                }
            }

            this.colDecimalNeedsBump = versionMeetsMinimum(3, 23, 0);
            this.useNewUpdateCounts = versionMeetsMinimum(3, 22, 5);

            long threadId = buf.readLong();
            seed = buf.readString();

            if (Driver.trace)
            {
                Debug.msg(this, "Protocol Version: " + 
                          (int)this.protocolVersion);
                Debug.msg(this, "Server Version: " + this.serverVersion);
                Debug.msg(this, "Thread ID: " + threadId);
                Debug.msg(this, "Crypt Seed: " + seed);
            }

            // Client capabilities
            int clientParam = 0;

            if (buf.getPosition() < buf.getBufLength())
            {

                int serverCapabilities = buf.readInt();

                // Should be settable by user
                if ((serverCapabilities & CLIENT_COMPRESS) != 0)
                {

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
            if (this.protocolVersion > 9)
            {
                clientParam |= CLIENT_LONG_PASSWORD; // for long passwords
            }
            else
            {
                clientParam &= ~CLIENT_LONG_PASSWORD;
            }

            int passwordLength = 16;
            int userLength = 0;

            if (user != null)
            {
                userLength = user.length();
            }

            int packLength = (userLength + passwordLength) + 6 + 
                              HEADER_LENGTH;

            // Passwords can be 16 chars long
            Buffer packet = new Buffer(packLength);
            packet.writeInt(clientParam);
            packet.writeLongInt(packLength);

            // User/Password data
            packet.writeString(user);

            if (this.protocolVersion > 9)
            {
                packet.writeString(Util.newCrypt(password, seed));
            }
            else
            {
                packet.writeString(Util.oldCrypt(password, seed));
            }

            send(packet);

            // Check for errors
            Buffer b = readPacket();
            byte status = b.readByte();

            if (status == (byte)0xff)
            {

                String message = "";
                int errno = 2000;

                if (this.protocolVersion > 9)
                {
                    errno = b.readInt();
                    message = b.readString();
                    clearReceive();

                    String xOpen = SQLError.mysqlToXOpen(errno);

                    if (xOpen.equals("S1000"))
                    {
                        throw new java.sql.SQLException("Communication failure during handshake. Is there a server running on " + 
                                                        this.host + ":" + this.port + 
                                                        "?");
                    }
                    else
                    {
                        throw new java.sql.SQLException(SQLError.get(xOpen) + 
                                                        ": " + message, xOpen, 
                                                        errno);
                    }
                }
                else
                {
                    message = b.readString();
                    clearReceive();

                    if (message.indexOf("Access denied") != -1)
                    {
                        throw new java.sql.SQLException(SQLError.get("28000") + 
                                                        ": " + message, 
                                                        "28000", errno);
                    }
                    else
                    {
                        throw new java.sql.SQLException(SQLError.get("08001") + 
                                                        ": " + message, 
                                                        "08001", errno);
                    }
                }
            }
            else if (status == 0x00)
            {

                if (this.serverMajorVersion >= 3 && this.serverMinorVersion >= 22 && 
                    this.serverSubMinorVersion >= 5)
                {
                    packet.newReadLength();
                    packet.newReadLength();
                }
                else
                {
                    packet.readLength();
                    packet.readLength();
                }
            }
            else
            {
                throw new java.sql.SQLException("Unknown Status code from server", 
                                                "08007", status);
            }

            //if ((clientParam & CLIENT_COMPRESS) != 0) {
            //use_compression = true;
            //}
        }
        catch (IOException ioEx)
        {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": " + 
                                            ioEx.getClass().getName(), "08S01", 0);
        }
    }

    /**
     * Log-off of the MySQL server and close the socket.
     */
    final void quit()
             throws IOException
    {

        Buffer packet = new Buffer(6);
        this.packetSequence = -1;
        packet.writeByte((byte)MysqlDefs.QUIT);
        send(packet);
        forceClose();
    }

    /**
     * Sets the buffer size to max-buf
     */
    void resetMaxBuf()
    {
        this.reusablePacket.setMaxLength(this.connection.getMaxAllowedPacket());
        this.sendPacket.setMaxLength(this.connection.getMaxAllowedPacket());
    }

    /**
     * Send a command to the MySQL server
     * 
     * If data is to be sent with command, it should be put in ExtraData
     *
     * Raw packets can be sent by setting QueryPacket to something other
     * than null.
     */
    final Buffer sendCommand(int command, String extraData, Buffer queryPacket)
                      throws Exception
    {
        checkForOutstandingStreamingData();

        Buffer resultPacket = null; // results of our query
        byte statusCode; // query status code

        try
        {

            //
            // PreparedStatements construct their own packets,
            // for efficiency's sake.
            //
            // If this is a generic query, we need to re-use
            // the sending packet.
            //
            if (queryPacket == null)
            {

                int packLength = HEADER_LENGTH + COMP_HEADER_LENGTH + 1 + 
                                  (extraData != null ? extraData.length() : 0) + 2;

                if (this.sendPacket == null)
                {
                    this.sendPacket = new Buffer(packLength, 
                                             this.connection.getMaxAllowedPacket());
                }

                this.packetSequence = -1;
                this.sendPacket.clear();

                // Offset different for compression
                if (this.useCompression)
                {
                    this.sendPacket.setPosition(
                            this.sendPacket.getPosition() + COMP_HEADER_LENGTH);
                }

                this.sendPacket.writeByte((byte)command);

                if (command == MysqlDefs.INIT_DB || 
                    command == MysqlDefs.CREATE_DB || 
                    command == MysqlDefs.DROP_DB || 
                    command == MysqlDefs.QUERY)
                {
                    this.sendPacket.writeStringNoNull(extraData);
                }
                else if (command == MysqlDefs.PROCESS_KILL)
                {

                    long id = new Long(extraData).longValue();
                    this.sendPacket.writeLong(id);
                }
                else if (command == MysqlDefs.RELOAD && 
                         this.protocolVersion > 9)
                {
                    Debug.msg(this, "Reload");

                    //Packet.writeByte(reloadParam);
                }

                send(this.sendPacket);
            }
            else
            {
                this.packetSequence = -1;
                send(queryPacket); // packet passed by PreparedStatement
            }
        }
        catch (Exception ex)
        {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": " + 
                                            ex.getClass().getName(), "08S01", 
                                            0);
        }

        try
        {

            // Check return value, if we get a java.io.EOFException,
            // the server has gone away. We'll pass it on up the
            // exception chain and let someone higher up decide
            // what to do (barf, reconnect, etc).
            //Ret = readPacket();
            resultPacket = reuseAndReadPacket(this.reusablePacket);
            statusCode = resultPacket.readByte();
        }
        catch (java.io.EOFException eofe)
        {
            throw eofe;
        }
        catch (Exception fallThru)
        {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": " + 
                                            fallThru.getClass().getName(), 
                                            "08S01", 0);
        }

        try
        {

            // Error handling
            if (statusCode == (byte)0xff)
            {

                String errorMessage;
                int errno = 2000;

                if (this.protocolVersion > 9)
                {
                    errno = resultPacket.readInt();
                    errorMessage = resultPacket.readString();
                    clearReceive();

                    String xOpen = SQLError.mysqlToXOpen(errno);
                    throw new java.sql.SQLException(SQLError.get(xOpen) + 
                                                    ": " + errorMessage, xOpen, 
                                                    errno);
                }
                else
                {
                    errorMessage = resultPacket.readString();
                    clearReceive();

                    if (errorMessage.indexOf("Unknown column") != -1)
                    {
                        throw new java.sql.SQLException(SQLError.get("S0022") + 
                                                        ": " + errorMessage, 
                                                        "S0022", -1);
                    }
                    else
                    {
                        throw new java.sql.SQLException(SQLError.get("S1000") + 
                                                        ": " + errorMessage, 
                                                        "S1000", -1);
                    }
                }
            }
            else if (statusCode == 0x00)
            {

                if (command == MysqlDefs.CREATE_DB || 
                    command == MysqlDefs.DROP_DB)
                {

                    java.sql.SQLWarning newWarning = new java.sql.SQLWarning(
                                                     "Command=" + command + 
                                                     ": ");

                    if (this.warningChain != null)
                    {
                        newWarning.setNextException(this.warningChain);
                    }

                    this.warningChain = newWarning;
                }
            }
            else if (resultPacket.isLastDataPacket())
            {

                java.sql.SQLWarning newWarning = new java.sql.SQLWarning(
                                                 "Command=" + command + 
                                                 ": ");

                if (this.warningChain != null)
                {
                    newWarning.setNextException(this.warningChain);
                }

                this.warningChain = newWarning;
            }

            return resultPacket;
        }
        catch (IOException ioEx)
        {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": " + 
                                            ioEx.getClass().getName(), "08S01", 0);
        }
    }

    /**
     * Send a query specified in the String "Query" to the MySQL server.
     *
     * This method uses the specified character encoding to get the
     * bytes from the query string.
     */
    final ResultSet sqlQuery(String query, int maxRows, String characterEncoding, 
                             Connection conn, int resultSetType, 
                             boolean streamResults)
                      throws Exception
    {

        // We don't know exactly how many bytes we're going to get
        // from the query. Since we're dealing with Unicode, the
        // max is 2, so pad it (2 * query) + space for headers
        int packLength = HEADER_LENGTH + 1 + (query.length() * 2) + 2;

        if (this.sendPacket == null)
        {
            this.sendPacket = new Buffer(packLength, 
                                     this.connection.getMaxAllowedPacket());
        }
        else
        {
            this.sendPacket.clear();
        }

        this.sendPacket.writeByte((byte)MysqlDefs.QUERY);

        if (characterEncoding != null)
        {
            this.sendPacket.writeStringNoNull(query, characterEncoding);
        }
        else
        {
            this.sendPacket.writeStringNoNull(query);
        }

        return sqlQueryDirect(this.sendPacket, maxRows, conn, resultSetType, 
                              streamResults);
    }

    final ResultSet sqlQuery(String query, int maxRows, String encoding, 
                             int resultSetType, boolean streamResults)
                      throws Exception
    {

        return sqlQuery(query, maxRows, encoding, null, resultSetType, 
                        streamResults);
    }

    final ResultSet sqlQuery(String query, int maxRows, int resultSetType, 
                             boolean streamResults)
                      throws Exception
    {

        StringBuffer profileMsgBuf = null; // used if profiling
        long queryStartTime = 0;

        if (this.profileSql)
        {
            profileMsgBuf = new StringBuffer();
            queryStartTime = System.currentTimeMillis();
            profileMsgBuf.append("Query\t\"");
            profileMsgBuf.append(query);
            profileMsgBuf.append("\"\texecution time:\t");
        }

        // Send query command and sql query string
        clearAllReceive();

        Buffer packet = sendCommand(MysqlDefs.QUERY, query, null); //, (byte)0);

        if (this.profileSql)
        {

            long executionTime = System.currentTimeMillis() - 
                                 queryStartTime;
            profileMsgBuf.append(executionTime);
            profileMsgBuf.append("\t");
        }

        packet.setPosition(packet.getPosition() - 1);

        long columnCount = packet.readLength();

        if (Driver.trace)
        {
            Debug.msg(this, "Column count: " + columnCount);
        }

        if (columnCount == 0)
        {

            long updateCount = -1;
            long updateID = -1;

            try
            {

                if (this.useNewUpdateCounts)
                {
                    updateCount = packet.newReadLength();
                    updateID = packet.newReadLength();
                }
                else
                {
                    updateCount = (long)packet.readLength();
                    updateID = (long)packet.readLength();
                }
            }
            catch (Exception ex)
            {
                throw new java.sql.SQLException(SQLError.get("S1000") + 
                                                ": " + 
                                                ex.getClass().getName(), 
                                                "S1000", -1);
            }

            if (Driver.trace)
            {
                Debug.msg(this, "Update Count = " + updateCount);
            }

            if (this.profileSql)
            {
                System.err.println(profileMsgBuf.toString());
            }

            return buildResultSetWithUpdates(updateCount, updateID, null);
        }
        else
        {

            long fetchStartTime = 0;

            if (this.profileSql)
            {
                fetchStartTime = System.currentTimeMillis();
            }

            com.mysql.jdbc.ResultSet results = getResultSet(columnCount, 
                                                            maxRows, 
                                                            resultSetType, 
                                                            streamResults);

            if (this.profileSql)
            {

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
    final ResultSet sqlQueryDirect(Buffer queryPacket, int maxRows, 
                                   Connection conn, int resultSetType, 
                                   boolean streamResults)
                            throws Exception
    {

        long updateCount = -1;
        long updateID = -1;
        StringBuffer profileMsgBuf = null; // used if profiling
        long queryStartTime = 0;

        if (this.profileSql)
        {
            profileMsgBuf = new StringBuffer();
            queryStartTime = System.currentTimeMillis();

            byte[] queryBuf = queryPacket.getByteBuffer();

            // Extract the actual query from the network packet
            String query = new String(queryBuf, 5, 
                                      (queryPacket.getPosition() - 5));
            profileMsgBuf.append("Query\t\"");
            profileMsgBuf.append(query);
            profileMsgBuf.append("\"\texecution time:\t");
        }

        // Send query command and sql query string
        clearAllReceive();

        Buffer resultPacket = sendCommand(MysqlDefs.QUERY, null, queryPacket);

        if (this.profileSql)
        {

            long executionTime = System.currentTimeMillis() - 
                                 queryStartTime;
            profileMsgBuf.append(executionTime);
            profileMsgBuf.append("\t");
        }

        resultPacket.setPosition(resultPacket.getPosition() - 1);

        long columnCount = resultPacket.readLength();

        if (Driver.trace)
        {
            Debug.msg(this, "Column count: " + columnCount);
        }

        if (columnCount == 0)
        {

            try
            {

                if (this.useNewUpdateCounts)
                {
                    updateCount = resultPacket.newReadLength();
                    updateID = resultPacket.newReadLength();
                }
                else
                {
                    updateCount = (long)resultPacket.readLength();
                    updateID = (long)resultPacket.readLength();
                }
            }
            catch (Exception ex)
            {
                throw new java.sql.SQLException(SQLError.get("S1000") + 
                                                ": " + 
                                                ex.getClass().getName(), 
                                                "S1000", -1);
            }

            if (Driver.trace)
            {
                Debug.msg(this, "Update Count = " + updateCount);
            }

            if (this.profileSql)
            {
                System.err.println(profileMsgBuf.toString());
            }

            return buildResultSetWithUpdates(updateCount, updateID, conn);
        }
        else
        {

            long fetchStartTime = 0;

            if (this.profileSql)
            {
                fetchStartTime = System.currentTimeMillis();
            }

            com.mysql.jdbc.ResultSet results = getResultSet(columnCount, 
                                                            maxRows, 
                                                            resultSetType, 
                                                            streamResults);

            if (this.profileSql)
            {

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

        if (getServerMajorVersion() >= major)
        {

            if (getServerMajorVersion() == major)
            {

                if (getServerMinorVersion() >= minor)
                {

                    if (getServerMinorVersion() == minor)
                    {

                        if (getServerSubMinorVersion() >= subminor)
                        {

                            return true;
                        }
                        else
                        {

                            return false;
                        }
                    }
                    else
                    {

                        // newer than major.minor
                        return true;
                    }
                }
                else
                {

                    // older than major.minor
                    return false;
                }
            }
            else
            {

                // newer than major
                return true;
            }
        }
        else
        {

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

        try
        {

            int len = this.mysqlInput.available();

            if (len > 0)
            {

                Buffer packet = readPacket();

                if (packet.getByteBuffer()[0] == (byte)0xff)
                {
                    clearReceive();

                    return;
                }

                while (!packet.isLastDataPacket())
                {

                    // larger than the socket buffer.
                    packet = readPacket();

                    if (packet.getByteBuffer()[0] == (byte)0xff)
                    {

                        break;
                    }
                }
            }

            clearReceive();
        }
        catch (IOException ioEx)
        {
            throw new SQLException("Communication link failure: " + 
                                   ioEx.getClass().getName(), "08S01");
        }
    }

    /**
     * Clear waiting data in the InputStream
     */
    private final void clearReceive()
                             throws IOException
    {

        int len = this.mysqlInput.available();

        if (len > 0)
        {

            // _Mysql_Input.skipBytes(len);
        }
    }

    /**
     * Retrieve one row from the MySQL server.
     *
     * Note: this method is not thread-safe, but it is only called
     * from methods that are guarded by synchronizing on this object.
     */
    final byte[][] nextRow(int columnCount)
                    throws Exception
    {

        // Get the next incoming packet, re-using the packet because
        // all the data we need gets copied out of it.
        Buffer rowPacket = reuseAndReadPacket(this.reusablePacket);

        // check for errors.
        if (rowPacket.readByte() == (byte)0xff)
        {

            String errorMessage;
            int errno = 2000;

            if (this.protocolVersion > 9)
            {
                errno = rowPacket.readInt();
                errorMessage = rowPacket.readString();

                String xOpen = SQLError.mysqlToXOpen(errno);
                clearReceive();
                throw new java.sql.SQLException(SQLError.get(SQLError.get(
                                                                     xOpen)) + 
                                                ": " + errorMessage, xOpen, 
                                                errno);
            }
            else
            {
                errorMessage = rowPacket.readString();
                clearReceive();
                throw new java.sql.SQLException(errorMessage, 
                                                SQLError.mysqlToXOpen(errno), 
                                                errno);
            }
        }

        // Away we go....
        rowPacket.setPosition(rowPacket.getPosition() - 1);

        int[] dataStart = new int[columnCount];
        byte[][] rowData = new byte[columnCount][];

        if (!rowPacket.isLastDataPacket())
        {

            for (int i = 0; i < columnCount; i++)
            {

                int p = rowPacket.getPosition();
                dataStart[i] = p;
                rowPacket.setPosition(
                        (int)rowPacket.readLength() + rowPacket.getPosition());
            }

            for (int i = 0; i < columnCount; i++)
            {
                rowPacket.setPosition(dataStart[i]);
                rowData[i] = rowPacket.readLenByteArray();

                if (Driver.trace)
                {

                    if (rowData[i] == null)
                    {
                        Debug.msg(this, "Field value: NULL");
                    }
                    else
                    {
                        Debug.msg(this, "Field value: " + rowData[i].toString());
                    }
                }
            }

            return rowData;
        }

        return null;
    }

    private final void readFully(InputStream in, byte[] b, int off, int len)
                          throws IOException
    {

        if (len < 0)
        {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;

        while (n < len)
        {

            int count = in.read(b, off + n, len - n);

            if (count < 0)
            {
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
        b0 = (byte)this.mysqlInput.read();
        b1 = (byte)this.mysqlInput.read();
        b2 = (byte)this.mysqlInput.read();

        // If a read failure is detected, close the socket and throw an IOException
        if ((int)b0 == -1 && (int)b1 == -1 && (int)b2 == -1)
        {
            forceClose();
            throw new IOException("Unexpected end of input stream");
        }

        int packetLength = (int)((b0 & 0xff) + ((b1 & 0xff) << 8) + 
                           ((b2 & 0xff) << 16));
        byte packetSeq = (byte)this.mysqlInput.read();

        // Read data
        byte[] buffer = new byte[packetLength + 1];
        readFully(this.mysqlInput, buffer, 0, packetLength);
        buffer[packetLength] = 0;

        return new Buffer(buffer);
    }

    /**
     * Re-use a packet to read from the MySQL server
     */
    private final Buffer reuseAndReadPacket(Buffer reuse)
                                     throws IOException
    {

        byte b0;
        byte b1;
        byte b2;
        b0 = (byte)this.mysqlInput.read();
        b1 = (byte)this.mysqlInput.read();
        b2 = (byte)this.mysqlInput.read();

        // If a read failure is detected, close the socket and throw an IOException
        if ((int)b0 == -1 && (int)b1 == -1 && (int)b2 == -1)
        {
            forceClose();
            throw new IOException("Unexpected end of input stream");
        }

        //int packetLength = (int) (ub(b0) + (256 * ub(b1)) + (256 * 256 * ub(b2)));
        int packetLength = (int)((b0 & 0xff) + ((b1 & 0xff) << 8) + 
                           ((b2 & 0xff) << 16));
        byte packetSeq = (byte)this.mysqlInput.read();

        // Set the Buffer to it's original state
        reuse.setPosition(0);
        reuse.setSendLength(0);

        // Do we need to re-alloc the byte buffer?
        //
        // Note: We actually check the length of the buffer,
        // rather than buf_length, because buf_length is not
        // necesarily the actual length of the byte array
        // used as the buffer
        if (reuse.getByteBuffer().length <= packetLength)
        {
            reuse.setByteBuffer(new byte[packetLength + 1]);
        }

        // Set the new length
        reuse.setBufLength(packetLength);

        // Read the data from the server
        readFully(this.mysqlInput, reuse.getByteBuffer(), 0, packetLength);
        reuse.getByteBuffer()[packetLength] = 0; // Null-termination

        return reuse;
    }

    /**
     * Send a packet to the MySQL server
     */
    private final void send(Buffer packet)
                     throws IOException
    {

        int l = packet.getPosition();
        this.packetSequence++;
        packet.setPosition(0);
        packet.writeLongInt(l - HEADER_LENGTH);
        packet.writeByte(this.packetSequence);
        this.mysqlOutput.write(packet.getByteBuffer(), 0, l);
        this.mysqlOutput.flush();
    }

    void closeStreamer(RowData streamer)
                throws SQLException
    {

        if (this.streamingData == null)
        {
            throw new SQLException("Attempt to close streaming result set " + 
                                   streamer + 
                                   " when no streaming  result set was registered. This is an internal error.");
        }

        if (streamer != this.streamingData)
        {
            throw new SQLException("Attempt to close streaming result set " + 
                                   streamer + " that was not registered." + 
                                   " Only one streaming result set may be open and in use per-connection. Ensure that you have called .close() on " + 
                                   " any active result sets before attempting more queries.");
        }

        this.streamingData = null;
    }

    private void checkForOutstandingStreamingData()
                                           throws SQLException
    {

        if (this.streamingData != null)
        {
            throw new SQLException("Streaming result set " + this.streamingData + 
                                   " is still active." + 
                                   " Only one streaming result set may be open and in use per-connection. Ensure that you have called .close() on " + 
                                   " any active result sets before attempting more queries.");
        }
    }
}