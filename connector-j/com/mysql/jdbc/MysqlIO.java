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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.lang.ref.SoftReference;

import java.net.Socket;

import java.sql.SQLException;
import java.sql.SQLWarning;

import java.util.ArrayList;
import java.util.Properties;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


/**
 * This class is used by Connection for communicating with the
 * MySQL server.
 *
 * @see java.sql.Connection
 * @author Mark Matthews
 * @version $Id$
 */
public class MysqlIO {

    //~ Instance/static variables .............................................
    static final int NULL_LENGTH = ~0;
    static final int COMP_HEADER_LENGTH = 3;
    static final long MAX_THREE_BYTES = 255L * 255L * 255L;
    static final int MIN_COMPRESS_LEN = 50;
    static final int HEADER_LENGTH = 4;
    private static int maxBufferSize = 65535;
    private static final int CLIENT_COMPRESS = 32; /* Can use compression 
    protcol */
    private static final int CLIENT_CONNECT_WITH_DB = 8;
    private static final int CLIENT_FOUND_ROWS = 2;
    private static final int CLIENT_IGNORE_SPACE = 256; /* Ignore spaces 
    before '(' */
    private static final int CLIENT_LOCAL_FILES = 128; /* Can use LOAD DATA 
    LOCAL */

    /* Found instead of 
       affected rows */
    private static final int CLIENT_LONG_FLAG = 4; /* Get all column flags */
    private static final int CLIENT_LONG_PASSWORD = 1; /* new more secure 
    passwords */

    /* One can specify db 
       on connect */
    private static final int CLIENT_NO_SCHEMA = 16; /* Don't allow 
    db.table.column */
    private static final int CLIENT_ODBC = 64; /* Odbc client */
    private static final int CLIENT_PROTOCOL_41 = 16384;
    private static final int CLIENT_INTERACTIVE = 1024;
    private static final int CLIENT_SSL = 2048;

    //
    // For SQL Warnings
    //
    private SQLWarning warningChain = null;
    private boolean colDecimalNeedsBump = false; // do we need to increment the colDecimal flag?
    private com.mysql.jdbc.Connection connection;
    private Deflater deflater = null;
    private String host = null;
    private Inflater inflater = null;
    private int clientParam = 0;

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
    // Used to send large packets to the server versions 4+
    // We use a SoftReference, so that we don't penalize intermittent
    // use of this feature
    //
    private SoftReference splitBufRef;
    
    //
    // Packet used for 'LOAD DATA LOCAL INFILE'
    //
    // We use a SoftReference, so that we don't penalize intermittent
    // use of this feature
    //
    private SoftReference loadFileBufRef;

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
    private String socketFactoryClassName = null;
    private SocketFactory socketFactory = null;
    private boolean isInteractiveClient = false;
    
    /** 
     * Does the server support long column info?
     */
    
    private boolean hasLongColumnInfo = false;
    
    /**
     * Should we use 4.1 protocol extensions?
     */
    
    private boolean use41Extensions = false;

    //~ Constructors ..........................................................

    /**
     * Constructor:  Connect to the MySQL server and setup
     * a stream connection.
     *
     * @param host the hostname to connect to
     * @param port the port number that the server is listening on
     * @param socketFactoryClassName the socket factory to use
     * @param props the Properties from DriverManager.getConnection()
     * @param conn the Connection that is creating us
     * @param socketTimeout the timeout to set for the socket (0 means no timeout)
     * 
     * @throws IOException if an IOException occurs during connect.
     * @throws java.sql.SQLException if a database access error occurs.
     */
    public MysqlIO(String host, int port, String socketFactoryClassName, 
                   Properties props, com.mysql.jdbc.Connection conn, 
                   int socketTimeout)
            throws IOException, java.sql.SQLException {
        this.connection = conn;
        this.reusablePacket = new Buffer(this.connection.getNetBufferLength(), 
                                         this.connection.getMaxAllowedPacket());
        this.port = port;
        this.host = host;
        this.socketFactoryClassName = socketFactoryClassName;
        this.socketFactory = createSocketFactory();
        this.mysqlConnection = socketFactory.connect(this.host, props);

        if (socketTimeout != 0) {

            try {
                this.mysqlConnection.setSoTimeout(socketTimeout);
            } catch (Exception ex) {

                /* Ignore */
            }
        }
        
        this.mysqlConnection = this.socketFactory.beforeHandshake();
        this.mysqlInput = new BufferedInputStream(this.mysqlConnection.getInputStream(), 
                                                  16384);
        this.mysqlOutput = new BufferedOutputStream(this.mysqlConnection.getOutputStream(), 
                                                    16384);
        this.isInteractiveClient = this.connection.isInteractiveClient();
    }

    //~ Methods ...............................................................

    /**
     * Should the driver generate SQL statement profiles?
     * 
     * @param flag should the driver enable profiling?
     */
    public void setProfileSql(boolean flag) {
        this.profileSql = flag;
    }

    /**
     * Build a result set. Delegates to buildResultSetWithRows() to
     * build a JDBC-version-specific ResultSet, given rows as byte
     * data, and field information.
     * 
     * @param columnCount the number of columns in the result set
     * @param maxRows the maximum number of rows to read (-1 means all rows)
     * @param resultSetType the type of result set (CONCUR_UPDATABLE or READ_ONLY)
     * @param streamResults should the result set be read all at once, or streamed?
     * @param catalog the database name in use when the result set was created
     * 
     * @return a result set
     * @throws Exception if a database access error occurs
     */
    protected ResultSet getResultSet(long columnCount, int maxRows, 
                                     int resultSetType, boolean streamResults,
                                     String catalog)
                              throws Exception {

        Buffer packet; // The packet from the server
        Field[] fields = new Field[(int) columnCount];

        // Read in the column information
        for (int i = 0; i < columnCount; i++) {
            packet = readPacket();
            fields[i] = unpackField(packet, false);
        }

        packet = readPacket();

        RowData rowData = null;

        if (!streamResults) {

            ArrayList rows = new ArrayList();

            // Now read the data
            byte[][] rowBytes = nextRow((int) columnCount);
            int rowCount = 0;

            if (rowBytes != null) {
                rows.add(rowBytes);
                rowCount = 1;
            }

            while (rowBytes != null && rowCount < maxRows) {
                rowBytes = nextRow((int) columnCount);

                if (rowBytes != null) {
                    rows.add(rowBytes);
                    rowCount++;
                } else {

                    if (Driver.TRACE) {
                        Debug.msg(this, "* NULL Row *");
                    }
                }
            }

            if (Driver.TRACE) {
                Debug.msg(this, 
                          "* Fetched " + rows.size() + " rows from server *");
            }

            rowData = new RowDataStatic(rows);
        } else {
            rowData = new RowDataDynamic(this, (int) columnCount);
            this.streamingData = rowData;
        }

        return buildResultSetWithRows(catalog, fields, rowData, resultSetType);
    }

    /**
     * Unpacks the Field information from the given
     * packet.
     * 
     * Understands pre 4.1 and post 4.1 server version field
     * packet structures.
     * 
     * @param packet the packet containing the field information
     */
    private final Field unpackField(Buffer packet, boolean extractDefaultValues) {

        if (this.use41Extensions) {

            // we only store the position of the string and
            // materialize only if needed...
            
            int databaseNameStart = packet.getPosition() + 1;
            int databaseNameLength = packet.fastSkipLenString();
            
            int tableNameStart = packet.getPosition() + 1;
            int tableNameLength = packet.fastSkipLenString();

            // orgTableName is never used so skip
            int originalTableNameStart = packet.getPosition() + 1;
            int originalTableNameLength = packet.fastSkipLenString();

            // we only store the position again...
            int nameStart = packet.getPosition() + 1;
            int nameLength = packet.fastSkipLenString();

            // orgColName is not required so skip...
            int originalColumnNameStart = packet.getPosition() + 1;
            int originalColumnNameLength = packet.fastSkipLenString();

            int colLength = packet.readnBytes();
            int colType = packet.readnBytes();

            packet.readByte();

            short colFlag = 0;
            
            if (this.hasLongColumnInfo) {
                colFlag = (short) (packet.readInt());
            } else {
                colFlag = (short) (packet.readByte() & 0xff);
            }
            
            int colDecimals = (packet.readByte() & 0xff);

            //if (INTERNAL_NUM_FIELD(field))
            //   field->flags|= NUM_FLAG;

            int defaultValueStart = -1;
            int defaultValueLength = -1;
            
            if (extractDefaultValues) {
                defaultValueStart = packet.getPosition() + 1;
                defaultValueLength = packet.fastSkipLenString();
            }
            
            //if (default_value && row->data[8])
            //   field->def=strdup_root(alloc,(char*) row->data[8]);
            //else
            //   field->def=0;
            //field->max_length= 0;
            
            Field field = new Field(this.connection, packet.getBufferSource(), 
                                    databaseNameStart, databaseNameLength,
                                    tableNameStart, tableNameLength, 
                                    originalTableNameStart, originalTableNameLength,
                                    nameStart, nameLength, 
                                    originalColumnNameStart, originalColumnNameLength,
                                    colLength, 
                                    colType, 
                                    colFlag, 
                                    colDecimals,
                                    defaultValueStart, defaultValueLength);
            return field;
        } else {

            int tableNameStart = packet.getPosition() + 1;
            int tableNameLength = packet.fastSkipLenString();
            int nameStart = packet.getPosition() + 1;
            int nameLength = packet.fastSkipLenString();
            int colLength = packet.readnBytes();
            int colType = packet.readnBytes();
            packet.readByte(); // We know it's currently 2

            short colFlag = 0;
            
            if (this.hasLongColumnInfo) {
                colFlag = (short) (packet.readInt());
            } else {
                colFlag = (short) (packet.readByte() & 0xff);
            }
            
            int colDecimals = (packet.readByte() & 0xff);

            if (this.colDecimalNeedsBump) {
                colDecimals++;
            }

            Field field = new Field(this.connection, packet.getBufferSource(), nameStart, 
                                    nameLength, tableNameStart, 
                                    tableNameLength, colLength, colType, 
                                    colFlag, colDecimals);

            return field;
        }
    }

    /**
     * Forcibly closes the underlying socket to MySQL.
     * 
     * @throws IOException from the socket.close() method
     */
    protected final void forceClose()
                             throws IOException {
        this.mysqlConnection.close();
    }

    private com.mysql.jdbc.ResultSet buildResultSetWithRows(String catalog, com.mysql.jdbc.Field[] fields, 
                                                            RowData rows,
                                                            int resultSetConcurrency)
                                                     throws SQLException {

        switch (resultSetConcurrency) {

            case java.sql.ResultSet.CONCUR_READ_ONLY:
                return new com.mysql.jdbc.ResultSet(catalog, fields, rows, this.connection);

            case java.sql.ResultSet.CONCUR_UPDATABLE:
                return new com.mysql.jdbc.UpdatableResultSet(catalog, fields, rows, 
                                                             this.connection);

            default:
                return new com.mysql.jdbc.ResultSet(catalog, fields, rows, this.connection);
        }
    }

    private com.mysql.jdbc.ResultSet buildResultSetWithUpdates(Buffer resultPacket) 
        throws SQLException {

        long updateCount = -1;
            long updateID = -1;
        
            try {

                if (this.useNewUpdateCounts) {
                    updateCount = resultPacket.newReadLength();
                    updateID = resultPacket.newReadLength();
                } else {
                    updateCount = (long) resultPacket.readLength();
                    updateID = (long) resultPacket.readLength();
                }
                
                if (this.use41Extensions) {
                    int serverStatus = resultPacket.readInt();
                    int warningCount = resultPacket.readInt();
                }
            } catch (Exception ex) {
                throw new java.sql.SQLException(SQLError.get("S1000") + ": "
                                                + ex.getClass().getName(), 
                                                "S1000", -1);
            }

            if (Driver.TRACE) {
                Debug.msg(this, "Update Count = " + updateCount);
            }


        return new com.mysql.jdbc.ResultSet(updateCount, updateID);
    }

    static int getMaxBuf() {

        return maxBufferSize;
    }

    /**
     * Get the major version of the MySQL server we are
     * talking to.
     */
    final int getServerMajorVersion() {

        return this.serverMajorVersion;
    }

    /**
     * Get the minor version of the MySQL server we are
     * talking to.
     */
    final int getServerMinorVersion() {

        return this.serverMinorVersion;
    }

    /**
     * Get the sub-minor version of the MySQL server we are
     * talking to.
     */
    final int getServerSubMinorVersion() {

        return this.serverSubMinorVersion;
    }

    /**
     * Get the version string of the server we are talking to
     */
    String getServerVersion() {

        return this.serverVersion;
    }

    /**
     * Initialize communications with the MySQL server.
     *
     * Handles logging on, and handling initial connection errors.
     */
    void init(String user, String password)
       throws java.sql.SQLException {

        String seed;

        try {

            // Read the first packet
            Buffer buf = readPacket();

            // Get the protocol version
            this.protocolVersion = buf.readByte();

            if (this.protocolVersion == -1) {

                try {
                    this.mysqlConnection.close();
                } catch (Exception e) {
                    ;
                }

                throw new SQLException("Server configuration denies access to data source", 
                                       "08001", 0);
            }

            this.serverVersion = buf.readString();

            // Parse the server version into major/minor/subminor
            int point = this.serverVersion.indexOf(".");

            if (point != -1) {

                try {

                    int n = Integer.parseInt(this.serverVersion.substring(0, 
                                                                          point));
                    this.serverMajorVersion = n;
                } catch (NumberFormatException NFE1) {
                    ;
                }

                String remaining = this.serverVersion.substring(point + 1, 
                                                                this.serverVersion.length());
                point = remaining.indexOf(".");

                if (point != -1) {

                    try {

                        int n = Integer.parseInt(remaining.substring(0, point));
                        this.serverMinorVersion = n;
                    } catch (NumberFormatException nfe) {
                        ;
                    }

                    remaining = remaining.substring(point + 1, 
                                                    remaining.length());

                    int pos = 0;

                    while (pos < remaining.length()) {

                        if (remaining.charAt(pos) < '0'
                            || remaining.charAt(pos) > '9') {

                            break;
                        }

                        pos++;
                    }

                    try {

                        int n = Integer.parseInt(remaining.substring(0, pos));
                        this.serverSubMinorVersion = n;
                    } catch (NumberFormatException nfe) {
                        ;
                    }
                }
            }

            this.colDecimalNeedsBump = versionMeetsMinimum(3, 23, 0);
            this.colDecimalNeedsBump = !versionMeetsMinimum(3, 23, 15); // guess? Not noted in changelog
            this.useNewUpdateCounts = versionMeetsMinimum(3, 22, 5);

            long threadId = buf.readLong();
            seed = buf.readString();

            if (Driver.TRACE) {
                Debug.msg(this, 
                          "Protocol Version: " + (int) this.protocolVersion);
                Debug.msg(this, "Server Version: " + this.serverVersion);
                Debug.msg(this, "Thread ID: " + threadId);
                Debug.msg(this, "Crypt Seed: " + seed);
            }

            if (buf.getPosition() < buf.getBufLength()) {

                int serverCapabilities = buf.readInt();

                if ((serverCapabilities & CLIENT_COMPRESS) != 0
                    && this.connection.useCompression()) {

                    // The following match with ZLIB's
                    // decompress() and compress()
                    this.deflater = new Deflater();
                    this.inflater = new Inflater();
                    clientParam |= CLIENT_COMPRESS;
                    useCompression = true;
                }

                if ((serverCapabilities & CLIENT_SSL) == 0
                    && this.connection.useSSL()) {
                    this.connection.setUseSSL(false);
                }
                
                if ((serverCapabilities & CLIENT_LONG_FLAG) != 0) {
                    // We understand other column flags, as well
                
                    clientParam |= CLIENT_LONG_FLAG;
                    this.hasLongColumnInfo = true;
                }
            }

            // return FOUND rows
            clientParam |= CLIENT_FOUND_ROWS;
            
            clientParam |= CLIENT_LOCAL_FILES;
            
            if (isInteractiveClient) {
                clientParam |= CLIENT_INTERACTIVE;
            }

            // Authenticate
            if (this.protocolVersion > 9) {
                clientParam |= CLIENT_LONG_PASSWORD; // for long passwords
            } else {
                clientParam &= ~CLIENT_LONG_PASSWORD;
            }

            //
            // 4.1 has some differences in the protocol
            //
            if (versionMeetsMinimum(4, 1, 0)) {
               clientParam |= CLIENT_PROTOCOL_41;
               this.use41Extensions = true;
            }

            int passwordLength = 16;
            int userLength = 0;

            if (user != null) {
                userLength = user.length();
            }

            int packLength = (userLength + passwordLength) + 6 + HEADER_LENGTH;
            Buffer packet = null;

            if (!connection.useSSL()) {

                // Passwords can be 16 chars long
                packet = new Buffer(packLength);
                packet.writeInt(clientParam);
                packet.writeLongInt(packLength);

                // User/Password data
                packet.writeString(user);

                if (this.protocolVersion > 9) {
                    packet.writeString(Util.newCrypt(password, seed));
                } else {
                    packet.writeString(Util.oldCrypt(password, seed));
                }

                send(packet);
            } else {
                clientParam |= CLIENT_SSL;
                packet = new Buffer(packLength);
                packet.writeInt(clientParam);
                send(packet);

                javax.net.ssl.SSLSocketFactory sslFact = (javax.net.ssl.SSLSocketFactory) javax.net.ssl.SSLSocketFactory.getDefault();
                this.mysqlConnection = sslFact.createSocket(
                                               this.mysqlConnection, this.host, 
                                               this.port, true);

                String[] allowedProtocols = ((javax.net.ssl.SSLSocket) this.mysqlConnection).getSupportedProtocols();
                String[] enabledProtocols = ((javax.net.ssl.SSLSocket) this.mysqlConnection).getEnabledProtocols();

                // need to force TLSv1, or else JSSE tries to do a SSLv2 handshake
                // which MySQL doesn't understand
                ((javax.net.ssl.SSLSocket) this.mysqlConnection).setEnabledProtocols(
                        new String[] { "TLSv1" });
                ((javax.net.ssl.SSLSocket) this.mysqlConnection).startHandshake();
                this.mysqlInput = new BufferedInputStream(this.mysqlConnection.getInputStream(), 
                                                          16384);
                this.mysqlOutput = new BufferedOutputStream(this.mysqlConnection.getOutputStream(), 
                                                            16384);
                this.mysqlOutput.flush();
                packet.clear();
                packet.writeInt(clientParam);
                packet.writeLongInt(packLength);

                // User/Password data
                packet.writeString(user);

                if (this.protocolVersion > 9) {
                    packet.writeString(Util.newCrypt(password, seed));
                } else {
                    packet.writeString(Util.oldCrypt(password, seed));
                }

                send(packet);
            }

            // Check for errors
            Buffer b = readPacket();
            byte status = b.readByte();

            if (status == (byte) 0xff) {

                String message = "";
                int errno = 2000;

                if (this.protocolVersion > 9) {
                    errno = b.readInt();
                    message = b.readString();
                    clearReceive();

                    String xOpen = SQLError.mysqlToXOpen(errno);

                    if (xOpen.equals("S1000")) {

                        StringBuffer mesg = new StringBuffer(
                                                    "Communication failure during handshake.");

                        if (connection != null
                            && !connection.useParanoidErrorMessages()) {
                            mesg.append(" Is there a MySQL server running on ");
                            mesg.append(this.host);
                            mesg.append(":");
                            mesg.append(this.port);
                            mesg.append("?");
                        }

                        throw new java.sql.SQLException(mesg.toString());
                    } else {
                        throw new java.sql.SQLException(SQLError.get(xOpen)
                                                        + ": " + message, 
                                                        xOpen, errno);
                    }
                } else {
                    message = b.readString();
                    clearReceive();

                    if (message.indexOf("Access denied") != -1) {
                        throw new java.sql.SQLException(SQLError.get("28000")
                                                        + ": " + message, 
                                                        "28000", errno);
                    } else {
                        throw new java.sql.SQLException(SQLError.get("08001")
                                                        + ": " + message, 
                                                        "08001", errno);
                    }
                }
            } else if (status == 0x00) {

                if (this.serverMajorVersion >= 3
                    && this.serverMinorVersion >= 22
                    && this.serverSubMinorVersion >= 5) {
                    packet.newReadLength();
                    packet.newReadLength();
                } else {
                    packet.readLength();
                    packet.readLength();
                }
            } else {
                throw new java.sql.SQLException("Unknown Status code from server", 
                                                "08007", status);
            }
        } catch (IOException ioEx) {
            StringBuffer message = new StringBuffer(SQLError.get("08S01"));
            message.append(": ");
            message.append(ioEx.getClass().getName());
            message.append(", underlying cause: ");
            message.append(ioEx.getMessage());
            
            if (!this.connection.useParanoidErrorMessages()) {
                message.append(Util.stackTraceToString(ioEx));
            }
            
            throw new java.sql.SQLException(message.toString(), "08S01", 0);
        }
    }

    /**
     * Log-off of the MySQL server and close the socket.
     */
    final void quit()
             throws IOException {

        Buffer packet = new Buffer(6);
        this.packetSequence = -1;
        packet.writeByte((byte) MysqlDefs.QUIT);
        send(packet);
        forceClose();
    }

    /**
     * Sets the buffer size to max-buf
     */
    void resetMaxBuf() {
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
                      throws Exception {
        checkForOutstandingStreamingData();

        Buffer resultPacket = null; // results of our query
        byte statusCode; // query status code

        try {

            //
            // PreparedStatements construct their own packets,
            // for efficiency's sake.
            //
            // If this is a generic query, we need to re-use
            // the sending packet.
            //
            if (queryPacket == null) {

                int packLength = HEADER_LENGTH + COMP_HEADER_LENGTH + 1
                                 + (extraData != null ? extraData.length() : 0) + 2;

                if (this.sendPacket == null) {
                    this.sendPacket = new Buffer(packLength, 
                                                 this.connection.getMaxAllowedPacket());
                }

                this.packetSequence = -1;
                this.sendPacket.clear();

                // Offset different for compression
                if (this.useCompression) {
                    this.sendPacket.setPosition(
                            this.sendPacket.getPosition() + COMP_HEADER_LENGTH);
                }

                this.sendPacket.writeByte((byte) command);

                if (command == MysqlDefs.INIT_DB
                    || command == MysqlDefs.CREATE_DB
                    || command == MysqlDefs.DROP_DB
                    || command == MysqlDefs.QUERY) {
                    this.sendPacket.writeStringNoNull(extraData);
                } else if (command == MysqlDefs.PROCESS_KILL) {

                    long id = new Long(extraData).longValue();
                    this.sendPacket.writeLong(id);
                } else if (command == MysqlDefs.RELOAD
                           && this.protocolVersion > 9) {
                    Debug.msg(this, "Reload");

                    //Packet.writeByte(reloadParam);
                }

                send(this.sendPacket);
            } else {
                this.packetSequence = -1;
                send(queryPacket); // packet passed by PreparedStatement
            }
        } catch (Exception ex) {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": "
                                            + ex.getClass().getName(), "08S01", 
                                            0);
        }

        try {

            // Check return value, if we get a java.io.EOFException,
            // the server has gone away. We'll pass it on up the
            // exception chain and let someone higher up decide
            // what to do (barf, reconnect, etc).
            //Ret = readPacket();
            resultPacket = reuseAndReadPacket(this.reusablePacket);
            statusCode = resultPacket.readByte();
        } catch (java.io.EOFException eofe) {
            throw eofe;
        } catch (Exception fallThru) {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": "
                                            + fallThru.getClass().getName(), 
                                            "08S01", 0);
        }

        try {

            // Error handling
            if (statusCode == (byte) 0xff) {

                String errorMessage;
                int errno = 2000;

                if (this.protocolVersion > 9) {
                    errno = resultPacket.readInt();
                    errorMessage = resultPacket.readString();
                    clearReceive();

                    String xOpen = SQLError.mysqlToXOpen(errno);
                    throw new java.sql.SQLException(SQLError.get(xOpen) + ": "
                                                    + errorMessage, xOpen, 
                                                    errno);
                } else {
                    errorMessage = resultPacket.readString();
                    clearReceive();

                    if (errorMessage.indexOf("Unknown column") != -1) {
                        throw new java.sql.SQLException(SQLError.get("S0022")
                                                        + ": " + errorMessage, 
                                                        "S0022", -1);
                    } else {
                        throw new java.sql.SQLException(SQLError.get("S1000")
                                                        + ": " + errorMessage, 
                                                        "S1000", -1);
                    }
                }
            } else if (statusCode == 0x00) {

                if (command == MysqlDefs.CREATE_DB
                    || command == MysqlDefs.DROP_DB) {

                    java.sql.SQLWarning newWarning = new java.sql.SQLWarning(
                                                             "Command="
                                                             + command + ": ");

                    if (this.warningChain != null) {
                        newWarning.setNextException(this.warningChain);
                    }

                    this.warningChain = newWarning;
                }
            } else if (resultPacket.isLastDataPacket()) {

                java.sql.SQLWarning newWarning = new java.sql.SQLWarning(
                                                         "Command=" + command
                                                         + ": ");

                if (this.warningChain != null) {
                    newWarning.setNextException(this.warningChain);
                }

                this.warningChain = newWarning;
            }

            return resultPacket;
        } catch (IOException ioEx) {
            throw new java.sql.SQLException(SQLError.get("08S01") + ": "
                                            + ioEx.getClass().getName(), 
                                            "08S01", 0);
        }
    }

    /**
     * Prepares a query for execution (MySQL server 4.1+)
     */
    final ResultSet prepareQuery(String query, String characterEncoding)
                          throws Exception {

        // We don't know exactly how many bytes we're going to get
        // from the query. Since we're dealing with Unicode, the
        // max is 2, so pad it (2 * query) + space for headers
        int packLength = HEADER_LENGTH + 1 + (query.length() * 2) + 2;

        if (this.sendPacket == null) {
            this.sendPacket = new Buffer(packLength, 
                                         this.connection.getMaxAllowedPacket());
        } else {
            this.sendPacket.clear();
        }

        this.sendPacket.writeByte((byte) MysqlDefs.COM_PREPARE);

        if (characterEncoding != null) {
            this.sendPacket.writeStringNoNull(query, characterEncoding);
        } else {
            this.sendPacket.writeStringNoNull(query);
        }

        Buffer resultPacket = sendCommand(MysqlDefs.COM_PREPARE, null, 
                                          this.sendPacket);
        resultPacket.setPosition(resultPacket.getPosition() - 1);

        long columnCount = resultPacket.readLength();

        if (Driver.TRACE) {
            Debug.msg(this, "Column count: " + columnCount);
        }

        if (columnCount != 0) {

            com.mysql.jdbc.ResultSet results = getResultSet(columnCount, -1, 
                                                            java.sql.ResultSet.CONCUR_READ_ONLY, 
                                                            false, null);

            return results;
        } else {
            throw new SQLException("No handle or metadata returned for statement prepare", 
                                   "S1000");
        }
    }

    /**
     * Send a query stored in a packet directly to the server.
     */
    final ResultSet executePreparedQuery(Buffer paramPacket, int maxRows, 
                                         Connection conn, int resultSetType, 
                                         boolean streamResults)
                                  throws Exception {

        long updateCount = -1;
        long updateID = -1;
        StringBuffer profileMsgBuf = null; // used if profiling
        long queryStartTime = 0;

        if (this.profileSql) {
            profileMsgBuf = new StringBuffer();
            queryStartTime = System.currentTimeMillis();
            profileMsgBuf.append("\"\texecution time:\t");
        }

        // Send query command and sql query string
        
        Buffer resultPacket = sendCommand(MysqlDefs.COM_EXECUTE, null, 
                                          paramPacket);

        if (this.profileSql) {

            long executionTime = System.currentTimeMillis() - queryStartTime;
            profileMsgBuf.append(executionTime);
            profileMsgBuf.append("\t");
        }

        resultPacket.setPosition(resultPacket.getPosition() - 1);

        long columnCount = resultPacket.readLength();

        if (Driver.TRACE) {
            Debug.msg(this, "Column count: " + columnCount);
        }

        if (columnCount == 0) {

            if (this.profileSql) {
                System.err.println(profileMsgBuf.toString());
            }

            return buildResultSetWithUpdates(resultPacket);
        } else {

            long fetchStartTime = 0;

            if (this.profileSql) {
                fetchStartTime = System.currentTimeMillis();
            }

            com.mysql.jdbc.ResultSet results = getResultSet(columnCount, 
                                                            maxRows, 
                                                            resultSetType, 
                                                            streamResults, null);

            if (this.profileSql) {

                long fetchElapsedTime = System.currentTimeMillis()
                                        - fetchStartTime;
                profileMsgBuf.append("result set fetch time:\t");
                profileMsgBuf.append(fetchElapsedTime);
                
                System.err.println(profileMsgBuf.toString()); 
            }

            return results;
        }
    }

    /**
     * Send a query specified in the String "Query" to the MySQL server.
     *
     * This method uses the specified character encoding to get the
     * bytes from the query string.
     */
    final ResultSet sqlQuery(String query, int maxRows, 
                             String characterEncoding, Connection conn, 
                             int resultSetType, boolean streamResults, String catalog)
                      throws Exception {

        // We don't know exactly how many bytes we're going to get
        // from the query. Since we're dealing with Unicode, the
        // max is 2, so pad it (2 * query) + space for headers
        int packLength = HEADER_LENGTH + 1 + (query.length() * 2) + 2;

        if (this.sendPacket == null) {
            this.sendPacket = new Buffer(packLength, 
                                         this.connection.getMaxAllowedPacket());
        } else {
            this.sendPacket.clear();
        }

        this.sendPacket.writeByte((byte) MysqlDefs.QUERY);

        if (characterEncoding != null) {
            this.sendPacket.writeStringNoNull(query, characterEncoding);
        } else {
            this.sendPacket.writeStringNoNull(query);
        }

        return sqlQueryDirect(this.sendPacket, maxRows, conn, resultSetType, 
                              streamResults, catalog);
    }

    /**
     * Send a query stored in a packet directly to the server.
     */
    final ResultSet sqlQueryDirect(Buffer queryPacket, int maxRows, 
                                   Connection conn, int resultSetType, 
                                   boolean streamResults, String catalog)
                            throws Exception {

        
        StringBuffer profileMsgBuf = null; // used if profiling
        long queryStartTime = 0;

        if (this.profileSql) {
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
       
        Buffer resultPacket = sendCommand(MysqlDefs.QUERY, null, queryPacket);

        if (this.profileSql) {

            long executionTime = System.currentTimeMillis() - queryStartTime;
            profileMsgBuf.append(executionTime);
            profileMsgBuf.append("\t");
        }

        resultPacket.setPosition(resultPacket.getPosition() - 1);

        long columnCount = resultPacket.readFieldLength();

        if (Driver.TRACE) {
            Debug.msg(this, "Column count: " + columnCount);
        }

        if (columnCount == 0) {
            
            if (this.profileSql) {
                System.err.println(profileMsgBuf.toString());
            }

            return buildResultSetWithUpdates(resultPacket);
        } else if (columnCount == Buffer.NULL_LENGTH) {
            System.out.println("LOAD DATA LOCAL");
            
            String fileName = resultPacket.readString();
            
            System.out.println("Filename: " + fileName);
            
            return sendFileToServer(fileName);
            
            
        } else {

            long fetchStartTime = 0;

            if (this.profileSql) {
                fetchStartTime = System.currentTimeMillis();
            }

            com.mysql.jdbc.ResultSet results = getResultSet(columnCount, 
                                                            maxRows, 
                                                            resultSetType, 
                                                            streamResults,
                                                            catalog);

            if (this.profileSql) {

                long fetchElapsedTime = System.currentTimeMillis()
                                        - fetchStartTime;
                profileMsgBuf.append("result set fetch time:\t");
                profileMsgBuf.append(fetchElapsedTime);
                System.err.println(profileMsgBuf.toString());
            }

            return results;
        }
    }

    /** 
     * Returns the host this IO is connected to
     */
    String getHost() {
        return this.host;
    }
    
    /**
     * Does the version of the MySQL server we are connected to
     * meet the given minimums?
     */
    boolean versionMeetsMinimum(int major, int minor, int subminor) {

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
                                throws java.sql.SQLException {

        try {

            int len = this.mysqlInput.available();

            if (len > 0) {

                Buffer packet = readPacket();

                if (packet.getByteBuffer()[0] == (byte) 0xff) {
                    clearReceive();

                    return;
                }

                while (!packet.isLastDataPacket()) {

                    // larger than the socket buffer.
                    packet = readPacket();

                    if (packet.getByteBuffer()[0] == (byte) 0xff) {

                        break;
                    }
                }
            }

            clearReceive();
        } catch (IOException ioEx) {
            throw new SQLException("Communication link failure: "
                                   + ioEx.getClass().getName(), "08S01");
        }
    }

    /**
     * Clear waiting data in the InputStream
     */
    private final void clearReceive()
                             throws IOException {

        int len = this.mysqlInput.available();

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
    final byte[][] nextRow(int columnCount)
                    throws Exception {

        // Get the next incoming packet, re-using the packet because
        // all the data we need gets copied out of it.
        Buffer rowPacket = reuseAndReadPacket(this.reusablePacket);
        
        // check for errors.
        if (rowPacket.readByte() == (byte) 0xff) {

            String errorMessage;
            int errno = 2000;

            if (this.protocolVersion > 9) {
                errno = rowPacket.readInt();
                errorMessage = rowPacket.readString();

                String xOpen = SQLError.mysqlToXOpen(errno);
                clearReceive();
                throw new java.sql.SQLException(SQLError.get(SQLError.get(
                                                                     xOpen))
                                                + ": " + errorMessage, xOpen, 
                                                errno);
            } else {
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
        int offset = rowPacket.wasMultiPacket() ? HEADER_LENGTH + 1 : 0;

        if (!rowPacket.isLastDataPacket()) {

            for (int i = 0; i < columnCount; i++) {

                int p = rowPacket.getPosition();
                dataStart[i] = p;
                rowPacket.setPosition(
                        (int) rowPacket.readLength() + rowPacket.getPosition());
            }

            for (int i = 0; i < columnCount; i++) {
                rowPacket.setPosition(dataStart[i]);
                rowData[i] = rowPacket.readLenByteArray(offset);

                if (Driver.TRACE) {

                    if (rowData[i] == null) {
                        Debug.msg(this, "Field value: NULL");
                    } else {
                        Debug.msg(this, 
                                  "Field value: " + rowData[i].toString());
                    }
                }
            }

            return rowData;
        }

        return null;
    }

    private final void readFully(InputStream in, byte[] b, int off, int len)
                          throws IOException {

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
                             throws IOException {

        int packetLength = mysqlInput.read() + (mysqlInput.read() << 8)
                           + (mysqlInput.read() << 16);

        // -1 for all values through above assembly sequence
        if (packetLength == -65793) {
            forceClose();
            throw new IOException("Unexpected end of input stream");
        }
        
        // we don't look at packet sequence in this case
        //this.mysqlInput.skip(1);
        int b = this.mysqlInput.read();
        
        // Read data
        byte[] buffer = new byte[packetLength + 1];
        readFully(this.mysqlInput, buffer, 0, packetLength);
        buffer[packetLength] = 0;

        Buffer packet = new Buffer(buffer);
 
        return packet;
    }

    /**
     * Re-use a packet to read from the MySQL server
     */
    private final Buffer reuseAndReadPacket(Buffer reuse)
                                     throws IOException, SQLException {

        if (reuse.wasMultiPacket()) {

            // Check available, as we ate the "last data packet" packet
            int bytesAvail = mysqlInput.available();

            if (bytesAvail <= 2) {
                reuse.setBufLength(1);
                reuse.setPosition(0);
                reuse.writeByte((byte) 254);
                mysqlInput.skip(bytesAvail);
            }

            reuse.setWasMultiPacket(false);

            return reuse;
        }

        reuse.setWasMultiPacket(false);

        int packetLength = mysqlInput.read() + (mysqlInput.read() << 8)
                           + (mysqlInput.read() << 16);
                           
      

        // -1 for all values through above assembly sequence
        if (packetLength == -65793) {
            forceClose();
            throw new IOException("Unexpected end of input stream");
        }

        byte multiPacketSeq = (byte) this.mysqlInput.read();
        
        // Set the Buffer to it's original state
        reuse.setPosition(0);
        reuse.setSendLength(0);

        // Do we need to re-alloc the byte buffer?
        //
        // Note: We actually check the length of the buffer,
        // rather than buf_length, because buf_length is not
        // necesarily the actual length of the byte array
        // used as the buffer
        if (reuse.getByteBuffer().length <= packetLength) {
            reuse.setByteBuffer(new byte[packetLength + 1]);
        }

        // Set the new length
        reuse.setBufLength(packetLength);

        // Read the data from the server
        readFully(this.mysqlInput, reuse.getByteBuffer(), 0, packetLength);

        boolean isMultiPacket = false;

        if (packetLength == MAX_THREE_BYTES) {
            reuse.setPosition((int) MAX_THREE_BYTES);

            int packetEndPoint = packetLength;

            // it's multi-packet
            isMultiPacket = true;
            packetLength = mysqlInput.read() + (mysqlInput.read() << 8)
                           + (mysqlInput.read() << 16);

            // -1 for all values through above assembly sequence
            if (packetLength == -65793) {
                forceClose();
                throw new IOException("Unexpected end of input stream");
            }

            Buffer multiPacket = new Buffer(packetLength);
            boolean firstMultiPkt = true;

            for (; ;) {

                if (!firstMultiPkt) {
                    packetLength = mysqlInput.read()
                                   + (mysqlInput.read() << 8)
                                   + (mysqlInput.read() << 16);

                    // -1 for all values through above assembly sequence
                    if (packetLength == -65793) {
                        forceClose();
                        throw new IOException("Unexpected end of input stream");
                    }
                } else {
                    firstMultiPkt = false;
                }

                if (packetLength == 1) {

                    break; // end of multipacket sequence
                }

                byte newPacketSeq = (byte) this.mysqlInput.read();

                if (newPacketSeq != (multiPacketSeq + 1)) {
                    throw new IOException("Packets received out of order");
                }

                multiPacketSeq = newPacketSeq;

                // Set the Buffer to it's original state
                multiPacket.setPosition(0);
                multiPacket.setSendLength(0);

                // Set the new length
                multiPacket.setBufLength(packetLength);

                // Read the data from the server
                byte[] byteBuf = multiPacket.getByteBuffer();
                int lengthToWrite = packetLength;
                readFully(this.mysqlInput, byteBuf, 0, packetLength);
                reuse.writeBytesNoNull(byteBuf, 0, lengthToWrite);
                packetEndPoint += lengthToWrite;
            }

            reuse.writeByte((byte) 0);
            reuse.setPosition(0);
            reuse.setWasMultiPacket(true);
        }

        if (!isMultiPacket) {
            reuse.getByteBuffer()[packetLength] = 0; // Null-termination
        }

        return reuse;
    }

    /**
     * Reads and sends a file to the server for LOAD DATA LOCAL INFILE
     * 
     * @param fileName the file name to send.
     */
    private final ResultSet sendFileToServer(String fileName) 
        throws IOException, SQLException {
        Buffer filePacket = (loadFileBufRef == null)
                                  ? null : (Buffer) (loadFileBufRef.get());
            
        int packetLength = alignPacketSize(this.connection.getMaxAllowedPacket() - 16, 4096);
                              
        if (filePacket == null) {
            filePacket = new Buffer((int) (packetLength + HEADER_LENGTH));
            loadFileBufRef = new SoftReference(filePacket);
        }
        
        
        filePacket.clear();
        send(filePacket, 0);
        
        byte[] fileBuf = new byte[packetLength];
        
        BufferedInputStream fileIn = null;
        
        try {
            fileIn = new BufferedInputStream(new FileInputStream(fileName));
        
            int bytesRead = 0;
        
            while ((bytesRead = fileIn.read(fileBuf)) != -1) {
                filePacket.clear();
                filePacket.writeBytesNoNull(fileBuf, 0, bytesRead);
                send(filePacket);
            }
        
            
        } catch (IOException ioEx) {
            StringBuffer messageBuf = new StringBuffer("Unable to open file ");
            
            if (!this.connection.useParanoidErrorMessages()) {
                messageBuf.append("'");
                if (fileName != null) {
                    messageBuf.append(fileName);
                }
                messageBuf.append("'");
            }
            
            messageBuf.append("for 'LOAD DATA LOCAL INFILE' command.");
            
            if (!this.connection.useParanoidErrorMessages()) {
                messageBuf.append("Due to underlying IOException: ");
                messageBuf.append(Util.stackTraceToString(ioEx));
            }
            
            throw new SQLException(messageBuf.toString(), "S1009");
        } finally {
            
            if (fileIn != null) {
                try {
                    fileIn.close();
                } catch (Exception ex) {
                    // ignore
                }
                
                fileIn = null;
            } else {
                // file open failed, but server needs one packet
                filePacket.clear();
                send(filePacket);
            }
        }
        
        // send empty packet to mark EOF
        filePacket.clear();
        send (filePacket);
            
        Buffer resultPacket = checkErrorPacket();
        
       
        return buildResultSetWithUpdates(resultPacket);
    }

    /** 
     * Checks for errors in the reply packet, and if none, returns the
     * reply packet, ready for reading
     */
	private Buffer checkErrorPacket() throws EOFException, SQLException {
		int statusCode = 0;
		Buffer resultPacket = null;
		
		try {
		
		    // Check return value, if we get a java.io.EOFException,
		    // the server has gone away. We'll pass it on up the
		    // exception chain and let someone higher up decide
		    // what to do (barf, reconnect, etc).
		    //Ret = readPacket();
		    resultPacket = reuseAndReadPacket(this.reusablePacket);
		    statusCode = resultPacket.readByte();
		} catch (java.io.EOFException eofe) {
		    throw eofe;
		} catch (Exception fallThru) {
		    throw new java.sql.SQLException(SQLError.get("08S01") + ": "
		                                    + fallThru.getClass().getName(), 
		                                    "08S01", 0);
		}
		                                    
		try {
		
		    // Error handling
		    if (statusCode == (byte) 0xff) {
		
		        String errorMessage;
		        int errno = 2000;
		
		        if (this.protocolVersion > 9) {
		            errno = resultPacket.readInt();
		            errorMessage = resultPacket.readString();
		            clearReceive();
		
		            String xOpen = SQLError.mysqlToXOpen(errno);
		            throw new java.sql.SQLException(SQLError.get(xOpen) + ": "
		                                            + errorMessage, xOpen, 
		                                            errno);
		        } else {
		            errorMessage = resultPacket.readString();
		            clearReceive();
		
		            if (errorMessage.indexOf("Unknown column") != -1) {
		                throw new java.sql.SQLException(SQLError.get("S0022")
		                                                + ": " + errorMessage, 
		                                                "S0022", -1);
		            } else {
		                throw new java.sql.SQLException(SQLError.get("S1000")
		                                                + ": " + errorMessage, 
		                                                "S1000", -1);
		            }
		        }
            }
		  
		} catch (IOException ioEx) {
		    throw new java.sql.SQLException(SQLError.get("08S01") + ": "
		                                    + ioEx.getClass().getName(), 
		                                    "08S01", 0);
		}
		return resultPacket;
	}
    
    /**
     * Send a packet to the MySQL server
     */
    private final void send(Buffer packet)
                     throws IOException {
   
        int l = packet.getPosition();
        send(packet, l);
    }

    private final void send(Buffer packet, int packetLen)
                     throws IOException {

        
        
        if (serverMajorVersion >= 4 && packetLen >= MAX_THREE_BYTES) {
            sendSplitPackets(packet);
        } else {

            int headerLength = HEADER_LENGTH;
            this.packetSequence++;
            packet.setPosition(0);

            if (useCompression) {
                packet.writeLongInt(packetLen - headerLength);
                packet.writeByte(this.packetSequence);

                if (packetLen < MIN_COMPRESS_LEN) {

                    // Don't compress small packets
                    
                    headerLength += COMP_HEADER_LENGTH;

                    Buffer compressedPacket = new Buffer(packetLen + headerLength);
                    
                    compressedPacket.setPosition(0);
                    compressedPacket.writeLongInt(0);
                    compressedPacket.writeLongInt(packetLen - HEADER_LENGTH);
                    compressedPacket.writeByte(this.packetSequence);
                    
                    // FIXME: Do this quicker :)
                    for (int i = HEADER_LENGTH + 1; i < packetLen; i++) {
                        compressedPacket.writeByte(packet.getByteBuffer()[i]);
                    }
                        
               

                    this.mysqlOutput.write(compressedPacket.getByteBuffer(), 0, 
                                       compressedPacket.getPosition());
                    this.mysqlOutput.flush();
                    
                    return;
                } else {
                    headerLength += COMP_HEADER_LENGTH;

                    Buffer compressedPacket = new Buffer(packetLen + headerLength);
                    byte[] compressedBytes = compressedPacket.getByteBuffer();
                    
                    
                    deflater.setInput(packet.getByteBuffer(), HEADER_LENGTH, 
                                      packetLen);
                    int compLen = deflater.deflate(compressedBytes, headerLength, 
                                               packetLen);
                    compressedPacket.setPosition(0);
                    compressedPacket.writeLongInt(compLen);
                    compressedPacket.writeLongInt(packetLen - headerLength);
                    compressedPacket.writeByte(this.packetSequence);
               

                    this.mysqlOutput.write(compressedPacket.getByteBuffer(), 0, 
                                       compLen);
                    this.mysqlOutput.flush();
                }
            } else {
                packet.writeLongInt(packetLen - headerLength);
                packet.writeByte(this.packetSequence);
                this.mysqlOutput.write(packet.getByteBuffer(), 0, packetLen);
                this.mysqlOutput.flush();
            }
        }
    }
    
    

    /** 
     * Sends a large packet to the server as a series of smaller packets
     */
    private final void sendSplitPackets(Buffer packet)
                                 throws IOException {

        //
        // Big packets are handled by splitting them in packets of MAX_THREE_BYTES
        // length. The last packet is always a packet that is < MAX_THREE_BYTES.
        // (The last packet may even have a length of 0)
        //
        //
        // NB: Guarded by execSQL. If the driver changes architecture, this
        // will need to be synchronized in some other way
        //
        Buffer headerPacket = (splitBufRef == null)
                                  ? null : (Buffer) (splitBufRef.get());

        //
        // Store this packet in a soft reference...It can be re-used if not GC'd (so clients
        // that use it frequently won't have to re-alloc the 16M buffer), but we don't
        // penalize infrequent users of large packets by keeping 16M allocated all of the time
        //
        if (headerPacket == null) {
            headerPacket = new Buffer((int) (MAX_THREE_BYTES + HEADER_LENGTH));
            splitBufRef = new SoftReference(headerPacket);
        }

        int len = packet.getPosition();
        int splitSize = (int) MAX_THREE_BYTES;
        int originalPacketPos = HEADER_LENGTH;
        byte[] origPacketBytes = packet.getByteBuffer();
        byte[] headerPacketBytes = headerPacket.getByteBuffer();

        while (len >= MAX_THREE_BYTES) {
            headerPacket.setPosition(0);
            headerPacket.writeLongInt(splitSize);
            this.packetSequence++;
            headerPacket.writeByte(this.packetSequence);
            System.arraycopy(origPacketBytes, originalPacketPos, 
                             headerPacketBytes, 4, splitSize);
            this.mysqlOutput.write(headerPacketBytes, 0, 
                                   splitSize + HEADER_LENGTH);
            this.mysqlOutput.flush();
            originalPacketPos += splitSize;
            len -= splitSize;
        }

        //
        // Write last packet
        //
        headerPacket.clear();
        headerPacket.setPosition(0);
        headerPacket.writeLongInt(len - HEADER_LENGTH);
        this.packetSequence++;
        headerPacket.writeByte(this.packetSequence);

        if (len != 0) {
            System.arraycopy(origPacketBytes, originalPacketPos, 
                             headerPacketBytes, 4, len - HEADER_LENGTH);
        }

        this.mysqlOutput.write(headerPacket.getByteBuffer(), 0, len);
        this.mysqlOutput.flush();
    }

    void closeStreamer(RowData streamer)
                throws SQLException {

        if (this.streamingData == null) {
            throw new SQLException("Attempt to close streaming result set "
                                   + streamer
                                   + " when no streaming  result set was registered. This is an internal error.");
        }

        if (streamer != this.streamingData) {
            throw new SQLException("Attempt to close streaming result set "
                                   + streamer + " that was not registered."
                                   + " Only one streaming result set may be open and in use per-connection. Ensure that you have called .close() on "
                                   + " any active result sets before attempting more queries.");
        }

        this.streamingData = null;
    }
    
    /**
     * Returns the packet used for sending data (used by PreparedStatement)
     * 
     * Guarded by external synchronization on a mutex.
     * 
     * @return A packet to send data with
     */
    
    Buffer getSendPacket() {
        if (this.sendPacket == null) {
                    this.sendPacket = new Buffer(this.connection.getNetBufferLength(), 
                                                 this.connection.getMaxAllowedPacket());
        }
        
        return this.sendPacket;
    }
        
       
    private void checkForOutstandingStreamingData()
                                           throws SQLException {

        if (this.streamingData != null) {
            throw new SQLException("Streaming result set "
                                   + this.streamingData + " is still active."
                                   + " Only one streaming result set may be open and in use per-connection. Ensure that you have called .close() on "
                                   + " any active result sets before attempting more queries.");
        }
    }

    private SocketFactory createSocketFactory()
                                       throws SQLException {

        try {

            if (socketFactoryClassName == null) {
                throw new SQLException("No name specified for socket factory", 
                                       "08001");
            }

            return (SocketFactory) (Class.forName(socketFactoryClassName).newInstance());
        } catch (Exception ex) {
            throw new SQLException("Could not create socket factory '"
                                   + socketFactoryClassName
                                   + "' due to underlying exception: "
                                   + ex.toString(), "08001");
        }
    }

    /**
     * Does the server send back extra column info?
     * 
     * @return true if so
     */
	public boolean hasLongColumnInfo() {
		return this.hasLongColumnInfo;
	}
    
    private int alignPacketSize(int a, int l) {
        return    (((a) + (l) - 1) & ~((l) - 1));
    }
}