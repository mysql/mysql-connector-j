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
 */

/**
 * This class is used by Connection for communicating with the
 * MySQL server.
 *
 * @see java.sql.Connection
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */

package org.gjt.mm.mysql;

import java.io.*;
import java.net.*;
import java.util.*;
import java.sql.*;

import java.sql.SQLException;

import java.util.zip.Deflater;
import java.util.zip.Inflater;

public abstract class MysqlIO {
	/** The connection to the server */

	private Socket _Mysql_Conn = null;

	/** Buffered data from the server */

	//private BufferedInputStream  _Mysql_Buf_Input          = null;

	/** Buffered data to the server */

	//private BufferedOutputStream _Mysql_Buf_Output         = null;

	/** Data from the server */

	//private DataInputStream      _Mysql_Input              = null;

	private InputStream _Mysql_Input = null;

	/** Data to the server */

	//private DataOutputStream     _Mysql_Output             = null;

	private BufferedOutputStream _Mysql_Output = null;

	/** Current open, streaming result set (if any) */

	private ResultSet _PendingResultSet = null;

	static int MAXBUF = 65535;
	static final int HEADER_LENGTH = 4;
	static final int COMP_HEADER_LENGTH = 3;

	private byte _packetSequence = 0;
	private byte _protocol_V = 0;
	private String _Server_V = null;
	private int _server_major_version = 0;
	private int _server_minor_version = 0;
	private int _server_sub_minor_version = 0;

	private int _port = 3306;
	private String _Host = null;

	private Deflater _Deflater = null;
	private Inflater _Inflater = null;

	//
	// Use this when reading in rows to avoid thousands of new()
	// calls, because the byte arrays just get copied out of the
	// packet anyway
	//

	private Buffer _ReusablePacket = null;

	private Buffer _SendPacket = null;

	//
	// For SQL Warnings
	//

	java.sql.SQLWarning _Warning = null;

	private static int CLIENT_LONG_PASSWORD = 1; /* new more secure 
	passwords */

	private static int CLIENT_FOUND_ROWS = 2;
	/* Found instead of 
	affected rows */

	private static int CLIENT_LONG_FLAG = 4; /* Get all column flags */

	private static int CLIENT_CONNECT_WITH_DB = 8;
	/* One can specify db 
	on connect */

	private static int CLIENT_NO_SCHEMA = 16; /* Don't allow 
	db.table.column */

	private static int CLIENT_COMPRESS = 32; /* Can use compression 
	protcol */

	private static int CLIENT_ODBC = 64; /* Odbc client */

	private static int CLIENT_LOCAL_FILES = 128; /* Can use LOAD DATA 
	LOCAL */

	private static int CLIENT_IGNORE_SPACE = 256; /* Ignore spaces 
	before '(' */

	private boolean use_compression = false;

	private org.gjt.mm.mysql.Connection _Conn;

	/**
	 * Constructor:  Connect to the MySQL server and setup
	 * a stream connection.
	 *
	 * @param host the hostname to connect to
	 * @param port the port number that the server is listening on
	 * @exception IOException if an IOException occurs during connect.
	 */

	public MysqlIO(String Host, int port, org.gjt.mm.mysql.Connection Conn)
		throws IOException, java.sql.SQLException {
		_Conn = Conn;
		_ReusablePacket =
			new Buffer(_Conn.getNetBufferLength(), _Conn.getMaxAllowedPacket());

		_port = port;
		_Host = Host;

		_Mysql_Conn = new Socket(_Host, _port);

		try {
			_Mysql_Conn.setTcpNoDelay(true);
		}
		catch (Exception ex) {
			/* Ignore */
		}

		_Mysql_Input = _Mysql_Conn.getInputStream();
		_Mysql_Output = new BufferedOutputStream(_Mysql_Conn.getOutputStream());

		//_Mysql_Input  = new DataInputStream(_Mysql_Buf_Input);
		//_Mysql_Output = new DataOutputStream(_Mysql_Buf_Output);
	}

	/**
	 * Initialize communications with the MySQL server.
	 *
	 * Handles logging on, and handling initial connection errors.
	 */

	void init(String User, String Password) throws java.sql.SQLException {
		String Seed;

		try {
			// Read the first packet
			Buffer Buf = readPacket();

			// Get the protocol version
			_protocol_V = Buf.readByte();

			if (_protocol_V == -1) {
				try {
					_Mysql_Conn.close();
				}
				catch (Exception E) {
				}

				throw new SQLException(
					"Server configuration denies access to data source",
					"08001",
					0);
			}
			_Server_V = Buf.readString();

			// Parse the server version into major/minor/subminor 

			int point = _Server_V.indexOf(".");

			if (point != -1) {
				try {
					int n = Integer.parseInt(_Server_V.substring(0, point));
					_server_major_version = n;
				}
				catch (NumberFormatException NFE1) {
				}

				String Remaining = _Server_V.substring(point + 1, _Server_V.length());

				point = Remaining.indexOf(".");

				if (point != -1) {
					try {
						int n = Integer.parseInt(Remaining.substring(0, point));
						_server_minor_version = n;
					}
					catch (NumberFormatException NFE2) {
					}

					Remaining = Remaining.substring(point + 1, Remaining.length());

					int pos = 0;

					while (pos < Remaining.length()) {
						if (Remaining.charAt(pos) < '0' || Remaining.charAt(pos) > '9') {
							break;
						}
						pos++;
					}

					try {
						int n = Integer.parseInt(Remaining.substring(0, pos));
						_server_sub_minor_version = n;
					}
					catch (NumberFormatException NFE3) {
					}
				}
			}

			long threadId = Buf.readLong();

			Seed = Buf.readString();

			if (Driver.trace) {
				Debug.msg(this, "Protocol Version: " + (int) _protocol_V);
				Debug.msg(this, "Server Version: " + _Server_V);
				Debug.msg(this, "Thread ID: " + threadId);
				Debug.msg(this, "Crypt Seed: " + Seed);
			}

			// Client capabilities
			int clientParam = 0;

			if (Buf.pos < Buf.buf_length) {
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

			if (_protocol_V > 9) {
				clientParam |= CLIENT_LONG_PASSWORD; // for long passwords
			}
			else {
				clientParam &= ~CLIENT_LONG_PASSWORD;
			}

			int password_length = 16;
			int user_length = 0;

			if (User != null) {
				user_length = User.length();
			}

			int pack_length = (user_length + password_length) + 6 + HEADER_LENGTH;
			// Passwords can be 16 chars long

			Buffer Packet = new Buffer(pack_length);
			Packet.writeInt(clientParam);
			Packet.writeLongInt(pack_length);

			// User/Password data
			Packet.writeString(User);

			if (_protocol_V > 9) {
				Packet.writeString(Util.newCrypt(Password, Seed));
			}
			else {
				Packet.writeString(Util.oldCrypt(Password, Seed));
			}

			send(Packet);

			// Check for errors
			Buffer B = readPacket();
			byte status = B.readByte();

			if (status == (byte) 0xff) {
				String Message = "";
				int errno = 2000;

				if (_protocol_V > 9) {
					errno = B.readInt();
					Message = B.readString();
					clearReceive();
					String XOpen = SQLError.mysqlToXOpen(errno);

					if (XOpen.equals("S1000")) {
						throw new java.sql.SQLException(
							"Communication failure during handshake. Is there a server running on "
								+ _Host
								+ ":"
								+ _port
								+ "?");
					}
					else {
						throw new java.sql.SQLException(
							SQLError.get(XOpen) + ": " + Message,
							XOpen,
							errno);
					}
				}
				else {
					Message = B.readString();
					clearReceive();
					if (Message.indexOf("Access denied") != -1) {
						throw new java.sql.SQLException(
							SQLError.get("28000") + ": " + Message,
							"28000",
							errno);
					}
					else {
						throw new java.sql.SQLException(
							SQLError.get("08001") + ": " + Message,
							"08001",
							errno);
					}
				}
			}
			else
				if (status == 0x00) {

					if (_server_major_version >= 3
						&& _server_minor_version >= 22
						&& _server_sub_minor_version >= 5) {
						Packet.newReadLength();
						Packet.newReadLength();
					}
					else {
						Packet.readLength();
						Packet.readLength();
					}
				}
				else {
					throw new java.sql.SQLException(
						"Unknown Status code from server",
						"08007",
						status);
				}

			//if ((clientParam & CLIENT_COMPRESS) != 0) {
			//use_compression = true;
			//}
		}
		catch (IOException E) {
			throw new java.sql.SQLException(
				SQLError.get("08S01") + ": " + E.getClass().getName(),
				"08S01",
				0);
		}
	}

	/**
	 * Sets the buffer size to max-buf
	 */

	void resetMaxBuf() {
		_ReusablePacket.max_length = _Conn.getMaxAllowedPacket();
		_SendPacket.max_length = _Conn.getMaxAllowedPacket();
	}

	/**
	 * Get the version string of the server we are talking to
	 */

	String getServerVersion() {
		return _Server_V;
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
		throws Exception {
		Buffer Ret = null; // results of our query
		byte statusCode; // query status code

		try {

			//
			// PreparedStatements construct their own packets,
			// for efficiency's sake.
			//
			// If this is a generic query, we need to re-use
			// the sending packet.
			//

			if (QueryPacket == null) {
				int pack_length =
					HEADER_LENGTH
						+ COMP_HEADER_LENGTH
						+ 1
						+ (ExtraData != null ? ExtraData.length() : 0)
						+ 2;

				if (_SendPacket == null) {
					_SendPacket = new Buffer(pack_length, _Conn.getMaxAllowedPacket());
				}

				_packetSequence = -1;
				_SendPacket.clear();

				// Offset different for compression
				if (use_compression) {
					_SendPacket.pos += COMP_HEADER_LENGTH;
				}

				_SendPacket.writeByte((byte) command);

				if (command == MysqlDefs.INIT_DB
					|| command == MysqlDefs.CREATE_DB
					|| command == MysqlDefs.DROP_DB
					|| command == MysqlDefs.QUERY) {

					_SendPacket.writeStringNoNull(ExtraData);
				}
				else
					if (command == MysqlDefs.PROCESS_KILL) {
						long id = new Long(ExtraData).longValue();
						_SendPacket.writeLong(id);
					}
					else
						if (command == MysqlDefs.RELOAD && _protocol_V > 9) {
							Debug.msg(this, "Reload");
							//Packet.writeByte(reloadParam);
						}

				send(_SendPacket);
			}
			else {
				_packetSequence = -1;

				send(QueryPacket); // packet passed by PreparedStatement
			}

		}
		catch (Exception Ex) {

			throw new java.sql.SQLException(
				SQLError.get("08S01") + ": " + Ex.getClass().getName(),
				"08S01",
				0);
		}

		try {

			// Check return value, if we get a java.io.EOFException,
			// the server has gone away. We'll pass it on up the 
			// exception chain and let someone higher up decide
			// what to do (barf, reconnect, etc).

			Ret = readPacket();

			statusCode = Ret.readByte();
		}
		catch (java.io.EOFException EOFE) {
			throw EOFE;
		}
		catch (Exception FallThru) {
			throw new java.sql.SQLException(
				SQLError.get("08S01") + ": " + FallThru.getClass().getName(),
				"08S01",
				0);
		}

		try {

			// Error handling
			if (statusCode == (byte) 0xff) {
				String ErrorMessage;
				int errno = 2000;

				if (_protocol_V > 9) {
					errno = Ret.readInt();
					ErrorMessage = Ret.readString();
					clearReceive();
					String XOpen = SQLError.mysqlToXOpen(errno);

					throw new java.sql.SQLException(
						SQLError.get(XOpen) + ": " + ErrorMessage,
						XOpen,
						errno);
				}
				else {
					ErrorMessage = Ret.readString();
					clearReceive();

					if (ErrorMessage.indexOf("Unknown column") != -1) {
						throw new java.sql.SQLException(
							SQLError.get("S0022") + ": " + ErrorMessage,
							"S0022",
							-1);
					}
					else {
						throw new java.sql.SQLException(
							SQLError.get("S1000") + ": " + ErrorMessage,
							"S1000",
							-1);
					}
				}
			}
			else
				if (statusCode == 0x00) {
					if (command == MysqlDefs.CREATE_DB || command == MysqlDefs.DROP_DB) {
						java.sql.SQLWarning NW = new java.sql.SQLWarning("Command=" + command + ": ");
						if (_Warning != null)
							NW.setNextException(_Warning);
						_Warning = NW;
					}
				}
				else
					if (Ret.isLastDataPacket()) {
						java.sql.SQLWarning NW = new java.sql.SQLWarning("Command=" + command + ": ");
						if (_Warning != null)
							NW.setNextException(_Warning);
						_Warning = NW;
					}
			return Ret;
		}
		catch (IOException E) {
			throw new java.sql.SQLException(
				SQLError.get("08S01") + ": " + E.getClass().getName(),
				"08S01",
				0);
		}
	}

	/**
	 * Send a query stored in a packet directly to the server.
	 */

	final ResultSet sqlQueryDirect(
		Buffer QueryPacket,
		int max_rows,
		Connection Conn)
		throws Exception {
		long updateCount = -1;
		long updateID = -1;

		// Send query command and sql query string
		clearAllReceive();

		Buffer Packet = sendCommand(MysqlDefs.QUERY, null, QueryPacket);
		Packet.pos--;

		long columnCount = Packet.readLength();

		if (Driver.trace) {
			Debug.msg(this, "Column count: " + columnCount);
		}

		if (columnCount == 0) {
			try {

				if (versionMeetsMinimum(3, 22, 5)) {
					updateCount = Packet.newReadLength();
					updateID = Packet.newReadLength();

					//
					// Everyone else expects what MySQL calls
					// "rows matched" for an update count
					//

					//String ExtraInfo = Packet.readString();

					//if (ExtraInfo.length() > 0) {
					//long newUpdateCount = getMatchedRows(ExtraInfo);

					//if (newUpdateCount > -1) {
					//    updateCount = newUpdateCount;
					//}
					// }
				}
				else {
					updateCount = (long) Packet.readLength();
					updateID = (long) Packet.readLength();
				}
			}
			catch (Exception E) {
				throw new java.sql.SQLException(
					SQLError.get("S1000") + ": " + E.getClass().getName(),
					"S1000",
					-1);
			}

			if (Driver.trace) {
				Debug.msg(this, "Update Count = " + updateCount);
			}

			return buildResultSetWithUpdates(updateCount, updateID, Conn);
		}
		else {
			return getResultSet(columnCount, max_rows);
		}
	}

	/**
	 * Send a query specified in the String "Query" to the MySQL server.
	 *
	 * This method uses the specified character encoding to get the
	 * bytes from the query string.
	 */

	final ResultSet sqlQuery(
		String Query,
		int max_rows,
		String Encoding,
		Connection Conn)
		throws Exception {
		// We don't know exactly how many bytes we're going to get 
		// from the query. Since we're dealing with Unicode, the 
		// max is 2, so pad it (2 * query) + space for headers 

		int pack_length = HEADER_LENGTH + 1 + (Query.length() * 2) + 2;

		if (_SendPacket == null) {
			_SendPacket = new Buffer(pack_length, _Conn.getMaxAllowedPacket());
		}
		else {
			_SendPacket.clear();
		}

		_SendPacket.writeByte((byte) MysqlDefs.QUERY);

		if (Encoding != null) {
			_SendPacket.writeStringNoNull(Query, Encoding);
		}
		else {
			_SendPacket.writeStringNoNull(Query);
		}

		return sqlQueryDirect(_SendPacket, max_rows, Conn);
	}

	final ResultSet sqlQuery(String Query, int max_rows, String Encoding)
		throws Exception {
		return sqlQuery(Query, max_rows, Encoding, null);
	}

	final ResultSet sqlQuery(String Query, int max_rows) throws Exception {
		long updateCount = -1;
		long updateID = -1;

		// Send query command and sql query string
		clearAllReceive();
		Buffer Packet = sendCommand(MysqlDefs.QUERY, Query, null); //, (byte)0);
		Packet.pos--;

		long columnCount = Packet.readLength();

		if (Driver.trace) {
			Debug.msg(this, "Column count: " + columnCount);
		}

		if (columnCount == 0) {
			try {

				if (versionMeetsMinimum(3, 22, 5)) {
					updateCount = Packet.newReadLength();
					updateID = Packet.newReadLength();

					//
					// Everyone else expects what MySQL calls
					// "rows matched" for an update count
					//

					String ExtraInfo = Packet.readString();

					if (ExtraInfo.length() > 0) {
						long newUpdateCount = getMatchedRows(ExtraInfo);

						if (newUpdateCount > -1) {
							updateCount = newUpdateCount;
						}
					}
				}
				else {
					updateCount = (long) Packet.readLength();
					updateID = (long) Packet.readLength();
				}
			}
			catch (Exception E) {
				throw new java.sql.SQLException(
					SQLError.get("S1000") + ": " + E.getClass().getName(),
					"S1000",
					-1);
			}

			if (Driver.trace) {
				Debug.msg(this, "Update Count = " + updateCount);
			}

			return buildResultSetWithUpdates(updateCount, updateID, null);
		}
		else {
			return getResultSet(columnCount, max_rows);
		}
	}

	/**
	 * Build a result set. Delegates to buildResultSetWithRows() to
	 * build a JDBC-version-specific ResultSet, given rows as byte
	 * data, and field information.
	 */

	protected ResultSet getResultSet(long columnCount, int max_rows)
		throws Exception {
		Buffer Packet; // The packet from the server

		Field[] Fields = new Field[(int) columnCount];

		// Read in the column information
		for (int i = 0; i < columnCount; i++) {
			Packet = readPacket();
			String TableName = Packet.readLenString();
			String ColName = Packet.readLenString();
			int colLength = Packet.readnBytes();
			int colType = Packet.readnBytes();
			Packet.readByte(); // We know it's currently 2
			short colFlag = (short) (Packet.readByte() & 0xff);
			int colDecimals = (Packet.readByte() & 0xff);

			if (versionMeetsMinimum(3, 23, 0)) {
				colDecimals++;
			}

			Fields[i] =
				new Field(TableName, ColName, colLength, colType, colFlag, colDecimals);
		}
		Packet = readPacket();

		Vector Rows = new Vector();
		// Now read the data
		byte[][] Row = nextRow((int) columnCount);
		int row_count = 0;

		if (Row != null) {
			Rows.addElement(Row);
			row_count = 1;
		}

		while (Row != null && row_count < max_rows) {
			Row = nextRow((int) columnCount);
			if (Row != null) {
				Rows.addElement(Row);
				row_count++;
			}
			else {
				if (Driver.trace) {
					Debug.msg(this, "* NULL Row *");
				}
			}
		}
		if (Driver.trace) {
			Debug.msg(this, "* Fetched " + Rows.size() + " rows from server *");
		}

		return buildResultSetWithRows(Fields, Rows, null);
	}

	/**
	 * Retrieve one row from the MySQL server.
	 *
	 * Note: this method is not thread-safe, but it is only called
	 * from methods that are guarded by synchronizing on this object.
	 */

	private final byte[][] nextRow(int columnCount) throws Exception {
		// Get the next incoming packet, re-using the packet because
		// all the data we need gets copied out of it.

		Buffer Packet = reuseAndReadPacket(_ReusablePacket);

		// check for errors.

		if (Packet.readByte() == (byte) 0xff) {
			String ErrorMessage;
			int errno = 2000;

			if (_protocol_V > 9) {
				errno = Packet.readInt();
				ErrorMessage = Packet.readString();
				String XOpen = SQLError.mysqlToXOpen(errno);
				clearReceive();
				throw new java.sql.SQLException(
					SQLError.get(SQLError.get(XOpen)) + ": " + ErrorMessage,
					XOpen,
					errno);
			}
			else {
				ErrorMessage = Packet.readString();
				clearReceive();
				throw new java.sql.SQLException(
					ErrorMessage,
					SQLError.mysqlToXOpen(errno),
					errno);
			}
		}

		// Away we go....

		Packet.pos--;

		int[] dataStart = new int[columnCount];
		byte[][] Row = new byte[columnCount][];

		if (!Packet.isLastDataPacket()) {

			for (int i = 0; i < columnCount; i++) {
				int p = Packet.pos;
				dataStart[i] = p;
				Packet.pos = (int) Packet.readLength() + Packet.pos;
			}
			for (int i = 0; i < columnCount; i++) {
				Packet.pos = dataStart[i];
				Row[i] = Packet.readLenByteArray();

				if (Driver.trace) {
					if (Row[i] == null) {
						Debug.msg(this, "Field value: NULL");
					}
					else {
						Debug.msg(this, "Field value: " + Row[i].toString());
					}
				}
			}
			return Row;
		}
		return null;
	}

	/**
	 * Log-off of the MySQL server and close the socket.
	 */

	final void quit() throws IOException {
		Buffer Packet = new Buffer(6);
		_packetSequence = -1;
		Packet.writeByte((byte) MysqlDefs.QUIT);
		send(Packet);
		forceClose();
	}

	protected final void forceClose() throws IOException {
		_Mysql_Conn.close();
	}

	/**
	 * Get the major version of the MySQL server we are
	 * talking to.
	 */

	final int getServerMajorVersion() {
		return _server_major_version;
	}

	/**
	 * Get the minor version of the MySQL server we are
	 * talking to.
	 */

	final int getServerMinorVersion() {
		return _server_minor_version;
	}

	/**
	 * Get the sub-minor version of the MySQL server we are
	 * talking to.
	 */

	final int getServerSubMinorVersion() {
		return _server_sub_minor_version;
	}

	/**
	 * Read one packet from the MySQL server
	 */

	private final Buffer readPacket() throws IOException {

		byte b0, b1, b2;

		b0 = (byte) _Mysql_Input.read();
		b1 = (byte) _Mysql_Input.read();
		b2 = (byte) _Mysql_Input.read();

		// If a read failure is detected, close the socket and throw an IOException 

		if ((int) b0 == -1 && (int) b1 == -1 && (int) b2 == -1) {
			forceClose();

			throw new IOException("Unexpected end of input stream");
		}

		int packetLength = (int) (ub(b0) + (256 * ub(b1)) + (256 * 256 * ub(b2)));

		byte packetSeq = (byte) _Mysql_Input.read();

		// Read data
		byte[] buffer = new byte[packetLength + 1];

		readFully(_Mysql_Input, buffer, 0, packetLength);

		buffer[packetLength] = 0;

		return new Buffer(buffer);
	}

	/**
	 * Re-use a packet to read from the MySQL server
	 */

	private final Buffer reuseAndReadPacket(Buffer Reuse) throws IOException {

		byte b0, b1, b2;

		b0 = (byte) _Mysql_Input.read();
		b1 = (byte) _Mysql_Input.read();
		b2 = (byte) _Mysql_Input.read();

		// If a read failure is detected, close the socket and throw an IOException 
		if ((int) b0 == -1 && (int) b1 == -1 && (int) b2 == -1) {
			forceClose();

			throw new IOException("Unexpected end of input stream");
		}

		int packetLength = (int) (ub(b0) + (256 * ub(b1)) + (256 * 256 * ub(b2)));

		byte packetSeq = (byte) _Mysql_Input.read();

		// Set the Buffer to it's original state

		Reuse.pos = 0;
		Reuse.send_length = 0;

		// Do we need to re-alloc the byte buffer?
		//
		// Note: We actually check the length of the buffer,
		// rather than buf_length, because buf_length is not
		// necesarily the actual length of the byte array
		// used as the buffer

		if (Reuse.buf.length <= packetLength) {
			Reuse.buf = new byte[packetLength + 1];
		}

		// Set the new length

		Reuse.buf_length = packetLength;

		// Read the data from the server

		readFully(_Mysql_Input, Reuse.buf, 0, packetLength);

		Reuse.buf[packetLength] = 0; // Null-termination

		return Reuse;
	}

	/**
	 * Send a packet to the MySQL server
	 */

	private final void send(Buffer Packet) throws IOException {
		int l = Packet.pos;
		_packetSequence++;
		Packet.pos = 0;
		Packet.writeLongInt(l - HEADER_LENGTH);
		Packet.writeByte(_packetSequence);
		_Mysql_Output.write(Packet.buf, 0, l);
		_Mysql_Output.flush();

		int total_header_length = HEADER_LENGTH;
	}

	/**
	 * Clear waiting data in the InputStream
	 */

	private final void clearReceive() throws IOException {

		int len = _Mysql_Input.available();

		if (len > 0) {
			// _Mysql_Input.skipBytes(len);
		}
	}

	/**
	 * Clear all data in the InputStream that is being
	 * sent by the MySQL server.
	 */

	private final void clearAllReceive() throws java.sql.SQLException {
		try {
			int len = _Mysql_Input.available();

			if (len > 0) {
				Buffer Packet = readPacket();
				if (Packet.buf[0] == (byte) 0xff) {
					clearReceive();
					return;
				}
				while (!Packet.isLastDataPacket()) {
					// larger than the socket buffer.
					Packet = readPacket();
					if (Packet.buf[0] == (byte) 0xff)
						break;
				}
			}
			clearReceive();
		}
		catch (IOException E) {
			throw new SQLException(
				"Communication link failure: " + E.getClass().getName(),
				"08S01");
		}
	}

	protected abstract ResultSet buildResultSetWithRows(
		Field[] Fields,
		Vector Rows,
		Connection Conn);

	protected abstract ResultSet buildResultSetWithUpdates(
		long updateCount,
		long updateID,
		Connection Conn);

	static int getMaxBuf() {
		return MAXBUF;
	}

	private final static int ub(byte b) {
		return b < 0 ? (int) (256 + b) : b;
	}

	/**
	 * Retrieves matched rows count from EXTRA_INFO in MySQL protocol
	 * (for servers 3.22.5 or newer)
	 *
	 * Most JDBC clients expect this value, rather than the actual number
	 * of rows updated.
	 *
	 * This code assumes that this value is the first number in the
	 * EXTRA_INFO string.
	 */

	private final static long getMatchedRows(String Info) {
		int info_length = Info.length();

		StringBuffer MatchedRowCount = new StringBuffer();
		boolean seen_first_digit = false;

		for (int i = 0; i < info_length; i++) {
			char c = Info.charAt(i);

			if (Character.isDigit(c)) {
				if (!seen_first_digit) {
					seen_first_digit = true;
				}
				else {
					break;
				}

				MatchedRowCount.append(c);
			}
		}

		if (MatchedRowCount.length() > 0) {
			try {
				return Long.parseLong(MatchedRowCount.toString());
			}
			catch (NumberFormatException NFE) { /* Do Nothing */
			}
		}

		return -1;
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
						}
						else {
							return false;
						}
					}
					else {
						// newer than major.minor
						return true;
					}
				}
				else {
					// older than major.minor
					return false;
				}
			}
			else {
				// newer than major
				return true;
			}
		}
		else {
			return false;
		}
	}

	private final void readFully(InputStream in, byte b[], int off, int len)
		throws IOException {
		if (len < 0)
			throw new IndexOutOfBoundsException();

		int n = 0;

		while (n < len) {
			int count = in.read(b, off + n, len - n);
			if (count < 0)
				throw new EOFException();
			n += count;
		}
	}
}
