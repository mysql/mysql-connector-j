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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

import java.math.BigDecimal;

import java.net.URL;

import java.sql.Array;
import java.sql.Clob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;


/**
 * A SQL Statement is pre-compiled and stored in a PreparedStatement object.
 * This object can then be used to efficiently execute this statement multiple
 * times.
 *
 * <p><B>Note:</B> The setXXX methods for setting IN parameter values must
 * specify types that are compatible with the defined SQL type of the input
 * parameter.  For instance, if the IN parameter has SQL type Integer, then
 * setInt should be used.
 *
 * <p>If arbitrary parameter type conversions are required, then the setObject
 * method should be used with a target SQL type.
 *
 * @see java.sql.ResultSet
 * @see java.sql.PreparedStatement
 * @author Mark Matthews
 * @version $Id$
 */
public class PreparedStatement extends com.mysql.jdbc.Statement
    implements java.sql.PreparedStatement {
    private static final SimpleDateFormat TSDF = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");
    private Buffer sendPacket = null;
    private java.sql.DatabaseMetaData dbmd = null;
    
    private String originalSql = null;
    private boolean[] isNull = null;
    private boolean[] isStream = null;
    private InputStream[] parameterStreams = null;
    private byte[][] parameterValues = null;
    private byte[][] staticSqlStrings = null;
    private byte[] streamConvertBuf = new byte[4096];
    private int[] streamLengths = null;
    private boolean hasLimitClause = false;
    private boolean useTrueBoolean = false;
    private char firstCharOfStmt;

    /**
     * Constructor for the PreparedStatement class.
     *
     * @param conn the connection creating this statement
     * @param sql the SQL for this statement
     * @param catalog the catalog/database this statement should
     *                 be issued against
     *
     * @throws java.sql.SQLException if a database error occurs.
     */
    public PreparedStatement(Connection conn, String sql, String catalog)
        throws java.sql.SQLException {
        super(conn, catalog);

        if (sql == null) {
            throw new SQLException("SQL String can not be NULL", "S1009");
        }

        this.dbmd = this.connection.getMetaData();

        String quotedIdentifierString = this.dbmd.getIdentifierQuoteString();

        char quotedIdentifierChar = 0;

        if ((quotedIdentifierString != null)
                && !quotedIdentifierString.equals(" ")
                && (quotedIdentifierString.length() > 0)) {
            quotedIdentifierChar = quotedIdentifierString.charAt(0);
        }

        //
        // If we're using timezone support (and we should be
        // to be correct), then make sure that dates are
        // issued in the Server's timezone, and not the client's
        //
        if (this.connection.useTimezone()) {
            TSDF.setTimeZone(this.connection.getServerTimezone());
        }

        useTrueBoolean = connection.getIO().versionMeetsMinimum(3, 21, 23);
        hasLimitClause = (sql.toUpperCase().indexOf("LIMIT") != -1);

        char[] statementAsChars = sql.toCharArray();
        int statementLength = statementAsChars.length;
        int placeHolderCount = 0;

        for (int i = 0; i < statementLength; i++) {
            if (statementAsChars[i] == '?') {
                placeHolderCount++;
            }

            if (!Character.isWhitespace(statementAsChars[i])) {
                // Determine what kind of statement we're doing (_S_elect, _I_nsert, etc.)
                firstCharOfStmt = Character.toUpperCase(statementAsChars[i]);
            }
        }

        ArrayList endpointList = new ArrayList(placeHolderCount + 1);
        boolean inQuotes = false;
        boolean inQuotedId = false;
        int lastParmEnd = 0;
        int i;
        originalSql = sql;
        connection = conn;

        int pre1 = 0;
        int pre2 = 0;

        for (i = 0; i < statementLength; ++i) {
            char c = statementAsChars[i];

            // are we in a quoted identifier?
            if ((quotedIdentifierChar != 0) && (c == quotedIdentifierChar)) {
                inQuotedId = !inQuotedId;
            }

            // only respect quotes when not in a quoted identifier
            if (!inQuotedId) {
                if ((c == '\'') && (pre1 == '\\') && (pre2 == '\\')) {
                    inQuotes = !inQuotes;
                } else if ((c == '\'') && (pre1 != '\\')) {
                    inQuotes = !inQuotes;
                }
            }

            if ((c == '?') && !inQuotes) {
                endpointList.add(new EndPoint(lastParmEnd, i));
                lastParmEnd = i + 1;
            }

            pre2 = pre1;
            pre1 = c;
        }

        endpointList.add(new EndPoint(lastParmEnd, statementLength));
        staticSqlStrings = new byte[endpointList.size()][];

        

        for (i = 0; i < staticSqlStrings.length; i++) {
            if (this.charEncoding == null) {
                EndPoint ep = (EndPoint) endpointList.get(i);
                int end = ep.end;
                int begin = ep.begin;
                int len = end - begin;
                byte[] buf = new byte[len];

                for (int j = 0; j < len; j++) {
                    buf[j] = (byte) statementAsChars[begin + j];
                }

                staticSqlStrings[i] = buf;
            } else {
                try {
                    EndPoint ep = (EndPoint) endpointList.get(i);
                    int end = ep.end;
                    int begin = ep.begin;
                    int len = end - begin;
                    String temp = new String(statementAsChars, begin, len);
                    staticSqlStrings[i] = StringUtils.getBytes(temp,
                            this.charEncoding);
                } catch (java.io.UnsupportedEncodingException ue) {
                    throw new SQLException(ue.toString());
                }
            }
        }

        int numberOfParameters = staticSqlStrings.length - 1;

        parameterValues = new byte[numberOfParameters][];
        parameterStreams = new InputStream[numberOfParameters];
        isStream = new boolean[numberOfParameters];
        streamLengths = new int[numberOfParameters];
        isNull = new boolean[numberOfParameters];
        clearParameters();

        for (int j = 0; j < numberOfParameters; j++) {
            isStream[j] = false;
        }
    }

    /**
     * JDBC 2.0
     *
     * Set an Array parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param x an object representing an SQL array
     *
     * @throws SQLException because this method is not implemented.
     */
    public void setArray(int i, Array x) throws SQLException {
        throw new NotImplemented();
    }

    /**
     * When a very large ASCII value is input to a LONGVARCHAR parameter,
     * it may be more practical to send it via a java.io.InputStream.
     * JDBC will read the data from the stream as needed, until it reaches
     * end-of-file.  The JDBC driver will do any necessary conversion from
     * ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard Java
     * stream object or your own subclass that implements the standard
     * interface.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @param length the number of bytes in the stream
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized void setAsciiStream(int parameterIndex, InputStream x,
        int length) throws java.sql.SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.VARCHAR);
        } else {
            setBinaryStream(parameterIndex, x, length);
        }
    }

    /**
     * Set a parameter to a java.lang.BigDecimal value.  The driver
     * converts this to a SQL NUMERIC value when it sends it to the
     * database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setBigDecimal(int parameterIndex, BigDecimal x)
        throws java.sql.SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.DECIMAL);
        } else {
            setInternal(parameterIndex, fixDecimalExponent(x.toString()));
        }
    }

    /**
     * When a very large binary value is input to a LONGVARBINARY parameter,
     * it may be more practical to send it via a java.io.InputStream.
     * JDBC will read the data from the stream as needed, until it reaches
     * end-of-file.
     *
     * <P><B>Note:</B> This stream object can either be a standard Java
     * stream object or your own subclass that implements the standard
     * interface.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @param length the number of bytes to read from the stream (ignored)
     *
     * @throws java.sql.SQLException if a database access error occurs
     */
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
        throws java.sql.SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.BINARY);
        } else {
            if ((parameterIndex < 1)
                    || (parameterIndex > staticSqlStrings.length)) {
                throw new java.sql.SQLException(
                    "Parameter index out of range (" + parameterIndex + " > "
                    + staticSqlStrings.length + ")", "S1009");
            }

            parameterStreams[parameterIndex - 1] = x;
            isStream[parameterIndex - 1] = true;
            streamLengths[parameterIndex - 1] = length;
            isNull[parameterIndex - 1] = false;
        }
    }

    /**
     * JDBC 2.0
     *
     * Set a BLOB parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param x an object representing a BLOB
     *
     * @throws SQLException if a database error occurs
     */
    public void setBlob(int i, java.sql.Blob x) throws SQLException {
        setBinaryStream(i, x.getBinaryStream(), Integer.MAX_VALUE);
    }

    /**
     * Set a parameter to a Java boolean value.  The driver converts this
     * to a SQL BIT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     *
     * @throws java.sql.SQLException if a database access error occurs
     */
    public void setBoolean(int parameterIndex, boolean x)
        throws java.sql.SQLException {
        if (useTrueBoolean) {
            setInternal(parameterIndex, x ? "'1'" : "'0'");
        } else {
            setInternal(parameterIndex, x ? "'t'" : "'f'");
        }
    }

    /**
     * Set a parameter to a Java byte value.  The driver converts this to
     * a SQL TINYINT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setByte(int parameterIndex, byte x)
        throws java.sql.SQLException {
        setInternal(parameterIndex, String.valueOf(x));
    }

    /**
     * Set a parameter to a Java array of bytes.  The driver converts this
     * to a SQL VARBINARY or LONGVARBINARY (depending on the argument's
     * size relative to the driver's limits on VARBINARYs) when it sends
     * it to the database.
     *
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setBytes(int parameterIndex, byte[] x)
        throws java.sql.SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.BINARY);
        } else {
            // escape them
            int numBytes = x.length;

            ByteArrayOutputStream bOut = new ByteArrayOutputStream(numBytes);

            bOut.write('\'');

            for (int i = 0; i < numBytes; ++i) {
                byte b = x[i];

                switch (b) {
                case 0: /* Must be escaped for 'mysql' */
                    bOut.write('\\');
                    bOut.write('0');

                    break;

                case '\n': /* Must be escaped for logs */
                    bOut.write('\\');
                    bOut.write('n');

                    break;

                case '\r':
                    bOut.write('\\');
                    bOut.write('r');

                    break;

                case '\\':
                    bOut.write('\\');
                    bOut.write('\\');

                    break;

                case '\'':
                    bOut.write('\\');
                    bOut.write('\'');

                    break;

                case '"': /* Better safe than sorry */
                    bOut.write('\\');
                    bOut.write('"');

                    break;

                case '\032': /* This gives problems on Win32 */
                    bOut.write('\\');
                    bOut.write('Z');

                    break;

                default:
                    bOut.write(b);
                }
            }

            bOut.write('\'');

            setInternal(parameterIndex, bOut.toByteArray());
        }
    }

    /**
     * JDBC 2.0
     *
     * When a very large UNICODE value is input to a LONGVARCHAR
     * parameter, it may be more practical to send it via a
     * java.io.Reader. JDBC will read the data from the stream
     * as needed, until it reaches end-of-file.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param reader the java reader which contains the UNICODE data
     * @param length the number of characters in the stream
     * @exception SQLException if a database-access error occurs.
     */
    public void setCharacterStream(int parameterIndex, java.io.Reader reader,
        int length) throws SQLException {
        try {
            if (reader == null) {
                setNull(parameterIndex, Types.LONGVARCHAR);
            } else {
                char[] c = null;
                int len = 0;

                boolean useLength = this.connection.useStreamLengthsInPrepStmts();

                if (useLength && (length != -1)) {
                    c = new char[length];

                    int numCharsRead = readFully(reader, c, length); // blocks until all read

                    setString(parameterIndex, new String(c, 0, numCharsRead));
                } else {
                    c = new char[4096];

                    StringBuffer buf = new StringBuffer();

                    while ((len = reader.read(c)) != -1) {
                        buf.append(c, 0, len);
                    }

                    setString(parameterIndex, buf.toString());
                }
            }
        } catch (java.io.IOException ioEx) {
            throw new SQLException(ioEx.toString(), "S1000");
        }
    }

    /**
     * JDBC 2.0
     *
     * Set a CLOB parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param x an object representing a CLOB
     *
     * @throws SQLException if a database error occurs
     */
    public void setClob(int i, Clob x) throws SQLException {
        setString(i, x.getSubString(0L, (int) x.length()));
    }

    /**
     * Set a parameter to a java.sql.Date value.  The driver converts this
     * to a SQL DATE value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setDate(int parameterIndex, java.sql.Date x)
        throws java.sql.SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.DATE);
        } else {
            // FIXME: Have instance version of this, problem as it's
            //        not thread-safe :(
            SimpleDateFormat dateFormatter = new SimpleDateFormat(
                    "''yyyy-MM-dd''");
            setInternal(parameterIndex, dateFormatter.format(x));
        }
    }

    /**
     * Set a parameter to a java.sql.Date value.  The driver converts this
     * to a SQL DATE value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @param cal the calendar to interpret the date with
     * @exception SQLException if a database-access error occurs.
     */
    public void setDate(int parameterIndex, java.sql.Date x, Calendar cal)
        throws SQLException {
        setDate(parameterIndex, x);
    }

    /**
     * Set a parameter to a Java double value.  The driver converts this
     * to a SQL DOUBLE value when it sends it to the database
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setDouble(int parameterIndex, double x)
        throws java.sql.SQLException {
        setInternal(parameterIndex, fixDecimalExponent(String.valueOf(x)));
    }

    /**
     * Set a parameter to a Java float value.  The driver converts this
     * to a SQL FLOAT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setFloat(int parameterIndex, float x)
        throws java.sql.SQLException {
        setInternal(parameterIndex, fixDecimalExponent(String.valueOf(x)));
    }

    /**
     * Set a parameter to a Java int value.  The driver converts this to
     * a SQL INTEGER value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setInt(int parameterIndex, int x) throws java.sql.SQLException {
        setInternal(parameterIndex, String.valueOf(x));
    }

    /**
     * Set a parameter to a Java long value.  The driver converts this to
     * a SQL BIGINT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setLong(int parameterIndex, long x)
        throws java.sql.SQLException {
        setInternal(parameterIndex, String.valueOf(x));
    }

    /**
     * The number, types and properties of a ResultSet's columns
     * are provided by the getMetaData method.
     *
     * @return the description of a ResultSet's columns
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        throw new NotImplemented();
    }

    /**
     * Set a parameter to SQL NULL
     *
     * <p><B>Note:</B> You must specify the parameters SQL type (although
     * MySQL ignores it)
     *
     * @param parameterIndex the first parameter is 1, etc...
     * @param sqlType the SQL type code defined in java.sql.Types
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setNull(int parameterIndex, int sqlType)
        throws java.sql.SQLException {
        setInternal(parameterIndex, "null");
        isNull[parameterIndex - 1] = true;
    }

    //--------------------------JDBC 2.0-----------------------------

    /**
     * Set a parameter to SQL NULL.
     *
     * <P><B>Note:</B> You must specify the parameter's SQL type.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param sqlType SQL type code defined by java.sql.Types
     * @param arg argument parameters for null
     * @exception SQLException if a database-access error occurs.
     */
    public void setNull(int parameterIndex, int sqlType, String arg)
        throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    /**
     * Set the value of a parameter using an object; use the java.lang
     * equivalent objects for integral values.
     *
     * <P>The given Java object will be converted to the targetSqlType before
     * being sent to the database.
     *
     * <P>note that this method may be used to pass database-specific
     * abstract data types.  This is done by using a Driver-specific
     * Java type and using a targetSqlType of java.sql.Types.OTHER
     *
     * @param parameterIndex the first parameter is 1...
     * @param parameterObj the object containing the input parameter value
     * @param targetSqlType The SQL type to be send to the database
     * @param scale For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC
     *      types this is the number of digits after the decimal.  For
     *      all other types this value will be ignored.
     * @throws java.sql.SQLException if a database access error occurs
     */
    public void setObject(int parameterIndex, Object parameterObj,
        int targetSqlType, int scale) throws java.sql.SQLException {
        if (parameterObj == null) {
            setNull(parameterIndex, java.sql.Types.OTHER);
        } else {
            try {
                switch (targetSqlType) {
                case Types.BIT:
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.REAL:
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.DECIMAL:
                case Types.NUMERIC:

                    Number parameterAsNum;

                    if (parameterObj instanceof Boolean) {
                        parameterAsNum = ((Boolean) parameterObj).booleanValue()
                            ? new Integer(1) : new Integer(0);
                    } else if (parameterObj instanceof String) {
                        switch (targetSqlType) {
                        case Types.BIT:
                            parameterAsNum = (Boolean.getBoolean((String) parameterObj)
                                ? new Integer("1") : new Integer("0"));

                            break;

                        case Types.TINYINT:
                        case Types.SMALLINT:
                        case Types.INTEGER:
                            parameterAsNum = Integer.valueOf((String) parameterObj);

                            break;

                        case Types.BIGINT:
                            parameterAsNum = Long.valueOf((String) parameterObj);

                            break;

                        case Types.REAL:
                            parameterAsNum = Float.valueOf((String) parameterObj);

                            break;

                        case Types.FLOAT:
                        case Types.DOUBLE:
                            parameterAsNum = Double.valueOf((String) parameterObj);

                            break;

                        case Types.DECIMAL:
                        case Types.NUMERIC:default:
                            parameterAsNum = new java.math.BigDecimal((String) parameterObj);
                        }
                    } else {
                        parameterAsNum = (Number) parameterObj;
                    }

                    switch (targetSqlType) {
                    case Types.BIT:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                        setInt(parameterIndex, parameterAsNum.intValue());

                        break;

                    case Types.BIGINT:
                        setLong(parameterIndex, parameterAsNum.longValue());

                        break;

                    case Types.REAL:
                        setFloat(parameterIndex, parameterAsNum.floatValue());

                        break;

                    case Types.FLOAT:
                    case Types.DOUBLE:
                        setDouble(parameterIndex, parameterAsNum.doubleValue());

                        break;

                    case Types.DECIMAL:
                    case Types.NUMERIC:default:

                        if (parameterAsNum instanceof java.math.BigDecimal) {
                            setBigDecimal(parameterIndex,
                                (java.math.BigDecimal) parameterAsNum);
                        } else if (parameterAsNum instanceof java.math.BigInteger) {
                            setBigDecimal(parameterIndex,
                                new java.math.BigDecimal(
                                    (java.math.BigInteger) parameterAsNum, scale));
                        } else {
                            setBigDecimal(parameterIndex,
                                new java.math.BigDecimal(
                                    parameterAsNum.doubleValue()));
                        }

                        break;
                    }

                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    setString(parameterIndex, parameterObj.toString());

                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:

                    if (parameterObj instanceof String) {
                        setBytes(parameterIndex,
                            ((String) parameterObj).getBytes());
                    } else {
                        setBytes(parameterIndex, (byte[]) parameterObj);
                    }

                    break;

                case Types.DATE:
                case Types.TIMESTAMP:

                    java.util.Date parameterAsDate;

                    if (parameterObj instanceof String) {
                        ParsePosition pp = new ParsePosition(0);
                        java.text.DateFormat sdf = new java.text.SimpleDateFormat(getDateTimePattern(
                                    (String) parameterObj, false));
                        parameterAsDate = sdf.parse((String) parameterObj, pp);
                    } else {
                        parameterAsDate = (java.util.Date) parameterObj;
                    }

                    switch (targetSqlType) {
                    case Types.DATE:

                        if (parameterAsDate instanceof java.sql.Date) {
                            setDate(parameterIndex,
                                (java.sql.Date) parameterAsDate);
                        } else {
                            setDate(parameterIndex,
                                new java.sql.Date(parameterAsDate.getTime()));
                        }

                        break;

                    case Types.TIMESTAMP:

                        if (parameterAsDate instanceof java.sql.Timestamp) {
                            setTimestamp(parameterIndex,
                                (java.sql.Timestamp) parameterAsDate);
                        } else {
                            setTimestamp(parameterIndex,
                                new java.sql.Timestamp(
                                    parameterAsDate.getTime()));
                        }

                        break;
                    }

                    break;

                case Types.TIME:

                    if (parameterObj instanceof String) {
                        java.text.DateFormat sdf = new java.text.SimpleDateFormat(getDateTimePattern(
                                    (String) parameterObj, true));
                        setTime(parameterIndex,
                            new java.sql.Time(sdf.parse((String) parameterObj)
                                                 .getTime()));
                    } else if (parameterObj instanceof Timestamp) {
                        Timestamp xT = (Timestamp) parameterObj;
                        setTime(parameterIndex,
                            new java.sql.Time(xT.getHours(), xT.getMinutes(),
                                xT.getSeconds()));
                    } else {
                        setTime(parameterIndex, (java.sql.Time) parameterObj);
                    }

                    break;

                case Types.OTHER:
                    setSerializableObject(parameterIndex, parameterObj);

                    break;

                default:
                    throw new java.sql.SQLException("Unknown Types value",
                        "S1000");
                }
            } catch (Exception ex) {
                if (ex instanceof java.sql.SQLException) {
                    throw (java.sql.SQLException) ex;
                } else {
                    throw new java.sql.SQLException("Cannot convert "
                        + parameterObj.getClass().toString()
                        + " to SQL type requested due to "
                        + ex.getClass().getName() + " - " + ex.getMessage(),
                        "S1000");
                }
            }
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param parameterIndex DOCUMENT ME!
     * @param parameterObj DOCUMENT ME!
     * @param targetSqlType DOCUMENT ME!
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public void setObject(int parameterIndex, Object parameterObj,
        int targetSqlType) throws java.sql.SQLException {
        setObject(parameterIndex, parameterObj, targetSqlType, 0);
    }

    /**
     * DOCUMENT ME!
     *
     * @param parameterIndex DOCUMENT ME!
     * @param parameterObj DOCUMENT ME!
     * @throws java.sql.SQLException DOCUMENT ME!
     */
    public void setObject(int parameterIndex, Object parameterObj)
        throws java.sql.SQLException {
        if (parameterObj == null) {
            setNull(parameterIndex, java.sql.Types.OTHER);
        } else {
            if (parameterObj instanceof Byte) {
                setInt(parameterIndex, ((Byte) parameterObj).intValue());
            } else if (parameterObj instanceof String) {
                setString(parameterIndex, (String) parameterObj);
            } else if (parameterObj instanceof BigDecimal) {
                setBigDecimal(parameterIndex, (BigDecimal) parameterObj);
            } else if (parameterObj instanceof Short) {
                setShort(parameterIndex, ((Short) parameterObj).shortValue());
            } else if (parameterObj instanceof Integer) {
                setInt(parameterIndex, ((Integer) parameterObj).intValue());
            } else if (parameterObj instanceof Long) {
                setLong(parameterIndex, ((Long) parameterObj).longValue());
            } else if (parameterObj instanceof Float) {
                setFloat(parameterIndex, ((Float) parameterObj).floatValue());
            } else if (parameterObj instanceof Double) {
                setDouble(parameterIndex, ((Double) parameterObj).doubleValue());
            } else if (parameterObj instanceof byte[]) {
                setBytes(parameterIndex, (byte[]) parameterObj);
            } else if (parameterObj instanceof java.sql.Date) {
                setDate(parameterIndex, (java.sql.Date) parameterObj);
            } else if (parameterObj instanceof Time) {
                setTime(parameterIndex, (Time) parameterObj);
            } else if (parameterObj instanceof Timestamp) {
                setTimestamp(parameterIndex, (Timestamp) parameterObj);
            } else if (parameterObj instanceof Boolean) {
                setBoolean(parameterIndex,
                    ((Boolean) parameterObj).booleanValue());
            } else if (parameterObj instanceof InputStream) {
                setBinaryStream(parameterIndex, (InputStream) parameterObj, -1);
            } else if (parameterObj instanceof java.sql.Blob) {
                setBlob(parameterIndex, (java.sql.Blob) parameterObj);
            } else if (parameterObj instanceof java.sql.Clob) {
                setClob(parameterIndex, (java.sql.Clob) parameterObj);
            } else {
                setSerializableObject(parameterIndex, parameterObj);
            }
        }
    }

    /**
     * @see PreparedStatement#getParameterMetaData()
     */
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new NotImplemented();
    }

    /**
     * JDBC 2.0
     *
     * Set a REF(&lt;structured-type&gt;) parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param x an object representing data of an SQL REF Type
     *
     * @throws SQLException if a database error occurs
     */
    public void setRef(int i, Ref x) throws SQLException {
        throw new NotImplemented();
    }

    /**
     * Set a parameter to a Java short value.  The driver converts this
     * to a SQL SMALLINT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setShort(int parameterIndex, short x)
        throws java.sql.SQLException {
        setInternal(parameterIndex, String.valueOf(x));
    }

    /**
     * Set a parameter to a Java String value.  The driver converts this
     * to a SQL VARCHAR or LONGVARCHAR value (depending on the arguments
     * size relative to the driver's limits on VARCHARs) when it sends it
     * to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setString(int parameterIndex, String x)
        throws java.sql.SQLException {
        // if the passed string is null, then set this column to null
        if (x == null) {
            setInternal(parameterIndex, "null".getBytes());
        } else {
            StringBuffer buf = new StringBuffer((int) (x.length() * 1.1));
            buf.append('\'');

            int stringLength = x.length();

            for (int i = 0; i < stringLength; ++i) {
                char c = x.charAt(i);

                switch (c) {
                case 0: /* Must be escaped for 'mysql' */
                    buf.append('\\');
                    buf.append('0');

                    break;

                case '\n': /* Must be escaped for logs */
                    buf.append('\\');
                    buf.append('n');

                    break;

                case '\r':
                    buf.append('\\');
                    buf.append('r');

                    break;

                case '\\':
                    buf.append('\\');
                    buf.append('\\');

                    break;

                case '\'':
                    buf.append('\\');
                    buf.append('\'');

                    break;

                case '"': /* Better safe than sorry */
                    buf.append('\\');
                    buf.append('"');

                    break;

                case '\032': /* This gives problems on Win32 */
                    buf.append('\\');
                    buf.append('Z');

                    break;

                default:
                    buf.append(c);
                }
            }

            buf.append('\'');

            String parameterAsString = buf.toString();

            byte[] parameterAsBytes = null;

            if (this.charConverter != null) {
                parameterAsBytes = this.charConverter.toBytes(parameterAsString);
            } else {
                try {
                    parameterAsBytes = StringUtils.getBytes(parameterAsString,
                            this.charEncoding);
                } catch (UnsupportedEncodingException uEE) {
                    throw new SQLException("Unsupported encoding '"
                        + this.charEncoding + "'", "S1009");
                }
            }

            setInternal(parameterIndex, parameterAsBytes);
        }
    }

    /**
     * Set a parameter to a java.sql.Time value.  The driver converts
     * this to a SQL TIME value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...));
     * @param x the parameter value
     * @throws java.sql.SQLException if a database access error occurs
     */
    public void setTime(int parameterIndex, Time x)
        throws java.sql.SQLException {
        setTimeInternal(parameterIndex, x, TimeZone.getDefault());
    }

    /**
     * Set a parameter to a java.sql.Time value.  The driver converts this
     * to a SQL TIME value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @param cal the cal specifying the timezone
     * @throws SQLException if a database-access error occurs.
     */
    public void setTime(int parameterIndex, java.sql.Time x, Calendar cal)
        throws SQLException {
        setTimeInternal(parameterIndex, x, cal.getTimeZone());
    }

    /**
     * Set a parameter to a java.sql.Timestamp value.  The driver converts
     * this to a SQL TIMESTAMP value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @throws java.sql.SQLException if a database access error occurs
     */
    public void setTimestamp(int parameterIndex, Timestamp x)
        throws java.sql.SQLException {
        setTimestampInternal(parameterIndex, x, TimeZone.getDefault());
    }

    /**
     * Set a parameter to a java.sql.Timestamp value.  The driver
     * converts this to a SQL TIMESTAMP value when it sends it to the
     * database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @param cal the calendar specifying the timezone to use
     * @throws SQLException if a database-access error occurs.
     */
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x,
        Calendar cal) throws SQLException {
        setTimestampInternal(parameterIndex, x, cal.getTimeZone());
    }

    /**
     * @see PreparedStatement#setURL(int, URL)
     */
    public void setURL(int parameterIndex, URL arg) throws SQLException {
        if (arg != null) {
            setString(parameterIndex, arg.toString());
        } else {
            setNull(parameterIndex, Types.CHAR);
        }
    }

    /**
     * When a very large Unicode value is input to a LONGVARCHAR parameter,
     * it may be more practical to send it via a java.io.InputStream.
     * JDBC will read the data from the stream as needed, until it reaches
     * end-of-file.  The JDBC driver will do any necessary conversion from
     * UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard Java
     * stream object or your own subclass that implements the standard
     * interface.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @param length the number of bytes to read from the stream
     * @throws java.sql.SQLException if a database access error occurs
     */
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
        throws java.sql.SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.VARCHAR);
        } else {
            setBinaryStream(parameterIndex, x, length);
        }
    }

    /**
     * JDBC 2.0
     *
     * Add a set of parameters to the batch.
     *
     * @exception SQLException if a database-access error occurs.
     * @see Statement#addBatch
     */
    public void addBatch() throws SQLException {
        if (batchedArgs == null) {
            batchedArgs = new ArrayList();
        }

        batchedArgs.add(new BatchParams(parameterValues, parameterStreams,
                isStream, streamLengths, isNull));
    }

    /**
     * In general, parameter values remain in force for repeated used of a
     * Statement.  Setting a parameter value automatically clears its
     * previous value.  However, in some cases, it is useful to immediately
     * release the resources used by the current parameter values; this
     * can be done by calling clearParameters
     *
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void clearParameters() throws java.sql.SQLException {
        for (int i = 0; i < parameterValues.length; i++) {
            parameterValues[i] = null;
            parameterStreams[i] = null;
            isStream[i] = false;
            isNull[i] = false;
        }
    }

    /**
     * Closes this prepared statement and releases all
     * resources.
     *
     * @throws SQLException if database error occurs.
     */
    public void close() throws SQLException {
        super.close();

        this.dbmd = null;
        this.originalSql = null;
        this.staticSqlStrings = null;
        this.parameterValues = null;
        this.parameterStreams = null;
        this.isStream = null;
        this.streamLengths = null;
        this.isNull = null;
        this.streamConvertBuf = null;
        this.sendPacket = null;
    }

    /**
     * Some prepared statements return multiple results; the execute method
     * handles these complex statements as well as the simpler form of
     * statements handled by executeQuery and executeUpdate
     *
     * @return true if the next result is a ResultSet; false if it is an
     *      update count or there are no more results
     * @exception java.sql.SQLException if a database access error occurs
     */
    public boolean execute() throws java.sql.SQLException {
        if (connection.isReadOnly() && (firstCharOfStmt != 'S')) {
            throw new SQLException("Connection is read-only. "
                + "Queries leading to data modification are not allowed",
                "S1009");
        }

        checkClosed();

        if (sendPacket == null) {
            sendPacket = new Buffer(connection.getNetBufferLength(),
                    connection.getMaxAllowedPacket());
        } else {
            sendPacket.clear();
        }

        sendPacket.writeByte((byte) MysqlDefs.QUERY);

        boolean useStreamLengths = this.connection.useStreamLengthsInPrepStmts();

        for (int i = 0; i < parameterValues.length; i++) {
            if ((parameterValues[i] == null) && (parameterStreams[i] == null)) {
                throw new java.sql.SQLException(
                    "No value specified for parameter " + (i + 1));
            }

            sendPacket.writeBytesNoNull(staticSqlStrings[i]);

            if (isStream[i]) {
                sendPacket.writeBytesNoNull(streamToBytes(parameterStreams[i],
                        streamLengths[i], useStreamLengths));
            } else {
                sendPacket.writeBytesNoNull(parameterValues[i]);
            }
        }

        sendPacket.writeBytesNoNull(staticSqlStrings[parameterValues.length]);

        ResultSet rs = null;

        synchronized (connection.getMutex()) {
            String oldCatalog = null;

            if (!connection.getCatalog().equals(currentCatalog)) {
                oldCatalog = connection.getCatalog();
                connection.setCatalog(currentCatalog);
            }

            // If there isn't a limit clause in the SQL
            // then limit the number of rows to return in
            // an efficient manner. Only do this if
            // setMaxRows() hasn't been used on any Statements
            // generated from the current Connection (saves
            // a query, and network traffic).
            //
            // Only apply max_rows to selects
            //
            if (connection.useMaxRows()) {
                if (firstCharOfStmt == 'S') {
                    if (hasLimitClause) {
                        rs = connection.execSQL((String) null, maxRows,
                                sendPacket, resultSetConcurrency,
                                createStreamingResultSet(), true,
                                this.currentCatalog);
                    } else {
                        if (maxRows <= 0) {
                            connection.execSQL("SET OPTION SQL_SELECT_LIMIT=DEFAULT",
                                -1, this.currentCatalog);
                        } else {
                            connection.execSQL("SET OPTION SQL_SELECT_LIMIT="
                                + maxRows, -1, this.currentCatalog);
                        }
                    }
                } else {
                    connection.execSQL("SET OPTION SQL_SELECT_LIMIT=DEFAULT",
                        -1, this.currentCatalog);
                }

                // Finally, execute the query
                rs = connection.execSQL(null, -1, sendPacket,
                        resultSetConcurrency, createStreamingResultSet(),
                        (firstCharOfStmt == 'S'), this.currentCatalog);
            } else {
                rs = connection.execSQL(null, -1, sendPacket,
                        resultSetConcurrency, createStreamingResultSet(),
                        (firstCharOfStmt == 'S'), this.currentCatalog);
            }

            if (oldCatalog != null) {
                connection.setCatalog(oldCatalog);
            }
        }

        lastInsertId = rs.getUpdateID();

        if (rs != null) {
            results = rs;
        }

        rs.setConnection(connection);
        rs.setResultSetType(resultSetType);
        rs.setResultSetConcurrency(resultSetConcurrency);
        rs.setStatement(this);

        return ((rs != null) && rs.reallyResult());
    }

    /**
     * JDBC 2.0
     *
     * Submit a batch of commands to the database for execution.
     * This method is optional.
     *
     * @return an array of update counts containing one element for each
     * command in the batch.  The array is ordered according
     * to the order in which commands were inserted into the batch
     * @exception SQLException if a database-access error occurs, or the
     * driver does not support batch statements
     */
    public int[] executeBatch() throws SQLException {
        if (connection.isReadOnly()) {
            throw new SQLException("Connection is read-only. "
                + "Queries leading to data modification are not allowed",
                "S1009");
        }

        try {
            int[] updateCounts = null;

            if (batchedArgs != null) {
                int nbrCommands = batchedArgs.size();
                updateCounts = new int[nbrCommands];

                for (int i = 0; i < nbrCommands; i++) {
                    updateCounts[i] = -3;
                }

                SQLException sqlEx = null;

                int commandIndex = 0;

                for (commandIndex = 0; commandIndex < nbrCommands;
                        commandIndex++) {
                    Object arg = batchedArgs.get(commandIndex);

                    if (arg instanceof String) {
                        updateCounts[commandIndex] = executeUpdate((String) arg);
                    } else {
                        BatchParams paramArg = (BatchParams) arg;

                        try {
                            updateCounts[commandIndex] = executeUpdate(paramArg.parameterStrings,
                                    paramArg.parameterStreams,
                                    paramArg.isStream, paramArg.streamLengths,
                                    paramArg.isNull);
                        } catch (SQLException ex) {
                            updateCounts[commandIndex] = EXECUTE_FAILED;

                            if (this.connection.continueBatchOnError()) {
                                sqlEx = ex;
                            } else {
                                int[] newUpdateCounts = new int[commandIndex];
                                System.arraycopy(updateCounts, 0,
                                    newUpdateCounts, 0, commandIndex);

                                throw new java.sql.BatchUpdateException(ex
                                    .getMessage(), ex.getSQLState(),
                                    ex.getErrorCode(), newUpdateCounts);
                            }
                        }
                    }
                }

                if (sqlEx != null) {
                    throw new java.sql.BatchUpdateException(sqlEx.getMessage(),
                        sqlEx.getSQLState(), sqlEx.getErrorCode(), updateCounts);
                }
            }

            return (updateCounts != null) ? updateCounts : new int[0];
        } finally {
            clearBatch();
        }
    }

    /**
     * A Prepared SQL query is executed and its ResultSet is returned
     *
     * @return a ResultSet that contains the data produced by the
     *      query - never null
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized java.sql.ResultSet executeQuery()
        throws java.sql.SQLException {
        checkClosed();

        if (sendPacket == null) {
            sendPacket = new Buffer(connection.getNetBufferLength(),
                    connection.getMaxAllowedPacket());
        } else {
            sendPacket.clear();
        }

        sendPacket.writeByte((byte) MysqlDefs.QUERY);

        boolean useStreamLengths = this.connection.useStreamLengthsInPrepStmts();

        for (int i = 0; i < parameterValues.length; i++) {
            if ((parameterValues[i] == null) && (parameterStreams[i] == null)) {
                throw new java.sql.SQLException(
                    "No value specified for parameter " + (i + 1), "07001");
            }

            sendPacket.writeBytesNoNull(staticSqlStrings[i]);

            if (isStream[i]) {
                sendPacket.writeBytesNoNull(streamToBytes(parameterStreams[i],
                        streamLengths[i], useStreamLengths));
            } else {
                sendPacket.writeBytesNoNull(parameterValues[i]);
            }
        }

        sendPacket.writeBytesNoNull(staticSqlStrings[parameterValues.length]);

        if (results != null) {
            results.close();
        }

        // We need to execute this all together
        // So synchronize on the Connection's mutex (because
        // even queries going through there synchronize
        // on the same mutex.
        synchronized (connection.getMutex()) {
            String oldCatalog = null;

            if (!connection.getCatalog().equals(currentCatalog)) {
                oldCatalog = connection.getCatalog();
                connection.setCatalog(currentCatalog);
            }

            if (connection.useMaxRows()) {
                // If there isn't a limit clause in the SQL
                // then limit the number of rows to return in
                // an efficient manner. Only do this if
                // setMaxRows() hasn't been used on any Statements
                // generated from the current Connection (saves
                // a query, and network traffic).
                if (hasLimitClause) {
                    results = connection.execSQL((String) null, maxRows,
                            sendPacket, resultSetConcurrency,
                            createStreamingResultSet(), true,
                            this.currentCatalog);
                } else {
                    if (maxRows <= 0) {
                        connection.execSQL("SET OPTION SQL_SELECT_LIMIT=DEFAULT",
                            -1, this.currentCatalog);
                    } else {
                        connection.execSQL("SET OPTION SQL_SELECT_LIMIT="
                            + maxRows, -1, this.currentCatalog);
                    }

                    results = connection.execSQL(null, -1, sendPacket,
                            resultSetConcurrency, createStreamingResultSet(),
                            true, this.currentCatalog);

                    if (oldCatalog != null) {
                        connection.setCatalog(oldCatalog);
                    }
                }
            } else {
                results = connection.execSQL(null, -1, sendPacket,
                        resultSetConcurrency, createStreamingResultSet(), true,
                        this.currentCatalog);
            }

            if (oldCatalog != null) {
                connection.setCatalog(oldCatalog);
            }
        }

        lastInsertId = results.getUpdateID();
        nextResults = results;
        results.setConnection(connection);
        results.setResultSetType(resultSetType);
        results.setResultSetConcurrency(resultSetConcurrency);
        results.setStatement(this);

        if (!results.reallyResult()) {
            if (!connection.getAutoCommit()) {
                try {
                    connection.rollback();
                } catch (SQLException sqlEx) {
                    // FIXME: Log later?
                }
            }

            throw new SQLException("Can not issue INSERT/UPDATE/DELETE with executeQuery()",
                "S1009");
        }

        return (java.sql.ResultSet) results;
    }

    /**
     * Execute a SQL INSERT, UPDATE or DELETE statement.  In addition,
     * SQL statements that return nothing such as SQL DDL statements can
     * be executed.
     *
     * @return either the row count for INSERT, UPDATE or DELETE; or
     *      0 for SQL statements that return nothing.
     * @exception java.sql.SQLException if a database access error occurs
     */
    public synchronized int executeUpdate() throws java.sql.SQLException {
        return executeUpdate(parameterValues, parameterStreams, isStream,
            streamLengths, isNull);
    }

    /**
     * Returns this PreparedStatement represented as a string.
     *
     * @return this PreparedStatement represented as a string.
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append(super.toString());
        buf.append(": ");

        try {
            for (int i = 0; i < parameterValues.length; ++i) {
                if (this.charEncoding != null) {
                    buf.append(new String(staticSqlStrings[i], this.charEncoding));
                } else {
                    buf.append(new String(staticSqlStrings[i]));
                }

                if ((parameterValues[i] == null) && !isStream[i]) {
                    buf.append("** NOT SPECIFIED **");
                } else if (isStream[i]) {
                    buf.append("** STREAM DATA **");
                } else {
                    if (this.charConverter != null) {
                        buf.append(this.charConverter.toString(
                                parameterValues[i]));
                    } else {
                        if (this.charEncoding != null) {
                            buf.append(new String(parameterValues[i],
                                    this.charEncoding));
                        } else {
                            buf.append(StringUtils.toAsciiString(
                                    parameterValues[i]));
                        }
                    }
                }
            }

            if (this.charEncoding != null) {
                buf.append(new String(
                        staticSqlStrings[parameterValues.length],
                        this.charEncoding));
            } else {
                buf.append(staticSqlStrings[parameterValues.length]);
            }
        } catch (UnsupportedEncodingException uue) {
            throw new RuntimeException("Unsupported character encoding '"
                + this.charEncoding + "'");
        }

        return buf.toString();
    }

    /**
     * Added to allow batch-updates
     *
     * @param batchedParameterStrings string values used in single statement
     * @param batchedParameterStreams stream values used in single statement
     * @param batchedIsStream flags for streams used in single statement
     * @param batchedIsNull flags for parameters that are null
     * @param batchedStreamLengths lengths of streams to be read.
     *
     * @return the update count
     *
     * @throws SQLException if a database error occurs
     */
    protected synchronized int executeUpdate(byte[][] batchedParameterStrings,
        InputStream[] batchedParameterStreams, boolean[] batchedIsStream,
        int[] batchedStreamLengths, boolean[] batchedIsNull)
        throws SQLException {
        if (connection.isReadOnly()) {
            throw new SQLException("Connection is read-only. "
                + "Queries leading to data modification are not allowed",
                "S1009");
        }

        checkClosed();

        if (sendPacket == null) {
            sendPacket = new Buffer(connection.getNetBufferLength(),
                    connection.getMaxAllowedPacket());
        } else {
            sendPacket.clear();
        }

        sendPacket.writeByte((byte) MysqlDefs.QUERY);

        boolean useStreamLengths = this.connection.useStreamLengthsInPrepStmts();

        for (int i = 0; i < batchedParameterStrings.length; i++) {
            if ((batchedParameterStrings[i] == null)
                    && (batchedParameterStreams[i] == null)) {
                throw new java.sql.SQLException(
                    "No value specified for parameter " + (i + 1), "07001");
            }

            sendPacket.writeBytesNoNull(staticSqlStrings[i]);

            if (batchedIsStream[i]) {
                sendPacket.writeBytesNoNull(streamToBytes(
                        batchedParameterStreams[i], batchedStreamLengths[i],
                        useStreamLengths));
            } else {
                sendPacket.writeBytesNoNull(parameterValues[i]);
            }
        }

        sendPacket.writeBytesNoNull(staticSqlStrings[batchedParameterStrings.length]);

        // The checking and changing of catalogs
        // must happen in sequence, so synchronize
        // on the same mutex that _conn is using
        ResultSet rs = null;

        synchronized (connection.getMutex()) {
            String oldCatalog = null;

            if (!connection.getCatalog().equals(currentCatalog)) {
                oldCatalog = connection.getCatalog();
                connection.setCatalog(currentCatalog);
            }

            //
            // Only apply max_rows to selects
            //
            if (connection.useMaxRows()) {
                connection.execSQL("SET OPTION SQL_SELECT_LIMIT=DEFAULT", -1,
                    this.currentCatalog);
            }

            rs = connection.execSQL(null, -1, sendPacket, resultSetConcurrency,
                    false, false, this.currentCatalog);

            if (oldCatalog != null) {
                connection.setCatalog(oldCatalog);
            }
        }

        if (rs.reallyResult()) {
            // FIXME: Rollback?
            throw new java.sql.SQLException("Results returned for UPDATE ONLY.",
                "01S03");
        } else {
            updateCount = rs.getUpdateCount();

            int truncatedUpdateCount = 0;

            if (updateCount > Integer.MAX_VALUE) {
                truncatedUpdateCount = Integer.MAX_VALUE;
            } else {
                truncatedUpdateCount = (int) updateCount;
            }

            lastInsertId = rs.getUpdateID();

            return truncatedUpdateCount;
        }
    }

    byte[] getBytes(int parameterIndex) throws SQLException {
        if (isStream[parameterIndex]) {
            return streamToBytes(parameterStreams[parameterIndex], false,
                streamLengths[parameterIndex],
                this.connection.useStreamLengthsInPrepStmts());
        } else {
            byte[] parameterVal = parameterValues[parameterIndex];

            if (parameterVal == null) {
                return null;
            }

            if ((parameterVal[0] == '\'')
                    && (parameterVal[parameterVal.length - 1] == '\'')) {
                byte[] valNoQuotes = new byte[parameterVal.length - 2];
                System.arraycopy(parameterVal, 1, valNoQuotes, 0,
                    parameterVal.length - 2);

                return valNoQuotes;
            } else {
                return parameterVal;
            }
        }
    }

    boolean isNull(int paramIndex) {
        return isNull[paramIndex];
    }

    /**
     * Sets the concurrency for result sets generated by this statement
     */
    void setResultSetConcurrency(int concurrencyFlag) {
        resultSetConcurrency = concurrencyFlag;
    }

    /**
     * Sets the result set type for result sets generated by this statement
     */
    void setResultSetType(int typeFlag) {
        resultSetType = typeFlag;
    }

    private final String getDateTimePattern(String dt, boolean toTime)
        throws Exception {
        //
        // Special case
        //
        int dtLength = (dt != null) ? dt.length() : 0;

        if ((dtLength >= 8) && (dtLength <= 10)) {
            int dashCount = 0;
            boolean isDateOnly = true;

            for (int i = 0; i < dtLength; i++) {
                char c = dt.charAt(i);

                if (!Character.isDigit(c) && (c != '-')) {
                    isDateOnly = false;

                    break;
                }

                if (c == '-') {
                    dashCount++;
                }
            }

            if (isDateOnly && (dashCount == 2)) {
                return "yyyy-MM-dd";
            }
        }

        //
        // Special case - time-only
        //
        boolean colonsOnly = true;

        for (int i = 0; i < dtLength; i++) {
            char c = dt.charAt(i);

            if (!Character.isDigit(c) && (c != ':')) {
                colonsOnly = false;

                break;
            }
        }

        if (colonsOnly) {
            return "HH:mm:ss";
        }

        int n;
        int z;
        int count;
        int maxvecs;
        char c;
        char separator;
        StringReader reader = new StringReader(dt + " ");
        ArrayList vec = new ArrayList();
        ArrayList vecRemovelist = new ArrayList();
        Object[] nv = new Object[3];
        Object[] v;
        nv[0] = new Character('y');
        nv[1] = new StringBuffer();
        nv[2] = new Integer(0);
        vec.add(nv);

        if (toTime) {
            nv = new Object[3];
            nv[0] = new Character('h');
            nv[1] = new StringBuffer();
            nv[2] = new Integer(0);
            vec.add(nv);
        }

        while ((z = reader.read()) != -1) {
            separator = (char) z;
            maxvecs = vec.size();

            for (count = 0; count < maxvecs; count++) {
                v = (Object[]) vec.get(count);
                n = ((Integer) v[2]).intValue();
                c = getSuccessor(((Character) v[0]).charValue(), n);

                if (!Character.isLetterOrDigit(separator)) {
                    if ((c == ((Character) v[0]).charValue()) && (c != 'S')) {
                        vecRemovelist.add(v);
                    } else {
                        ((StringBuffer) v[1]).append(separator);

                        if ((c == 'X') || (c == 'Y')) {
                            v[2] = new Integer(4);
                        }
                    }
                } else {
                    if (c == 'X') {
                        c = 'y';
                        nv = new Object[3];
                        nv[1] = (new StringBuffer(((StringBuffer) v[1])
                                .toString())).append('M');
                        nv[0] = new Character('M');
                        nv[2] = new Integer(1);
                        vec.add(nv);
                    } else if (c == 'Y') {
                        c = 'M';
                        nv = new Object[3];
                        nv[1] = (new StringBuffer(((StringBuffer) v[1])
                                .toString())).append('d');
                        nv[0] = new Character('d');
                        nv[2] = new Integer(1);
                        vec.add(nv);
                    }

                    ((StringBuffer) v[1]).append(c);

                    if (c == ((Character) v[0]).charValue()) {
                        v[2] = new Integer(n + 1);
                    } else {
                        v[0] = new Character(c);
                        v[2] = new Integer(1);
                    }
                }
            }

            int size = vecRemovelist.size();

            for (int i = 0; i < size; i++) {
                v = (Object[]) vecRemovelist.get(i);
                vec.remove(v);
            }

            vecRemovelist.clear();
        }

        int size = vec.size();

        for (int i = 0; i < size; i++) {
            v = (Object[]) vec.get(i);
            c = ((Character) v[0]).charValue();
            n = ((Integer) v[2]).intValue();

            boolean bk = getSuccessor(c, n) != c;
            boolean atEnd = (((c == 's') || (c == 'm')
                || ((c == 'h') && toTime)) && bk);
            boolean finishesAtDate = (bk && (c == 'd') && !toTime);
            boolean containsEnd = (((StringBuffer) v[1]).toString().indexOf('W') != -1);

            if ((!atEnd && !finishesAtDate) || (containsEnd)) {
                vecRemovelist.add(v);
            }
        }

        size = vecRemovelist.size();

        for (int i = 0; i < size; i++) {
            vec.remove(vecRemovelist.get(i));
        }

        vecRemovelist.clear();
        v = (Object[]) vec.get(0); //might throw exception

        StringBuffer format = (StringBuffer) v[1];
        format.setLength(format.length() - 1);

        return format.toString();
    }

    private final void setInternal(int paramIndex, byte[] val)
        throws java.sql.SQLException {
        if ((paramIndex < 1) || (paramIndex > staticSqlStrings.length)) {
            throw new SQLException("Parameter index out of range ("
                + paramIndex + " > " + staticSqlStrings.length + ").", "S1009");
        }

        if (this.isClosed) {
            throw new SQLException("PreparedStatement has been closed. No further operations allowed.",
                "S1009");
        }

        isStream[paramIndex - 1] = false;
        isNull[paramIndex - 1] = false;
        parameterStreams[paramIndex - 1] = null;
        parameterValues[paramIndex - 1] = val;
    }

    private final void setInternal(int paramIndex, String val)
        throws java.sql.SQLException {
        byte[] parameterAsBytes = null;

        if (this.charConverter != null) {
            parameterAsBytes = this.charConverter.toBytes(val);
        } else {
            try {
                parameterAsBytes = StringUtils.getBytes(val, this.charEncoding);
            } catch (UnsupportedEncodingException uEE) {
                throw new SQLException("Unsupported encoding '"
                    + this.charEncoding + "'", "S1009");
            }
        }

        setInternal(paramIndex, parameterAsBytes);
    }

    /**
     * Sets the value for the placeholder as a serialized Java
     * object (used by various forms of setObject()
     */
    private final void setSerializableObject(int parameterIndex,
        Object parameterObj) throws SQLException {
        try {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            ObjectOutputStream objectOut = new ObjectOutputStream(bytesOut);
            objectOut.writeObject(parameterObj);
            objectOut.flush();
            objectOut.close();
            bytesOut.flush();
            bytesOut.close();

            byte[] buf = bytesOut.toByteArray();
            ByteArrayInputStream bytesIn = new ByteArrayInputStream(buf);
            setBinaryStream(parameterIndex, bytesIn, buf.length);
        } catch (Exception ex) {
            throw new java.sql.SQLException("Invalid argument value: "
                + ex.getClass().getName(), "S1009");
        }
    }

    private final char getSuccessor(char c, int n) {
        return ((c == 'y') && (n == 2)) ? 'X'
                                        : (((c == 'y') && (n < 4)) ? 'y'
                                                                   : ((c == 'y')
        ? 'M'
        : (((c == 'M') && (n == 2)) ? 'Y'
                                    : (((c == 'M') && (n < 3)) ? 'M'
                                                               : ((c == 'M')
        ? 'd'
        : (((c == 'd') && (n < 2)) ? 'd'
                                   : ((c == 'd') ? 'H'
                                                 : (((c == 'H') && (n < 2))
        ? 'H'
        : ((c == 'H') ? 'm'
                      : (((c == 'm') && (n < 2)) ? 'm'
                                                 : ((c == 'm') ? 's'
                                                               : (((c == 's')
        && (n < 2)) ? 's' : 'W'))))))))))));
    }

    /**
      * Set a parameter to a java.sql.Time value.  The driver converts
      * this to a SQL TIME value when it sends it to the database, using
      * the given timezone.
      *
      * @param parameterIndex the first parameter is 1...));
      * @param x the parameter value
      * @param tz the timezone to use
      * @throws java.sql.SQLException if a database access error occurs
      */
    private void setTimeInternal(int parameterIndex, Time x, TimeZone tz)
        throws java.sql.SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.TIME);
        } else {
            x = TimeUtil.changeTimezone(this.connection, x, tz,
                    this.connection.getServerTimezone());
            setInternal(parameterIndex, "'" + x.toString() + "'");
        }
    }

    /**
     * Set a parameter to a java.sql.Timestamp value.  The driver
     * converts this to a SQL TIMESTAMP value when it sends it to the
     * database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @param tz the timezone to use
     * @throws SQLException if a database-access error occurs.
     */
    private void setTimestampInternal(int parameterIndex, Timestamp x,
        TimeZone tz) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, java.sql.Types.TIMESTAMP);
        } else {
            String timestampString = null;
            x = TimeUtil.changeTimezone(this.connection, x, tz,
                    this.connection.getServerTimezone());

            synchronized (TSDF) {
                timestampString = "'" + TSDF.format(x) + "'";
            }

            setInternal(parameterIndex, timestampString); // SimpleDateFormat is not thread-safe
        }
    }

    //
    // Adds '+' to decimal numbers that are positive (MySQL doesn't
    // understand them otherwise
    //
    private static final String fixDecimalExponent(String dString) {
        int ePos = dString.indexOf("E");

        if (ePos == -1) {
            ePos = dString.indexOf("e");
        }

        if (ePos != -1) {
            if (dString.length() > (ePos + 1)) {
                char maybeMinusChar = dString.charAt(ePos + 1);

                if (maybeMinusChar != '-') {
                    StringBuffer buf = new StringBuffer(dString.length() + 1);
                    buf.append(dString.substring(0, ePos + 1));
                    buf.append('+');
                    buf.append(dString.substring(ePos + 1, dString.length()));
                    dString = buf.toString();
                }
            }
        }

        return dString;
    }

    private final void escapeblockFast(byte[] buf,
        ByteArrayOutputStream bytesOut, int size) {
        int lastwritten = 0;

        for (int i = 0; i < size; i++) {
            byte b = buf[i];

            if (b == '\0') {
                //write stuff not yet written
                if (i > lastwritten) {
                    bytesOut.write(buf, lastwritten, i - lastwritten);
                }

                //write escape
                bytesOut.write('\\');
                bytesOut.write('0');
                lastwritten = i + 1;
            } else {
                if ((b == '\\') || (b == '\'') || (b == '"')) {
                    //write stuff not yet written
                    if (i > lastwritten) {
                        bytesOut.write(buf, lastwritten, i - lastwritten);
                    }

                    //write escape
                    bytesOut.write('\\');
                    lastwritten = i; //not i+1 as b wasn't written.
                }
            }
        }

        //write out remaining stuff from buffer
        if (lastwritten < size) {
            bytesOut.write(buf, lastwritten, size - lastwritten);
        }
    }

    private final int readblock(InputStream i, byte[] b, int length)
        throws java.sql.SQLException {
        try {
            int lengthToRead = length;

            if (lengthToRead > b.length) {
                lengthToRead = b.length;
            }

            return i.read(b, 0, lengthToRead);
        } catch (Throwable E) {
            throw new java.sql.SQLException("Error reading from InputStream "
                + E.getClass().getName(), "S1000");
        }
    }

    private final int readblock(InputStream i, byte[] b)
        throws java.sql.SQLException {
        try {
            return i.read(b);
        } catch (Throwable E) {
            throw new java.sql.SQLException("Error reading from InputStream "
                + E.getClass().getName(), "S1000");
        }
    }

    /**
     * For the setXXXStream() methods. Basically converts an
     * InputStream into a String. Not very efficient, but it
     * works.
     *
     */
    private final byte[] streamToBytes(InputStream in, int streamLength,
        boolean useLength) throws java.sql.SQLException {
        return streamToBytes(in, true, streamLength, useLength);
    }

    private final byte[] streamToBytes(InputStream in, boolean escape,
        int streamLength, boolean useLength) throws java.sql.SQLException {
        try {
            if (streamLength == -1) {
                useLength = false;
            }

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

            int bc = -1;

            if (useLength) {
                bc = readblock(in, streamConvertBuf, streamLength);
            } else {
                bc = readblock(in, streamConvertBuf);
            }

            int lengthLeftToRead = streamLength - bc;

            if (escape) {
                bytesOut.write('\'');
            }

            while (bc > 0) {
                if (escape) {
                    escapeblockFast(streamConvertBuf, bytesOut, bc);
                } else {
                    bytesOut.write(streamConvertBuf, 0, bc);
                }

                if (useLength) {
                    bc = readblock(in, streamConvertBuf, lengthLeftToRead);

                    if (bc > 0) {
                        lengthLeftToRead -= bc;
                    }
                } else {
                    bc = readblock(in, streamConvertBuf);
                }
            }

            if (escape) {
                bytesOut.write('\'');
            }

            return bytesOut.toByteArray();
        } finally {
            try {
                in.close();
            } catch (IOException ioEx) {
                ;
            }

            in = null;
        }
    }

    /**
     * Reads length bytes from reader into buf. Blocks until
     * enough input is available
     */
    private static int readFully(Reader reader, char[] buf, int length)
        throws IOException {
        int numCharsRead = 0;

        while (numCharsRead < length) {
            int count = reader.read(buf, numCharsRead, length - numCharsRead);

            if (count < 0) {
                break;
            }

            numCharsRead += count;
        }

        return numCharsRead;
    }

    class BatchParams {
        boolean[] isNull = null;
        boolean[] isStream = null;
        InputStream[] parameterStreams = null;
        byte[][] parameterStrings = null;
        int[] streamLengths = null;

        BatchParams(byte[][] strings, InputStream[] streams,
            boolean[] isStreamFlags, int[] lengths, boolean[] isNullFlags) {
            //
            // Make copies
            //
            parameterStrings = new byte[strings.length][];
            parameterStreams = new InputStream[streams.length];
            isStream = new boolean[isStreamFlags.length];
            streamLengths = new int[lengths.length];
            isNull = new boolean[isNullFlags.length];
            System.arraycopy(strings, 0, parameterStrings, 0, strings.length);
            System.arraycopy(streams, 0, parameterStreams, 0, streams.length);
            System.arraycopy(isStreamFlags, 0, isStream, 0, isStreamFlags.length);
            System.arraycopy(lengths, 0, streamLengths, 0, lengths.length);
            System.arraycopy(isNullFlags, 0, isNull, 0, isNullFlags.length);
        }
    }

    class EndPoint {
        int begin;
        int end;

        EndPoint(int b, int e) {
            begin = b;
            end = e;
        }
    }
}
