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
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;

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

import java.text.NumberFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;

import sun.io.CharToByteConverter;


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
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id$
 */

public class PreparedStatement
    extends com.mysql.jdbc.Statement
    implements java.sql.PreparedStatement
{

    //~ Instance/static variables .............................................

    protected static SimpleDateFormat TSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
         
    static boolean                    timezoneSet = false;
    static final Object               tzMutex     = new Object();
    protected static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

    /**
     * Formatter for double - Steve Ferguson
     */
    private static NumberFormat        doubleFormatter;
    private static java.util.Hashtable templateCache       = 
            new java.util.Hashtable();
    protected boolean[]                isNull             = null;
    protected boolean[]                isStream           = null;
    protected InputStream[]            parameterStreams   = null;
    protected String[]                 parameterValues    = null;
    private String                     originalSql                = null;
    private byte[]                     streamConvertBuf                 = new byte[4096];
    private char                       firstCharOfStmt;
    private boolean                    hasLimitClause     = false;
    private Buffer                     sendPacket         = null;
    private byte[][]                   staticSqlStrings   = null;
    private boolean                    useTrueBoolean     = false;
    private byte[]                     resultSetByteValues;

    //~ Initializers ..........................................................

    // Class Initializer
    static {
        doubleFormatter = NumberFormat.getNumberInstance(java.util.Locale.US);
        doubleFormatter.setGroupingUsed(false);

        // attempt to prevent truncation
        doubleFormatter.setMaximumFractionDigits(12);
    }

    //~ Constructors ..........................................................

    /**
     * Constructor for the PreparedStatement class.
     */
    public PreparedStatement(Connection conn, String sql, String catalog)
                      throws java.sql.SQLException
    {
        super(conn, catalog);
        
        
        
        //if (_conn.useTimezone())
        //{
        	//_TSDF.setTimeZone(_conn.getServerTimezone());
        //	_TSDF.setTimeZone(TimeZone.getTimeZone("GMT"));
        //}
        
        useTrueBoolean = connection.getIO().versionMeetsMinimum(3, 21, 23);
                 
        hasLimitClause = (sql.toUpperCase().indexOf("LIMIT") != -1);

        char[] statementAsChars = sql.toCharArray();
        int    statementLength  = statementAsChars.length;
        int    placeHolderCount = 0;
        
        for (int i = 0; i < statementLength; i++) {

            if (statementAsChars[i] == '?') {
                placeHolderCount++;
            }
            
            if (!Character.isWhitespace(statementAsChars[i])) {
            	// Determine what kind of statement we're doing (_S_elect, _I_nsert, etc.)
        		firstCharOfStmt = Character.toUpperCase(statementAsChars[i]);
        	}
        }

        ArrayList endpointList           = new ArrayList(placeHolderCount + 1);
        boolean   inQuotes    = false;
        int       lastParmEnd = 0;
        int       i;
        originalSql  = sql;
        connection = conn;

        int pre1 = 0;
        int pre2 = 0;

        for (i = 0; i < statementLength; ++i) {

            int c = statementAsChars[i];

            if (c == '\'' && pre1 == '\\' && pre2 == '\\') {
                inQuotes = !inQuotes;
            } else if (c == '\'' && pre1 != '\\') {
                inQuotes = !inQuotes;
            }

            if (c == '?' && !inQuotes) {
                endpointList.add(new EndPoint(lastParmEnd, i));
                lastParmEnd = i + 1;
            }

            pre2 = pre1;
            pre1 = c;
        }

        endpointList.add(new EndPoint(lastParmEnd, statementLength));
        staticSqlStrings = new byte[endpointList.size()][];

        String characterEncoding = null;

        if (connection.useUnicode()) {
            characterEncoding = connection.getEncoding();
        }

        for (i = 0; i < staticSqlStrings.length; i++) {

            if (characterEncoding == null) {

                EndPoint ep    = (EndPoint)endpointList.get(i);
                int      end   = ep.end;
                int      begin = ep.begin;
                int      len   = end - begin;
                byte[]   buf   = new byte[len];

                for (int j = 0; j < len; j++) {
                    buf[j] = (byte)statementAsChars[begin + j];
                }

                staticSqlStrings[i] = buf;
            } else {

                try {

                    EndPoint ep          = (EndPoint)endpointList.get(i);
                    int      end         = ep.end;
                    int      begin       = ep.begin;
                    int      len         = end - begin;
                    String   temp        = new String(statementAsChars, begin, 
                                                      len);
                    staticSqlStrings[i] = temp.getBytes(characterEncoding);
                } catch (java.io.UnsupportedEncodingException ue) {
                    throw new SQLException(ue.toString());
                }
            }
        }

        parameterValues  = new String[staticSqlStrings.length - 1];
        parameterStreams = new InputStream[staticSqlStrings.length - 1];
        isStream         = new boolean[staticSqlStrings.length - 1];
        isNull           = new boolean[staticSqlStrings.length - 1];
        clearParameters();

        for (int j = 0; j < parameterValues.length; j++) {
            isStream[j] = false;
        }
    }

    //~ Methods ...............................................................

    /**
     * JDBC 2.0
     *
     * Set an Array parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param x an object representing an SQL array
     */
    public void setArray(int i, Array x)
                  throws SQLException
    {
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
    public synchronized void setAsciiStream(int parameterIndex, InputStream X, 
                                            int length)
                                     throws java.sql.SQLException
    {

        if (X == null) {
            setNull(parameterIndex, java.sql.Types.VARCHAR);
        } else {
            setBinaryStream(parameterIndex, X, length);
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
    public void setBigDecimal(int parameterIndex, BigDecimal X)
                       throws java.sql.SQLException
    {

        if (X == null) {
            setNull(parameterIndex, java.sql.Types.DECIMAL);
        } else {
            setInternal(parameterIndex, fixDecimalExponent(X.toString()));
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
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setBinaryStream(int parameterIndex, InputStream X, int length)
                         throws java.sql.SQLException
    {

        if (X == null) {
            setNull(parameterIndex, java.sql.Types.BINARY);
        } else {

            if (parameterIndex < 1 || 
                parameterIndex > staticSqlStrings.length) {
                throw new java.sql.SQLException("Parameter index out of range (" + 
                                                parameterIndex + " > " + 
                                                staticSqlStrings.length + 
                                                ")", "S1009");
            }

            parameterStreams[parameterIndex - 1] = X;
            isStream[parameterIndex - 1]         = true;
            isNull[parameterIndex - 1]           = false;
        }
    }

    /**
     * JDBC 2.0
     *
     * Set a BLOB parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param x an object representing a BLOB
     */
    public void setBlob(int i, java.sql.Blob x)
                 throws SQLException
    {
        setBinaryStream(i, x.getBinaryStream(), Integer.MAX_VALUE);
    }

    /**
     * Set a parameter to a Java boolean value.  The driver converts this
     * to a SQL BIT value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setBoolean(int parameterIndex, boolean x)
                    throws java.sql.SQLException
    {

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
                 throws java.sql.SQLException
    {
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
                  throws java.sql.SQLException
    {

        if (x == null) {
            setNull(parameterIndex, java.sql.Types.BINARY);
        } else {

            ByteArrayInputStream BIn = new ByteArrayInputStream(x);
            setBinaryStream(parameterIndex, BIn, x.length);
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
     * @param x the java reader which contains the UNICODE data
     * @param length the number of characters in the stream 
     * @exception SQLException if a database-access error occurs.
     */
    public void setCharacterStream(int parameterIndex, java.io.Reader reader, 
                                   int length)
                            throws SQLException
    {

        try {

            if (reader == null) {
                setNull(parameterIndex, Types.LONGVARCHAR);
            } else {	
            	StringBuffer buf = new StringBuffer(length);
            	
                char[]               c   = new char[4096];
                
                int len = 0;
                
                while ((len = reader.read(c)) != -1)
                {
                	buf.append(c, 0, len);
                }
                
                setString(parameterIndex, buf.toString());
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
     */
    public void setClob(int i, Clob x)
                 throws SQLException
    {
        throw new NotImplemented();
    }

    /**
     * Set a parameter to a java.sql.Date value.  The driver converts this
     * to a SQL DATE value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setDate(int parameterIndex, java.sql.Date X)
                 throws java.sql.SQLException
    {

        if (X == null) {
            setNull(parameterIndex, java.sql.Types.DATE);
        } else {

            SimpleDateFormat DF = new SimpleDateFormat("''yyyy-MM-dd''");
            setInternal(parameterIndex, DF.format(X));
        }
    }

    /**
     * Set a parameter to a java.sql.Date value.  The driver converts this
     * to a SQL DATE value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @exception SQLException if a database-access error occurs.
     */
    public void setDate(int parameterIndex, java.sql.Date X, Calendar Cal)
                 throws SQLException
    {
        setDate(parameterIndex, X);
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
                   throws java.sql.SQLException
    {

        //set(parameterIndex, _DoubleFormatter.format(x));
        //if (x <= Long.MAX_VALUE && x >= Long.MIN_VALUE)
        //{
        //   synchronized (_DoubleFormatter)
        //   {
        //      setInternal(parameterIndex, _DoubleFormatter.format(x));
        //   }
        //}
        //else
        //{
        setInternal(parameterIndex, fixDecimalExponent(String.valueOf(x)));

        //}
        // - Fix for large doubles by Steve Ferguson
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
                  throws java.sql.SQLException
    {
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
    public void setInt(int parameterIndex, int x)
                throws java.sql.SQLException
    {
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
                 throws java.sql.SQLException
    {
        setInternal(parameterIndex, String.valueOf(x));
    }

    /**
     * The number, types and properties of a ResultSet's columns
     * are provided by the getMetaData method.
     *
     * @return the description of a ResultSet's columns
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.ResultSetMetaData getMetaData()
                                           throws SQLException
    {
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
                 throws java.sql.SQLException
    {
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
     * @exception SQLException if a database-access error occurs.
     */
    public void setNull(int parameterIndex, int sqlType, String Arg)
                 throws SQLException
    {
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
     * @param x the object containing the input parameter value
     * @param targetSqlType The SQL type to be send to the database
     * @param scale For java.sql.Types.DECIMAL or java.sql.Types.NUMERIC
     *      types this is the number of digits after the decimal.  For 
     *      all other types this value will be ignored.
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setObject(int parameterIndex, Object X, int targetSqlType, 
                          int scale)
                   throws java.sql.SQLException
    {

        if (X == null) {
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

                        Number X_as_number;

                        if (X instanceof Boolean) {
                            X_as_number = ((Boolean)X).booleanValue()
                                              ? new Integer(1) : new Integer(0);
                        } else if (X instanceof String) {

                            switch (targetSqlType) {

                                case Types.BIT:
                                    X_as_number = (Boolean.getBoolean(
                                                           (String)X)
                                                       ? new Integer("1")
                                                       : new Integer("0"));

                                    break;

                                case Types.TINYINT:
                                case Types.SMALLINT:
                                case Types.INTEGER:
                                    X_as_number = Integer.valueOf((String)X);

                                    break;

                                case Types.BIGINT:
                                    X_as_number = Long.valueOf((String)X);

                                    break;

                                case Types.REAL:
                                    X_as_number = Float.valueOf((String)X);

                                    break;

                                case Types.FLOAT:
                                case Types.DOUBLE:
                                    X_as_number = Double.valueOf((String)X);

                                    break;

                                case Types.DECIMAL:
                                case Types.NUMERIC:
                                default:
                                    X_as_number = new java.math.BigDecimal(
                                                          (String)X);
                            }
                        } else {
                            X_as_number = (Number)X;
                        }

                        switch (targetSqlType) {

                            case Types.BIT:
                            case Types.TINYINT:
                            case Types.SMALLINT:
                            case Types.INTEGER:
                                setInt(parameterIndex, X_as_number.intValue());

                                break;

                            case Types.BIGINT:
                                setLong(parameterIndex, 
                                        X_as_number.longValue());

                                break;

                            case Types.REAL:
                                setFloat(parameterIndex, 
                                         X_as_number.floatValue());

                                break;

                            case Types.FLOAT:
                            case Types.DOUBLE:
                                setDouble(parameterIndex, 
                                          X_as_number.doubleValue());

                                break;

                            case Types.DECIMAL:
                            case Types.NUMERIC:
                            default:

                                if (X_as_number instanceof java.math.BigDecimal) {
                                    setBigDecimal(parameterIndex, 
                                                  (java.math.BigDecimal)X_as_number);
                                } else if (X_as_number instanceof java.math.BigInteger) {
                                    setBigDecimal(parameterIndex, 
                                                  new java.math.BigDecimal(
                                                          (java.math.BigInteger)X_as_number, 
                                                          scale));
                                } else {
                                    setBigDecimal(parameterIndex, 
                                                  new java.math.BigDecimal(X_as_number.doubleValue()));
                                }

                                break;
                        }

                        break;

                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                        setString(parameterIndex, X.toString());

                        break;

                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:

                        if (X instanceof String) {
                            setBytes(parameterIndex, ((String)X).getBytes());
                        } else {
                            setBytes(parameterIndex, (byte[])X);
                        }

                        break;

                    case Types.DATE:
                    case Types.TIMESTAMP:

                        java.util.Date X_as_date;

                        if (X instanceof String) {

                            ParsePosition        pp  = new ParsePosition(0);
                            java.text.DateFormat sdf = new java.text.SimpleDateFormat(getDateTimePattern(
                                                                                              (String)X, 
                                                                                              false));
                            X_as_date = sdf.parse((String)X, pp);
                        } else {
                            X_as_date = (java.util.Date)X;
                        }

                        switch (targetSqlType) {

                            case Types.DATE:

                                if (X_as_date instanceof java.sql.Date) {
                                    setDate(parameterIndex, 
                                            (java.sql.Date)X_as_date);
                                } else {
                                    setDate(parameterIndex, 
                                            new java.sql.Date(X_as_date.getTime()));
                                }

                                break;

                            case Types.TIMESTAMP:

                                if (X_as_date instanceof java.sql.Timestamp) {
                                    setTimestamp(parameterIndex, 
                                                 (java.sql.Timestamp)X_as_date);
                                } else {
                                    setTimestamp(parameterIndex, 
                                                 new java.sql.Timestamp(X_as_date.getTime()));
                                }

                                break;
                        }

                        break;

                    case Types.TIME:

                        if (X instanceof String) {

                            java.text.DateFormat sdf = new java.text.SimpleDateFormat(getDateTimePattern(
                                                                                              (String)X, 
                                                                                              true));
                            setTime(parameterIndex, 
                                    new java.sql.Time(sdf.parse((String)X).getTime()));
                        } else if (X instanceof Timestamp) {

                            Timestamp xT = (Timestamp)X;
                            setTime(parameterIndex, 
                                    new java.sql.Time(xT.getHours(), 
                                                      xT.getMinutes(), 
                                                      xT.getSeconds()));
                        } else {
                            setTime(parameterIndex, (java.sql.Time)X);
                        }

                        break;

                    case Types.OTHER:

                        try {

                            ByteArrayOutputStream BytesOut  = 
                                    new ByteArrayOutputStream();
                            ObjectOutputStream    ObjectOut = 
                                    new ObjectOutputStream(BytesOut);
                            ObjectOut.writeObject(X);
                            ObjectOut.flush();
                            ObjectOut.close();
                            BytesOut.flush();
                            BytesOut.close();

                            byte[]               buf     = BytesOut.toByteArray();
                            ByteArrayInputStream BytesIn = new ByteArrayInputStream(
                                                                   buf);
                            setBinaryStream(parameterIndex, BytesIn, -1);
                        } catch (Exception E) {
                            throw new java.sql.SQLException("Invalid argument value: " + 
                                                            E.getClass().getName(), 
                                                            "S1009");
                        }

                        break;

                    default:
                        throw new java.sql.SQLException("Unknown Types value", 
                                                        "S1000");
                }
            } catch (Exception ex) {

                if (ex instanceof java.sql.SQLException) {
                    throw (java.sql.SQLException)ex;
                } else {
                    throw new java.sql.SQLException("Cannot convert " + 
                                                    X.getClass().toString() + 
                                                    " to SQL type requested due to " + 
                                                    ex.getClass().getName() + 
                                                    " - " + ex.getMessage(), 
                                                    "S1000");
                }
            }
        }
    }

    public void setObject(int parameterIndex, Object X, int targetSqlType)
                   throws java.sql.SQLException
    {
        setObject(parameterIndex, X, targetSqlType, 0);
    }

    public void setObject(int parameterIndex, Object X)
                   throws java.sql.SQLException
    {

        if (X == null) {
            setNull(parameterIndex, java.sql.Types.OTHER);
        } else {

            if (X instanceof Byte) {
                setInt(parameterIndex, ((Byte)X).intValue());
            } else if (X instanceof String) {
                setString(parameterIndex, (String)X);
            } else if (X instanceof BigDecimal) {
                setBigDecimal(parameterIndex, (BigDecimal)X);
            } else if (X instanceof Short) {
                setShort(parameterIndex, ((Short)X).shortValue());
            } else if (X instanceof Integer) {
                setInt(parameterIndex, ((Integer)X).intValue());
            } else if (X instanceof Long) {
                setLong(parameterIndex, ((Long)X).longValue());
            } else if (X instanceof Float) {
                setFloat(parameterIndex, ((Float)X).floatValue());
            } else if (X instanceof Double) {
                setDouble(parameterIndex, ((Double)X).doubleValue());
            } else if (X instanceof byte[]) {
                setBytes(parameterIndex, (byte[])X);
            } else if (X instanceof java.sql.Date) {
                setDate(parameterIndex, (java.sql.Date)X);
            } else if (X instanceof Time) {
                setTime(parameterIndex, (Time)X);
            } else if (X instanceof Timestamp) {
                setTimestamp(parameterIndex, (Timestamp)X);
            } else if (X instanceof Boolean) {
                setBoolean(parameterIndex, ((Boolean)X).booleanValue());
            } else if (X instanceof InputStream) {
                setBinaryStream(parameterIndex, (InputStream)X, -1);
            } else {

                try {

                    ByteArrayOutputStream BytesOut  = new ByteArrayOutputStream();
                    ObjectOutputStream    ObjectOut = new ObjectOutputStream(
                                                              BytesOut);
                    ObjectOut.writeObject(X);
                    ObjectOut.flush();
                    ObjectOut.close();
                    BytesOut.flush();
                    BytesOut.close();

                    byte[]               buf     = BytesOut.toByteArray();
                    ByteArrayInputStream BytesIn = new ByteArrayInputStream(
                                                           buf);
                    setBinaryStream(parameterIndex, BytesIn, -1);
                } catch (Exception E) {
                    throw new java.sql.SQLException("Invalid argument value: " + 
                                                    E.getClass().getName(), 
                                                    "S1009");
                }
            }
        }
    }

    /**
     * JDBC 2.0
     *
     * Set a REF(&lt;structured-type&gt;) parameter.
     *
     * @param i the first parameter is 1, the second is 2, ...
     * @param x an object representing data of an SQL REF Type
     */
    public void setRef(int i, Ref x)
                throws SQLException
    {
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
                  throws java.sql.SQLException
    {
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
                   throws java.sql.SQLException
    {

        // if the passed string is null, then set this column to null
        if (x == null) {
            setInternal(parameterIndex, "null");
        } else {

            StringBuffer B = new StringBuffer((int)(x.length() * 1.1));
            B.append('\'');

            for (int i = 0; i < x.length(); ++i) {

                char c = x.charAt(i);

                switch (c) {

                    case 0: /* Must be escaped for 'mysql' */
                        B.append('\\');
                        B.append('0');

                        break;

                    case '\n': /* Must be escaped for logs */
                        B.append('\\');
                        B.append('n');

                        break;

                    case '\r':
                        B.append('\\');
                        B.append('r');

                        break;

                    case '\\':
                        B.append('\\');
                        B.append('\\');

                        break;

                    case '\'':
                        B.append('\\');
                        B.append('\'');

                        break;

                    case '"': /* Better safe than sorry */
                        B.append('\\');
                        B.append('"');

                        break;

                    case '\032': /* This gives problems on Win32 */
                        B.append('\\');
                        B.append('Z');

                        break;

                    default:
                        B.append(c);
                }
            }

            B.append('\'');
            setInternal(parameterIndex, B.toString());
        }
    }

    /**
     * Set a parameter to a java.sql.Time value.  The driver converts
     * this to a SQL TIME value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...));
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setTime(int parameterIndex, Time X)
                 throws java.sql.SQLException
    {

        if (X == null) {
            setNull(parameterIndex, java.sql.Types.TIME);
        } else {
        	
        	//if (_conn.useTimezone())
           	//{
        	//	
        	//	X = TimeUtil.changeTimezone(X, TimeUtil.GMT_TIMEZONE, _conn.getServerTimezone());       		
           	//}
           	
            setInternal(parameterIndex, "'" + X.toString() + "'");
        }
    }

    /**
     * Set a parameter to a java.sql.Time value.  The driver converts this
     * to a SQL TIME value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value
     * @exception SQLException if a database-access error occurs.
     */
    public void setTime(int parameterIndex, java.sql.Time X, Calendar Cal)
                 throws SQLException
    {
        setTime(parameterIndex, X);
    }

    /**
     * Set a parameter to a java.sql.Timestamp value.  The driver converts
     * this to a SQL TIMESTAMP value when it sends it to the database.
     *
     * @param parameterIndex the first parameter is 1...
     * @param x the parameter value
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setTimestamp(int parameterIndex, Timestamp X)
                      throws java.sql.SQLException
    {

        if (X == null) {
            setNull(parameterIndex, java.sql.Types.TIMESTAMP);
        } else {

            String TimestampString = null;
            
            
           	//if (_conn.useTimezone())
           	//{
        	//	
        	//	X = TimeUtil.changeTimezone(X, TimeUtil.GMT_TIMEZONE, _conn.getServerTimezone());       		
           	//}
           	

            synchronized (TSDF) {
                TimestampString = "'" + TSDF.format(X) + "'";
            }

            setInternal(parameterIndex, TimestampString); // SimpleDateFormat is not thread-safe
        }
    }

    /**
     * Set a parameter to a java.sql.Timestamp value.  The driver
     * converts this to a SQL TIMESTAMP value when it sends it to the
     * database.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the parameter value 
     * @exception SQLException if a database-access error occurs.
     */
    public void setTimestamp(int parameterIndex, java.sql.Timestamp X, 
                             Calendar Cal)
                      throws SQLException
    {
        setTimestamp(parameterIndex, X);
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
     * @exception java.sql.SQLException if a database access error occurs
     */
    public void setUnicodeStream(int parameterIndex, InputStream X, int length)
                          throws java.sql.SQLException
    {

        if (X == null) {
            setNull(parameterIndex, java.sql.Types.VARCHAR);
        } else {
            setBinaryStream(parameterIndex, X, length);
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
    public void addBatch()
                  throws SQLException
    {

        if (batchedArgs == null) {
            batchedArgs = new ArrayList();
        }

        batchedArgs.add(new BatchParams(parameterValues, parameterStreams, 
                                         isStream, isNull));
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
    public void clearParameters()
                         throws java.sql.SQLException
    {

        for (int i = 0; i < parameterValues.length; i++) {
            parameterValues[i]  = null;
            parameterStreams[i] = null;
            isStream[i]         = false;
            isNull[i]           = false;
        }
    }

    /**
     * Closes this prepared statement and releases all
     * resources.
     */
    public void close()
               throws SQLException
    {
        super.close();
        originalSql              = null;
        staticSqlStrings = null;
        parameterValues  = null;
        parameterStreams = null;
        isStream         = null;
        isNull           = null;
        streamConvertBuf               = null;
        sendPacket       = null;
        templateCache     = null;
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
    public boolean execute()
                    throws java.sql.SQLException
    {
        checkClosed();

        if (sendPacket == null) {
            sendPacket = new Buffer(connection.getNetBufferLength(), 
                                     connection.getMaxAllowedPacket());
        } else {
            sendPacket.clear();
        }

        sendPacket.writeByte((byte)MysqlDefs.QUERY);

        String Encoding = null;

        if (connection.useUnicode()) {
            Encoding = connection.getEncoding();
        }

        try {

            for (int i = 0; i < parameterValues.length; i++) {

                if (parameterValues[i] == null && 
                    parameterStreams[i] == null) {
                    throw new java.sql.SQLException("No value specified for parameter " + 
                                                    (i + 1));
                }

                //if (Encoding != null) {
                sendPacket.writeBytesNoNull(staticSqlStrings[i]);

                //}
                //else {
                //    _SendPacket.writeStringNoNull(_TemplateStrings[i]);
                //}
                if (isStream[i]) {
                    sendPacket.writeBytesNoNull(streamToBytes(
                                                         parameterStreams[i]));
                } else {

                    if (Encoding != null) {
                        sendPacket.writeStringNoNull(parameterValues[i], 
                                                      Encoding);
                    } else {
                        sendPacket.writeStringNoNull(parameterValues[i]);
                    }
                }
            }

            //if (Encoding != null) {
            sendPacket.writeBytesNoNull(
                    staticSqlStrings[parameterValues.length]);

            // }
            // else {
            //   _SendPacket.writeStringNoNull(_TemplateStrings[_ParameterStrings.length]);
            // }
        } catch (java.io.UnsupportedEncodingException UE) {
            throw new SQLException("Unsupported character encoding '" + 
                                   Encoding + "'");
        }

        ResultSet RS = null;

        synchronized (connection.getMutex()) {

            String OldCatalog = null;

            if (!connection.getCatalog().equals(currentCatalog)) {
                OldCatalog = connection.getCatalog();
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
                        RS = connection.execSQL((String)null, maxRows, sendPacket, resultSetType, createStreamingResultSet());
                    } else {

                        if (maxRows <= 0) {
                            connection.execSQL(
                                    "SET OPTION SQL_SELECT_LIMIT=DEFAULT", -1);
                        } else {
                            connection.execSQL(
                                    "SET OPTION SQL_SELECT_LIMIT=" + 
                                    maxRows, -1);
                        }
                    }
                } else {
                    connection.execSQL("SET OPTION SQL_SELECT_LIMIT=DEFAULT", -1);
                }

                // Finally, execute the query
                RS = connection.execSQL(null, -1, sendPacket, resultSetType, createStreamingResultSet());
            } else {
                RS = connection.execSQL(null, -1, sendPacket, resultSetType, createStreamingResultSet());
            }

            if (OldCatalog != null) {
                connection.setCatalog(OldCatalog);
            }
        }

        lastInsertId = RS.getUpdateID();

        if (RS != null) {
            results = RS;
        }

        RS.setConnection(connection);
        RS.setResultSetType(resultSetType);
        RS.setResultSetConcurrency(resultSetConcurrency);
        RS.setStatement(this);

        return (RS != null && RS.reallyResult());
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
    public int[] executeBatch()
                       throws SQLException
    {

        try {

            int[] updateCounts = null;

            if (batchedArgs != null) {

                int nbrCommands = batchedArgs.size();
                updateCounts = new int[nbrCommands];

                for (int i = 0; i < nbrCommands; i++) {
                    updateCounts[i] = -3;
                }

                SQLException sqlEx = null;

                for (int i = 0; i < nbrCommands; i++) {

                    Object arg = batchedArgs.get(i);

                    if (arg instanceof String) {
                        updateCounts[i] = executeUpdate((String)arg);
                    } else {

                        BatchParams paramArg = (BatchParams)arg;

                        try {
                            updateCounts[i] = executeUpdate(
                                                      paramArg.parameterStrings, 
                                                      paramArg.parameterStreams, 
                                                      paramArg.isStream, 
                                                      paramArg.isNull);
                        } catch (SQLException ex) {
                            sqlEx = ex;
                        }
                    }
                }

                if (sqlEx != null) {
                    throw new java.sql.BatchUpdateException(sqlEx.getMessage(), 
                                                            sqlEx.getSQLState(), 
                                                            sqlEx.getErrorCode(), 
                                                            updateCounts);
                }
            }

            return updateCounts != null ? updateCounts : new int[0];
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
                                                 throws java.sql.SQLException
    {
        checkClosed();

        if (sendPacket == null) {
            sendPacket = new Buffer(connection.getNetBufferLength(), 
                                     connection.getMaxAllowedPacket());
        } else {
            sendPacket.clear();
        }

        sendPacket.writeByte((byte)MysqlDefs.QUERY);

        String Encoding = null;

        if (connection.useUnicode()) {
            Encoding = connection.getEncoding();
        }

        try {

            for (int i = 0; i < parameterValues.length; i++) {

                if (parameterValues[i] == null && 
                    parameterStreams[i] == null) {
                    throw new java.sql.SQLException("No value specified for parameter " + 
                                                    (i + 1), "07001");
                }

                sendPacket.writeBytesNoNull(staticSqlStrings[i]);

                if (isStream[i]) {
                    sendPacket.writeBytesNoNull(streamToBytes(
                                                         parameterStreams[i]));
                } else {

                    if (Encoding != null) {
                        sendPacket.writeStringNoNull(parameterValues[i], 
                                                      Encoding);
                    } else {
                        sendPacket.writeStringNoNull(parameterValues[i]);
                    }
                }
            }

            sendPacket.writeBytesNoNull(
                    staticSqlStrings[parameterValues.length]);
        } catch (java.io.UnsupportedEncodingException UE) {
            throw new SQLException("Unsupported character encoding '" + 
                                   Encoding + "'");
        }

        if (results != null) {
            results.close();
        }

        // We need to execute this all together
        // So synchronize on the Connection's mutex (because
        // even queries going through there synchronize
        // on the same mutex.
        synchronized (connection.getMutex()) {

            String OldCatalog = null;

            if (!connection.getCatalog().equals(currentCatalog)) {
                OldCatalog = connection.getCatalog();
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
                    results = connection.execSQL((String)null, maxRows, sendPacket, resultSetType, createStreamingResultSet());
                } else {

                    if (maxRows <= 0) {
                        connection.execSQL("SET OPTION SQL_SELECT_LIMIT=DEFAULT", 
                                      -1);
                    } else {
                        connection.execSQL(
                                "SET OPTION SQL_SELECT_LIMIT=" + maxRows, -1);
                    }

                    results = connection.execSQL(null, -1, sendPacket, 
                                             resultSetType, createStreamingResultSet());

                    if (OldCatalog != null) {
                        connection.setCatalog(OldCatalog);
                    }
                }
            } else {
                results = connection.execSQL(null, -1, sendPacket, resultSetType, createStreamingResultSet());
            }

            if (OldCatalog != null) {
                connection.setCatalog(OldCatalog);
            }
        }

        lastInsertId = results.getUpdateID();
        nextResults  = results;
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

        return (java.sql.ResultSet)results;
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
    public synchronized int executeUpdate()
                                   throws java.sql.SQLException
    {

        return executeUpdate(parameterValues, parameterStreams, isStream, 
                             isNull);
    }

    public String toString()
    {

        String Encoding = null;

        if (connection != null && connection.useUnicode()) {
            Encoding = connection.getEncoding();
        }

        StringBuffer SB = new StringBuffer();
        SB.append(super.toString());
        SB.append(": ");

        try {

            for (int i = 0; i < parameterValues.length; ++i) {

                if (Encoding != null) {
                    SB.append(new String(staticSqlStrings[i], Encoding));
                } else {
                    SB.append(new String(staticSqlStrings[i]));
                }

                if (parameterValues[i] == null && !isStream[i]) {
                    SB.append("** NOT SPECIFIED **");
                } else if (isStream[i]) {
                    SB.append("** STREAM DATA **");
                } else {

                    if (doEscapeProcessing) {

                        try {
                            parameterValues[i] = escaper.escapeSQL(
                                                          parameterValues[i]);
                        } catch (SQLException SQE) {
                        }
                    }

                    if (Encoding != null) {
                        SB.append(new String(parameterValues[i].getBytes(), 
                                             Encoding));
                    } else {
                        SB.append(new String(parameterValues[i].getBytes()));
                    }
                }
            }

            if (Encoding != null) {
                SB.append(new String(staticSqlStrings[parameterValues.length], 
                                     Encoding));
            } else {
                SB.append(staticSqlStrings[parameterValues.length]);
            }
        } catch (java.io.UnsupportedEncodingException UE) {
            SB.append("\n\n** WARNING **\n\n Unsupported character encoding '");
            SB.append(Encoding);
            SB.append("'");
        }

        return SB.toString();
    }

    /**
     * Added to allow batch-updates
     */
    protected synchronized int executeUpdate(String[] batchedParameterStrings, 
                                             InputStream[] batchedParameterStreams, 
                                             boolean[] batchedIsStream, 
                                             boolean[] batchedIsNull)
                                      throws java.sql.SQLException
    {
        checkClosed();

        if (sendPacket == null) {
            sendPacket = new Buffer(connection.getNetBufferLength(), 
                                     connection.getMaxAllowedPacket());
        } else {
            sendPacket.clear();
        }

        sendPacket.writeByte((byte)MysqlDefs.QUERY);

        String Encoding = null;

        if (connection.useUnicode()) {
            Encoding = connection.getEncoding();
        }

        try {

            for (int i = 0; i < batchedParameterStrings.length; i++) {

                if (batchedParameterStrings[i] == null && 
                    batchedParameterStreams[i] == null) {
                    throw new java.sql.SQLException("No value specified for parameter " + 
                                                    (i + 1), "07001");
                }

                //if (Encoding != null) {
                sendPacket.writeBytesNoNull(staticSqlStrings[i]);

                //}
                //else {
                //   _SendPacket.writeStringNoNull(_TemplateStrings[i]);
                //}
                if (batchedIsStream[i]) {
                    sendPacket.writeBytesNoNull(streamToBytes(
                                                         batchedParameterStreams[i]));
                } else {

                    if (Encoding != null) {
                        sendPacket.writeStringNoNull(batchedParameterStrings[i], 
                                                      Encoding);
                    } else {
                        sendPacket.writeStringNoNull(batchedParameterStrings[i]);
                    }
                }
            }

            //if (Encoding != null) {
            sendPacket.writeBytesNoNull(
                    staticSqlStrings[batchedParameterStrings.length]);

            // }
            // else {
            //   _SendPacket.writeStringNoNull(_TemplateStrings[_ParameterStrings.length]);
            //}
        } catch (java.io.UnsupportedEncodingException UE) {
            throw new SQLException("Unsupported character encoding '" + 
                                   Encoding + "'");
        }

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
                connection.execSQL("SET OPTION SQL_SELECT_LIMIT=DEFAULT", -1);
            }

            rs = connection.execSQL(null, -1, sendPacket, resultSetType, false);

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
                truncatedUpdateCount = (int)updateCount;
            }

            lastInsertId = rs.getUpdateID();

            return truncatedUpdateCount;
        }
    }

    //
    // Adds '+' to decimal numbers that are positive (MySQL doesn't
    // understand them otherwise
    //
    protected final static String fixDecimalExponent(String dString)
    {

        int ePos = dString.indexOf("E");

        if (ePos == -1) {
            ePos = dString.indexOf("e");
        }

        if (ePos != -1) {

            if (dString.length() > ePos + 1) {

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

    /**
     * For the setXXXStream() methods. Basically converts an
     * InputStream into a String. Not very efficient, but it
     * works.
     *
     */
    protected final byte[] streamToBytes(InputStream in)
                                  throws java.sql.SQLException
    {

        return streamToBytes(in, true);
    }

    protected final byte[] streamToBytes(InputStream in, boolean escape)
                                  throws java.sql.SQLException
    {

        try {

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            int                   bc = readblock(in, streamConvertBuf);

            if (escape) {
                bytesOut.write('\'');
            }

            while (bc > 0) {

                if (escape) {
                    escapeblock(streamConvertBuf, bytesOut, bc);
                } else {
                    bytesOut.write(streamConvertBuf, 0, bc);
                }

                bc = readblock(in, streamConvertBuf);
            }

            if (escape) {
                bytesOut.write('\'');
            }

            return bytesOut.toByteArray();
        } finally {

            try {
                in.close();
            } catch (IOException ioEx) {
            }

            in = null;
        }
    }

    byte[] getBytes(int parameterIndex)
             throws SQLException
    {

        if (isStream[parameterIndex]) {

            return streamToBytes(parameterStreams[parameterIndex], false);
        } else {

            String encoding = null;

            if (connection.useUnicode()) {
                encoding = connection.getEncoding();
            }

            if (encoding != null) {

                try {

                    String stringVal = parameterValues[parameterIndex];

                    if (stringVal.startsWith("'") && 
                        stringVal.endsWith("'")) {
                        stringVal = stringVal.substring(1, 
                                                        stringVal.length() - 1);
                    }

                    return stringVal.getBytes(encoding);
                } catch (java.io.UnsupportedEncodingException uee) {
                    throw new SQLException("Unsupported character encoding '" + 
                                           encoding + "'");
                }
            } else {

                String stringVal = parameterValues[parameterIndex];

                if (stringVal.startsWith("'") && stringVal.endsWith("'")) {
                    stringVal = stringVal.substring(1, stringVal.length() - 1);
                }

                return stringVal.getBytes();
            }
        }
    }

    boolean isNull(int paramIndex)
    {

        return isNull[paramIndex];
    }

    /**
     * Sets the concurrency for result sets generated by this statement
     */
    void setResultSetConcurrency(int concurrencyFlag)
    {
        resultSetConcurrency = concurrencyFlag;
    }

    /**
     * Sets the result set type for result sets generated by this statement
     */
    void setResultSetType(int typeFlag)
    {
        resultSetType = typeFlag;
    }

    private final String getDateTimePattern(String dt, boolean toTime)
                                     throws Exception
    {

        //
        // Special case
        //
        int dtLength = dt != null ? dt.length() : 0;

        if (dtLength >= 8 && dtLength <= 10) {

            int     dashCount  = 0;
            boolean isDateOnly = true;

            for (int i = 0; i < dtLength; i++) {

                char c = dt.charAt(i);

                if (!Character.isDigit(c) && c != '-') {
                    isDateOnly = false;

                    break;
                }

                if (c == '-') {
                    dashCount++;
                }
            }

            if (isDateOnly && dashCount == 2) {

                return "yyyy-MM-dd";
            }
        }

        //
        // Special case - time-only
        //
        boolean colonsOnly = true;

        for (int i = 0; i < dtLength; i++) {

            char c = dt.charAt(i);

            if (!Character.isDigit(c) && c != ':') {
                colonsOnly = false;

                break;
            }
        }

        if (colonsOnly) {

            return "HH:mm:ss";
        }

        int          n;
        int          z;
        int          count;
        int          maxvecs;
        char         c;
        char         separator;
        StringReader reader         = new StringReader(dt + " ");
        ArrayList    vec            = new ArrayList();
        ArrayList    vec_removelist = new ArrayList();
        Object[]     nv             = new Object[3];
        Object[]     v;
        nv[0] = new Character('y');
        nv[1] = new StringBuffer();
        nv[2] = new Integer(0);
        vec.add(nv);

        if (toTime) {
            nv    = new Object[3];
            nv[0] = new Character('h');
            nv[1] = new StringBuffer();
            nv[2] = new Integer(0);
            vec.add(nv);
        }

        while ((z = reader.read()) != -1) {
            separator = (char)z;
            maxvecs   = vec.size();

            for (count = 0; count < maxvecs; count++) {
                v = (Object[])vec.get(count);
                n = ((Integer)v[2]).intValue();
                c = getSuccessor(((Character)v[0]).charValue(), n);

                if (!Character.isLetterOrDigit(separator)) {

                    if ((c == ((Character)v[0]).charValue()) && (c != 'S')) {
                        vec_removelist.add(v);
                    } else {
                        ((StringBuffer)v[1]).append(separator);

                        if (c == 'X' || c == 'Y') {
                            v[2] = new Integer(4);
                        }
                    }
                } else {

                    if (c == 'X') {
                        c     = 'y';
                        nv    = new Object[3];
                        nv[1] = (new StringBuffer(((StringBuffer)v[1]).toString())).append(
                                        'M');
                        nv[0] = new Character('M');
                        nv[2] = new Integer(1);
                        vec.add(nv);
                    } else if (c == 'Y') {
                        c     = 'M';
                        nv    = new Object[3];
                        nv[1] = (new StringBuffer(((StringBuffer)v[1]).toString())).append(
                                        'd');
                        nv[0] = new Character('d');
                        nv[2] = new Integer(1);
                        vec.add(nv);
                    }

                    ((StringBuffer)v[1]).append(c);

                    if (c == ((Character)v[0]).charValue()) {
                        v[2] = new Integer(n + 1);
                    } else {
                        v[0] = new Character(c);
                        v[2] = new Integer(1);
                    }
                }
            }

            int size = vec_removelist.size();

            for (int i = 0; i < size; i++) {
                v = (Object[])vec_removelist.get(i);
                vec.remove(v);
            }

            vec_removelist.clear();
        }

        int size = vec.size();

        for (int i = 0; i < size; i++) {
            v = (Object[])vec.get(i);
            c = ((Character)v[0]).charValue();
            n = ((Integer)v[2]).intValue();

            boolean bk = getSuccessor(c, n) != c;
            boolean atEnd          = ((c == 's' || c == 'm' || 
                                         (c == 'h' && toTime)) && bk);
            boolean finishesAtDate = (bk && (c == 'd') && !toTime);
            boolean containsEnd    = (((StringBuffer)v[1]).toString().indexOf(
                                              'W') != -1);

            if ((!atEnd && !finishesAtDate) || (containsEnd)) {
                vec_removelist.add(v);
            }
        }

        size = vec_removelist.size();

        for (int i = 0; i < size; i++)
            vec.remove(vec_removelist.get(i));

        vec_removelist.clear();
        v = (Object[])vec.get(0); //might throw exception

        StringBuffer format = (StringBuffer)v[1];
        format.setLength(format.length() - 1);

        return format.toString();
    }

    private final void setInternal(int paramIndex, String val)
                            throws java.sql.SQLException
    {

        if (paramIndex < 1 || paramIndex > staticSqlStrings.length) {
            throw new java.sql.SQLException("Parameter index out of range (" + 
                                            paramIndex + " > " + 
                                            staticSqlStrings.length + ").", 
                                            "S1009");
        }

        isStream[paramIndex - 1]         = false;
        isNull[paramIndex - 1]           = false;
        parameterStreams[paramIndex - 1] = null;
        parameterValues[paramIndex - 1]  = val;
    }

    private final char getSuccessor(char c, int n)
    {

        return (c == 'y' && n == 2)
                   ? 'X'
                   : ((c == 'y' && n < 4) //Md
                          ? 'y'
                          : ((c == 'y')
                                 ? 'M'
                                 : ((c == 'M' && n == 2)
                                        ? 'Y'
                                        : ((c == 'M' && n < 3)
                                               ? 'M'
                                               : ((c == 'M')
                                                      ? 'd'
                                                      : ((c == 'd' && 
                                                                                n < 2)
                                                             ? 'd'
                                                             : ((c == 'd')
                                                                    ? 'H'
                                                                    : ((c == 'H' && 
                                                                                                      n < 2)
                                                                           ? 'H'
                                                                           : ((c == 'H')
                                                                                  ? 'm'
                                                                                  : ((c == 'm' && 
                                                                                                                            n < 2)
                                                                                         ? 'm'
                                                                                         : ((c == 'm')
                                                                                                ? 's'
                                                                                                : ((c == 's' && 
                                                                                                                                                  n < 2)
                                                                                                       ? 's'
                                                                                                       : 'W'))))))))))));
    }

    private final void escapeblock(byte[] buf, ByteArrayOutputStream bytesOut, 
                                   int size)
    {

        byte[] out    = new byte[buf.length * 2];
        int    iIndex = 0;

        for (int i = 0; i < size; i++) {

            byte b = buf[i];

            switch (b) {

                case 0: /* Must be escaped for 'mysql' */
                    out[iIndex++] = (byte)'\\';
                    out[iIndex++] = (byte)'0';

                    break;

                case '\n': /* Must be escaped for logs */
                    out[iIndex++] = (byte)'\\';
                    out[iIndex++] = (byte)'n';

                    break;

                case '\r':
                    out[iIndex++] = (byte)'\\';
                    out[iIndex++] = (byte)'r';

                    break;

                case '\\':
                    out[iIndex++] = (byte)'\\';
                    out[iIndex++] = (byte)'\\';

                    break;

                case '\'':
                    out[iIndex++] = (byte)'\\';
                    out[iIndex++] = (byte)'\'';

                    break;

                case '"': /* Better safe than sorry */
                    out[iIndex++] = (byte)'\\';
                    out[iIndex++] = (byte)'"';

                    break;

                case '\032': /* This gives problems on Win32 */
                    out[iIndex++] = (byte)'\\';
                    out[iIndex++] = (byte)'Z';

                    break;

                default:
                    out[iIndex++] = b;
            }
        }

        bytesOut.write(out, 0, iIndex);
    }

    private final int readblock(InputStream i, byte[] b)
                         throws java.sql.SQLException
    {

        try {

            return i.read(b);
        } catch (Throwable E) {
            throw new java.sql.SQLException("Error reading from InputStream " + 
                                            E.getClass().getName(), "S1000");
        }
    }

    //~ Inner classes .........................................................

    class BatchParams
    {

        boolean[]     isNull           = null;
        boolean[]     isStream         = null;
        InputStream[] parameterStreams = null;
        String[]      parameterStrings = null;

        BatchParams(String[] strings, InputStream[] streams, 
                    boolean[] isStreamFlags, boolean[] isNullFlags)
        {

            //
            // Make copies
            //
            parameterStrings = new String[strings.length];
            parameterStreams = new InputStream[streams.length];
            isStream         = new boolean[isStreamFlags.length];
            isNull           = new boolean[isNullFlags.length];
            System.arraycopy(strings, 0, parameterStrings, 0, strings.length);
            System.arraycopy(streams, 0, parameterStreams, 0, streams.length);
            System.arraycopy(isStreamFlags, 0, isStream, 0, 
                             isStreamFlags.length);
            System.arraycopy(isNullFlags, 0, isNull, 0, isNullFlags.length);
        }
    }

    class EndPoint
    {

        int begin;
        int end;

        EndPoint(int b, int e)
        {
            begin = b;
            end   = e;
        }
    }
	/**
	 * @see PreparedStatement#getParameterMetaData()
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException {
		throw new NotImplemented();
	}

	/**
	 * @see PreparedStatement#setURL(int, URL)
	 */
	
	public void setURL(int parameterIndex, URL arg) throws SQLException {
		if (arg != null)
		{
			setString(parameterIndex, arg.toString());
		}
		else
		{
			setNull(parameterIndex, Types.CHAR);
		}
	}
}