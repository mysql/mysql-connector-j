/*
 * MM JDBC Drivers for MySQL
 *
 * $Id: Connection.java,v 1.3 2002/04/21 03:03:46 mark_matthews Exp $
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
 * @author Mark Matthews <mmatthew@worldserver.com>
 * @version $Id: Connection.java,v 1.3 2002/04/21 03:03:46 mark_matthews Exp $
 */

package com.mysql.jdbc.jdbc2;

import java.io.UnsupportedEncodingException;

import java.sql.*;
import java.util.Properties;

public class Connection extends com.mysql.jdbc.Connection implements java.sql.Connection
{
  
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

    public void connectionInit(String host, 
			       int port, 
			       Properties info, 
			       String database, 
			       String url, 
			       com.mysql.jdbc.Driver d) 
	throws java.sql.SQLException
    {
	super.connectionInit(host, port, info, database, url, d);
    }
  
  
	public java.sql.CallableStatement prepareCall(String sql) throws java.sql.SQLException
  	{
      
      if (_useUltraDevWorkAround) {
          return new UltraDevWorkAround(prepareStatement(sql));
      }
      else {
          throw new java.sql.SQLException("Callable statments not supported.", "S1C00"); 
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
    
    public java.sql.DatabaseMetaData getMetaData() throws java.sql.SQLException
    {
	return new DatabaseMetaData(this, _database);
    }


    protected com.mysql.jdbc.MysqlIO createNewIO(String host, int port) throws Exception
    {
	return new IO(host, port, this);
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

    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException
    {
        Statement stmt = new com.mysql.jdbc.jdbc2.Statement(this, _database);
        
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
    
    public java.sql.Statement createStatement() throws SQLException
    {
        return createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
                               java.sql.ResultSet.CONCUR_READ_ONLY);
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
	return prepareStatement(sql, java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, 
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
        
        PreparedStatement pStmt = new com.mysql.jdbc.jdbc2.PreparedStatement(this, 
						    sql, 
						    _database);

        pStmt.setResultSetType(resultSetType);
        pStmt.setResultSetConcurrency(resultSetConcurrency);
        
	return pStmt;
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
     * JDBC 2.0
     *
     * Get the type-map object associated with this connection.
     * By default, the map returned is empty.
     */

    public java.util.Map getTypeMap() throws SQLException
    {
	throw new NotImplemented();
    }

    /**
     * JDBC 2.0
     *
     * Install a type-map object as the default type-map for
     * this connection
     */

    public void setTypeMap(java.util.Map map) throws SQLException
    {
	throw new NotImplemented();
    }
    
      /**
     * Wrapper class for UltraDev CallableStatements that 
     * are really PreparedStatments.
     *
     * Nice going, macromedia!
     */
    
    class UltraDevWorkAround implements java.sql.CallableStatement {
        java.sql.PreparedStatement delegate = null;

        UltraDevWorkAround(java.sql.PreparedStatement pstmt) {
            delegate = pstmt;
        }
        
        public void addBatch() throws java.sql.SQLException {
            delegate.addBatch();
        }
        
        public void addBatch(java.lang.String p1) throws java.sql.SQLException {
            delegate.addBatch(p1);
        }
        
        public void cancel() throws java.sql.SQLException {
            delegate.cancel();
        }
        
        public void clearBatch() throws java.sql.SQLException {
            delegate.clearBatch();
        }
        
        public void clearParameters() throws java.sql.SQLException {
            delegate.clearParameters();
        }
        
        public void clearWarnings() throws java.sql.SQLException {
            delegate.clearWarnings();
        }
        
        public void close() throws java.sql.SQLException {
            delegate.close();
        }
        
        public boolean execute() throws java.sql.SQLException {
            return delegate.execute();
        }
        
        public boolean execute(java.lang.String p1) throws java.sql.SQLException {
            return delegate.execute(p1);
        }
        
        public int[] executeBatch() throws java.sql.SQLException {
            return delegate.executeBatch();
        }
        
        public java.sql.ResultSet executeQuery() throws java.sql.SQLException {
            return delegate.executeQuery();
        }
        
        public java.sql.ResultSet executeQuery(java.lang.String p1) throws java.sql.SQLException {
            return delegate.executeQuery(p1);
        }
        
        public int executeUpdate() throws java.sql.SQLException {
            return delegate.executeUpdate();
        }
        
        public int executeUpdate(java.lang.String p1) throws java.sql.SQLException {
            return delegate.executeUpdate(p1);
        }
        
        public java.sql.Array getArray(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.math.BigDecimal getBigDecimal(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.math.BigDecimal getBigDecimal(int p1,int p2) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.sql.Blob getBlob(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public boolean getBoolean(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public byte getByte(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public byte[] getBytes(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.sql.Clob getClob(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
            
        }
        
        public java.sql.Connection getConnection() throws java.sql.SQLException {
            return delegate.getConnection();
        }
        
        public java.sql.Date getDate(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.sql.Date getDate(int p1,final java.util.Calendar p2) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public double getDouble(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public int getFetchDirection() throws java.sql.SQLException {
            return delegate.getFetchDirection();
        }
        
        public int getFetchSize() throws java.sql.SQLException {
            return delegate.getFetchSize();
        }
        
        public float getFloat(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public int getInt(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public long getLong(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public int getMaxFieldSize() throws java.sql.SQLException {
            return delegate.getMaxFieldSize();
        }
        
        public int getMaxRows() throws java.sql.SQLException {
            return delegate.getMaxRows();
        }
        
        public java.sql.ResultSetMetaData getMetaData() throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public boolean getMoreResults() throws java.sql.SQLException {
            return delegate.getMoreResults();
        }
        
        public java.lang.Object getObject(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.lang.Object getObject(int p1,final java.util.Map p2) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public int getQueryTimeout() throws java.sql.SQLException {
            return delegate.getQueryTimeout();
        }
        
        public java.sql.Ref getRef(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.sql.ResultSet getResultSet() throws java.sql.SQLException {
            return delegate.getResultSet();
        }
        
        public int getResultSetConcurrency() throws java.sql.SQLException {
            return delegate.getResultSetConcurrency();
        }
        
        public int getResultSetType() throws java.sql.SQLException {
            return delegate.getResultSetType();
        }
        
        public short getShort(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.lang.String getString(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.sql.Time getTime(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.sql.Time getTime(int p1,final java.util.Calendar p2) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.sql.Timestamp getTimestamp(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public java.sql.Timestamp getTimestamp(int p1,final java.util.Calendar p2) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public int getUpdateCount() throws java.sql.SQLException {
            return delegate.getUpdateCount();
        }
        
        public java.sql.SQLWarning getWarnings() throws java.sql.SQLException {
            return delegate.getWarnings();
        }
        
        public void registerOutParameter(int p1,int p2) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public void registerOutParameter(int p1,int p2,int p3) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public void registerOutParameter(int p1,int p2,java.lang.String p3) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public void setArray(int p1,final java.sql.Array p2) throws java.sql.SQLException {
            delegate.setArray(p1, p2);
        }
        
        public void setAsciiStream(int p1,final java.io.InputStream p2,int p3) throws java.sql.SQLException {
            delegate.setAsciiStream(p1, p2, p3);
        }
        
        public void setBigDecimal(int p1,final java.math.BigDecimal p2) throws java.sql.SQLException {
            delegate.setBigDecimal(p1, p2);
        }
        
        public void setBinaryStream(int p1,final java.io.InputStream p2,int p3) throws java.sql.SQLException {
            delegate.setBinaryStream(p1, p2, p3);
        }
        
        public void setBlob(int p1,final java.sql.Blob p2) throws java.sql.SQLException {
            delegate.setBlob(p1, p2);
        }
        
        public void setBoolean(int p1,boolean p2) throws java.sql.SQLException {
            delegate.setBoolean(p1, p2);
        }
        
        public void setByte(int p1,byte p2) throws java.sql.SQLException {
            delegate.setByte(p1, p2);
        }
        
        public void setBytes(int p1,byte[] p2) throws java.sql.SQLException {
            delegate.setBytes(p1, p2);
        }
        
        public void setCharacterStream(int p1,final java.io.Reader p2,int p3) throws java.sql.SQLException {
            delegate.setCharacterStream(p1, p2, p3);
        }
        
        public void setClob(int p1,final java.sql.Clob p2) throws java.sql.SQLException {
            delegate.setClob(p1, p2);
        }
        
        public void setCursorName(java.lang.String p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public void setDate(int p1,final java.sql.Date p2) throws java.sql.SQLException {
            delegate.setDate(p1, p2);
        }
        
        public void setDate(int p1,final java.sql.Date p2,final java.util.Calendar p3) throws java.sql.SQLException {
            delegate.setDate(p1, p2, p3);
        }
        
        public void setDouble(int p1,double p2) throws java.sql.SQLException {
            delegate.setDouble(p1, p2);
        }
        
        public void setEscapeProcessing(boolean p1) throws java.sql.SQLException {
            delegate.setEscapeProcessing(p1);
        }
        
        public void setFetchDirection(int p1) throws java.sql.SQLException {
            delegate.setFetchDirection(p1);
        }
        
        public void setFetchSize(int p1) throws java.sql.SQLException {
            delegate.setFetchSize(p1);
        }
        
        public void setFloat(int p1,float p2) throws java.sql.SQLException {
            delegate.setFloat(p1, p2);
        }
        
        public void setInt(int p1,int p2) throws java.sql.SQLException {
            delegate.setInt(p1, p2);
        }
        
        public void setLong(int p1,long p2) throws java.sql.SQLException {
            delegate.setLong(p1, p2);
        }
        
        public void setMaxFieldSize(int p1) throws java.sql.SQLException {
            delegate.setMaxFieldSize(p1);
        }
        
        public void setMaxRows(int p1) throws java.sql.SQLException {
            delegate.setMaxRows(p1);
        }
        
        public void setNull(int p1,int p2) throws java.sql.SQLException {
            delegate.setNull(p1, p2);
        }
        
        public void setNull(int p1,int p2,java.lang.String p3) throws java.sql.SQLException {
            delegate.setNull(p1, p2, p3);
        }
        
        public void setObject(int p1,final java.lang.Object p2) throws java.sql.SQLException {
            delegate.setObject(p1, p2);
        }
        
        public void setObject(int p1,final java.lang.Object p2,int p3) throws java.sql.SQLException {
            delegate.setObject(p1, p2, p3);
        }
        
        public void setObject(int p1,final java.lang.Object p2,int p3,int p4) throws java.sql.SQLException {
            delegate.setObject(p1, p2, p3, p4);
        }
        
        public void setQueryTimeout(int p1) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public void setRef(int p1,final java.sql.Ref p2) throws java.sql.SQLException {
            throw new SQLException("Not supported");
        }
        
        public void setShort(int p1,short p2) throws java.sql.SQLException {
            delegate.setShort(p1, p2);
        }
        
        public void setString(int p1,java.lang.String p2) throws java.sql.SQLException {
            delegate.setString(p1, p2);
        }
        
        public void setTime(int p1,final java.sql.Time p2) throws java.sql.SQLException {
            delegate.setTime(p1, p2);
        }
        
        public void setTime(int p1,final java.sql.Time p2,final java.util.Calendar p3) throws java.sql.SQLException {
            delegate.setTime(p1, p2, p3);
        }
        
        public void setTimestamp(int p1,final java.sql.Timestamp p2) throws java.sql.SQLException {
            delegate.setTimestamp(p1, p2);
        }
        
        public void setTimestamp(int p1,final java.sql.Timestamp p2,final java.util.Calendar p3) throws java.sql.SQLException {
            delegate.setTimestamp(p1, p2, p3);
        }
        
        public void setUnicodeStream(int p1,final java.io.InputStream p2,int p3) throws java.sql.SQLException {
            delegate.setUnicodeStream(p1, p2, p3);
        }
        
        public boolean wasNull() throws java.sql.SQLException {
           throw new SQLException("Not supported");
        }
        
    }
}
