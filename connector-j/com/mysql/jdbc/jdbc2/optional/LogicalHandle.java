/**
 * This class serves as a wrapper for the org.gjt.mm.mysql.jdbc2.Connection class.  
 * It is returned to the application server which may wrap it again and then return
 * it to the application client in response to dataSource.getConnection().  
 *
 * <p>All method invocations are forwarded to org.gjt.mm.mysql.jdbc2.Connection 
 * unless the close method was previously called, in which case a sqlException is 
 * thrown.  The close method performs a 'logical close' on the connection.  
 *
 * <p>All sqlExceptions thrown by the physical connection are intercepted and sent to 
 * connectionEvent listeners before being thrown to client.
 *
 * @see org.gjt.mm.mysql.jdbc2.Connection
 * @see org.gjt.mm.mysql.jdbc2.optional.MysqlPooledConnection
 * @author Todd Wolff <todd.wolff@prodigy.net>
 */

package com.mysql.jdbc.jdbc2.optional;

import java.sql.*;
import com.mysql.jdbc.jdbc2.optional.MysqlPooledConnection;

public class LogicalHandle implements Connection
{
    private boolean closed;
    private MysqlPooledConnection mpc = null;
    private Connection mc = null;
    private String invalidHandleStr = "Logical handle no longer valid";
    
    /**
    * Construct a new LogicalHandle and set instance variables
    *
    * @param mysqlPooledConnection reference to object that instantiated this object
    * @param mysqlConnection physical connection to db
    * @exception java.sql.SQLException
    */
   
    public LogicalHandle(MysqlPooledConnection mysqlPooledConnection, Connection mysqlConnection)
        throws SQLException {
        mpc = mysqlPooledConnection;
        mc = mysqlConnection;
        closed = false;
    }
    
    /**
     * Allows clients to determine how long this connection
     * has been idle.
     */
    
    public long getIdleFor()
    {
    	return ((com.mysql.jdbc.Connection)mc).getIdleFor();
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
   
    public void clearWarnings() throws java.sql.SQLException  {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                mc.clearWarnings();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * The physical connection is not actually closed.  the physical connection is closed
    * when the application server calls mysqlPooledConnection.close().  this object is 
    * de-referenced by the pooled connection each time mysqlPooledConnection.getConnection() 
    * is called by app server.  
    * 
    * @exception java.sql.SQLException
    */
     
    public synchronized void close()
        throws SQLException {
        if(closed) {
            return;
        }
        mpc.callListener(1, null);
        // set closed status to true so that if application client tries to make additional
        // calls a sqlException will be thrown.  The physical connection is
        // re-used by the pooled connection each time getConnection is called. 
        closed = true;
    }

    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
   
    public void commit() throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                mc.commit();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public java.sql.Statement createStatement() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.createStatement();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency)
    throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.createStatement(resultSetType, resultSetConcurrency);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public boolean getAutoCommit() throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.getAutoCommit();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public String getCatalog() throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.getCatalog();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public java.sql.DatabaseMetaData getMetaData() throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.getMetaData();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public int getTransactionIsolation() throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.getTransactionIsolation();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public java.util.Map getTypeMap() throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.getTypeMap();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public java.sql.SQLWarning getWarnings() throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.getWarnings();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public boolean isClosed() throws java.sql.SQLException {
        return (closed || mc.isClosed());   
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public boolean isReadOnly() throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.isReadOnly();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public String nativeSQL(String sql) throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.nativeSQL(sql);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public java.sql.CallableStatement prepareCall(String sql) 
    throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.prepareCall(sql);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public java.sql.CallableStatement prepareCall(String sql, 
                                                  int resultSetType,
                                                  int resultSetConcurrency) 
    throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.prepareCall(sql, resultSetType, resultSetConcurrency);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public java.sql.PreparedStatement prepareStatement(String sql) 
    throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.prepareStatement(sql);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public java.sql.PreparedStatement prepareStatement(String sql, 
                                                       int resultSetType,
                                                       int resultSetConcurrency) 
    throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                return mc.prepareStatement(sql, resultSetType, resultSetConcurrency);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public void rollback() throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                mc.rollback();
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public void setAutoCommit(boolean autoCommit) throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                mc.setAutoCommit(autoCommit);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public void setCatalog(String catalog) throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                mc.setCatalog(catalog);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public void setReadOnly(boolean readOnly) throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                mc.setReadOnly(readOnly);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public void setTransactionIsolation(int level) throws java.sql.SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                mc.setTransactionIsolation(level);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
    
    /**
    * Passes call to method on physical connection instance.  Notifies listeners of
    * any caught exceptions before re-throwing to client.
    * 
    * @exception java.sql.SQLException
    */
    
    public void setTypeMap(java.util.Map map) throws SQLException {
        if (closed) {
            throw new SQLException(invalidHandleStr);
        } else {    
            try {
                mc.setTypeMap(map);
            } catch (SQLException sqlException) {
                mpc.callListener(2, sqlException);
                throw sqlException;
            }       
        }    
    }
}
