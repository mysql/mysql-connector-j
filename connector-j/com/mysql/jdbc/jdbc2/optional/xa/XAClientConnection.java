package com.mysql.jdbc.jdbc2.optional.xa;
/*
 * Licensed under the X license (see http://www.x.org/terms.htm)
 */


import java.sql.Connection;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;


/**
 * Wrapper for database connections used by an XAConnection.  When close is
 * called, it does not close the underlying connection, just informs the
 * XAConnection that close was called.  The connection will not be closed (or
 * returned to the pool) until the transactional details are taken care of.
 * This instance only lives as long as one client is using it - though we
 * probably want to consider reusing it to save object allocations.
 *
 * @author Aaron Mulder (ammulder@alumni.princeton.edu)
 */
public class XAClientConnection implements Connection {
    private final static String CLOSED = "Connection has been closed!";

    private Connection con;
    private Vector listeners;
    private MysqlXAConnectionWrapper xaCon;


    /**
     * Creates a new connection wrapper.
     * @param xaCon The handler for all the transactional details.
     * @param con The "real" database connection to wrap.
     */
    public XAClientConnection(MysqlXAConnectionWrapper xaCon, Connection con) {
        this.con = con;
        this.xaCon = xaCon;
        
        listeners = new Vector();
    }

  

    

    /**
     * Gets a reference to the "real" connection.  This should only be used if
     * you need to cast that to a specific type to call a proprietary method -
     * you will defeat all the pooling if you use the underlying connection
     * directly.
     */
    public Connection getUnderlyingConnection() {
        return this.con;
    }

    /**
     * Closes this connection wrapper permanently.  All further calls with throw
     * a SQLException.
     */
    public void shutdown() {
        this.con = null;
        this.listeners = null;
        this.xaCon = null;
    }

    /**
     * Indicates that an error occured on this connection.
     */
    public void setError(SQLException e) {
        this.xaCon.setConnectionError(e);
    }



    // ---- Implementation of java.sql.Connection ----
    public Statement createStatement() throws SQLException {
        if(con == null) throw new SQLException(CLOSED);
        try {
            Statement st = con.createStatement();
     
            return st;
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.prepareStatement(sql);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.prepareCall(sql);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public String nativeSQL(String sql) throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.nativeSQL(sql);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        if(((MysqlXAResource)xaCon.getXAResource()).isTransaction() && autoCommit)
            throw new SQLException("Cannot set AutoCommit for a transactional connection: See JDBC 2.0 Optional Package Specification section 7.1 (p25)");

        try {
            this.con.setAutoCommit(autoCommit);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }

    }

    public boolean getAutoCommit() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.getAutoCommit();
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public void commit() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        if(((MysqlXAResource)xaCon.getXAResource()).isTransaction())
            throw new SQLException("Cannot commit a transactional connection: See JDBC 2.0 Optional Package Specification section 7.1 (p25)");
        try {
            this.con.commit();
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public void rollback() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        if(((MysqlXAResource)xaCon.getXAResource()).isTransaction())
            throw new SQLException("Cannot rollback a transactional connection: See JDBC 2.0 Optional Package Specification section 7.1 (p25)");
    }

    public void close() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
     
        xaCon.clientConnectionClosed();
        shutdown();
    }

    public boolean isClosed() throws SQLException {
        if(this.con == null) return true;
        try {
            return this.con.isClosed();
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.getMetaData();
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            this.con.setReadOnly(readOnly);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public boolean isReadOnly() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return con.isReadOnly();
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public void setCatalog(String catalog) throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            this.con.setCatalog(catalog);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public String getCatalog() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.getCatalog();
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            this.con.setTransactionIsolation(level);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public int getTransactionIsolation() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.getTransactionIsolation();
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.getWarnings();
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public void clearWarnings() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            this.con.clearWarnings();
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.createStatement(resultSetType, resultSetConcurrency);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if(con == null) throw new SQLException(CLOSED);
        try {
            return this.con.prepareStatement(sql, resultSetType, resultSetConcurrency);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.prepareCall(sql, resultSetType, resultSetConcurrency);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public Map getTypeMap() throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            return this.con.getTypeMap();
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }

    public void setTypeMap(Map map) throws SQLException {
        if(this.con == null) throw new SQLException(CLOSED);
        try {
            this.con.setTypeMap(map);
        } catch(SQLException e) {
            setError(e);
            throw e;
        }
    }
	/**
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public Statement createStatement(
		int resultSetType,
		int resultSetConcurrency,
		int resultSetHoldability)
		throws SQLException {
		return null;
	}

	/**
	 * @see java.sql.Connection#getHoldability()
	 */
	public int getHoldability() throws SQLException {
		return 0;
	}

	/**
	 * @see java.sql.Connection#prepareCall(String, int, int, int)
	 */
	public CallableStatement prepareCall(
		String sql,
		int resultSetType,
		int resultSetConcurrency,
		int resultSetHoldability)
		throws SQLException {
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(String, int, int, int)
	 */
	public PreparedStatement prepareStatement(
		String sql,
		int resultSetType,
		int resultSetConcurrency,
		int resultSetHoldability)
		throws SQLException {
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(String, int)
	 */
	public PreparedStatement prepareStatement(
		String sql,
		int autoGeneratedKeys)
		throws SQLException {
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(String, int[])
	 */
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
		throws SQLException {
		return null;
	}

	/**
	 * @see java.sql.Connection#prepareStatement(String, String[])
	 */
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
		throws SQLException {
		return null;
	}

	/**
	 * @see java.sql.Connection#releaseSavepoint(Savepoint)
	 */
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
	}

	/**
	 * @see java.sql.Connection#rollback(Savepoint)
	 */
	public void rollback(Savepoint savepoint) throws SQLException {
	}

	/**
	 * @see java.sql.Connection#setHoldability(int)
	 */
	public void setHoldability(int holdability) throws SQLException {
	}

	/**
	 * @see java.sql.Connection#setSavepoint()
	 */
	public Savepoint setSavepoint() throws SQLException {
		return null;
	}

	/**
	 * @see java.sql.Connection#setSavepoint(String)
	 */
	public Savepoint setSavepoint(String name) throws SQLException {
		return null;
	}

}


