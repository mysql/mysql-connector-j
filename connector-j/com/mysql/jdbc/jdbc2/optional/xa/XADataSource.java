package com.mysql.jdbc.jdbc2.optional.xa;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.*;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;


/**
 * @author mmatthew
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public class XADataSource
    extends MysqlConnectionPoolDataSource
    implements javax.sql.XADataSource {

    /**
     * @see javax.sql.XADataSource#getXAConnection()
     */
    public XAConnection getXAConnection()
                                 throws SQLException {

        Connection conn = getConnection();

        return wrapConnection(conn);
    }

    /**
     * @see javax.sql.XADataSource#getXAConnection(String, String)
     */
    public XAConnection getXAConnection(String user, String password)
                                 throws SQLException {

        Connection conn = getConnection(user, password);

        return wrapConnection(conn);
    }

    /**
     * Wraps a connection as a 'fake' XAConnection
     */
    
    private XAConnection wrapConnection(Connection conn)
                                 throws SQLException {

        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {

            // ignore
        }

        MysqlXAResource res = new MysqlXAResource(conn);
        MysqlXAConnectionWrapper xacon = new MysqlXAConnectionWrapper(conn, 
                                                                      res);
        res.setXAConnection(xacon);

        return xacon;
    }
}