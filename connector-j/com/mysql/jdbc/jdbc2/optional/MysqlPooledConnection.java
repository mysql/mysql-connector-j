/**
 * This class is used to wrap and return a physical connection within a logical handle.
 * It also registers and notifies ConnectionEventListeners of any ConnectionEvents
 *
 * @see javax.sql.PooledConnection
 * @see org.gjt.mm.mysql.jdbc2.optional.LogicalHandle
 * @author Todd Wolff <todd.wolff@prodigy.net>
 */
 
package com.mysql.jdbc.jdbc2.optional;

import java.sql.*;
import java.util.Enumeration;
import java.util.Hashtable;
import javax.sql.*;
import com.mysql.jdbc.jdbc2.optional.LogicalHandle;

public class MysqlPooledConnection implements PooledConnection {
    
    private Hashtable eventListeners;
    protected Connection logicalHandle;
    protected Connection physicalConn;

    /**
    * Construct a new MysqlPooledConnection and set instance variables
    *
    * @param connection physical connection to db
    */
    
    public MysqlPooledConnection(Connection connection) {
        logicalHandle = null;
        physicalConn = connection;
        eventListeners = new Hashtable(10);
    }

    /**
    * Adds ConnectionEventListeners to a hash table to be used for notification of
    * ConnectionEvents
    *
    * @param connectioneventlistener listener to be notified with ConnectionEvents
    */
    
    public synchronized void addConnectionEventListener(ConnectionEventListener connectioneventlistener) {
        if(eventListeners != null) {
            eventListeners.put(connectioneventlistener, connectioneventlistener);
        }    
    }

    /**
    * Removes ConnectionEventListeners from hash table used for notification of
    * ConnectionEvents
    *
    * @param connectioneventlistener listener to be removed
    */
    
    public synchronized void removeConnectionEventListener(ConnectionEventListener connectioneventlistener) {
        if(eventListeners != null) {
            eventListeners.remove(connectioneventlistener);
        }    
    }

    /**
    * Invoked by the container.  Return a logicalHandle object that wraps a physical 
    * connection.
    *
    * @exception java.sql.SQLException
    */
    
    public synchronized Connection getConnection()
        throws SQLException {
        if(physicalConn == null) {
            SQLException sqlException = new SQLException("Physical Connection doesn't exist");
            callListener(2, sqlException);
            return null;
        }
        try {
            if(logicalHandle != null) {
                logicalHandle.close();
            }
            logicalHandle = new LogicalHandle(this, physicalConn);
        } catch(SQLException sqlException) {
            callListener(2, sqlException);
            return null;
        }
        return logicalHandle;
    }
    
    /**
    * Invoked by the container (not the client), and should close the physical connection.
    * This will be called if the pool is destroyed or the connectionEventListener receives
    * a connectionErrorOccurred event.
    *
    * @exception java.sql.SQLException
    */
    
    public synchronized void close() throws SQLException {
        physicalConn.close();
        physicalConn = null;
        callListener(2, null);
    }
    
    /**
    * Notifies all registered ConnectionEventListeners of ConnectionEvents.  Instantiates
    * a new ConnectionEvent which wraps sqlException and invokes either connectionClose
    * or connectionErrorOccurred on listener as appropriate.
    * 
    * @param i value indicating whether connectionClosed or connectionErrorOccurred called
    * @exception java.sql.SQLException
    */
    
    protected synchronized void callListener(int i, SQLException sqlException) {
        if(eventListeners == null) {
            return;
        }
        Enumeration enumeration = eventListeners.keys();
        ConnectionEvent connectionevent = new ConnectionEvent(this, sqlException);
        while(enumeration.hasMoreElements()) {
            ConnectionEventListener connectioneventlistener = (ConnectionEventListener)enumeration.nextElement();
            ConnectionEventListener connectioneventlistener1 = (ConnectionEventListener)eventListeners.get(connectioneventlistener);
            if(i == 1) {
                connectioneventlistener1.connectionClosed(connectionevent);
            } else if(i == 2) {
                connectioneventlistener1.connectionErrorOccurred(connectionevent);
            }    
        }
    }

}
