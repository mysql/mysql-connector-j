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
package com.mysql.jca;

import java.io.PrintWriter;

import java.util.ArrayList;
import java.util.Iterator;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;
import javax.resource.spi.ResourceAdapterInternalException;

import javax.security.auth.Subject;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;


/**
 * ManagedConnection instance represents a physical connection to the 
 * underlying EIS.
 *
 * A ManagedConnection instance provides access to a pair of 
 * interfaces: javax.transaction.xa.XAResource and 
 * javax.resource.spi.LocalTransaction.
 *
 * XAResource interface is used by the transaction manager to 
 * associate and dissociate a transaction with the underlying 
 * EIS resource manager instance and to perform two-phase 
 * commit protocol. The ManagedConnection interface is not 
 * directly used by the transaction manager. More details on 
 * the XAResource interface are described in the JTA 
 * specification.
 * 
 * The LocalTransaction interface is used by the application 
 * server to manage local transactions. 
 * 
 * @author Mark Matthews
 */
public class MysqlManagedConnection
    implements ManagedConnection,
               XAResource {

    /** List of connection event listeners */
    private ArrayList listeners = new ArrayList();

    /** 
     * ManagedConnectionFactory for this ManagedConnection. It acts
     * as the resource manager (RM) for this connection
     */
    private MysqlManagedConnectionFactory connFact = null;

    /**
     * List of underlying connections that we're managing
     */
    private ArrayList managedConnectionsList = new ArrayList();

    /**
     * Currently-managed connection
     */
    private ConnectionWrapper currentConnection;

    /**
     * Creates a new connection handle for the underlying physical 
     * connection represented by the ManagedConnection instance. 
     * This connection handle is used by the application code to 
     * refer to the underlying physical connection. A connection 
     * handle is tied to its ManagedConnection instance in a 
     * resource adapter implementation specific way.
     *
     * The ManagedConnection uses the Subject and additional 
     * ConnectionRequest Info (which is specific to resource adapter 
     * and opaque to application server) to set the state of the 
     * physical connection.
     * 
     * @param subject Security Subject
     * @param cxRequestInfo ConnectionRequestInfo instance 
     * 
     * @return generic Object instance representing the connection handle. 
     *          For CCI, the connection handle created by a ManagedConnection 
     *          instance is of the type javax.resource.cci.Connection. 
     * 
     * @throws ResourceException generic exception if operation fails 
     * @throws ResourceAdapterInternalException resource adapter internal error condition 
     * @throws SecurityException security related error condition 
     * @throws CommException failed communication with EIS instance 
     * @throws EISSystemException internal error condition in EIS instance - used 
     *                             if EIS instance is involved in setting state of ManagedConnection
     */
    public Object getConnection(Subject subject, 
                                ConnectionRequestInfo cxRequestInfo)
                         throws ResourceException {

        return null;
    }

    /**
     * Destroys the physical connection to the underlying resource manager.
     * 
     * To manage the size of the connection pool, an application server 
     * can explictly call ManagedConnection.destroy to destroy a physical 
     * connection. A resource adapter should destroy all allocated system 
     * resources for this ManagedConnection instance when the method destroy 
     * is called.
     * 
     * @throws ResourceException generic exception if operation failed 
     * @throws IllegalStateException illegal state for destroying connection
     */
    public void destroy()
                 throws ResourceException {
    }

    /**
     * Application server calls this method to force any cleanup on the 
     * ManagedConnection instance. 
     * 
     * The method ManagedConnection.cleanup initiates a cleanup of 
     * any client-specific state as maintained by a ManagedConnection 
     * instance. The cleanup should invalidate all connection handles 
     * that had been created using this ManagedConnection instance. 
     * Any attempt by an application component to use the connection 
     * handle after cleanup of the underlying ManagedConnection should 
     * result in an exception. 
     *
     * The cleanup of ManagedConnection is always driven by an 
     * application server. An application server should not invoke 
     * ManagedConnection.cleanup when there is an uncompleted 
     * transaction (associated with a ManagedConnection instance) in 
     * progress. 
     * 
     * The invocation of ManagedConnection.cleanup method on an already 
     * cleaned-up connection should not throw an exception. 
     * 
     * The cleanup of ManagedConnection instance resets its client 
     * specific state and prepares the connection to be put back in 
     * to a connection pool. The cleanup method should not cause 
     * resource adapter to close the physical pipe and reclaim system 
     * resources associated with the physical connection. 
     * 
     * @throws ResourceException generic exception if operation fails 
     * @throws ResourceAdapterInternalException resource adapter internal error condition 
     * @throws IllegalStateException illegal state for calling connection cleanup. Example - if a localtransaction is in progress that doesn't allow connection cleanup
     */
    public void cleanup()
                 throws ResourceException, ResourceAdapterInternalException, 
                        IllegalStateException {

        for (Iterator i = managedConnectionsList.iterator(); i.hasNext();) {

            // clean up each connection
        }

        managedConnectionsList.clear();
    }

    /**
     * Used by the container to change the association of an 
     * application-level connection handle with a ManagedConneciton 
     * instance. The container should find the right ManagedConnection 
     * instance and call the associateConnection method. 
     * 
     * The resource adapter is required to implement the 
     * associateConnection method. The method implementation for a 
     * ManagedConnection should dissociate the connection handle 
     * (passed as a parameter) from its currently associated 
     * ManagedConnection and associate the new connection handle with 
     * itself. 
     * 
     * @param connection application-level connection handle 
     *  
     * @throws ResourceException failed to associate the connection handle with this ManagedConnection instance 
     * @throws IllegalStateException illegal state for invoking this method 
     * @throws ResourceAdapterInternalException resource adapter internal error condition
     */
    public void associateConnection(Object connection)
                             throws ResourceException {

        try {
            ((ConnectionWrapper) connection).isClosed(); // FIXME
            managedConnectionsList.add(connection);
        } catch (ClassCastException cce) {
            throw new ResourceException("MysqlManagedConnection only manages MySQL Connections. Connection class used was '"
                                        + connection.getClass().getName()
                                        + "'");
        }
    }

    /**
     * Adds a connection event listener to the ManagedConnection instance. 
     *
     * The registered ConnectionEventListener instances are notified of 
     * connection close and error events, also of local transaction 
     * related events on the Managed Connection. 
     *
     * @param listener a new ConnectionEventListener to be registered
     */
    public void addConnectionEventListener(ConnectionEventListener listener) {
        listeners.add(listener);
    }

    /**
     * Removes an already registered connection event listener from the 
     * ManagedConnection instance. 
     *
     * @param listener already registered connection event listener to 
     * be removed
     */
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        listeners.remove(listener);
    }

    /**
     * Returns an javax.transaction.xa.XAresource instance. An 
     * application server enlists this XAResource instance with 
     * the Transaction Manager if the ManagedConnection instance 
     * is being used in a JTA transaction that is being coordinated 
     * by the Transaction Manager. 
     *
     * @return XAResource instance 
     * 
     * @throws ResourceException generic exception if operation fails 
     * @throws NotSupportedException if the operation is not supported 
     * @throws ResourceAdapterInternalException resource adapter internal 
     * error condition
     */
    public XAResource getXAResource()
                             throws ResourceException {

        return this;
    }

    /**
     * Returns an javax.resource.spi.LocalTransaction instance. The 
     * LocalTransaction interface is used by the container to manage 
     * local transactions for a RM instance. 
     * 
     * @return LocalTransaction instance 
     *
     * @throws ResourceException generic exception if operation fails 
     * @throws NotSupportedException if the operation is not supported 
     * @throws ResourceAdapterInternalException resource adapter internal error condition
     */
    public LocalTransaction getLocalTransaction()
                                         throws ResourceException {

        if (currentConnection != null) {

            return new MysqlLocalTransaction(currentConnection, this);
        } else {

            return null;
        }
    }

    /**
     * Gets the metadata information for this connection's underlying 
     * EIS resource manager instance. The ManagedConnectionMetaData 
     * interface provides information about the underlying EIS 
     * instance associated with the ManagedConenction instance. 
     * 
     * @return ManagedConnectionMetaData instance 
     * 
     * @throws ResourceException generic exception if operation fails 
     * @throws NotSupportedException if the operation is not supported
     */
    public ManagedConnectionMetaData getMetaData()
                                          throws ResourceException {

        return null;
    }

    /**
     * Sets the log writer for this ManagedConnection instance. 
     *
     * The log writer is a character output stream to which all logging 
     * and tracing messages for this ManagedConnection instance will be 
     * printed. Application Server manages the association of output 
     * stream with the ManagedConnection instance based on the 
     * connection pooling requirements.
     * 
     * When a ManagedConnection object is initially created, the default 
     * log writer associated with this instance is obtained from the 
     * ManagedConnectionFactory. An application server can set a 
     * log writer specific to this ManagedConnection to log/trace this 
     * instance using setLogWriter method.
     * 
     * @param out Character Output stream to be associated 
     * 
     * @throws ResourceException generic exception if operation fails 
     * @throws ResourceAdapterInternalException resource adapter related error condition
     */
    public void setLogWriter(PrintWriter arg0)
                      throws ResourceException {
    }

    /**
     * Gets the log writer for this ManagedConnection instance. 
     * 
     * The log writer is a character output stream to which all logging 
     * and tracing messages for this ManagedConnection instance will be 
     * printed. ConnectionManager manages the association of output 
     * stream with the ManagedConnection instance based on the 
     * connection pooling requirements.
     * 
     * The Log writer associated with a ManagedConnection instance can 
     * be one set as default from the ManagedConnectionFactory (that 
     * created this connection) or one set specifically for this 
     * instance by the application server.
     * 
     * @return Character ourput stream associated with this ManagedConnection instance 
     * 
     * @throws ResourceException generic exception if operation fails
     */
    public PrintWriter getLogWriter()
                             throws ResourceException {

        return null;
    }

    /**
     * @see javax.transaction.xa.XAResource#commit(Xid, boolean)
     */
    public void commit(Xid arg0, boolean arg1)
                throws XAException {
    }

    /**
     * @see javax.transaction.xa.XAResource#end(Xid, int)
     */
    public void end(Xid arg0, int arg1)
             throws XAException {
    }

    /**
     * @see javax.transaction.xa.XAResource#forget(Xid)
     */
    public void forget(Xid arg0)
                throws XAException {
    }

    /**
     * @see javax.transaction.xa.XAResource#getTransactionTimeout()
     */
    public int getTransactionTimeout()
                              throws XAException {

        return 0;
    }

    /**
     * @see javax.transaction.xa.XAResource#isSameRM(XAResource)
     */
    public boolean isSameRM(XAResource arg0)
                     throws XAException {

        return false;
    }

    /**
     * @see javax.transaction.xa.XAResource#prepare(Xid)
     */
    public int prepare(Xid arg0)
                throws XAException {

        return 0;
    }

    /**
     * @see javax.transaction.xa.XAResource#recover(int)
     */
    public Xid[] recover(int arg0)
                  throws XAException {

        return null;
    }

    /**
     * @see javax.transaction.xa.XAResource#rollback(Xid)
     */
    public void rollback(Xid arg0)
                  throws XAException {
    }

    /**
     * @see javax.transaction.xa.XAResource#setTransactionTimeout(int)
     */
    public boolean setTransactionTimeout(int arg0)
                                  throws XAException {

        return false;
    }

    /**
     * @see javax.transaction.xa.XAResource#start(Xid, int)
     */
    public void start(Xid arg0, int arg1)
               throws XAException {
    }

    protected void notifyTransactionStarted() {

        for (Iterator i = listeners.iterator(); i.hasNext();) {
            ((ConnectionEventListener) i.next()).localTransactionStarted(new ConnectionEvent(
                                                                                 this, 
                                                                                 0));
        }
    }

    protected void notifyTransactionCommitted() {

        for (Iterator i = listeners.iterator(); i.hasNext();) {
            ((ConnectionEventListener) i.next()).localTransactionCommitted(new ConnectionEvent(
                                                                                   this, 
                                                                                   0));
        }
    }

    protected void notifyTransactionRolledBack() {

        for (Iterator i = listeners.iterator(); i.hasNext();) {
            ((ConnectionEventListener) i.next()).localTransactionRolledback(new ConnectionEvent(
                                                                                    this, 
                                                                                    0));
        }
    }
}