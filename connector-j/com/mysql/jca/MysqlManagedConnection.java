package com.mysql.jca;

import java.io.PrintWriter;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;
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
 */
public class MysqlManagedConnection implements ManagedConnection, XAResource {

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
	public Object getConnection(Subject subject, ConnectionRequestInfo cxRequestInfo)
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
	public void destroy() throws ResourceException {
	}

	/**
	 * @see javax.resource.spi.ManagedConnection#cleanup()
	 */
	public void cleanup() throws ResourceException {
	}

	/**
	 * @see javax.resource.spi.ManagedConnection#associateConnection(Object)
	 */
	public void associateConnection(Object arg0) throws ResourceException {
	}

	/**
	 * @see javax.resource.spi.ManagedConnection#addConnectionEventListener(ConnectionEventListener)
	 */
	public void addConnectionEventListener(ConnectionEventListener arg0) {
	}

	/**
	 * @see javax.resource.spi.ManagedConnection#removeConnectionEventListener(ConnectionEventListener)
	 */
	public void removeConnectionEventListener(ConnectionEventListener arg0) {
	}

	/**
	 * @see javax.resource.spi.ManagedConnection#getXAResource()
	 */
	public XAResource getXAResource() throws ResourceException {
		return this;
	}

	/**
	 * @see javax.resource.spi.ManagedConnection#getLocalTransaction()
	 */
	public LocalTransaction getLocalTransaction() throws ResourceException {
		return null;
	}

	/**
	 * @see javax.resource.spi.ManagedConnection#getMetaData()
	 */
	public ManagedConnectionMetaData getMetaData() throws ResourceException {
		return null;
	}

	/**
	 * @see javax.resource.spi.ManagedConnection#setLogWriter(PrintWriter)
	 */
	public void setLogWriter(PrintWriter arg0) throws ResourceException {
	}

	/**
	 * @see javax.resource.spi.ManagedConnection#getLogWriter()
	 */
	public PrintWriter getLogWriter() throws ResourceException {
		return null;
	}

	/**
	 * @see javax.transaction.xa.XAResource#commit(Xid, boolean)
	 */
	public void commit(Xid arg0, boolean arg1) throws XAException {
	}

	/**
	 * @see javax.transaction.xa.XAResource#end(Xid, int)
	 */
	public void end(Xid arg0, int arg1) throws XAException {
	}

	/**
	 * @see javax.transaction.xa.XAResource#forget(Xid)
	 */
	public void forget(Xid arg0) throws XAException {
	}

	/**
	 * @see javax.transaction.xa.XAResource#getTransactionTimeout()
	 */
	public int getTransactionTimeout() throws XAException {
		return 0;
	}

	/**
	 * @see javax.transaction.xa.XAResource#isSameRM(XAResource)
	 */
	public boolean isSameRM(XAResource arg0) throws XAException {
		return false;
	}

	/**
	 * @see javax.transaction.xa.XAResource#prepare(Xid)
	 */
	public int prepare(Xid arg0) throws XAException {
		return 0;
	}

	/**
	 * @see javax.transaction.xa.XAResource#recover(int)
	 */
	public Xid[] recover(int arg0) throws XAException {
		return null;
	}

	/**
	 * @see javax.transaction.xa.XAResource#rollback(Xid)
	 */
	public void rollback(Xid arg0) throws XAException {
	}

	/**
	 * @see javax.transaction.xa.XAResource#setTransactionTimeout(int)
	 */
	public boolean setTransactionTimeout(int arg0) throws XAException {
		return false;
	}

	/**
	 * @see javax.transaction.xa.XAResource#start(Xid, int)
	 */
	public void start(Xid arg0, int arg1) throws XAException {
	}

}
