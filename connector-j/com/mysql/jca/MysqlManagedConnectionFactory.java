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

import java.util.Set;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;

import javax.security.auth.Subject;


/**
 * ManagedConnectionFactory instance is a factory of both ManagedConnection 
 * and EIS-specific connection factory instances. This interface supports 
 * connection pooling by providing methods for matching and creation of 
 * ManagedConnection instance. A ManagedConnectionFactory instance is 
 * required to be a JavaBean. 
 */
public class MysqlManagedConnectionFactory
    implements ManagedConnectionFactory
{

    /**
     * Creates a Connection Factory instance. The Connection Factory 
     * instance gets initialized with the passed ConnectionManager. 
     * 
     * In the managed scenario, ConnectionManager is provided by the 
     * application server.
     *
     * @param cxManager ConnectionManager to be associated with created 
     *                   EIS connection factory instance 
     * 
     * @return EIS-specific Connection Factory instance or 
     *                       javax.resource.cci.ConnectionFactory instance 
     *
     * @throws ResourceException Generic exception 
     * @throws ResourceAdapterInternalException Resource adapter related 
     *                                           error condition
     */
    public Object createConnectionFactory(ConnectionManager arg0)
                                   throws ResourceException
    {

        return null;
    }

    /**
     * Creates a Connection Factory instance. The Connection Factory instance 
     * gets initialized with a default ConnectionManager provided by the 
     * resource adapter.
     *
     * @return EIS-specific Connection Factory instance or 
     *                       javax.resource.cci.ConnectionFactory instance Throws:
     * 
     * @throws ResourceException Generic exception 
     * @throws ResourceAdapterInternalException Resource adapter related error condition
     */
    public Object createConnectionFactory()
                                   throws ResourceException
    {

        return null;
    }

    /**
     * Creates a new physical connection to the underlying EIS resource manager,
     *
     * ManagedConnectionFactory uses the security information (passed as Subject) 
     * and additional ConnectionRequestInfo (which is specific to ResourceAdapter 
     * and opaque to application server) to create this new connection.
     * 

     * @param cxRequestInfo Additional resource adapter specific connection 
     *                       request information 
     * 
     * @return ManagedConnection instance 
     * 
     * @throws ResourceException generic exception 
     * @throws SecurityException security related error 
     * @throws ResourceAllocationException failed to allocate system resources 
     *                                      for connection request 
     * @throws ResourceAdapterInternalException resource adapter related error 
     *                                           condition 
     * @throws EISSystemException internal error condition in EIS instance
     */
    public ManagedConnection createManagedConnection(Subject subject, 
                                                     ConnectionRequestInfo cxRequestInfo)
                                              throws ResourceException
    {

        return null;
    }

    /**
     * Returns a matched connection from the candidate set of connections.
     *
     * ManagedConnectionFactory uses the security info (as in Subject) and 
     * information provided through ConnectionRequestInfo and additional 
     * Resource Adapter specific criteria to do matching. Note that 
     * criteria used for matching is specific to a resource adapter and is 
     * not prescribed by the Connector specification.
     *
     * This method returns a ManagedConnection instance that is the best 
     * match for handling the connection allocation request.
     *
     * @param connectionSet candidate connection set
     * @param subject security info
     * @param cxRequestInfo additional resource adapter specific 
     *                       connection request information 
     * 
     * @return ManagedConnection if resource adapter finds an acceptable match otherwise null 
     * 
     * @throws ResourceException generic exception 
     * @throws SecurityException security related error 
     * @throws ResourceAdapterInternalException resource adapter related error condition 
     * @throws NotSupportedException - if operation is not supported
     */
    public ManagedConnection matchManagedConnections(Set connectionSet, 
                                                     Subject subject, 
                                                     ConnectionRequestInfo cxRequestInfo)
                                              throws ResourceException
    {

        return null;
    }

    /**
     * Set the log writer for this ManagedConnectionFactory instance.
     *
     * The log writer is a character output stream to which all logging 
     * and tracing messages for this ManagedConnectionfactory instance 
     * will be printed.
     *  
     * ApplicationServer manages the association of output stream with 
     * the ManagedConnectionFactory. When a ManagedConnectionFactory object 
     * is created the log writer is initially null, in other words, logging 
     * is disabled. Once a log writer is associated with a 
     * ManagedConnectionFactory, logging and tracing for 
     * ManagedConnectionFactory instance is enabled.
     * 
     * The ManagedConnection instances created by ManagedConnectionFactory 
     * &quot;inherits&quot; the log writer, which can be overridden by ApplicationServer 
     * using ManagedConnection.setLogWriter to set ManagedConnection specific 
     * logging and tracing.
     * 
     * @param out PrintWriter - an out stream for error logging and tracing 
     * @throws ResourceException generic exception 
     * @throws ResourceAdapterInternalException resource adapter related error condition
     */
    public void setLogWriter(PrintWriter out)
                      throws ResourceException
    {
    }

    /**
     * Get the log writer for this ManagedConnectionFactory instance.
     *
     * The log writer is a character output stream to which all logging 
     * and tracing messages for this ManagedConnectionFactory instance 
     * will be printed
     *
     * ApplicationServer manages the association of output stream with the 
     * ManagedConnectionFactory. When a ManagedConnectionFactory object is 
     * created the log writer is initially null, in other words, logging 
     * is disabled. 
     */
    public PrintWriter getLogWriter()
                             throws ResourceException
    {

        return null;
    }

    /**
     * Gets the associated ResourceAdapter JavaBean.
     *
     * @return ResourceAdapter JavaBean.
     */
    public ResourceAdapter getResourceAdapter()
    {

        return null;
    }

    /**
     * Associate this ManagedConnectionFactory JavaBean with a 
     * ResourceAdapter JavaBean. 
     * 
     * Note, this method must be called exactly once; that is, the association 
     * must not change during the lifetime of this ManagedConnectionFactory 
     * JavaBean.
     * 
     * @param ra a ResourceAdapter JavaBean. 
     * 
     * @throws ResourceException generic exception. 
     * @throws ResourceAdapterInternalException resource adapter related error 
     *                                           condition. 
     * @throws ResourceException
     */
    public void setResourceAdapter(ResourceAdapter arg0)
                            throws ResourceException
    {
    }
}