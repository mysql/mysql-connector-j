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

import java.sql.SQLException;
import java.sql.Statement;

import javax.resource.ResourceException;
import javax.resource.spi.LocalTransaction;


/**
 * Implementation of the JCA LocalTransaction Interface for MySQL.
 * 
 * LocalTransaction interface provides support for transactions that 
 * are managed internal to an EIS resource manager, and do not 
 * require an external transaction manager. 
 *
 * A resource adapter implements the javax.resource.spi.LocalTransaction 
 * interface to provide support for local transactions that are 
 * performed on the underlying resource manager. 
 * 
 * If a resource adapter supports the LocalTransaction interface, then 
 * the application server can choose to perform local transaction 
 * optimization (uses local transaction instead of a JTA transaction for a single 
 * resource manager case). 
 * 
 * @author Mark Matthews
 */
public class MysqlLocalTransaction
    implements LocalTransaction {

    private ConnectionWrapper conn = null;
    private MysqlManagedConnection managedConn = null;

    /**
     * Creates a new MysqlLocalTransaction object.
     * 
     * @param conn the wrapped JDBC connection
     * @param managedConn the ManagedConnection that created us.
     */
    public MysqlLocalTransaction(ConnectionWrapper conn, 
                                 MysqlManagedConnection managedConn) {
        this.conn = conn;
        this.managedConn = managedConn;
    }

    /**
     * Begins a local transaction 
     *
     * @throws ResourceException generic exception if operation fails 
     * @throws LocalTransactionException error condition related to local transaction management 
     * @throws ResourceAdapterInternalException error condition internal to resource adapter 
     * @throws EISSystemException EIS instance specific error condition
     */
    public void begin()
               throws ResourceException {
        Statement stmt = null;
        
        try {
            conn.setAutoCommit(false);
            stmt = this.conn.createStatement();
            stmt.executeUpdate("BEGIN");
            
            this.managedConn.notifyTransactionStarted();
            
        } catch (SQLException sqlEx) {
            throw new ResourceException("Unable to start transaction: "
                                        + sqlEx);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }
                
                stmt = null;
            }       
        }
         
        
    }

    /**
     * Commits a local transaction 
     *
     * @throws ResourceException generic exception if operation fails 
     * @throws LocalTransactionException error condition related to local transaction management 
     * @throws ResourceAdapterInternalException error condition internal to resource adapter 
     * @throws EISSystemException EIS instance specific error condition
     */
    public void commit()
                throws ResourceException {

        try {
            conn.commit();
            this.managedConn.notifyTransactionCommitted();
        } catch (SQLException sqlEx) {
            throw new ResourceException("Unable to commit transaction: "
                                        + sqlEx);
        } 
    }

    /**
     * Rolls back a local transaction 
     *
     * @throws ResourceException generic exception if operation fails 
     * @throws LocalTransactionException error condition related to local transaction management 
     * @throws ResourceAdapterInternalException error condition internal to resource adapter 
     * @throws EISSystemException EIS instance specific error condition
     */
    public void rollback()
                  throws ResourceException {

        try {
            conn.rollback();
            this.managedConn.notifyTransactionRolledBack();
        } catch (SQLException sqlEx) {
            throw new ResourceException("Unable to rollback transaction: "
                                        + sqlEx);
        } 
    }
}