package com.mysql.jca;

import com.mysql.jdbc.Connection;

import java.sql.SQLException;

import javax.resource.ResourceException;
import javax.resource.cci.LocalTransaction;
import javax.resource.spi.ConnectionEvent;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;


/**
 * @author Owner
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public class MysqlLocalTransaction
    implements LocalTransaction
{

    private ConnectionWrapper conn = null;

    /**
     * Creates a new MysqlLocalTransaction object.
     * 
     * @param c DOCUMENT ME!
     */
    public MysqlLocalTransaction(ConnectionWrapper c)
    {
        this.conn = c;
    }

    /**
     * DOCUMENT ME!
     * 
     * @throws ResourceException DOCUMENT ME!
     */
    public void begin()
               throws ResourceException
    {
    }

    /**
     * DOCUMENT ME!
     * 
     * @throws ResourceException DOCUMENT ME!
     */
    public void commit()
                throws ResourceException
    {

        try
        {
            conn.commit();
        }
        catch (SQLException sqlEx)
        {
            throw new ResourceException("Unable to commit transaction: " + 
                                        sqlEx);
        }
        finally
        {
            conn = null;
        }
    }

    /**
     * DOCUMENT ME!
     * 
     * @throws ResourceException DOCUMENT ME!
     */
    public void rollback()
                  throws ResourceException
    {

        try
        {
            conn.rollback();
        }
        catch (SQLException sqlEx)
        {
            throw new ResourceException("Unable to rollback transaction: " + 
                                        sqlEx);
        }
        finally
        {
            conn = null;
        }
    }
}