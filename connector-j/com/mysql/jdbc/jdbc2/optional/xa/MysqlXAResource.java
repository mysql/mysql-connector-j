package com.mysql.jdbc.jdbc2.optional.xa;

import java.sql.*;


/*
 * Licensed under the X license (see http://www.x.org/terms.htm)
 */
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;


/**
 * JTA resource implementation for JDBC 1.0 connections.  This is somewhat
 * limited in two respects.  First, it does not support two-phase commits since
 * JDBC 1.0 does not.  It will operate in the presence of two-phase commits, but
 * will throw heuristic exceptions if there is a failure during a commit or
 * rollback.  Second, it can only be associated with one transaction
 * at a time, and will throw exceptions if a second transaction tries to
 * attach before the first has called commit, rollback, or forget.
 * <P><FONT COLOR="RED"><B>Warning:</B></FONT></P> This implementation assumes
 * that forget will be called after a failed commit or rollback.  Otherwise,
 * the database connection will never be closed.</P>
 *
 * @author Aaron Mulder (ammulder@alumni.princeton.edu)
 */
public class MysqlXAResource
    implements XAResource {

    private Connection con;
    private MysqlXAConnectionWrapper xaCon;
    private Xid current;
    private boolean active = false;
    private int timeoutIgnored = 0;

    /**
     * Creates a new instance as the transactional resource for the specified
     * underlying connection.
     */
    public MysqlXAResource(Connection con) {
        this.con = con;
    }

    /**
     * Sets the XAConnection associated with this XAResource.  This is required,
     * but both classes cannot include an instance of the other in their
     * constructor!
     * @throws java.lang.IllegalStateException
     *    Occurs when this is called more than once.
     */
    void setXAConnection(MysqlXAConnectionWrapper xaCon) {

        if (this.xaCon != null) {
            throw new IllegalStateException();
        }

        this.xaCon = xaCon;
    }

    public MysqlXAConnectionWrapper getXAConnection() {

        return xaCon;
    }

    /**
     * Gets whether there is outstanding work on behalf of a Transaction.  If
     * there is not, then a connection that is closed will cause the
     * XAConnection to be closed or returned to a pool.  If there is, then the
     * XAConnection must be kept open until commit or rollback is called.
     */
    public boolean isTransaction() {

        return current != null;
    }

    /**
     * Closes this instance permanently.
     */
    public void close() {
        con = null;
        current = null;
        xaCon = null;
    }

    /**
     * Commits a transaction.
     * @throws XAException
     *     Occurs when the state was not correct (end never called), the
     *     transaction ID is wrong, the connection was set to Auto-Commit,
     *     or the commit on the underlying connection fails.  The error code
     *     differs depending on the exact situation.
     */
    public void commit(Xid id, boolean twoPhase)
                throws XAException {

        if (active && !twoPhase) {

            // End was not called!
            System.err.println(
                    "WARNING: Connection not closed before transaction commit.\nConnection will not participate in any future transactions.\nAre you sure you want to be doing this?");
        }

        if (current == null || !id.equals(current)) {

            // wrong Xid
            throw new XAException(XAException.XAER_NOTA);
        }

        try {

            if (con.getAutoCommit()) {
                throw new XAException(XAException.XA_HEURCOM);
            }
        } catch (SQLException e) {
        }

        try {
            con.commit();
        } catch (SQLException e) {

            try {
                con.rollback();

                if (!twoPhase) {
                    throw new XAException(XAException.XA_RBROLLBACK);
                }
            } catch (SQLException e2) {
            }

            if (twoPhase) {
                throw new XAException(XAException.XA_HEURRB); // no 2PC!
            } else {
                throw new XAException(XAException.XA_RBOTHER);

                // Truly, neither committed nor rolled back.  Ouch!
            }
        }

        current = null;

        if (active) {
            active = false; // No longer associated with the original transaction
        } else {
            xaCon.transactionFinished(); // No longer in use at all
        }
    }

    /**
     * Dissociates a resource from a global transaction.
     * @throws XAException
     *     Occurs when the state was not correct (end called twice), or the
     *     transaction ID is wrong.
     */
    public void end(Xid id, int flags)
             throws javax.transaction.xa.XAException {

        if (!active) {

            // End was called twice!
            throw new XAException(XAException.XAER_PROTO);
        }

        if (current == null || !id.equals(current)) {
            throw new XAException(XAException.XAER_NOTA);
        }

        active = false;
    }

    /**
     * Indicates that no further action will be taken on behalf of this
     * transaction (after a heuristic failure).  It is assumed this will be
     * called after a failed commit or rollback.
     * @throws XAException
     *     Occurs when the state was not correct (end never called), or the
     *     transaction ID is wrong.
     */
    public void forget(Xid id)
                throws javax.transaction.xa.XAException {

        if (current == null || !id.equals(current)) {
            throw new XAException(XAException.XAER_NOTA);
        }

        current = null;
        xaCon.transactionFailed();

        if (active) {

            // End was not called!
            System.err.println(
                    "WARNING: Connection not closed before transaction forget.\nConnection will not participate in any future transactions.\nAre you sure you want to be doing this?");
        }
    }

    /**
     * Gets the transaction timeout.
     */
    public int getTransactionTimeout()
                              throws javax.transaction.xa.XAException {

        return timeoutIgnored;
    }

    /**
     * Since the concept of resource managers does not really apply here (all
     * JDBC connections must be managed individually), indicates whether the
     * specified resource is the same as this one.
     */
    public boolean isSameRM(XAResource res)
                     throws javax.transaction.xa.XAException {

        return res == this;
    }

    /**
     * Prepares a transaction to commit.  Since JDBC 1.0 does not support
     * 2-phase commits, this claims the commit is OK (so long as some work was
     * done on behalf of the specified transaction).
     * @throws XAException
     *     Occurs when the state was not correct (end never called), the
     *     transaction ID is wrong, or the connection was set to Auto-Commit.
     */
    public int prepare(Xid id)
                throws javax.transaction.xa.XAException {

        if (active) {

            // End was not called!
            System.err.println(
                    "WARNING: Connection not closed before transaction commit.\nConnection will not participate in any future transactions.\nAre you sure you want to be doing this?");
        }

        if (current == null || !id.equals(current)) {

            // wrong Xid
            throw new XAException(XAException.XAER_NOTA);
        }

        try {

            if (con.getAutoCommit()) {
                throw new XAException(XAException.XA_HEURCOM);
            }
        } catch (SQLException e) {
        }

        return XA_OK;
    }

    /**
     * Returns all transaction IDs where work was done with no corresponding
     * commit, rollback, or forget.  Not really sure why this is useful in the
     * context of JDBC drivers.
     */
    public Xid[] recover(int flag)
                  throws javax.transaction.xa.XAException {

        if (current == null) {

            return new Xid[0];
        } else {

            return new Xid[] { current };
        }
    }

    /**
     * Rolls back the work, assuming it was done on behalf of the specified
     * transaction.
     * @throws XAException
     *     Occurs when the state was not correct (end never called), the
     *     transaction ID is wrong, the connection was set to Auto-Commit,
     *     or the rollback on the underlying connection fails.  The error code
     *     differs depending on the exact situation.
     */
    public void rollback(Xid id)
                  throws javax.transaction.xa.XAException {

        if (active) {

            // End was not called!
            System.err.println(
                    "WARNING: Connection not closed before transaction rollback.\nConnection will not participate in any future transactions.\nAre you sure you want to be doing this?");
        }

        if (current == null || !id.equals(current)) {

            // wrong Xid
            throw new XAException(XAException.XAER_NOTA);
        }

        try {

            if (con.getAutoCommit()) {
                throw new XAException(XAException.XA_HEURCOM);
            }
        } catch (SQLException e) {
        }

        try {
            con.rollback();
        } catch (SQLException e) {
            throw new XAException("Rollback failed: " + e.getMessage());
        }

        current = null;

        if (active) {
            active = false; // No longer associated with the original transaction
        } else {
            xaCon.transactionFinished(); // No longer in use at all
        }
    }

    /**
     * Sets the transaction timeout.  This is saved, but the value is not used
     * by the current implementation.
     */
    public boolean setTransactionTimeout(int timeout)
                                  throws javax.transaction.xa.XAException {
        timeoutIgnored = timeout;

        return true;
    }

    /**
     * Associates a JDBC connection with a global transaction.  We assume that
     * end will be called followed by prepare, commit, or rollback.
     * If start is called after end but before commit or rollback, there is no
     * way to distinguish work done by different transactions on the same
     * connection).  If start is called more than once before
     * end, either it's a duplicate transaction ID or illegal transaction ID
     * (since you can't have two transactions associated with one DB
     * connection).
     * @throws XAException
     *     Occurs when the state was not correct (start called twice), the
     *     transaction ID is wrong, or the instance has already been closed.
     */
    public void start(Xid id, int flags)
               throws javax.transaction.xa.XAException {

        if (active) { // Start was called twice!

            if (current != null && id.equals(current))
                throw new XAException(XAException.XAER_DUPID);
            else
                throw new XAException(XAException.XAER_PROTO);
        }

        if (current != null && !id.equals(current)) {
            throw new XAException(XAException.XAER_NOTA);
        }

        if (con == null) {
            throw new XAException(XAException.XA_RBOTHER);
        }

        current = id;
        active = true;
    }
}