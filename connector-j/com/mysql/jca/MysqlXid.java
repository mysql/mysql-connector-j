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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

import java.net.InetAddress;

import java.rmi.server.UID;

import javax.transaction.xa.Xid;


/**
 * XA-ID for MySQL.
 * 
 * MySQL does not support XA (yet), so we create our own XID.
 * 
 * @author Mark Matthews
 */
public class MysqlXid
    implements Xid,
               Serializable {

    /**
     * We'll use a value that MySQL should never use once
     * it implements XA. That way .equals() will work correctly.
     */
    private static final int FORMAT_ID = Integer.MIN_VALUE;
    private static int branchQualifier = 1;
    private byte[] branchAsBytes = null;
    private byte[] globalIdAsBytes = null;
    private int hashCode;

    /**
     * Constructor for MysqlXid.
     */
    public MysqlXid() {
        super();

        InetAddress localhost = null;

        try {
            localhost = InetAddress.getLocalHost();
        } catch (Exception ex) {

            try {
                localhost = InetAddress.getByName("127.0.0.1");
            } catch (Exception furtherEx) {

                // we'll punt later
            }
        }

        UID id = new UID();
        hashCode = id.hashCode(); // little chance for collision

        try {

            ByteArrayOutputStream globalIdBytesOut = new ByteArrayOutputStream();
            DataOutputStream dOut = new DataOutputStream(globalIdBytesOut);
            id.write(dOut);
            dOut.flush();

            if (localhost != null) {

                byte[] addressAsBytes = localhost.getAddress();

                for (int i = 0; i < addressAsBytes.length; i++) {
                    globalIdBytesOut.write(addressAsBytes[i]);
                }
            }

            globalIdAsBytes = globalIdBytesOut.toByteArray();
            dOut.close();
            globalIdBytesOut.close();

            ByteArrayOutputStream branchQualBytesOut = new ByteArrayOutputStream();
            dOut = new DataOutputStream(branchQualBytesOut);
            dOut.write(branchQualifier);
            dOut.flush();
            branchAsBytes = branchQualBytesOut.toByteArray();
            dOut.close();
            branchQualBytesOut.close();
        } catch (IOException ioEx) {

            // ignore, won't happen with these types of streams
        }
    }

    /**
     * @see javax.transaction.xa.Xid#getFormatId()
     */
    public int getFormatId() {

        return FORMAT_ID;
    }

    /**
     * @see javax.transaction.xa.Xid#getGlobalTransactionId()
     */
    public byte[] getGlobalTransactionId() {

        return (byte[]) globalIdAsBytes.clone();
    }

    /**
     * @see javax.transaction.xa.Xid#getBranchQualifier()
     */
    public byte[] getBranchQualifier() {

        return (byte[]) branchAsBytes.clone();
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {

        try {

            Xid other = (Xid) obj;

            if (other.getFormatId() != other.getFormatId()) {

                return false;
            }

            byte[] otherGlobalID = other.getGlobalTransactionId();
            byte[] otherBranchID = other.getBranchQualifier();

            if (globalIdAsBytes.length != otherGlobalID.length
                || branchAsBytes.length != otherBranchID.length) {

                return false;
            }

            for (int i = 0; i < globalIdAsBytes.length; ++i) {

                if (otherGlobalID[i] != globalIdAsBytes[i]) {

                    return false;
                }
            }

            for (int i = 0; i < branchAsBytes.length; ++i) {

                if (otherBranchID[i] != branchAsBytes[i]) {

                    return false;
                }
            }

            return true;
        } catch (ClassCastException cce) {

            return false;
        }
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {

        return hashCode;
    }
}