/*
 * Copyright (c) 2005, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc;

import javax.transaction.xa.Xid;

/**
 * Implementation of the XID interface for MySQL XA
 */
public class MysqlXid implements Xid {

    int hash = 0;

    byte[] myBqual;

    int myFormatId;

    byte[] myGtrid;

    public MysqlXid(byte[] gtrid, byte[] bqual, int formatId) {
        this.myGtrid = gtrid;
        this.myBqual = bqual;
        this.myFormatId = formatId;
    }

    @Override
    public boolean equals(Object another) {

        if (another instanceof Xid) {
            Xid anotherAsXid = (Xid) another;

            if (this.myFormatId != anotherAsXid.getFormatId()) {
                return false;
            }

            byte[] otherBqual = anotherAsXid.getBranchQualifier();
            byte[] otherGtrid = anotherAsXid.getGlobalTransactionId();

            if (otherGtrid != null && otherGtrid.length == this.myGtrid.length) {
                int length = otherGtrid.length;

                for (int i = 0; i < length; i++) {
                    if (otherGtrid[i] != this.myGtrid[i]) {
                        return false;
                    }
                }

                if (otherBqual != null && otherBqual.length == this.myBqual.length) {
                    length = otherBqual.length;

                    for (int i = 0; i < length; i++) {
                        if (otherBqual[i] != this.myBqual[i]) {
                            return false;
                        }
                    }
                } else {
                    return false;
                }

                return true;
            }
        }

        return false;
    }

    public byte[] getBranchQualifier() {
        return this.myBqual;
    }

    public int getFormatId() {
        return this.myFormatId;
    };

    public byte[] getGlobalTransactionId() {
        return this.myGtrid;
    }

    @Override
    public synchronized int hashCode() {
        if (this.hash == 0) {
            for (int i = 0; i < this.myGtrid.length; i++) {
                this.hash = 33 * this.hash + this.myGtrid[i];
            }
        }

        return this.hash;
    }
}
