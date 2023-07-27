/*
 * Copyright (c) 2007, 2023, Oracle and/or its affiliates.
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

import java.sql.SQLException;
import java.util.Iterator;

import com.mysql.cj.jdbc.DatabaseMetaData.IteratorWithCleanup;

public abstract class IterateBlock<T> {

    IteratorWithCleanup<T> iteratorWithCleanup;
    Iterator<T> javaIterator;
    boolean stopIterating = false;

    IterateBlock(IteratorWithCleanup<T> i) {
        this.iteratorWithCleanup = i;
        this.javaIterator = null;
    }

    IterateBlock(Iterator<T> i) {
        this.javaIterator = i;
        this.iteratorWithCleanup = null;
    }

    public void doForAll() throws SQLException {
        if (this.iteratorWithCleanup != null) {
            try {
                while (this.iteratorWithCleanup.hasNext()) {
                    forEach(this.iteratorWithCleanup.next());

                    if (this.stopIterating) {
                        break;
                    }
                }
            } finally {
                this.iteratorWithCleanup.close();
            }
        } else {
            while (this.javaIterator.hasNext()) {
                forEach(this.javaIterator.next());

                if (this.stopIterating) {
                    break;
                }
            }
        }
    }

    abstract void forEach(T each) throws SQLException;

    public final boolean fullIteration() {
        return !this.stopIterating;
    }

}
