/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.xdevapi;

/**
 * A client-side representation of X Plugin server object, e.g. table, collection, etc.
 */
public interface DatabaseObject {

    /**
     * Type of database objects.
     */
    enum DbObjectType {
        COLLECTION, TABLE, VIEW, COLLECTION_VIEW
    };

    /**
     * Existence states of database objects.
     */
    enum DbObjectStatus {
        EXISTS, NOT_EXISTS, UNKNOWN
    };

    /**
     * Retrieve the session owning the given schema object.
     * 
     * @return {@link Session}
     */
    Session getSession();

    /**
     * Retrieve the schema owning this database object.
     * 
     * @return {@link Schema}
     */
    Schema getSchema();

    /**
     * Retrieve the name of the database object represented by the Java object.
     * 
     * @return name
     */
    String getName();

    /**
     * Query the existence of this database object.
     * 
     * @return {@link DbObjectStatus}
     */
    DbObjectStatus existsInDatabase();
}
