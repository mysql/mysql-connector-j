/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.util.List;

/**
 * A client-side representation of a database schema. Provides access to the schema contents.
 */
public interface Schema extends DatabaseObject {

    /* Browse functions */

    /**
     * Retrieve the set of collections existing in this schema.
     * 
     * @return list of {@link Collection} objects
     */
    List<Collection> getCollections();

    /**
     * Retrieve the set of collections existing in this schema and matching the given pattern.
     * 
     * @param pattern
     *            match pattern
     * @return list of {@link Collection} objects
     */
    List<Collection> getCollections(String pattern);

    /**
     * Retrieve the set of tables existing in this schema.
     * 
     * @return list of {@link Table} objects
     */
    List<Table> getTables();

    /**
     * Retrieve the set of tables existing in this schema and matching the given pattern.
     * 
     * @param pattern
     *            match pattern
     * @return list of {@link Table} objects
     */
    List<Table> getTables(String pattern);

    /* Other functions */

    /**
     * Retrieve a reference to the named collection.
     * 
     * @param name
     *            collection name
     * @return {@link Collection}
     */
    Collection getCollection(String name);

    /**
     * Retrieve a reference to the named collection hinting that an exception should be thrown if the collection is not known to the server.
     * 
     * @param name
     *            collection name
     * @param requireExists
     *            true if required to exist
     * @return {@link Collection}
     */
    Collection getCollection(String name, boolean requireExists);

    /**
     * Retrieve a reference to the named collection using the table API.
     * 
     * @param name
     *            collection name
     * @return {@link Table}
     */
    Table getCollectionAsTable(String name);

    /**
     * Retrieve a reference to the named table.
     * 
     * @param name
     *            table name
     * @return {@link Table}
     */
    Table getTable(String name);

    /**
     * Retrieve a reference to the named table hinting that an exception should be thrown if the collection is not known to the server.
     * 
     * @param tableName
     *            table name
     * @param requireExists
     *            true if required to exist
     * @return {@link Table}
     */
    Table getTable(String tableName, boolean requireExists);

    /* Create functions */

    /**
     * Create a new collection.
     * 
     * @param name
     *            collection name
     * @return {@link Collection}
     */
    Collection createCollection(String name);

    /**
     * Create a new collection if it does not already exist on the server.
     * 
     * @param name
     *            collection name
     * @param reuseExistingObject
     *            true if allowed to reuse
     * @return {@link Collection}
     */
    Collection createCollection(String name, boolean reuseExistingObject);

    /**
     * Drop the collection from this schema.
     * 
     * @param collectionName
     *            name of collection to drop
     */
    void dropCollection(String collectionName);
}
