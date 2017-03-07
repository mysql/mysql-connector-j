/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.api.xdevapi;

import java.util.List;

/**
 * A client interface to the session on the X Plugin server.
 */
public interface BaseSession {

    /**
     * Retrieve the list of Schema objects for which the current user has access.
     * 
     * @return list of Schema objects
     */
    List<Schema> getSchemas();

    /**
     * Retrieve the Schema corresponding to name.
     * 
     * @param schemaName
     *            name of schema to retrieve
     * @return {@link Schema}
     */
    Schema getSchema(String schemaName);

    /**
     * Retrieve the default schema name which may be configured at connect time.
     * 
     * @return default schema name
     */
    String getDefaultSchemaName();

    /**
     * Retrieve the default schema which may be configured at connect time.
     * 
     * @return default {@link Schema}
     */
    Schema getDefaultSchema();

    /**
     * Create and return a new schema with the name given by name.
     * 
     * @param schemaName
     *            name of schema to create
     * @return {@link Schema} created
     */
    Schema createSchema(String schemaName);

    /**
     * Create and return a new schema with the name given by name. If the schema already exists, a reference to it is returned.
     * 
     * @param schemaName
     *            name of schema to create
     * @param reuseExistingObject
     *            true to reuse
     * @return {@link Schema} created
     */
    Schema createSchema(String schemaName, boolean reuseExistingObject);

    /**
     * Drop the existing schema with the name given by name.
     * 
     * @param schemaName
     *            name of schema to drop
     */
    void dropSchema(String schemaName);

    /**
     * Drop the collection in given schema.
     * 
     * @param schemaName
     *            schema name
     * @param collectionName
     *            name of collection to drop
     */
    void dropCollection(String schemaName, String collectionName);

    /**
     * Drop the table in given schema.
     * 
     * @param schemaName
     *            schema name
     * @param tableName
     *            name of table to drop
     */
    void dropTable(String schemaName, String tableName);

    /**
     * Get the URL used to create this session.
     * 
     * @return URI
     */
    String getUri();

    /**
     * Is this session open?
     * 
     * @return true if session is open
     */
    boolean isOpen();

    /**
     * Close this session.
     */
    void close();

    /**
     * Start a new transaction.
     */
    void startTransaction();

    /**
     * Commit the transaction.
     */
    void commit();

    /**
     * Rollback the transaction.
     */
    void rollback();

}
