/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.x;

import java.util.List;

/**
 * A client-side representation of a database schema. Provides access to the schema contents.
 */
public interface Schema extends DatabaseObject {

    /* Browse functions */

    /**
     * Retrieve the set of collections existing in this schema.
     */
    List<Collection> getCollections();

    /**
     * Retrieve the set of tables existing in this schema.
     */
    List<Table> getTables();

    /* Other functions */

    /**
     * Retrieve a reference to the named collection.
     */
    Collection getCollection(String name);

    /**
     * Retrieve a reference to the named collection hinting that an exception should be thrown if the collection is not known to the server.
     */
    Collection getCollection(String name, boolean requireExists);

    /**
     * Retrieve a reference to the named collection using the table API.
     */
    Table getCollectionAsTable(String name);

    /**
     * Retrieve a reference to the named table.
     */
    Table getTable(String name);

    /**
     * Retrieve a reference to the named table hinting that an exception should be thrown if the collection is not known to the server.
     */
    Table getTable(String tableName, boolean requireExists);

    /* Create functions */

    /**
     * Create a new collection.
     */
    Collection createCollection(String name);

    /**
     * Create a new collection if it does not already exist on the server.
     */
    Collection createCollection(String name, boolean reuseExistingObject);

}
