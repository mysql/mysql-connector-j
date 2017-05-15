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

import java.util.Map;

import com.mysql.cj.xdevapi.DbDoc;

/**
 * A client-side representation of a collection of documents. This interface allows access/manipulation to the collection through add/find/modify/remove
 * statements.
 */
public interface Collection extends DatabaseObject {
    /**
     * Add a document in the form of a Map.
     * 
     * @param doc
     *            map of key-value parameters representing the document fields
     * @return {@link AddStatement}
     */
    AddStatement add(Map<String, ?> doc);

    /**
     * Add one or more documents.
     * 
     * @param jsonStrings
     *            one or more documents given as JSON strings
     * @return {@link AddStatement}
     */
    AddStatement add(String... jsonStrings);

    // TODO we have to keep add(DbDoc document) method because the DbDoc does extend the TreeMap<String, JsonValue>,
    // thus w/o this method the col.add(dbdoc) will call the add(Map<String, ?> doc) method (which is not implemented yet)
    // instead of add(DbDoc... documents).
    /**
     * Add a document in the form of a DbDoc.
     * 
     * @param document
     *            {@link DbDoc}
     * @return {@link AddStatement}
     */
    AddStatement add(DbDoc document);

    /**
     * Add a sequence of documents.
     * 
     * @param documents
     *            one or more documents given as {@link DbDoc}
     * @return {@link AddStatement}
     */
    AddStatement add(DbDoc... documents);

    /**
     * Create a new find statement retrieving all documents in the collection.
     * 
     * @return {@link FindStatement}
     */
    FindStatement find();

    /**
     * Create a new find statement retrieving documents matching the given search condition.
     *
     * @param searchCondition
     *            condition expression
     * @return {@link FindStatement}
     */
    FindStatement find(String searchCondition);

    /**
     * Create a new modify statement affecting documents matching the given search condition.
     * 
     * @param searchCondition
     *            condition expression
     * @return {@link ModifyStatement}
     */
    ModifyStatement modify(String searchCondition);

    /**
     * Create a new removal statement affecting documents matching the given search condition.
     * 
     * @param searchCondition
     *            condition expression
     * @return {@link RemoveStatement}
     */
    RemoveStatement remove(String searchCondition);

    /**
     * Create a new statement defining the creation of an index on this collection.
     * 
     * @param indexName
     *            index name
     * @param unique
     *            unique flag
     * @return {@link CreateCollectionIndexStatement}
     */
    CreateCollectionIndexStatement createIndex(String indexName, boolean unique);

    /**
     * Create a new statement defining the removal of an index on this collection.
     * 
     * @param indexName
     *            index name
     * @return {@link DropCollectionIndexStatement}
     */
    DropCollectionIndexStatement dropIndex(String indexName);

    /**
     * Query the number of documents in this collection.
     * 
     * @return The number of documents in this collection
     */
    long count();

    /**
     * Create a new document.
     * 
     * @return {@link DbDoc}
     */
    DbDoc newDoc();
}
