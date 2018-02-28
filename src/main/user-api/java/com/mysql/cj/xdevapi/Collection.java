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

import java.util.Map;

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
     * <p>
     * Example: collection.createIndex("myIndex",
     * "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"GEOJSON\", \"required\": true, \"options\": 2, \"srid\": 4326}], \"type\":\"SPATIAL\"}");
     * 
     * @param indexName
     *            index name
     * @param indexDefinition
     *            JSON document with the following fields:
     *            <ul>
     *            <li>fields: array of IndexField objects, each describing a single document member to be included in the index (see below)</li>
     *            <li>type: string, (optional) the type of index. One of INDEX or SPATIAL (case insensitive). Default is INDEX and may be omitted.</li>
     *            </ul>
     *            where single IndexField description consists of the following fields:
     *            <ul>
     *            <li>field: string, the full document path to the document member or field to be indexed</li>
     *            <li>type: string, one of the supported SQL column types to map the field into (see below for a list). For numeric types, the optional UNSIGNED
     *            keyword may follow. For the TEXT type, the length to consider for indexing may be added. Type descriptions are case insensitive.</li>
     *            <li>required: bool, (optional) true if the field is required to exist in the document. Defaults to false, except for GEOJSON where it defaults
     *            to true</li>
     *            <li>options: int, (optional) special option flags for use when decoding GEOJSON data</li>
     *            <li>srid: int, (optional) srid value for use when decoding GEOJSON data</li>
     *            </ul>
     * @return {@link Result}
     */
    Result createIndex(String indexName, DbDoc indexDefinition);

    /**
     * Create a new statement defining the creation of an index on this collection.
     * <p>
     * Example: collection.createIndex("myIndex",
     * "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"GEOJSON\", \"required\": true, \"options\": 2, \"srid\": 4326}], \"type\":\"SPATIAL\"}");
     * 
     * @param indexName
     *            index name
     * @param jsonIndexDefinition
     *            JSON document with the following fields:
     *            <ul>
     *            <li>fields: array of IndexField objects, each describing a single document member to be included in the index (see below)</li>
     *            <li>type: string, (optional) the type of index. One of INDEX or SPATIAL. Default is INDEX and may be omitted.</li>
     *            </ul>
     *            where single IndexField description consists of the following fields:
     *            <ul>
     *            <li>field: string, the full document path to the document member or field to be indexed</li>
     *            <li>type: string, one of the supported SQL column types to map the field into (see below for a list). For numeric types, the optional UNSIGNED
     *            keyword may follow. For the TEXT type, the length to consider for indexing may be added.</li>
     *            <li>required: bool, (optional) true if the field is required to exist in the document. Defaults to false, except for GEOJSON where it defaults
     *            to true</li>
     *            <li>options: int, (optional) special option flags for use when decoding GEOJSON data</li>
     *            <li>srid: int, (optional) srid value for use when decoding GEOJSON data</li>
     *            </ul>
     * @return {@link Result}
     */
    Result createIndex(String indexName, String jsonIndexDefinition);

    /**
     * Create a new statement defining the removal of an index on this collection.
     * 
     * @param indexName
     *            index name
     */
    void dropIndex(String indexName);

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

    /**
     * Takes in a document object that will replace the matching document. If no matches are found, the function returns normally with no changes being made.
     * 
     * @param id
     *            the document id of the document to be replaced
     * @param doc
     *            the new document, which may contain expressions. If document contains an _id value, it is ignored.
     * @return
     *         Result object, which will indicate the number of affected documents (1 or 0, if none)
     */
    Result replaceOne(String id, DbDoc doc);

    /**
     * Takes in a document object that will replace the matching document. If no matches are found, the function returns normally with no changes being made.
     * 
     * @param id
     *            the document id of the document to be replaced
     * @param jsonString
     *            the new document, given as JSON string, which may contain expressions. If document contains an _id value, it is ignored.
     * @return
     *         Result object, which will indicate the number of affected documents (1 or 0, if none)
     */
    Result replaceOne(String id, String jsonString);

    /**
     * Adds the document to the collection. The following algorithm applies:
     * 
     * @param id
     *            the document id of the document to be replaced
     * @param doc
     *            the new document, which may contain expressions. If doc contains an _id value and it does not match the given id then the error
     *            will be thrown.
     * @return
     *         Result object, which will indicate the number of affected documents (0 - if none, 1 - if added, 2 - if replaced)
     */
    Result addOrReplaceOne(String id, DbDoc doc);

    /**
     * Adds the document to the collection. The following algorithm applies:
     * 
     * @param id
     *            the document id of the document to be replaced
     * @param jsonString
     *            the new document, given as JSON string, which may contain expressions. If doc contains an _id value and it does not match the given id then
     *            the error will be thrown.
     * @return
     *         Result object, which will indicate the number of affected documents (0 - if none, 1 - if added, 2 - if replaced)
     */
    Result addOrReplaceOne(String id, String jsonString);

    /**
     * Return the document with the given id.
     * 
     * @param id
     *            the document id of the document to be retrieved
     * @return
     *         the document, or NULL if no match found
     */
    DbDoc getOne(String id);

    /**
     * Removes the document with the given id.
     * 
     * @param id
     *            the document id of the document to be removed
     * @return
     *         Returns a Result object, which will indicate the number of removed documents (1 or 0, if none)
     */
    Result removeOne(String id);

}
