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
 * A statement representing a set of document modifications.
 */
public interface ModifyStatement extends Statement<ModifyStatement, Result> {
    /**
     * Add/replace the order specification for this statement.
     * 
     * @param sortFields
     *            sort expression
     * @return {@link ModifyStatement}
     */
    ModifyStatement sort(String... sortFields);

    /**
     * Add/replace the document limit for this statement.
     * 
     * @param numberOfRows
     *            limit
     * @return {@link ModifyStatement}
     */
    ModifyStatement limit(long numberOfRows);

    /**
     * Add an update to the statement setting the field as the document path to the given value for all documents matching the search criteria.
     * 
     * @param docPath
     *            document path to the given value
     * @param value
     *            value to set
     * @return {@link ModifyStatement}
     */
    ModifyStatement set(String docPath, Object value);

    /**
     * Add an update to the statement setting the field, if it exists at the document path, to the given value.
     * 
     * @param docPath
     *            document path to the given value
     * @param value
     *            value to set
     * @return {@link ModifyStatement}
     */
    ModifyStatement change(String docPath, Object value);

    /**
     * Nullify the given fields.
     * 
     * @param fields
     *            one or more field names
     * @return {@link ModifyStatement}
     */
    ModifyStatement unset(String... fields);

    /**
     * Takes in a patch object and applies it on all documents matching the modify() filter, using the JSON_MERGE_PATCH() function.
     * Please note that {@link DbDoc} does not support expressions as a field values, please use {@link #patch(String)} method if you need
     * such functionality.
     * 
     * @param document
     *            patch object
     * @return {@link ModifyStatement}
     */
    ModifyStatement patch(DbDoc document);

    /**
     * Takes in a document patch and applies it on all documents matching the modify() filter, using the JSON_MERGE_PATCH() function.
     * A document patch is similar to a JSON object, with the key difference that document field values can be nested expressions in addition to literal values.
     * <br>
     * Example:<br>
     * collection.modify("_id = :id")<br>
     * .patch("{\"zip\": address.zip-300000, \"street\": CONCAT($.name, '''s street: ', $.address.street)}")<br>
     * .bind("id", "2").execute();
     * 
     * @param document
     *            patch object
     * @return {@link ModifyStatement}
     */
    ModifyStatement patch(String document);

    /**
     * Insert a value into the specified array.
     * 
     * @param field
     *            document path to the array field
     * @param value
     *            value to insert
     * @return {@link ModifyStatement}
     */
    ModifyStatement arrayInsert(String field, Object value);

    /**
     * Append a value to the specified array.
     * 
     * @param field
     *            document path to the array field
     * @param value
     *            value to append
     * @return {@link ModifyStatement}
     */
    ModifyStatement arrayAppend(String field, Object value);
}
